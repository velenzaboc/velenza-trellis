package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	fgRunnerFleetRoot         = "MVP-LANE-FLEET-GOVERNANCE"
	fgRunnerAcceptanceRoot    = "MVP-ACC-GRAPH-HYGIENE"
	fgRunnerBoardRoot         = "MVP-EXECUTION-BOARD-20260909"
	fgRunnerExpectedLaneCount = 15
)

type FGValidatorBindingEvidence struct {
	Host             string `json:"host"`
	AccountOrHarness string `json:"account_or_harness"`
	PRorSpecRef      string `json:"pr_or_spec_ref"`
	CheckpointRef    string `json:"checkpoint_ref"`
	MeasuredAt       string `json:"measured_at"`
}

func (e FGValidatorBindingEvidence) primitive() FGBindingEvidence {
	return FGBindingEvidence{Host: e.Host, AccountOrHarness: e.AccountOrHarness, PRorSpecRef: e.PRorSpecRef, CheckpointRef: e.CheckpointRef, MeasuredAt: e.MeasuredAt}
}

type FGValidatorSnapshotInputs struct {
	FleetGovernance ProjectSnapshotResult `json:"fleet_governance"`
	Acceptance      ProjectSnapshotResult `json:"acceptance"`
	Board           ProjectSnapshotResult `json:"board"`
}

type FGValidatorReachability struct {
	Repo   string                `json:"repo"`
	SHA    string                `json:"sha"`
	Ref    string                `json:"ref"`
	Class  FGFreshnessReachClass `json:"class"`
	Reason string                `json:"reason,omitempty"`
}

type FGValidatorFreshnessInput struct {
	Window       string                    `json:"window"`
	Refs         []string                  `json:"refs"`
	Reachability []FGValidatorReachability `json:"reachability"`
}

type FGValidatorRunnerInput struct {
	SchemaVersion   int                                   `json:"schema_version"`
	TaskStatuses    []TaskStatusDescriptor                `json:"task_statuses"`
	DependencyKinds []DependencyKindDescriptor            `json:"dependency_kinds"`
	ArtifactKinds   []ArtifactKindDescriptor              `json:"artifact_kinds"`
	Snapshots       FGValidatorSnapshotInputs             `json:"snapshots"`
	BindingEvidence map[string]FGValidatorBindingEvidence `json:"binding_evidence"`
	Freshness       FGValidatorFreshnessInput             `json:"freshness"`
}

type FGValidatorRevisions struct {
	FleetGovernance string `json:"fleet_governance"`
	Acceptance      string `json:"acceptance"`
	Board           string `json:"board"`
}

type FGValidatorUniqueResults struct {
	FleetGovernance FGUniqueBindingCheckResult `json:"fleet_governance"`
}

type FGValidatorActiveResults struct {
	FleetGovernance FGActiveBindingCheckResult `json:"fleet_governance"`
}

type FGValidatorTerminalResults struct {
	Acceptance FGTerminalProofCheckResult `json:"acceptance"`
}

type FGValidatorFreshnessResults struct {
	FleetGovernance FGFreshnessResult `json:"fleet_governance"`
	Acceptance      FGFreshnessResult `json:"acceptance"`
}

type FGValidatorCheckResults struct {
	UniqueBinding       FGValidatorUniqueResults    `json:"unique_binding"`
	ActiveBinding       FGValidatorActiveResults    `json:"active_binding"`
	TerminalProof       FGValidatorTerminalResults  `json:"terminal_proof"`
	Freshness           FGValidatorFreshnessResults `json:"freshness"`
	DependencyIntegrity FGDependencyIntegrityResult `json:"dependency_integrity"`
}

type FGValidatorRosterLane struct {
	LaneTaskID       string   `json:"lane_task_id"`
	ActivationTaskID string   `json:"activation_task_id,omitempty"`
	BindingCount     int      `json:"binding_count"`
	BindingLinkIDs   []string `json:"binding_link_ids"`
	BlockerLinkIDs   []string `json:"blocker_link_ids"`
	Findings         []string `json:"findings"`
	Passed           bool     `json:"passed"`
}

type FGValidatorRosterResult struct {
	Passed            bool                    `json:"passed"`
	ExpectedLaneCount int                     `json:"expected_lane_count"`
	LaneCount         int                     `json:"lane_count"`
	Findings          []string                `json:"findings"`
	Lanes             []FGValidatorRosterLane `json:"lanes"`
}

type FGValidatorRunnerResult struct {
	SchemaVersion int                     `json:"schema_version"`
	ProjectID     string                  `json:"project_id"`
	Revisions     FGValidatorRevisions    `json:"revisions"`
	Checks        FGValidatorCheckResults `json:"checks"`
	Roster        FGValidatorRosterResult `json:"roster"`
	Passed        bool                    `json:"passed"`
}

type fgValidatorReachabilityKey struct{ repo, sha, ref string }
type fgValidatorFrozenReader map[fgValidatorReachabilityKey]FGFreshnessRefResult

func (r fgValidatorFrozenReader) Reachability(_ context.Context, repo, sha, ref string) FGFreshnessRefResult {
	if result, ok := r[fgValidatorReachabilityKey{repo, sha, ref}]; ok {
		return result
	}
	return FGFreshnessRefResult{Ref: ref, Class: FGFreshnessUnverifiable, Reason: "frozen_result_missing"}
}

func fgValidatorCollection(label string, state CollectionState, rows int, limit int) error {
	if state.Capped || !state.Complete || state.Returned != rows || state.Collected != rows || state.AuthorizedTotal != rows || state.NextCursor != "" {
		return fmt.Errorf("fg validator runner: %s collection is not a complete uncapped exact-total capture", label)
	}
	if limit <= 0 || state.Limit != limit {
		return fmt.Errorf("fg validator runner: %s collection limit does not match the snapshot limit", label)
	}
	return nil
}

func fgValidatorPreflightSnapshot(label string, snapshot ProjectSnapshotResult, project, mode, root string) error {
	if snapshot.ProjectID == "" || (project != "" && snapshot.ProjectID != project) {
		return fmt.Errorf("fg validator runner: %s project_id mismatch", label)
	}
	if snapshot.StoreType != fleetPlanNativeStoreType {
		return fmt.Errorf("fg validator runner: %s store_type must be %q", label, fleetPlanNativeStoreType)
	}
	if snapshot.Scope.Mode != mode || snapshot.Scope.RootTaskID != root {
		return fmt.Errorf("fg validator runner: %s scope/root mismatch", label)
	}
	if !snapshot.Scope.Complete || snapshot.Scope.Capped || len(snapshot.Scope.MissingTaskIDs) != 0 || snapshot.Scope.MembershipCount != len(snapshot.Tasks) {
		return fmt.Errorf("fg validator runner: %s scope is not a complete uncapped exact-membership capture", label)
	}
	if !snapshot.Complete || snapshot.Continuation != "" || snapshot.Revision == "" {
		return fmt.Errorf("fg validator runner: %s snapshot is incomplete or has no opaque revision", label)
	}
	readAt, err := fgFreshnessParseTime(label+" read_timestamp", snapshot.ReadTimestamp)
	if err != nil {
		return fmt.Errorf("fg validator runner: %s read_timestamp: %w", label, err)
	}
	if readAt.IsZero() {
		return fmt.Errorf("fg validator runner: %s read_timestamp resolves to the zero time", label)
	}
	if err := fgValidatorCollection(label+" tasks", snapshot.Collections.Tasks, len(snapshot.Tasks), snapshot.Limit); err != nil {
		return err
	}
	if err := fgValidatorCollection(label+" dependencies", snapshot.Collections.Dependencies, len(snapshot.Dependencies), snapshot.Limit); err != nil {
		return err
	}
	if err := fgValidatorCollection(label+" worklinks", snapshot.Collections.Worklinks, len(snapshot.Worklinks), snapshot.Limit); err != nil {
		return err
	}

	rootCount := 0
	tasks := make(map[string]bool, len(snapshot.Tasks))
	for _, row := range snapshot.Tasks {
		if row.TaskID == "" {
			return fmt.Errorf("fg validator runner: %s task has empty task_id", label)
		}
		if tasks[row.TaskID] {
			return fmt.Errorf("fg validator runner: %s task_id %q appears more than once", label, row.TaskID)
		}
		if row.ProjectID != snapshot.ProjectID {
			return fmt.Errorf("fg validator runner: %s task %q project mismatch", label, row.TaskID)
		}
		if row.TaskID == root {
			rootCount++
		}
		if _, err := fgFreshnessParseTime(label+" task "+row.TaskID+" created_at", row.CreatedAt); err != nil {
			return err
		}
		if _, err := fgFreshnessParseTime(label+" task "+row.TaskID+" updated_at", row.UpdatedAt); err != nil {
			return err
		}
		tasks[row.TaskID] = true
	}
	if rootCount != 1 {
		return fmt.Errorf("fg validator runner: %s requires exactly one root task %q", label, root)
	}
	dependencies := make(map[string]bool, len(snapshot.Dependencies))
	for _, row := range snapshot.Dependencies {
		if row.DependencyID == "" {
			return fmt.Errorf("fg validator runner: %s dependency has empty dependency_id", label)
		}
		if dependencies[row.DependencyID] {
			return fmt.Errorf("fg validator runner: %s dependency_id %q appears more than once", label, row.DependencyID)
		}
		dependencies[row.DependencyID] = true
		if row.ProjectID != snapshot.ProjectID {
			return fmt.Errorf("fg validator runner: %s dependency %q project mismatch", label, row.DependencyID)
		}
		if _, err := fgFreshnessParseTime(label+" dependency "+row.DependencyID+" created_at", row.CreatedAt); err != nil {
			return err
		}
	}
	artifacts := make(map[string]bool, len(snapshot.Worklinks))
	for _, row := range snapshot.Worklinks {
		if row.ArtifactID == "" {
			return fmt.Errorf("fg validator runner: %s worklink has empty artifact_id", label)
		}
		if artifacts[row.ArtifactID] {
			return fmt.Errorf("fg validator runner: %s artifact_id %q appears more than once", label, row.ArtifactID)
		}
		artifacts[row.ArtifactID] = true
		if row.ProjectID != snapshot.ProjectID {
			return fmt.Errorf("fg validator runner: %s worklink %q project mismatch", label, row.ArtifactID)
		}
		if !tasks[row.TaskID] {
			return fmt.Errorf("fg validator runner: %s worklink %q names an out-of-scope task", label, row.ArtifactID)
		}
		if _, err := fgFreshnessParseTime(label+" worklink "+row.ArtifactID+" created_at", row.CreatedAt); err != nil {
			return err
		}
	}
	hashes, err := snapshotContentHashes(snapshot.Tasks, snapshot.Dependencies, snapshot.Worklinks, snapshot.Counts)
	if err != nil {
		return fmt.Errorf("fg validator runner: %s content hashes: %w", label, err)
	}
	if hashes != snapshot.ContentHashes {
		return fmt.Errorf("fg validator runner: %s content hashes do not match the captured rows", label)
	}
	return nil
}

func fgValidatorPrimitiveEvidence(in map[string]FGValidatorBindingEvidence) map[string]FGBindingEvidence {
	out := make(map[string]FGBindingEvidence, len(in))
	for id, evidence := range in {
		out[id] = evidence.primitive()
	}
	return out
}

func fgValidatorTerminalInput(snapshot ProjectSnapshotResult) ([]FGTaskSnapshot, []FGWorklinkSnapshot, error) {
	tasks := make([]FGTaskSnapshot, 0, len(snapshot.Tasks))
	for _, row := range snapshot.Tasks {
		created, err := fgFreshnessParseTime("task "+row.TaskID+" created_at", row.CreatedAt)
		if err != nil {
			return nil, nil, err
		}
		tasks = append(tasks, FGTaskSnapshot{TaskID: row.TaskID, Status: row.Status, ParentID: row.ParentID, CreatedAt: created})
	}
	links := make([]FGWorklinkSnapshot, 0, len(snapshot.Worklinks))
	for _, row := range snapshot.Worklinks {
		created, err := fgFreshnessParseTime("worklink "+row.ArtifactID+" created_at", row.CreatedAt)
		if err != nil {
			return nil, nil, err
		}
		links = append(links, FGWorklinkSnapshot{TaskID: row.TaskID, ArtifactType: row.ArtifactType, CreatedAt: created})
	}
	return tasks, links, nil
}

func fgValidatorRunExecutionScope(ctx context.Context, artifactReg ArtifactKindRegistry, snapshot ProjectSnapshotResult, evidence map[string]FGBindingEvidence, window time.Duration, refs []string, reader FGFreshnessReader) (FGUniqueBindingCheckResult, FGActiveBindingCheckResult, FGFreshnessResult, error) {
	unique, err := RunFGUniqueBindingCheck(ctx, artifactReg, snapshot, evidence)
	if err != nil {
		return FGUniqueBindingCheckResult{}, FGActiveBindingCheckResult{}, FGFreshnessResult{}, err
	}
	active, err := RunFGActiveBindingCheck(ctx, artifactReg, snapshot, evidence)
	if err != nil {
		return FGUniqueBindingCheckResult{}, FGActiveBindingCheckResult{}, FGFreshnessResult{}, err
	}
	freshness, err := RunFGFreshnessCheck(ctx, artifactReg, snapshot, FGFreshnessOptions{MeasuredAt: snapshot.ReadTimestamp, Window: window, Refs: append([]string(nil), refs...), Reader: reader})
	if err != nil {
		return FGUniqueBindingCheckResult{}, FGActiveBindingCheckResult{}, FGFreshnessResult{}, err
	}
	return unique, active, freshness, nil
}

func fgValidatorRunAcceptanceScope(ctx context.Context, statusReg TaskStatusRegistry, artifactReg ArtifactKindRegistry, snapshot ProjectSnapshotResult, window time.Duration, refs []string, reader FGFreshnessReader) (FGTerminalProofCheckResult, FGFreshnessResult, error) {
	tasks, links, err := fgValidatorTerminalInput(snapshot)
	if err != nil {
		return FGTerminalProofCheckResult{}, FGFreshnessResult{}, err
	}
	terminal, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, links)
	if err != nil {
		return FGTerminalProofCheckResult{}, FGFreshnessResult{}, err
	}
	freshness, err := RunFGFreshnessCheck(ctx, artifactReg, snapshot, FGFreshnessOptions{MeasuredAt: snapshot.ReadTimestamp, Window: window, Refs: append([]string(nil), refs...), Reader: reader})
	if err != nil {
		return FGTerminalProofCheckResult{}, FGFreshnessResult{}, err
	}
	return terminal, freshness, nil
}

func fgValidatorRoster(ctx context.Context, reg ArtifactKindRegistry, snapshot ProjectSnapshotResult, evidence map[string]FGBindingEvidence) (FGValidatorRosterResult, error) {
	out := FGValidatorRosterResult{Passed: true, ExpectedLaneCount: fgRunnerExpectedLaneCount, Findings: []string{}, Lanes: []FGValidatorRosterLane{}}
	taskCounts := map[string]int{}
	children := map[string][]TaskRow{}
	for _, task := range snapshot.Tasks {
		taskCounts[task.TaskID]++
		children[task.ParentID] = append(children[task.ParentID], task)
	}
	lanes := make([]TaskRow, 0)
	for _, task := range children[fgRunnerBoardRoot] {
		if strings.HasPrefix(task.TaskID, "MVP-LANE-") {
			lanes = append(lanes, task)
		}
	}
	sort.Slice(lanes, func(i, j int) bool { return lanes[i].TaskID < lanes[j].TaskID })
	out.LaneCount = len(lanes)
	if len(lanes) != fgRunnerExpectedLaneCount {
		out.Passed = false
		out.Findings = append(out.Findings, fmt.Sprintf("lane_count=%d want=%d", len(lanes), fgRunnerExpectedLaneCount))
	}
	linksByTask := map[string][]WorklinkRow{}
	for _, link := range snapshot.Worklinks {
		if _, found := reg.Canonicalize(ctx, link.ArtifactType); !found {
			return FGValidatorRosterResult{}, fmt.Errorf("%w: board worklink %q artifact_type %q", ErrUnknownArtifactKind, link.ArtifactID, link.ArtifactType)
		}
		linksByTask[link.TaskID] = append(linksByTask[link.TaskID], link)
	}
	for _, lane := range lanes {
		row := FGValidatorRosterLane{LaneTaskID: lane.TaskID, BindingLinkIDs: []string{}, BlockerLinkIDs: []string{}, Findings: []string{}}
		if taskCounts[lane.TaskID] != 1 {
			row.Findings = append(row.Findings, "duplicate_lane_task")
		}
		activations := make([]TaskRow, 0)
		for _, child := range children[lane.TaskID] {
			if strings.HasPrefix(child.TaskID, "MVP-ACTIVATE-") {
				activations = append(activations, child)
			}
		}
		sort.Slice(activations, func(i, j int) bool { return activations[i].TaskID < activations[j].TaskID })
		if len(activations) != 1 {
			row.Findings = append(row.Findings, fmt.Sprintf("activation_count=%d", len(activations)))
		} else {
			row.ActivationTaskID = activations[0].TaskID
			if taskCounts[row.ActivationTaskID] != 1 {
				row.Findings = append(row.Findings, "duplicate_activation_task")
			}
			for _, link := range linksByTask[row.ActivationTaskID] {
				kind, _ := reg.Canonicalize(ctx, link.ArtifactType)
				switch kind {
				case "binding", "worktree":
					if _, err := ValidateFGBinding(ctx, reg, link, evidence[link.ArtifactID]); err == nil {
						row.BindingLinkIDs = append(row.BindingLinkIDs, link.ArtifactID)
					}
				case "blocker":
					if link.ArtifactID == "" || link.ArtifactRef == "" {
						return FGValidatorRosterResult{}, fmt.Errorf("fg validator runner: typed blocker on activation %q requires artifact_id and artifact_ref", row.ActivationTaskID)
					}
					row.BlockerLinkIDs = append(row.BlockerLinkIDs, link.ArtifactID)
				}
			}
			sort.Strings(row.BindingLinkIDs)
			sort.Strings(row.BlockerLinkIDs)
			row.BindingCount = len(row.BindingLinkIDs)
			if row.BindingCount > 1 {
				row.Findings = append(row.Findings, "multiple_valid_bindings")
			} else if row.BindingCount == 0 && len(row.BlockerLinkIDs) == 0 {
				row.Findings = append(row.Findings, "missing_valid_binding_or_typed_blocker")
			}
		}
		row.Passed = len(row.Findings) == 0
		out.Passed = out.Passed && row.Passed
		out.Lanes = append(out.Lanes, row)
	}
	return out, nil
}

func RunFGValidatorRunner(ctx context.Context, in FGValidatorRunnerInput) (FGValidatorRunnerResult, error) {
	if err := ctx.Err(); err != nil {
		return FGValidatorRunnerResult{}, err
	}
	if in.SchemaVersion != 1 {
		return FGValidatorRunnerResult{}, fmt.Errorf("fg validator runner: schema_version must be 1")
	}
	for i, d := range in.TaskStatuses {
		if err := d.Validate(i); err != nil {
			return FGValidatorRunnerResult{}, err
		}
	}
	if len(in.DependencyKinds) == 0 {
		return FGValidatorRunnerResult{}, fmt.Errorf("%w: fg validator runner requires dependency_kinds", ErrUnknownDependencyKind)
	}
	for i, d := range in.DependencyKinds {
		if err := d.Validate(i); err != nil {
			return FGValidatorRunnerResult{}, err
		}
	}
	for i, d := range in.ArtifactKinds {
		if err := d.Validate(i); err != nil {
			return FGValidatorRunnerResult{}, err
		}
	}
	statusReg, err := newTaskStatusRegistry(in.TaskStatuses)
	if err != nil {
		return FGValidatorRunnerResult{}, err
	}
	dependencyReg, err := newDependencyKindRegistry(in.DependencyKinds)
	if err != nil {
		return FGValidatorRunnerResult{}, err
	}
	artifactReg, err := newArtifactKindRegistry(in.ArtifactKinds)
	if err != nil {
		return FGValidatorRunnerResult{}, err
	}
	project := in.Snapshots.FleetGovernance.ProjectID
	if err := fgValidatorPreflightSnapshot("fleet_governance", in.Snapshots.FleetGovernance, project, snapshotScopeExecutionSubtree, fgRunnerFleetRoot); err != nil {
		return FGValidatorRunnerResult{}, err
	}
	if err := fgValidatorPreflightSnapshot("acceptance", in.Snapshots.Acceptance, project, snapshotScopeAcceptanceDependencyClosure, fgRunnerAcceptanceRoot); err != nil {
		return FGValidatorRunnerResult{}, err
	}
	if err := fgValidatorPreflightSnapshot("board", in.Snapshots.Board, project, snapshotScopeExecutionSubtree, fgRunnerBoardRoot); err != nil {
		return FGValidatorRunnerResult{}, err
	}
	dependencyIntegrity, err := RunFGDependencyIntegrityCheck(ctx, statusReg, in.Snapshots.FleetGovernance, in.Snapshots.Board, dependencyReg)
	if err != nil {
		return FGValidatorRunnerResult{}, err
	}
	window, err := time.ParseDuration(in.Freshness.Window)
	if err != nil || window <= 0 {
		return FGValidatorRunnerResult{}, fmt.Errorf("fg validator runner: freshness window must be an explicit positive duration")
	}
	reader := fgValidatorFrozenReader{}
	for _, row := range in.Freshness.Reachability {
		if row.Repo == "" || !fgBindingCommitPattern.MatchString(row.SHA) || !fgFreshnessRefOK(row.Ref) {
			return FGValidatorRunnerResult{}, fmt.Errorf("fg validator runner: invalid frozen reachability key")
		}
		if row.Class != FGFreshnessReachable && row.Class != FGFreshnessNotReachable && row.Class != FGFreshnessUnverifiable {
			return FGValidatorRunnerResult{}, fmt.Errorf("fg validator runner: invalid frozen reachability class %q", row.Class)
		}
		key := fgValidatorReachabilityKey{row.Repo, row.SHA, row.Ref}
		if _, dup := reader[key]; dup {
			return FGValidatorRunnerResult{}, fmt.Errorf("fg validator runner: duplicate frozen reachability key")
		}
		reader[key] = FGFreshnessRefResult{Ref: row.Ref, Class: row.Class, Reason: row.Reason}
	}
	evidence := fgValidatorPrimitiveEvidence(in.BindingEvidence)
	fU, fA, fF, err := fgValidatorRunExecutionScope(ctx, artifactReg, in.Snapshots.FleetGovernance, evidence, window, in.Freshness.Refs, reader)
	if err != nil {
		return FGValidatorRunnerResult{}, err
	}
	aT, aF, err := fgValidatorRunAcceptanceScope(ctx, statusReg, artifactReg, in.Snapshots.Acceptance, window, in.Freshness.Refs, reader)
	if err != nil {
		return FGValidatorRunnerResult{}, err
	}
	roster, err := fgValidatorRoster(ctx, artifactReg, in.Snapshots.Board, evidence)
	if err != nil {
		return FGValidatorRunnerResult{}, err
	}
	out := FGValidatorRunnerResult{SchemaVersion: 1, ProjectID: project,
		Revisions: FGValidatorRevisions{FleetGovernance: in.Snapshots.FleetGovernance.Revision, Acceptance: in.Snapshots.Acceptance.Revision, Board: in.Snapshots.Board.Revision},
		Checks: FGValidatorCheckResults{
			UniqueBinding:       FGValidatorUniqueResults{FleetGovernance: fU},
			ActiveBinding:       FGValidatorActiveResults{FleetGovernance: fA},
			TerminalProof:       FGValidatorTerminalResults{Acceptance: aT},
			Freshness:           FGValidatorFreshnessResults{FleetGovernance: fF, Acceptance: aF},
			DependencyIntegrity: dependencyIntegrity,
		}, Roster: roster}
	out.Passed = fU.Passed && fA.Passed && len(aT.Findings) == 0 && dependencyIntegrity.Passed && roster.Passed
	return out, nil
}

func cmdFGValidatorRunner(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("fg-validator-runner requires exactly one input JSON file")
	}
	b, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("fg-validator-runner read input: %w", err)
	}
	var in FGValidatorRunnerInput
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return fmt.Errorf("fg-validator-runner decode input: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("fg-validator-runner input must contain exactly one JSON value")
	}
	out, err := RunFGValidatorRunner(ctx, in)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		return fmt.Errorf("fg-validator-runner encode output: %w", err)
	}
	if !out.Passed {
		return fmt.Errorf("fg-validator-runner validation failed")
	}
	return nil
}
