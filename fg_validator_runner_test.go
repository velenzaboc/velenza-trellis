package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	fgRunnerProject = "arbor"
	fgRunnerReadAt  = "2026-09-30T23:40:55.215378Z"
)

func fgRunnerDescriptor(name string, terminal bool, absorbs ...string) TaskStatusDescriptor {
	raw := map[string]interface{}{"name": name, "terminal": terminal}
	if len(absorbs) > 0 {
		values := make([]interface{}, len(absorbs))
		for i := range absorbs {
			values[i] = absorbs[i]
		}
		raw["absorbs"] = values
	}
	return NewTaskStatusDescriptor(raw)
}

func fgRunnerArtifact(name string, absorbs ...string) ArtifactKindDescriptor {
	raw := map[string]interface{}{"name": name}
	if len(absorbs) > 0 {
		values := make([]interface{}, len(absorbs))
		for i := range absorbs {
			values[i] = absorbs[i]
		}
		raw["absorbs"] = values
	}
	return NewArtifactKindDescriptor(raw)
}

func fgRunnerSeal(t *testing.T, scope, root string, tasks []TaskRow, links []WorklinkRow) ProjectSnapshotResult {
	t.Helper()
	counts := TaskCountsResult{ByPillarStatus: []PillarStatusCount{}}
	hashes, err := snapshotContentHashes(tasks, []DependencyRow{}, links, counts)
	if err != nil {
		t.Fatal(err)
	}
	state := func(n int) CollectionState {
		return CollectionState{Returned: n, Collected: n, AuthorizedTotal: n, Limit: 10000, Complete: true}
	}
	return ProjectSnapshotResult{
		ProjectID: fgRunnerProject, StoreType: fleetPlanNativeStoreType, GeneratedAt: fgRunnerReadAt,
		Revision: "opaque/revision/value", ReadTimestamp: fgRunnerReadAt, Complete: true, Limit: 10000,
		Scope:         SnapshotScope{Mode: scope, RootTaskID: root, Complete: true, MembershipCount: len(tasks)},
		ContentHashes: hashes,
		Collections:   SnapshotCollections{Tasks: state(len(tasks)), Dependencies: state(0), Worklinks: state(len(links))},
		Tasks:         tasks, Dependencies: []DependencyRow{}, Worklinks: links, Counts: counts,
	}
}

func fgRunnerTask(id, parent, status string) TaskRow {
	return TaskRow{ProjectID: fgRunnerProject, TaskID: id, ParentID: parent, Status: status, Owner: "owner", CreatedAt: "2026-09-30T20:00:00Z", UpdatedAt: "2026-09-30T23:00:00Z"}
}

func fgRunnerValidBinding(id, task string) (WorklinkRow, FGValidatorBindingEvidence) {
	return WorklinkRow{
		ProjectID: fgRunnerProject, ArtifactID: id, TaskID: task, ArtifactType: "worktree", ArtifactRef: "candidate:" + id,
		Thread: "thread", CreatedAt: "2026-09-30T22:00:00Z", SessionID: "session", Repo: "task-graph",
		Branch: "task/runner", CommitSHA: "0123456789abcdef0123456789abcdef01234567", WorktreePath: "/home/developer/workspace/worktrees/runner",
	}, FGValidatorBindingEvidence{Host: "workstation-c", AccountOrHarness: "codex", PRorSpecRef: "candidate", CheckpointRef: "checkpoint#sha256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", MeasuredAt: fgRunnerReadAt}
}

func fgRunnerGoodInput(t *testing.T) FGValidatorRunnerInput {
	t.Helper()
	fleetRoot := fgRunnerTask(fgRunnerFleetRoot, "", "not_started")
	done := fgRunnerTask("DONE", fgRunnerAcceptanceRoot, "done")
	// The proof is after CreatedAt but before UpdatedAt, pinning the required
	// server-read creation time rather than the mutable update time.
	proof := WorklinkRow{ProjectID: fgRunnerProject, ArtifactID: "PROOF", TaskID: "DONE", ArtifactType: "validation", CreatedAt: "2026-09-30T21:00:00Z"}
	proof.Repo, proof.Branch, proof.CommitSHA = "task-graph", "main", "abcdef0123456789abcdef0123456789abcdef01"
	fleet := fgRunnerSeal(t, snapshotScopeExecutionSubtree, fgRunnerFleetRoot, []TaskRow{fleetRoot}, nil)
	acceptance := fgRunnerSeal(t, snapshotScopeAcceptanceDependencyClosure, fgRunnerAcceptanceRoot,
		[]TaskRow{fgRunnerTask(fgRunnerAcceptanceRoot, "", "not_started"), done}, []WorklinkRow{proof})

	boardTasks := []TaskRow{fgRunnerTask(fgRunnerBoardRoot, "", "not_started")}
	var boardLinks []WorklinkRow
	evidence := map[string]FGValidatorBindingEvidence{}
	for i := 0; i < fgRunnerExpectedLaneCount; i++ {
		lane := fmt.Sprintf("MVP-LANE-%02d", i)
		if i == 0 {
			lane = fgRunnerFleetRoot
		}
		activation := fmt.Sprintf("MVP-ACTIVATE-%02d", i)
		boardTasks = append(boardTasks, fgRunnerTask(lane, fgRunnerBoardRoot, "in_progress"), fgRunnerTask(activation, lane, "in_progress"))
		if i == fgRunnerExpectedLaneCount-1 {
			boardLinks = append(boardLinks, WorklinkRow{ProjectID: fgRunnerProject, ArtifactID: "BLOCKER", TaskID: activation, ArtifactType: "blocker_evidence", ArtifactRef: "blocker:lane-14", CreatedAt: "2026-09-30T22:00:00Z"})
			continue
		}
		link, ev := fgRunnerValidBinding(fmt.Sprintf("BIND-%02d", i), activation)
		boardLinks = append(boardLinks, link)
		evidence[link.ArtifactID] = ev
	}
	board := fgRunnerSeal(t, snapshotScopeExecutionSubtree, fgRunnerBoardRoot, boardTasks, boardLinks)
	in := FGValidatorRunnerInput{
		SchemaVersion:   1,
		TaskStatuses:    []TaskStatusDescriptor{fgRunnerDescriptor("not_started", false), fgRunnerDescriptor("in_progress", false), fgRunnerDescriptor("done", true)},
		ArtifactKinds:   []ArtifactKindDescriptor{fgRunnerArtifact("binding"), fgRunnerArtifact("worktree"), fgRunnerArtifact("blocker", "blocker_evidence"), fgRunnerArtifact("finding"), fgRunnerArtifact("validation")},
		Snapshots:       FGValidatorSnapshotInputs{FleetGovernance: fleet, Acceptance: acceptance, Board: board},
		BindingEvidence: evidence,
		Freshness: FGValidatorFreshnessInput{Window: "24h", Refs: []string{"origin/main"}, Reachability: []FGValidatorReachability{{
			Repo: "task-graph", SHA: proof.CommitSHA, Ref: "origin/main", Class: FGFreshnessReachable,
		}}},
	}
	registry, _, _ := auditDependencyRegistry(t)
	return auditAttachDependencyRegistry(t, in, registry)
}

func fgRunnerReseal(t *testing.T, snapshot *ProjectSnapshotResult) {
	t.Helper()
	hashes, err := snapshotContentHashes(snapshot.Tasks, snapshot.Dependencies, snapshot.Worklinks, snapshot.Counts)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ContentHashes = hashes
	snapshot.Scope.MembershipCount = len(snapshot.Tasks)
	snapshot.Collections.Tasks.Returned, snapshot.Collections.Tasks.Collected, snapshot.Collections.Tasks.AuthorizedTotal = len(snapshot.Tasks), len(snapshot.Tasks), len(snapshot.Tasks)
	snapshot.Collections.Dependencies.Returned, snapshot.Collections.Dependencies.Collected, snapshot.Collections.Dependencies.AuthorizedTotal = len(snapshot.Dependencies), len(snapshot.Dependencies), len(snapshot.Dependencies)
	snapshot.Collections.Worklinks.Returned, snapshot.Collections.Worklinks.Collected, snapshot.Collections.Worklinks.AuthorizedTotal = len(snapshot.Worklinks), len(snapshot.Worklinks), len(snapshot.Worklinks)
}

func TestFGValidatorRunnerHappyPathCreatedAtAndDeterminism(t *testing.T) {
	in := fgRunnerGoodInput(t)
	before, _ := json.Marshal(in)
	first, err := RunFGValidatorRunner(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunFGValidatorRunner(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("fixed input changed output:\n%s\n%s", a, b)
	}
	after, _ := json.Marshal(in)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("runner mutated input")
	}
	if !first.Passed || len(first.Checks.TerminalProof.Acceptance.Findings) != 0 || !first.Roster.Passed || len(first.Roster.Lanes) != fgRunnerExpectedLaneCount {
		t.Fatalf("unexpected result: %+v", first)
	}
	if first.Revisions.Board != "opaque/revision/value" {
		t.Fatalf("opaque revision was not preserved: %q", first.Revisions.Board)
	}
	if !first.Checks.DependencyIntegrity.Passed || first.Checks.DependencyIntegrity.FleetGovernanceRevision != "opaque/revision/value" || first.Checks.DependencyIntegrity.BoardRevision != "opaque/revision/value" {
		t.Fatalf("dependency-integrity result was not registered with opaque revisions: %+v", first.Checks.DependencyIntegrity)
	}
}

func TestFGValidatorRunnerDependencyIntegrityAffectsPassed(t *testing.T) {
	in := fgRunnerGoodInput(t)
	in.Snapshots.FleetGovernance.Tasks[0].Level = "L4"
	in.Snapshots.FleetGovernance.Tasks[0].Note = "dependencies=MISSING-PREREQUISITE"
	fgRunnerReseal(t, &in.Snapshots.FleetGovernance)

	got, err := RunFGValidatorRunner(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if got.Passed || got.Checks.DependencyIntegrity.Passed || len(got.Checks.DependencyIntegrity.Direction.Findings) != 1 {
		t.Fatalf("dependency-integrity failure did not fold into overall result: %+v", got)
	}
}

func TestFGValidatorRunnerKeepsIndependentSnapshotTimes(t *testing.T) {
	in := fgRunnerGoodInput(t)
	in.Snapshots.Acceptance.ReadTimestamp = "2026-09-30T23:39:01.62451Z"
	got, err := RunFGValidatorRunner(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if got.Checks.Freshness.FleetGovernance.MeasuredAt != fgRunnerReadAt || got.Checks.Freshness.Acceptance.MeasuredAt != "2026-09-30T23:39:01.62451Z" {
		t.Fatalf("snapshot measurement times were collapsed: %+v", got.Checks.Freshness)
	}
}

func TestFGValidatorRunnerRoutesChecksToApplicableScopesOnly(t *testing.T) {
	in := fgRunnerGoodInput(t)
	acceptanceBindingException := fgRunnerTask("ACCEPTANCE-IN-PROGRESS-NO-BINDING", fgRunnerAcceptanceRoot, "in_progress")
	acceptanceBindingException.Repo = "task-graph"
	in.Snapshots.Acceptance.Tasks = append(in.Snapshots.Acceptance.Tasks, acceptanceBindingException)
	fleetTerminalException := fgRunnerTask("FLEET-DONE-NO-PROOF", fgRunnerFleetRoot, "done")
	in.Snapshots.FleetGovernance.Tasks = append(in.Snapshots.FleetGovernance.Tasks, fleetTerminalException)
	in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, fleetTerminalException)
	fgRunnerReseal(t, &in.Snapshots.Acceptance)
	fgRunnerReseal(t, &in.Snapshots.FleetGovernance)
	fgRunnerReseal(t, &in.Snapshots.Board)

	got, err := RunFGValidatorRunner(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Passed {
		t.Fatalf("inapplicable scope checks changed verdict: %+v", got.Checks)
	}
	if !got.Checks.UniqueBinding.FleetGovernance.Passed || !got.Checks.ActiveBinding.FleetGovernance.Passed {
		t.Fatalf("execution-scope binding checks failed: %+v", got.Checks)
	}
	if len(got.Checks.TerminalProof.Acceptance.Findings) != 0 {
		t.Fatalf("acceptance terminal check failed: %+v", got.Checks.TerminalProof.Acceptance)
	}
}

func TestFGValidatorRunnerRefusesBlockerWithoutStableIdentityOrReference(t *testing.T) {
	for _, field := range []string{"artifact_id", "artifact_ref"} {
		t.Run(field, func(t *testing.T) {
			in := fgRunnerGoodInput(t)
			blocker := &in.Snapshots.Board.Worklinks[len(in.Snapshots.Board.Worklinks)-1]
			if field == "artifact_id" {
				blocker.ArtifactID = ""
			} else {
				blocker.ArtifactRef = ""
			}
			fgRunnerReseal(t, &in.Snapshots.Board)
			got, err := RunFGValidatorRunner(context.Background(), in)
			if err == nil || !reflect.DeepEqual(got, FGValidatorRunnerResult{}) {
				t.Fatalf("malformed blocker = (%+v, %v), want zero refusal", got, err)
			}
		})
	}
}

func TestFGValidatorRunnerPreflightRefusesEmptyAndDuplicateRowIdentities(t *testing.T) {
	dependency := func(id string) DependencyRow {
		return DependencyRow{ProjectID: fgRunnerProject, DependencyID: id, TaskID: fgRunnerAcceptanceRoot, DependsOnTaskID: fgRunnerAcceptanceRoot, Kind: "gates", CreatedAt: "2026-09-30T22:00:00Z"}
	}
	artifact := func(id string) WorklinkRow {
		return WorklinkRow{ProjectID: fgRunnerProject, ArtifactID: id, TaskID: fgRunnerBoardRoot, ArtifactType: "validation", ArtifactRef: "evidence:" + id, CreatedAt: "2026-09-30T22:00:00Z"}
	}
	cases := map[string]func(*FGValidatorRunnerInput){
		"empty_task_id": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, fgRunnerTask("", fgRunnerBoardRoot, "not_started"))
		},
		"duplicate_task_id": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, in.Snapshots.Board.Tasks[1])
		},
		"empty_dependency_id": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Acceptance.Dependencies = append(in.Snapshots.Acceptance.Dependencies, dependency(""))
		},
		"duplicate_dependency_id": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Acceptance.Dependencies = append(in.Snapshots.Acceptance.Dependencies, dependency("DEP"), dependency("DEP"))
		},
		"empty_artifact_id": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Board.Worklinks = append(in.Snapshots.Board.Worklinks, artifact(""))
		},
		"duplicate_artifact_id": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Board.Worklinks = append(in.Snapshots.Board.Worklinks, artifact("ART"), artifact("ART"))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := fgRunnerGoodInput(t)
			mutate(&in)
			fgRunnerReseal(t, &in.Snapshots.Acceptance)
			fgRunnerReseal(t, &in.Snapshots.Board)
			got, err := RunFGValidatorRunner(context.Background(), in)
			if err == nil || !reflect.DeepEqual(got, FGValidatorRunnerResult{}) {
				t.Fatalf("ambiguous identity = (%+v, %v), want zero refusal", got, err)
			}
		})
	}
}

func TestFGValidatorRunnerCommonPreflightRefusesBeforeChecks(t *testing.T) {
	cases := map[string]func(*ProjectSnapshotResult){
		"scope_incomplete":    func(s *ProjectSnapshotResult) { s.Scope.Complete = false },
		"scope_capped":        func(s *ProjectSnapshotResult) { s.Scope.Capped = true },
		"snapshot_incomplete": func(s *ProjectSnapshotResult) { s.Complete = false },
		"tasks_capped": func(s *ProjectSnapshotResult) {
			s.Collections.Tasks.Capped = true
			s.Collections.Tasks.Complete = false
		},
		"dependencies_capped": func(s *ProjectSnapshotResult) {
			s.Collections.Dependencies.Capped = true
			s.Collections.Dependencies.Complete = false
		},
		"worklinks_capped": func(s *ProjectSnapshotResult) {
			s.Collections.Worklinks.Capped = true
			s.Collections.Worklinks.Complete = false
		},
		"wrong_total":    func(s *ProjectSnapshotResult) { s.Collections.Tasks.AuthorizedTotal++ },
		"wrong_hash":     func(s *ProjectSnapshotResult) { s.ContentHashes.Tasks = "bad" },
		"empty_revision": func(s *ProjectSnapshotResult) { s.Revision = "" },
		"bad_timestamp":  func(s *ProjectSnapshotResult) { s.ReadTimestamp = "yesterday" },
		"wrong_project":  func(s *ProjectSnapshotResult) { s.ProjectID = "other" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := fgRunnerGoodInput(t)
			mutate(&in.Snapshots.Acceptance)
			got, err := RunFGValidatorRunner(context.Background(), in)
			if err == nil || !reflect.DeepEqual(got, FGValidatorRunnerResult{}) {
				t.Fatalf("got (%+v, %v), want zero refusal", got, err)
			}
		})
	}
}

func TestFGValidatorRunnerRosterFailsClosed(t *testing.T) {
	cases := map[string]func(*FGValidatorRunnerInput){
		"missing_lane": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Board.Tasks = in.Snapshots.Board.Tasks[:len(in.Snapshots.Board.Tasks)-2]
			in.Snapshots.Board.Worklinks = in.Snapshots.Board.Worklinks[:len(in.Snapshots.Board.Worklinks)-1]
		},
		"duplicate_lane": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, in.Snapshots.Board.Tasks[1])
		},
		"missing_activation": func(in *FGValidatorRunnerInput) { in.Snapshots.Board.Tasks[2].ParentID = "elsewhere" },
		"duplicate_activation": func(in *FGValidatorRunnerInput) {
			extra := in.Snapshots.Board.Tasks[2]
			extra.TaskID = "MVP-ACTIVATE-DUP"
			in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, extra)
		},
		"finding_only": func(in *FGValidatorRunnerInput) {
			in.Snapshots.Board.Worklinks[len(in.Snapshots.Board.Worklinks)-1].ArtifactType = "finding"
		},
		"missing_binding_evidence": func(in *FGValidatorRunnerInput) {
			delete(in.BindingEvidence, "BIND-00")
		},
		"duplicate_binding_even_with_blocker": func(in *FGValidatorRunnerInput) {
			activation := in.Snapshots.Board.Tasks[2].TaskID
			for i := 0; i < 2; i++ {
				link, ev := fgRunnerValidBinding(fmt.Sprintf("DUP-%d", i), activation)
				in.Snapshots.Board.Worklinks = append(in.Snapshots.Board.Worklinks, link)
				in.BindingEvidence[link.ArtifactID] = ev
			}
			in.Snapshots.Board.Worklinks = append(in.Snapshots.Board.Worklinks, WorklinkRow{ProjectID: fgRunnerProject, ArtifactID: "ALSO-BLOCKED", TaskID: activation, ArtifactType: "blocker", ArtifactRef: "blocker:duplicate-binding", CreatedAt: "2026-09-30T22:00:00Z"})
		},
		"unknown_kind": func(in *FGValidatorRunnerInput) { in.Snapshots.Board.Worklinks[0].ArtifactType = "mystery" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := fgRunnerGoodInput(t)
			mutate(&in)
			fgRunnerReseal(t, &in.Snapshots.Board)
			got, err := RunFGValidatorRunner(context.Background(), in)
			if name == "unknown_kind" || name == "duplicate_lane" {
				if err == nil || !reflect.DeepEqual(got, FGValidatorRunnerResult{}) {
					t.Fatalf("unknown kind = (%+v,%v), want refusal", got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Roster.Passed || got.Passed {
				t.Fatalf("case falsely passed: %+v", got.Roster)
			}
		})
	}
}

func TestFGValidatorRunnerFileCLIReadsOnlyInputAndWritesJSON(t *testing.T) {
	in := fgRunnerGoodInput(t)
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	var stdout strings.Builder
	if err := cmdFGValidatorRunner(context.Background(), []string{path}, &stdout); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("CLI changed its input")
	}
	var got FGValidatorRunnerResult
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &got); err != nil {
		t.Fatalf("stdout is not one JSON result: %v\n%s", err, stdout.String())
	}
	if !got.Passed {
		t.Fatalf("CLI result failed: %+v", got)
	}
	if err := cmdFGValidatorRunner(context.Background(), []string{}, &stdout); err == nil {
		t.Fatal("missing file must refuse")
	}
	if _, err := time.ParseDuration(in.Freshness.Window); err != nil {
		t.Fatal(err)
	}
}
