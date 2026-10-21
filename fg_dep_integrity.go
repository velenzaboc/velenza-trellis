package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const fgDependencyIntegrityScopeClaim = "bounded to the FleetGovernance snapshot scope; no whole-graph claim"

type FGDependencyDirectionFindingKind string

const (
	FGDependencyDirectionMissing  FGDependencyDirectionFindingKind = "missing_edge"
	FGDependencyDirectionInverted FGDependencyDirectionFindingKind = "inverted_edge"
)

type FGDependencyDirectionFinding struct {
	Kind           FGDependencyDirectionFindingKind `json:"kind"`
	TaskID         string                           `json:"task_id"`
	PrerequisiteID string                           `json:"prerequisite_id"`
	DependencyID   string                           `json:"dependency_id,omitempty"`
}

type FGDependencyDirectionResult struct {
	Passed   bool                           `json:"passed"`
	Findings []FGDependencyDirectionFinding `json:"findings"`
}

type FGDependencyReciprocityFinding struct {
	Kind         string `json:"kind"`
	DependencyID string `json:"dependency_id"`
	Detail       string `json:"detail"`
}

type FGDependencyReciprocityResult struct {
	Passed           bool                             `json:"passed"`
	LogicalEdgeCount int                              `json:"logical_edge_count"`
	Findings         []FGDependencyReciprocityFinding `json:"findings"`
}

type FGDependencyAncestryFindingKind string

const (
	FGDependencyAncestryMissingParent FGDependencyAncestryFindingKind = "missing_parent"
	FGDependencyAncestryParentCycle   FGDependencyAncestryFindingKind = "parent_cycle"
	FGDependencyAncestryNoDirectLane  FGDependencyAncestryFindingKind = "no_direct_lane"
	FGDependencyAncestryNestedLane    FGDependencyAncestryFindingKind = "nested_lane_marker"
)

type FGDependencyAncestryFinding struct {
	Kind   FGDependencyAncestryFindingKind `json:"kind"`
	TaskID string                          `json:"task_id"`
	AtID   string                          `json:"at_id,omitempty"`
	Detail string                          `json:"detail"`
}

type FGDependencyAncestryResult struct {
	Passed   bool                          `json:"passed"`
	Findings []FGDependencyAncestryFinding `json:"findings"`
}

type FGDependencyCycleResult struct {
	Passed                        bool     `json:"passed"`
	BoundedToFleetGovernanceScope bool     `json:"bounded_to_fleet_governance_scope"`
	ScopeClaim                    string   `json:"scope_claim"`
	Path                          []string `json:"path"`
}

type FGDependencyIntegrityResult struct {
	Passed                  bool                          `json:"passed"`
	FleetGovernanceRevision string                        `json:"fleet_governance_revision"`
	BoardRevision           string                        `json:"board_revision"`
	ScopeClaim              string                        `json:"scope_claim"`
	Direction               FGDependencyDirectionResult   `json:"direction"`
	Reciprocity             FGDependencyReciprocityResult `json:"reciprocity"`
	Ancestry                FGDependencyAncestryResult    `json:"ancestry"`
	Cycle                   FGDependencyCycleResult       `json:"cycle"`
}

type fgDependencyEdge struct {
	id     string
	source string
	target string
}

func fgDependencyIntegrityInput(snapshot ProjectSnapshotResult, mode, root, label string) error {
	if !snapshot.Complete || !snapshot.Scope.Complete || snapshot.Scope.Capped || snapshot.Scope.Mode != mode || snapshot.Scope.RootTaskID != root {
		return fmt.Errorf("fleet dependency integrity: %s must be a complete uncapped %s snapshot rooted at %q", label, mode, root)
	}
	if snapshot.Revision == "" {
		return fmt.Errorf("fleet dependency integrity: %s requires an opaque revision", label)
	}
	return nil
}

// fgDependencyDeclarations returns the last syntactically valid applicable
// clause. L5 rows are leaves; every other row is a group/shared row. Splitting
// on semicolons makes the start/semicolon boundary explicit and ensures a
// malformed appended clause cannot replace the previous valid declaration.
func fgDependencyDeclarations(task TaskRow) []string {
	key := "dependencies"
	if task.Level == "L5" {
		key = "leaf_prerequisites"
	}
	var last []string
	found := false
	for _, rawClause := range strings.Split(task.Note, ";") {
		clause := strings.TrimSpace(rawClause)
		prefix := key + "="
		if !strings.HasPrefix(clause, prefix) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(clause, prefix))
		if value == "none" {
			last, found = []string{}, true
			continue
		}
		parts := strings.Split(value, "|")
		valid := len(parts) > 0
		for _, id := range parts {
			if !fgDependencyTaskID(id) {
				valid = false
				break
			}
		}
		if valid {
			last, found = append([]string(nil), parts...), true
		}
	}
	if !found {
		return nil
	}
	return last
}

func fgDependencyTaskID(id string) bool {
	if id == "" {
		return false
	}
	for i, r := range id {
		alphanumeric := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if i == 0 && !alphanumeric {
			return false
		}
		if alphanumeric || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func fgDependencyNormalize(rows []DependencyRow) ([]fgDependencyEdge, FGDependencyReciprocityResult) {
	result := FGDependencyReciprocityResult{Passed: true, Findings: []FGDependencyReciprocityFinding{}}
	byID := map[string]fgDependencyEdge{}
	conflicted := map[string]bool{}
	for _, row := range rows {
		kind := row.Kind
		if kind == "" {
			kind = "gates"
		}
		if kind != "gates" && kind != "gated_by" {
			continue
		}
		edge := fgDependencyEdge{id: row.DependencyID, source: row.DependsOnTaskID, target: row.TaskID}
		previous, exists := byID[row.DependencyID]
		if !exists {
			byID[row.DependencyID] = edge
			continue
		}
		if previous.source == edge.source && previous.target == edge.target {
			continue
		}
		if !conflicted[row.DependencyID] {
			result.Findings = append(result.Findings, FGDependencyReciprocityFinding{
				Kind: "incompatible_duplicate_projection", DependencyID: row.DependencyID,
				Detail: fmt.Sprintf("dependency %q has incompatible endpoints %q->%q and %q->%q", row.DependencyID, previous.source, previous.target, edge.source, edge.target),
			})
			conflicted[row.DependencyID] = true
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	edges := make([]fgDependencyEdge, 0, len(ids))
	for _, id := range ids {
		if !conflicted[id] {
			edges = append(edges, byID[id])
		}
	}
	sort.Slice(result.Findings, func(i, j int) bool { return result.Findings[i].DependencyID < result.Findings[j].DependencyID })
	result.LogicalEdgeCount = len(edges)
	result.Passed = len(result.Findings) == 0
	return edges, result
}

func fgDependencyDirection(ctx context.Context, statusReg TaskStatusRegistry, tasks []TaskRow, edges []fgDependencyEdge) (FGDependencyDirectionResult, error) {
	result := FGDependencyDirectionResult{Passed: true, Findings: []FGDependencyDirectionFinding{}}
	forward := map[string][]fgDependencyEdge{}
	for _, edge := range edges {
		forward[edge.source+"\x00"+edge.target] = append(forward[edge.source+"\x00"+edge.target], edge)
	}
	ordered := append([]TaskRow(nil), tasks...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].TaskID < ordered[j].TaskID })
	for _, task := range ordered {
		canonical, found := statusReg.Canonicalize(ctx, task.Status)
		if !found {
			return FGDependencyDirectionResult{}, fmt.Errorf("%w: task %q status literal %q", ErrUnknownTaskStatus, task.TaskID, task.Status)
		}
		if canonical == "cancelled" || canonical == "archived" {
			continue
		}
		for _, prerequisite := range fgDependencyDeclarations(task) {
			correct := forward[prerequisite+"\x00"+task.TaskID]
			reverse := forward[task.TaskID+"\x00"+prerequisite]
			for _, edge := range reverse {
				result.Findings = append(result.Findings, FGDependencyDirectionFinding{Kind: FGDependencyDirectionInverted, TaskID: task.TaskID, PrerequisiteID: prerequisite, DependencyID: edge.id})
			}
			if len(correct) == 0 && len(reverse) == 0 {
				result.Findings = append(result.Findings, FGDependencyDirectionFinding{Kind: FGDependencyDirectionMissing, TaskID: task.TaskID, PrerequisiteID: prerequisite})
			}
		}
	}
	sort.Slice(result.Findings, func(i, j int) bool {
		a, b := result.Findings[i], result.Findings[j]
		if a.DependencyID != b.DependencyID {
			return a.DependencyID < b.DependencyID
		}
		if a.TaskID != b.TaskID {
			return a.TaskID < b.TaskID
		}
		if a.PrerequisiteID != b.PrerequisiteID {
			return a.PrerequisiteID < b.PrerequisiteID
		}
		return a.Kind < b.Kind
	})
	result.Passed = len(result.Findings) == 0
	return result, nil
}

func fgDependencyAncestry(tasks, boardTasks []TaskRow) FGDependencyAncestryResult {
	result := FGDependencyAncestryResult{Passed: true, Findings: []FGDependencyAncestryFinding{}}
	lookup := map[string]TaskRow{}
	duplicates := map[string]bool{}
	for _, task := range boardTasks {
		if _, exists := lookup[task.TaskID]; exists {
			duplicates[task.TaskID] = true
		}
		lookup[task.TaskID] = task
	}
	ids := make([]string, 0, len(tasks))
	seenJudged := map[string]bool{}
	for _, task := range tasks {
		if task.TaskID != fgRunnerBoardRoot && !seenJudged[task.TaskID] {
			ids = append(ids, task.TaskID)
			seenJudged[task.TaskID] = true
		}
	}
	sort.Strings(ids)
	for _, taskID := range ids {
		current := taskID
		seen := map[string]bool{}
		directLaneCount := 0
		failed := false
		for current != fgRunnerBoardRoot {
			if seen[current] {
				result.Findings = append(result.Findings, FGDependencyAncestryFinding{Kind: FGDependencyAncestryParentCycle, TaskID: taskID, AtID: current, Detail: "parent chain contains a cycle"})
				failed = true
				break
			}
			seen[current] = true
			row, exists := lookup[current]
			if !exists || duplicates[current] {
				result.Findings = append(result.Findings, FGDependencyAncestryFinding{Kind: FGDependencyAncestryMissingParent, TaskID: taskID, AtID: current, Detail: "parent chain row is missing or ambiguous"})
				failed = true
				break
			}
			if strings.HasPrefix(row.TaskID, "MVP-LANE-") {
				if row.ParentID == fgRunnerBoardRoot {
					directLaneCount++
				} else {
					result.Findings = append(result.Findings, FGDependencyAncestryFinding{Kind: FGDependencyAncestryNestedLane, TaskID: taskID, AtID: row.TaskID, Detail: "lane marker is not a direct child of the board"})
					failed = true
				}
			}
			if row.ParentID == "" {
				break
			}
			current = row.ParentID
		}
		if !failed && directLaneCount != 1 {
			result.Findings = append(result.Findings, FGDependencyAncestryFinding{Kind: FGDependencyAncestryNoDirectLane, TaskID: taskID, Detail: fmt.Sprintf("parent chain reaches %d direct board lane children; want exactly one", directLaneCount)})
		}
	}
	sort.Slice(result.Findings, func(i, j int) bool {
		a, b := result.Findings[i], result.Findings[j]
		if a.TaskID != b.TaskID {
			return a.TaskID < b.TaskID
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.AtID < b.AtID
	})
	result.Passed = len(result.Findings) == 0
	return result
}

func fgDependencyCycle(edges []fgDependencyEdge) FGDependencyCycleResult {
	result := FGDependencyCycleResult{Passed: true, BoundedToFleetGovernanceScope: true, ScopeClaim: fgDependencyIntegrityScopeClaim, Path: []string{}}
	adjSet := map[string]map[string]bool{}
	nodes := map[string]bool{}
	for _, edge := range edges {
		nodes[edge.source], nodes[edge.target] = true, true
		if adjSet[edge.source] == nil {
			adjSet[edge.source] = map[string]bool{}
		}
		adjSet[edge.source][edge.target] = true
	}
	adj := map[string][]string{}
	for source, targets := range adjSet {
		for target := range targets {
			adj[source] = append(adj[source], target)
		}
		sort.Strings(adj[source])
	}
	ordered := make([]string, 0, len(nodes))
	for node := range nodes {
		ordered = append(ordered, node)
	}
	sort.Strings(ordered)
	state := map[string]uint8{}
	stack := []string{}
	stackIndex := map[string]int{}
	var visit func(string) bool
	visit = func(node string) bool {
		state[node] = 1
		stackIndex[node] = len(stack)
		stack = append(stack, node)
		for _, next := range adj[node] {
			switch state[next] {
			case 0:
				if visit(next) {
					return true
				}
			case 1:
				start := stackIndex[next]
				result.Path = append(append([]string(nil), stack[start:]...), next)
				return true
			}
		}
		stack = stack[:len(stack)-1]
		delete(stackIndex, node)
		state[node] = 2
		return false
	}
	for _, node := range ordered {
		if state[node] == 0 && visit(node) {
			result.Passed = false
			break
		}
	}
	return result
}

// RunFGDependencyIntegrityCheck is a pure read-side check over two caller-
// supplied coherent snapshots. FleetGovernance defines exactly the judged task
// and dependency set. Board is used only as a parent lookup for those tasks.
func RunFGDependencyIntegrityCheck(ctx context.Context, statusReg TaskStatusRegistry, fleetGovernance, board ProjectSnapshotResult) (FGDependencyIntegrityResult, error) {
	if err := ctx.Err(); err != nil {
		return FGDependencyIntegrityResult{}, err
	}
	if statusReg == nil {
		return FGDependencyIntegrityResult{}, fmt.Errorf("%w: RunFGDependencyIntegrityCheck requires a non-nil TaskStatusRegistry", ErrUnknownTaskStatus)
	}
	if err := fgDependencyIntegrityInput(fleetGovernance, snapshotScopeExecutionSubtree, fgRunnerFleetRoot, "fleet_governance"); err != nil {
		return FGDependencyIntegrityResult{}, err
	}
	if err := fgDependencyIntegrityInput(board, snapshotScopeExecutionSubtree, fgRunnerBoardRoot, "board"); err != nil {
		return FGDependencyIntegrityResult{}, err
	}
	edges, reciprocity := fgDependencyNormalize(fleetGovernance.Dependencies)
	direction, err := fgDependencyDirection(ctx, statusReg, fleetGovernance.Tasks, edges)
	if err != nil {
		return FGDependencyIntegrityResult{}, err
	}
	ancestry := fgDependencyAncestry(fleetGovernance.Tasks, board.Tasks)
	cycle := fgDependencyCycle(edges)
	out := FGDependencyIntegrityResult{
		FleetGovernanceRevision: fleetGovernance.Revision, BoardRevision: board.Revision,
		ScopeClaim: fgDependencyIntegrityScopeClaim, Direction: direction, Reciprocity: reciprocity,
		Ancestry: ancestry, Cycle: cycle,
	}
	out.Passed = direction.Passed && reciprocity.Passed && ancestry.Passed && cycle.Passed
	return out, nil
}
