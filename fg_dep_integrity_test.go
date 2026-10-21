package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const (
	fgDepPreimageSHA = "7edf84d8e5b882786d1599b9002e1f566c3b40915efd8e198292a07c12eafdf2"
	fgDepBoardSHA    = "77803e2d024418fe5cdf6bab438fa51136fb80edf8b603b7166edc89aad60ccf"
	fgDepR15SHA      = "d71ec0e826df6ed256e18267ce62fcff128a16d871d9b6fd8ce599e00e7dbc7d"
)

func fgDepStatusRegistry(t *testing.T) TaskStatusRegistry {
	t.Helper()
	reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		fgRunnerDescriptor("not_started", false),
		fgRunnerDescriptor("in_progress", false),
		fgRunnerDescriptor("done", true),
		fgRunnerDescriptor("cancelled", true),
		fgRunnerDescriptor("archived", true),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func fgDepSnapshots(tasks []TaskRow, dependencies []DependencyRow) (ProjectSnapshotResult, ProjectSnapshotResult) {
	fg := ProjectSnapshotResult{
		ProjectID: "arbor", Revision: "fg/opaque:revision", Complete: true,
		Scope: SnapshotScope{Mode: snapshotScopeExecutionSubtree, RootTaskID: fgRunnerFleetRoot, Complete: true, MembershipCount: len(tasks)},
		Tasks: tasks, Dependencies: dependencies,
	}
	boardTasks := []TaskRow{{ProjectID: "arbor", TaskID: fgRunnerBoardRoot, Status: "not_started"}}
	seen := map[string]bool{fgRunnerBoardRoot: true}
	for _, task := range tasks {
		if !seen[task.TaskID] {
			boardTasks = append(boardTasks, task)
			seen[task.TaskID] = true
		}
	}
	board := ProjectSnapshotResult{
		ProjectID: "arbor", Revision: "board/opaque:revision", Complete: true,
		Scope: SnapshotScope{Mode: snapshotScopeExecutionSubtree, RootTaskID: fgRunnerBoardRoot, Complete: true, MembershipCount: len(boardTasks)},
		Tasks: boardTasks,
	}
	return fg, board
}

func fgDepTask(id, parent, level, status, note string) TaskRow {
	return TaskRow{ProjectID: "arbor", TaskID: id, ParentID: parent, Level: level, Status: status, Note: note}
}

func fgDepRow(id, source, target, kind string) DependencyRow {
	return DependencyRow{ProjectID: "arbor", DependencyID: id, TaskID: target, DependsOnTaskID: source, Kind: kind}
}

func TestFGDepIntegrityHistoricalPreimageExactEightInversions(t *testing.T) {
	// Deterministic unit derivative, not a historical capture. The selected
	// task notes and dependency IDs come from exact preimage SHA fgDepPreimageSHA;
	// selector: the eight accepted-R15 dependency_removes endpoints plus their
	// declaring L4 rows. Ancestry-only parents are derived from board SHA
	// fgDepBoardSHA and do not expand the judged task set.
	_ = fgDepPreimageSHA
	_ = fgDepBoardSHA
	groups := []struct {
		id    string
		decls []string
	}{
		{"MVP-FG-15-LANE-ROSTER-20260909", []string{"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"}},
		{"MVP-FG-ONE-TASK-ONE-PR-AGGREGATION-20260909", []string{"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"}},
		{"MVP-FG-RESTART-CHECKPOINT-DURABILITY-20260909", []string{"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"}},
		{"MVP-FG-STATUS-EVIDENCE-FRESHNESS-20260909", []string{"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"}},
		{"MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909", []string{"MVP-FG-15-LANE-ROSTER-20260909", "MVP-FG-ONE-TASK-ONE-PR-AGGREGATION-20260909", "MVP-FG-RESTART-CHECKPOINT-DURABILITY-20260909", "MVP-FG-STATUS-EVIDENCE-FRESHNESS-20260909"}},
		{"MVP-FG-PRODUCTION-SEAL-CRITERIA-20260909", []string{"MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909"}},
	}
	tasks := []TaskRow{fgDepTask(fgRunnerFleetRoot, fgRunnerBoardRoot, "L2", "not_started", "")}
	for _, group := range groups {
		tasks = append(tasks, fgDepTask(group.id, fgRunnerFleetRoot, "L4", "not_started", "dependencies="+joinTaskIDs(group.decls)))
	}
	rows := []DependencyRow{
		fgDepRow("DEP-3018ab9a-e84f-50cc-ad2e-1f37f4539651", "MVP-FG-15-LANE-ROSTER-20260909", "MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909", "gates"),
		fgDepRow("DEP-c91ec186-3a9a-5a1d-9448-00c4a284d098", "MVP-FG-ONE-TASK-ONE-PR-AGGREGATION-20260909", "MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909", "gates"),
		fgDepRow("DEP-ca3d305b-ba26-5c40-9746-31bc64c3813d", "MVP-FG-RESTART-CHECKPOINT-DURABILITY-20260909", "MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909", "gates"),
		fgDepRow("DEP-4d906c3b-1e4d-56ee-b757-f1a917eff680", "MVP-FG-STATUS-EVIDENCE-FRESHNESS-20260909", "MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909", "gates"),
		fgDepRow("DEP-7de3b3ff-a4bc-58f6-86a5-fc3690038c79", "MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909", "MVP-FG-15-LANE-ROSTER-20260909", "gates"),
		fgDepRow("DEP-225ab45c-1df9-5831-bcd9-f31484b09174", "MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909", "MVP-FG-RESTART-CHECKPOINT-DURABILITY-20260909", "gates"),
		fgDepRow("DEP-bd1c77a0-3f96-5ca1-9349-d26e5cda6a7b", "MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909", "MVP-FG-STATUS-EVIDENCE-FRESHNESS-20260909", "gates"),
		fgDepRow("DEP-29e9848f-f016-5cf0-b93c-9e519b0ae24f", "MVP-FG-PRODUCTION-SEAL-CRITERIA-20260909", "MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909", "gates"),
	}
	fg, board := fgDepSnapshots(tasks, rows)
	got, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"DEP-225ab45c-1df9-5831-bcd9-f31484b09174", "DEP-29e9848f-f016-5cf0-b93c-9e519b0ae24f",
		"DEP-3018ab9a-e84f-50cc-ad2e-1f37f4539651", "DEP-4d906c3b-1e4d-56ee-b757-f1a917eff680",
		"DEP-7de3b3ff-a4bc-58f6-86a5-fc3690038c79", "DEP-bd1c77a0-3f96-5ca1-9349-d26e5cda6a7b",
		"DEP-c91ec186-3a9a-5a1d-9448-00c4a284d098", "DEP-ca3d305b-ba26-5c40-9746-31bc64c3813d",
	}
	var ids []string
	for _, finding := range got.Direction.Findings {
		if finding.Kind == FGDependencyDirectionInverted {
			ids = append(ids, finding.DependencyID)
		}
	}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("inversion IDs = %v, want %v", ids, want)
	}
	if got.Passed || got.Direction.Passed {
		t.Fatalf("inverted preimage passed: %+v", got)
	}
}

func joinTaskIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += "|"
		}
		out += id
	}
	return out
}

func TestFGDepIntegrityGrammarLastValidAndExcludedText(t *testing.T) {
	tasks := []TaskRow{
		fgDepTask(fgRunnerFleetRoot, fgRunnerBoardRoot, "L2", "not_started", ""),
		fgDepTask("GROUP", fgRunnerFleetRoot, "L4", "not_started", "dependencies=A; dependencies=B; dependencies=BAD||VALUE"),
		fgDepTask("LEAF", "GROUP", "L5", "not_started", "prose dependencies=PROSE; prereq_gates=X; group_gate=Y; history=leaf_prerequisites=Z; leaf_prerequisites=none"),
		fgDepTask("A", fgRunnerFleetRoot, "L4", "not_started", ""),
		fgDepTask("B", fgRunnerFleetRoot, "L4", "not_started", ""),
	}
	fg, board := fgDepSnapshots(tasks, []DependencyRow{fgDepRow("DEP-B", "B", "GROUP", "gates")})
	got, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Direction.Passed || len(got.Direction.Findings) != 0 {
		t.Fatalf("grammar produced findings: %+v", got.Direction)
	}
}

func TestFGDepIntegrityMissingAndCanonicalStatusExceptions(t *testing.T) {
	tasks := []TaskRow{
		fgDepTask(fgRunnerFleetRoot, fgRunnerBoardRoot, "L2", "not_started", ""),
		fgDepTask("ACTIVE", fgRunnerFleetRoot, "L5", "not_started", "leaf_prerequisites=MISSING"),
		fgDepTask("CANCELLED", fgRunnerFleetRoot, "L5", "cancelled", "leaf_prerequisites=MISSING"),
		fgDepTask("ARCHIVED", fgRunnerFleetRoot, "L5", "archived", "leaf_prerequisites=MISSING"),
	}
	fg, board := fgDepSnapshots(tasks, nil)
	got, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Direction.Findings) != 1 || got.Direction.Findings[0].Kind != FGDependencyDirectionMissing || got.Direction.Findings[0].TaskID != "ACTIVE" {
		t.Fatalf("status exception findings: %+v", got.Direction.Findings)
	}

	fg.Tasks[1].Status = "unknown"
	if _, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board); err == nil {
		t.Fatal("unknown status did not fail closed")
	}
}

func TestFGDepIntegrityReciprocityConflictCycleAndAncestry(t *testing.T) {
	t.Run("reciprocal_projection_counted_once", func(t *testing.T) {
		tasks := []TaskRow{
			fgDepTask(fgRunnerFleetRoot, fgRunnerBoardRoot, "L2", "not_started", ""),
			fgDepTask("A", fgRunnerFleetRoot, "L5", "not_started", "leaf_prerequisites=none"),
			fgDepTask("B", fgRunnerFleetRoot, "L5", "not_started", "leaf_prerequisites=A"),
		}
		fg, board := fgDepSnapshots(tasks, []DependencyRow{fgDepRow("DEP-1", "A", "B", "gates"), fgDepRow("DEP-1", "A", "B", "gated_by")})
		got, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Passed || got.Reciprocity.LogicalEdgeCount != 1 {
			t.Fatalf("reciprocal projection result: %+v", got)
		}
	})

	t.Run("incompatible_projection", func(t *testing.T) {
		tasks := []TaskRow{fgDepTask(fgRunnerFleetRoot, fgRunnerBoardRoot, "L2", "not_started", "")}
		fg, board := fgDepSnapshots(tasks, []DependencyRow{fgDepRow("DEP-X", "A", "B", "gates"), fgDepRow("DEP-X", "A", "C", "gated_by")})
		got, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
		if err != nil {
			t.Fatal(err)
		}
		if got.Reciprocity.Passed || len(got.Reciprocity.Findings) != 1 || got.Reciprocity.Findings[0].DependencyID != "DEP-X" {
			t.Fatalf("conflict result: %+v", got.Reciprocity)
		}
	})

	t.Run("closed_two_node_cycle", func(t *testing.T) {
		tasks := []TaskRow{
			fgDepTask(fgRunnerFleetRoot, fgRunnerBoardRoot, "L2", "not_started", ""),
			fgDepTask("A", fgRunnerFleetRoot, "L5", "not_started", "leaf_prerequisites=B"),
			fgDepTask("B", fgRunnerFleetRoot, "L5", "not_started", "leaf_prerequisites=A"),
		}
		fg, board := fgDepSnapshots(tasks, []DependencyRow{fgDepRow("D1", "A", "B", "gates"), fgDepRow("D2", "B", "A", "gates")})
		got, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
		if err != nil {
			t.Fatal(err)
		}
		if got.Cycle.Passed || !reflect.DeepEqual(got.Cycle.Path, []string{"A", "B", "A"}) || !got.Cycle.BoundedToFleetGovernanceScope {
			t.Fatalf("cycle result: %+v", got.Cycle)
		}
	})

	tests := []struct {
		name string
		edit func(*ProjectSnapshotResult)
		kind FGDependencyAncestryFindingKind
	}{
		{"missing_parent", func(board *ProjectSnapshotResult) { board.Tasks[1].ParentID = "MISSING" }, FGDependencyAncestryMissingParent},
		{"parent_cycle", func(board *ProjectSnapshotResult) {
			board.Tasks[1].ParentID = "PARENT"
			board.Tasks = append(board.Tasks, fgDepTask("PARENT", "CHILD", "L4", "not_started", ""))
		}, FGDependencyAncestryParentCycle},
		{"no_direct_lane", func(board *ProjectSnapshotResult) { board.Tasks[1].ParentID = fgRunnerBoardRoot }, FGDependencyAncestryNoDirectLane},
		{"nested_lane", func(board *ProjectSnapshotResult) {
			board.Tasks[2].ParentID = "MVP-LANE-TOP"
			board.Tasks = append(board.Tasks, fgDepTask("MVP-LANE-TOP", fgRunnerBoardRoot, "L1", "not_started", ""))
		}, FGDependencyAncestryNestedLane},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tasks := []TaskRow{fgDepTask("CHILD", "MVP-LANE-DIRECT", "L5", "not_started", "leaf_prerequisites=none")}
			fg, board := fgDepSnapshots(tasks, nil)
			board.Tasks = append(board.Tasks, fgDepTask("MVP-LANE-DIRECT", fgRunnerBoardRoot, "L1", "not_started", ""))
			tc.edit(&board)
			got, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
			if err != nil {
				t.Fatal(err)
			}
			if got.Ancestry.Passed || len(got.Ancestry.Findings) == 0 || got.Ancestry.Findings[0].Kind != tc.kind {
				t.Fatalf("ancestry result: %+v", got.Ancestry)
			}
		})
	}
}

func TestFGDepIntegrityR15DerivedPositiveReplayAndCancelled(t *testing.T) {
	// Deterministic derivative of accepted R15 SHA fgDepR15SHA and exact Board
	// SHA fgDepBoardSHA. Selector: validation_results.rule1_reproduction rows,
	// their exact declared arrays, and Board parent chains. This is deliberately
	// not labeled as a post-repair FleetGovernance historical capture.
	_ = fgDepR15SHA
	groups := map[string][]string{
		"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909":    {"MVP-VALIDATE-FLEET-GOVERNANCE-20260910"},
		"MVP-FG-RESTART-CHECKPOINT-DURABILITY-20260909": {"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"},
		"MVP-FG-STATUS-EVIDENCE-FRESHNESS-20260909":     {"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"},
		"MVP-FG-ONE-TASK-ONE-PR-AGGREGATION-20260909":   {"MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"},
		"MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909":   {"MVP-DISPATCH-THREAD-ROSTER-20260909", "MVP-FG-ONE-TASK-ONE-PR-AGGREGATION-20260909", "MVP-FG-RESTART-CHECKPOINT-DURABILITY-20260909", "MVP-FG-STATUS-EVIDENCE-FRESHNESS-20260909"},
		"MVP-FG-PRODUCTION-SEAL-CRITERIA-20260909":      {"MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909"},
	}
	leaves := map[string][]string{
		"MVP-FG-ID-ENVELOPE-SCHEMA-20260911":          {},
		"MVP-FG-ID-BINDING-VALIDATOR-20260911":        {"MVP-FG-ID-ENVELOPE-SCHEMA-20260911"},
		"MVP-FG-ID-UNIQUE-BINDING-CHECK-20260911":     {"MVP-FG-ID-BINDING-VALIDATOR-20260911"},
		"MVP-FG-CKPT-SCHEMA-FIXTURE-20260911":         {"MVP-FG-ID-ENVELOPE-SCHEMA-20260911"},
		"MVP-FG-CKPT-NO-CLOBBER-SELECTOR-20260911":    {"MVP-FG-CKPT-SCHEMA-FIXTURE-20260911"},
		"MVP-FG-CKPT-RESUME-REPLAY-20260911":          {"MVP-FG-CKPT-NO-CLOBBER-SELECTOR-20260911"},
		"MVP-FG-STATUS-TERMINAL-PROOF-CHECK-20260911": {},
		"MVP-FG-STATUS-ACTIVE-BINDING-CHECK-20260911": {"MVP-FG-ID-UNIQUE-BINDING-CHECK-20260911"},
		"MVP-FG-EVIDENCE-FRESHNESS-STAMP-20260911":    {"MVP-FG-STATUS-TERMINAL-PROOF-CHECK-20260911"},
		"MVP-FG-STATUS-WRITE-GATE-DECISION-20260911":  {"MVP-FG-STATUS-ACTIVE-BINDING-CHECK-20260911", "MVP-FG-STATUS-TERMINAL-PROOF-CHECK-20260911"},
		"MVP-FG-PR-TASKLINE-EVIDENCE-20260911":        {},
		"MVP-FG-PR-GUARD-SCENARIO-RECEIPT-20260911":   {"MVP-FG-PR-TASKLINE-EVIDENCE-20260911"},
		"MVP-FG-VAL-READONLY-RUNNER-20260911":         {"MVP-FG-EVIDENCE-FRESHNESS-STAMP-20260911", "MVP-FG-ID-UNIQUE-BINDING-CHECK-20260911", "MVP-FG-STATUS-ACTIVE-BINDING-CHECK-20260911", "MVP-FG-STATUS-TERMINAL-PROOF-CHECK-20260911"},
		"MVP-FG-VAL-DEP-INTEGRITY-CHECK-20260911":     {"MVP-FG-VAL-READONLY-RUNNER-20260911"},
		"MVP-FG-VAL-RECEIPT-FREEZE-20260911":          {"MVP-FG-CKPT-RESUME-REPLAY-20260911", "MVP-FG-PR-TASKLINE-EVIDENCE-20260911", "MVP-FG-VAL-DEP-INTEGRITY-CHECK-20260911"},
		"MVP-FG-SEAL-CRITERIA-INSTRUMENTS-20260911":   {},
		"MVP-FG-SEAL-MEASUREMENT-RECEIPT-20260911":    {"MVP-FG-SEAL-CRITERIA-INSTRUMENTS-20260911", "MVP-FG-VAL-RECEIPT-FREEZE-20260911"},
	}
	tasks := []TaskRow{fgDepTask(fgRunnerFleetRoot, fgRunnerBoardRoot, "L2", "not_started", "")}
	var rows []DependencyRow
	addRows := func(prefix, target string, declared []string) {
		for i, prerequisite := range declared {
			rows = append(rows, fgDepRow(prefix+string(rune('A'+i)), prerequisite, target, "gates"))
		}
	}
	for id, declared := range groups {
		tasks = append(tasks, fgDepTask(id, "MVP-DECOMP-FLEET-GOVERNANCE-20260909", "L4", "not_started", "history=x; dependencies=OLD; dependencies="+joinTaskIDs(declared)))
		addRows("R15-G-"+id+"-", id, declared)
	}
	leafParent := func(id string) string {
		switch {
		case strings.HasPrefix(id, "MVP-FG-ID-"):
			return "MVP-FG-IDENTITY-EVIDENCE-CONTRACT-20260909"
		case strings.HasPrefix(id, "MVP-FG-CKPT-"):
			return "MVP-FG-RESTART-CHECKPOINT-DURABILITY-20260909"
		case strings.HasPrefix(id, "MVP-FG-STATUS-") || id == "MVP-FG-EVIDENCE-FRESHNESS-STAMP-20260911":
			return "MVP-FG-STATUS-EVIDENCE-FRESHNESS-20260909"
		case strings.HasPrefix(id, "MVP-FG-PR-"):
			return "MVP-FG-ONE-TASK-ONE-PR-AGGREGATION-20260909"
		case strings.HasPrefix(id, "MVP-FG-VAL-"):
			return "MVP-FG-INDEPENDENT-VALIDATOR-GATES-20260909"
		default:
			return "MVP-FG-PRODUCTION-SEAL-CRITERIA-20260909"
		}
	}
	for id, declared := range leaves {
		value := "none"
		if len(declared) > 0 {
			value = joinTaskIDs(declared)
		}
		tasks = append(tasks, fgDepTask(id, leafParent(id), "L5", "not_started", "prereq_gates=IGNORED; leaf_prerequisites="+value))
		addRows("R15-L-"+id+"-", id, declared)
	}
	tasks = append(tasks, fgDepTask("MVP-FG-15-LANE-ROSTER-20260909", "MVP-DECOMP-FLEET-GOVERNANCE-20260909", "L4", "cancelled", "dependencies=MISSING"))
	fg, board := fgDepSnapshots(tasks, rows)
	board.Tasks = append(board.Tasks,
		fgDepTask("MVP-DECOMP-FLEET-GOVERNANCE-20260909", "MVP-ACTIVATE-FLEET-GOVERNANCE-20260909", "L3", "not_started", ""),
		fgDepTask("MVP-ACTIVATE-FLEET-GOVERNANCE-20260909", fgRunnerFleetRoot, "L2", "in_progress", ""))
	first, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunFGDependencyIntegrityCheck(context.Background(), fgDepStatusRegistry(t), fg, board)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !first.Passed || !reflect.DeepEqual(a, b) {
		t.Fatalf("positive/replay failed:\n%s\n%s", a, b)
	}
	if len(groups) != 6 || len(leaves) != 17 {
		t.Fatalf("R15 selector drifted: groups=%d leaves=%d", len(groups), len(leaves))
	}
}
