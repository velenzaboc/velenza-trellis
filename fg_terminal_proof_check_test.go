package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fg_terminal_proof_check_test.go is the unit-level (no Spanner emulator)
// table-test suite for MVP-FG-STATUS-TERMINAL-PROOF-CHECK-20260911. Every
// function under test is pure — no I/O — so this runs in the default
// `go test ./...`.

// newFGTestRegistries builds a TaskStatusRegistry and ArtifactKindRegistry
// mirroring the members and absorbs lists this check actually depends on in
// task-graph.config.json (task_statuses / artifact_kinds), scoped to the
// subset these tests exercise. Mirrors the construction pattern used
// throughout registries_test.go.
func newFGTestRegistries(t *testing.T) (TaskStatusRegistry, ArtifactKindRegistry) {
	t.Helper()

	statusReg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{
			"name": "not_started", "terminal": false,
			"absorbs": []interface{}{"not_started", "discovered", "pending", "gap", "planned"},
		}),
		NewTaskStatusDescriptor(map[string]interface{}{
			"name": "in_progress", "terminal": false,
			"absorbs": []interface{}{"in_progress"},
		}),
		NewTaskStatusDescriptor(map[string]interface{}{
			"name": "done", "terminal": true,
			"absorbs": []interface{}{"done", "completed", "complete", "resolved", "admitted_live", "audit_probe"},
		}),
		NewTaskStatusDescriptor(map[string]interface{}{
			"name": "cancelled", "terminal": true,
			"absorbs": []interface{}{"cancelled", "invalid", "out_of_scope_current_goal", "superseded"},
		}),
	})
	if err != nil {
		t.Fatalf("build TaskStatusRegistry: %v", err)
	}

	// artifact_kinds: the eight proof-set members (each a canonical name in
	// its own right, per task-graph.config.json) plus "dispatch" and
	// "finding" — the two canonical kinds the live negative-control row
	// actually carries, deliberately outside the proof set.
	artifactReg, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "validation"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "review"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "evidence"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "pr"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "commit"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "admission"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "deploy"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "runtime"}),
		NewArtifactKindDescriptor(map[string]interface{}{
			"name": "dispatch", "absorbs": []interface{}{"agent", "agent_dispatch"},
		}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "finding"}),
	})
	if err != nil {
		t.Fatalf("build ArtifactKindRegistry: %v", err)
	}

	return statusReg, artifactReg
}

// TestFGTerminalProofCheck is the single entry point named by the row's
// evidence_command (go test -race -count=1 -run TestFGTerminalProof ./...);
// it fans out into every acceptance criterion as a subtest so a failure
// names exactly which criterion broke.
func TestFGTerminalProofCheck(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	t.Run("acceptance_positive_done_with_validation_worklink_after_creation_passes", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-001", Status: "done", CreatedAt: t0},
		}
		worklinks := []FGWorklinkSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-001", ArtifactType: "validation", CreatedAt: t0.Add(time.Hour)},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 0 {
			t.Fatalf("expected zero findings for a done task with a post-creation validation worklink, got %+v", got.Findings)
		}
	})

	t.Run("acceptance_negative_live_row_MVP_LANE_RESTART_PROMPT_20260909_reports_done_without_proof_kind", func(t *testing.T) {
		// Fixture built from the measured live read of MVP-LANE-RESTART-PROMPT-20260909
		// at 2026-09-11T14:02:30.609016Z: status done, four active worklinks —
		// LNK-14d3007c dispatch, LNK-5b2982f7 dispatch, LNK-d4ff7b48 finding,
		// LNK-b21907e6 dispatch — none a member of the proof-kind set.
		statusReg, artifactReg := newFGTestRegistries(t)
		created := t0
		tasks := []FGTaskSnapshot{
			{TaskID: "MVP-LANE-RESTART-PROMPT-20260909", Status: "done", CreatedAt: created},
		}
		worklinks := []FGWorklinkSnapshot{
			{TaskID: "MVP-LANE-RESTART-PROMPT-20260909", ArtifactType: "dispatch", CreatedAt: created.Add(time.Minute)},
			{TaskID: "MVP-LANE-RESTART-PROMPT-20260909", ArtifactType: "dispatch", CreatedAt: created.Add(2 * time.Minute)},
			{TaskID: "MVP-LANE-RESTART-PROMPT-20260909", ArtifactType: "finding", CreatedAt: created.Add(3 * time.Minute)},
			{TaskID: "MVP-LANE-RESTART-PROMPT-20260909", ArtifactType: "dispatch", CreatedAt: created.Add(4 * time.Minute)},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 1 {
			t.Fatalf("expected exactly one finding, got %d: %+v", len(got.Findings), got.Findings)
		}
		f := got.Findings[0]
		if f.Kind != FGFindingDoneWithoutProofKind {
			t.Fatalf("expected kind %q, got %q", FGFindingDoneWithoutProofKind, f.Kind)
		}
		if f.TaskID != "MVP-LANE-RESTART-PROMPT-20260909" {
			t.Fatalf("expected finding to name MVP-LANE-RESTART-PROMPT-20260909, got %q", f.TaskID)
		}
	})

	t.Run("done_row_with_zero_worklinks_fails_naming_the_row", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-ZERO-LINKS", Status: "done", CreatedAt: t0},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 1 {
			t.Fatalf("expected exactly one finding, got %d: %+v", len(got.Findings), got.Findings)
		}
		if got.Findings[0].TaskID != "SYNTH-TEST-ONLY-ZERO-LINKS" {
			t.Fatalf("finding must name the zero-worklink row, got %q", got.Findings[0].TaskID)
		}
		if got.Findings[0].Kind != FGFindingDoneWithoutProofKind {
			t.Fatalf("expected kind %q, got %q", FGFindingDoneWithoutProofKind, got.Findings[0].Kind)
		}
	})

	t.Run("done_parent_with_not_started_child_is_flagged", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-PARENT", Status: "done", CreatedAt: t0},
			{TaskID: "SYNTH-TEST-ONLY-CHILD", Status: "not_started", ParentID: "SYNTH-TEST-ONLY-PARENT", CreatedAt: t0},
		}
		worklinks := []FGWorklinkSnapshot{
			// Parent has proof, so it must NOT also raise a done_without_proof_kind
			// finding — this subtest isolates the parent/child check.
			{TaskID: "SYNTH-TEST-ONLY-PARENT", ArtifactType: "validation", CreatedAt: t0.Add(time.Hour)},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 1 {
			t.Fatalf("expected exactly one finding, got %d: %+v", len(got.Findings), got.Findings)
		}
		f := got.Findings[0]
		if f.Kind != FGFindingTerminalParentNonTerminalChild {
			t.Fatalf("expected kind %q, got %q", FGFindingTerminalParentNonTerminalChild, f.Kind)
		}
		if f.TaskID != "SYNTH-TEST-ONLY-PARENT" || f.ChildID != "SYNTH-TEST-ONLY-CHILD" {
			t.Fatalf("expected parent=SYNTH-TEST-ONLY-PARENT child=SYNTH-TEST-ONLY-CHILD, got parent=%q child=%q", f.TaskID, f.ChildID)
		}
		if f.ChildStatus != "not_started" {
			t.Fatalf("expected child_status not_started, got %q", f.ChildStatus)
		}
	})

	t.Run("cancelled_parent_with_done_child_is_not_flagged_both_terminal", func(t *testing.T) {
		// A terminal parent with a TERMINAL child (done, cancelled, ...) is
		// fine — only a non-terminal child is a finding.
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-PARENT-2", Status: "cancelled", CreatedAt: t0},
			{TaskID: "SYNTH-TEST-ONLY-CHILD-2", Status: "done", ParentID: "SYNTH-TEST-ONLY-PARENT-2", CreatedAt: t0},
		}
		worklinks := []FGWorklinkSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-CHILD-2", ArtifactType: "pr", CreatedAt: t0.Add(time.Hour)},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 0 {
			t.Fatalf("expected zero findings, got %+v", got.Findings)
		}
	})

	t.Run("proof_kind_set_never_admits_finding_or_dispatch", func(t *testing.T) {
		if fgProofKindSet["finding"] {
			t.Fatal("fgProofKindSet must never admit \"finding\"")
		}
		if fgProofKindSet["dispatch"] {
			t.Fatal("fgProofKindSet must never admit \"dispatch\"")
		}
		wantMembers := []string{"validation", "review", "evidence", "pr", "commit", "admission", "deploy", "runtime"}
		if len(fgProofKindSet) != len(wantMembers) {
			t.Fatalf("expected exactly %d proof-kind members, got %d: %v", len(wantMembers), len(fgProofKindSet), fgProofKindSet)
		}
		for _, m := range wantMembers {
			if !fgProofKindSet[m] {
				t.Fatalf("expected fgProofKindSet to admit %q", m)
			}
		}
	})

	t.Run("worklink_created_exactly_at_task_creation_counts_as_proof", func(t *testing.T) {
		// The row note says "created at or AFTER" — equality must count.
		// Load-bearing on Windows, where back-to-back time.Now() calls can
		// land on the identical timestamp (repo footgun list).
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-EXACT-BOUNDARY", Status: "done", CreatedAt: t0},
		}
		worklinks := []FGWorklinkSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-EXACT-BOUNDARY", ArtifactType: "validation", CreatedAt: t0},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 0 {
			t.Fatalf("expected zero findings for a worklink created exactly at task creation time, got %+v", got.Findings)
		}
	})

	t.Run("worklink_created_before_task_creation_does_not_count_as_proof", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-STALE-PROOF", Status: "done", CreatedAt: t0},
		}
		worklinks := []FGWorklinkSnapshot{
			// A validation worklink that predates the task's own creation
			// cannot be proof of THIS completion (e.g. carried over from a
			// prior incarnation of the row's id).
			{TaskID: "SYNTH-TEST-ONLY-STALE-PROOF", ArtifactType: "validation", CreatedAt: t0.Add(-time.Hour)},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 1 {
			t.Fatalf("expected exactly one finding, got %d: %+v", len(got.Findings), got.Findings)
		}
	})

	t.Run("non_done_terminal_status_is_exempt_from_the_proof_check", func(t *testing.T) {
		// Only "done" is checked for proof (per the row note); "cancelled" is
		// terminal but is not a completion claim requiring proof of work.
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-CANCELLED", Status: "cancelled", CreatedAt: t0},
		}
		got, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Findings) != 0 {
			t.Fatalf("expected zero findings for a cancelled (non-done terminal) task, got %+v", got.Findings)
		}
	})

	t.Run("unrecognized_task_status_literal_fails_closed", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-BAD-STATUS", Status: "totally_unrecognized_literal", CreatedAt: t0},
		}
		_, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, nil)
		if !errors.Is(err, ErrUnknownTaskStatus) {
			t.Fatalf("expected ErrUnknownTaskStatus, got %v", err)
		}
	})

	t.Run("unrecognized_worklink_artifact_type_literal_fails_closed", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-BAD-KIND", Status: "done", CreatedAt: t0},
		}
		worklinks := []FGWorklinkSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-BAD-KIND", ArtifactType: "totally_unrecognized_literal", CreatedAt: t0.Add(time.Hour)},
		}
		_, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if !errors.Is(err, ErrUnknownArtifactKind) {
			t.Fatalf("expected ErrUnknownArtifactKind, got %v", err)
		}
	})

	t.Run("nil_registries_fail_closed_rather_than_panic", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		if _, err := RunFGTerminalProofCheck(ctx, nil, artifactReg, nil, nil); !errors.Is(err, ErrUnknownTaskStatus) {
			t.Fatalf("expected ErrUnknownTaskStatus for nil status registry, got %v", err)
		}
		if _, err := RunFGTerminalProofCheck(ctx, statusReg, nil, nil, nil); !errors.Is(err, ErrUnknownArtifactKind) {
			t.Fatalf("expected ErrUnknownArtifactKind for nil artifact registry, got %v", err)
		}
	})

	t.Run("acceptance_replay_identical_output_on_rerun", func(t *testing.T) {
		statusReg, artifactReg := newFGTestRegistries(t)
		tasks := []FGTaskSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-REPLAY-PARENT", Status: "done", CreatedAt: t0},
			{TaskID: "SYNTH-TEST-ONLY-REPLAY-CHILD", Status: "not_started", ParentID: "SYNTH-TEST-ONLY-REPLAY-PARENT", CreatedAt: t0},
		}
		worklinks := []FGWorklinkSnapshot{
			{TaskID: "SYNTH-TEST-ONLY-REPLAY-PARENT", ArtifactType: "dispatch", CreatedAt: t0.Add(time.Hour)},
		}
		first, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error on first run: %v", err)
		}
		second, err := RunFGTerminalProofCheck(ctx, statusReg, artifactReg, tasks, worklinks)
		if err != nil {
			t.Fatalf("unexpected error on second run: %v", err)
		}
		if len(first.Findings) != len(second.Findings) {
			t.Fatalf("replay mismatch: first has %d findings, second has %d", len(first.Findings), len(second.Findings))
		}
		for i := range first.Findings {
			if first.Findings[i] != second.Findings[i] {
				t.Fatalf("replay mismatch at index %d: first=%+v second=%+v", i, first.Findings[i], second.Findings[i])
			}
		}
		// Two independent findings expected: done_without_proof_kind (only
		// dispatch worklinks) AND terminal_parent_non_terminal_child.
		if len(first.Findings) != 2 {
			t.Fatalf("expected 2 findings (done_without_proof_kind + terminal_parent_non_terminal_child), got %d: %+v", len(first.Findings), first.Findings)
		}
	})
}
