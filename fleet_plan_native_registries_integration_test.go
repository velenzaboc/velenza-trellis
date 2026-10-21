//go:build integration

// P3-GT-FP-REGISTRIES DB-wired enforcement tests. Same convention as
// integration_test.go: Spanner-emulator-backed, excluded from the default
// `go test ./...`, run via `go test -tags=integration` (or `make
// test-integration`). registries_test.go covers the pure (no I/O) registry
// mechanics and the exhaustiveness fixture; this file covers the actual
// write-path wiring in fleet_plan_native.go — the part a pure unit test
// cannot reach because it runs inside a real Spanner ReadWriteTransaction.

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

// restrictedStatusEnv builds a nativeProjectEnv-shaped Env whose
// task_statuses graph is DELIBERATELY narrow (not_started -> in_progress
// only) rather than the fully-connected production graph, so an illegal
// transition actually exists to test against. Production's own graph is
// fully connected by deliberate design (see registries.go
// taskStatusTransitionLegal doc comment) — proving the MECHANISM correctly
// refuses an out-of-graph transition requires a graph that has one.
func restrictedStatusEnv(t *testing.T) *Env {
	t.Helper()
	env := nativeProjectEnv(t)
	reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "not_started", "legal_next": []interface{}{"in_progress"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "in_progress", "legal_next": []interface{}{"done"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done"}),
	})
	if err != nil {
		t.Fatalf("build restricted task_statuses registry: %v", err)
	}
	env.TaskStatuses = reg
	return env
}

func TestIntegrationNativeCreateUnknownStatusRejected(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	in := nativeTaskCreateInput("arbor", "BAD-STATUS-TASK")
	in.Payload["status"] = "this_is_not_a_registered_status"
	if _, err := handleCreate(ctx, env, in); err == nil || !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("create with unregistered status = %v, want ErrUnknownTaskStatus", err)
	}
	requireNativeTaskAbsent(t, ctx, env, "arbor", "BAD-STATUS-TASK")
}

func TestIntegrationNativeSetStatusUnknownRejected(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "SET-STATUS-UNKNOWN", "task", "not_started")
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "SET-STATUS-UNKNOWN", Status: "not_a_registered_status"}); err == nil || !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("set_status to unregistered value = %v, want ErrUnknownTaskStatus", err)
	}
	got, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: "SET-STATUS-UNKNOWN"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("get after rejected set_status: %v", err)
	}
	if got.CurrentVersion.Payload["status"] != "not_started" {
		t.Fatalf("status after rejected set_status = %v, want unchanged not_started", got.CurrentVersion.Payload["status"])
	}
}

func TestIntegrationNativeSetStatusIllegalTransitionRejected(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := restrictedStatusEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "ILLEGAL-TRANSITION", "task", "not_started")
	// not_started's legal_next is [in_progress] only — jumping straight to
	// "done" is not declared and must be refused.
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "ILLEGAL-TRANSITION", Status: "done"}); err == nil || !errors.Is(err, ErrIllegalStatusTransition) {
		t.Fatalf("not_started -> done on a restricted graph = %v, want ErrIllegalStatusTransition", err)
	}
	// The declared transition must still succeed.
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "ILLEGAL-TRANSITION", Status: "in_progress"}); err != nil {
		t.Fatalf("not_started -> in_progress (declared legal) should succeed: %v", err)
	}
	// Same-status is always legal regardless of the declared graph.
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "ILLEGAL-TRANSITION", Status: "in_progress"}); err != nil {
		t.Fatalf("same-status set_status (idempotent) should always succeed: %v", err)
	}
}

func TestIntegrationNativeAddWorklinkUnknownArtifactKindRejected(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "WORKLINK-BAD-KIND", "task", "not_started")
	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID: "arbor", TaskID: "WORKLINK-BAD-KIND", ArtifactType: "not_a_registered_artifact_kind", ArtifactRef: "ref-1",
	}); err == nil || !errors.Is(err, ErrUnknownArtifactKind) {
		t.Fatalf("add_worklink with unregistered artifact_type = %v, want ErrUnknownArtifactKind", err)
	}
	links, err := handleWorklinks(ctx, env, worklinksInput{ProjectID: "arbor", TaskID: "WORKLINK-BAD-KIND"})
	if err != nil {
		t.Fatalf("worklinks after rejected add: %v", err)
	}
	if len(links.Worklinks) != 0 {
		t.Fatalf("expected zero worklinks after rejected add, got %+v", links.Worklinks)
	}
}

func TestIntegrationNativeDependencySelfLoopRejected(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "SELF-DEP", "task", "not_started")
	if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "SELF-DEP", TargetID: "SELF-DEP", EdgeLabel: "gates"}); err == nil || !errors.Is(err, ErrDependencySelfLoop) {
		t.Fatalf("gates self-loop = %v, want ErrDependencySelfLoop", err)
	}
}

// TestIntegrationNativeDependencyCycleRejected is the direct regression
// test for the brief's premise: A gates B, B gates C, then C gates A would
// close a 3-cycle A->C->B->A in the "depends on" graph and must be refused
// rather than silently admitted (the defect this whole task exists to
// close — see gt-w3-fp-registries.md for the primary-source proof that NO
// such cycle exists in the live active graph today, and this test proves a
// NEW one cannot be introduced going forward).
func TestIntegrationNativeDependencyCycleRejected(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "CYC-A", "task", "not_started")
	createNativeTask(t, ctx, env, "arbor", "CYC-B", "task", "not_started")
	createNativeTask(t, ctx, env, "arbor", "CYC-C", "task", "not_started")

	if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "CYC-A", TargetID: "CYC-B", EdgeLabel: "gates"}); err != nil {
		t.Fatalf("A gates B: %v", err)
	}
	if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "CYC-B", TargetID: "CYC-C", EdgeLabel: "gates"}); err != nil {
		t.Fatalf("B gates C: %v", err)
	}
	if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "CYC-C", TargetID: "CYC-A", EdgeLabel: "gates"}); err == nil || !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("C gates A (closing the cycle) = %v, want ErrDependencyCycle", err)
	}

	// The graph must be unaffected by the refused insert: A and B still
	// have exactly the two legal edges, no third.
	aEdges, err := handleNeighbors(ctx, env, neighborsInput{ProjectID: "arbor", NodeID: "CYC-A", Direction: "out", EdgeLabels: []string{"gates"}}, &snapshotPin{})
	if err != nil {
		t.Fatalf("neighbors CYC-A: %v", err)
	}
	if len(aEdges.Edges) != 1 {
		t.Fatalf("CYC-A gates-out edges after refused cycle insert = %+v, want exactly 1 (A gates B)", aEdges.Edges)
	}
}

// TestIntegrationNativeDependencyKindConflictRejected seeds a raw
// non-gates TaskDeps row directly (source_map — no public verb creates one,
// matching how the live legacy rows in task-plan actually got there;
// see gt-w3-fp-registries.md reconciliation table) then proves a
// task_graph_link(gates) on the SAME ordered pair refuses rather than
// silently repurposing that row.
func TestIntegrationNativeDependencyKindConflictRejected(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "CONFLICT-A", "task", "not_started")
	createNativeTask(t, ctx, env, "arbor", "CONFLICT-B", "task", "not_started")

	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		t.Fatalf("native adapter: %v", err)
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	// Seed: CONFLICT-B depends_on CONFLICT-A under kind=source_map — the
	// same ordered pair task_graph_link(gates, source=A, target=B) will
	// target (which calls nativeInsertDep(taskID=B, dependsOnTaskID=A)).
	// nativeProjectSchema's TaskDeps.created_at carries no
	// allow_commit_timestamp option (unlike the real DDL), so a concrete
	// time.Now() value is used rather than spanner.CommitTimestamp.
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("TaskDeps", []string{"project_id", "dep_id", "task_id", "depends_on_task_id", "kind", "created_at"},
			[]interface{}{"arbor", "DEP-SEEDED-CONFLICT", "CONFLICT-B", "CONFLICT-A", "source_map", time.Now().UTC()}),
	})
	if err != nil {
		t.Fatalf("seed source_map dep row: %v", err)
	}

	if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "CONFLICT-A", TargetID: "CONFLICT-B", EdgeLabel: "gates"}); err == nil || !errors.Is(err, ErrDependencyKindConflict) {
		t.Fatalf("gates link over an existing active source_map row for the same pair = %v, want ErrDependencyKindConflict", err)
	}

	// The seeded row must be untouched — still source_map, not silently
	// flipped to gates.
	row, err := client.Single().ReadRow(ctx, "TaskDeps", spanner.Key{"arbor", "DEP-SEEDED-CONFLICT"}, []string{"kind"})
	if err != nil {
		t.Fatalf("read seeded dep row: %v", err)
	}
	var kind string
	if err := row.Columns(&kind); err != nil {
		t.Fatalf("decode kind: %v", err)
	}
	if kind != "source_map" {
		t.Fatalf("seeded dep row kind = %q after refused conflicting link, want unchanged source_map", kind)
	}
}

// ---------------- status-preserving updates on legacy-status rows ----------------

// absorbsStatusEnv extends nativeProjectEnv's fixture registry with the real
// config's absorbs reconciliation shape ("done" absorbs completed/resolved/…,
// "cancelled" absorbs invalid/…, "archived" absorbs audit_probe), so the DB-wired tests below
// can exercise rows carrying legacy literals the way the live task-plan
// table does.
func absorbsStatusEnv(t *testing.T) *Env {
	t.Helper()
	env := nativeProjectEnv(t)
	reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "not_started", "legal_next": []interface{}{"in_progress", "blocked", "in_review", "done", "cancelled", "archived"}, "absorbs": []interface{}{"not_started", "discovered", "pending", "gap"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "in_progress", "legal_next": []interface{}{"not_started", "blocked", "in_review", "done", "cancelled", "archived"}, "absorbs": []interface{}{"in_progress"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "blocked", "legal_next": []interface{}{"not_started", "in_progress", "in_review", "done", "cancelled", "archived"}, "absorbs": []interface{}{"blocked", "blocked_on_template"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "in_review", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "done", "cancelled", "archived"}, "absorbs": []interface{}{"in_review"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "in_review", "cancelled", "archived"}, "absorbs": []interface{}{"done", "completed", "complete", "resolved", "admitted_live"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "cancelled", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "in_review", "done", "archived"}, "absorbs": []interface{}{"cancelled", "invalid", "out_of_scope_current_goal", "superseded"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "archived", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "in_review", "done", "cancelled"}, "absorbs": []interface{}{"archived", "audit_probe"}}),
	})
	if err != nil {
		t.Fatalf("build absorbs task_statuses registry: %v", err)
	}
	env.TaskStatuses = reg
	return env
}

// seedLegacyStatusTask inserts a Tasks row DIRECTLY with a legacy status
// literal — handleCreate correctly refuses non-canonical statuses, and the
// live task-plan rows carrying them predate the registry, so a raw seed is
// exactly how such rows actually came to exist (same convention as
// TestIntegrationNativeDependencyKindConflictRejected's source_map seed).
func seedLegacyStatusTask(t *testing.T, ctx context.Context, env *Env, projectID, id, status, note string) {
	t.Helper()
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		t.Fatalf("native adapter: %v", err)
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	now := time.Now().UTC()
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("Tasks",
			[]string{"project_id", "id", "title", "pillar", "layer", "status", "owner", "is_gap", "note", "created_at", "updated_at"},
			[]interface{}{projectID, id, id, "P", "L1", status, "UNOWNED", false, note, now, now}),
	})
	if err != nil {
		t.Fatalf("seed legacy-status task %s/%s (%s): %v", projectID, id, status, err)
	}
}

func requireNativeTaskField(t *testing.T, ctx context.Context, env *Env, projectID, id, field string, want interface{}) {
	t.Helper()
	got, err := handleGet(ctx, env, getInput{ProjectID: projectID, NodeID: id}, &snapshotPin{})
	if err != nil {
		t.Fatalf("get %s/%s: %v", projectID, id, err)
	}
	if got.CurrentVersion.Payload[field] != want {
		t.Fatalf("task %s/%s %s = %v, want %v", projectID, id, field, got.CurrentVersion.Payload[field], want)
	}
}

// TestIntegrationNativeStatusPreservingUpdateOnLegacyStatusRow is the
// DB-wired regression test for the 2026-08-20 defect: a row whose stored
// status is a legacy literal ("audit_probe") must accept a get→update round
// trip that passes that status back UNCHANGED alongside a note edit — via
// BOTH task_graph_update and task_graph_set_status — without the caller
// being forced to corrupt the status to a canonical value.
func TestIntegrationNativeStatusPreservingUpdateOnLegacyStatusRow(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := absorbsStatusEnv(t)
	defer env.Substrates.Close()

	seedLegacyStatusTask(t, ctx, env, "arbor", "LEGACY-AUDIT-PROBE", "audit_probe", "original note")

	// task_graph_update: status passed back unchanged + note edit.
	if _, err := handleUpdate(ctx, env, updateInput{
		ProjectID: "arbor", NodeID: "LEGACY-AUDIT-PROBE",
		Payload: map[string]interface{}{"status": "audit_probe", "note": "note landed through the MCP path"},
	}); err != nil {
		t.Fatalf("status-preserving update on audit_probe row = %v, want success", err)
	}
	requireNativeTaskField(t, ctx, env, "arbor", "LEGACY-AUDIT-PROBE", "note", "note landed through the MCP path")
	requireNativeTaskField(t, ctx, env, "arbor", "LEGACY-AUDIT-PROBE", "status", "audit_probe")

	// task_graph_set_status: same legacy literal is a legal no-op.
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "LEGACY-AUDIT-PROBE", Status: "audit_probe"}); err != nil {
		t.Fatalf("same-status set_status on audit_probe row = %v, want success", err)
	}
	requireNativeTaskField(t, ctx, env, "arbor", "LEGACY-AUDIT-PROBE", "status", "audit_probe")
}

// TestIntegrationNativeLegacyStatusRowTransitionsViaCanonicalMember proves a
// legacy-status row is not merely note-editable but fully MUTABLE: its
// transition graph is evaluated from the canonical member that absorbs its
// stored literal ("completed" -> done -> in_progress is legal), and the
// write lands the canonical target.
func TestIntegrationNativeLegacyStatusRowTransitionsViaCanonicalMember(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := absorbsStatusEnv(t)
	defer env.Substrates.Close()

	seedLegacyStatusTask(t, ctx, env, "arbor", "LEGACY-COMPLETED", "completed", "")
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "LEGACY-COMPLETED", Status: "in_progress"}); err != nil {
		t.Fatalf("completed (absorbed by done) -> in_progress = %v, want success", err)
	}
	requireNativeTaskField(t, ctx, env, "arbor", "LEGACY-COMPLETED", "status", "in_progress")
}

// TestIntegrationNativeStatusChangeStillFailsClosed proves the registry did
// NOT re-open: a status CHANGE to an absorbed legacy literal or to a novel
// literal is refused, and the row keeps its stored value.
func TestIntegrationNativeStatusChangeStillFailsClosed(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := absorbsStatusEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "FAIL-CLOSED", "task", "not_started")
	// Change TO an absorbed legacy literal: refused (writes stay canonical).
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "FAIL-CLOSED", Status: "completed"}); err == nil || !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("change to absorbed literal completed = %v, want ErrUnknownTaskStatus", err)
	}
	// Change TO a genuinely novel literal: refused.
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "FAIL-CLOSED", Status: "definitely_novel_status"}); err == nil || !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("change to novel literal = %v, want ErrUnknownTaskStatus", err)
	}
	requireNativeTaskField(t, ctx, env, "arbor", "FAIL-CLOSED", "status", "not_started")

	// Same on a legacy-status row: novel CHANGE refused even though the
	// row's own literal round-trips.
	seedLegacyStatusTask(t, ctx, env, "arbor", "LEGACY-FAIL-CLOSED", "resolved", "")
	if _, err := handleSetStatus(ctx, env, setStatusInput{ProjectID: "arbor", TaskID: "LEGACY-FAIL-CLOSED", Status: "definitely_novel_status"}); err == nil || !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("novel change on legacy row = %v, want ErrUnknownTaskStatus", err)
	}
	requireNativeTaskField(t, ctx, env, "arbor", "LEGACY-FAIL-CLOSED", "status", "resolved")
}

// TestIntegrationNativeStatusMustBeNonEmptyString closes the fail-open hole
// the old gate carried: a status key holding "" (or a non-string) used to
// skip validation entirely and write through to the NOT NULL column.
func TestIntegrationNativeStatusMustBeNonEmptyString(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := absorbsStatusEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "EMPTY-STATUS", "task", "not_started")
	if _, err := handleUpdate(ctx, env, updateInput{
		ProjectID: "arbor", NodeID: "EMPTY-STATUS",
		Payload: map[string]interface{}{"status": ""},
	}); err == nil {
		t.Fatal("update with status \"\" succeeded, want refusal")
	}
	if _, err := handleUpdate(ctx, env, updateInput{
		ProjectID: "arbor", NodeID: "EMPTY-STATUS",
		Payload: map[string]interface{}{"status": 42},
	}); err == nil {
		t.Fatal("update with non-string status succeeded, want refusal")
	}
	requireNativeTaskField(t, ctx, env, "arbor", "EMPTY-STATUS", "status", "not_started")
}
