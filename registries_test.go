package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"
)

// registries_test.go is the unit-level (no Spanner emulator) test suite for
// P3-GT-FP-REGISTRIES's three closed-set registries: TaskStatusRegistry
// (Tasks.status), DependencyKindRegistry (TaskDeps.kind), ArtifactKindRegistry
// (TaskLinks.artifact_type). Every function under test here is pure — no I/O
// — so this file runs in the default `go test ./...` (no -tags=integration,
// no emulator). DB-wired enforcement (nativeInsertDep, nativeUpdateTaskFields,
// nativeHandleAddWorklink) is covered separately in
// fleet_plan_native_registries_integration_test.go (tagged integration).

// ---------------- TaskStatusDescriptor.Validate ----------------

func TestTaskStatusDescriptorValidate(t *testing.T) {
	tests := []struct {
		name    string
		raw     map[string]interface{}
		wantErr bool
	}{
		{"valid minimal", map[string]interface{}{"name": "done"}, false},
		{"missing name", map[string]interface{}{}, true},
		{"empty name", map[string]interface{}{"name": ""}, true},
		{"legal_next wrong type", map[string]interface{}{"name": "done", "legal_next": "not-an-array"}, true},
		{"absorbs wrong type", map[string]interface{}{"name": "done", "absorbs": "not-an-array"}, true},
		{"valid full", map[string]interface{}{"name": "done", "terminal": true, "legal_next": []interface{}{"archived"}, "absorbs": []interface{}{"done", "completed"}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewTaskStatusDescriptor(tc.raw).Validate(0)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// ---------------- DependencyKindDescriptor.Validate ----------------

func TestDependencyKindDescriptorValidate(t *testing.T) {
	tests := []struct {
		name    string
		raw     map[string]interface{}
		wantErr bool
	}{
		{"valid minimal", map[string]interface{}{"name": "gates", "edge_family": "dep"}, false},
		{"missing name", map[string]interface{}{"edge_family": "dep"}, true},
		{"missing edge_family", map[string]interface{}{"name": "gates"}, true},
		{"legal_next wrong type", map[string]interface{}{"name": "gates", "edge_family": "dep", "legal_next": 5}, true},
		{"absorbs wrong type", map[string]interface{}{"name": "gates", "edge_family": "dep", "absorbs": 5}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewDependencyKindDescriptor(tc.raw).Validate(0)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// ---------------- ArtifactKindDescriptor.Validate ----------------

func TestArtifactKindDescriptorValidate(t *testing.T) {
	tests := []struct {
		name    string
		raw     map[string]interface{}
		wantErr bool
	}{
		{"valid minimal", map[string]interface{}{"name": "pr"}, false},
		{"missing name", map[string]interface{}{}, true},
		{"name too long for STRING(32)", map[string]interface{}{"name": "this_canonical_name_is_definitely_too_long_for_the_column"}, true},
		{"absorbs wrong type", map[string]interface{}{"name": "pr", "absorbs": 5}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewArtifactKindDescriptor(tc.raw).Validate(0)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// ---------------- registry construction: duplicates and double-claims ----------------

func TestNewTaskStatusRegistryRejectsDuplicateName(t *testing.T) {
	_, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done"}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done"}),
	})
	if err == nil {
		t.Fatal("expected error for duplicate member name, got nil")
	}
}

func TestNewTaskStatusRegistryRejectsDoubleClaimedLiteral(t *testing.T) {
	// "completed" absorbed by both "done" and "resolved" — a legacy literal
	// must resolve to exactly one canonical member.
	_, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "absorbs": []interface{}{"completed"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "resolved", "absorbs": []interface{}{"completed"}}),
	})
	if err == nil {
		t.Fatal("expected error for a literal double-claimed by two members' absorbs lists, got nil")
	}
}

func TestNewTaskStatusRegistryRejectsLiteralCollidingWithAnotherMembersName(t *testing.T) {
	// "blocked" is itself a canonical member name AND is claimed by
	// "not_started"'s absorbs list — must refuse, not silently let the
	// absorbs claim shadow the canonical member.
	_, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "blocked"}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "not_started", "absorbs": []interface{}{"blocked"}}),
	})
	if err == nil {
		t.Fatal("expected error for a literal colliding with another member's own canonical name, got nil")
	}
}

func TestNewTaskStatusRegistrySelfAbsorptionIsHarmless(t *testing.T) {
	// A canonical name appearing inside its OWN absorbs list (the real
	// config does this throughout, e.g. "pr" absorbs "pr") must construct
	// cleanly — same claimant, not a conflict.
	reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "absorbs": []interface{}{"done", "completed"}}),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := reg.Resolve(context.Background(), "done"); err != nil {
		t.Fatalf("Resolve(done): %v", err)
	}
}

func TestNewDependencyKindRegistryRejectsDoubleClaimedLiteral(t *testing.T) {
	_, err := newDependencyKindRegistry([]DependencyKindDescriptor{
		NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep", "absorbs": []interface{}{"blocks"}}),
		NewDependencyKindDescriptor(map[string]interface{}{"name": "source_map", "edge_family": "ref", "absorbs": []interface{}{"blocks"}}),
	})
	if err == nil {
		t.Fatal("expected error for a literal double-claimed across dependency_kinds members, got nil")
	}
}

func TestNewArtifactKindRegistryRejectsDoubleClaimedLiteral(t *testing.T) {
	_, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "pr", "absorbs": []interface{}{"github_pr"}}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "review", "absorbs": []interface{}{"github_pr"}}),
	})
	if err == nil {
		t.Fatal("expected error for a literal double-claimed across artifact_kinds members, got nil")
	}
}

// ---------------- Resolve (strict, canonical-only) vs Canonicalize (reconciling) ----------------

func TestTaskStatusRegistryResolveRejectsLegacyLiteral(t *testing.T) {
	reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "absorbs": []interface{}{"done", "completed"}}),
	})
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}
	// Resolve is the WRITE gate — a legacy literal that Canonicalize
	// reconciles for reads must NOT be independently write-able; only the
	// canonical name itself resolves. This is the load-bearing property that
	// keeps the registry actually closed going forward.
	if _, err := reg.Resolve(context.Background(), "completed"); !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("Resolve(completed) = %v, want ErrUnknownTaskStatus (absorbs is read-side only)", err)
	}
	if _, err := reg.Resolve(context.Background(), "done"); err != nil {
		t.Fatalf("Resolve(done): %v", err)
	}
}

func TestTaskStatusRegistryCanonicalize(t *testing.T) {
	reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "absorbs": []interface{}{"done", "completed", "resolved"}}),
	})
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}
	for _, literal := range []string{"done", "completed", "resolved"} {
		got, found := reg.Canonicalize(context.Background(), literal)
		if !found || got != "done" {
			t.Errorf("Canonicalize(%q) = (%q, %v), want (done, true)", literal, got, found)
		}
	}
	if _, found := reg.Canonicalize(context.Background(), "totally_unrecognized_literal"); found {
		t.Error("Canonicalize(unrecognized) found=true, want false")
	}
}

func TestDependencyKindRegistryResolveAndCanonicalize(t *testing.T) {
	reg, err := newDependencyKindRegistry([]DependencyKindDescriptor{
		NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep", "blocking": true, "absorbs": []interface{}{"gates", "blocks", "hard"}}),
		NewDependencyKindDescriptor(map[string]interface{}{"name": "archived", "edge_family": "dep", "lifecycle_marker": true, "absorbs": []interface{}{"archived"}}),
	})
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}
	if _, err := reg.Resolve(context.Background(), "blocks"); !errors.Is(err, ErrUnknownDependencyKind) {
		t.Fatalf("Resolve(blocks) = %v, want ErrUnknownDependencyKind", err)
	}
	got, found := reg.Canonicalize(context.Background(), "hard")
	if !found || got != "gates" {
		t.Fatalf("Canonicalize(hard) = (%q, %v), want (gates, true)", got, found)
	}
	gatesDesc, err := reg.Resolve(context.Background(), "gates")
	if err != nil {
		t.Fatalf("Resolve(gates): %v", err)
	}
	if !gatesDesc.Blocking() {
		t.Error("gates.Blocking() = false, want true")
	}
	archivedDesc, err := reg.Resolve(context.Background(), "archived")
	if err != nil {
		t.Fatalf("Resolve(archived): %v", err)
	}
	if !archivedDesc.LifecycleMarker() {
		t.Error("archived.LifecycleMarker() = false, want true")
	}
}

func TestArtifactKindRegistryResolveAndCanonicalize(t *testing.T) {
	reg, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "pr", "absorbs": []interface{}{"pr", "github_pr", "pull_request"}}),
	})
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}
	if _, err := reg.Resolve(context.Background(), "github_pr"); !errors.Is(err, ErrUnknownArtifactKind) {
		t.Fatalf("Resolve(github_pr) = %v, want ErrUnknownArtifactKind", err)
	}
	got, found := reg.Canonicalize(context.Background(), "pull_request")
	if !found || got != "pr" {
		t.Fatalf("Canonicalize(pull_request) = (%q, %v), want (pr, true)", got, found)
	}
}

// ---------------- transition legality ----------------

func TestTaskStatusTransitionLegal(t *testing.T) {
	restricted := NewTaskStatusDescriptor(map[string]interface{}{
		"name": "not_started", "legal_next": []interface{}{"in_progress"},
	})
	if !taskStatusTransitionLegal(restricted, "not_started") {
		t.Error("same-status transition must always be legal (idempotent set_status)")
	}
	if !taskStatusTransitionLegal(restricted, "in_progress") {
		t.Error("in_progress is in legal_next, must be legal")
	}
	if taskStatusTransitionLegal(restricted, "done") {
		t.Error("done is NOT in legal_next and is a different status, must be illegal")
	}
}

func TestDependencyKindTransitionLegal(t *testing.T) {
	gates := NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep", "legal_next": []interface{}{"archived"}})
	sourceMap := NewDependencyKindDescriptor(map[string]interface{}{"name": "source_map", "edge_family": "ref", "legal_next": []interface{}{"archived"}})
	archived := NewDependencyKindDescriptor(map[string]interface{}{"name": "archived", "edge_family": "dep", "lifecycle_marker": true, "legal_next": []interface{}{"gates", "source_map"}})

	if !dependencyKindTransitionLegal(gates, gates) {
		t.Error("same-kind transition must always be legal")
	}
	if !dependencyKindTransitionLegal(gates, archived) {
		t.Error("gates -> archived is declared legal_next, must be legal")
	}
	if !dependencyKindTransitionLegal(archived, gates) {
		t.Error("archived -> gates is declared legal_next AND archived is the lifecycle marker leaving it, must be legal")
	}
	// The real defect this proves: gates -> source_map directly (skipping
	// archived) is NOT declared anywhere and must be refused — a dependency
	// kind change goes through the archive/reactivate cycle, never a direct
	// same-row kind swap.
	if dependencyKindTransitionLegal(gates, sourceMap) {
		t.Error("gates -> source_map directly is not a declared transition, must be illegal")
	}
	// Transitioning INTO archived (the lifecycle marker) is always legal
	// from ANY current kind, even one whose legal_next doesn't mention it —
	// matches nativeHandleUnlink's existing archive-from-any-kind behavior.
	noArchiveDeclared := NewDependencyKindDescriptor(map[string]interface{}{"name": "custom", "edge_family": "dep", "legal_next": []interface{}{}})
	if !dependencyKindTransitionLegal(noArchiveDeclared, archived) {
		t.Error("transitioning into the lifecycle-marker kind must always be legal regardless of the source kind's own legal_next")
	}
}

// ---------------- wouldCreateDependencyCycle ----------------

func TestWouldCreateDependencyCycleNoExistingEdges(t *testing.T) {
	would, _ := wouldCreateDependencyCycle(nil, "A", "B")
	if would {
		t.Error("empty graph, A depends on B: no cycle possible")
	}
}

func TestWouldCreateDependencyCycleDirectTwoCycle(t *testing.T) {
	// A already depends on B (A -> B). Proposing B depends on A (B -> A)
	// closes a direct 2-cycle — the exact shape the brief's (refuted) claim
	// described.
	existing := []depEdge{{TaskID: "A", DependsOnTaskID: "B"}}
	would, cycle := wouldCreateDependencyCycle(existing, "B", "A")
	if !would {
		t.Fatal("expected a 2-cycle to be detected")
	}
	if len(cycle) == 0 {
		t.Error("expected a non-empty cycle path")
	}
}

func TestWouldCreateDependencyCycleTransitiveChain(t *testing.T) {
	// A depends on B, B depends on C (chain). Proposing C depends on A
	// closes a 3-cycle: A -> B -> C -> A.
	existing := []depEdge{
		{TaskID: "A", DependsOnTaskID: "B"},
		{TaskID: "B", DependsOnTaskID: "C"},
	}
	would, cycle := wouldCreateDependencyCycle(existing, "C", "A")
	if !would {
		t.Fatal("expected a transitive 3-cycle to be detected")
	}
	if len(cycle) < 3 {
		t.Errorf("expected cycle path of at least 3 nodes, got %v", cycle)
	}
}

func TestWouldCreateDependencyCycleSelfLoop(t *testing.T) {
	would, _ := wouldCreateDependencyCycle(nil, "A", "A")
	if !would {
		t.Error("A depends on A is trivially a cycle")
	}
}

func TestWouldCreateDependencyCycleAcyclicGraphNoFalsePositive(t *testing.T) {
	// A diamond: D depends on B and C, both depend on E. Adding a fresh leaf
	// F depending on D must NOT be flagged.
	existing := []depEdge{
		{TaskID: "D", DependsOnTaskID: "B"},
		{TaskID: "D", DependsOnTaskID: "C"},
		{TaskID: "B", DependsOnTaskID: "E"},
		{TaskID: "C", DependsOnTaskID: "E"},
	}
	would, cycle := wouldCreateDependencyCycle(existing, "F", "D")
	if would {
		t.Errorf("expected no cycle in a diamond + fresh leaf, got cycle %v", cycle)
	}
}

// TestWouldCreateDependencyCycleTerminatesOnCorruptCyclicInput is the
// defensive read-path regression: the function must terminate (not
// infinite-loop) even if the INPUT edge set already contains a cycle —
// verified live-data has zero such cycles as of this task (see
// gt-w3-fp-registries.md), but the checker must not assume that holds
// forever. Go's own test runner enforces a wall-clock timeout, so a hang
// here fails the test suite rather than the assertions below; this test's
// real job is to run at all and land on a defined answer.
func TestWouldCreateDependencyCycleTerminatesOnCorruptCyclicInput(t *testing.T) {
	corrupt := []depEdge{
		{TaskID: "X", DependsOnTaskID: "Y"},
		{TaskID: "Y", DependsOnTaskID: "X"}, // pre-existing cycle in the input itself
	}
	would, _ := wouldCreateDependencyCycle(corrupt, "Z", "X")
	if would {
		t.Error("Z -> X does not by itself close a new cycle through Z; the pre-existing X<->Y cycle is a separate, already-committed defect this insert doesn't create")
	}
}

// ---------------- exhaustiveness: measured live values vs task-graph.config.json ----------------

// measuredLiveTaskStatuses is the frozen result of:
//
//	SELECT status, COUNT(*) n FROM Tasks GROUP BY status ORDER BY n DESC
//
// against task-plan (example-gcp-project / example-spanner-instance),
// measured 2026-08-16 as part of P3-GT-FP-REGISTRIES. 19 distinct values,
// 1901 total rows. See gt-w3-fp-registries.md for the full query output.
var measuredLiveTaskStatuses = []string{
	"done", "blocked", "not_started", "in_progress", "completed",
	"out_of_scope_current_goal", "invalid", "archived", "in_review",
	"discovered", "admitted_live", "resolved", "complete", "gap",
	"superseded", "pending", "audit_probe", "blocked_on_template", "cancelled",
}

// measuredLiveTaskStatusesAppearingAfterClosure is the frozen result of
// re-running the same query on 2026-09-05:
//
//	SELECT status, COUNT(*) n, MIN(created_at) FROM Tasks GROUP BY status
//
// Two literals present then were absent from the 2026-08-16 measurement
// above, and both were first CREATED after it — "planned" on 2026-08-24
// (5,827 rows) and "corrected_unmerged" on 2026-08-25 (4 rows). They came
// from writers that predate these registries and therefore never consulted
// them: the July Cloud Run revision and the workstation-c local daemon.
//
// This is the case the reconciliation table exists to absorb, and leaving it
// unabsorbed was not cosmetic. checkTaskStatusWrite Canonicalize's the
// CURRENT literal before checking the transition graph, so an unabsorbed
// current status refuses EVERY status change on that row: all 5,831 rows
// were frozen until these entries landed. The 2026-08-16 list is kept
// verbatim as the historical measurement rather than restated.
var measuredLiveTaskStatusesAppearingAfterClosure = []string{
	"planned", "corrected_unmerged",
}

// measuredLiveTaskStatusesAfterNormalization is the frozen result of
//
//	SELECT status, COUNT(*) n FROM Tasks GROUP BY status ORDER BY n DESC
//
// measured 2026-09-14, after MVP-GRAPH-STATUS-NORMALIZATION migrated every
// legacy literal through the registry-checked write path (census worklink
// LNK-b5ea4bd0). 7 distinct values, all canonical members: not_started 31002,
// done 1748, blocked 1076, cancelled 403, archived 334, in_progress 261,
// in_review 12. With zero non-canonical rows left, every task_statuses absorbs
// list shrank to its own member name. The two measurements above stay verbatim
// as the history those absorbs lists reconciled.
var measuredLiveTaskStatusesAfterNormalization = []string{
	"not_started", "done", "blocked", "cancelled", "archived", "in_progress", "in_review",
}

// measuredLiveDependencyKinds is the frozen result of:
//
//	SELECT kind, COUNT(*) n FROM TaskDeps GROUP BY kind ORDER BY n DESC
//
// measured 2026-08-16. 6 distinct values, 1049 total rows.
var measuredLiveDependencyKinds = []string{
	"gates", "archived", "source_map", "blocks", "blocks_until_done", "hard",
}

// measuredLiveDependencyKindsAppearingAfterClosure is the frozen result of
// re-running the same query on 2026-09-05. Seven literals were present that
// the 2026-08-16 measurement did not see, all first created 2026-08-24..26:
//
//	gated_by 375, finding_gate 77, requires 15, closure 6,
//	validation_gate 5, atomic_finding 3, composition_gate 3
//
// Unlike an unabsorbed task status, which fails CLOSED, an unabsorbed
// dependency kind fails OPEN twice over. nativeActiveDependencyEdges skips
// any kind Canonicalize cannot place (`continue`), so those 484 edges were
// invisible to the acyclicity check; and the duplicate-edge branch treats a
// not-found existing kind as no conflict, returning a silent no-op instead
// of ErrDependencyKindConflict.
//
// Direction, resolved from the code rather than the label: line 1309 calls
// nativeDependencyID(projectID, dependsOnTaskID, taskID), so gatesSource is
// depends_on_task_id and gatesTarget is task_id — the blocker is
// depends_on_task_id. The gated_by rows carry that same column convention,
// not the inverse: joining TaskDeps to itself finds ZERO gates/gated_by
// reciprocal pairs and ZERO same-direction duplicates, so they are a
// disjoint relabelling, not a second stored direction.
//
// The five gate-shaped literals are therefore absorbed onto gates
// (blocking), and closure/atomic_finding onto source_map (non-blocking),
// which preserves their current effective treatment — they are skipped from
// the blocking set today — while making them visible to the conflict check.
//
// Cycle safety was measured before making them visible, not assumed: a DFS
// over the exported edge set finds 0 cycles with kind=gates alone (52,851
// edges / 23,271 nodes) and 0 with all five gate-shaped kinds included
// (53,326 / 23,480). The detector is positive-controlled — injecting one
// synthetic 2-cycle makes it report exactly that cycle.
var measuredLiveDependencyKindsAppearingAfterClosure = []string{
	"gated_by", "finding_gate", "validation_gate", "composition_gate",
	"requires", "closure", "atomic_finding",
}

// measuredLiveArtifactTypes is the frozen result of:
//
//	SELECT artifact_type, COUNT(*) n FROM TaskLinks GROUP BY artifact_type
//
// measured 2026-08-16. 389 distinct values, 6398 total rows. Loaded from a
// fixture file (testdata/measured_artifact_types.txt, one literal per line)
// rather than inlined here — 389 lines of prose-adjacent literal data would
// itself be an R6 no-prose / atomic-single-clause violation on a Go source
// file; a plain-text fixture is the natural home for a flat literal list.
func loadMeasuredLiveArtifactTypes(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("testdata/measured_artifact_types.txt")
	if err != nil {
		t.Fatalf("read testdata/measured_artifact_types.txt: %v", err)
	}
	var out []string
	for _, line := range splitNonEmptyLines(string(data)) {
		out = append(out, line)
	}
	if len(out) != 389 {
		t.Fatalf("testdata/measured_artifact_types.txt has %d entries, want 389 (the measured live distinct artifact_type count)", len(out))
	}
	return out
}

func splitNonEmptyLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			if line != "" {
				out = append(out, line)
			}
			start = i + 1
		}
	}
	return out
}

// loadRealTaskGraphConfig reads and parses the actual task-graph.config.json
// shipped in this repo (not a test fixture) — the exhaustiveness property
// this test proves is about the REAL reconciliation table a deployment
// actually boots with, not a copy of it.
func loadRealTaskGraphConfig(t *testing.T) SpecGraphConfig {
	t.Helper()
	data, err := os.ReadFile("task-graph.config.json")
	if err != nil {
		t.Fatalf("read task-graph.config.json: %v", err)
	}
	var cfg SpecGraphConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse task-graph.config.json: %v", err)
	}
	return cfg
}

// TestTaskStatusesExhaustiveAgainstMeasuredLiveValues is the falsifiable
// reconciliation proof for Tasks.status against the current live vocabulary:
// every literal in the 2026-09-14 census must canonicalize to itself as one of
// the 7 registry members, and every historical legacy literal that is not a
// member name must no longer canonicalize, because the migration that made the
// census canonical-only also retired those absorbs entries.
func TestTaskStatusesExhaustiveAgainstMeasuredLiveValues(t *testing.T) {
	cfg := loadRealTaskGraphConfig(t)
	if len(cfg.TaskStatuses) == 0 {
		t.Fatal("task-graph.config.json carries no task_statuses — registry not closed")
	}
	reg, err := newTaskStatusRegistry(cfg.TaskStatuses)
	if err != nil {
		t.Fatalf("build task_statuses registry from real config: %v", err)
	}
	seenCanonical := map[string]bool{}
	for _, literal := range measuredLiveTaskStatusesAfterNormalization {
		canonical, found := reg.Canonicalize(context.Background(), literal)
		if !found {
			t.Errorf("measured live status %q does not canonicalize to any task_statuses member", literal)
			continue
		}
		if canonical != literal {
			t.Errorf("measured live status %q canonicalized to %q; a canonical-only census must map each literal onto itself", literal, canonical)
		}
		if _, err := reg.Resolve(context.Background(), canonical); err != nil {
			t.Errorf("status %q canonicalized to %q, which does not itself Resolve: %v", literal, canonical, err)
		}
		seenCanonical[canonical] = true
	}
	members := map[string]bool{}
	for _, literal := range measuredLiveTaskStatusesAfterNormalization {
		members[literal] = true
	}
	for _, literal := range append(append([]string{}, measuredLiveTaskStatuses...), measuredLiveTaskStatusesAppearingAfterClosure...) {
		if members[literal] {
			continue
		}
		if canonical, found := reg.Canonicalize(context.Background(), literal); found {
			t.Errorf("retired legacy status %q still canonicalizes to %q; absorbs lists must hold only their own member name", literal, canonical)
		}
	}
	all, _ := reg.All(context.Background())
	var names []string
	for _, d := range all {
		names = append(names, d.Name())
	}
	sort.Strings(names)
	if len(names) != 7 {
		t.Errorf("task_statuses has %d canonical members, want 7 (%v)", len(names), names)
	}
	for _, n := range names {
		if !seenCanonical[n] {
			t.Errorf("canonical status %q claims zero measured live literals — every canonical member should have live evidence", n)
		}
	}
}

func TestDependencyKindsExhaustiveAgainstMeasuredLiveValues(t *testing.T) {
	cfg := loadRealTaskGraphConfig(t)
	if len(cfg.DependencyKinds) == 0 {
		t.Fatal("task-graph.config.json carries no dependency_kinds — registry not closed")
	}
	reg, err := newDependencyKindRegistry(cfg.DependencyKinds)
	if err != nil {
		t.Fatalf("build dependency_kinds registry from real config: %v", err)
	}
	for _, literal := range append(append([]string{}, measuredLiveDependencyKinds...), measuredLiveDependencyKindsAppearingAfterClosure...) {
		canonical, found := reg.Canonicalize(context.Background(), literal)
		if !found {
			t.Errorf("measured live dependency kind %q does not canonicalize to any dependency_kinds member", literal)
			continue
		}
		if _, err := reg.Resolve(context.Background(), canonical); err != nil {
			t.Errorf("kind %q canonicalized to %q, which does not itself Resolve: %v", literal, canonical, err)
		}
	}
	all, _ := reg.All(context.Background())
	if len(all) != 3 {
		t.Errorf("dependency_kinds has %d canonical members, want 3", len(all))
	}
}

// TestArtifactKindsExhaustiveAgainstMeasuredLiveValues is the big one: every
// one of the 389 measured live TaskLinks.artifact_type literals must
// canonicalize to exactly one of the closed artifact_kinds members, with
// zero left unclaimed and zero double-claimed (double-claim is already
// refused at construction — see newArtifactKindRegistry — so a successful
// build here already proves that half; this test proves the other half:
// nothing measured live was left OUT of every member's absorbs list).
func TestArtifactKindsExhaustiveAgainstMeasuredLiveValues(t *testing.T) {
	cfg := loadRealTaskGraphConfig(t)
	if len(cfg.ArtifactKinds) == 0 {
		t.Fatal("task-graph.config.json carries no artifact_kinds — registry not closed")
	}
	reg, err := newArtifactKindRegistry(cfg.ArtifactKinds)
	if err != nil {
		t.Fatalf("build artifact_kinds registry from real config: %v", err)
	}
	measured := loadMeasuredLiveArtifactTypes(t)
	var missing []string
	for _, literal := range measured {
		canonical, found := reg.Canonicalize(context.Background(), literal)
		if !found {
			missing = append(missing, literal)
			continue
		}
		if _, err := reg.Resolve(context.Background(), canonical); err != nil {
			t.Errorf("artifact_type %q canonicalized to %q, which does not itself Resolve: %v", literal, canonical, err)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%d measured live artifact_type literal(s) do not canonicalize to any artifact_kinds member: %v", len(missing), missing)
	}
	all, _ := reg.All(context.Background())
	if len(all) != 27 {
		t.Errorf("artifact_kinds has %d canonical members, want 27", len(all))
	}
}

// ---------------- checkTaskStatusWrite: status-preserving round trips ----------------

// checkStatusWriteFixtureRegistry builds a small registry with absorbs and a
// DELIBERATELY restricted transition graph (done may only reopen to
// in_progress) so both an illegal transition and a legal one exist to test.
// "resolved" and "completed" are absorbed legacy literals of "done",
// mirroring the real config's reconciliation shape.
func checkStatusWriteFixtureRegistry(t *testing.T) TaskStatusRegistry {
	t.Helper()
	reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "not_started", "legal_next": []interface{}{"in_progress"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "in_progress", "legal_next": []interface{}{"done"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "legal_next": []interface{}{"in_progress"}, "absorbs": []interface{}{"done", "completed", "resolved"}}),
	})
	if err != nil {
		t.Fatalf("build fixture registry: %v", err)
	}
	return reg
}

// TestCheckTaskStatusWriteMatrix is the unit-level matrix for the 2026-08-20
// defect fix: a status-preserving write (incoming == stored) NEVER fails —
// legacy literal, canonical member, or even a literal the registry has never
// heard of — while a status CHANGE still fails closed unless it names a
// canonical member and the transition graph (evaluated from the stored
// literal's canonical member) allows it.
func TestCheckTaskStatusWriteMatrix(t *testing.T) {
	reg := checkStatusWriteFixtureRegistry(t)
	ctx := context.Background()
	tests := []struct {
		name    string
		current string
		next    string
		wantErr error // nil = must succeed
	}{
		{"same canonical", "done", "done", nil},
		{"same legacy literal (resolved round trip)", "resolved", "resolved", nil},
		{"same legacy literal (completed round trip)", "completed", "completed", nil},
		{"same literal even when registry does not know it", "totally_unregistered", "totally_unregistered", nil},
		{"legacy current, legal canonical change", "resolved", "in_progress", nil},
		{"legacy current, illegal canonical change", "completed", "not_started", ErrIllegalStatusTransition},
		{"canonical current, illegal canonical change", "not_started", "done", ErrIllegalStatusTransition},
		{"change to absorbed literal refused", "done", "completed", ErrUnknownTaskStatus},
		{"change to novel literal refused", "in_progress", "definitely_not_a_status", ErrUnknownTaskStatus},
		{"unclaimed current literal refuses a change", "totally_unregistered", "in_progress", ErrUnknownTaskStatus},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTaskStatusWrite(ctx, reg, "T-1", tc.current, tc.next)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("checkTaskStatusWrite(current=%q, next=%q) = %v, want nil", tc.current, tc.next, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("checkTaskStatusWrite(current=%q, next=%q) = %v, want %v", tc.current, tc.next, err, tc.wantErr)
			}
		})
	}
}

// TestCheckTaskStatusWriteAbsorbedLiteralErrorNamesCanonicalMember pins the
// guidance in the refusal: writing "completed" as a CHANGE target must tell
// the caller the canonical member ("done") to write instead.
func TestCheckTaskStatusWriteAbsorbedLiteralErrorNamesCanonicalMember(t *testing.T) {
	reg := checkStatusWriteFixtureRegistry(t)
	err := checkTaskStatusWrite(context.Background(), reg, "T-1", "in_progress", "completed")
	if !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("change to absorbed literal = %v, want ErrUnknownTaskStatus", err)
	}
	if !strings.Contains(err.Error(), `"done"`) {
		t.Fatalf("refusal message %q does not name the canonical member done", err.Error())
	}
}

// TestCheckTaskStatusWriteStatusPreservingAgainstRealConfigLiveVocabulary is
// the falsifiable proof, against every SHIPPED config, that every literal in
// the live Tasks.status census round-trips: a get→update that passes the row's
// own status back unchanged succeeds, AND a status change from that row to a
// canonical member is accepted — i.e. NO live row is frozen by its status.
// It began as the regression test for the 2026-08-20 defect
// (task_graph_update/set_status rejected "resolved"/"completed"/"audit_probe"/
// "discovered" even passed back unchanged); since the 2026-09-14 census holds
// only canonical members, it runs over that census.
func TestCheckTaskStatusWriteStatusPreservingAgainstRealConfigLiveVocabulary(t *testing.T) {
	for _, path := range []string{
		"task-graph.config.json",
		"task-graph.post-cutover.config.json",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var cfg SpecGraphConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			reg, err := newTaskStatusRegistry(cfg.TaskStatuses)
			if err != nil {
				t.Fatalf("build task_statuses registry from %s: %v", path, err)
			}
			ctx := context.Background()
			for _, literal := range measuredLiveTaskStatusesAfterNormalization {
				if err := checkTaskStatusWrite(ctx, reg, "T-LIVE", literal, literal); err != nil {
					t.Errorf("status-preserving round trip of live literal %q failed: %v", literal, err)
				}
				canonical, found := reg.Canonicalize(ctx, literal)
				if !found {
					t.Errorf("live literal %q is not claimed by any member — config no longer covers the live vocabulary", literal)
					continue
				}
				// The production graph is fully connected, so a change to
				// any OTHER canonical member must be legal from every live
				// literal — pick one deterministically.
				target := "in_progress"
				if canonical == "in_progress" {
					target = "done"
				}
				if err := checkTaskStatusWrite(ctx, reg, "T-LIVE", literal, target); err != nil {
					t.Errorf("status change %q -> %q refused: %v — a live legacy-status row must be mutable to canonical members", literal, target, err)
				}
			}
		})
	}
}

// TestRealConfigValidates proves the actual shipped task-graph.config.json
// (and its post-cutover sibling, which must carry the same registries or a
// server started through that config boots with an EMPTY closed set and
// rejects every write) passes SpecGraphConfig.Validate() — the same gate
// loadConfig() runs at boot. The cloud-run sibling was removed on 2026-09-06
// with the Cloud Run deployment.
func TestRealConfigValidates(t *testing.T) {
	for _, path := range []string{
		"task-graph.config.json",
		"task-graph.post-cutover.config.json",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var cfg SpecGraphConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("%s failed Validate(): %v", path, err)
			}
			if len(cfg.TaskStatuses) == 0 || len(cfg.DependencyKinds) == 0 || len(cfg.ArtifactKinds) == 0 {
				t.Fatalf("%s: task_statuses=%d dependency_kinds=%d artifact_kinds=%d — every deployment config must carry all three closed registries, not just the primary one", path, len(cfg.TaskStatuses), len(cfg.DependencyKinds), len(cfg.ArtifactKinds))
			}
		})
	}
}

// TestRealConfigTaskStatusesAbsorbOnlyTheirOwnName pins the end state of the
// 2026-09-13 owner ruling on MVP-GRAPH-STATUS-NORMALIZATION: once the live
// census of non-canonical Tasks.status literals reached zero (2026-09-14,
// worklink LNK-b5ea4bd0), every shipped config's task_statuses absorbs list
// shrinks to exactly its own member name, so no alias map survives as a
// permanent half-state. The terminal set stays done, cancelled and archived,
// because the canonical count surface takes the terminal flag from this
// registry.
func TestRealConfigTaskStatusesAbsorbOnlyTheirOwnName(t *testing.T) {
	wantTerminal := map[string]bool{
		"not_started": false,
		"in_progress": false,
		"blocked":     false,
		"in_review":   false,
		"done":        true,
		"cancelled":   true,
		"archived":    true,
	}
	for _, path := range []string{
		"task-graph.config.json",
		"task-graph.post-cutover.config.json",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var cfg SpecGraphConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			if len(cfg.TaskStatuses) != len(wantTerminal) {
				t.Fatalf("%s: task_statuses has %d members, want %d", path, len(cfg.TaskStatuses), len(wantTerminal))
			}
			for _, d := range cfg.TaskStatuses {
				terminal, known := wantTerminal[d.Name()]
				if !known {
					t.Errorf("%s: unexpected task_statuses member %q", path, d.Name())
					continue
				}
				if absorbs := d.Absorbs(); len(absorbs) != 1 || absorbs[0] != d.Name() {
					t.Errorf("%s: member %q absorbs %q, want exactly [%q]", path, d.Name(), absorbs, d.Name())
				}
				if d.Terminal() != terminal {
					t.Errorf("%s: member %q terminal = %v, want %v", path, d.Name(), d.Terminal(), terminal)
				}
			}
		})
	}
}
