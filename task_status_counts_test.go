package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// task_status_counts_test.go covers the canonical-status fold behind
// task_graph_task_counts and the project snapshot counts
// (MVP-GRAPH-STATUS-NORMALIZATION). Raw (pillar, status literal) groups are
// reported again per task_statuses member, the terminal flag comes from
// config, and no row is ever dropped. Every function under test is pure, so
// this file runs in the default `go test ./...`.

func realConfigTaskStatusRegistry(t *testing.T) TaskStatusRegistry {
	t.Helper()
	cfg := loadRealTaskGraphConfig(t)
	reg, err := newTaskStatusRegistry(cfg.TaskStatuses)
	if err != nil {
		t.Fatalf("build task_statuses registry from real config: %v", err)
	}
	return reg
}

// requireCanonicalCountsSumToRaw asserts the fold neither drops nor invents a
// row: for every pillar, the canonical counts add up to the raw counts.
func requireCanonicalCountsSumToRaw(t *testing.T, raw []PillarStatusCount, groups []PillarCanonicalStatusCount) {
	t.Helper()
	rawByPillar := map[string]int{}
	for _, r := range raw {
		rawByPillar[r.Pillar] += r.Count
	}
	foldedByPillar := map[string]int{}
	for _, g := range groups {
		foldedByPillar[g.Pillar] += g.Count
	}
	if !reflect.DeepEqual(rawByPillar, foldedByPillar) {
		t.Fatalf("canonical counts per pillar %v do not sum to raw counts per pillar %v", foldedByPillar, rawByPillar)
	}
}

// TestCanonicalStatusCountsFoldRealConfig runs the shipped config's registry
// over canonical and legacy literal shapes. Every task_statuses member now
// absorbs only its own name (the non-canonical census reached zero on
// 2026-09-14), so each canonical member folds onto itself with its configured
// terminal flag, while a retired legacy spelling such as completed, planned or
// audit_probe, a literal nobody ever absorbed, and an empty status are all
// counted as unrecognized and listed rather than dropped or re-bucketed.
func TestCanonicalStatusCountsFoldRealConfig(t *testing.T) {
	raw := []PillarStatusCount{
		{Pillar: "ASSESS", Status: "archived", Count: 1},
		{Pillar: "ASSESS", Status: "audit_probe", Count: 4},
		{Pillar: "ASSESS", Status: "cancelled", Count: 2},
		{Pillar: "ASSESS", Status: "completed", Count: 3},
		{Pillar: "ASSESS", Status: "done", Count: 5},
		{Pillar: "ASSESS", Status: "not_started", Count: 6},
		{Pillar: "ASSESS", Status: "planned", Count: 7},
		{Pillar: "ASSESS", Status: "status_no_member_absorbs", Count: 9},
		{Pillar: "BUILD", Status: "blocked", Count: 12},
		{Pillar: "BUILD", Status: "in_progress", Count: 13},
		{Pillar: "BUILD", Status: "in_review", Count: 14},
		{Pillar: "BUILD", Status: "completed", Count: 10},
		{Pillar: "BUILD", Status: "", Count: 11},
	}
	groups, literals, err := canonicalStatusCounts(context.Background(), realConfigTaskStatusRegistry(t), raw)
	if err != nil {
		t.Fatalf("canonicalStatusCounts: %v", err)
	}
	want := []PillarCanonicalStatusCount{
		{Pillar: "ASSESS", CanonicalStatus: "archived", Terminal: true, Count: 1},
		{Pillar: "ASSESS", CanonicalStatus: "cancelled", Terminal: true, Count: 2},
		{Pillar: "ASSESS", CanonicalStatus: "done", Terminal: true, Count: 5},
		{Pillar: "ASSESS", CanonicalStatus: "not_started", Terminal: false, Count: 6},
		{Pillar: "ASSESS", CanonicalStatus: unrecognizedTaskStatus, Terminal: false, Count: 23},
		{Pillar: "BUILD", CanonicalStatus: "blocked", Terminal: false, Count: 12},
		{Pillar: "BUILD", CanonicalStatus: "in_progress", Terminal: false, Count: 13},
		{Pillar: "BUILD", CanonicalStatus: "in_review", Terminal: false, Count: 14},
		{Pillar: "BUILD", CanonicalStatus: unrecognizedTaskStatus, Terminal: false, Count: 21},
	}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("canonical groups =\n%+v\nwant\n%+v", groups, want)
	}
	if wantLiterals := []string{"", "audit_probe", "completed", "planned", "status_no_member_absorbs"}; !reflect.DeepEqual(literals, wantLiterals) {
		t.Fatalf("unrecognized literals = %q, want %q", literals, wantLiterals)
	}
	requireCanonicalCountsSumToRaw(t, raw, groups)
}

// TestCanonicalStatusCountsTakeTerminalFromConfig flips one member's terminal
// flag in config and requires the count surface to follow it both ways, so
// a hard-coded terminal set cannot pass.
func TestCanonicalStatusCountsTakeTerminalFromConfig(t *testing.T) {
	raw := []PillarStatusCount{
		{Pillar: "P", Status: "completed", Count: 2},
		{Pillar: "P", Status: "in_progress", Count: 1},
	}
	for _, doneTerminal := range []bool{true, false} {
		reg, err := newTaskStatusRegistry([]TaskStatusDescriptor{
			NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "terminal": doneTerminal, "absorbs": []interface{}{"done", "completed"}}),
			NewTaskStatusDescriptor(map[string]interface{}{"name": "in_progress"}),
		})
		if err != nil {
			t.Fatalf("build registry: %v", err)
		}
		groups, literals, err := canonicalStatusCounts(context.Background(), reg, raw)
		if err != nil {
			t.Fatalf("canonicalStatusCounts: %v", err)
		}
		want := []PillarCanonicalStatusCount{
			{Pillar: "P", CanonicalStatus: "done", Terminal: doneTerminal, Count: 2},
			{Pillar: "P", CanonicalStatus: "in_progress", Terminal: false, Count: 1},
		}
		if !reflect.DeepEqual(groups, want) {
			t.Fatalf("config terminal=%v: groups = %+v, want %+v", doneTerminal, groups, want)
		}
		if len(literals) != 0 {
			t.Fatalf("config terminal=%v: unrecognized literals = %q, want none", doneTerminal, literals)
		}
	}
}

// TestCanonicalStatusCountsWithoutRegistryCountEveryRowUnrecognized: an Env
// with no task_statuses registry claims no literal, so every row lands in the
// unrecognized bucket rather than disappearing from the canonical view.
func TestCanonicalStatusCountsWithoutRegistryCountEveryRowUnrecognized(t *testing.T) {
	raw := []PillarStatusCount{
		{Pillar: "P", Status: "done", Count: 2},
		{Pillar: "P", Status: "completed", Count: 3},
	}
	groups, literals, err := canonicalStatusCounts(context.Background(), nil, raw)
	if err != nil {
		t.Fatalf("canonicalStatusCounts: %v", err)
	}
	want := []PillarCanonicalStatusCount{{Pillar: "P", CanonicalStatus: unrecognizedTaskStatus, Terminal: false, Count: 5}}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("groups = %+v, want %+v", groups, want)
	}
	if wantLiterals := []string{"completed", "done"}; !reflect.DeepEqual(literals, wantLiterals) {
		t.Fatalf("unrecognized literals = %q, want %q", literals, wantLiterals)
	}
	requireCanonicalCountsSumToRaw(t, raw, groups)
}

type canonicalizesButCannotResolveRegistry struct{}

func (canonicalizesButCannotResolveRegistry) Resolve(context.Context, string) (TaskStatusDescriptor, error) {
	return TaskStatusDescriptor{}, ErrUnknownTaskStatus
}

func (canonicalizesButCannotResolveRegistry) Canonicalize(context.Context, string) (string, bool) {
	return "done", true
}

func (canonicalizesButCannotResolveRegistry) All(context.Context) ([]TaskStatusDescriptor, error) {
	return nil, nil
}

// TestCanonicalStatusCountsRefuseAnInconsistentRegistry: a registry that
// places a literal on a member it cannot resolve has no terminal flag to
// report, so the fold fails rather than guessing one.
func TestCanonicalStatusCountsRefuseAnInconsistentRegistry(t *testing.T) {
	_, _, err := canonicalStatusCounts(context.Background(), canonicalizesButCannotResolveRegistry{}, []PillarStatusCount{{Pillar: "P", Status: "completed", Count: 1}})
	if !errors.Is(err, ErrUnknownTaskStatus) {
		t.Fatalf("inconsistent registry error = %v, want ErrUnknownTaskStatus", err)
	}
}

// TestNativeTaskCountsFromPublishesEmptyCanonicalFieldsAsArrays: the live
// positive control reads an empty unrecognized_status_literals, so an empty
// result must serialize as [] rather than null.
func TestNativeTaskCountsFromPublishesEmptyCanonicalFieldsAsArrays(t *testing.T) {
	counts, err := nativeTaskCountsFrom(context.Background(), realConfigTaskStatusRegistry(t), QueryResult{}, QueryResult{})
	if err != nil {
		t.Fatalf("nativeTaskCountsFrom: %v", err)
	}
	b, err := json.Marshal(counts)
	if err != nil {
		t.Fatalf("marshal counts: %v", err)
	}
	for _, want := range []string{`"by_pillar_canonical_status":[]`, `"unrecognized_status_literals":[]`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("counts JSON %s does not contain %s", b, want)
		}
	}
}

// TestTaskStatusDescriptorValidateRefusesTheUnrecognizedBucketName keeps the
// count bucket unambiguous: no configured member may be named unrecognized.
func TestTaskStatusDescriptorValidateRefusesTheUnrecognizedBucketName(t *testing.T) {
	err := NewTaskStatusDescriptor(map[string]interface{}{"name": unrecognizedTaskStatus}).Validate(0)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("Validate on a member named %q = %v, want a reserved-name refusal", unrecognizedTaskStatus, err)
	}
}
