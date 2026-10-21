package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func fgCheckpointRegistry(t *testing.T) ArtifactKindRegistry {
	t.Helper()
	reg, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "checkpoint", "absorbs": []interface{}{"implementation_checkpoint"}}),
	})
	if err != nil {
		t.Fatalf("newArtifactKindRegistry: %v", err)
	}
	return reg
}

func fgEpoch(value uint64) *uint64 { return &value }

func fgCheckpointRow(id, project, task, ref, created string) WorklinkRow {
	return WorklinkRow{ArtifactID: id, ProjectID: project, TaskID: task, ArtifactType: "checkpoint", ArtifactRef: ref, CreatedAt: created}
}

func TestFGCheckpointSelect(t *testing.T) {
	ctx := context.Background()
	reg := fgCheckpointRegistry(t)
	hashA := strings.Repeat("a", 64)
	hashBUpper := strings.Repeat("B", 64)
	rows := []WorklinkRow{
		fgCheckpointRow("later", "arbor", "TASK", `C:\workspace\checkpoints\b.json#`+hashBUpper, "2026-09-11 03:00:00.123456789-04"),
		fgCheckpointRow("earlier", "arbor", "TASK", "/tmp/a.json#sha256="+hashA, "2026-09-11T06:00:00Z"),
	}
	evidence := map[string]FGCheckpointEvidence{"earlier": {Epoch: fgEpoch(7)}, "later": {Epoch: fgEpoch(8)}}

	t.Run("distinct_epochs_select_later_and_normalize_digest", func(t *testing.T) {
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", rows, evidence, nil)
		if got.Outcome != FGCheckpointSelected || got.Checkpoint == nil || got.Checkpoint.ArtifactID != "later" {
			t.Fatalf("selection = %#v", got)
		}
		if got.Checkpoint.Path != `C:\workspace\checkpoints\b.json` || got.Checkpoint.SHA256 != strings.ToLower(hashBUpper) {
			t.Fatalf("parsed checkpoint = %#v", got.Checkpoint)
		}
	})

	t.Run("higher_epoch_wins_over_later_created_at", func(t *testing.T) {
		inverted := []WorklinkRow{
			fgCheckpointRow("high-epoch-early", "arbor", "TASK", "/tmp/high.json#"+hashA, "2026-09-11T05:00:00Z"),
			fgCheckpointRow("low-epoch-late", "arbor", "TASK", "/tmp/low.json#sha256="+hashA, "2026-09-11T09:00:00Z"),
		}
		ev := map[string]FGCheckpointEvidence{"high-epoch-early": {Epoch: fgEpoch(9)}, "low-epoch-late": {Epoch: fgEpoch(3)}}
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", inverted, ev, nil)
		if got.Outcome != FGCheckpointSelected || got.Checkpoint == nil || got.Checkpoint.ArtifactID != "high-epoch-early" {
			t.Fatalf("epoch must order ahead of created_at; selection = %#v", got)
		}
	})

	t.Run("equal_max_epoch_is_ambiguous_regardless_of_created_at", func(t *testing.T) {
		tied := map[string]FGCheckpointEvidence{"earlier": {Epoch: fgEpoch(8)}, "later": {Epoch: fgEpoch(8)}}
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", rows, tied, nil)
		if got.Outcome != FGCheckpointAmbiguous || !reflect.DeepEqual(got.Evidence, []string{"earlier", "later"}) {
			t.Fatalf("selection = %#v", got)
		}
	})

	t.Run("epochless_uses_parsed_time_not_lexical_offset", func(t *testing.T) {
		epochless := []WorklinkRow{
			fgCheckpointRow("absolute-later", "arbor", "TASK", "/tmp/later#"+hashA, "2026-09-11 03:00:00-04"),
			fgCheckpointRow("lexical-later", "arbor", "TASK", "/tmp/earlier#"+hashA, "2026-09-11 06:30:00+00"),
		}
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", epochless, nil, nil)
		if got.Outcome != FGCheckpointSelected || got.Checkpoint.ArtifactID != "absolute-later" {
			t.Fatalf("selection = %#v", got)
		}
	})

	t.Run("epochless_exact_time_tie_is_ambiguous", func(t *testing.T) {
		tied := []WorklinkRow{
			fgCheckpointRow("a", "arbor", "TASK", "/tmp/a#"+hashA, "2026-09-11 03:00:00-04"),
			fgCheckpointRow("b", "arbor", "TASK", "/tmp/b#"+hashA, "2026-09-11T07:00:00Z"),
		}
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", tied, nil, nil)
		if got.Outcome != FGCheckpointAmbiguous || !reflect.DeepEqual(got.Evidence, []string{"a", "b"}) {
			t.Fatalf("selection = %#v", got)
		}
	})

	t.Run("malformed_candidate_refuses_without_older_fallback", func(t *testing.T) {
		bad := append([]WorklinkRow(nil), rows...)
		bad[0].ArtifactRef = "/tmp/newest-without-hash"
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", bad, evidence, nil)
		if got.Outcome != FGCheckpointUnverifiable || !reflect.DeepEqual(got.Evidence, []string{"later"}) || got.Checkpoint != nil {
			t.Fatalf("selection = %#v", got)
		}
	})

	t.Run("mixed_epoch_evidence_refuses", func(t *testing.T) {
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", rows, map[string]FGCheckpointEvidence{"later": {Epoch: fgEpoch(8)}}, nil)
		if got.Outcome != FGCheckpointUnverifiable || !reflect.DeepEqual(got.Evidence, []string{"earlier", "later"}) {
			t.Fatalf("selection = %#v", got)
		}
	})

	t.Run("strictly_later_classified_binding_makes_selection_stale", func(t *testing.T) {
		bindings := []WorklinkRow{{ArtifactID: "binding-old", CreatedAt: "2026-09-11T06:59:59Z"}, {ArtifactID: "binding-new", CreatedAt: "2026-09-11T07:00:01Z"}}
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", rows, evidence, bindings)
		if got.Outcome != FGCheckpointStale || !reflect.DeepEqual(got.Evidence, []string{"binding-new", "later"}) {
			t.Fatalf("selection = %#v", got)
		}
	})

	t.Run("equal_time_binding_is_not_strictly_later", func(t *testing.T) {
		bindings := []WorklinkRow{{ArtifactID: "binding", CreatedAt: rows[0].CreatedAt}}
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", rows, evidence, bindings)
		if got.Outcome != FGCheckpointSelected {
			t.Fatalf("selection = %#v", got)
		}
	})

	t.Run("scope_alias_and_deterministic_replay", func(t *testing.T) {
		scoped := append([]WorklinkRow(nil), rows...)
		scoped[0].ArtifactType = "implementation_checkpoint"
		scoped = append(scoped,
			fgCheckpointRow("other-task", "arbor", "OTHER", "/tmp/x#"+hashA, "2099-01-01T00:00:00Z"),
			fgCheckpointRow("other-project", "elsewhere", "TASK", "/tmp/y#"+hashA, "2099-01-01T00:00:00Z"),
			WorklinkRow{ArtifactID: "binding", ProjectID: "arbor", TaskID: "TASK", ArtifactType: "worktree"},
		)
		first := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", scoped, evidence, nil)
		second := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", scoped, evidence, nil)
		if !reflect.DeepEqual(first, second) || first.Outcome != FGCheckpointSelected || first.Checkpoint.ArtifactID != "later" {
			t.Fatalf("first = %#v; second = %#v", first, second)
		}
	})

	t.Run("no_checkpoint_is_unverifiable", func(t *testing.T) {
		got := SelectFGCheckpoint(ctx, reg, "arbor", "TASK", nil, nil, nil)
		if got.Outcome != FGCheckpointUnverifiable || got.Checkpoint != nil {
			t.Fatalf("selection = %#v", got)
		}
	})
}
