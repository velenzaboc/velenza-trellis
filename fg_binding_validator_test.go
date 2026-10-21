package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

// These tests use the landed envelope fixtures and the native TaskLinks
// projection. Literal rule expectations deliberately do not share constants
// with the implementation, so changing a rule's wire name breaks its test.
func TestFGBindingValidator(t *testing.T) {
	readJSON := func(name string, dst any) {
		t.Helper()
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, dst); err != nil {
			t.Fatal(err)
		}
	}
	var fixture map[string]string
	readJSON("testdata/fg_envelope_positive.json", &fixture)
	var config struct {
		ArtifactKinds []ArtifactKindDescriptor `json:"artifact_kinds"`
	}
	readJSON("task-graph.config.json", &config)
	reg, err := newArtifactKindRegistry(config.ArtifactKinds)
	if err != nil {
		t.Fatal(err)
	}

	// Only coordinates with actual TaskLinks columns come from the row.
	// The caller supplies the five envelope coordinates absent from that row.
	fromFixture := func(f map[string]string) (WorklinkRow, FGBindingEvidence) {
		rows := nativeSnapshotWorklinkRowsFrom(QueryResult{Rows: []map[string]interface{}{{
			"project_id": f["project_id"], "task_id": f["task_id"],
			"artifact_id": "fixture-link", "artifact_type": "binding",
			"artifact_ref": "task-graph/REQUIREMENTS.md#section-2",
			"repo":         f["repo"], "thread": f["thread"], "session_id": f["session"],
			"worktree_path": f["absolute_worktree"], "branch": f["branch"], "commit_sha": f["commit_sha"],
			"created_at": "2026-09-01T00:00:00Z", "url": "https://example.invalid/artifact",
			"note": "free text is not envelope metadata", "pr_number": int64(47), "pr_state": "merged",
		}}})
		return rows[0], FGBindingEvidence{
			Host: f["host"], AccountOrHarness: f["account_or_harness"],
			PRorSpecRef: f["pr_or_spec_ref"], CheckpointRef: f["checkpoint_ref"], MeasuredAt: f["measured_at"],
		}
	}
	row, evidence := fromFixture(fixture)

	type field struct {
		name string
		set  func(*WorklinkRow, *FGBindingEvidence, string)
	}
	fields := []field{
		{"project_id", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.ProjectID = v }},
		{"task_id", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.TaskID = v }},
		{"repo", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.Repo = v }},
		{"host", func(_ *WorklinkRow, e *FGBindingEvidence, v string) { e.Host = v }},
		{"account_or_harness", func(_ *WorklinkRow, e *FGBindingEvidence, v string) { e.AccountOrHarness = v }},
		{"thread", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.Thread = v }},
		{"session_id", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.SessionID = v }},
		{"worktree_path", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.WorktreePath = v }},
		{"branch", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.Branch = v }},
		{"commit_sha", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.CommitSHA = v }},
		{"pr_or_spec_ref", func(_ *WorklinkRow, e *FGBindingEvidence, v string) { e.PRorSpecRef = v }},
		{"checkpoint_ref", func(_ *WorklinkRow, e *FGBindingEvidence, v string) { e.CheckpointRef = v }},
		{"measured_at", func(_ *WorklinkRow, e *FGBindingEvidence, v string) { e.MeasuredAt = v }},
		{"artifact_ref", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.ArtifactRef = v }},
		{"artifact_type", func(r *WorklinkRow, _ *FGBindingEvidence, v string) { r.ArtifactType = v }},
	}
	type testCase struct {
		name, field, value, rule string
	}
	cases := []testCase{
		{"positive_fixture", "", "", ""},
		{"worktree_kind", "artifact_type", "worktree", ""},
		{"checkpoint_kind", "artifact_type", "checkpoint", ""},
		{"absorbed_binding", "artifact_type", "live_binding", ""},
		{"absorbed_worktree", "artifact_type", "branch", ""},
		{"absorbed_checkpoint", "artifact_type", "implementation_checkpoint", ""},
		{"unknown_kind", "artifact_type", "unknown", "artifact_type"},
		{"out_of_scope_kind", "artifact_type", "pr", "artifact_type"},
		{"out_of_scope_alias", "artifact_type", "agent", "artifact_type"},
		{"placeholder_ref", "artifact_ref", "current-task:task-123", "placeholder_ref"},
		{"bare_placeholder", "artifact_ref", "current-task:", "placeholder_ref"},
		{"literal_prefix_only", "artifact_ref", "spec#current-task:example", ""},
		{"missing_commit", "commit_sha", "", "missing_commit"},
		{"short_sha", "commit_sha", "665d77c", "invalid_commit_sha"},
		{"long_sha", "commit_sha", fixture["commit_sha"] + "a", "invalid_commit_sha"},
		{"uppercase_sha", "commit_sha", strings.ToUpper(fixture["commit_sha"]), "invalid_commit_sha"},
		{"nonhex_sha", "commit_sha", strings.Repeat("g", 40), "invalid_commit_sha"},
		{"sha_whitespace", "commit_sha", fixture["commit_sha"] + "\n", "invalid_commit_sha"},
		{"unknown_host", "host", "runner-host", "unknown_host"},
		{"uppercase_host", "host", "WORKSTATION-C", "unknown_host"},
		{"workstation_a_host", "host", "workstation-a", ""},
		{"workstation_b_host", "host", "workstation-b", ""},
		{"invalid_repo", "repo", "Task_Graph", "invalid_repo"},
		{"relative_worktree", "worktree_path", "worktrees/task", "relative_worktree"},
		{"drive_relative_worktree", "worktree_path", "D:worktrees/task", "relative_worktree"},
		{"workspace_root_linux", "worktree_path", "/home/developer/workspace", "workspace_root_worktree"},
		{"workspace_root_linux_normalized", "worktree_path", "/home/developer/workspace/worktrees/..", "workspace_root_worktree"},
		{"checkpoint_missing_digest", "checkpoint_ref", "/tmp/checkpoint.json", "invalid_checkpoint_ref"},
		{"checkpoint_missing_path", "checkpoint_ref", "#sha256=" + strings.Repeat("a", 64), "invalid_checkpoint_ref"},
		{"checkpoint_short_digest", "checkpoint_ref", "/tmp/c#sha256=abc", "invalid_checkpoint_ref"},
		{"checkpoint_uppercase_digest", "checkpoint_ref", "/tmp/c#sha256=" + strings.Repeat("A", 64), "invalid_checkpoint_ref"},
		{"bad_measured_at", "measured_at", "yesterday", "invalid_measured_at"},
		{"impossible_date", "measured_at", "2026-02-30T01:02:03Z", "invalid_measured_at"},
		{"one_digit_hour", "measured_at", "2026-09-13T4:21:37Z", "invalid_measured_at"},
		{"comma_fraction", "measured_at", "2026-09-13T04:21:37,123Z", "invalid_measured_at"},
		{"offset_time", "measured_at", "2026-09-13T04:21:37.123456789-04:00", ""},
		{"embedded_unresolved", "checkpoint_ref", "/tmp/UNRESOLVED#sha256=" + strings.Repeat("a", 64), "unresolved"},
	}
	for _, p := range []string{`D:\workspace`, "D:/workspace/", "d:/workspace/.", "/workspace/arbor", "/Users/developer/workspace/", "/workspace/arbor/worktrees/.."} {
		cases = append(cases, testCase{"workspace_root_" + p, "worktree_path", p, "workspace_root_worktree"})
	}
	for _, p := range []string{`D:\workspace\repos\task-graph`, "d:/WORKSPACE/REPOS/task-graph/subdir", "/workspace/arbor/repos/task-graph", "/Users/developer/workspace/repos/task-graph/.git", "/home/developer/workspace/repos/task-graph", "/tmp/repos/task-graph", "/tmp/repos", "/tmp/worktrees/../repos/task-graph"} {
		cases = append(cases, testCase{"shared_mirror_" + p, "worktree_path", p, "shared_mirror_worktree"})
	}
	for _, p := range []string{`D:\workspace\worktrees\task`, "/workspace/arbor/worktrees/task", "/Users/developer/workspace/worktrees/task", "/tmp/repos-copy/task", "/tmp/worktrees/repos-task"} {
		cases = append(cases, testCase{"isolated_worktree_" + p, "worktree_path", p, ""})
	}
	for _, f := range fields {
		cases = append(cases, testCase{"unresolved_" + f.name, f.name, "UNRESOLVED", "unresolved"})
		rule := "missing_field"
		if f.name == "commit_sha" {
			rule = "missing_commit"
		} else if f.name == "artifact_type" {
			rule = "artifact_type"
		}
		cases = append(cases, testCase{"empty_" + f.name, f.name, "", rule})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, e := row, evidence
			for _, f := range fields {
				if f.name == tc.field {
					f.set(&r, &e, tc.value)
				}
			}
			beforeRow, beforeEvidence := r, e
			got, err := ValidateFGBinding(context.Background(), reg, r, e)
			if r != beforeRow || e != beforeEvidence {
				t.Fatal("validator mutated its input")
			}
			again, againErr := ValidateFGBinding(context.Background(), reg, r, e)
			if !reflect.DeepEqual(got, again) || !reflect.DeepEqual(err, againErr) {
				t.Fatalf("non-deterministic result: (%+v, %v), (%+v, %v)", got, err, again, againErr)
			}
			if tc.rule != "" {
				assertFGBindingRejection(t, got, err, tc.rule, tc.field)
				return
			}
			if err != nil {
				t.Fatalf("unexpected rejection: %v", err)
			}
			b, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var mapped map[string]string
			if err := json.Unmarshal(b, &mapped); err != nil {
				t.Fatal(err)
			}
			want := make(map[string]string, len(fixture))
			for k, v := range fixture {
				want[k] = v
			}
			if tc.field != "" && tc.field != "artifact_type" && tc.field != "artifact_ref" {
				key := tc.field
				if key == "worktree_path" {
					key = "absolute_worktree"
				}
				want[key] = tc.value
			}
			if !reflect.DeepEqual(mapped, want) {
				t.Fatalf("mapped envelope = %#v, want exact fixture = %#v", mapped, want)
			}
			assertFGBindingSchema(t, mapped)
		})
	}
	t.Run("missing_commit_fixture", func(t *testing.T) {
		var missing map[string]string
		readJSON("testdata/fg_envelope_missing_commit.json", &missing)
		r, e := fromFixture(missing)
		got, err := ValidateFGBinding(context.Background(), reg, r, e)
		assertFGBindingRejection(t, got, err, "missing_commit", "commit_sha")
	})
	t.Run("missing_registry", func(t *testing.T) {
		got, err := ValidateFGBinding(context.Background(), nil, row, evidence)
		assertFGBindingRejection(t, got, err, "artifact_type", "artifact_type")
	})
}

func assertFGBindingRejection(t *testing.T, got FGBindingEnvelope, err error, rule, field string) {
	t.Helper()
	var rejection *FGBindingRejection
	if !errors.As(err, &rejection) {
		t.Fatalf("error = %v, want *FGBindingRejection (%s, %s)", err, rule, field)
	}
	if string(rejection.Rule) != rule || rejection.Field != field {
		t.Fatalf("rejection = %+v, want rule=%s field=%s", rejection, rule, field)
	}
	if got != (FGBindingEnvelope{}) {
		t.Fatalf("rejected input leaked a usable envelope: %+v", got)
	}
	if !strings.Contains(err.Error(), rule) || !strings.Contains(err.Error(), field) {
		t.Fatalf("error omitted rule or field: %v", err)
	}
}

// Independently check the output against every constraint in the landed flat
// string schema, without adding a production dependency or reading it at runtime.
func assertFGBindingSchema(t *testing.T, envelope map[string]string) {
	t.Helper()
	var schema struct {
		Required             []string `json:"required"`
		AdditionalProperties bool     `json:"additionalProperties"`
		Properties           map[string]struct {
			Type      string   `json:"type"`
			MinLength int      `json:"minLength"`
			Pattern   string   `json:"pattern"`
			Enum      []string `json:"enum"`
			Format    string   `json:"format"`
		} `json:"properties"`
	}
	b, err := os.ReadFile("testdata/fg_envelope.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatal(err)
	}
	for _, key := range schema.Required {
		if _, ok := envelope[key]; !ok {
			t.Errorf("schema requires %s", key)
		}
	}
	for key, value := range envelope {
		p, ok := schema.Properties[key]
		if !ok && !schema.AdditionalProperties {
			t.Fatalf("unexpected envelope property %s", key)
		}
		if p.Type != "string" || len([]rune(value)) < p.MinLength || (p.Pattern != "" && !regexp.MustCompile(p.Pattern).MatchString(value)) {
			t.Errorf("%s=%q violates landed schema", key, value)
		}
		if len(p.Enum) > 0 {
			found := false
			for _, allowed := range p.Enum {
				found = found || value == allowed
			}
			if !found {
				t.Errorf("%s=%q is not in schema enum", key, value)
			}
		}
		if p.Format == "date-time" {
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				t.Error(err)
			}
		}
	}
}
