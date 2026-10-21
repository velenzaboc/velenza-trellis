package main

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func fgUniqueReadJSON(t *testing.T, name string, out any) {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
}

func fgUniqueRegistry(t *testing.T) ArtifactKindRegistry {
	t.Helper()
	var config struct {
		Kinds []ArtifactKindDescriptor `json:"artifact_kinds"`
	}
	fgUniqueReadJSON(t, "task-graph.config.json", &config)
	reg, err := newArtifactKindRegistry(config.Kinds)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// Explicit synthetic coordinates, independent of task owner/session and notes.
func fgUniqueInput(t *testing.T) (ProjectSnapshotResult, map[string]FGBindingEvidence) {
	t.Helper()
	var envelope FGBindingEnvelope
	fgUniqueReadJSON(t, "testdata/fg_envelope_positive.json", &envelope)
	row := WorklinkRow{
		ProjectID: "arbor", TaskID: "TASK", ArtifactID: "LNK-b", ArtifactType: "worktree",
		ArtifactRef: "test-binding", Repo: envelope.Repo, Thread: envelope.Thread,
		SessionID: envelope.Session, WorktreePath: envelope.AbsoluteWorktree,
		Branch: envelope.Branch, CommitSHA: envelope.CommitSHA,
	}
	evidence := FGBindingEvidence{
		Host: envelope.Host, AccountOrHarness: envelope.AccountOrHarness,
		PRorSpecRef: envelope.PRorSpecRef, CheckpointRef: envelope.CheckpointRef, MeasuredAt: envelope.MeasuredAt,
	}
	return ProjectSnapshotResult{
		ProjectID: "arbor", Complete: true,
		Scope:     SnapshotScope{Mode: "execution_subtree", RootTaskID: "TASK", Complete: true},
		Tasks:     []TaskRow{{ProjectID: "arbor", TaskID: "TASK", Status: "in_progress", Repo: "task-graph"}},
		Worklinks: []WorklinkRow{row},
	}, map[string]FGBindingEvidence{row.ArtifactID: evidence}
}

func fgUniqueJSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Literal JSON expectations pin field names, classes, counts, pass/fail, task
// identity, and link ID order without sharing production classification logic.
func fgUniqueAssert(t *testing.T, reg ArtifactKindRegistry, snapshot ProjectSnapshotResult, evidence map[string]FGBindingEvidence, want string) {
	t.Helper()
	beforeSnapshot, beforeEvidence := fgUniqueJSON(t, snapshot), fgUniqueJSON(t, evidence)
	got, err := RunFGUniqueBindingCheck(context.Background(), reg, snapshot, evidence)
	if err != nil {
		t.Fatalf("unexpected refusal: %v", err)
	}
	if actual := fgUniqueJSON(t, got); actual != want {
		t.Fatalf("result = %s, want %s", actual, want)
	}
	if fgUniqueJSON(t, snapshot) != beforeSnapshot || fgUniqueJSON(t, evidence) != beforeEvidence {
		t.Fatal("check mutated its input")
	}
}

const fgUniqueOne = `{"passed":true,"tasks":[{"task_id":"TASK","class":"exactly_one","count":1,"link_ids":["LNK-b"]}]}`
const fgUniqueZero = `{"passed":false,"tasks":[{"task_id":"TASK","class":"zero","count":0,"link_ids":[]}]}`
const fgUniqueEmpty = `{"passed":true,"tasks":[]}`

func TestFGUniqueBindingEligibility(t *testing.T) {
	reg := fgUniqueRegistry(t)
	for _, status := range []string{"in_progress", "not_started", "blocked", "in_review", "done", "cancelled", "archived", "", "unknown", "IN_PROGRESS", " in_progress", "in_progress "} {
		for _, repo := range []string{"task-graph", ""} {
			t.Run(status+"/repo="+repo, func(t *testing.T) {
				t.Parallel()
				s, e := fgUniqueInput(t)
				s.Tasks[0].Status, s.Tasks[0].Repo = status, repo
				s.Worklinks = nil
				want := fgUniqueEmpty
				if status == "in_progress" && repo != "" {
					want = fgUniqueZero
				}
				fgUniqueAssert(t, reg, s, e, want)
			})
		}
	}
	t.Run("nonempty_repo_is_literal", func(t *testing.T) {
		s, e := fgUniqueInput(t)
		s.Tasks[0].Repo = " "
		s.Worklinks = nil
		fgUniqueAssert(t, reg, s, e, fgUniqueZero)
	})
}

func TestFGUniqueBindingLinkKinds(t *testing.T) {
	reg := fgUniqueRegistry(t)
	kinds, err := reg.All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range kinds {
		literals := append([]string{kind.Name()}, kind.Absorbs()...)
		slices.Sort(literals)
		for _, literal := range slices.Compact(literals) {
			t.Run(literal, func(t *testing.T) {
				t.Parallel()
				s, e := fgUniqueInput(t)
				s.Worklinks[0].ArtifactType = literal
				want := fgUniqueZero
				if kind.Name() == "worktree" || kind.Name() == "binding" {
					want = fgUniqueOne
				}
				fgUniqueAssert(t, reg, s, e, want)
			})
		}
	}
	for _, literal := range []string{"", "unknown", "WORKTREE", " worktree", "worktree "} {
		t.Run("unrecognized="+literal, func(t *testing.T) {
			s, e := fgUniqueInput(t)
			s.Worklinks[0].ArtifactType = literal
			fgUniqueAssert(t, reg, s, e, fgUniqueZero)
		})
	}
	// Even an otherwise valid checkpoint passes the prerequisite validator but
	// cannot count as an active binding for this narrower check.
	t.Run("checkpoint_passes_validator_but_does_not_count", func(t *testing.T) {
		s, e := fgUniqueInput(t)
		s.Worklinks[0].ArtifactType = "checkpoint"
		if _, err := ValidateFGBinding(context.Background(), reg, s.Worklinks[0], e["LNK-b"]); err != nil {
			t.Fatal(err)
		}
		fgUniqueAssert(t, reg, s, e, fgUniqueZero)
	})
}

func TestFGUniqueBindingInvalidLinks(t *testing.T) {
	reg := fgUniqueRegistry(t)
	cases := []struct {
		name string
		edit func(*WorklinkRow, *FGBindingEvidence)
	}{
		{"missing_project", func(w *WorklinkRow, _ *FGBindingEvidence) { w.ProjectID = "" }},
		{"missing_task", func(w *WorklinkRow, _ *FGBindingEvidence) { w.TaskID = "" }},
		{"missing_repo", func(w *WorklinkRow, _ *FGBindingEvidence) { w.Repo = "" }},
		{"missing_commit", func(w *WorklinkRow, _ *FGBindingEvidence) { w.CommitSHA = "" }},
		{"invalid_commit", func(w *WorklinkRow, _ *FGBindingEvidence) { w.CommitSHA = "short" }},
		{"missing_thread", func(w *WorklinkRow, _ *FGBindingEvidence) { w.Thread = "" }},
		{"missing_session", func(w *WorklinkRow, _ *FGBindingEvidence) { w.SessionID = "" }},
		{"missing_worktree", func(w *WorklinkRow, _ *FGBindingEvidence) { w.WorktreePath = "" }},
		{"relative_worktree", func(w *WorklinkRow, _ *FGBindingEvidence) { w.WorktreePath = "worktrees/task" }},
		{"workspace_root", func(w *WorklinkRow, _ *FGBindingEvidence) { w.WorktreePath = "D:/workspace" }},
		{"shared_mirror", func(w *WorklinkRow, _ *FGBindingEvidence) { w.WorktreePath = "/tmp/repos/task-graph" }},
		{"missing_branch", func(w *WorklinkRow, _ *FGBindingEvidence) { w.Branch = "" }},
		{"missing_ref", func(w *WorklinkRow, _ *FGBindingEvidence) { w.ArtifactRef = "" }},
		{"placeholder_ref", func(w *WorklinkRow, _ *FGBindingEvidence) { w.ArtifactRef = "current-task:TASK" }},
		{"unresolved", func(w *WorklinkRow, _ *FGBindingEvidence) { w.Branch = "UNRESOLVED" }},
		{"missing_host", func(_ *WorklinkRow, e *FGBindingEvidence) { e.Host = "" }},
		{"unknown_host", func(_ *WorklinkRow, e *FGBindingEvidence) { e.Host = "unknown" }},
		{"missing_harness", func(_ *WorklinkRow, e *FGBindingEvidence) { e.AccountOrHarness = "" }},
		{"missing_proof", func(_ *WorklinkRow, e *FGBindingEvidence) { e.PRorSpecRef = "" }},
		{"missing_checkpoint", func(_ *WorklinkRow, e *FGBindingEvidence) { e.CheckpointRef = "" }},
		{"checkpoint_without_hash", func(_ *WorklinkRow, e *FGBindingEvidence) { e.CheckpointRef = "/tmp/checkpoint" }},
		{"missing_measurement", func(_ *WorklinkRow, e *FGBindingEvidence) { e.MeasuredAt = "" }},
		{"invalid_measurement", func(_ *WorklinkRow, e *FGBindingEvidence) { e.MeasuredAt = "yesterday" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, e := fgUniqueInput(t)
			bad := s.Worklinks[0]
			bad.ArtifactID = "LNK-invalid"
			badEvidence := e["LNK-b"]
			tc.edit(&bad, &badEvidence)
			if _, err := ValidateFGBinding(context.Background(), reg, bad, badEvidence); err == nil {
				t.Fatal("invalid-link control unexpectedly passed the prerequisite validator")
			}
			e[bad.ArtifactID] = badEvidence
			s.Worklinks = append(s.Worklinks, bad)
			fgUniqueAssert(t, reg, s, e, fgUniqueOne)
			s.Worklinks = []WorklinkRow{bad}
			fgUniqueAssert(t, reg, s, e, fgUniqueZero)
		})
	}
	for _, name := range []string{"nil_evidence", "wrong_link_evidence", "no_owner_inference", "no_link_inference", "nil_registry"} {
		t.Run(name, func(t *testing.T) {
			s, e := fgUniqueInput(t)
			r := reg
			switch name {
			case "nil_evidence":
				e = nil
			case "wrong_link_evidence":
				e["another-link"] = e["LNK-b"]
				delete(e, "LNK-b")
			case "no_owner_inference":
				s.Tasks[0].Owner = s.Worklinks[0].Thread
				s.Tasks[0].SessionID = s.Worklinks[0].SessionID
				s.Worklinks[0].Thread, s.Worklinks[0].SessionID = "", ""
			case "no_link_inference":
				s.Worklinks[0].Note = fgUniqueJSON(t, e["LNK-b"])
				s.Worklinks[0].URL = e["LNK-b"].PRorSpecRef
				s.Worklinks[0].CreatedAt = e["LNK-b"].MeasuredAt
				s.Worklinks[0].Branch = "workstation-c/claude/checkpoint"
				e = nil
			case "nil_registry":
				r = nil
			}
			fgUniqueAssert(t, r, s, e, fgUniqueZero)
		})
	}
}

func TestFGUniqueBindingClassification(t *testing.T) {
	reg := fgUniqueRegistry(t)
	cases := []struct {
		name string
		ids  []string
		want string
	}{
		{"zero", nil, fgUniqueZero},
		{"exactly_one", []string{"LNK-b"}, fgUniqueOne},
		{"two", []string{"LNK-b", "LNK-a"}, `{"passed":false,"tasks":[{"task_id":"TASK","class":"more_than_one","count":2,"link_ids":["LNK-a","LNK-b"]}]}`},
		{"three", []string{"LNK-c", "LNK-a", "LNK-b"}, `{"passed":false,"tasks":[{"task_id":"TASK","class":"more_than_one","count":3,"link_ids":["LNK-a","LNK-b","LNK-c"]}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, e := fgUniqueInput(t)
			link, evidence := s.Worklinks[0], e["LNK-b"]
			s.Worklinks = nil
			for _, id := range tc.ids {
				w := link
				w.ArtifactID = id
				s.Worklinks = append(s.Worklinks, w)
				e[id] = evidence
			}
			fgUniqueAssert(t, reg, s, e, tc.want)
		})
	}
	for _, name := range []string{"other_task", "other_project", "no_tasks", "owner_only", "empty_owner_with_binding", "different_repo_valid_binding"} {
		t.Run(name, func(t *testing.T) {
			s, e := fgUniqueInput(t)
			want := fgUniqueZero
			switch name {
			case "other_task":
				s.Worklinks[0].TaskID = "OTHER"
			case "other_project":
				s.Worklinks[0].ProjectID = "other-project"
			case "no_tasks":
				s.Tasks = nil
				want = fgUniqueEmpty
			case "owner_only":
				s.Tasks[0].Owner = "workstation-c/claude/thread"
				s.Tasks[0].SessionID = "session"
				s.Tasks[0].Note = fgUniqueJSON(t, s.Worklinks[0])
				s.Worklinks = nil
			case "empty_owner_with_binding":
				want = fgUniqueOne
			case "different_repo_valid_binding":
				s.Worklinks[0].Repo = "another-repo"
				want = fgUniqueOne
			}
			fgUniqueAssert(t, reg, s, e, want)
		})
	}
}

func TestFGUniqueBindingScope(t *testing.T) {
	reg := fgUniqueRegistry(t)
	for _, mode := range []string{"execution_subtree", "acceptance_dependency_closure", "project", "", "unknown"} {
		for _, complete := range []bool{true, false} {
			t.Run(mode+"/complete="+fgUniqueJSON(t, complete), func(t *testing.T) {
				s, e := fgUniqueInput(t)
				s.Scope.Mode, s.Scope.Complete = mode, complete
				if complete && (mode == "execution_subtree" || mode == "acceptance_dependency_closure") {
					fgUniqueAssert(t, reg, s, e, fgUniqueOne)
					return
				}
				got, err := RunFGUniqueBindingCheck(context.Background(), reg, s, e)
				if err == nil || !strings.Contains(err.Error(), "complete scoped snapshot required") || !reflect.DeepEqual(got, FGUniqueBindingCheckResult{}) {
					t.Fatalf("refusal = (%+v, %v), want zero result and complete scoped snapshot required", got, err)
				}
			})
		}
	}
	t.Run("partial_collections", func(t *testing.T) {
		s, e := fgUniqueInput(t)
		s.Complete = false
		got, err := RunFGUniqueBindingCheck(context.Background(), reg, s, e)
		if err == nil || !strings.Contains(err.Error(), "complete scoped snapshot required") || !reflect.DeepEqual(got, FGUniqueBindingCheckResult{}) {
			t.Fatalf("partial snapshot accepted: (%+v, %v)", got, err)
		}
	})
	t.Run("incomplete_empty_scope", func(t *testing.T) {
		s, e := fgUniqueInput(t)
		s.Scope.Complete = false
		s.Tasks = nil
		got, err := RunFGUniqueBindingCheck(context.Background(), reg, s, e)
		if err == nil || !strings.Contains(err.Error(), "complete scoped snapshot required") || !reflect.DeepEqual(got, FGUniqueBindingCheckResult{}) {
			t.Fatalf("incomplete empty snapshot accepted: (%+v, %v)", got, err)
		}
	})
	for _, collection := range []string{"tasks", "worklinks"} {
		t.Run("last_page_is_not_whole_"+collection, func(t *testing.T) {
			s, e := fgUniqueInput(t)
			// A final native page can have complete=true after collecting two
			// rows cumulatively while carrying only the last row locally.
			state := CollectionState{Returned: 1, Collected: 2, AuthorizedTotal: 2, Complete: true}
			if collection == "tasks" {
				s.Collections.Tasks = state
			} else {
				s.Collections.Worklinks = state
			}
			got, err := RunFGUniqueBindingCheck(context.Background(), reg, s, e)
			if err == nil || !strings.Contains(err.Error(), "complete scoped snapshot required") || !reflect.DeepEqual(got, FGUniqueBindingCheckResult{}) {
				t.Fatalf("last page accepted as full inventory: (%+v, %v)", got, err)
			}
		})
	}
}

func TestFGUniqueBindingReplay(t *testing.T) {
	reg := fgUniqueRegistry(t)
	s, e := fgUniqueInput(t)
	baseTask, baseLink, evidence := s.Tasks[0], s.Worklinks[0], e["LNK-b"]
	s.Tasks, s.Worklinks = nil, nil
	for _, id := range []string{"Z-ONE", "M-ZERO", "A-DUPLICATE"} {
		task := baseTask
		task.TaskID = id
		s.Tasks = append(s.Tasks, task)
	}
	for _, entry := range []struct{ task, link string }{{"Z-ONE", "LNK-z"}, {"A-DUPLICATE", "LNK-b"}, {"A-DUPLICATE", "LNK-a"}} {
		w := baseLink
		w.TaskID, w.ArtifactID = entry.task, entry.link
		s.Worklinks = append(s.Worklinks, w)
		e[entry.link] = evidence
	}
	want := `{"passed":false,"tasks":[{"task_id":"A-DUPLICATE","class":"more_than_one","count":2,"link_ids":["LNK-a","LNK-b"]},{"task_id":"M-ZERO","class":"zero","count":0,"link_ids":[]},{"task_id":"Z-ONE","class":"exactly_one","count":1,"link_ids":["LNK-z"]}]}`
	t.Run("permutations", func(t *testing.T) {
		for range 20 {
			fgUniqueAssert(t, reg, s, e, want)
			slices.Reverse(s.Tasks)
			slices.Reverse(s.Worklinks)
		}
	})
	for i := range 8 {
		t.Run("shared_input_"+fgUniqueJSON(t, i), func(t *testing.T) {
			t.Parallel()
			fgUniqueAssert(t, reg, s, e, want)
		})
	}
}

func TestFGUniqueBindingFixture(t *testing.T) {
	reg := fgUniqueRegistry(t)
	const activation = "MVP-ACTIVATE-FLEET-GOVERNANCE-20260909"
	const realBinding = "LNK-b40bff48-32c3-4c35-88f2-e7da736dd765"
	const realDispatch = "LNK-c74fe26b-eb5d-4f03-8d6a-dd1f1bcfc3fd"
	const positive = `{"passed":true,"tasks":[{"task_id":"MVP-ACTIVATE-FLEET-GOVERNANCE-20260909","class":"exactly_one","count":1,"link_ids":["LNK-b40bff48-32c3-4c35-88f2-e7da736dd765"]},{"task_id":"MVP-DISPATCH-THREAD-ROSTER-20260909","class":"exactly_one","count":1,"link_ids":["LNK-00000000-0000-4000-8000-000000000001"]}]}`
	cases := []struct{ name, want string }{
		{"preserved_links_without_synthetic_coordinates", `{"passed":false,"tasks":[{"task_id":"MVP-ACTIVATE-FLEET-GOVERNANCE-20260909","class":"zero","count":0,"link_ids":[]},{"task_id":"MVP-DISPATCH-THREAD-ROSTER-20260909","class":"zero","count":0,"link_ids":[]}]}`},
		{"one_per_task", positive},
		{"one_worktree_per_task", positive},
		{"missing_activation", `{"passed":false,"tasks":[{"task_id":"MVP-ACTIVATE-FLEET-GOVERNANCE-20260909","class":"zero","count":0,"link_ids":[]},{"task_id":"MVP-DISPATCH-THREAD-ROSTER-20260909","class":"exactly_one","count":1,"link_ids":["LNK-00000000-0000-4000-8000-000000000001"]}]}`},
		{"duplicate_activation", `{"passed":false,"tasks":[{"task_id":"MVP-ACTIVATE-FLEET-GOVERNANCE-20260909","class":"more_than_one","count":2,"link_ids":["LNK-00000000-0000-4000-8000-000000000002","LNK-b40bff48-32c3-4c35-88f2-e7da736dd765"]},{"task_id":"MVP-DISPATCH-THREAD-ROSTER-20260909","class":"exactly_one","count":1,"link_ids":["LNK-00000000-0000-4000-8000-000000000001"]}]}`},
		{"invalid_second_binding", positive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var f struct {
				Provenance struct {
					Revision string `json:"source_revision"`
				} `json:"provenance"`
				Snapshot         ProjectSnapshotResult        `json:"snapshot"`
				PositiveBindings []WorklinkRow                `json:"positive_bindings"`
				SecondBinding    WorklinkRow                  `json:"second_binding"`
				Evidence         map[string]FGBindingEvidence `json:"evidence_by_link_id"`
			}
			fgUniqueReadJSON(t, "testdata/fg_unique_binding_scoped_snapshot.json", &f)
			s := f.Snapshot
			if f.Provenance.Revision != "fp1:2026-09-11T14:02:30.609016Z" || s.Revision != f.Provenance.Revision || len(s.Tasks) != 14 || len(s.Worklinks) != 36 || !s.Scope.Complete || s.Scope.RootTaskID != "MVP-LANE-FLEET-GOVERNANCE" {
				t.Fatal("fixture no longer preserves the bounded source roster/revision")
			}
			realIDs := map[string]string{realBinding: "binding", realDispatch: "dispatch"}
			for _, w := range s.Worklinks {
				if kind, ok := realIDs[w.ArtifactID]; ok {
					if w.TaskID != activation || w.ArtifactType != kind || w.CommitSHA != "" || w.Repo != "" {
						t.Fatalf("real source row was relabelled or supplied fictional coordinates: %+v", w)
					}
					delete(realIDs, w.ArtifactID)
				}
			}
			if len(realIDs) != 0 {
				t.Fatalf("fixture lost real rows: %v", realIDs)
			}
			if tc.name != "preserved_links_without_synthetic_coordinates" {
				for _, w := range f.PositiveBindings {
					if _, err := ValidateFGBinding(context.Background(), reg, w, f.Evidence[w.ArtifactID]); err != nil {
						t.Fatalf("synthetic positive control is invalid: %v", err)
					}
					if tc.name == "missing_activation" && w.TaskID == activation {
						continue
					}
					if tc.name == "one_worktree_per_task" {
						w.ArtifactType = "worktree"
					}
					s.Worklinks = slices.DeleteFunc(s.Worklinks, func(old WorklinkRow) bool { return old.ArtifactID == w.ArtifactID })
					s.Worklinks = append(s.Worklinks, w)
				}
			}
			if tc.name == "duplicate_activation" || tc.name == "invalid_second_binding" {
				if _, err := ValidateFGBinding(context.Background(), reg, f.SecondBinding, f.Evidence[f.SecondBinding.ArtifactID]); err != nil {
					t.Fatalf("synthetic duplicate control is invalid: %v", err)
				}
				if tc.name == "invalid_second_binding" {
					f.SecondBinding.CommitSHA = ""
				}
				s.Worklinks = append(s.Worklinks, f.SecondBinding)
			}
			for range 3 {
				fgUniqueAssert(t, reg, s, f.Evidence, tc.want)
			}
		})
	}
}
