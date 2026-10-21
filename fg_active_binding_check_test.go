package main

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// Reuses the fgUnique* fixtures from the prerequisite check's tests; expectations
// below are literal JSON so they share no classification logic with production.

const fgActiveRealOwner = "01a08d4c-934a-7bb2-8b03-727ff88d7092"

func fgActiveInput(t *testing.T) (ProjectSnapshotResult, map[string]FGBindingEvidence) {
	t.Helper()
	s, e := fgUniqueInput(t)
	s.Tasks[0].Owner = fgActiveRealOwner
	return s, e
}

func fgActiveAssert(t *testing.T, reg ArtifactKindRegistry, snapshot ProjectSnapshotResult, evidence map[string]FGBindingEvidence, want string) {
	t.Helper()
	beforeSnapshot, beforeEvidence := fgUniqueJSON(t, snapshot), fgUniqueJSON(t, evidence)
	got, err := RunFGActiveBindingCheck(context.Background(), reg, snapshot, evidence)
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

func fgActiveRefused(t *testing.T, reg ArtifactKindRegistry, snapshot ProjectSnapshotResult, evidence map[string]FGBindingEvidence, contains string) {
	t.Helper()
	got, err := RunFGActiveBindingCheck(context.Background(), reg, snapshot, evidence)
	if err == nil || !strings.Contains(err.Error(), contains) || !reflect.DeepEqual(got, FGActiveBindingCheckResult{}) {
		t.Fatalf("refusal = (%+v, %v), want zero result and %q", got, err, contains)
	}
}

const (
	fgActivePass    = `{"passed":true,"tasks":[{"task_id":"TASK","binding_class":"exactly_one","count":1,"link_ids":["LNK-b"],"findings":[]}]}`
	fgActiveNoLink  = `{"passed":false,"tasks":[{"task_id":"TASK","binding_class":"zero","count":0,"link_ids":[],"findings":["in_progress_without_current_link"]}]}`
	fgActivePlace   = `{"passed":false,"tasks":[{"task_id":"TASK","binding_class":"exactly_one","count":1,"link_ids":["LNK-b"],"findings":["placeholder_owner"]}]}`
	fgActiveDup     = `{"passed":false,"tasks":[{"task_id":"TASK","binding_class":"more_than_one","count":2,"link_ids":["LNK-a","LNK-b"],"findings":["multiple_current_links"]}]}`
	fgActiveDupPlac = `{"passed":false,"tasks":[{"task_id":"TASK","binding_class":"more_than_one","count":2,"link_ids":["LNK-a","LNK-b"],"findings":["multiple_current_links","placeholder_owner"]}]}`
	fgActiveEmpty   = `{"passed":true,"tasks":[]}`
)

func TestFGActiveBindingPositive(t *testing.T) {
	reg := fgUniqueRegistry(t)
	s, e := fgActiveInput(t)
	fgActiveAssert(t, reg, s, e, fgActivePass)
	// The worktree kind and the binding kind both count as a current link.
	s.Worklinks[0].ArtifactType = "binding"
	fgActiveAssert(t, reg, s, e, fgActivePass)
}

func TestFGActiveBindingMissingLink(t *testing.T) {
	reg := fgUniqueRegistry(t)
	t.Run("no_worklinks", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Worklinks = nil
		fgActiveAssert(t, reg, s, e, fgActiveNoLink)
	})
	t.Run("only_checkpoint_link", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Worklinks[0].ArtifactType = "checkpoint"
		fgActiveAssert(t, reg, s, e, fgActiveNoLink)
	})
	t.Run("malformed_link_does_not_count", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Worklinks[0].CommitSHA = ""
		fgActiveAssert(t, reg, s, e, fgActiveNoLink)
	})
	t.Run("missing_evidence_does_not_count", func(t *testing.T) {
		s, _ := fgActiveInput(t)
		fgActiveAssert(t, reg, s, nil, fgActiveNoLink)
	})
	t.Run("nil_registry_counts_nothing", func(t *testing.T) {
		s, e := fgActiveInput(t)
		fgActiveAssert(t, nil, s, e, fgActiveNoLink)
	})
	t.Run("link_of_another_task", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Worklinks[0].TaskID = "OTHER"
		fgActiveAssert(t, reg, s, e, fgActiveNoLink)
	})
	// Placeholder owner is only judged for a task that has a valid link.
	t.Run("placeholder_owner_without_link_reports_only_missing_link", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Tasks[0].Owner = "LANE-FLEET-GOVERNANCE"
		s.Worklinks = nil
		fgActiveAssert(t, reg, s, e, fgActiveNoLink)
	})
}

func TestFGActiveBindingPlaceholderOwner(t *testing.T) {
	reg := fgUniqueRegistry(t)
	for _, owner := range []string{"UNOWNED", "LANE-FLEET-GOVERNANCE", "LANE-", "LANE-x", "FleetTaskGraph"} {
		t.Run("placeholder="+owner, func(t *testing.T) {
			t.Parallel()
			s, e := fgActiveInput(t)
			s.Tasks[0].Owner = owner
			fgActiveAssert(t, reg, s, e, fgActivePlace)
		})
	}
	for _, owner := range []string{"", fgActiveRealOwner, "unowned", "Unowned", "lane-x", "LANE", "XLANE-A", "FleetTaskGraphX", "fleettaskgraph", " UNOWNED", "UNOWNED ", "workstation-c/claude/thread"} {
		t.Run("real="+owner, func(t *testing.T) {
			t.Parallel()
			s, e := fgActiveInput(t)
			s.Tasks[0].Owner = owner
			fgActiveAssert(t, reg, s, e, fgActivePass)
		})
	}
	t.Run("owner_is_not_inferred_from_link", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Tasks[0].Owner = ""
		s.Tasks[0].SessionID = "UNOWNED"
		s.Worklinks[0].Thread = "UNOWNED"
		e2 := e["LNK-b"]
		s.Worklinks[0].Note = "UNOWNED"
		e["LNK-b"] = e2
		fgActiveAssert(t, reg, s, e, fgActivePass)
	})
}

func TestFGActiveBindingDuplicateLinks(t *testing.T) {
	reg := fgUniqueRegistry(t)
	dup := func(t *testing.T) (ProjectSnapshotResult, map[string]FGBindingEvidence) {
		s, e := fgActiveInput(t)
		w := s.Worklinks[0]
		w.ArtifactID = "LNK-a"
		s.Worklinks = append(s.Worklinks, w)
		e["LNK-a"] = e["LNK-b"]
		return s, e
	}
	t.Run("two_valid_links_fail", func(t *testing.T) {
		s, e := dup(t)
		fgActiveAssert(t, reg, s, e, fgActiveDup)
	})
	t.Run("duplicate_with_placeholder_owner_reports_both", func(t *testing.T) {
		s, e := dup(t)
		s.Tasks[0].Owner = "UNOWNED"
		fgActiveAssert(t, reg, s, e, fgActiveDupPlac)
	})
	t.Run("invalid_second_link_does_not_make_duplicate", func(t *testing.T) {
		s, e := dup(t)
		s.Worklinks[1].CommitSHA = "short"
		fgActiveAssert(t, reg, s, e, fgActivePass)
	})
}

func TestFGActiveBindingEligibility(t *testing.T) {
	reg := fgUniqueRegistry(t)
	for _, status := range []string{"in_progress", "not_started", "blocked", "in_review", "done", "cancelled", "archived", "", "IN_PROGRESS", " in_progress"} {
		for _, repo := range []string{"task-graph", ""} {
			t.Run(status+"/repo="+repo, func(t *testing.T) {
				t.Parallel()
				s, e := fgActiveInput(t)
				s.Tasks[0].Status, s.Tasks[0].Repo, s.Tasks[0].Owner = status, repo, "UNOWNED"
				s.Worklinks = nil
				want := fgActiveEmpty
				if status == "in_progress" && repo != "" {
					want = fgActiveNoLink
				}
				fgActiveAssert(t, reg, s, e, want)
			})
		}
	}
	t.Run("ineligible_task_with_placeholder_and_valid_link_is_omitted", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Tasks[0].Status, s.Tasks[0].Owner = "done", "UNOWNED"
		fgActiveAssert(t, reg, s, e, fgActiveEmpty)
	})
}

func TestFGActiveBindingRefusals(t *testing.T) {
	reg := fgUniqueRegistry(t)
	const scope = "complete scoped snapshot required"
	for _, mode := range []string{"execution_subtree", "acceptance_dependency_closure", "project", "", "unknown"} {
		for _, complete := range []bool{true, false} {
			t.Run(mode+"/complete="+fgUniqueJSON(t, complete), func(t *testing.T) {
				s, e := fgActiveInput(t)
				s.Scope.Mode, s.Scope.Complete = mode, complete
				if complete && (mode == "execution_subtree" || mode == "acceptance_dependency_closure") {
					fgActiveAssert(t, reg, s, e, fgActivePass)
					return
				}
				fgActiveRefused(t, reg, s, e, scope)
			})
		}
	}
	t.Run("partial_collections", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Complete = false
		fgActiveRefused(t, reg, s, e, scope)
	})
	t.Run("incomplete_empty_scope", func(t *testing.T) {
		s, e := fgActiveInput(t)
		s.Scope.Complete, s.Tasks = false, nil
		fgActiveRefused(t, reg, s, e, scope)
	})
	for _, collection := range []string{"tasks", "worklinks"} {
		t.Run("last_page_is_not_whole_"+collection, func(t *testing.T) {
			s, e := fgActiveInput(t)
			state := CollectionState{Returned: 1, Collected: 2, AuthorizedTotal: 2, Complete: true}
			if collection == "tasks" {
				s.Collections.Tasks = state
			} else {
				s.Collections.Worklinks = state
			}
			fgActiveRefused(t, reg, s, e, scope)
		})
	}
	// Two eligible rows sharing one task ID make the owner lookup ambiguous.
	t.Run("duplicate_eligible_task_id", func(t *testing.T) {
		s, e := fgActiveInput(t)
		other := s.Tasks[0]
		other.Owner = "UNOWNED"
		s.Tasks = append(s.Tasks, other)
		fgActiveRefused(t, reg, s, e, "duplicate task")
	})
	t.Run("duplicate_ineligible_task_id_is_ignored", func(t *testing.T) {
		s, e := fgActiveInput(t)
		other := s.Tasks[0]
		other.Status, other.Owner = "done", "UNOWNED"
		s.Tasks = append(s.Tasks, other)
		fgActiveAssert(t, reg, s, e, fgActivePass)
	})
}

func TestFGActiveBindingReplay(t *testing.T) {
	reg := fgUniqueRegistry(t)
	s, e := fgActiveInput(t)
	baseTask, baseLink, evidence := s.Tasks[0], s.Worklinks[0], e["LNK-b"]
	s.Tasks, s.Worklinks = nil, nil
	for _, task := range []struct{ id, owner string }{{"Z-ONE", fgActiveRealOwner}, {"M-ZERO", "UNOWNED"}, {"A-DUP", "FleetTaskGraph"}, {"B-PLACE", "LANE-X"}} {
		tk := baseTask
		tk.TaskID, tk.Owner = task.id, task.owner
		s.Tasks = append(s.Tasks, tk)
	}
	for _, entry := range []struct{ task, link string }{{"Z-ONE", "LNK-z"}, {"A-DUP", "LNK-b"}, {"A-DUP", "LNK-a"}, {"B-PLACE", "LNK-p"}} {
		w := baseLink
		w.TaskID, w.ArtifactID = entry.task, entry.link
		s.Worklinks = append(s.Worklinks, w)
		e[entry.link] = evidence
	}
	want := `{"passed":false,"tasks":[` +
		`{"task_id":"A-DUP","binding_class":"more_than_one","count":2,"link_ids":["LNK-a","LNK-b"],"findings":["multiple_current_links","placeholder_owner"]},` +
		`{"task_id":"B-PLACE","binding_class":"exactly_one","count":1,"link_ids":["LNK-p"],"findings":["placeholder_owner"]},` +
		`{"task_id":"M-ZERO","binding_class":"zero","count":0,"link_ids":[],"findings":["in_progress_without_current_link"]},` +
		`{"task_id":"Z-ONE","binding_class":"exactly_one","count":1,"link_ids":["LNK-z"],"findings":[]}]}`
	t.Run("permutations", func(t *testing.T) {
		for range 20 {
			fgActiveAssert(t, reg, s, e, want)
			slices.Reverse(s.Tasks)
			slices.Reverse(s.Worklinks)
		}
	})
	for i := range 8 {
		t.Run("shared_input_"+fgUniqueJSON(t, i), func(t *testing.T) {
			t.Parallel()
			fgActiveAssert(t, reg, s, e, want)
		})
	}
}

// The two 2026-09-11 historical cases, constructed from the task note: an
// in_progress task with zero worktree links, and a task whose only current link
// is valid while its owner is a LANE-* placeholder.
func TestFGActiveBindingHistoricalCases(t *testing.T) {
	reg := fgUniqueRegistry(t)
	s, e := fgActiveInput(t)
	link := s.Worklinks[0]
	s.Tasks = []TaskRow{
		{ProjectID: "arbor", TaskID: "MVP-DISPATCH-THREAD-ROSTER-20260909", Status: "in_progress", Repo: "task-graph", Owner: fgActiveRealOwner},
		{ProjectID: "arbor", TaskID: "MVP-ACC-GRAPH-HYGIENE", Status: "in_progress", Repo: "task-graph", Owner: "LANE-FLEET-GOVERNANCE"},
	}
	link.TaskID = "MVP-ACC-GRAPH-HYGIENE"
	s.Worklinks = []WorklinkRow{link}
	want := `{"passed":false,"tasks":[` +
		`{"task_id":"MVP-ACC-GRAPH-HYGIENE","binding_class":"exactly_one","count":1,"link_ids":["LNK-b"],"findings":["placeholder_owner"]},` +
		`{"task_id":"MVP-DISPATCH-THREAD-ROSTER-20260909","binding_class":"zero","count":0,"link_ids":[],"findings":["in_progress_without_current_link"]}]}`
	fgActiveAssert(t, reg, s, e, want)
}
