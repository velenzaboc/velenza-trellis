package main

import (
	"context"
	"fmt"
	"sort"
)

// FGTaskBindingResult reports one eligible task. Class is exactly_one (pass),
// zero (missing binding), or more_than_one (duplicate binding). LinkIDs lists
// only valid active bindings, in ascending order; zero has an empty list.
type FGTaskBindingResult struct {
	TaskID  string   `json:"task_id"`
	Class   string   `json:"class"`
	Count   int      `json:"count"`
	LinkIDs []string `json:"link_ids"`
}

// FGUniqueBindingCheckResult is ordered by task ID. Passed requires exactly
// one valid binding for every in_progress task with a non-empty repo.
type FGUniqueBindingCheckResult struct {
	Passed bool                  `json:"passed"`
	Tasks  []FGTaskBindingResult `json:"tasks"`
}

// RunFGUniqueBindingCheck evaluates one complete scoped project snapshot.
// Its worklinks must be the active rows supplied by task_graph_project_snapshot,
// whose native query excludes archived links. Evidence is explicit, keyed by
// artifact_id within that project; absent evidence never falls back to owner,
// session, branch, timestamps, or free text. The registry is already loaded and
// stable, as required by ValidateFGBinding. No I/O or input mutation occurs.
//
// Incomplete/unscoped inputs return an error and no usable result. Individual
// invalid links do not count. A nil registry makes every binding invalid under
// the prerequisite validator's contract, so eligible tasks report zero.
func RunFGUniqueBindingCheck(ctx context.Context, reg ArtifactKindRegistry, snapshot ProjectSnapshotResult, evidence map[string]FGBindingEvidence) (FGUniqueBindingCheckResult, error) {
	if !snapshot.Scope.Complete || !snapshot.Complete ||
		(snapshot.Scope.Mode != snapshotScopeExecutionSubtree && snapshot.Scope.Mode != snapshotScopeAcceptanceDependencyClosure) ||
		snapshot.Collections.Tasks.AuthorizedTotal > len(snapshot.Tasks) ||
		snapshot.Collections.Worklinks.AuthorizedTotal > len(snapshot.Worklinks) {
		return FGUniqueBindingCheckResult{}, fmt.Errorf("fleet unique binding: complete scoped snapshot required")
	}

	type taskKey struct{ project, task string }
	links := make(map[taskKey][]WorklinkRow)
	for _, row := range snapshot.Worklinks {
		key := taskKey{row.ProjectID, row.TaskID}
		links[key] = append(links[key], row)
	}
	out := FGUniqueBindingCheckResult{Passed: true, Tasks: make([]FGTaskBindingResult, 0)}
	for _, task := range snapshot.Tasks {
		if task.Status != "in_progress" || task.Repo == "" {
			continue
		}
		ids := make([]string, 0)
		for _, row := range links[taskKey{task.ProjectID, task.TaskID}] {
			if reg == nil {
				continue
			}
			kind, found := reg.Canonicalize(ctx, row.ArtifactType)
			if !found || (kind != "worktree" && kind != "binding") {
				continue
			}
			if _, err := ValidateFGBinding(ctx, reg, row, evidence[row.ArtifactID]); err != nil {
				continue
			}
			ids = append(ids, row.ArtifactID)
		}
		sort.Strings(ids)
		class := "exactly_one"
		if len(ids) == 0 {
			class = "zero"
		} else if len(ids) > 1 {
			class = "more_than_one"
		}
		out.Passed = out.Passed && len(ids) == 1
		out.Tasks = append(out.Tasks, FGTaskBindingResult{TaskID: task.TaskID, Class: class, Count: len(ids), LinkIDs: ids})
	}
	sort.Slice(out.Tasks, func(i, j int) bool { return out.Tasks[i].TaskID < out.Tasks[j].TaskID })
	return out, nil
}
