package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const (
	fgActiveFindingNoLink   = "in_progress_without_current_link"
	fgActiveFindingMultiple = "multiple_current_links"
	fgActiveFindingPlace    = "placeholder_owner"
)

// FGActiveBindingTaskResult reports one eligible task. BindingClass, Count and
// LinkIDs are the prerequisite unique-binding classification, unchanged.
// Findings is empty for a passing task, otherwise a subset of
// in_progress_without_current_link, multiple_current_links and placeholder_owner
// in that order.
type FGActiveBindingTaskResult struct {
	TaskID       string   `json:"task_id"`
	BindingClass string   `json:"binding_class"`
	Count        int      `json:"count"`
	LinkIDs      []string `json:"link_ids"`
	Findings     []string `json:"findings"`
}

// FGActiveBindingCheckResult is ordered by task ID. Passed requires every
// eligible task to have zero findings.
type FGActiveBindingCheckResult struct {
	Passed bool                        `json:"passed"`
	Tasks  []FGActiveBindingTaskResult `json:"tasks"`
}

func fgActivePlaceholderOwner(owner string) bool {
	return owner == "UNOWNED" || owner == "FleetTaskGraph" || strings.HasPrefix(owner, "LANE-")
}

// RunFGActiveBindingCheck composes RunFGUniqueBindingCheck with each eligible
// task's owner. Eligible means in_progress with a non-empty repo. Scope,
// completeness and per-link validity refusals come from the prerequisite check
// unchanged; this check adds only the owner lookup, which is by exact task ID
// and therefore refuses two eligible rows that share one. Placeholder owner is
// judged only for a task with at least one valid link. No I/O or input mutation.
func RunFGActiveBindingCheck(ctx context.Context, reg ArtifactKindRegistry, snapshot ProjectSnapshotResult, evidence map[string]FGBindingEvidence) (FGActiveBindingCheckResult, error) {
	unique, err := RunFGUniqueBindingCheck(ctx, reg, snapshot, evidence)
	if err != nil {
		return FGActiveBindingCheckResult{}, fmt.Errorf("fleet active binding: %w", err)
	}
	owners := make(map[string]string)
	for _, task := range snapshot.Tasks {
		if task.Status != "in_progress" || task.Repo == "" {
			continue
		}
		if _, dup := owners[task.TaskID]; dup {
			return FGActiveBindingCheckResult{}, fmt.Errorf("fleet active binding: duplicate task %q in eligible snapshot rows", task.TaskID)
		}
		owners[task.TaskID] = task.Owner
	}
	out := FGActiveBindingCheckResult{Passed: true, Tasks: make([]FGActiveBindingTaskResult, 0, len(unique.Tasks))}
	for _, bound := range unique.Tasks {
		findings := make([]string, 0)
		switch bound.Class {
		case "zero":
			findings = append(findings, fgActiveFindingNoLink)
		case "more_than_one":
			findings = append(findings, fgActiveFindingMultiple)
		}
		if bound.Count > 0 && fgActivePlaceholderOwner(owners[bound.TaskID]) {
			findings = append(findings, fgActiveFindingPlace)
		}
		out.Passed = out.Passed && len(findings) == 0
		out.Tasks = append(out.Tasks, FGActiveBindingTaskResult{
			TaskID: bound.TaskID, BindingClass: bound.Class, Count: bound.Count, LinkIDs: bound.LinkIDs, Findings: findings,
		})
	}
	sort.Slice(out.Tasks, func(i, j int) bool { return out.Tasks[i].TaskID < out.Tasks[j].TaskID })
	return out, nil
}
