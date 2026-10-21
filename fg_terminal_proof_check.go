package main

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// fg_terminal_proof_check.go implements MVP-FG-STATUS-TERMINAL-PROOF-CHECK-20260911:
// a pure, read-only check over a scoped snapshot that:
//
//  1. flags every task whose canonical status (via TaskStatusRegistry.Canonicalize,
//     registries.go) is "done" but carries no ACTIVE worklink of a proof-bearing
//     artifact_type (fgProofKindSet, below) created at or after the task's own
//     creation time, and
//  2. flags every terminal-status parent task that has a non-terminal child
//     (TaskStatusDescriptor.Terminal(), registries.go).
//
// This performs NO bulk closure, NO status writes, and NO product-gate
// judgement — RunFGTerminalProofCheck issues zero I/O and mutates nothing; it
// only classifies an already-read, caller-supplied snapshot and reports.
//
// The proof-kind set is CLOSED and must never be widened to admit "finding"
// or "dispatch": a done task whose only worklinks are dispatch/finding rows
// (e.g. the live row MVP-LANE-RESTART-PROMPT-20260909 — see
// fg_terminal_proof_check_test.go) is a genuine finding, and its remediation
// is a validation-kind (or other proof-set member) worklink recorded by the
// task's own owner route (routed to MVP-GRAPH-WORKLINK-BACKFILL), never a
// change to this set.

// fgProofKindSet is the closed set of canonical artifact_kinds members
// (task-graph.config.json artifact_kinds) that count as proof a "done" task's
// completion is backed by real work product. Every member name below is a
// canonical (non-absorbed) artifact_kinds entry, verified against
// task-graph.config.json as of MVP-FG-STATUS-TERMINAL-PROOF-CHECK-20260911.
var fgProofKindSet = map[string]bool{
	"validation": true,
	"review":     true,
	"evidence":   true,
	"pr":         true,
	"commit":     true,
	"admission":  true,
	"deploy":     true,
	"runtime":    true,
}

// FGTaskSnapshot is one task row from the scoped snapshot this check reads.
// It carries only what this check needs: identity, the raw (possibly legacy,
// non-canonical) status literal as stored, parentage, and creation time.
type FGTaskSnapshot struct {
	TaskID    string
	Status    string
	ParentID  string
	CreatedAt time.Time
}

// FGWorklinkSnapshot is one ACTIVE worklink row from the scoped snapshot.
// Only active worklinks are meaningful proof; an archived worklink
// (task_graph_archive_worklink) must be excluded by the caller assembling the
// snapshot — this check trusts the snapshot's membership and does not
// re-derive archival state itself. Verified against the live query a caller
// would use: nativeSnapshotWorklinksQueryScoped (fleet_plan_native.go)
// already carries `AND COALESCE(artifact_status, 'recorded') != 'archived'`
// server-side, so a caller wiring this check off task_graph_project_snapshot
// can satisfy this contract by construction, with no extra filtering step.
type FGWorklinkSnapshot struct {
	TaskID       string
	ArtifactType string
	CreatedAt    time.Time
}

// FGFindingKind names the two finding shapes this check reports.
type FGFindingKind string

const (
	// FGFindingDoneWithoutProofKind: a canonically-"done" task carries no
	// active worklink of a proof-bearing artifact_type created at or after
	// the task's own creation time (including the zero-worklinks case).
	FGFindingDoneWithoutProofKind FGFindingKind = "done_without_proof_kind"

	// FGFindingTerminalParentNonTerminalChild: a terminal-status task has at
	// least one direct child task whose status is not terminal.
	FGFindingTerminalParentNonTerminalChild FGFindingKind = "terminal_parent_non_terminal_child"
)

// FGFinding is one reported problem. ChildID/ChildStatus are populated only
// for FGFindingTerminalParentNonTerminalChild.
type FGFinding struct {
	Kind        FGFindingKind `json:"kind"`
	TaskID      string        `json:"task_id"`
	Detail      string        `json:"detail"`
	ChildID     string        `json:"child_id,omitempty"`
	ChildStatus string        `json:"child_status,omitempty"`
}

// FGTerminalProofCheckResult is the full, deterministically-ordered output.
type FGTerminalProofCheckResult struct {
	Findings []FGFinding `json:"findings"`
}

// RunFGTerminalProofCheck is pure and read-only over its arguments: no
// Spanner I/O, no writes, no status mutation. Calling it twice on the same
// (tasks, worklinks) input, with the same registries, MUST produce
// byte-identical output — callers may rely on this for replay/caching.
//
// It fails closed (returns an error, reports nothing) when a task's status
// literal or a worklink's artifact_type literal cannot be canonicalized by
// the supplied registry — per registries.go's own fail-closed discipline,
// this check must never silently skip a row it cannot classify.
func RunFGTerminalProofCheck(
	ctx context.Context,
	statusReg TaskStatusRegistry,
	artifactReg ArtifactKindRegistry,
	tasks []FGTaskSnapshot,
	worklinks []FGWorklinkSnapshot,
) (FGTerminalProofCheckResult, error) {
	if statusReg == nil {
		return FGTerminalProofCheckResult{}, fmt.Errorf("%w: RunFGTerminalProofCheck requires a non-nil TaskStatusRegistry", ErrUnknownTaskStatus)
	}
	if artifactReg == nil {
		return FGTerminalProofCheckResult{}, fmt.Errorf("%w: RunFGTerminalProofCheck requires a non-nil ArtifactKindRegistry", ErrUnknownArtifactKind)
	}

	worklinksByTask := make(map[string][]FGWorklinkSnapshot, len(worklinks))
	for _, w := range worklinks {
		worklinksByTask[w.TaskID] = append(worklinksByTask[w.TaskID], w)
	}

	canonicalStatus := make(map[string]string, len(tasks))
	terminalByTask := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		canon, found := statusReg.Canonicalize(ctx, t.Status)
		if !found {
			return FGTerminalProofCheckResult{}, fmt.Errorf(
				"%w: task %q status literal %q is not recognized by any task_statuses member's absorbs list",
				ErrUnknownTaskStatus, t.TaskID, t.Status)
		}
		desc, err := statusReg.Resolve(ctx, canon)
		if err != nil {
			return FGTerminalProofCheckResult{}, err
		}
		canonicalStatus[t.TaskID] = canon
		terminalByTask[t.TaskID] = desc.Terminal()
	}

	var findings []FGFinding

	// Check 1: every canonically-"done" task carries proof.
	for _, t := range tasks {
		if canonicalStatus[t.TaskID] != "done" {
			continue
		}
		links := worklinksByTask[t.TaskID]
		if len(links) == 0 {
			findings = append(findings, FGFinding{
				Kind:   FGFindingDoneWithoutProofKind,
				TaskID: t.TaskID,
				Detail: fmt.Sprintf("task %q is done and carries zero active worklinks", t.TaskID),
			})
			continue
		}

		hasProof := false
		for _, w := range links {
			canonKind, found := artifactReg.Canonicalize(ctx, w.ArtifactType)
			if !found {
				return FGTerminalProofCheckResult{}, fmt.Errorf(
					"%w: worklink on task %q has artifact_type literal %q not recognized by any artifact_kinds member's absorbs list",
					ErrUnknownArtifactKind, t.TaskID, w.ArtifactType)
			}
			if !fgProofKindSet[canonKind] {
				continue
			}
			if w.CreatedAt.Before(t.CreatedAt) {
				continue
			}
			hasProof = true
			break
		}
		if !hasProof {
			findings = append(findings, FGFinding{
				Kind:   FGFindingDoneWithoutProofKind,
				TaskID: t.TaskID,
				Detail: fmt.Sprintf(
					"task %q is done but none of its %d active worklink(s) is both a proof-bearing artifact_type (%s) and created at or after the task's creation time",
					t.TaskID, len(links), sortedProofKindNames()),
			})
		}
	}

	// Check 2: terminal parent with a non-terminal child.
	childrenByParent := make(map[string][]FGTaskSnapshot, len(tasks))
	for _, t := range tasks {
		if t.ParentID != "" {
			childrenByParent[t.ParentID] = append(childrenByParent[t.ParentID], t)
		}
	}
	for _, parent := range tasks {
		if !terminalByTask[parent.TaskID] {
			continue
		}
		children := append([]FGTaskSnapshot(nil), childrenByParent[parent.TaskID]...)
		sort.Slice(children, func(i, j int) bool { return children[i].TaskID < children[j].TaskID })
		for _, child := range children {
			if terminalByTask[child.TaskID] {
				continue
			}
			findings = append(findings, FGFinding{
				Kind:        FGFindingTerminalParentNonTerminalChild,
				TaskID:      parent.TaskID,
				ChildID:     child.TaskID,
				ChildStatus: canonicalStatus[child.TaskID],
				Detail: fmt.Sprintf(
					"parent %q is terminal (canonical status %q) but child %q is non-terminal (canonical status %q)",
					parent.TaskID, canonicalStatus[parent.TaskID], child.TaskID, canonicalStatus[child.TaskID]),
			})
		}
	}

	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Kind != findings[j].Kind {
			return findings[i].Kind < findings[j].Kind
		}
		if findings[i].TaskID != findings[j].TaskID {
			return findings[i].TaskID < findings[j].TaskID
		}
		return findings[i].ChildID < findings[j].ChildID
	})

	return FGTerminalProofCheckResult{Findings: findings}, nil
}

// sortedProofKindNames renders fgProofKindSet's members in stable sorted
// order for deterministic, readable finding text.
func sortedProofKindNames() string {
	names := make([]string, 0, len(fgProofKindSet))
	for k := range fgProofKindSet {
		names = append(names, k)
	}
	sort.Strings(names)
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}
