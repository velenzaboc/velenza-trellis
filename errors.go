package main

import (
	"errors"
	"fmt"
)

// Sentinel errors. Each one tells the caller what's pending and where to look
// for the design decision that would unblock it.

var (
	// ErrSchemaPending — the column schema for the substrate this verb would
	// touch is still being designed. The verb's orchestration shape is real;
	// the column-level write is not yet defined.
	ErrSchemaPending = errors.New("schema pending: column-level design not finalized in Node Identity & Admission spec")

	// ErrSubstrateNotProvisioned — the bottom-tier substrate exists in the
	// design but its DDL has not been applied.
	ErrSubstrateNotProvisioned = errors.New("substrate not yet provisioned")

	// ErrRegistryNotLoaded — a closed-set registry is not yet populated.
	ErrRegistryNotLoaded = errors.New("registry not yet loaded")

	// ErrUnknownNodeKind — node_kind value not found in the registry.
	ErrUnknownNodeKind = errors.New("unknown node_kind")

	// ErrUnknownEdgeLabel — edge_label not found in the registry.
	ErrUnknownEdgeLabel = errors.New("unknown edge_label")

	// ErrReciprocityViolation — proposed edge would violate the reciprocity
	// rule the edge_label declares.
	ErrReciprocityViolation = errors.New("reciprocity violation")

	// ErrClosureWouldDangle — archive refused because dependents would orphan.
	ErrClosureWouldDangle = errors.New("archive refused: closure would dangle")

	// ErrContractDrift — re-admission's contract does not validate against the
	// prior version's contract (Vision Principle 11 invariant).
	ErrContractDrift = errors.New("contract drift: re-admission contract incompatible with prior")

	// ErrCeremonyAtomicityBroken — step inside the 10-step ceremony failed
	// after the transaction was opened; transaction aborted.
	ErrCeremonyAtomicityBroken = errors.New("admission ceremony aborted")

	// ErrSnapshotNotPinned — read verb invoked without a snapshot pin and the
	// active snapshot resolver returned no eligible snapshot.
	ErrSnapshotNotPinned = errors.New("no snapshot pinned and no active snapshot available")

	// ErrModeMismatch — verb invoked in a mode that doesn't support it.
	// REQUIREMENTS.md §7.
	ErrModeMismatch = errors.New("verb not available in this mode")

	// ErrForbiddenDatabaseTarget — spec-graph init or reset invoked against a
	// database whose name doesn't match the corpus-graph-* allowlist.
	// REQUIREMENTS.md §7.
	ErrForbiddenDatabaseTarget = errors.New("forbidden database target: only corpus-graph-* names are allowed for init/reset")

	// ErrSchemaNotApplied — any verb invoked before spec-graph init has
	// applied the schema. REQUIREMENTS.md §7.
	ErrSchemaNotApplied = errors.New("schema not yet applied — run `spec-graph init`")

	// ErrImportDependencyUnresolved — spec_graph_import cannot topologically
	// order the submissions because of a cycle or missing referent.
	// REQUIREMENTS.md §7.
	ErrImportDependencyUnresolved = errors.New("import dependency unresolved: cycle or missing referent")

	// ErrSchemaFingerprintMismatch — pre-drop guard refused because the
	// target database's schema does not match the task-graph experimental
	// shape exactly. Catches the case where the allowlist passes
	// (config-driven, can be edited to permit anything) but the database
	// at the configured spanner_database is actually a different system —
	// rather than this graph's Nodes / Edges / ArtifactAdmissionLog tables.
	ErrSchemaFingerprintMismatch = errors.New("schema fingerprint mismatch: target database does not look like a task-graph experimental DB")

	// ErrEdgeKindMismatch — edge admission refused because a resolved
	// endpoint's node_kind is not in the edge_label's allowed kind set.
	// Source-side: src.node_kind not in desc.source_kinds. Target-side:
	// tgt.node_kind not in desc.target_kinds. Reciprocal pairs are
	// validated in both directions so an asymmetric config cannot half-admit.
	ErrEdgeKindMismatch = errors.New("edge endpoint kind mismatch: resolved node_kind not in edge_label's allowed kind set")

	// ErrEdgeSelfLoopForbidden — edge admission refused because source_id
	// equals target_id and the edge_label declares forbid_self_loops=true.
	// Per-label policy declared in config (no binary default).
	ErrEdgeSelfLoopForbidden = errors.New("self-loop forbidden by edge_label policy: source_id equals target_id")

	// ErrDuplicateCanonicalName — Layer-2 dedup refused: a node already
	// exists in the same organization + node_kind with the same
	// canonical_name. The (node_kind, canonical_name) pair is the logical
	// primary key for declaration-bearing kinds per AUTHORING_GUIDE §2
	// zero_duplication. Caller should either spec_graph_update the existing
	// node, choose a different canonical_name, or link to the existing one
	// via edges instead of re-declaring it.
	ErrDuplicateCanonicalName = errors.New("duplicate canonical_name: a node with this (node_kind, canonical_name) already exists in this organization")

	// ErrPossibleDuplicate — Layer-3 dedup refused: embedding-similarity
	// search surfaced one or more existing nodes within the configured
	// cosine-distance threshold that were NOT in the submission's
	// acknowledge_distinct_from list. Caller must examine each candidate
	// (via spec_graph_get) and either acknowledge it as distinct (add to
	// acknowledge_distinct_from and re-submit) or use the existing node
	// instead of creating a new one. The wrapping PossibleDuplicateDetail
	// carries the candidate list.
	ErrPossibleDuplicate = errors.New("possible duplicate: similar node(s) already exist; acknowledge them as distinct or use the existing one")

	// -------- P3-GT-FP-REGISTRIES: task-plan closed-registry errors --------
	//
	// Tasks.status, TaskDeps.kind and TaskLinks.artifact_type were free-text
	// columns with no registry backing them — see registries.go for the
	// closed task_statuses / dependency_kinds / artifact_kinds registries
	// (config-driven, same shape as NodeKindRegistry/EdgeLabelRegistry) that
	// close them. These errors are returned by the task-plan-native write
	// paths (fleet_plan_native.go) once a proposed value fails registry
	// resolution, transition-legality, endpoint, or uniqueness/acyclicity
	// checks. NEW writes must supply an already-canonical value; a legacy
	// free-text literal is reconciled for READS via each descriptor's
	// `absorbs` list (Canonicalize), never auto-translated at write time —
	// see the reconciliation table in registries.go / task-graph.config.json.

	// ErrUnknownTaskStatus — Tasks.status value not found in the closed
	// task_statuses registry.
	ErrUnknownTaskStatus = errors.New("unknown task status")

	// ErrIllegalStatusTransition — the proposed status is a member of the
	// closed registry but is not in the current status's declared
	// legal_next set (and is not a same-status no-op).
	ErrIllegalStatusTransition = errors.New("illegal task status transition")

	// ErrUnknownDependencyKind — TaskDeps.kind value not found in the closed
	// dependency_kinds registry.
	ErrUnknownDependencyKind = errors.New("unknown dependency kind")

	// ErrIllegalDependencyTransition — the proposed TaskDeps.kind is a
	// member of the closed registry but is not in the current kind's
	// declared legal_next set (and is not a same-kind no-op).
	ErrIllegalDependencyTransition = errors.New("illegal dependency kind transition")

	// ErrDependencySelfLoop — a TaskDeps row was proposed with
	// task_id == depends_on_task_id. Forbidden for every dependency_kinds
	// member per registries.go dependencyKindForbidsSelfLoops (there is no
	// per-kind opt-out today — every closed member forbids it).
	ErrDependencySelfLoop = errors.New("dependency self-loop forbidden: task_id equals depends_on_task_id")

	// ErrDependencyCycle — the proposed TaskDeps row would close a cycle in
	// the active blocking-dependency graph (a dependency_kinds member whose
	// blocking=true). Returned with the discovered cycle path in the error
	// text. Non-blocking kinds (e.g. source_map) are exempt — see
	// registries.go wouldCreateDependencyCycle.
	ErrDependencyCycle = errors.New("dependency cycle refused")

	// ErrDependencyKindConflict — an ACTIVE (non-lifecycle-marker) TaskDeps
	// row already exists for the ordered (task_id, depends_on_task_id) pair
	// under a DIFFERENT kind than the one now being requested. Uniqueness is
	// enforced per ordered pair, not per (pair, kind) — see
	// nativeInsertDep. Archive the existing edge first (or use its existing
	// kind) rather than silently overwriting it.
	ErrDependencyKindConflict = errors.New("dependency kind conflict: an active dependency already exists for this pair under a different kind")

	// ErrUnknownArtifactKind — TaskLinks.artifact_type value not found in
	// the closed artifact_kinds registry. New writes must supply an
	// already-canonical member name; see registries.go ArtifactKindRegistry.
	ErrUnknownArtifactKind = errors.New("unknown artifact kind")

	ErrTaskClaimCollision = errors.New("task claim collision: task is already owned")
)

type TaskClaimCollisionDetail struct {
	TaskID    string
	Owner     string
	SessionID string
	Repo      string
}

func (d TaskClaimCollisionDetail) Error() string {
	return fmt.Sprintf("%s: task=%q owner=%q session_id=%q repo=%q", ErrTaskClaimCollision, d.TaskID, d.Owner, d.SessionID, d.Repo)
}

func (d TaskClaimCollisionDetail) Unwrap() error { return ErrTaskClaimCollision }

// DuplicateCanonicalNameDetail wraps ErrDuplicateCanonicalName with the
// existing node's id + version so the caller can immediately fetch or update
// it without a second lookup.
type DuplicateCanonicalNameDetail struct {
	NodeKind        string
	CanonicalName   string
	ExistingNodeID  string
	ExistingVersion int
}

func (d DuplicateCanonicalNameDetail) Error() string {
	return fmt.Sprintf("%s: node_kind=%q canonical_name=%q already admitted as node_id=%q (version %d)",
		ErrDuplicateCanonicalName, d.NodeKind, d.CanonicalName, d.ExistingNodeID, d.ExistingVersion)
}

func (d DuplicateCanonicalNameDetail) Unwrap() error { return ErrDuplicateCanonicalName }

// PossibleDuplicateCandidate is one similarity-search hit reported back to the
// caller alongside ErrPossibleDuplicate.
type PossibleDuplicateCandidate struct {
	NodeID         string  `json:"node_id"`
	NodeKind       string  `json:"node_kind"`
	CanonicalName  string  `json:"canonical_name,omitempty"`
	CosineDistance float64 `json:"cosine_distance"`
}

// PossibleDuplicateDetail wraps ErrPossibleDuplicate with the list of
// candidates that breached the configured similarity threshold and were not
// in the submission's acknowledge_distinct_from list. The caller must
// inspect each candidate and either re-submit with the candidate's node_id
// added to acknowledge_distinct_from (proceed with admission) or abandon
// the create in favor of using the existing node.
type PossibleDuplicateDetail struct {
	Threshold  float64                      `json:"threshold"`
	Candidates []PossibleDuplicateCandidate `json:"candidates"`
}

func (d PossibleDuplicateDetail) Error() string {
	return fmt.Sprintf("%s: %d candidate(s) within cosine distance %.4f — re-submit with acknowledge_distinct_from=%v to proceed",
		ErrPossibleDuplicate, len(d.Candidates), d.Threshold, candidateIDs(d.Candidates))
}

func (d PossibleDuplicateDetail) Unwrap() error { return ErrPossibleDuplicate }

func candidateIDs(cs []PossibleDuplicateCandidate) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.NodeID
	}
	return out
}

// SchemaPendingDetail wraps ErrSchemaPending with a structured note about what
// design decision is missing.
type SchemaPendingDetail struct {
	Step      string
	Needs     string
	OwnerSpec string
}

func (d SchemaPendingDetail) Error() string {
	return fmt.Sprintf("%s: %s (needs %s; owner: %s)", ErrSchemaPending, d.Step, d.Needs, d.OwnerSpec)
}

func (d SchemaPendingDetail) Unwrap() error { return ErrSchemaPending }

// SubstrateNotProvisionedDetail names which substrate is missing.
type SubstrateNotProvisionedDetail struct {
	Substrate string
	NeededDDL string
}

func (d SubstrateNotProvisionedDetail) Error() string {
	return fmt.Sprintf("%s: substrate=%q needs DDL %s", ErrSubstrateNotProvisioned, d.Substrate, d.NeededDDL)
}

func (d SubstrateNotProvisionedDetail) Unwrap() error { return ErrSubstrateNotProvisioned }
