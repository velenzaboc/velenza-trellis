package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
	"github.com/wI2L/jsondiff"
)

// Env carries dependencies for every handler. Constructed in cmdMCP and
// passed by pointer.
type Env struct {
	Config      SpecGraphConfig
	Kinds       NodeKindRegistry
	Edges       EdgeLabelRegistry
	Projections ProjectionRegistry
	Substrates  SubstrateRouter
	Embed       *vertexai.EmbedClient // nil when Config.Embedding is unset
	Now         func() time.Time

	// TaskStatuses / DependencyKinds / ArtifactKinds are the
	// P3-GT-FP-REGISTRIES closed-set registries gating the task-plan-native
	// write paths (fleet_plan_native.go). nil when task-graph.config.json
	// carries no task_statuses/dependency_kinds/artifact_kinds — callers
	// must treat a nil registry the same as ErrUnknownTaskStatus / etc.
	// (fail closed, never skip the check).
	TaskStatuses    TaskStatusRegistry
	DependencyKinds DependencyKindRegistry
	ArtifactKinds   ArtifactKindRegistry

	// resolveNativeAdapter is a narrow orchestration seam for handler tests.
	// Production Env construction leaves it nil and therefore uses the exact
	// soleNativeFleetPlanAdapter implementation. Tests can return (nil, nil)
	// to exercise schema-v1 orchestration through a contract-level adapter
	// without opening a Spanner client.
	resolveNativeAdapter func(context.Context, *Env) (*SpannerSubstrateAdapter, error)

	// SnapshotContinuationKey authenticates project-snapshot continuation
	// coordinates. It is supplied only at process construction; no key is ever
	// returned in an MCP result or stored in a task-plan row.
	SnapshotContinuationKey []byte
	SnapshotContinuationTTL time.Duration
}

func handlerNativeAdapter(ctx context.Context, env *Env) (*SpannerSubstrateAdapter, error) {
	if env != nil && env.resolveNativeAdapter != nil {
		return env.resolveNativeAdapter(ctx, env)
	}
	return soleNativeFleetPlanAdapter(ctx, env)
}

// ---------------- typed inputs (one per verb) ----------------

type useInput struct {
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"snapshot id to pin; omit to report current pin"`
}

type createInput struct {
	NodeKind  string `json:"node_kind" jsonschema:"projection-axis kind: section | symbol | sentinel | invariant | assumption | config | circuit | doc_object | decision | ..."`
	StableID  string `json:"stable_id,omitempty" jsonschema:"omit to mint a new id; provide to re-admit at v_N+1 under the same id"`
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	// Contract / Payload are typed as map[string]interface{} so the MCP
	// inputSchema reflects to {"type":"object","additionalProperties":true}
	// — the canonical "any JSON object" shape Claude Code accepts.
	// Plain interface{} reflects to bare boolean `true`, which strict MCP
	// clients reject.
	Contract      map[string]interface{} `json:"contract" jsonschema:"the stable surface this node promises"`
	Payload       map[string]interface{} `json:"payload" jsonschema:"kind-specific body"`
	ParentRef     string                 `json:"parent_ref,omitempty"`
	GoverningSpec string                 `json:"governing_spec,omitempty"`
	SpecSection   string                 `json:"spec_section,omitempty"`

	// AcknowledgeDistinctFrom carries node_ids the caller has examined and
	// confirmed are NOT duplicates of the proposed node. Used to override
	// Layer-3 (embedding-similarity) dedup when the proposed node really
	// is a distinct concept that happens to be semantically close to an
	// existing one. On the first create call the caller leaves this empty;
	// if the gate refuses with ErrPossibleDuplicate, the caller inspects
	// the returned candidates (via spec_graph_get), confirms distinctness,
	// re-submits with each candidate node_id added here. Layer-1 (definition
	// hash) and Layer-2 (canonical_name) cannot be acknowledged — those are
	// hard collisions that require update / rename / different design.
	AcknowledgeDistinctFrom []string `json:"acknowledge_distinct_from,omitempty" jsonschema:"node_ids the caller has examined and confirmed are not duplicates of the proposed node — bypasses Layer-3 similarity dedup for those specific candidates"`
}

type updateInput struct {
	ProjectID string                 `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	NodeID    string                 `json:"node_id" jsonschema:"stable id of the node to re-admit"`
	Contract  map[string]interface{} `json:"contract" jsonschema:"the stable surface this node promises — MUST be byte-equivalent to the prior version per node_version_chain_axiom"`
	Payload   map[string]interface{} `json:"payload" jsonschema:"kind-specific body"`
}

type archiveInput struct {
	NodeID string `json:"node_id"`
}

type linkInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	SourceID  string `json:"source_id"`
	TargetID  string `json:"target_id"`
	EdgeLabel string `json:"edge_label" jsonschema:"closed-registry edge label"`
}

type unlinkInput struct {
	EdgeID string `json:"edge_id"`
}

type getInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	NodeID    string `json:"node_id"`
	Version   string `json:"version,omitempty"`
}

type listInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	NodeKind  string `json:"node_kind,omitempty"`
	Status    string `json:"status,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type searchInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	Query     string `json:"query"`
	NodeKind  string `json:"node_kind,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type neighborsInput struct {
	ProjectID  string   `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	NodeID     string   `json:"node_id"`
	Depth      int      `json:"depth,omitempty"`
	EdgeLabels []string `json:"edge_labels,omitempty"`
	Direction  string   `json:"direction,omitempty"`
}

type closureInput struct {
	ProjectID      string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	NodeID         string `json:"node_id"`
	ClosureProfile string `json:"closure_profile"`
	MaxDepth       int    `json:"max_depth"`
	ResultCap      int    `json:"result_cap"`
}

type impactInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace for fleet_plan_native; omitted resolves to config.default_project_id"`
	NodeID    string `json:"node_id"`
	MaxDepth  int    `json:"max_depth"`
	ResultCap int    `json:"result_cap"`
}

type historyInput struct {
	NodeID string `json:"node_id"`
}

type diffInput struct {
	NodeID   string `json:"node_id"`
	VersionA string `json:"version_a"`
	VersionB string `json:"version_b"`
}

type projectInput struct {
	NodeID     string `json:"node_id"`
	Projection string `json:"projection"`
}

type queryInput struct {
	SQL    string                 `json:"sql" jsonschema:"GoogleSQL or GQL query — READ-ONLY; SELECT/GRAPH_TABLE/MATCH only"`
	Params map[string]interface{} `json:"params,omitempty"`
}

type importInput struct {
	Submissions []ArtifactSubmission `json:"submissions,omitempty"`
	FilePath    string               `json:"file_path,omitempty" jsonschema:"alternative to submissions: path to a JSON file containing the submissions array, OR a directory whose *.json files are loaded in name order. Leading-underscore filenames (e.g. _edges.json, _findings.json) are skipped — they are author/validator artifacts, not submissions."`
}

type healthInput struct{}

// ---------------- handler bodies ----------------

func handleUse(_ context.Context, _ *Env, in useInput, pin *snapshotPin) (Snapshot, error) {
	if in.SnapshotID == "" {
		return Snapshot{SnapshotID: pin.SnapshotID}, nil
	}
	pin.SnapshotID = in.SnapshotID
	return Snapshot{SnapshotID: in.SnapshotID}, nil
}

func handleCreate(ctx context.Context, env *Env, in createInput) (MutationWriteResult, error) {
	if in.NodeKind == "task" {
		if err := guardReviewNoteWrite(nil, in.Payload["note"]); err != nil { return MutationWriteResult{}, err }
	}
	if in.NodeKind == "artifact" && namesWriterHandoverHistory(stringFromMap(in.Payload, "note")) {
		return MutationWriteResult{}, errWriterHandoverHistory()
	}
	if adapter, err := handlerNativeAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleCreate(ctx, env, in)
	}
	if in.NodeKind == "" {
		return MutationWriteResult{}, fmt.Errorf("node_kind required")
	}
	desc, err := env.Kinds.Resolve(ctx, in.NodeKind)
	if err != nil {
		return MutationWriteResult{}, err
	}
	sub := ArtifactSubmission{
		ArtifactType:   desc.ArtifactType(),
		OrganizationID: env.Config.DefaultOrganizationID,
		NodeKind:       in.NodeKind,
		StableID:       in.StableID,
		Contract:       in.Contract,
		Payload:        in.Payload,
		GoverningSpec:  in.GoverningSpec,
		SpecSection:    in.SpecSection,
	}
	// Pre-admission dedup gate. Runs Layer 1 (definition_hash) →
	// Layer 2 (canonical_name) → Layer 3 (embedding similarity) BEFORE
	// the ceremony writes any row. Skipped entirely when env.Config.Dedup
	// is nil. See dedup.go for the layer design.
	if idempotent, err := runDedupGate(ctx, env, sub, in.AcknowledgeDistinctFrom); err != nil {
		return MutationWriteResult{}, err
	} else if idempotent != nil {
		return admissionWriteResult(*idempotent), nil
	}
	rec, err := AdmitArtifact(ctx, env, sub)
	if err != nil {
		return MutationWriteResult{}, err
	}
	return admissionWriteResult(rec), nil
}

func handleUpdate(ctx context.Context, env *Env, in updateInput) (MutationWriteResult, error) {
	if adapter, err := handlerNativeAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleUpdate(ctx, env, in)
	}
	if in.NodeID == "" {
		return MutationWriteResult{}, fmt.Errorf("node_id required")
	}
	prior, err := readNodeAcrossSubstrates(ctx, env, "", in.NodeID, "", "")
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("read prior version: %w", err)
	}
	if prior.NodeKind == "artifact" && namesWriterHandoverHistory(stringFromMap(in.Payload, "note")) {
		return MutationWriteResult{}, errWriterHandoverHistory()
	}
	// Contract monotonicity is universal per node_version_chain_axiom — every
	// re-admission's contract must be byte-equivalent to the prior version's
	// (only payload may change). Enforced in every mode, including
	// experimental_bootstrap.
	if err := validateContractMonotonicity(prior.CurrentVersion.Contract, in.Contract); err != nil {
		return MutationWriteResult{}, err
	}
	desc, err := env.Kinds.Resolve(ctx, prior.NodeKind)
	if err != nil {
		return MutationWriteResult{}, err
	}
	sub := ArtifactSubmission{
		ArtifactType:   desc.ArtifactType(),
		OrganizationID: env.Config.DefaultOrganizationID,
		NodeKind:       prior.NodeKind,
		StableID:       in.NodeID,
		Contract:       in.Contract,
		Payload:        in.Payload,
		PriorVersion:   &prior.CurrentVersion,
	}
	rec, err := AdmitArtifact(ctx, env, sub)
	if err != nil {
		return MutationWriteResult{}, err
	}
	return admissionWriteResult(rec), nil
}

func handleArchive(ctx context.Context, env *Env, in archiveInput) (AdmissionRecord, error) {
	if in.NodeID == "" {
		return AdmissionRecord{}, fmt.Errorf("node_id required")
	}
	prior, err := readNodeAcrossSubstrates(ctx, env, "", in.NodeID, "", "")
	if err != nil {
		return AdmissionRecord{}, err
	}
	// Dangling-closure check: refuse to archive a node with active dependents.
	if err := checkDanglingClosure(ctx, env, in.NodeID); err != nil {
		return AdmissionRecord{}, err
	}
	desc, err := env.Kinds.Resolve(ctx, prior.NodeKind)
	if err != nil {
		return AdmissionRecord{}, err
	}
	sub := ArtifactSubmission{
		ArtifactType:   desc.ArtifactType(),
		OrganizationID: env.Config.DefaultOrganizationID,
		NodeKind:       prior.NodeKind,
		StableID:       in.NodeID,
		PriorVersion:   &prior.CurrentVersion,
		IsRetirement:   true,
	}
	return AdmitArtifact(ctx, env, sub)
}

// handleLink admits one typed edge and its reciprocal mirror when the label
// declares reciprocity.
func handleLink(ctx context.Context, env *Env, in linkInput) (MutationWriteResult, error) {
	if adapter, err := handlerNativeAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleLink(ctx, env, in)
	}
	if in.SourceID == "" || in.TargetID == "" || in.EdgeLabel == "" {
		return MutationWriteResult{}, fmt.Errorf("source_id, target_id, edge_label required")
	}
	rec, err := admitEdge(ctx, env, in.SourceID, in.TargetID, in.EdgeLabel)
	if err != nil {
		return MutationWriteResult{}, err
	}
	return admissionWriteResult(rec), nil
}

// admitEdge is the schema_v1-native edge admission path shared by handleLink
// and handleImport. It resolves both endpoints via readNodeAcrossSubstrates,
// validates endpoint kinds + self-loop policy
// per validateEdgeEndpoints, then prepares the primary edge plan (and the
// reciprocal mirror plan, when the label declares one) and commits both in
// ONE Spanner ReadWriteTransaction via adapter.CommitAdmission — matching
// REQUIREMENTS.md §2 reciprocity_atomic.
func admitEdge(ctx context.Context, env *Env, sourceID, targetID, edgeLabel string) (AdmissionRecord, error) {
	desc, err := env.Edges.Resolve(ctx, edgeLabel)
	if err != nil {
		return AdmissionRecord{}, err
	}
	orgID := env.Config.DefaultOrganizationID
	if orgID == "" {
		return AdmissionRecord{}, fmt.Errorf("default_organization_id required")
	}

	src, err := readNodeAcrossSubstrates(ctx, env, "", sourceID, "", "")
	if err != nil {
		return AdmissionRecord{}, fmt.Errorf("resolve source %q: %w", sourceID, err)
	}
	tgt, err := readNodeAcrossSubstrates(ctx, env, "", targetID, "", "")
	if err != nil {
		return AdmissionRecord{}, fmt.Errorf("resolve target %q: %w", targetID, err)
	}
	if err := validateEdgeEndpoints(desc, src, tgt); err != nil {
		return AdmissionRecord{}, err
	}

	// Idempotent no-op: Edges' PK is (org, edge_id), not
	// (source, target, label) — a fresh admission mints a new random
	// edge_id regardless, so calling task_graph_link twice with the same
	// endpoints+label would otherwise silently create a duplicate parallel
	// edge. Return the existing edge's record instead of admitting a new one.
	if existing, found, err := findActiveEdge(ctx, env, src.NodeID, tgt.NodeID, edgeLabel); err != nil {
		return AdmissionRecord{}, fmt.Errorf("check existing edge: %w", err)
	} else if found {
		return AdmissionRecord{
			ArtifactType:    "edge",
			OrganizationID:  orgID,
			ArtifactID:      existing.EdgeID,
			ArtifactVersion: 1,
			AdmittedAt:      existing.AdmittedAt,
			AdmittedBy:      env.Config.ExperimentalAdmitter,
		}, nil
	}

	all, err := env.Substrates.All(ctx)
	if err != nil {
		return AdmissionRecord{}, err
	}
	if len(all) == 0 {
		return AdmissionRecord{}, fmt.Errorf("no substrates registered")
	}
	adapter := all[0]

	primaryPlan, err := PrepareAdmissionPlan(ctx, env, ArtifactSubmission{
		ArtifactType:      "edge",
		OrganizationID:    orgID,
		EdgeLabel:         edgeLabel,
		EdgeSourceID:      src.NodeID,
		EdgeSourceVersion: src.CurrentVersion.Version,
		EdgeTargetID:      tgt.NodeID,
		EdgeTargetVersion: tgt.CurrentVersion.Version,
		StableID:          uuid.NewString(),
	})
	if err != nil {
		return AdmissionRecord{}, fmt.Errorf("prepare edge: %w", err)
	}
	plans := []AdmissionPlan{primaryPlan}

	// Reciprocity: when the label declares an inverse, the mirror is
	// admitted in the SAME CommitAdmission transaction with cross-linked
	// ReciprocalEdgeID.
	if mirrorLabel := desc.ReciprocalLabel(); mirrorLabel != "" {
		mirrorDesc, err := env.Edges.Resolve(ctx, mirrorLabel)
		if err != nil {
			return AdmissionRecord{}, fmt.Errorf("resolve reciprocal label %q: %w", mirrorLabel, err)
		}
		if err := validateEdgeEndpoints(mirrorDesc, tgt, src); err != nil {
			return AdmissionRecord{}, fmt.Errorf("mirror direction: %w", err)
		}
		mirrorPlan, err := PrepareAdmissionPlan(ctx, env, ArtifactSubmission{
			ArtifactType:      "edge",
			OrganizationID:    orgID,
			EdgeLabel:         mirrorLabel,
			EdgeSourceID:      tgt.NodeID,
			EdgeSourceVersion: tgt.CurrentVersion.Version,
			EdgeTargetID:      src.NodeID,
			EdgeTargetVersion: src.CurrentVersion.Version,
			StableID:          uuid.NewString(),
		})
		if err != nil {
			return AdmissionRecord{}, fmt.Errorf("prepare reciprocal edge: %w", err)
		}
		plans[0].Edge.ReciprocalEdgeID = mirrorPlan.Edge.EdgeID
		mirrorPlan.Edge.ReciprocalEdgeID = plans[0].Edge.EdgeID
		plans = append(plans, mirrorPlan)
	}

	recs, err := adapter.CommitAdmission(ctx, plans...)
	if err != nil {
		return AdmissionRecord{}, err
	}
	if len(recs) == 0 {
		return AdmissionRecord{}, fmt.Errorf("CommitAdmission returned no records")
	}
	return recs[0], nil
}

func handleUnlink(ctx context.Context, env *Env, in unlinkInput) (MutationWriteResult, error) {
	if in.EdgeID == "" {
		return MutationWriteResult{}, fmt.Errorf("edge_id required")
	}
	if adapter, err := handlerNativeAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleUnlink(ctx, env, in.EdgeID)
	}
	// Read the existing edge first so the retirement submission carries the
	// edge_label + endpoints + versions the ceremony needs to route and audit.
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	var existing Edge
	var found bool
	for _, a := range adapters {
		e, err := a.ReadEdge(ctx, in.EdgeID, "")
		if err == nil {
			existing = e
			found = true
			break
		}
	}
	if !found {
		return MutationWriteResult{}, fmt.Errorf("edge %q not found in any substrate", in.EdgeID)
	}
	sub := ArtifactSubmission{
		ArtifactType:      "edge",
		OrganizationID:    env.Config.DefaultOrganizationID,
		EdgeLabel:         existing.EdgeLabel,
		EdgeSourceID:      existing.SourceID,
		EdgeSourceVersion: existing.SourceVersion,
		EdgeTargetID:      existing.TargetID,
		EdgeTargetVersion: existing.TargetVersion,
		StableID:          in.EdgeID,
		IsRetirement:      true,
	}
	rec, err := AdmitArtifact(ctx, env, sub)
	if err != nil {
		return MutationWriteResult{}, err
	}
	return admissionWriteResult(rec), nil
}

func handleGet(ctx context.Context, env *Env, in getInput, pin *snapshotPin) (Node, error) {
	projectID, err := resolveProjectID(env, in.ProjectID)
	if err != nil {
		return Node{}, err
	}
	return readNodeAcrossSubstrates(ctx, env, projectID, in.NodeID, in.Version, pin.SnapshotID)
}

func handleList(ctx context.Context, env *Env, in listInput, pin *snapshotPin) (NodeListResult, error) {
	if in.Status == "" {
		in.Status = "active"
	}
	if in.Limit <= 0 {
		in.Limit = 50
	}
	projectID, err := resolveProjectID(env, in.ProjectID)
	if err != nil {
		return NodeListResult{}, err
	}
	filter := ListFilter{
		NodeKind:       in.NodeKind,
		Status:         in.Status,
		OrganizationID: env.Config.DefaultOrganizationID,
		ProjectID:      projectID,
		SnapshotID:     pin.SnapshotID,
		Limit:          in.Limit,
	}
	if in.NodeKind != "" {
		desc, err := env.Kinds.Resolve(ctx, in.NodeKind)
		if err != nil {
			return NodeListResult{}, err
		}
		adapter, err := env.Substrates.Adapter(ctx, desc)
		if err != nil {
			return NodeListResult{}, err
		}
		filter.ProjectID, err = resolveProjectIDForAdapter(env, adapter, in.ProjectID)
		if err != nil {
			return NodeListResult{}, err
		}
		ns, err := adapter.ListNodes(ctx, filter)
		return NodeListResult{Nodes: ns}, err
	}
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return NodeListResult{}, err
	}
	out := NodeListResult{}
	for _, a := range adapters {
		adapterFilter := filter
		adapterFilter.ProjectID, err = resolveProjectIDForAdapter(env, a, in.ProjectID)
		if err != nil {
			return out, fmt.Errorf("substrate %q project scope: %w", a.ID(), err)
		}
		ns, err := a.ListNodes(ctx, adapterFilter)
		if err != nil {
			return out, fmt.Errorf("substrate %q list: %w", a.ID(), err)
		}
		out.Nodes = append(out.Nodes, ns...)
	}
	return out, nil
}

func handleSearch(ctx context.Context, env *Env, in searchInput, _ *snapshotPin) (NodeListResult, error) {
	if in.Query == "" {
		return NodeListResult{}, fmt.Errorf("query required")
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 15
	}
	// fleet_plan_native has no node table and no embedding column: search is a
	// Spanner-side ranked term match over the live rows, and must not demand
	// embedding config the native store cannot use.
	if adapter, err := handlerNativeAdapter(ctx, env); err != nil {
		return NodeListResult{}, err
	} else if adapter != nil {
		projectID, err := resolveProjectIDForAdapter(env, adapter, in.ProjectID)
		if err != nil {
			return NodeListResult{}, err
		}
		nodes, err := nativeSearchNodes(ctx, adapter, projectID, in.Query, in.NodeKind, limit)
		if err != nil {
			return NodeListResult{}, err
		}
		return NodeListResult{Nodes: nodes}, nil
	}
	if env.Embed == nil {
		return NodeListResult{}, fmt.Errorf("embedding not configured — set Config.Embedding in spec-graph.config.json to enable spec_graph_search")
	}
	vec, err := env.Embed.Embed(ctx, in.Query, vertexai.TaskTypeRetrievalQuery)
	if err != nil {
		return NodeListResult{}, fmt.Errorf("embed query: %w", err)
	}
	// Bootstrap routes all reads to the single substrate.
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return NodeListResult{}, err
	}
	if len(adapters) == 0 {
		return NodeListResult{}, fmt.Errorf("no substrates registered")
	}
	nodes, err := adapters[0].SearchByEmbedding(ctx, vec, in.NodeKind, limit, env.Config.DefaultOrganizationID)
	if err != nil {
		return NodeListResult{}, err
	}
	return NodeListResult{Nodes: nodes}, nil
}

func handleNeighbors(ctx context.Context, env *Env, in neighborsInput, pin *snapshotPin) (EdgeListResult, error) {
	if in.NodeID == "" {
		return EdgeListResult{}, fmt.Errorf("node_id required")
	}
	if in.Direction == "" {
		in.Direction = "both"
	}
	if in.Depth <= 0 {
		in.Depth = 1
	}
	projectID, err := resolveProjectID(env, in.ProjectID)
	if err != nil {
		return EdgeListResult{}, err
	}
	prior, err := readNodeAcrossSubstrates(ctx, env, projectID, in.NodeID, "", pin.SnapshotID)
	if err != nil {
		return EdgeListResult{}, err
	}
	desc, err := env.Kinds.Resolve(ctx, prior.NodeKind)
	if err != nil {
		return EdgeListResult{}, err
	}
	adapter, err := env.Substrates.Adapter(ctx, desc)
	if err != nil {
		return EdgeListResult{}, err
	}
	projectID, err = resolveProjectIDForAdapter(env, adapter, in.ProjectID)
	if err != nil {
		return EdgeListResult{}, err
	}
	edges, err := adapter.Neighbors(ctx, in.NodeID, NeighborOpts{
		Direction:      in.Direction,
		EdgeLabels:     in.EdgeLabels,
		Depth:          in.Depth,
		SnapshotID:     pin.SnapshotID,
		OrganizationID: env.Config.DefaultOrganizationID,
		ProjectID:      projectID,
	})
	return EdgeListResult{Edges: edges}, err
}

func handleClosure(ctx context.Context, env *Env, in closureInput, _ *snapshotPin) (NodeListResult, error) {
	if in.NodeID == "" {
		return NodeListResult{}, fmt.Errorf("node_id required")
	}
	if in.MaxDepth <= 0 {
		in.MaxDepth = 3
	}
	if in.ResultCap <= 0 {
		in.ResultCap = 100
	}
	labels, err := resolveClosureProfile(ctx, env, in.ClosureProfile)
	if err != nil {
		return NodeListResult{}, err
	}
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return NodeListResult{}, err
	}
	if len(adapters) == 0 {
		return NodeListResult{}, fmt.Errorf("no substrates registered")
	}
	projectID, err := resolveProjectIDForAdapter(env, adapters[0], in.ProjectID)
	if err != nil {
		return NodeListResult{}, err
	}
	nodes, err := adapters[0].TraverseClosure(ctx, projectID, in.NodeID, labels, in.MaxDepth, in.ResultCap, "forward")
	return NodeListResult{Nodes: nodes}, err
}

func handleImpact(ctx context.Context, env *Env, in impactInput, _ *snapshotPin) (NodeListResult, error) {
	if in.NodeID == "" {
		return NodeListResult{}, fmt.Errorf("node_id required")
	}
	if in.MaxDepth <= 0 {
		in.MaxDepth = 3
	}
	if in.ResultCap <= 0 {
		in.ResultCap = 100
	}
	// Impact = all edge labels, reverse direction.
	all, err := env.Edges.All(ctx)
	if err != nil {
		return NodeListResult{}, err
	}
	labels := make([]string, 0, len(all))
	for _, l := range all {
		labels = append(labels, l.Name())
	}
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return NodeListResult{}, err
	}
	if len(adapters) == 0 {
		return NodeListResult{}, fmt.Errorf("no substrates registered")
	}
	projectID, err := resolveProjectIDForAdapter(env, adapters[0], in.ProjectID)
	if err != nil {
		return NodeListResult{}, err
	}
	nodes, err := adapters[0].TraverseClosure(ctx, projectID, in.NodeID, labels, in.MaxDepth, in.ResultCap, "reverse")
	return NodeListResult{Nodes: nodes}, err
}

// resolveClosureProfile maps a closure_profile name to the set of edge_labels
// whose edge_family equals the profile string. The profile string IS the
// edge_family — no Go-resident aliases. Callers pick whichever edge_family
// values they declared in spec-graph.config.json edge_labels and pass that
// literal as closure_profile.
func resolveClosureProfile(ctx context.Context, env *Env, profile string) ([]string, error) {
	if profile == "" {
		return nil, fmt.Errorf("closure_profile required (use the edge_family value declared in spec-graph.config.json edge_labels)")
	}
	all, err := env.Edges.All(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, l := range all {
		if l.EdgeFamily() == profile {
			out = append(out, l.Name())
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("closure_profile %q matched zero edge_labels (no edge_label has edge_family=%q in config)", profile, profile)
	}
	return out, nil
}

func handleHistory(ctx context.Context, env *Env, in historyInput) (VersionListResult, error) {
	if in.NodeID == "" {
		return VersionListResult{}, fmt.Errorf("node_id required")
	}
	// Walk every registered substrate; first one with history wins (bootstrap
	// has one substrate, so this is trivial).
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return VersionListResult{}, err
	}
	for _, a := range adapters {
		vs, err := a.History(ctx, in.NodeID)
		if err == nil && len(vs) > 0 {
			return VersionListResult{Versions: vs}, nil
		}
	}
	return VersionListResult{}, fmt.Errorf("node %q has no history in any registered substrate", in.NodeID)
}

func handleDiff(ctx context.Context, env *Env, in diffInput) (DiffResult, error) {
	if in.NodeID == "" || in.VersionA == "" || in.VersionB == "" {
		return DiffResult{}, fmt.Errorf("node_id, version_a, version_b all required")
	}
	a, err := readNodeAcrossSubstrates(ctx, env, "", in.NodeID, in.VersionA, "")
	if err != nil {
		return DiffResult{}, fmt.Errorf("read version_a: %w", err)
	}
	b, err := readNodeAcrossSubstrates(ctx, env, "", in.NodeID, in.VersionB, "")
	if err != nil {
		return DiffResult{}, fmt.Errorf("read version_b: %w", err)
	}
	// Per REQUIREMENTS.md §12.2: structural diff is RFC 6902 over the
	// (contract + payload) tree of both versions. Operations are paths
	// rooted at "/contract/..." and "/payload/...".
	srcTree := map[string]interface{}{
		"contract": a.CurrentVersion.Contract,
		"payload":  a.CurrentVersion.Payload,
	}
	dstTree := map[string]interface{}{
		"contract": b.CurrentVersion.Contract,
		"payload":  b.CurrentVersion.Payload,
	}
	patch, err := jsondiff.Compare(srcTree, dstTree, jsondiff.Invertible())
	if err != nil {
		return DiffResult{}, fmt.Errorf("compute diff: %w", err)
	}
	ops := make([]DiffOp, 0, len(patch))
	for _, p := range patch {
		op := DiffOp{Op: string(p.Type), Path: p.Path, Value: p.Value, OldValue: p.OldValue}
		ops = append(ops, op)
	}
	out := DiffResult{
		NodeID:     in.NodeID,
		VersionA:   a.CurrentVersion.Version,
		VersionB:   b.CurrentVersion.Version,
		Operations: ops,
	}
	if a.Status != b.Status {
		out.StatusChange = a.Status + "->" + b.Status
	}
	if a.NodeKind != b.NodeKind {
		out.KindChange = a.NodeKind + "->" + b.NodeKind
	}
	out.Identical = len(ops) == 0 && out.StatusChange == "" && out.KindChange == ""
	return out, nil
}

func handleProject(ctx context.Context, env *Env, in projectInput, pin *snapshotPin) (ProjectResult, error) {
	if in.NodeID == "" || in.Projection == "" {
		return ProjectResult{}, fmt.Errorf("node_id and projection required")
	}
	proj, err := env.Projections.Resolve(ctx, in.Projection)
	if err != nil {
		return ProjectResult{}, err
	}
	root, err := readNodeAcrossSubstrates(ctx, env, "", in.NodeID, "", pin.SnapshotID)
	if err != nil {
		return ProjectResult{}, err
	}
	b, err := proj.Render(ctx, root, nil)
	if err != nil {
		return ProjectResult{}, err
	}
	return ProjectResult{
		Projection:  in.Projection,
		ContentType: "application/json",
		Body:        string(b),
	}, nil
}

// ---------------- new handlers per REQUIREMENTS.md §3.4 - §3.7 ----------------

func handleQuery(ctx context.Context, env *Env, in queryInput) (QueryResult, error) {
	if env.Config.Mode != ModeExperimentalBootstrap || !env.Config.ExperimentalQueryEnabled {
		return QueryResult{}, fmt.Errorf("%w: spec_graph_query requires mode=experimental_bootstrap and experimental_query_enabled=true", ErrModeMismatch)
	}
	if in.SQL == "" {
		return QueryResult{}, fmt.Errorf("sql required")
	}
	if !isReadOnlySQL(in.SQL) {
		return QueryResult{}, fmt.Errorf("query must be READ-ONLY (SELECT, GRAPH, or MATCH); got: %s", strings.SplitN(strings.TrimSpace(in.SQL), " ", 2)[0])
	}
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return QueryResult{}, err
	}
	if len(adapters) == 0 {
		return QueryResult{}, fmt.Errorf("no substrates registered")
	}
	// Bootstrap routes all queries to the single substrate.
	return adapters[0].ExecuteReadOnlyQuery(ctx, in.SQL, in.Params)
}

// importBatchNode tracks a node prepared earlier in the SAME handleImport
// batch: node_kind + the version it will admit at (always 1 for a fresh
// create — no prior_version). Lets a same-batch edge resolve its endpoints'
// versions without a live read.
type importBatchNode struct {
	kind    string
	version int
}

// handleImport admits a batch of task/artifact node + edge submissions into
// the schema_v1 Nodes/Edges substrate. Per REQUIREMENTS.md §3.6: every plan
// is prepared in memory first (ceremony steps 1-8 via PrepareAdmissionPlan);
// ANY prepare failure aborts the whole batch before any substrate write
// (all-or-none — see TestIntegrationImportAtomicAllOrNone). Successfully
// prepared plans are committed together in ONE Spanner ReadWriteTransaction
// via the variadic CommitAdmission (matching TestIntegrationImportHappyPathBatch).
// Reciprocal edge labels (parent_of/child_of, gates/gated_by) auto-admit
// their mirror within the same batch, mirroring admitEdge/handleLink. This
// is the migration + reseed surface (task_graph_import).
func handleImport(ctx context.Context, env *Env, in importInput) (ImportResult, error) {
	subs := in.Submissions
	if len(subs) == 0 && in.FilePath != "" {
		loaded, err := loadSubmissionsFromPath(in.FilePath)
		if err != nil {
			return ImportResult{}, err
		}
		subs = loaded
	}
	if len(subs) == 0 {
		return ImportResult{}, fmt.Errorf("no submissions provided")
	}
	// Refuse manufactured acceptance before generic preparation or backend
	// errors. Actual admission/native writers repeat the fence in transaction.
	for _, sub := range subs {
		if sub.NodeKind == "task" {
			if err := guardReviewNoteWrite(nil, sub.Payload["note"]); err != nil { return ImportResult{}, err }
		}
	}

	orgID := env.Config.DefaultOrganizationID
	if orgID == "" {
		return ImportResult{}, fmt.Errorf("default_organization_id required")
	}

	all, err := env.Substrates.All(ctx)
	if err != nil {
		return ImportResult{}, err
	}
	if len(all) == 0 {
		return ImportResult{}, fmt.Errorf("no substrates registered")
	}
	adapter := all[0]

	ordered, err := topologicallyOrder(subs)
	if err != nil {
		return ImportResult{}, fmt.Errorf("prepare aborted: order submissions: %w", err)
	}

	batchNodes := map[string]importBatchNode{}
	batchEdges := map[string]bool{}
	resolveEndpoint := func(nodeID string) (Node, error) {
		if n, ok := batchNodes[nodeID]; ok {
			return Node{NodeID: nodeID, NodeKind: n.kind, CurrentVersion: NodeVersion{Version: n.version}}, nil
		}
		return readNodeAcrossSubstrates(ctx, env, "", nodeID, "", "")
	}

	plans := make([]AdmissionPlan, 0, len(ordered))
	primaryIdx := make([]int, 0, len(ordered))
	var skippedIDs []string

	for i, sub := range ordered {
		sub.OrganizationID = orgID
		if sub.NodeKind == "artifact" && namesWriterHandoverHistory(stringFromMap(sub.Payload, "note")) {
			return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d]: %w", i, errWriterHandoverHistory())
		}
		switch {
		case sub.NodeKind != "":
			desc, err := env.Kinds.Resolve(ctx, sub.NodeKind)
			if err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d]: %w", i, err)
			}
			sub.ArtifactType = desc.ArtifactType()

			// Idempotency: a re-import of an already-admitted stable_id must
			// never attempt a fresh version=1 INSERT (Nodes' PK is
			// (org, node_id, version) — that would collide and abort the
			// whole batch). Read the active version first: identical
			// contract+payload → true no-op (skip); same contract, changed
			// payload → re-admit as a version bump; changed contract →
			// refuse (node_version_chain_axiom — identity does not
			// silently drift on a re-sync).
			if existing, exErr := readNodeAcrossSubstrates(ctx, env, "", sub.StableID, "", ""); exErr == nil {
				if err := validateContractMonotonicity(existing.CurrentVersion.Contract, sub.Contract); err != nil {
					return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] stable_id=%q: %w", i, sub.StableID, err)
				}
				payloadMatch, err := canonicalJSONEqual(existing.CurrentVersion.Payload, sub.Payload)
				if err != nil {
					return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] stable_id=%q: compare payload: %w", i, sub.StableID, err)
				}
				batchNodes[sub.StableID] = importBatchNode{kind: sub.NodeKind, version: existing.CurrentVersion.Version}
				if payloadMatch {
					skippedIDs = append(skippedIDs, sub.StableID)
					continue
				}
				sub.PriorVersion = &existing.CurrentVersion
			}

			plan, err := PrepareAdmissionPlan(ctx, env, sub)
			if err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] node_kind=%q stable_id=%q: %w", i, sub.NodeKind, sub.StableID, err)
			}
			batchNodes[plan.Node.NodeID] = importBatchNode{kind: sub.NodeKind, version: plan.Node.Version}
			primaryIdx = append(primaryIdx, len(plans))
			plans = append(plans, plan)

		case sub.EdgeLabel != "":
			desc, err := env.Edges.Resolve(ctx, sub.EdgeLabel)
			if err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d]: %w", i, err)
			}
			src, err := resolveEndpoint(sub.EdgeSourceID)
			if err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] edge source %q: %w", i, sub.EdgeSourceID, err)
			}
			tgt, err := resolveEndpoint(sub.EdgeTargetID)
			if err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] edge target %q: %w", i, sub.EdgeTargetID, err)
			}
			if err := validateEdgeEndpoints(desc, src, tgt); err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d]: %w", i, err)
			}

			// Idempotency: Edges' PK is (org, edge_id), not
			// (source, target, label) — a fresh admission mints a new
			// random edge_id regardless of whether the same logical
			// relationship already exists, so nothing at the storage layer
			// stops a naive re-import from silently duplicating the edge.
			// Skip (in-batch AND against what's already committed) when
			// an active edge with this exact source/target/label exists.
			edgeKey := sub.EdgeSourceID + "|" + sub.EdgeTargetID + "|" + sub.EdgeLabel
			if batchEdges[edgeKey] {
				skippedIDs = append(skippedIDs, edgeKey)
				continue
			}
			if _, found, err := findActiveEdge(ctx, env, sub.EdgeSourceID, sub.EdgeTargetID, sub.EdgeLabel); err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d]: check existing edge: %w", i, err)
			} else if found {
				batchEdges[edgeKey] = true
				skippedIDs = append(skippedIDs, edgeKey)
				continue
			}
			batchEdges[edgeKey] = true

			sub.ArtifactType = "edge"
			sub.EdgeSourceVersion = src.CurrentVersion.Version
			sub.EdgeTargetVersion = tgt.CurrentVersion.Version
			if sub.StableID == "" {
				sub.StableID = uuid.NewString()
			}
			plan, err := PrepareAdmissionPlan(ctx, env, sub)
			if err != nil {
				return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] edge_label=%q: %w", i, sub.EdgeLabel, err)
			}
			primaryIdx = append(primaryIdx, len(plans))
			plans = append(plans, plan)

			if mirrorLabel := desc.ReciprocalLabel(); mirrorLabel != "" {
				mirrorDesc, err := env.Edges.Resolve(ctx, mirrorLabel)
				if err != nil {
					return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] reciprocal label %q: %w", i, mirrorLabel, err)
				}
				if err := validateEdgeEndpoints(mirrorDesc, tgt, src); err != nil {
					return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] mirror direction: %w", i, err)
				}
				mirrorPlan, err := PrepareAdmissionPlan(ctx, env, ArtifactSubmission{
					ArtifactType:      "edge",
					OrganizationID:    orgID,
					EdgeLabel:         mirrorLabel,
					EdgeSourceID:      sub.EdgeTargetID,
					EdgeSourceVersion: sub.EdgeTargetVersion,
					EdgeTargetID:      sub.EdgeSourceID,
					EdgeTargetVersion: sub.EdgeSourceVersion,
					StableID:          uuid.NewString(),
				})
				if err != nil {
					return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d] reciprocal: %w", i, err)
				}
				plans[len(plans)-1].Edge.ReciprocalEdgeID = mirrorPlan.Edge.EdgeID
				mirrorPlan.Edge.ReciprocalEdgeID = plans[len(plans)-1].Edge.EdgeID
				plans = append(plans, mirrorPlan)
			}

		default:
			return ImportResult{}, fmt.Errorf("prepare aborted: submission[%d]: neither node_kind nor edge_label set", i)
		}
	}

	result := ImportResult{Total: len(ordered), Skipped: len(skippedIDs), SkippedIDs: skippedIDs}
	if len(plans) == 0 {
		// Every submission was an idempotent no-op — nothing to commit.
		return result, nil
	}

	recs, err := adapter.CommitAdmission(ctx, plans...)
	if err != nil {
		return ImportResult{}, err
	}

	result.Admitted = len(primaryIdx)
	for _, pi := range primaryIdx {
		result.Records = append(result.Records, recs[pi])
		result.OrderedIDs = append(result.OrderedIDs, recs[pi].ArtifactID)
	}
	return result, nil
}

// findActiveEdge returns the existing active edge (if any) with the given
// source/target/label. Edges' primary key is (organization_id, edge_id),
// not (source, target, label) — a fresh admission mints a new random
// edge_id regardless of whether the same logical relationship already
// exists, so nothing at the storage layer prevents a naive re-admission
// from silently duplicating the edge. Callers use this to make
// re-admission of the same logical edge an idempotent no-op instead.
func findActiveEdge(ctx context.Context, env *Env, sourceID, targetID, edgeLabel string) (Edge, bool, error) {
	all, err := env.Substrates.All(ctx)
	if err != nil {
		return Edge{}, false, err
	}
	if len(all) == 0 {
		return Edge{}, false, nil
	}
	edges, err := all[0].Neighbors(ctx, sourceID, NeighborOpts{Direction: "out", EdgeLabels: []string{edgeLabel}})
	if err != nil {
		return Edge{}, false, err
	}
	for _, e := range edges {
		if e.TargetID == targetID && e.Status == "active" {
			return e, true, nil
		}
	}
	return Edge{}, false, nil
}

// canonicalJSONEqual reports whether a and b marshal to byte-identical
// canonical JSON (sorted keys at every level) — used to detect a true
// no-op re-admission (payload unchanged) versus a real update.
func canonicalJSONEqual(a, b interface{}) (bool, error) {
	ab, err := canonicalJSON(a)
	if err != nil {
		return false, err
	}
	bb, err := canonicalJSON(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ab, bb), nil
}

// resolveAdapterForSubmission picks the substrate adapter that will own this
// submission's row. Mirrors the routing inside AdmitArtifact but exposes it
// to batch callers (handleImport) so they can group plans by adapter before
// calling the variadic CommitAdmission. Nodes route by node_kind via the
// kind registry; edges route to the first registered substrate (bootstrap
// has one; the multi-substrate routing for edges is a cutover concern).
func resolveAdapterForSubmission(ctx context.Context, env *Env, sub ArtifactSubmission) (SubstrateAdapter, error) {
	if sub.NodeKind != "" {
		desc, err := env.Kinds.Resolve(ctx, sub.NodeKind)
		if err != nil {
			return nil, err
		}
		return env.Substrates.Adapter(ctx, desc)
	}
	if sub.EdgeLabel != "" {
		all, err := env.Substrates.All(ctx)
		if err != nil {
			return nil, err
		}
		if len(all) == 0 {
			return nil, fmt.Errorf("no substrates registered")
		}
		return all[0], nil
	}
	return nil, fmt.Errorf("submission must have either node_kind or edge_label")
}

// loadSubmissionsFromPath accepts either a single JSON file (containing an
// array OR a single object) OR a directory whose *.json files are loaded in
// filename order. Matches bootstrap/seed_nodes/ layout (one file per node,
// admitted in name order per the README).
func loadSubmissionsFromPath(path string) ([]ArtifactSubmission, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if !info.IsDir() {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", path, err)
		}
		return parseSubmissionsJSON(data, path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %w", path, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		// Underscore-prefixed files are author/validator artifacts
		// (_edges.json carries the parent admission driver's edge list,
		// _findings.json carries the validator's verdict). They are not
		// submissions and the directory scan skips them per
		// AUTHORING_GUIDE.md §11.
		if strings.HasPrefix(e.Name(), "_") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	var out []ArtifactSubmission
	for _, name := range names {
		full := filepath.Join(path, name)
		data, err := os.ReadFile(full)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", full, err)
		}
		got, err := parseSubmissionsJSON(data, full)
		if err != nil {
			return nil, err
		}
		// Seed_node filenames are "NNN_<canonical_name>.json". When the
		// file body doesn't carry an explicit stable_id, derive it from
		// the filename: strip the NNN_ prefix and the .json suffix.
		canonicalName := canonicalNameFromFilename(name)
		for i := range got {
			if got[i].StableID == "" {
				got[i].StableID = canonicalName
			}
		}
		out = append(out, got...)
	}
	return out, nil
}

// resolveSubmissionCanonicalName extracts the canonical_name component of
// the OOD_GRAPH_PROTOCOL_v1 §3 identity tuple from an ArtifactSubmission.
// Precedence:
//
//  1. sub.StableID (explicit caller-set name; e.g. CLI / single-file
//     admission paths that derive name from filename)
//  2. sub.Contract["canonical_name"] (the typical inline-submission path —
//     the canonical_name lives in the contract per the authoring guide)
//  3. sub.Contract["canonical_err_name"] (sentinel-specific; sentinels use
//     PascalCase Err-prefixed names)
//
// Returns an error if none of the three resolve. Falling back to a kind-only
// name (e.g. "section") collapses every same-kind submission to the same
// UUIDv5 — the canonical_name component of the identity tuple MUST be
// distinct per node.
func resolveSubmissionCanonicalName(sub ArtifactSubmission) (string, error) {
	if sub.StableID != "" {
		return sub.StableID, nil
	}
	if sub.Contract != nil {
		if v, ok := sub.Contract["canonical_name"].(string); ok && v != "" {
			return v, nil
		}
		if v, ok := sub.Contract["canonical_err_name"].(string); ok && v != "" {
			return v, nil
		}
	}
	return "", fmt.Errorf("identity required — set stable_id OR contract.canonical_name (sentinels: contract.canonical_err_name); cannot derive UUIDv5 from kind alone")
}

// canonicalNameFromFilename strips the leading "NNN_" admission-order
// prefix and the ".json" suffix from a seed_node filename, leaving the
// canonical name. "001_origin.json" → "origin";
// "002_node_identity_axiom.json" → "node_identity_axiom".
func canonicalNameFromFilename(name string) string {
	base := strings.TrimSuffix(name, ".json")
	// Strip leading "NNN_" if present (3+ digits then underscore).
	for i, r := range base {
		if r >= '0' && r <= '9' {
			continue
		}
		if r == '_' && i > 0 {
			return base[i+1:]
		}
		break
	}
	return base
}

// parseSubmissionsJSON accepts either a top-level array of submissions OR a
// single submission object, returning a slice either way.
func parseSubmissionsJSON(data []byte, source string) ([]ArtifactSubmission, error) {
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "[") {
		var arr []ArtifactSubmission
		if err := json.Unmarshal(data, &arr); err != nil {
			return nil, fmt.Errorf("parse %q as array: %w", source, err)
		}
		return arr, nil
	}
	var one ArtifactSubmission
	if err := json.Unmarshal(data, &one); err != nil {
		return nil, fmt.Errorf("parse %q as object: %w", source, err)
	}
	return []ArtifactSubmission{one}, nil
}

func handleHealth(ctx context.Context, env *Env, _ healthInput, pin *snapshotPin) (HealthResult, error) {
	out := HealthResult{
		Mode:              env.Config.Mode,
		ActiveSnapshotPin: pin.SnapshotID,
		RegistrySizes: map[string]int{
			"substrates":  len(env.Config.Substrates),
			"node_kinds":  len(env.Config.NodeKinds),
			"edge_labels": len(env.Config.EdgeLabels),
			"projections": len(env.Config.Projections),
		},
		OverallReady: true,
	}

	// Closed-registry readiness is computed BEFORE the substrate probe. The
	// probe returns early when env.Substrates.All fails, and a substrate error
	// must not mask a registry blocker that refuses every write on its own —
	// two independent blockers must both be reported. This needs no adapter and
	// performs no I/O: task_store_type is a config field (registryHealth).
	registrySizes, registryBlocking := registryHealth(ctx, env)
	for k, v := range registrySizes {
		out.RegistrySizes[k] = v
	}
	if len(registryBlocking) > 0 {
		// A server that can accept no write is not ready, whatever the
		// substrate probe goes on to say.
		out.OverallReady = false
		out.BlockingIssues = append(out.BlockingIssues, registryBlocking...)
	}

	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		out.OverallReady = false
		out.BlockingIssues = append(out.BlockingIssues, err.Error())
		return out, nil
	}
	for _, a := range adapters {
		h := SubstrateHealth{ID: a.ID(), Database: a.Database()}
		if err := a.CheckSchema(ctx); err != nil {
			h.Reachable = true
			h.SchemaApplied = false
			h.Error = err.Error()
			out.OverallReady = false
			out.BlockingIssues = append(out.BlockingIssues, fmt.Sprintf("substrate %q: %s", a.ID(), err.Error()))
		} else {
			h.Reachable = true
			h.SchemaApplied = true
		}
		out.Substrates = append(out.Substrates, h)
	}
	// Mode is required by Validate(); if we got here, mode is set. No
	// "unset" branch — the binary would have refused to boot.
	return out, nil
}

// ---------------- shared helpers ----------------

func resolveProjectID(env *Env, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if env != nil && env.Config.DefaultProjectID != "" {
		return env.Config.DefaultProjectID, nil
	}
	return "", nil
}

func resolveProjectIDForAdapter(env *Env, adapter SubstrateAdapter, explicit string) (string, error) {
	if spannerAdapter, ok := adapter.(*SpannerSubstrateAdapter); ok && spannerAdapter.isFleetPlanNative() {
		return resolveNativeProjectID(env, spannerAdapter, explicit)
	}
	return resolveProjectID(env, explicit)
}

func readNodeAcrossSubstrates(ctx context.Context, env *Env, projectID, nodeID, version, snapshotID string) (Node, error) {
	if nodeID == "" {
		return Node{}, fmt.Errorf("node_id required")
	}
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return Node{}, err
	}
	var lastErr error
	for _, a := range adapters {
		adapterProjectID := projectID
		if spannerAdapter, ok := a.(*SpannerSubstrateAdapter); ok && spannerAdapter.isFleetPlanNative() {
			var err error
			adapterProjectID, err = resolveNativeProjectID(env, spannerAdapter, projectID)
			if err != nil {
				return Node{}, err
			}
		}
		n, err := a.ReadNodeInProject(ctx, adapterProjectID, nodeID, version, snapshotID)
		if err == nil {
			return n, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("node %q not found in any substrate", nodeID)
	}
	return Node{}, lastErr
}

// checkDanglingClosure refuses to archive a node that has active incoming
// edges (other nodes depend on it). Queries Edges where target_node_id =
// the archive candidate AND status = 'active'; any match returns
// ErrClosureWouldDangle with the dependent-edge inventory.
func checkDanglingClosure(ctx context.Context, env *Env, nodeID string) error {
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		return err
	}
	var depEdges []Edge
	for _, a := range adapters {
		edges, err := a.Neighbors(ctx, nodeID, NeighborOpts{Direction: "in"})
		if err != nil {
			return fmt.Errorf("substrate %q dangling-closure probe: %w", a.ID(), err)
		}
		for _, e := range edges {
			if e.Status == "active" {
				depEdges = append(depEdges, e)
			}
		}
	}
	if len(depEdges) == 0 {
		return nil
	}
	var summary []string
	for _, e := range depEdges {
		summary = append(summary, fmt.Sprintf("%s --%s--> %s", e.SourceID, e.EdgeLabel, e.TargetID))
	}
	return fmt.Errorf("%w: %d active incoming edge(s) — retire them first (unlink) before archiving: %s",
		ErrClosureWouldDangle, len(depEdges), strings.Join(summary, "; "))
}

// validateContractMonotonicity enforces node_version_chain_axiom's
// "contract is byte-equivalent across versions" rule. Compares canonical-JSON
// serializations of the prior and proposed contracts; any difference returns
// ErrContractDrift. Universal — not mode-gated.
func validateContractMonotonicity(prior, proposed interface{}) error {
	priorBytes, err := canonicalJSON(prior)
	if err != nil {
		return fmt.Errorf("canonical prior contract: %w", err)
	}
	proposedBytes, err := canonicalJSON(proposed)
	if err != nil {
		return fmt.Errorf("canonical proposed contract: %w", err)
	}
	if !bytes.Equal(priorBytes, proposedBytes) {
		return fmt.Errorf("%w: contract is byte-equivalent invariant per node_version_chain_axiom — re-admission may refine payload but never alter contract", ErrContractDrift)
	}
	return nil
}

func isReadOnlySQL(s string) bool {
	t := strings.ToUpper(strings.TrimLeft(s, " \t\r\n"))
	return strings.HasPrefix(t, "SELECT") ||
		strings.HasPrefix(t, "WITH") ||
		strings.HasPrefix(t, "GRAPH") ||
		strings.HasPrefix(t, "MATCH")
}

// topologicallyOrder returns the submissions in dependency order: a node
// whose payload references another node-id appearing in the same import
// must come AFTER its referent. References are detected by scanning the
// payload JSON for "$ref" strings naming other submissions' stable ids.
//
// This is a simple Kahn-style topological sort. Cycles or unresolved
// referents return ErrImportDependencyUnresolved.
func topologicallyOrder(subs []ArtifactSubmission) ([]ArtifactSubmission, error) {
	knownIDs := map[string]int{}
	for i, s := range subs {
		if s.StableID == "" {
			continue // anonymous nodes are leaves
		}
		if _, dup := knownIDs[s.StableID]; dup {
			return nil, fmt.Errorf("%w: duplicate stable_id %q in submissions", ErrImportDependencyUnresolved, s.StableID)
		}
		knownIDs[s.StableID] = i
	}

	// Build the dependency edges: from submission i, find all $ref strings in
	// its contract+payload that reference other submission stable_ids.
	deps := make([][]int, len(subs))
	for i, s := range subs {
		refs := scanRefs(s.Contract, s.Payload)
		for _, r := range refs {
			if j, ok := knownIDs[r]; ok && j != i {
				deps[i] = append(deps[i], j)
			}
		}
	}

	// Kahn's algorithm: indegree is incoming dep count.
	indegree := make([]int, len(subs))
	for _, ds := range deps {
		for _, d := range ds {
			indegree[d]++
		}
	}
	// Actually we want dependents to come AFTER referents. Reverse:
	// edge i → j means "i references j" so j must come before i.
	// So the result order is: all nodes with no outgoing deps first.
	out := make([]ArtifactSubmission, 0, len(subs))
	queue := []int{}
	outdegree := make([]int, len(subs))
	for i := range deps {
		outdegree[i] = len(deps[i])
		if outdegree[i] == 0 {
			queue = append(queue, i)
		}
	}
	// Sort queue for stable output
	sort.Ints(queue)
	processed := map[int]bool{}
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		if processed[i] {
			continue
		}
		processed[i] = true
		out = append(out, subs[i])
		// Anyone who depended on i can have their outdegree decremented.
		for j, ds := range deps {
			for _, d := range ds {
				if d == i {
					outdegree[j]--
					if outdegree[j] == 0 && !processed[j] {
						queue = append(queue, j)
						sort.Ints(queue)
					}
				}
			}
		}
	}
	if len(out) != len(subs) {
		return nil, fmt.Errorf("%w: cycle or unresolved referent (%d of %d submissions could not be ordered)", ErrImportDependencyUnresolved, len(subs)-len(out), len(subs))
	}
	return out, nil
}

// scanRefs walks already-parsed JSON values for any string of the form
// "$ref:<id>" or a key named "$ref" pointing at a string id. Inputs are
// interface{} (the form Contract/Payload arrive in post-deserialization).
func scanRefs(values ...interface{}) []string {
	var out []string
	for _, v := range values {
		if v == nil {
			continue
		}
		collectRefs(v, &out)
	}
	return out
}

func collectRefs(v interface{}, out *[]string) {
	switch t := v.(type) {
	case json.RawMessage:
		var parsed interface{}
		if err := json.Unmarshal(t, &parsed); err == nil {
			collectRefs(parsed, out)
		}
	case []byte:
		var parsed interface{}
		if err := json.Unmarshal(t, &parsed); err == nil {
			collectRefs(parsed, out)
		}
	case map[string]interface{}:
		for k, vv := range t {
			if k == "$ref" {
				if s, ok := vv.(string); ok {
					*out = append(*out, s)
				}
			}
			collectRefs(vv, out)
		}
	case []interface{}:
		for _, e := range t {
			collectRefs(e, out)
		}
	case string:
		if strings.HasPrefix(t, "$ref:") {
			*out = append(*out, strings.TrimPrefix(t, "$ref:"))
		}
	}
}
