package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"google.golang.org/protobuf/types/known/structpb"
)

// coverageLaneAdapter is a stateful schema-v1 adapter used to exercise the
// public handler orchestration without replacing the storage contract with a
// handler-specific mock. CommitAdmission persists the plans it receives, so
// create/update/link/import tests can immediately verify the corresponding
// read path through the same adapter.
type coverageLaneAdapter struct {
	id          string
	description string
	database    string
	nodes       map[string]Node
	edges       map[string]Edge
	histories   map[string][]NodeVersion
	queryResult QueryResult
	queryErr    error
	checkErr    error
	commitErr   error
	emptyCommit bool
	readErr     error
	listErr     error
	neighborErr error
	historyErr  error
	traverseErr error
	traverse    []Node
	commits     [][]AdmissionPlan
	traversals  []coverageLaneTraversal
}

type coverageLaneTraversal struct {
	projectID string
	nodeID    string
	labels    []string
	maxDepth  int
	resultCap int
	direction string
}

func newCoverageLaneAdapter() *coverageLaneAdapter {
	return &coverageLaneAdapter{
		id:          "coverage_schema",
		description: "coverage schema-v1 adapter",
		database:    "projects/test/instances/test/databases/test",
		nodes:       map[string]Node{},
		edges:       map[string]Edge{},
		histories:   map[string][]NodeVersion{},
	}
}

func coverageLaneNodeKey(id, version string) string {
	if version == "" {
		return id
	}
	return id + "@" + version
}

func (a *coverageLaneAdapter) ID() string          { return a.id }
func (a *coverageLaneAdapter) Description() string { return a.description }
func (a *coverageLaneAdapter) Database() string    { return a.database }
func (a *coverageLaneAdapter) Close() error        { return nil }
func (a *coverageLaneAdapter) CheckSchema(context.Context) error {
	return a.checkErr
}
func (a *coverageLaneAdapter) VerifyExperimentalSchema(context.Context) error { return nil }

func (a *coverageLaneAdapter) CommitAdmission(_ context.Context, plans ...AdmissionPlan) ([]AdmissionRecord, error) {
	copyPlans := append([]AdmissionPlan(nil), plans...)
	a.commits = append(a.commits, copyPlans)
	if a.commitErr != nil {
		return nil, a.commitErr
	}
	if a.emptyCommit {
		return nil, nil
	}
	records := make([]AdmissionRecord, 0, len(plans))
	for _, plan := range plans {
		record := plan.Record
		if plan.Node != nil {
			n := Node{
				NodeID:         plan.Node.NodeID,
				NodeKind:       record.ArtifactType,
				OrganizationID: plan.OrganizationID,
				Status:         plan.Node.Status,
				CreatedAt:      plan.Node.AdmittedAt,
				UpdatedAt:      plan.Node.AdmittedAt,
				CurrentVersion: *plan.Node,
			}
			a.nodes[coverageLaneNodeKey(n.NodeID, strconv.Itoa(n.CurrentVersion.Version))] = n
			a.nodes[n.NodeID] = n
			a.histories[n.NodeID] = append(a.histories[n.NodeID], n.CurrentVersion)
		}
		if plan.Edge != nil {
			edge := *plan.Edge
			edge.OrganizationID = plan.OrganizationID
			a.edges[edge.EdgeID] = edge
		}
		records = append(records, record)
	}
	return records, nil
}

func (a *coverageLaneAdapter) ReadNode(ctx context.Context, nodeID, version, snapshotID string) (Node, error) {
	return a.ReadNodeInProject(ctx, "", nodeID, version, snapshotID)
}

func (a *coverageLaneAdapter) ReadNodeInProject(_ context.Context, _ string, nodeID, version, _ string) (Node, error) {
	if a.readErr != nil {
		return Node{}, a.readErr
	}
	if n, ok := a.nodes[coverageLaneNodeKey(nodeID, version)]; ok {
		return n, nil
	}
	if version == "" {
		if n, ok := a.nodes[nodeID]; ok {
			return n, nil
		}
	}
	return Node{}, errors.New("coverage node not found")
}

func (a *coverageLaneAdapter) ReadEdge(_ context.Context, edgeID, _ string) (Edge, error) {
	if edge, ok := a.edges[edgeID]; ok {
		return edge, nil
	}
	return Edge{}, errors.New("coverage edge not found")
}

func (a *coverageLaneAdapter) ListNodes(_ context.Context, filter ListFilter) ([]Node, error) {
	if a.listErr != nil {
		return nil, a.listErr
	}
	var out []Node
	for key, node := range a.nodes {
		if strings.Contains(key, "@") {
			continue
		}
		if filter.NodeKind != "" && node.NodeKind != filter.NodeKind {
			continue
		}
		if filter.Status != "" && node.Status != filter.Status {
			continue
		}
		out = append(out, node)
	}
	return out, nil
}

func (a *coverageLaneAdapter) Neighbors(_ context.Context, nodeID string, opts NeighborOpts) ([]Edge, error) {
	if a.neighborErr != nil {
		return nil, a.neighborErr
	}
	wanted := map[string]bool{}
	for _, label := range opts.EdgeLabels {
		wanted[label] = true
	}
	var out []Edge
	for _, edge := range a.edges {
		if len(wanted) > 0 && !wanted[edge.EdgeLabel] {
			continue
		}
		match := false
		switch opts.Direction {
		case "in":
			match = edge.TargetID == nodeID
		case "out":
			match = edge.SourceID == nodeID
		default:
			match = edge.SourceID == nodeID || edge.TargetID == nodeID
		}
		if match {
			out = append(out, edge)
		}
	}
	return out, nil
}

func (a *coverageLaneAdapter) History(_ context.Context, nodeID string) ([]NodeVersion, error) {
	if a.historyErr != nil {
		return nil, a.historyErr
	}
	return append([]NodeVersion(nil), a.histories[nodeID]...), nil
}

func (a *coverageLaneAdapter) ExecuteReadOnlyQuery(_ context.Context, _ string, _ map[string]interface{}) (QueryResult, error) {
	return a.queryResult, a.queryErr
}

func (a *coverageLaneAdapter) SearchByEmbedding(context.Context, []float32, string, int, string) ([]Node, error) {
	return nil, nil
}

func (a *coverageLaneAdapter) FindByDefinitionHash(context.Context, string, string) (Node, bool, error) {
	return Node{}, false, nil
}

func (a *coverageLaneAdapter) FindByCanonicalName(context.Context, string, string, string) (Node, bool, error) {
	return Node{}, false, nil
}

func (a *coverageLaneAdapter) FindSimilarByEmbedding(context.Context, string, []float32, float64, int) ([]SimilarNode, error) {
	return nil, nil
}

func (a *coverageLaneAdapter) TraverseClosure(_ context.Context, projectID, nodeID string, labels []string, maxDepth, resultCap int, direction string) ([]Node, error) {
	a.traversals = append(a.traversals, coverageLaneTraversal{
		projectID: projectID,
		nodeID:    nodeID,
		labels:    append([]string(nil), labels...),
		maxDepth:  maxDepth,
		resultCap: resultCap,
		direction: direction,
	})
	return append([]Node(nil), a.traverse...), a.traverseErr
}

type coverageLaneRouter struct {
	adapters   []SubstrateAdapter
	allErr     error
	adapterErr error
}

func (r *coverageLaneRouter) Adapter(context.Context, NodeKindDescriptor) (SubstrateAdapter, error) {
	if r.adapterErr != nil {
		return nil, r.adapterErr
	}
	if len(r.adapters) == 0 {
		return nil, errors.New("coverage adapter unavailable")
	}
	return r.adapters[0], nil
}

func (r *coverageLaneRouter) All(context.Context) ([]SubstrateAdapter, error) {
	if r.allErr != nil {
		return nil, r.allErr
	}
	return append([]SubstrateAdapter(nil), r.adapters...), nil
}

func (r *coverageLaneRouter) ByID(id string) (SubstrateAdapter, error) {
	for _, adapter := range r.adapters {
		if adapter.ID() == id {
			return adapter, nil
		}
	}
	return nil, errors.New("coverage adapter id unavailable")
}

func (r *coverageLaneRouter) Close() error {
	for _, adapter := range r.adapters {
		if err := adapter.Close(); err != nil {
			return err
		}
	}
	return nil
}

func coverageLaneEnv(adapter *coverageLaneAdapter) *Env {
	kinds := []NodeKindDescriptor{
		NewNodeKindDescriptor(map[string]interface{}{
			"name":          "axiom",
			"substrate_id":  adapter.id,
			"artifact_type": "axiom",
			"version_scope": "stable_id_preserved",
		}),
	}
	edges := []EdgeLabelDescriptor{
		NewEdgeLabelDescriptor(map[string]interface{}{
			"name":              "references",
			"source_kinds":      []interface{}{"axiom"},
			"target_kinds":      []interface{}{"axiom"},
			"edge_family":       "ref",
			"forbid_self_loops": true,
		}),
		NewEdgeLabelDescriptor(map[string]interface{}{
			"name":             "load_bearing_for",
			"source_kinds":     []interface{}{"axiom"},
			"target_kinds":     []interface{}{"axiom"},
			"reciprocal_label": "depends_on",
			"edge_family":      "dep",
		}),
		NewEdgeLabelDescriptor(map[string]interface{}{
			"name":             "depends_on",
			"source_kinds":     []interface{}{"axiom"},
			"target_kinds":     []interface{}{"axiom"},
			"reciprocal_label": "load_bearing_for",
			"edge_family":      "dep",
		}),
	}
	cfg := SpecGraphConfig{
		Mode:                     ModeExperimentalBootstrap,
		ExperimentalAdmitter:     "COVERAGE_BOOTSTRAP",
		ExperimentalQueryEnabled: true,
		DefaultOrganizationID:    "coverage-org",
		DefaultProjectID:         "coverage-project",
		Substrates: []SubstrateDescriptor{{
			ID:                adapter.id,
			SpannerDatabase:   adapter.database,
			NodeTable:         "Nodes",
			EdgeTable:         "Edges",
			AdmissionLogTable: "ArtifactAdmissionLog",
		}},
		NodeKinds:  kinds,
		EdgeLabels: edges,
		Projections: []ProjectionDescriptor{{
			Name:       "task_canonical_json",
			OwningSpec: "coverage",
		}},
	}
	return &Env{
		Config:      cfg,
		Kinds:       newKindRegistry(kinds),
		Edges:       newEdgeRegistry(edges),
		Projections: newProjectionRegistry(cfg.Projections),
		Substrates:  &coverageLaneRouter{adapters: []SubstrateAdapter{adapter}},
		Now: func() time.Time {
			return time.Date(2026, 9, 12, 20, 0, 0, 0, time.UTC)
		},
		resolveNativeAdapter: func(context.Context, *Env) (*SpannerSubstrateAdapter, error) {
			return nil, nil
		},
	}
}

func coverageLaneNode(id string, version int, status string, contract, payload map[string]interface{}) Node {
	when := time.Date(2026, 9, 12, 19, version, 0, 0, time.UTC)
	return Node{
		NodeID:         id,
		NodeKind:       "axiom",
		OrganizationID: "coverage-org",
		Status:         status,
		CreatedAt:      when,
		UpdatedAt:      when,
		CurrentVersion: NodeVersion{
			NodeID:     id,
			Version:    version,
			Status:     status,
			Contract:   contract,
			Payload:    payload,
			AdmittedAt: when,
			AdmittedBy: "COVERAGE_BOOTSTRAP",
		},
	}
}

func TestCoverageSubstrateLaneSchemaV1HandlerBehavior(t *testing.T) {
	ctx := context.Background()
	adapter := newCoverageLaneAdapter()
	env := coverageLaneEnv(adapter)
	pin := &snapshotPin{SnapshotID: "initial-snapshot"}

	contract := map[string]interface{}{"canonical_name": "source"}
	source := coverageLaneNode("source", 1, "active", contract, map[string]interface{}{"value": "old"})
	target := coverageLaneNode("target", 1, "active", map[string]interface{}{"canonical_name": "target"}, map[string]interface{}{"value": "target"})
	archive := coverageLaneNode("archive", 1, "active", map[string]interface{}{"canonical_name": "archive"}, map[string]interface{}{"value": "retire"})
	adapter.nodes[source.NodeID] = source
	adapter.nodes[target.NodeID] = target
	adapter.nodes[archive.NodeID] = archive
	adapter.histories[source.NodeID] = []NodeVersion{source.CurrentVersion}
	adapter.queryResult = QueryResult{Columns: []string{"answer"}, Rows: []map[string]interface{}{{"answer": "42"}}}
	adapter.traverse = []Node{target}

	used, err := handleUse(ctx, env, useInput{}, pin)
	if err != nil || used.SnapshotID != "initial-snapshot" {
		t.Fatalf("report snapshot pin: got=%+v err=%v", used, err)
	}
	used, err = handleUse(ctx, env, useInput{SnapshotID: "next-snapshot"}, pin)
	if err != nil || used.SnapshotID != "next-snapshot" || pin.SnapshotID != "next-snapshot" {
		t.Fatalf("set snapshot pin: got=%+v pin=%+v err=%v", used, pin, err)
	}

	created, err := handleCreate(ctx, env, createInput{
		NodeKind: "axiom",
		StableID: "created",
		Contract: map[string]interface{}{"canonical_name": "created"},
		Payload:  map[string]interface{}{"value": "created"},
	})
	if err != nil || created.AdmissionRecord == nil || created.AdmissionRecord.ArtifactID != "created" {
		t.Fatalf("create: got=%+v err=%v", created, err)
	}

	updated, err := handleUpdate(ctx, env, updateInput{
		NodeID:   source.NodeID,
		Contract: contract,
		Payload:  map[string]interface{}{"value": "new"},
	})
	if err != nil || updated.AdmissionRecord == nil || updated.AdmissionRecord.ArtifactVersion != 2 {
		t.Fatalf("update: got=%+v err=%v", updated, err)
	}

	archived, err := handleArchive(ctx, env, archiveInput{NodeID: archive.NodeID})
	if err != nil || archived.ArtifactID != archive.NodeID || archived.ArtifactVersion != 2 {
		t.Fatalf("archive: got=%+v err=%v", archived, err)
	}

	linked, err := handleLink(ctx, env, linkInput{SourceID: source.NodeID, TargetID: target.NodeID, EdgeLabel: "references"})
	if err != nil || linked.AdmissionRecord == nil || linked.AdmissionRecord.ArtifactID == "" {
		t.Fatalf("link: got=%+v err=%v", linked, err)
	}
	edgeID := linked.AdmissionRecord.ArtifactID
	if _, ok := adapter.edges[edgeID]; !ok {
		t.Fatalf("link did not persist edge %q", edgeID)
	}
	idempotent, err := handleLink(ctx, env, linkInput{SourceID: source.NodeID, TargetID: target.NodeID, EdgeLabel: "references"})
	if err != nil || idempotent.AdmissionRecord == nil || idempotent.AdmissionRecord.ArtifactID != edgeID {
		t.Fatalf("idempotent link: got=%+v err=%v", idempotent, err)
	}
	unlinked, err := handleUnlink(ctx, env, unlinkInput{EdgeID: edgeID})
	if err != nil || unlinked.AdmissionRecord == nil || unlinked.AdmissionRecord.ArtifactID != edgeID {
		t.Fatalf("unlink: got=%+v err=%v", unlinked, err)
	}

	got, err := handleGet(ctx, env, getInput{NodeID: target.NodeID}, pin)
	if err != nil || got.NodeID != target.NodeID {
		t.Fatalf("get: got=%+v err=%v", got, err)
	}
	listed, err := handleList(ctx, env, listInput{}, pin)
	if err != nil || len(listed.Nodes) == 0 {
		t.Fatalf("list all: got=%+v err=%v", listed, err)
	}
	listed, err = handleList(ctx, env, listInput{NodeKind: "axiom", Limit: 2}, pin)
	if err != nil || len(listed.Nodes) == 0 {
		t.Fatalf("list kind: got=%+v err=%v", listed, err)
	}

	neighbors, err := handleNeighbors(ctx, env, neighborsInput{NodeID: source.NodeID, Direction: "out", EdgeLabels: []string{"references"}}, pin)
	if err != nil || len(neighbors.Edges) != 1 || neighbors.Edges[0].Status != "retired" {
		// The preceding unlink retired the edge, and the fake stores that
		// retirement exactly as the schema-v1 adapter would.
		t.Fatalf("neighbors after unlink: got=%+v err=%v", neighbors, err)
	}

	closure, err := handleClosure(ctx, env, closureInput{NodeID: source.NodeID, ClosureProfile: "ref"}, pin)
	if err != nil || len(closure.Nodes) != 1 {
		t.Fatalf("closure: got=%+v err=%v", closure, err)
	}
	impact, err := handleImpact(ctx, env, impactInput{NodeID: target.NodeID}, pin)
	if err != nil || len(impact.Nodes) != 1 {
		t.Fatalf("impact: got=%+v err=%v", impact, err)
	}
	if len(adapter.traversals) != 2 || adapter.traversals[0].direction != "forward" || adapter.traversals[1].direction != "reverse" {
		t.Fatalf("traversal routing: %+v", adapter.traversals)
	}
	labels, err := resolveClosureProfile(ctx, env, "dep")
	if err != nil || len(labels) != 2 {
		t.Fatalf("resolve closure profile: labels=%v err=%v", labels, err)
	}

	history, err := handleHistory(ctx, env, historyInput{NodeID: source.NodeID})
	if err != nil || len(history.Versions) == 0 {
		t.Fatalf("history: got=%+v err=%v", history, err)
	}

	diffV1 := coverageLaneNode("diff", 1, "active", map[string]interface{}{"canonical_name": "diff"}, map[string]interface{}{"value": "before", "remove": true})
	diffV2 := coverageLaneNode("diff", 2, "retired", map[string]interface{}{"canonical_name": "diff"}, map[string]interface{}{"value": "after", "add": 2})
	diffV2.NodeKind = "changed-kind"
	adapter.nodes[coverageLaneNodeKey("diff", "1")] = diffV1
	adapter.nodes[coverageLaneNodeKey("diff", "2")] = diffV2
	diff, err := handleDiff(ctx, env, diffInput{NodeID: "diff", VersionA: "1", VersionB: "2"})
	if err != nil || diff.Identical || diff.StatusChange != "active->retired" || diff.KindChange != "axiom->changed-kind" || len(diff.Operations) == 0 {
		t.Fatalf("diff: got=%+v err=%v", diff, err)
	}

	projected, err := handleProject(ctx, env, projectInput{NodeID: target.NodeID, Projection: "task_canonical_json"}, pin)
	if err != nil || projected.ContentType != "application/json" || !strings.Contains(projected.Body, target.NodeID) {
		t.Fatalf("project: got=%+v err=%v", projected, err)
	}
	queried, err := handleQuery(ctx, env, queryInput{SQL: "SELECT 42 AS answer"})
	if err != nil || !reflect.DeepEqual(queried, adapter.queryResult) {
		t.Fatalf("query: got=%+v err=%v", queried, err)
	}

	imports := []ArtifactSubmission{
		{NodeKind: "axiom", StableID: "batch-root", Contract: map[string]interface{}{"canonical_name": "batch-root"}, Payload: map[string]interface{}{"value": "root"}},
		{NodeKind: "axiom", StableID: "batch-child", Contract: map[string]interface{}{"canonical_name": "batch-child"}, Payload: map[string]interface{}{"parent": "$ref:batch-root"}},
		{EdgeLabel: "load_bearing_for", StableID: "batch-edge", EdgeSourceID: "batch-child", EdgeTargetID: "batch-root", Payload: map[string]interface{}{"refs": []interface{}{"$ref:batch-root", "$ref:batch-child"}}},
	}
	imported, err := handleImport(ctx, env, importInput{Submissions: imports})
	if err != nil || imported.Total != 3 || imported.Admitted != 3 || len(imported.Records) != 3 {
		t.Fatalf("import: got=%+v err=%v", imported, err)
	}
	idempotentImport, err := handleImport(ctx, env, importInput{Submissions: imports})
	if err != nil || idempotentImport.Total != 3 || idempotentImport.Skipped != 3 || idempotentImport.Admitted != 0 {
		t.Fatalf("idempotent import: got=%+v err=%v", idempotentImport, err)
	}

	health, err := handleHealth(ctx, env, healthInput{}, pin)
	if err != nil || !health.OverallReady || len(health.Substrates) != 1 || !health.Substrates[0].SchemaApplied {
		t.Fatalf("health: got=%+v err=%v", health, err)
	}
}

func TestCoverageSubstrateLaneHandlerRefusals(t *testing.T) {
	ctx := context.Background()
	baseAdapter := newCoverageLaneAdapter()
	baseEnv := coverageLaneEnv(baseAdapter)
	pin := &snapshotPin{}
	baseAdapter.nodes["existing"] = coverageLaneNode("existing", 1, "active", map[string]interface{}{"canonical_name": "existing"}, map[string]interface{}{"value": "old"})

	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{"create requires kind", func() error { _, err := handleCreate(ctx, baseEnv, createInput{}); return err }, "node_kind required"},
		{"create resolves kind", func() error { _, err := handleCreate(ctx, baseEnv, createInput{NodeKind: "unknown"}); return err }, "unknown node"},
		{"update requires id", func() error { _, err := handleUpdate(ctx, baseEnv, updateInput{}); return err }, "node_id required"},
		{"update preserves contract", func() error {
			_, err := handleUpdate(ctx, baseEnv, updateInput{NodeID: "existing", Contract: map[string]interface{}{"canonical_name": "changed"}})
			return err
		}, "contract"},
		{"archive requires id", func() error { _, err := handleArchive(ctx, baseEnv, archiveInput{}); return err }, "node_id required"},
		{"link requires tuple", func() error { _, err := handleLink(ctx, baseEnv, linkInput{}); return err }, "source_id, target_id, edge_label required"},
		{"link resolves label", func() error {
			_, err := handleLink(ctx, baseEnv, linkInput{SourceID: "existing", TargetID: "existing", EdgeLabel: "unknown"})
			return err
		}, "unknown edge"},
		{"unlink requires id", func() error { _, err := handleUnlink(ctx, baseEnv, unlinkInput{}); return err }, "edge_id required"},
		{"unlink requires edge", func() error { _, err := handleUnlink(ctx, baseEnv, unlinkInput{EdgeID: "missing"}); return err }, "not found"},
		{"get requires id", func() error { _, err := handleGet(ctx, baseEnv, getInput{}, pin); return err }, "node_id required"},
		{"list resolves kind", func() error { _, err := handleList(ctx, baseEnv, listInput{NodeKind: "unknown"}, pin); return err }, "unknown node"},
		{"search requires query", func() error { _, err := handleSearch(ctx, baseEnv, searchInput{}, pin); return err }, "query required"},
		{"search requires embedding", func() error { _, err := handleSearch(ctx, baseEnv, searchInput{Query: "coverage"}, pin); return err }, "embedding not configured"},
		{"neighbors requires id", func() error { _, err := handleNeighbors(ctx, baseEnv, neighborsInput{}, pin); return err }, "node_id required"},
		{"closure requires id", func() error { _, err := handleClosure(ctx, baseEnv, closureInput{}, pin); return err }, "node_id required"},
		{"closure requires profile", func() error { _, err := handleClosure(ctx, baseEnv, closureInput{NodeID: "existing"}, pin); return err }, "closure_profile required"},
		{"closure requires matching profile", func() error {
			_, err := handleClosure(ctx, baseEnv, closureInput{NodeID: "existing", ClosureProfile: "missing"}, pin)
			return err
		}, "matched zero"},
		{"impact requires id", func() error { _, err := handleImpact(ctx, baseEnv, impactInput{}, pin); return err }, "node_id required"},
		{"history requires id", func() error { _, err := handleHistory(ctx, baseEnv, historyInput{}); return err }, "node_id required"},
		{"history requires versions", func() error { _, err := handleHistory(ctx, baseEnv, historyInput{NodeID: "missing"}); return err }, "no history"},
		{"diff requires coordinates", func() error { _, err := handleDiff(ctx, baseEnv, diffInput{}); return err }, "all required"},
		{"project requires tuple", func() error { _, err := handleProject(ctx, baseEnv, projectInput{}, pin); return err }, "node_id and projection required"},
		{"project resolves projection", func() error {
			_, err := handleProject(ctx, baseEnv, projectInput{NodeID: "existing", Projection: "missing"}, pin)
			return err
		}, "unknown projection"},
		{"query rejects disabled mode", func() error {
			disabled := *baseEnv
			disabled.Config.ExperimentalQueryEnabled = false
			_, err := handleQuery(ctx, &disabled, queryInput{SQL: "SELECT 1"})
			return err
		}, "requires mode"},
		{"query requires sql", func() error { _, err := handleQuery(ctx, baseEnv, queryInput{}); return err }, "sql required"},
		{"query rejects writes", func() error { _, err := handleQuery(ctx, baseEnv, queryInput{SQL: "DELETE FROM Nodes"}); return err }, "READ-ONLY"},
		{"import requires submissions", func() error { _, err := handleImport(ctx, baseEnv, importInput{}); return err }, "no submissions"},
		{"import rejects shapeless submission", func() error {
			_, err := handleImport(ctx, baseEnv, importInput{Submissions: []ArtifactSubmission{{StableID: "bad"}}})
			return err
		}, "neither node_kind nor edge_label"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("error=%v, want substring %q", err, tc.want)
			}
		})
	}

	danglingAdapter := newCoverageLaneAdapter()
	danglingEnv := coverageLaneEnv(danglingAdapter)
	danglingAdapter.nodes["blocked"] = coverageLaneNode("blocked", 1, "active", map[string]interface{}{"canonical_name": "blocked"}, nil)
	danglingAdapter.edges["incoming"] = Edge{EdgeID: "incoming", SourceID: "dependent", TargetID: "blocked", EdgeLabel: "references", Status: "active"}
	if _, err := handleArchive(ctx, danglingEnv, archiveInput{NodeID: "blocked"}); err == nil || !errors.Is(err, ErrClosureWouldDangle) {
		t.Fatalf("archive should refuse active dependents: %v", err)
	}

	brokenAdapter := newCoverageLaneAdapter()
	brokenAdapter.checkErr = errors.New("schema missing")
	brokenEnv := coverageLaneEnv(brokenAdapter)
	health, err := handleHealth(ctx, brokenEnv, healthInput{}, pin)
	if err != nil || health.OverallReady || health.Substrates[0].SchemaApplied || !strings.Contains(health.BlockingIssues[0], "schema missing") {
		t.Fatalf("unready health: got=%+v err=%v", health, err)
	}

	routerFailure := *baseEnv
	routerFailure.Substrates = &coverageLaneRouter{allErr: errors.New("router failed")}
	health, err = handleHealth(ctx, &routerFailure, healthInput{}, pin)
	if err != nil || health.OverallReady || !strings.Contains(health.BlockingIssues[0], "router failed") {
		t.Fatalf("router failure health: got=%+v err=%v", health, err)
	}
}

func TestCoverageSubstrateLaneHandlerHelpers(t *testing.T) {
	ctx := context.Background()
	adapter := newCoverageLaneAdapter()
	env := coverageLaneEnv(adapter)

	if got, err := resolveProjectID(env, "explicit"); err != nil || got != "explicit" {
		t.Fatalf("explicit project: got=%q err=%v", got, err)
	}
	if got, err := resolveProjectID(env, ""); err != nil || got != "coverage-project" {
		t.Fatalf("default project: got=%q err=%v", got, err)
	}
	if got, err := resolveProjectID(nil, ""); err != nil || got != "" {
		t.Fatalf("empty project: got=%q err=%v", got, err)
	}
	if got, err := resolveProjectIDForAdapter(env, adapter, "explicit"); err != nil || got != "explicit" {
		t.Fatalf("schema-v1 adapter project: got=%q err=%v", got, err)
	}

	if err := validateContractMonotonicity(map[string]interface{}{"b": 2, "a": 1}, map[string]interface{}{"a": 1, "b": 2}); err != nil {
		t.Fatalf("equivalent contract: %v", err)
	}
	if err := validateContractMonotonicity(map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2}); err == nil || !errors.Is(err, ErrContractDrift) {
		t.Fatalf("contract drift not detected: %v", err)
	}

	if equal, err := canonicalJSONEqual(map[string]interface{}{"b": 2, "a": 1}, map[string]interface{}{"a": 1, "b": 2}); err != nil || !equal {
		t.Fatalf("canonical equality: equal=%v err=%v", equal, err)
	}
	if equal, err := canonicalJSONEqual(map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2}); err != nil || equal {
		t.Fatalf("canonical inequality: equal=%v err=%v", equal, err)
	}

	if got, err := resolveAdapterForSubmission(ctx, env, ArtifactSubmission{NodeKind: "axiom"}); err != nil || got != adapter {
		t.Fatalf("resolve node adapter: got=%v err=%v", got, err)
	}
	if got, err := resolveAdapterForSubmission(ctx, env, ArtifactSubmission{EdgeLabel: "references"}); err != nil || got != adapter {
		t.Fatalf("resolve edge adapter: got=%v err=%v", got, err)
	}
	if _, err := resolveAdapterForSubmission(ctx, env, ArtifactSubmission{}); err == nil {
		t.Fatal("shapeless adapter resolution should fail")
	}

	arrayJSON := []byte(`[{"node_kind":"axiom","stable_id":"one"}]`)
	if parsed, err := parseSubmissionsJSON(arrayJSON, "array"); err != nil || len(parsed) != 1 || parsed[0].StableID != "one" {
		t.Fatalf("parse array: parsed=%+v err=%v", parsed, err)
	}
	objectJSON := []byte(`{"node_kind":"axiom","stable_id":"two"}`)
	if parsed, err := parseSubmissionsJSON(objectJSON, "object"); err != nil || len(parsed) != 1 || parsed[0].StableID != "two" {
		t.Fatalf("parse object: parsed=%+v err=%v", parsed, err)
	}
	if _, err := parseSubmissionsJSON([]byte("["), "broken-array"); err == nil {
		t.Fatal("broken array should fail")
	}
	if _, err := parseSubmissionsJSON([]byte("{"), "broken-object"); err == nil {
		t.Fatal("broken object should fail")
	}

	refs := scanRefs(
		map[string]interface{}{"$ref": "direct", "nested": []interface{}{"$ref:prefixed"}},
		[]byte(`{"$ref":"bytes"}`),
		json.RawMessage(`{"nested":"$ref:raw"}`),
		42,
	)
	for _, want := range []string{"direct", "prefixed", "bytes", "raw"} {
		if !containsString(refs, want) {
			t.Fatalf("refs %v missing %q", refs, want)
		}
	}

	ordered, err := topologicallyOrder([]ArtifactSubmission{
		{StableID: "dependent", Payload: map[string]interface{}{"ref": "$ref:dependency"}},
		{StableID: "dependency"},
		{},
	})
	if err != nil || len(ordered) != 3 || ordered[0].StableID != "dependency" {
		t.Fatalf("topological order: %+v err=%v", ordered, err)
	}

	adapter.nodes["readable"] = coverageLaneNode("readable", 1, "active", nil, nil)
	if got, err := readNodeAcrossSubstrates(ctx, env, "coverage-project", "readable", "", "snapshot"); err != nil || got.NodeID != "readable" {
		t.Fatalf("read across substrates: got=%+v err=%v", got, err)
	}
	if _, err := readNodeAcrossSubstrates(ctx, env, "", "missing", "", ""); err == nil {
		t.Fatal("missing read should fail")
	}
	if _, err := readNodeAcrossSubstrates(ctx, env, "", "", "", ""); err == nil {
		t.Fatal("empty read should fail")
	}

	adapter.edges["retired"] = Edge{EdgeID: "retired", TargetID: "readable", Status: "retired"}
	if err := checkDanglingClosure(ctx, env, "readable"); err != nil {
		t.Fatalf("retired incoming edge should not block: %v", err)
	}
}

func TestCoverageSubstrateLaneAdapterHelpersAndDecoders(t *testing.T) {
	ctx := context.Background()
	adapter := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{
		ID:                "schema",
		Description:       "schema adapter",
		SpannerDatabase:   "projects/test/instances/test/databases/test",
		NodeTable:         "Nodes",
		EdgeTable:         "Edges",
		AdmissionLogTable: "ArtifactAdmissionLog",
	}}
	if adapter.ID() != "schema" || adapter.Description() != "schema adapter" || !strings.HasSuffix(adapter.Database(), "/test") {
		t.Fatalf("descriptor accessors: id=%q description=%q database=%q", adapter.ID(), adapter.Description(), adapter.Database())
	}
	if nodeT, edgeT, logT, err := adapter.requireWriteTables(); err != nil || nodeT != "Nodes" || edgeT != "Edges" || logT != "ArtifactAdmissionLog" {
		t.Fatalf("write table resolution: %q %q %q err=%v", nodeT, edgeT, logT, err)
	}
	readOnly := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ID: "readonly", ReadOnly: true}}
	if _, _, _, err := readOnly.requireWriteTables(); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only write guard: %v", err)
	}
	missing := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ID: "missing"}}
	if _, _, _, err := missing.requireWriteTables(); err == nil || !strings.Contains(err.Error(), "all required") {
		t.Fatalf("missing write tables: %v", err)
	}
	if err := missing.CheckSchema(ctx); err == nil {
		t.Fatal("schema check should fail before I/O when tables are undeclared")
	}
	if err := missing.VerifyExperimentalSchema(ctx); err == nil || !strings.Contains(err.Error(), "no graph tables") {
		t.Fatalf("schema fingerprint should require declared graph tables: %v", err)
	}
	if _, err := missing.ReadNode(ctx, "node", "", ""); err == nil {
		t.Fatal("read node should require node_table")
	}
	if _, err := missing.ReadEdge(ctx, "edge", ""); err == nil {
		t.Fatal("read edge should require edge_table")
	}
	if _, err := missing.ListNodes(ctx, ListFilter{}); err == nil {
		t.Fatal("list should require node_table")
	}
	if _, err := missing.Neighbors(ctx, "node", NeighborOpts{}); err == nil {
		t.Fatal("neighbors should require edge_table")
	}
	if _, err := missing.History(ctx, "node"); err == nil {
		t.Fatal("history should require node_table")
	}
	if _, err := missing.SearchByEmbedding(ctx, nil, "", 1, ""); err == nil {
		t.Fatal("search should require node_table")
	}
	if _, _, err := missing.FindByDefinitionHash(ctx, "org", "hash"); err == nil {
		t.Fatal("definition lookup should require node_table")
	}
	if _, _, err := missing.FindByCanonicalName(ctx, "org", "axiom", "name"); err == nil {
		t.Fatal("canonical lookup should require node_table")
	}
	if _, err := missing.FindSimilarByEmbedding(ctx, "org", nil, 0.1, 1); err == nil {
		t.Fatal("similar lookup should require node_table")
	}
	if _, err := missing.TraverseClosure(ctx, "", "node", nil, 1, 1, "forward"); err == nil {
		t.Fatal("closure should require node_table")
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("close unused adapter: %v", err)
	}

	native := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ID: "native", TaskStoreType: "fleet_plan_native"}}
	if _, err := native.ReadNodeInProject(ctx, "project", "node", "2", ""); err == nil || !strings.Contains(err.Error(), "unversioned") {
		t.Fatalf("native version guard: %v", err)
	}
	if _, err := native.ReadEdge(ctx, "edge", ""); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("native edge read guard: %v", err)
	}

	if condIf(true, "yes") != "yes" || condIf(false, "yes") != "" {
		t.Fatal("conditional SQL fragment mismatch")
	}
	if nullableString("") != (spanner.NullString{Valid: false}) || nullableString("value") != "value" {
		t.Fatalf("nullable string mismatch: empty=%#v value=%#v", nullableString(""), nullableString("value"))
	}
	if nullableInt64(0) != (spanner.NullInt64{Valid: false}) || nullableInt64(7) != int64(7) {
		t.Fatalf("nullable int mismatch: zero=%#v value=%#v", nullableInt64(0), nullableInt64(7))
	}

	values := []interface{}{nil, nil, 4.5, "text", true, map[string]interface{}{"nested": "value"}, []interface{}{1.0, "two"}}
	for index := 1; index < len(values); index++ {
		protoValue, err := structpb.NewValue(values[index])
		if err != nil {
			t.Fatalf("new protobuf value %d: %v", index, err)
		}
		got := decodeStructpbValue(protoValue)
		if !reflect.DeepEqual(got, values[index]) {
			t.Fatalf("decode protobuf value %d: got=%#v want=%#v", index, got, values[index])
		}
	}
	if decodeStructpbValue(nil) != nil {
		t.Fatal("nil protobuf value should decode to nil")
	}

	when := time.Date(2026, 9, 12, 21, 0, 0, 0, time.UTC)
	nodeValues := []interface{}{
		"org", "node", int64(3), "axiom", "active",
		spanner.NullString{StringVal: `{"canonical_name":"node"}`, Valid: true},
		spanner.NullString{StringVal: `{"value":3}`, Valid: true},
		"definition", "manifest", []string{"signature"}, "admitter", when,
	}
	nodeRow, err := spanner.NewRow([]string{"organization_id", "node_id", "version", "node_kind", "status", "contract_json", "content_json", "definition_hash", "manifest_hash", "signature_chain", "admitted_by", "admitted_at"}, nodeValues)
	if err != nil {
		t.Fatalf("new node row: %v", err)
	}
	decodedNode, err := decodeNodeRow(nodeRow)
	if err != nil || decodedNode.NodeID != "node" || decodedNode.CurrentVersion.Version != 3 || decodedNode.CurrentVersion.Payload["value"] != float64(3) {
		t.Fatalf("decode node row: got=%+v err=%v", decodedNode, err)
	}
	distanceRow, err := spanner.NewRow(append(nodeRow.ColumnNames(), "distance"), append(nodeValues, float64(0.25)))
	if err != nil {
		t.Fatalf("new distance row: %v", err)
	}
	decodedDistanceNode, distance, err := decodeNodeRowWithDistanceValue(distanceRow)
	if err != nil || decodedDistanceNode.NodeID != "node" || distance != 0.25 {
		t.Fatalf("decode node distance: node=%+v distance=%v err=%v", decodedDistanceNode, distance, err)
	}
	decodedRankedNode, err := decodeNodeRowWithDistance(distanceRow)
	if err != nil || decodedRankedNode.NodeID != "node" {
		t.Fatalf("decode ranked node: node=%+v err=%v", decodedRankedNode, err)
	}

	edgeRow, err := spanner.NewRow(
		[]string{"organization_id", "edge_id", "source_node_id", "source_version", "target_node_id", "target_version", "edge_label", "status", "admitted_at", "reciprocal_edge_id"},
		[]interface{}{"org", "edge", "source", spanner.NullInt64{Int64: 1, Valid: true}, "target", spanner.NullInt64{Int64: 2, Valid: true}, "references", "active", when, spanner.NullString{StringVal: "mirror", Valid: true}},
	)
	if err != nil {
		t.Fatalf("new edge row: %v", err)
	}
	decodedEdge, err := decodeEdgeRow(edgeRow)
	if err != nil || decodedEdge.EdgeID != "edge" || decodedEdge.SourceVersion != 1 || decodedEdge.TargetVersion != 2 || decodedEdge.ReciprocalEdgeID != "mirror" {
		t.Fatalf("decode edge row: edge=%+v err=%v", decodedEdge, err)
	}

	badRow, err := spanner.NewRow([]string{"only"}, []interface{}{"one"})
	if err != nil {
		t.Fatalf("new bad row: %v", err)
	}
	if _, err := decodeNodeRow(badRow); err == nil {
		t.Fatal("short node row should fail")
	}
	if _, err := decodeNodeRowWithDistance(badRow); err == nil {
		t.Fatal("short ranked row should fail")
	}
	if _, _, err := decodeNodeRowWithDistanceValue(badRow); err == nil {
		t.Fatal("short distance row should fail")
	}
	if _, err := decodeEdgeRow(badRow); err == nil {
		t.Fatal("short edge row should fail")
	}
}

func TestCoverageSubstrateLaneRouterBehavior(t *testing.T) {
	ctx := context.Background()
	descriptors := []SubstrateDescriptor{
		{ID: "one", Description: "first", SpannerDatabase: "db-one"},
		{ID: "two", Description: "second", SpannerDatabase: "db-two"},
	}
	router := newRouter(gcpauthSelectorZero(), false, descriptors)
	defer router.Close()

	kind := NewNodeKindDescriptor(map[string]interface{}{
		"name":          "axiom",
		"substrate_id":  "one",
		"artifact_type": "axiom",
		"version_scope": "stable_id_preserved",
	})
	got, err := router.Adapter(ctx, kind)
	if err != nil || got.ID() != "one" {
		t.Fatalf("route kind: got=%v err=%v", got, err)
	}
	if _, err := router.Adapter(ctx, NewNodeKindDescriptor(map[string]interface{}{"name": "missing", "substrate_id": "absent"})); err == nil || !errors.Is(err, ErrSubstrateNotProvisioned) {
		t.Fatalf("missing kind route: %v", err)
	}
	if got, err := router.ByID("two"); err != nil || got.ID() != "two" {
		t.Fatalf("route by id: got=%v err=%v", got, err)
	}
	if _, err := router.ByID("absent"); err == nil {
		t.Fatal("missing substrate id should fail")
	}
	all, err := router.All(ctx)
	if err != nil || len(all) != 2 {
		t.Fatalf("route all: got=%v err=%v", all, err)
	}
}

// gcpauthSelectorZero returns the zero selector without adding another
// production credential seam. Router construction is storage-free; none of
// these tests asks an adapter to open a client.
func gcpauthSelectorZero() gcpauth.Selector { return gcpauth.Selector{} }
