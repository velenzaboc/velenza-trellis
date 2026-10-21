//go:build integration

// Integration tests against a local Spanner emulator. Excluded from the
// default `go test ./...` run; enable with `go test -tags=integration`.
// Driven via the Makefile target `make test-integration` which starts the
// emulator, creates a test database, runs these tests, then tears down.
//
// Coverage scope: features the Spanner emulator supports as of 2026 —
// DDL apply, mutations, standard SQL queries, the admission-ceremony +
// substrate write path, edge admission with reciprocity, RFC 6902 diff.
//
// NOT covered (emulator gaps; verified on real Spanner via the live MCP
// smoke walks): CREATE PROPERTY GRAPH, APPROX_COSINE_DISTANCE / vector
// indexes, GRAPH_TABLE traversal. The unit tests + manual smoke walks
// against corpus-graph-iter1 cover those paths.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

// integrationTestCredentials is a syntactically valid google_credentials
// selector for the emulator-backed integration tests. gcpauth.Selector.Validate
// requires a fully-shaped selector (credentials_source, a profile, and an
// identity matching the service-account-email pattern) even in
// experimental_bootstrap mode; SpannerSubstrateAdapter.getClient separately
// detects SPANNER_EMULATOR_HOST and skips real GCP auth entirely (see
// substrate.go, gcpauth.ValidateSpannerEmulatorHost), so this identity is
// never actually used to authenticate anything — it only has to pass static
// validation. The legacy per-substrate CredentialsFile field these tests used
// to set instead is now rejected by config.go's Validate ("legacy per-client
// credential selector is forbidden; use google_credentials").
var integrationTestCredentials = gcpauth.Selector{
	CredentialsSource:   gcpauth.CredentialsSourceApplicationDefault,
	CredentialsProfile:  gcpauth.CredentialsProfileImpersonatedServiceAccount,
	CredentialsIdentity: "task-graph-integration-test@example-gcp-project.iam.gserviceaccount.com",
}

const (
	emulatorProject  = "test-project"
	emulatorInstance = "test-instance"
	emulatorDatabase = "test-corpus-graph"
)

// emulatorResource is the resource path the integration tests target. The
// Spanner Go client honors SPANNER_EMULATOR_HOST and routes to localhost.
var emulatorResource = "projects/" + emulatorProject + "/instances/" + emulatorInstance + "/databases/" + emulatorDatabase

func requireEmulator(t *testing.T) {
	t.Helper()
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Fatal("SPANNER_EMULATOR_HOST not set — integration tests require the Spanner emulator running locally; see Makefile test-integration target")
	}
}

// integrationSchema is the subset of bootstrap/schema_v1.sql the Spanner
// emulator supports. The vector index + property graph DDL is omitted
// (emulator gaps); the underlying tables are still created so admission
// ceremony writes succeed.
var integrationSchema = []string{
	`CREATE TABLE Nodes (
		organization_id            STRING(64)  NOT NULL,
		node_id                    STRING(64)  NOT NULL,
		version                    INT64       NOT NULL,
		node_kind                  STRING(128) NOT NULL,
		status                     STRING(32)  NOT NULL,
		contract                   JSON,
		content                    JSON,
		definition_hash            STRING(128),
		manifest_hash              STRING(128),
		signature_chain            ARRAY<STRING(MAX)>,
		admitted_by                STRING(256) NOT NULL,
		admitted_at                TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
		superseded_by_version      INT64,
		governing_spec_ref         STRING(MAX),
		source_spec_lineage_refs   ARRAY<STRING(MAX)>,
		spec_section_ref           STRING(MAX),
		spec_symbol_ref            STRING(MAX),
		node_creation_contract_ref STRING(MAX),
		active_marker              STRING(64) AS (IF(status = 'active', node_id, NULL)) STORED,
		embedding_v2               ARRAY<FLOAT32>,
	) PRIMARY KEY (organization_id, node_id, version)`,
	`CREATE TABLE Edges (
		organization_id    STRING(64)  NOT NULL,
		edge_id            STRING(64)  NOT NULL,
		source_node_id     STRING(64)  NOT NULL,
		source_version     INT64       NOT NULL,
		target_node_id     STRING(64)  NOT NULL,
		target_version     INT64       NOT NULL,
		edge_label         STRING(128) NOT NULL,
		status             STRING(32)  NOT NULL,
		admitted_at        TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
		reciprocal_edge_id STRING(64),
	) PRIMARY KEY (organization_id, edge_id)`,
	`CREATE TABLE ArtifactAdmissionLog (
		organization_id        STRING(64)  NOT NULL,
		admission_id           STRING(64)  NOT NULL,
		artifact_id            STRING(64)  NOT NULL,
		artifact_type          STRING(128) NOT NULL,
		artifact_version       INT64       NOT NULL,
		artifact_hash          STRING(128) NOT NULL,
		manifest_hash          STRING(128) NOT NULL,
		admitted_by            STRING(256) NOT NULL,
		admitted_at            TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
		signature              STRING(MAX) NOT NULL,
		dual_control_approver  STRING(256),
		is_non_genesis         BOOL        NOT NULL,
		pass_report            JSON,
	) PRIMARY KEY (organization_id, admission_id)`,
}

// setupEmulatorDB drops + creates a fresh database, applies the integration
// schema. Returns when ready.
func setupEmulatorDB(ctx context.Context, t *testing.T) {
	setupEmulatorDBWithSchema(ctx, t, integrationSchema)
}

// ensureEmulatorInstance makes the integration suite self-contained. The
// reusable CI workflow starts a fresh emulator and invokes go test directly,
// so Makefile-only instance provisioning is not part of that execution path.
// CreateInstance is deliberately attempted on every setup: AlreadyExists is
// the sole idempotent success case, while every other admin failure remains a
// hard test failure.
func ensureEmulatorInstance(ctx context.Context, t *testing.T) {
	t.Helper()
	adminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatalf("instance admin client: %v", err)
	}
	defer adminClient.Close()

	op, err := adminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + emulatorProject,
		InstanceId: emulatorInstance,
		Instance: &instancepb.Instance{
			Name:        "projects/" + emulatorProject + "/instances/" + emulatorInstance,
			Config:      "projects/" + emulatorProject + "/instanceConfigs/emulator-config",
			DisplayName: "task-graph integration tests",
			NodeCount:   1,
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return
	}
	if err != nil {
		t.Fatalf("create emulator instance: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("create emulator instance wait: %v", err)
	}
}

func setupEmulatorDBWithSchema(ctx context.Context, t *testing.T, schema []string) {
	t.Helper()
	ensureEmulatorInstance(ctx, t)
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer adminClient.Close()

	// Drop if present.
	_ = adminClient.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: emulatorResource})

	// Create.
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          "projects/" + emulatorProject + "/instances/" + emulatorInstance,
		CreateStatement: "CREATE DATABASE `" + emulatorDatabase + "`",
	})
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("create database wait: %v", err)
	}

	// Apply schema.
	op2, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   emulatorResource,
		Statements: schema,
	})
	if err != nil {
		t.Fatalf("update ddl: %v", err)
	}
	if err := op2.Wait(ctx); err != nil {
		t.Fatalf("update ddl wait: %v", err)
	}
}

// integrationEnv builds an Env wired to the emulator. The emulator doesn't
// have Vertex AI access; Embed is nil so admission skips embedding.
func integrationEnv(t *testing.T) *Env {
	t.Helper()
	cfg := SpecGraphConfig{
		Mode:                  ModeExperimentalBootstrap,
		ExperimentalAdmitter:  "EXPERIMENTAL_BOOTSTRAP",
		DefaultOrganizationID: "test-org",
		ListenAddress:         "127.0.0.1:0",
		GoogleCredentials:     integrationTestCredentials,
		Substrates: []SubstrateDescriptor{{
			ID:                "test_substrate",
			SpannerDatabase:   emulatorResource,
			NodeTable:         "Nodes",
			EdgeTable:         "Edges",
			AdmissionLogTable: "ArtifactAdmissionLog",
		}},
		NodeKinds: []NodeKindDescriptor{
			NewNodeKindDescriptor(map[string]interface{}{
				"name":          "axiom",
				"substrate_id":  "test_substrate",
				"artifact_type": "axiom",
				"version_scope": "stable_id_preserved",
			}),
		},
		EdgeLabels: []EdgeLabelDescriptor{
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name":         "references",
				"source_kinds": []interface{}{"axiom"},
				"target_kinds": []interface{}{"axiom"},
				"edge_family":  "ref",
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
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	return &Env{
		Config:     cfg,
		Kinds:      newKindRegistry(cfg.NodeKinds),
		Edges:      newEdgeRegistry(cfg.EdgeLabels),
		Substrates: newRouter(cfg.GoogleCredentials, true, cfg.Substrates),
		Now:        time.Now,
	}
}

// TestIntegrationAdmitAndRead admits a node via the ceremony, reads it back,
// confirms the contract + payload + admission provenance round-trip.
func TestIntegrationAdmitAndRead(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := integrationEnv(t)
	defer env.Substrates.Close()

	sub := ArtifactSubmission{
		ArtifactType:   "axiom",
		OrganizationID: "test-org",
		NodeKind:       "axiom",
		Contract:       map[string]interface{}{"asserts": "test contract"},
		Payload:        map[string]interface{}{"title": "Test Axiom"},
	}
	rec, err := AdmitArtifact(ctx, env, sub)
	if err != nil {
		t.Fatalf("AdmitArtifact: %v", err)
	}
	if rec.ArtifactID == "" {
		t.Fatal("expected ArtifactID minted")
	}
	if rec.ArtifactVersion != 1 {
		t.Errorf("ArtifactVersion = %d, want 1", rec.ArtifactVersion)
	}
	if rec.DefinitionHash == "" || rec.ManifestHash == "" {
		t.Errorf("expected hashes populated, got def=%q man=%q", rec.DefinitionHash, rec.ManifestHash)
	}
	if rec.IsNonGenesis {
		t.Error("expected IsNonGenesis=false for EXPERIMENTAL_BOOTSTRAP")
	}

	got, err := readNodeAcrossSubstrates(ctx, env, "", rec.ArtifactID, "", "")
	if err != nil {
		t.Fatalf("readNodeAcrossSubstrates: %v", err)
	}
	if got.NodeKind != "axiom" {
		t.Errorf("NodeKind = %q, want axiom", got.NodeKind)
	}
	if got.Status != "active" {
		t.Errorf("Status = %q, want active", got.Status)
	}
	contractMap := got.CurrentVersion.Contract
	if contractMap["asserts"] != "test contract" {
		t.Errorf("Contract.asserts = %v, want 'test contract'", contractMap["asserts"])
	}
}

// TestIntegrationReciprocityAtomic admits a reciprocal edge pair and confirms
// BOTH edges land with cross-referenced reciprocal_edge_id in one tx.
func TestIntegrationReciprocityAtomic(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := integrationEnv(t)
	defer env.Substrates.Close()

	// Admit two nodes.
	n1Rec, err := AdmitArtifact(ctx, env, ArtifactSubmission{
		ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
		Contract: map[string]interface{}{"n": 1},
	})
	if err != nil {
		t.Fatalf("admit n1: %v", err)
	}
	n2Rec, err := AdmitArtifact(ctx, env, ArtifactSubmission{
		ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
		Contract: map[string]interface{}{"n": 2},
	})
	if err != nil {
		t.Fatalf("admit n2: %v", err)
	}

	// Link with the reciprocal label.
	_, err = handleLink(ctx, env, linkInput{
		SourceID: n1Rec.ArtifactID, TargetID: n2Rec.ArtifactID, EdgeLabel: "load_bearing_for",
	})
	if err != nil {
		t.Fatalf("handleLink: %v", err)
	}

	// Read both edges back via the substrate adapter.
	adapter, _ := env.Substrates.All(ctx)
	edges, err := adapter[0].Neighbors(ctx, n1Rec.ArtifactID, NeighborOpts{Direction: "both"})
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if len(edges) != 2 {
		t.Fatalf("expected 2 edges (primary + mirror), got %d", len(edges))
	}
	var primary, mirror *Edge
	for i, e := range edges {
		switch e.EdgeLabel {
		case "load_bearing_for":
			primary = &edges[i]
		case "depends_on":
			mirror = &edges[i]
		}
	}
	if primary == nil || mirror == nil {
		t.Fatalf("expected one load_bearing_for + one depends_on, got %+v", edges)
	}
	if primary.ReciprocalEdgeID != mirror.EdgeID {
		t.Errorf("primary.ReciprocalEdgeID = %q, want mirror.EdgeID %q", primary.ReciprocalEdgeID, mirror.EdgeID)
	}
	if mirror.ReciprocalEdgeID != primary.EdgeID {
		t.Errorf("mirror.ReciprocalEdgeID = %q, want primary.EdgeID %q", mirror.ReciprocalEdgeID, primary.EdgeID)
	}
}

// TestIntegrationDiffRFC6902 verifies the RFC 6902 diff shape over two
// versions of one node.
func TestIntegrationDiffRFC6902(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := integrationEnv(t)
	defer env.Substrates.Close()

	rec1, err := AdmitArtifact(ctx, env, ArtifactSubmission{
		ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
		Contract: map[string]interface{}{"asserts": "v1"},
		Payload:  map[string]interface{}{"title": "v1"},
	})
	if err != nil {
		t.Fatalf("admit v1: %v", err)
	}

	// Read v1 to seed PriorVersion for v2.
	v1Node, err := readNodeAcrossSubstrates(ctx, env, "", rec1.ArtifactID, "", "")
	if err != nil {
		t.Fatalf("read v1: %v", err)
	}

	_, err = AdmitArtifact(ctx, env, ArtifactSubmission{
		ArtifactType:   "axiom",
		OrganizationID: "test-org",
		NodeKind:       "axiom",
		StableID:       rec1.ArtifactID,
		Contract:       map[string]interface{}{"asserts": "v2", "added": "new"},
		Payload:        map[string]interface{}{"title": "v2"},
		PriorVersion:   &v1Node.CurrentVersion,
	})
	if err != nil {
		t.Fatalf("admit v2: %v", err)
	}

	diff, err := handleDiff(ctx, env, diffInput{
		NodeID: rec1.ArtifactID, VersionA: "1", VersionB: "2",
	})
	if err != nil {
		t.Fatalf("handleDiff: %v", err)
	}
	if diff.Identical {
		t.Error("expected diff to be non-identical")
	}
	if len(diff.Operations) == 0 {
		t.Fatal("expected RFC 6902 operations")
	}
	// Look for a replace or add op at /contract/asserts.
	foundAssertsChange := false
	for _, op := range diff.Operations {
		if strings.HasPrefix(op.Path, "/contract") && (op.Op == "replace" || op.Op == "add") {
			b, _ := json.Marshal(op.Value)
			if strings.Contains(string(b), "v2") || strings.Contains(string(b), "new") {
				foundAssertsChange = true
				break
			}
		}
	}
	if !foundAssertsChange {
		t.Errorf("expected /contract path change reflecting v1→v2, got operations=%+v", diff.Operations)
	}
}

// TestIntegrationImportAtomicAllOrNone is the regression test for the §3.6
// all-or-none property: when handleImport receives a batch where one
// submission fails ceremony validation (Phase 1 prepare), ZERO rows commit
// to the substrate. Under the pre-fix per-submission AdmitArtifact loop, the
// submissions ordered before the malformed one would have committed
// individually; under the Prepare-then-batch-Commit fix, none do.
func TestIntegrationImportAtomicAllOrNone(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := integrationEnv(t)
	defer env.Substrates.Close()

	in := importInput{
		Submissions: []ArtifactSubmission{
			{ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
				Contract: map[string]interface{}{"asserts": "n1"}, Payload: map[string]interface{}{"title": "n1"}},
			{ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
				Contract: map[string]interface{}{"asserts": "n2"}, Payload: map[string]interface{}{"title": "n2"}},
			{ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
				Contract: map[string]interface{}{"asserts": "n3"}, Payload: map[string]interface{}{"title": "n3"}},
			// Malformed: no node_kind, no edge_label → stepSchemaValidate
			// refuses at Phase 1. The three valid submissions are ordered
			// before this one (no $refs → input-order preserved by Kahn).
			{OrganizationID: "test-org"},
		},
	}
	res, err := handleImport(ctx, env, in)
	if err == nil {
		t.Fatalf("expected handleImport error on malformed submission; got nil; result=%+v", res)
	}
	if !strings.Contains(err.Error(), "prepare aborted") && !strings.Contains(err.Error(), "schema_validate") {
		t.Errorf("expected error to mention prepare/schema_validate failure; got: %v", err)
	}
	if res.Admitted != 0 {
		t.Errorf("Admitted = %d, want 0 (all-or-none per REQUIREMENTS.md §3.6)", res.Admitted)
	}

	// Confirm zero Nodes rows hit the substrate.
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		t.Fatalf("substrates.All: %v", err)
	}
	if len(adapters) != 1 {
		t.Fatalf("expected 1 substrate, got %d", len(adapters))
	}
	nodes, err := adapters[0].ListNodes(ctx, ListFilter{
		Status:         "active",
		OrganizationID: "test-org",
		Limit:          100,
	})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes committed after aborted batch, got %d (rows: %+v)", len(nodes), nodes)
	}
}

// TestIntegrationImportHappyPathBatch verifies the success case of the
// Prepare-then-batch-Commit pattern: a valid N-submission batch yields N
// rows committed and the result struct correctly reports both Total and
// Admitted.
func TestIntegrationImportHappyPathBatch(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := integrationEnv(t)
	defer env.Substrates.Close()

	in := importInput{
		Submissions: []ArtifactSubmission{
			{ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
				Contract: map[string]interface{}{"asserts": "h1"}, Payload: map[string]interface{}{"title": "h1"}},
			{ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
				Contract: map[string]interface{}{"asserts": "h2"}, Payload: map[string]interface{}{"title": "h2"}},
			{ArtifactType: "axiom", OrganizationID: "test-org", NodeKind: "axiom",
				Contract: map[string]interface{}{"asserts": "h3"}, Payload: map[string]interface{}{"title": "h3"}},
		},
	}
	res, err := handleImport(ctx, env, in)
	if err != nil {
		t.Fatalf("handleImport: %v", err)
	}
	if res.Total != 3 {
		t.Errorf("Total = %d, want 3", res.Total)
	}
	if res.Admitted != 3 {
		t.Errorf("Admitted = %d, want 3", res.Admitted)
	}
	if len(res.OrderedIDs) != 3 {
		t.Errorf("OrderedIDs len = %d, want 3", len(res.OrderedIDs))
	}

	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		t.Fatalf("substrates.All: %v", err)
	}
	nodes, err := adapters[0].ListNodes(ctx, ListFilter{
		Status:         "active",
		OrganizationID: "test-org",
		Limit:          100,
	})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes committed, got %d", len(nodes))
	}
}

// taskDomainEnv builds an Env wired to the emulator with the REAL task/
// artifact node_kinds + edge_labels from task-graph.config.json (rather than
// the generic axiom/references fixture integrationEnv uses), so the
// task_graph_tasks/_task_counts/_worklinks/_add_worklink/_set_status/_reown
// verbs — which hardcode node_kind='task' in their SQL — can be exercised
// end-to-end against the emulator before touching the real task-graph DB.
func taskDomainEnv(t *testing.T) *Env {
	t.Helper()
	cfg := SpecGraphConfig{
		Mode:                  ModeExperimentalBootstrap,
		ExperimentalAdmitter:  "EXPERIMENTAL_BOOTSTRAP",
		DefaultOrganizationID: "test-org",
		ListenAddress:         "127.0.0.1:0",
		GoogleCredentials:     integrationTestCredentials,
		Substrates: []SubstrateDescriptor{{
			ID:                "test_substrate",
			SpannerDatabase:   emulatorResource,
			NodeTable:         "Nodes",
			EdgeTable:         "Edges",
			AdmissionLogTable: "ArtifactAdmissionLog",
		}},
		NodeKinds: []NodeKindDescriptor{
			NewNodeKindDescriptor(map[string]interface{}{
				"name": "task", "substrate_id": "test_substrate", "artifact_type": "task", "version_scope": "stable_id_preserved",
			}),
			NewNodeKindDescriptor(map[string]interface{}{
				"name": "artifact", "substrate_id": "test_substrate", "artifact_type": "artifact", "version_scope": "stable_id_preserved",
			}),
		},
		EdgeLabels: []EdgeLabelDescriptor{
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "advances", "source_kinds": []interface{}{"artifact"}, "target_kinds": []interface{}{"task"}, "edge_family": "impl", "forbid_self_loops": true,
			}),
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "parent_of", "source_kinds": []interface{}{"task"}, "target_kinds": []interface{}{"task"}, "reciprocal_label": "child_of", "edge_family": "struct", "forbid_self_loops": true,
			}),
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "child_of", "source_kinds": []interface{}{"task"}, "target_kinds": []interface{}{"task"}, "reciprocal_label": "parent_of", "edge_family": "struct", "forbid_self_loops": true,
			}),
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	return &Env{
		Config:     cfg,
		Kinds:      newKindRegistry(cfg.NodeKinds),
		Edges:      newEdgeRegistry(cfg.EdgeLabels),
		Substrates: newRouter(cfg.GoogleCredentials, true, cfg.Substrates),
		Now:        time.Now,
	}
}

func taskSubmission(id, title, pillar, level, status, owner string, isGap bool, gapLabel, note string) ArtifactSubmission {
	return ArtifactSubmission{
		ArtifactType:   "task",
		OrganizationID: "test-org",
		NodeKind:       "task",
		StableID:       id,
		Contract: map[string]interface{}{
			"canonical_name": id, "task_ref": id, "level": level,
		},
		Payload: map[string]interface{}{
			"title": title, "pillar": pillar, "status": status, "owner": owner,
			"is_gap": isGap, "gap_label": gapLabel, "note": note,
		},
	}
}

var nativeProjectSchema = []string{
	`CREATE TABLE Tasks (
		project_id STRING(64) NOT NULL,
		id STRING(64) NOT NULL,
		title STRING(512) NOT NULL,
		pillar STRING(16) NOT NULL,
		layer STRING(8),
		status STRING(32) NOT NULL,
		owner STRING(64) NOT NULL,
		parent_id STRING(64),
		is_gap BOOL NOT NULL,
		note STRING(MAX),
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		gap_label STRING(16),
		session_id STRING(64),
		repo STRING(80),
		is_critical_path BOOL,
		started_at TIMESTAMP,
		done_at TIMESTAMP,
	) PRIMARY KEY (project_id, id)`,
	`CREATE TABLE TaskLinks (
		project_id STRING(64) NOT NULL,
		link_id STRING(64) NOT NULL,
		task_id STRING(64) NOT NULL,
		artifact_type STRING(32) NOT NULL,
		artifact_ref STRING(512) NOT NULL,
		thread STRING(64),
		note STRING(MAX),
		created_at TIMESTAMP NOT NULL,
		session_id STRING(64),
		repo STRING(80),
		branch STRING(200),
		commit_sha STRING(64),
		pr_number INT64,
		pr_state STRING(20),
		worktree_path STRING(300),
		url STRING(500),
		artifact_status STRING(30),
	) PRIMARY KEY (project_id, link_id)`,
	`CREATE TABLE TaskDeps (
		project_id STRING(64) NOT NULL,
		dep_id STRING(40) NOT NULL,
		task_id STRING(64) NOT NULL,
		depends_on_task_id STRING(64) NOT NULL,
		kind STRING(20),
		note STRING(MAX),
		created_at TIMESTAMP,
	) PRIMARY KEY (project_id, dep_id)`,
}

var nativePreCutoverSchema = []string{
	`CREATE TABLE Tasks (
		id STRING(64) NOT NULL,
		title STRING(512) NOT NULL,
		pillar STRING(16) NOT NULL,
		layer STRING(8),
		status STRING(32) NOT NULL,
		owner STRING(64) NOT NULL,
		parent_id STRING(64),
		is_gap BOOL NOT NULL,
		note STRING(MAX),
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		gap_label STRING(16),
		session_id STRING(64),
		repo STRING(80),
		is_critical_path BOOL,
		started_at TIMESTAMP,
		done_at TIMESTAMP,
	) PRIMARY KEY (id)`,
	`CREATE TABLE TaskLinks (
		link_id STRING(64) NOT NULL,
		task_id STRING(64) NOT NULL,
		artifact_type STRING(32) NOT NULL,
		artifact_ref STRING(512) NOT NULL,
		thread STRING(64),
		note STRING(MAX),
		created_at TIMESTAMP NOT NULL,
		session_id STRING(64),
		repo STRING(80),
		branch STRING(200),
		commit_sha STRING(64),
		pr_number INT64,
		pr_state STRING(20),
		worktree_path STRING(300),
		url STRING(500),
		artifact_status STRING(30),
	) PRIMARY KEY (link_id)`,
	`CREATE TABLE TaskDeps (
		dep_id STRING(40) NOT NULL,
		task_id STRING(64) NOT NULL,
		depends_on_task_id STRING(64) NOT NULL,
		kind STRING(20),
		note STRING(MAX),
		created_at TIMESTAMP,
	) PRIMARY KEY (dep_id)`,
}

func nativeProjectEnv(t *testing.T) *Env {
	t.Helper()
	return nativeFleetPlanEnv(t, "project_id")
}

func nativePreCutoverEnv(t *testing.T) *Env {
	t.Helper()
	return nativeFleetPlanEnv(t, "")
}

func nativeFleetPlanEnv(t *testing.T, projectColumn string) *Env {
	t.Helper()
	cfg := SpecGraphConfig{
		Mode:                  ModeExperimentalBootstrap,
		ExperimentalAdmitter:  "EXPERIMENTAL_BOOTSTRAP",
		DefaultOrganizationID: "test-org",
		DefaultProjectID:      "arbor",
		ListenAddress:         "127.0.0.1:0",
		GoogleCredentials:     integrationTestCredentials,
		Substrates: []SubstrateDescriptor{{
			ID:              "native_substrate",
			SpannerDatabase: emulatorResource,
			TaskStoreType:   fleetPlanNativeStoreType,
			ProjectColumn:   projectColumn,
			TaskTable:       "Tasks",
			WorklinkTable:   "TaskLinks",
			DepTable:        "TaskDeps",
		}},
		NodeKinds: []NodeKindDescriptor{
			NewNodeKindDescriptor(map[string]interface{}{
				"name": "task", "substrate_id": "native_substrate", "artifact_type": "task", "version_scope": "stable_id_preserved",
			}),
			NewNodeKindDescriptor(map[string]interface{}{
				"name": "artifact", "substrate_id": "native_substrate", "artifact_type": "artifact", "version_scope": "stable_id_preserved",
			}),
		},
		EdgeLabels: []EdgeLabelDescriptor{
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "advances", "source_kinds": []interface{}{"artifact"}, "target_kinds": []interface{}{"task"}, "edge_family": "impl", "forbid_self_loops": true,
			}),
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "parent_of", "source_kinds": []interface{}{"task"}, "target_kinds": []interface{}{"task"}, "reciprocal_label": "child_of", "edge_family": "struct", "forbid_self_loops": true,
			}),
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "child_of", "source_kinds": []interface{}{"task"}, "target_kinds": []interface{}{"task"}, "reciprocal_label": "parent_of", "edge_family": "struct", "forbid_self_loops": true,
			}),
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "gates", "source_kinds": []interface{}{"task"}, "target_kinds": []interface{}{"task"}, "reciprocal_label": "gated_by", "edge_family": "dep", "forbid_self_loops": true,
			}),
			NewEdgeLabelDescriptor(map[string]interface{}{
				"name": "gated_by", "source_kinds": []interface{}{"task"}, "target_kinds": []interface{}{"task"}, "reciprocal_label": "gates", "edge_family": "dep", "forbid_self_loops": true,
			}),
		},
		// P3-GT-FP-REGISTRIES: mirrors task-graph.config.json's task_statuses/
		// dependency_kinds/artifact_kinds shape (canonical members only — the
		// full `absorbs` reconciliation lists live in the real config and in
		// registries_test.go's exhaustiveness fixture; these tests only need
		// enough members to Resolve() the literals the native-path
		// integration tests actually write). task_statuses' legal_next is
		// fully-connected (every member reaches every other member) — real
		// fleet usage moves a task directly from not_started to done
		// (TestIntegrationNativeProjectNamespaceIsolation) and reopens a done
		// task, so a linear workflow DAG would reject legitimate operations;
		// closing the registry to exactly these 7 canonical values (instead
		// of the 19-literal free-text tail measured live) is the actual
		// defect fix, see registries.go taskStatusTransitionLegal.
		TaskStatuses: []TaskStatusDescriptor{
			NewTaskStatusDescriptor(map[string]interface{}{"name": "not_started", "legal_next": []interface{}{"in_progress", "blocked", "in_review", "done", "cancelled", "archived"}}),
			NewTaskStatusDescriptor(map[string]interface{}{"name": "in_progress", "legal_next": []interface{}{"not_started", "blocked", "in_review", "done", "cancelled", "archived"}}),
			NewTaskStatusDescriptor(map[string]interface{}{"name": "blocked", "legal_next": []interface{}{"not_started", "in_progress", "in_review", "done", "cancelled", "archived"}}),
			NewTaskStatusDescriptor(map[string]interface{}{"name": "in_review", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "done", "cancelled", "archived"}}),
			NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "in_review", "cancelled", "archived"}}),
			NewTaskStatusDescriptor(map[string]interface{}{"name": "cancelled", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "in_review", "done", "archived"}}),
			NewTaskStatusDescriptor(map[string]interface{}{"name": "archived", "legal_next": []interface{}{"not_started", "in_progress", "blocked", "in_review", "done", "cancelled"}}),
		},
		DependencyKinds: []DependencyKindDescriptor{
			NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep", "blocking": true, "legal_next": []interface{}{"archived"}}),
			NewDependencyKindDescriptor(map[string]interface{}{"name": "source_map", "edge_family": "ref", "blocking": false, "legal_next": []interface{}{"archived"}}),
			NewDependencyKindDescriptor(map[string]interface{}{"name": "archived", "edge_family": "dep", "blocking": false, "lifecycle_marker": true, "legal_next": []interface{}{"gates", "source_map"}}),
		},
		ArtifactKinds: []ArtifactKindDescriptor{
			NewArtifactKindDescriptor(map[string]interface{}{"name": "pr"}),
			NewArtifactKindDescriptor(map[string]interface{}{"name": "commit"}),
			NewArtifactKindDescriptor(map[string]interface{}{"name": "finding"}),
			NewArtifactKindDescriptor(map[string]interface{}{"name": "circuit"}),
			NewArtifactKindDescriptor(map[string]interface{}{"name": "spec"}),
			NewArtifactKindDescriptor(map[string]interface{}{"name": "dispatch"}),
			NewArtifactKindDescriptor(map[string]interface{}{"name": "binding"}),
			NewArtifactKindDescriptor(map[string]interface{}{"name": "other"}),
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate native project config: %v", err)
	}
	taskStatuses, err := newTaskStatusRegistry(cfg.TaskStatuses)
	if err != nil {
		t.Fatalf("build task_statuses registry: %v", err)
	}
	dependencyKinds, err := newDependencyKindRegistry(cfg.DependencyKinds)
	if err != nil {
		t.Fatalf("build dependency_kinds registry: %v", err)
	}
	artifactKinds, err := newArtifactKindRegistry(cfg.ArtifactKinds)
	if err != nil {
		t.Fatalf("build artifact_kinds registry: %v", err)
	}
	return &Env{
		Config:          cfg,
		Kinds:           newKindRegistry(cfg.NodeKinds),
		Edges:           newEdgeRegistry(cfg.EdgeLabels),
		Substrates:      newRouter(cfg.GoogleCredentials, true, cfg.Substrates),
		Now:             time.Now,
		TaskStatuses:    taskStatuses,
		DependencyKinds: dependencyKinds,
		ArtifactKinds:   artifactKinds,
	}
}

func createNativeTask(t *testing.T, ctx context.Context, env *Env, projectID, id, title, status string) {
	t.Helper()
	_, err := handleCreate(ctx, env, createInput{
		ProjectID: projectID,
		NodeKind:  "task",
		StableID:  id,
		Contract:  map[string]interface{}{"canonical_name": id, "task_ref": id, "level": "L1"},
		Payload: map[string]interface{}{
			"title": title, "pillar": "P", "layer": "L1", "status": status, "owner": "UNOWNED", "is_gap": false,
		},
	})
	if err != nil {
		t.Fatalf("create native task %s/%s: %v", projectID, id, err)
	}
}

func nativeTaskCreateInput(projectID, id string) createInput {
	return createInput{
		ProjectID: projectID,
		NodeKind:  "task",
		StableID:  id,
		Contract:  map[string]interface{}{"canonical_name": id, "task_ref": id, "level": "L1"},
		Payload: map[string]interface{}{
			"title": id, "pillar": "P", "layer": "L1", "status": "not_started", "owner": "UNOWNED", "is_gap": false,
		},
	}
}

func requireNativeTaskAbsent(t *testing.T, ctx context.Context, env *Env, projectID, id string) {
	t.Helper()
	if _, err := handleGet(ctx, env, getInput{ProjectID: projectID, NodeID: id}, &snapshotPin{}); err == nil {
		t.Fatalf("task %s/%s exists after rejected create", projectID, id)
	}
}

func requireNativeTaskParent(t *testing.T, ctx context.Context, env *Env, projectID, id, wantParent string) {
	t.Helper()
	got, err := handleGet(ctx, env, getInput{ProjectID: projectID, NodeID: id}, &snapshotPin{})
	if err != nil {
		t.Fatalf("get native task %s/%s: %v", projectID, id, err)
	}
	if gotParent := got.CurrentVersion.Payload["parent_id"]; gotParent != wantParent {
		t.Fatalf("task %s/%s parent_id = %v, want %q", projectID, id, gotParent, wantParent)
	}
}

func TestIntegrationNativeCreateParentValidation(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "PARENT", "Parent", "not_started")
	createNativeTask(t, ctx, env, "moonshot", "FOREIGN-PARENT", "Foreign parent", "not_started")

	parentRefOnly := nativeTaskCreateInput("arbor", "CHILD-PARENT-REF")
	parentRefOnly.ParentRef = "PARENT"
	if _, err := handleCreate(ctx, env, parentRefOnly); err != nil {
		t.Fatalf("ParentRef-only create: %v", err)
	}
	requireNativeTaskParent(t, ctx, env, "arbor", "CHILD-PARENT-REF", "PARENT")

	payloadOnly := nativeTaskCreateInput("arbor", "CHILD-PAYLOAD")
	payloadOnly.Payload["parent_id"] = "PARENT"
	if _, err := handleCreate(ctx, env, payloadOnly); err != nil {
		t.Fatalf("payload-only create: %v", err)
	}
	requireNativeTaskParent(t, ctx, env, "arbor", "CHILD-PAYLOAD", "PARENT")

	matching := nativeTaskCreateInput("arbor", "CHILD-MATCHING")
	matching.ParentRef = "PARENT"
	matching.Payload["parent_id"] = "PARENT"
	if _, err := handleCreate(ctx, env, matching); err != nil {
		t.Fatalf("matching dual-input create: %v", err)
	}
	requireNativeTaskParent(t, ctx, env, "arbor", "CHILD-MATCHING", "PARENT")

	failures := []struct {
		name      string
		childID   string
		parentRef string
		payloadID string
	}{
		{name: "mismatch", childID: "CHILD-MISMATCH", parentRef: "PARENT", payloadID: "OTHER"},
		{name: "missing-payload-parent", childID: "CHILD-MISSING", payloadID: "DOES-NOT-EXIST"},
		{name: "cross-project", childID: "CHILD-CROSS-PROJECT", parentRef: "FOREIGN-PARENT"},
		{name: "self", childID: "CHILD-SELF", parentRef: "CHILD-SELF"},
	}
	for _, tc := range failures {
		t.Run(tc.name, func(t *testing.T) {
			in := nativeTaskCreateInput("arbor", tc.childID)
			in.ParentRef = tc.parentRef
			if tc.payloadID != "" {
				in.Payload["parent_id"] = tc.payloadID
			}
			if _, err := handleCreate(ctx, env, in); err == nil {
				t.Fatalf("create should reject %+v", tc)
			}
			requireNativeTaskAbsent(t, ctx, env, "arbor", tc.childID)
		})
	}
}

func TestIntegrationNativeTaskClaimFirstWriterWins(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()
	if _, err := handleCreate(ctx, env, createInput{NodeKind: "task", StableID: "CLAIM-TASK", Contract: map[string]interface{}{"canonical_name": "CLAIM-TASK", "level": "L2"}, Payload: map[string]interface{}{"title": "claim", "pillar": "1", "status": "not_started", "owner": "UNOWNED", "is_gap": false}}); err != nil {
		t.Fatalf("create claim task: %v", err)
	}
	type result struct {
		owner string
		out   MutationWriteResult
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			out, err := handleClaim(ctx, env, claimInput{
				TaskID: "CLAIM-TASK", Owner: owner, SessionID: "session-" + owner, Repo: "task-graph",
				Binding: &ClaimBinding{Branch: "branch-" + owner, WorktreePath: "C:/worktrees/" + owner, Host: "workstation-c", Account: "developer", Harness: "codex", Thread: owner},
			})
			results <- result{owner: owner, out: out, err: err}
		}(owner)
	}
	wg.Wait()
	close(results)
	var winner string
	var winnerOut MutationWriteResult
	var loserErr error
	for r := range results {
		if r.err == nil {
			if winner != "" {
				t.Fatal("both concurrent claims succeeded")
			}
			winner = r.owner
			winnerOut = r.out
		} else {
			loserErr = r.err
		}
	}
	if winner == "" || loserErr == nil || !errors.Is(loserErr, ErrTaskClaimCollision) {
		t.Fatalf("winner=%q loserErr=%v; want one winner and collision", winner, loserErr)
	}
	if !strings.Contains(loserErr.Error(), winner) {
		t.Fatalf("collision %q does not report winning owner %q", loserErr, winner)
	}
	if winnerOut.ClaimReceipt == nil || winnerOut.ClaimReceipt.RowsAffected != 1 || winnerOut.ClaimReceipt.Branch != "branch-"+winner || winnerOut.ClaimReceipt.WorktreePath != "C:/worktrees/"+winner {
		t.Fatalf("winning server claim receipt = %+v, want persisted tuple plus initial optional binding", winnerOut.ClaimReceipt)
	}
	if _, err := handleClaim(ctx, env, claimInput{TaskID: "CLAIM-TASK", Owner: winner, SessionID: "session-" + winner, Repo: "other-repo"}); !errors.Is(err, ErrTaskClaimCollision) {
		t.Fatalf("repository-changing retry error = %v, want ErrTaskClaimCollision", err)
	}
	if _, err := handleClaim(ctx, env, claimInput{TaskID: "CLAIM-TASK", Owner: winner, SessionID: "session-" + winner, Repo: "task-graph", Binding: &ClaimBinding{Branch: "other-branch"}}); err == nil {
		t.Fatal("idempotent retry re-attested a different caller-supplied branch")
	}
	retry, err := handleClaim(ctx, env, claimInput{TaskID: "CLAIM-TASK", Owner: winner, SessionID: "session-" + winner, Repo: "task-graph"})
	if err != nil {
		t.Fatalf("binding-free idempotent retry: %v", err)
	}
	if retry.ClaimReceipt == nil || retry.ClaimReceipt.RowsAffected != 0 || retry.ClaimReceipt.Branch != "" || retry.ClaimReceipt.WorktreePath != "" {
		t.Fatalf("idempotent server claim receipt = %+v, want durable tuple only", retry.ClaimReceipt)
	}
}

// TestIntegrationNativeCreatePersistsCriticalPathAndOtherFields is the
// permanent create-then-read-back regression guard for
// P3-GT-FP-CREATE-DROPS-FIELDS: task_graph_create silently dropped
// is_critical_path (and, once enumerated against task_graph_update's
// accepted field set, session_id, repo, and the payload.level alias for
// layer too) on the task-plan-native write path. This exercises the real
// Spanner emulator end to end — create, then read back via handleGet —
// which the unit-level TestNativeTaskCreateUpdateFieldParity (no emulator,
// pure functions only) cannot reach, since it never runs the actual INSERT
// statement against a real Tasks table.
func TestIntegrationNativeCreatePersistsCriticalPathAndOtherFields(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	in := createInput{
		ProjectID: "arbor",
		NodeKind:  "task",
		StableID:  "CRITICAL-PATH-CREATE",
		Contract:  map[string]interface{}{"canonical_name": "CRITICAL-PATH-CREATE", "task_ref": "CRITICAL-PATH-CREATE"},
		Payload: map[string]interface{}{
			"title": "Critical path create", "pillar": "P", "level": "L2", "status": "not_started",
			"owner": "UNOWNED", "is_gap": false,
			"is_critical_path": true, "session_id": "sess-abc", "repo": "task-graph",
		},
	}
	if _, err := handleCreate(ctx, env, in); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: "CRITICAL-PATH-CREATE"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("get after create: %v", err)
	}
	payload := got.CurrentVersion.Payload
	if v, _ := payload["is_critical_path"].(bool); v != true {
		t.Fatalf("is_critical_path after create = %v, want true (payload=%+v)", payload["is_critical_path"], payload)
	}
	if v, _ := payload["session_id"].(string); v != "sess-abc" {
		t.Fatalf("session_id after create = %q, want %q (payload=%+v)", v, "sess-abc", payload)
	}
	if v, _ := payload["repo"].(string); v != "task-graph" {
		t.Fatalf("repo after create = %q, want %q (payload=%+v)", v, "task-graph", payload)
	}
	if got.CurrentVersion.Contract["level"] != "L2" {
		t.Fatalf("level (from payload.level, no contract.level supplied) after create = %v, want %q (contract=%+v)", got.CurrentVersion.Contract["level"], "L2", got.CurrentVersion.Contract)
	}

	// A task created with is_critical_path omitted entirely must read back
	// NULL/absent, not false — is_critical_path is a nullable column and
	// UPDATE already treats "key absent from the fields map" as "leave the
	// column alone", never as "set it false" (nativeBuildTaskUpdate). CREATE
	// must match that, not the is_gap NOT-NULL-defaults-false convention.
	inNoCP := createInput{
		ProjectID: "arbor",
		NodeKind:  "task",
		StableID:  "NO-CRITICAL-PATH-CREATE",
		Contract:  map[string]interface{}{"canonical_name": "NO-CRITICAL-PATH-CREATE", "task_ref": "NO-CRITICAL-PATH-CREATE", "level": "L1"},
		Payload: map[string]interface{}{
			"title": "No critical path field supplied", "pillar": "P", "status": "not_started",
			"owner": "UNOWNED", "is_gap": false,
		},
	}
	if _, err := handleCreate(ctx, env, inNoCP); err != nil {
		t.Fatalf("create without is_critical_path: %v", err)
	}
	gotNoCP, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: "NO-CRITICAL-PATH-CREATE"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("get task without is_critical_path: %v", err)
	}
	if got, present := gotNoCP.CurrentVersion.Payload["is_critical_path"]; !present || got != nil {
		t.Fatalf("task_graph_get is_critical_path = %#v (present=%v), want explicit nil for SQL NULL", got, present)
	}
	fullPayload := make(map[string]interface{}, len(gotNoCP.CurrentVersion.Payload))
	for key, value := range gotNoCP.CurrentVersion.Payload {
		fullPayload[key] = value
	}
	fullPayload["note"] = "get-update round trip preserved nullable critical path"
	if _, err := handleUpdate(ctx, env, updateInput{
		ProjectID: "arbor",
		NodeID:    "NO-CRITICAL-PATH-CREATE",
		Contract:  gotNoCP.CurrentVersion.Contract,
		Payload:   fullPayload,
	}); err != nil {
		t.Fatalf("full get-update round trip: %v", err)
	}
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		t.Fatalf("substrates: %v", err)
	}
	spannerAdapter, ok := adapters[0].(*SpannerSubstrateAdapter)
	if !ok {
		t.Fatalf("adapter[0] is %T, want *SpannerSubstrateAdapter", adapters[0])
	}
	client, err := spannerAdapter.getClient(ctx)
	if err != nil {
		t.Fatalf("get raw spanner client: %v", err)
	}
	row, err := client.Single().ReadRow(ctx, "Tasks", spanner.Key{"arbor", "NO-CRITICAL-PATH-CREATE"}, []string{"is_critical_path"})
	if err != nil {
		t.Fatalf("read raw row: %v", err)
	}
	var cp spanner.NullBool
	if err := row.Columns(&cp); err != nil {
		t.Fatalf("decode is_critical_path: %v", err)
	}
	if cp.Valid {
		t.Fatalf("is_critical_path after full get-update round trip = Valid:true (%v), want SQL NULL", cp.Bool)
	}

	// Explicit JSON null must also restore a concrete critical-path value to
	// SQL NULL without relying on a full payload replay.
	if _, err := handleUpdate(ctx, env, updateInput{
		ProjectID: "arbor",
		NodeID:    "CRITICAL-PATH-CREATE",
		Payload:   map[string]interface{}{"is_critical_path": nil},
	}); err != nil {
		t.Fatalf("explicit-null critical-path update: %v", err)
	}
	row, err = client.Single().ReadRow(ctx, "Tasks", spanner.Key{"arbor", "CRITICAL-PATH-CREATE"}, []string{"is_critical_path"})
	if err != nil {
		t.Fatalf("read explicit-null row: %v", err)
	}
	if err := row.Columns(&cp); err != nil {
		t.Fatalf("decode explicit-null is_critical_path: %v", err)
	}
	if cp.Valid {
		t.Fatalf("explicit null update produced Valid:true (%v), want SQL NULL", cp.Bool)
	}
}

// TestIntegrationNativeAddWorklinkPersistsProvenanceFields is the permanent
// create-then-read-back regression guard for
// P3-GT-FP-WORKLINK-FIELD-PARITY: task_graph_add_worklink's task-plan-native
// INSERT named its own literal eight-column list while
// nativeUpdateWorklinkFields accepted thirteen allow-listed fields PLUS a
// pr_number special case, so session_id, repo, branch, commit_sha, pr_number,
// pr_state, worktree_path and url were persisted by UPDATE and silently
// dropped by CREATE. This runs the real INSERT against a real TaskLinks table
// on the Spanner emulator and reads the row back through handleGet, which the
// unit-level TestNativeWorklinkCreateUpdateFieldParity (pure functions only)
// cannot reach.
//
// Both create entrypoints are covered: the typed task_graph_add_worklink
// surface, and task_graph_create(node_kind="artifact"), whose open-ended
// payload map carried the identical drop.
func TestIntegrationNativeAddWorklinkPersistsProvenanceFields(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "arbor", "WORKLINK-PROVENANCE", "task", "not_started")

	wantStrings := map[string]string{
		"thread":        "gt-w1-fp-worklink-parity",
		"note":          "provenance round trip",
		"session_id":    "sess-worklink-1",
		"repo":          "task-graph",
		"branch":        "gt-w1-fp-worklink-parity",
		"commit_sha":    "0123456789abcdef0123456789abcdef01234567",
		"pr_state":      "open",
		"worktree_path": "/workspace/arbor/worktrees/gt-w1-fp-worklink-parity-wt",
		"url":           "https://github.com/velenzaboc/velenza-trellis/pull/4242",
	}

	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID:    "arbor",
		TaskID:       "WORKLINK-PROVENANCE",
		ArtifactID:   "LNK-PROVENANCE",
		ArtifactType: "pr",
		ArtifactRef:  "https://github.com/velenzaboc/velenza-trellis/pull/4242",
		Thread:       wantStrings["thread"],
		Note:         wantStrings["note"],
		SessionID:    wantStrings["session_id"],
		Repo:         wantStrings["repo"],
		Branch:       wantStrings["branch"],
		CommitSHA:    wantStrings["commit_sha"],
		PRNumber:     4242,
		PRState:      wantStrings["pr_state"],
		WorktreePath: wantStrings["worktree_path"],
		URL:          wantStrings["url"],
	}); err != nil {
		t.Fatalf("add_worklink: %v", err)
	}

	got, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: "LNK-PROVENANCE"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("get after add_worklink: %v", err)
	}
	payload := got.CurrentVersion.Payload
	for field, want := range wantStrings {
		if v, _ := payload[field].(string); v != want {
			t.Fatalf("%s after add_worklink = %q, want %q (payload=%+v)", field, v, want, payload)
		}
	}
	if v, _ := payload["pr_number"].(int); v != 4242 {
		t.Fatalf("pr_number after add_worklink = %v, want 4242 (payload=%+v)", payload["pr_number"], payload)
	}
	if v, _ := payload["artifact_status"].(string); v != "recorded" {
		t.Fatalf("artifact_status after add_worklink = %q, want %q", v, "recorded")
	}

	// The SECOND create entrypoint: an open-ended payload through
	// task_graph_create(node_kind="artifact"). It needs no schema widening to
	// carry these keys and dropped every one of them all the same.
	if _, err := handleCreate(ctx, env, createInput{
		ProjectID: "arbor",
		NodeKind:  "artifact",
		StableID:  "LNK-PROVENANCE-CREATE",
		Contract: map[string]interface{}{
			"canonical_name":     "LNK-PROVENANCE-CREATE",
			"artifact_type_kind": "commit",
			"artifact_ref":       "0123456789abcdef0123456789abcdef01234567",
		},
		Payload: map[string]interface{}{
			"task_id":       "WORKLINK-PROVENANCE",
			"thread":        wantStrings["thread"],
			"note":          wantStrings["note"],
			"session_id":    wantStrings["session_id"],
			"repo":          wantStrings["repo"],
			"branch":        wantStrings["branch"],
			"commit_sha":    wantStrings["commit_sha"],
			"pr_number":     float64(4242), // the shape a JSON decode delivers
			"pr_state":      wantStrings["pr_state"],
			"worktree_path": wantStrings["worktree_path"],
			"url":           wantStrings["url"],
		},
	}); err != nil {
		t.Fatalf("create(node_kind=artifact): %v", err)
	}
	gotCreate, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: "LNK-PROVENANCE-CREATE"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("get after create(node_kind=artifact): %v", err)
	}
	createPayload := gotCreate.CurrentVersion.Payload
	for field, want := range wantStrings {
		if v, _ := createPayload[field].(string); v != want {
			t.Fatalf("%s after create(node_kind=artifact) = %q, want %q (payload=%+v)", field, v, want, createPayload)
		}
	}
	if v, _ := createPayload["pr_number"].(int); v != 4242 {
		t.Fatalf("pr_number after create(node_kind=artifact) = %v, want 4242 (payload=%+v)", createPayload["pr_number"], createPayload)
	}

	// A worklink created with no PR attached must read back SQL NULL, not 0 —
	// pr_number is a nullable INT64 column and UPDATE already treats an absent
	// key as "leave the column alone". Same NULL-not-zero contract as
	// is_critical_path on the task lane.
	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID: "arbor", TaskID: "WORKLINK-PROVENANCE", ArtifactID: "LNK-NO-PR",
		ArtifactType: "finding", ArtifactRef: "no pr attached",
	}); err != nil {
		t.Fatalf("add_worklink without pr_number: %v", err)
	}
	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		t.Fatalf("substrates: %v", err)
	}
	spannerAdapter, ok := adapters[0].(*SpannerSubstrateAdapter)
	if !ok {
		t.Fatalf("adapter[0] is %T, want *SpannerSubstrateAdapter", adapters[0])
	}
	client, err := spannerAdapter.getClient(ctx)
	if err != nil {
		t.Fatalf("get raw spanner client: %v", err)
	}
	row, err := client.Single().ReadRow(ctx, "TaskLinks", spanner.Key{"arbor", "LNK-NO-PR"}, []string{"pr_number"})
	if err != nil {
		t.Fatalf("read raw row: %v", err)
	}
	var pr spanner.NullInt64
	if err := row.Columns(&pr); err != nil {
		t.Fatalf("decode pr_number: %v", err)
	}
	if pr.Valid {
		t.Fatalf("pr_number on an add_worklink that never mentioned it = Valid:true (%d), want SQL NULL", pr.Int64)
	}
}

func TestIntegrationNativeCreateWithoutParentPreAndPostCutover(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema []string
		env    func(*testing.T) *Env
	}{
		{name: "pre-cutover", schema: nativePreCutoverSchema, env: nativePreCutoverEnv},
		{name: "post-cutover", schema: nativeProjectSchema, env: nativeProjectEnv},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireEmulator(t)
			ctx := context.Background()
			setupEmulatorDBWithSchema(ctx, t, tc.schema)
			env := tc.env(t)
			defer env.Substrates.Close()

			in := nativeTaskCreateInput("", "ROOT-TASK")
			if _, err := handleCreate(ctx, env, in); err != nil {
				t.Fatalf("no-parent create: %v", err)
			}
			requireNativeTaskParent(t, ctx, env, "", "ROOT-TASK", "")
		})
	}
}

func TestIntegrationNativePreCutoverRuntimeConfig(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativePreCutoverSchema)
	env := nativePreCutoverEnv(t)
	defer env.Substrates.Close()

	adapters, err := env.Substrates.All(ctx)
	if err != nil {
		t.Fatalf("substrates: %v", err)
	}
	if err := adapters[0].CheckSchema(ctx); err != nil {
		t.Fatalf("pre-cutover native schema should pass health: %v", err)
	}

	createNativeTask(t, ctx, env, "", "ARBOR-LEGACY", "Arbor legacy", "not_started")
	got, err := handleGet(ctx, env, getInput{NodeID: "ARBOR-LEGACY"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("default get before cutover: %v", err)
	}
	if got.ProjectID != "arbor" || got.CurrentVersion.Contract["project_id"] != "arbor" {
		t.Fatalf("default get stamped project_id %q contract %+v, want arbor", got.ProjectID, got.CurrentVersion.Contract)
	}

	if _, err := handleTasks(ctx, env, tasksInput{ProjectID: "moonshot"}); err == nil || !strings.Contains(err.Error(), "requires task-plan project namespace cutover") {
		t.Fatalf("non-default project list before cutover should fail closed, got %v", err)
	}
	if _, err := handleCreate(ctx, env, createInput{
		ProjectID: "moonshot",
		NodeKind:  "task",
		StableID:  "MOONSHOT",
		Contract:  map[string]interface{}{"canonical_name": "MOONSHOT", "task_ref": "MOONSHOT", "level": "L1"},
		Payload:   map[string]interface{}{"title": "Moonshot", "pillar": "P", "layer": "L1", "status": "not_started", "owner": "UNOWNED", "is_gap": false},
	}); err == nil || !strings.Contains(err.Error(), "requires task-plan project namespace cutover") {
		t.Fatalf("non-default project create before cutover should fail closed, got %v", err)
	}
}

func TestIntegrationNativeDependencyLinkIsIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema []string
		env    func(*testing.T) *Env
	}{
		{name: "pre-cutover", schema: nativePreCutoverSchema, env: nativePreCutoverEnv},
		{name: "post-cutover", schema: nativeProjectSchema, env: nativeProjectEnv},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireEmulator(t)
			ctx := context.Background()
			setupEmulatorDBWithSchema(ctx, t, tc.schema)
			env := tc.env(t)
			defer env.Substrates.Close()

			createNativeTask(t, ctx, env, "arbor", "BLOCKER", "Blocker", "not_started")
			createNativeTask(t, ctx, env, "arbor", "BLOCKED", "Blocked", "not_started")

			first, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "BLOCKER", TargetID: "BLOCKED", EdgeLabel: "gates"})
			if err != nil {
				t.Fatalf("first native gates link: %v", err)
			}
			second, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "BLOCKER", TargetID: "BLOCKED", EdgeLabel: "gates"})
			if err != nil {
				t.Fatalf("repeated native gates link: %v", err)
			}
			if first.NativeMutationReceipt == nil || second.NativeMutationReceipt == nil {
				t.Fatalf("native dependency receipts = first %+v second %+v", first, second)
			}
			if first.NativeMutationReceipt.ArtifactID != second.NativeMutationReceipt.ArtifactID {
				t.Fatalf("repeated link identities = %q and %q, want one stable dep_id", first.NativeMutationReceipt.ArtifactID, second.NativeMutationReceipt.ArtifactID)
			}
			if second.NativeMutationReceipt.RowsAffected != 0 {
				t.Fatalf("repeated link rows_affected = %d, want 0", second.NativeMutationReceipt.RowsAffected)
			}
			inverse, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "BLOCKED", TargetID: "BLOCKER", EdgeLabel: "gated_by"})
			if err != nil {
				t.Fatalf("reciprocal native gated_by link: %v", err)
			}
			if inverse.NativeMutationReceipt == nil || inverse.NativeMutationReceipt.ArtifactID != first.NativeMutationReceipt.ArtifactID || inverse.NativeMutationReceipt.RowsAffected != 0 {
				t.Fatalf("reciprocal link receipt = %+v, want existing dep_id %q with zero rows", inverse.NativeMutationReceipt, first.NativeMutationReceipt.ArtifactID)
			}

			adapter, err := soleNativeFleetPlanAdapter(ctx, env)
			if err != nil || adapter == nil {
				t.Fatalf("native adapter: %v", err)
			}
			res, err := adapter.ExecuteReadOnlyQuery(ctx, `SELECT COUNT(*) AS n FROM TaskDeps
		WHERE `+adapter.projectWhereAnd()+`task_id = @task_id AND depends_on_task_id = @depends_on_task_id
		AND COALESCE(kind, 'gates') != 'archived'`, map[string]interface{}{
				"project_id": "arbor", "task_id": "BLOCKED", "depends_on_task_id": "BLOCKER",
			})
			if err != nil {
				t.Fatalf("count native dependency rows: %v", err)
			}
			if len(res.Rows) != 1 || intField(res.Rows[0], "n") != 1 {
				t.Fatalf("native dependency row count = %+v, want exactly one", res.Rows)
			}
		})
	}
}

func TestIntegrationNativeNeighborEdgeIDUnlinksReciprocalDependency(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema []string
		env    func(*testing.T) *Env
	}{
		{name: "pre-cutover", schema: nativePreCutoverSchema, env: nativePreCutoverEnv},
		{name: "post-cutover", schema: nativeProjectSchema, env: nativeProjectEnv},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireEmulator(t)
			ctx := context.Background()
			setupEmulatorDBWithSchema(ctx, t, tc.schema)
			env := tc.env(t)
			defer env.Substrates.Close()

			createNativeTask(t, ctx, env, "arbor", "BLOCKER", "Blocker", "not_started")
			createNativeTask(t, ctx, env, "arbor", "BLOCKED", "Blocked", "not_started")
			linked, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "BLOCKER", TargetID: "BLOCKED", EdgeLabel: "gates"})
			if err != nil {
				t.Fatalf("native gates link: %v", err)
			}
			depID := linked.NativeMutationReceipt.ArtifactID

			gates, err := handleNeighbors(ctx, env, neighborsInput{ProjectID: "arbor", NodeID: "BLOCKER", Direction: "out", EdgeLabels: []string{"gates"}}, &snapshotPin{})
			if err != nil {
				t.Fatalf("gates neighbors: %v", err)
			}
			if len(gates.Edges) != 1 || gates.Edges[0].EdgeID != depID {
				t.Fatalf("gates neighbors = %+v, want exact dep_id %q", gates.Edges, depID)
			}
			gatedBy, err := handleNeighbors(ctx, env, neighborsInput{ProjectID: "arbor", NodeID: "BLOCKED", Direction: "out", EdgeLabels: []string{"gated_by"}}, &snapshotPin{})
			if err != nil {
				t.Fatalf("gated_by neighbors: %v", err)
			}
			if len(gatedBy.Edges) != 1 || gatedBy.Edges[0].EdgeID != depID || gatedBy.Edges[0].ReciprocalEdgeID != depID {
				t.Fatalf("gated_by neighbors = %+v, want reciprocal projections sharing dep_id %q", gatedBy.Edges, depID)
			}

			unlinked, err := handleUnlink(ctx, env, unlinkInput{EdgeID: gates.Edges[0].EdgeID})
			if err != nil {
				t.Fatalf("unlink exact neighbors edge_id %q: %v", gates.Edges[0].EdgeID, err)
			}
			if unlinked.NativeMutationReceipt == nil || unlinked.NativeMutationReceipt.RowsAffected != 1 {
				t.Fatalf("unlink receipt = %+v, want one archived native dependency", unlinked.NativeMutationReceipt)
			}
			for _, tc := range []struct {
				nodeID string
				label  string
			}{
				{nodeID: "BLOCKER", label: "gates"},
				{nodeID: "BLOCKED", label: "gated_by"},
			} {
				edges, err := handleNeighbors(ctx, env, neighborsInput{ProjectID: "arbor", NodeID: tc.nodeID, Direction: "out", EdgeLabels: []string{tc.label}}, &snapshotPin{})
				if err != nil {
					t.Fatalf("%s neighbors after unlink: %v", tc.label, err)
				}
				if len(edges.Edges) != 0 {
					t.Fatalf("%s neighbors after unlink = %+v, want none", tc.label, edges.Edges)
				}
			}
			idempotent, err := handleUnlink(ctx, env, unlinkInput{EdgeID: depID})
			if err != nil {
				t.Fatalf("idempotent native unlink: %v", err)
			}
			if idempotent.NativeMutationReceipt == nil || idempotent.NativeMutationReceipt.RowsAffected != 0 {
				t.Fatalf("idempotent unlink receipt = %+v, want zero rows affected", idempotent.NativeMutationReceipt)
			}
			relinked, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "BLOCKER", TargetID: "BLOCKED", EdgeLabel: "gates"})
			if err != nil {
				t.Fatalf("relink archived native dependency: %v", err)
			}
			if relinked.NativeMutationReceipt == nil || relinked.NativeMutationReceipt.ArtifactID != depID || relinked.NativeMutationReceipt.RowsAffected != 1 {
				t.Fatalf("relink receipt = %+v, want reactivated dep_id %q", relinked.NativeMutationReceipt, depID)
			}
		})
	}
}

// TestIntegrationNativeClosureAndImpactWalkDependencyChain is the regression
// test for the node_table-not-configured bug: task_graph_closure and
// task_graph_impact used to call SpannerSubstrateAdapter.TraverseClosure
// unconditionally, which required a schema_v1 node_table/edge_table even on
// a fleet_plan_native substrate (task_graph_iter1 declares task_table /
// worklink_table / dep_table instead — see task-graph.config.json). Both
// verbs failed closed with `substrate "…": node_table not configured` on
// every fleet_plan_native deployment, even though the ordinary read/write
// verbs (get/list/create/link/neighbors) worked fine. TraverseClosure now
// branches on isFleetPlanNative() into nativeTraverseClosure, which walks
// TaskDeps one hop at a time via nativeNeighbors instead of a GRAPH_TABLE
// query. This seeds A --gates--> B --gates--> C and checks both directions.
func TestIntegrationNativeClosureAndImpactWalkDependencyChain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema []string
		env    func(*testing.T) *Env
	}{
		{name: "pre-cutover", schema: nativePreCutoverSchema, env: nativePreCutoverEnv},
		{name: "post-cutover", schema: nativeProjectSchema, env: nativeProjectEnv},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireEmulator(t)
			ctx := context.Background()
			setupEmulatorDBWithSchema(ctx, t, tc.schema)
			env := tc.env(t)
			defer env.Substrates.Close()

			createNativeTask(t, ctx, env, "arbor", "A", "A", "not_started")
			createNativeTask(t, ctx, env, "arbor", "B", "B", "not_started")
			createNativeTask(t, ctx, env, "arbor", "C", "C", "not_started")
			if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "A", TargetID: "B", EdgeLabel: "gates"}); err != nil {
				t.Fatalf("link A gates B: %v", err)
			}
			if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "B", TargetID: "C", EdgeLabel: "gates"}); err != nil {
				t.Fatalf("link B gates C: %v", err)
			}

			closure, err := handleClosure(ctx, env, closureInput{ProjectID: "arbor", NodeID: "A", ClosureProfile: "dep", MaxDepth: 3, ResultCap: 100}, &snapshotPin{})
			if err != nil {
				t.Fatalf("closure from A: %v", err)
			}
			if got := nativeNodeIDSet(closure.Nodes); !got["B"] || !got["C"] || len(got) != 2 {
				t.Fatalf("closure from A = %+v, want exactly {B, C}", closure.Nodes)
			}

			impact, err := handleImpact(ctx, env, impactInput{ProjectID: "arbor", NodeID: "C", MaxDepth: 3, ResultCap: 100}, &snapshotPin{})
			if err != nil {
				t.Fatalf("impact on C: %v", err)
			}
			if got := nativeNodeIDSet(impact.Nodes); !got["A"] || !got["B"] || len(got) != 2 {
				t.Fatalf("impact on C = %+v, want exactly {A, B}", impact.Nodes)
			}

			// A one-hop cap must stop the walk after the immediate neighbor.
			shallow, err := handleClosure(ctx, env, closureInput{ProjectID: "arbor", NodeID: "A", ClosureProfile: "dep", MaxDepth: 1, ResultCap: 100}, &snapshotPin{})
			if err != nil {
				t.Fatalf("depth-1 closure from A: %v", err)
			}
			if got := nativeNodeIDSet(shallow.Nodes); !got["B"] || got["C"] || len(got) != 1 {
				t.Fatalf("depth-1 closure from A = %+v, want exactly {B}", shallow.Nodes)
			}
		})
	}
}

func nativeNodeIDSet(nodes []Node) map[string]bool {
	out := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		out[n.NodeID] = true
	}
	return out
}

func TestIntegrationNativeProjectNamespaceIsolation(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, ctx, env, "", "SHARED", "Arbor shared id", "not_started")
	createNativeTask(t, ctx, env, "moonshot", "SHARED", "Moonshot shared id", "blocked")
	createNativeTask(t, ctx, env, "moonshot", "MOON-CHILD", "Moonshot child", "not_started")
	createNativeTask(t, ctx, env, "arbor", "ARBOR-ONLY", "Arbor only", "not_started")

	gotDefault, err := handleGet(ctx, env, getInput{NodeID: "SHARED"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("default handleGet SHARED: %v", err)
	}
	if gotDefault.ProjectID != "arbor" || gotDefault.CurrentVersion.Payload["title"] != "Arbor shared id" {
		t.Fatalf("default get = project %q payload %+v, want arbor Arbor shared id", gotDefault.ProjectID, gotDefault.CurrentVersion.Payload)
	}
	gotMoon, err := handleGet(ctx, env, getInput{ProjectID: "moonshot", NodeID: "SHARED"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("moonshot handleGet SHARED: %v", err)
	}
	if gotMoon.ProjectID != "moonshot" || gotMoon.CurrentVersion.Payload["title"] != "Moonshot shared id" {
		t.Fatalf("moonshot get = project %q payload %+v", gotMoon.ProjectID, gotMoon.CurrentVersion.Payload)
	}

	defaultTasks, err := handleTasks(ctx, env, tasksInput{})
	if err != nil {
		t.Fatalf("default handleTasks: %v", err)
	}
	for _, task := range defaultTasks.Tasks {
		if task.ProjectID != "arbor" {
			t.Fatalf("default task listing leaked project %q row %+v", task.ProjectID, task)
		}
	}
	moonTasks, err := handleTasks(ctx, env, tasksInput{ProjectID: "moonshot"})
	if err != nil {
		t.Fatalf("moonshot handleTasks: %v", err)
	}
	for _, task := range moonTasks.Tasks {
		if task.ProjectID != "moonshot" {
			t.Fatalf("moonshot task listing leaked project %q row %+v", task.ProjectID, task)
		}
	}

	if _, err := handleSetStatus(ctx, env, setStatusInput{TaskID: "SHARED", Status: "done"}); err != nil {
		t.Fatalf("default set_status SHARED: %v", err)
	}
	moonAfterDefaultUpdate, err := handleGet(ctx, env, getInput{ProjectID: "moonshot", NodeID: "SHARED"}, &snapshotPin{})
	if err != nil {
		t.Fatalf("moonshot get after default update: %v", err)
	}
	if moonAfterDefaultUpdate.CurrentVersion.Payload["status"] != "blocked" {
		t.Fatalf("default update leaked to moonshot status=%v", moonAfterDefaultUpdate.CurrentVersion.Payload["status"])
	}

	if _, err := handleLink(ctx, env, linkInput{ProjectID: "moonshot", SourceID: "SHARED", TargetID: "MOON-CHILD", EdgeLabel: "parent_of"}); err != nil {
		t.Fatalf("moonshot parent link: %v", err)
	}
	moonEdges, err := handleNeighbors(ctx, env, neighborsInput{ProjectID: "moonshot", NodeID: "SHARED", Direction: "out", EdgeLabels: []string{"parent_of"}}, &snapshotPin{})
	if err != nil {
		t.Fatalf("moonshot neighbors: %v", err)
	}
	if len(moonEdges.Edges) != 1 || moonEdges.Edges[0].ProjectID != "moonshot" || moonEdges.Edges[0].TargetID != "MOON-CHILD" {
		t.Fatalf("moonshot parent edges = %+v, want one moonshot SHARED->MOON-CHILD", moonEdges.Edges)
	}
	defaultEdges, err := handleNeighbors(ctx, env, neighborsInput{NodeID: "SHARED", Direction: "out", EdgeLabels: []string{"parent_of"}}, &snapshotPin{})
	if err != nil {
		t.Fatalf("default neighbors: %v", err)
	}
	if len(defaultEdges.Edges) != 0 {
		t.Fatalf("default neighbors leaked moonshot edge: %+v", defaultEdges.Edges)
	}

	if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "ARBOR-ONLY", TargetID: "MOON-CHILD", EdgeLabel: "gates"}); err == nil {
		t.Fatal("expected cross-project gates link to fail because MOON-CHILD is absent from project arbor")
	}

	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID: "moonshot", TaskID: "SHARED", ArtifactID: "LNK-SHARED", ArtifactType: "commit", ArtifactRef: "abc123",
	}); err != nil {
		t.Fatalf("moonshot add worklink: %v", err)
	}
	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID: "moonshot", TaskID: "SHARED", ArtifactID: "LNK-KEEP", ArtifactType: "commit", ArtifactRef: "def456",
	}); err != nil {
		t.Fatalf("moonshot add authoritative worklink: %v", err)
	}
	moonLinks, err := handleWorklinks(ctx, env, worklinksInput{ProjectID: "moonshot", TaskID: "SHARED"})
	if err != nil {
		t.Fatalf("moonshot worklinks: %v", err)
	}
	if len(moonLinks.Worklinks) != 2 {
		t.Fatalf("moonshot worklinks = %+v, want two moonshot links", moonLinks.Worklinks)
	}
	for _, link := range moonLinks.Worklinks {
		if link.ProjectID != "moonshot" {
			t.Fatalf("moonshot worklinks leaked project %q row %+v", link.ProjectID, link)
		}
	}
	defaultLinks, err := handleWorklinks(ctx, env, worklinksInput{TaskID: "SHARED"})
	if err != nil {
		t.Fatalf("default worklinks: %v", err)
	}
	if len(defaultLinks.Worklinks) != 0 {
		t.Fatalf("default worklinks leaked moonshot link: %+v", defaultLinks.Worklinks)
	}

	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: "SHARED", LinkID: "LNK-SHARED"}); err == nil {
		t.Fatal("expected cross-project archive to fail because LNK-SHARED is absent from project arbor")
	}
	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "moonshot", TaskID: "MISSING", LinkID: "LNK-SHARED"}); err == nil || !strings.Contains(err.Error(), `task "MISSING" not found`) {
		t.Fatalf("missing-task archive should fail existence check, got %v", err)
	}
	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "moonshot", TaskID: "MOON-CHILD", LinkID: "LNK-SHARED"}); err == nil || !strings.Contains(err.Error(), `belongs to task "SHARED"`) {
		t.Fatalf("wrong-task archive should fail ownership check, got %v", err)
	}
	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "moonshot", TaskID: "SHARED", LinkID: "LNK-MISSING"}); err == nil || !strings.Contains(err.Error(), `worklink "LNK-MISSING" not found`) {
		t.Fatalf("unknown-link archive should fail existence check, got %v", err)
	}
	archiveResult, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "moonshot", TaskID: "SHARED", LinkID: "LNK-SHARED"})
	if err != nil {
		t.Fatalf("archive owned worklink: %v", err)
	}
	archiveReceipt := archiveResult.NativeMutationReceipt
	if archiveReceipt == nil || archiveReceipt.Operation != "archive_worklink" || archiveReceipt.ProjectID != "moonshot" || archiveReceipt.ArtifactID != "LNK-SHARED" || archiveReceipt.RowsAffected != 1 {
		t.Fatalf("archive receipt = %+v, want native archive_worklink receipt with one row", archiveReceipt)
	}
	remainingLinks, err := handleWorklinks(ctx, env, worklinksInput{ProjectID: "moonshot", TaskID: "SHARED"})
	if err != nil {
		t.Fatalf("worklinks after archive: %v", err)
	}
	if len(remainingLinks.Worklinks) != 1 || remainingLinks.Worklinks[0].ArtifactID != "LNK-KEEP" {
		t.Fatalf("worklinks after archive = %+v, want only LNK-KEEP", remainingLinks.Worklinks)
	}
	idempotentResult, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "moonshot", TaskID: "SHARED", LinkID: "LNK-SHARED"})
	if err != nil {
		t.Fatalf("idempotent re-archive: %v", err)
	}
	if idempotentResult.NativeMutationReceipt == nil || idempotentResult.NativeMutationReceipt.RowsAffected != 0 {
		t.Fatalf("idempotent receipt = %+v, want zero rows affected", idempotentResult.NativeMutationReceipt)
	}

	counts, err := handleTaskCounts(ctx, env, taskCountsInput{ProjectID: "moonshot"})
	if err != nil {
		t.Fatalf("moonshot counts: %v", err)
	}
	var total int
	for _, group := range counts.ByPillarStatus {
		total += group.Count
	}
	if total != 2 {
		t.Fatalf("moonshot grouped count total = %d, want 2", total)
	}
	var canonicalTotal int
	for _, group := range counts.ByPillarCanonicalStatus {
		canonicalTotal += group.Count
	}
	if canonicalTotal != 2 || len(counts.UnrecognizedStatusLiterals) != 0 {
		t.Fatalf("moonshot canonical counts = %+v (unrecognized %q), want both rows under task_statuses members", counts.ByPillarCanonicalStatus, counts.UnrecognizedStatusLiterals)
	}

	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		t.Fatalf("native adapter: %v", err)
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	row, err := client.Single().ReadRow(ctx, "Tasks", spanner.Key{"arbor", "SHARED"}, []string{"status"})
	if err != nil {
		t.Fatalf("read arbor/SHARED: %v", err)
	}
	var status string
	if err := row.Columns(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status != "done" {
		t.Fatalf("arbor/SHARED status = %q, want done", status)
	}
}

// TestIntegrationTaskDomainVerbs proves the six task_graph_* recipe verbs
// (task_graph_tasks / _task_counts / _worklinks / _add_worklink /
// _set_status / _reown) against a small task-domain graph on the emulator —
// the de-risking step before running the real task-plan migration.
func TestIntegrationTaskDomainVerbs(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := taskDomainEnv(t)
	defer env.Substrates.Close()

	p3 := taskSubmission("P3", "PILLAR 3", "3", "L2", "in_progress", "C-Suite", false, "", "")
	p3.Payload["session_id"] = "csuite-session"
	p3.Payload["repo"] = "task-graph"
	in := importInput{Submissions: []ArtifactSubmission{
		taskSubmission("GOAL", "Working business OS", "0", "L2", "in_progress", "UNOWNED", false, "", ""),
		taskSubmission("P2", "PILLAR 2", "2", "L2", "in_progress", "UNOWNED", false, "", "substrate only"),
		taskSubmission("P2-DATA", "Real DATA", "2", "L2", "gap", "UNOWNED", true, "GAP-A", "unverified"),
		taskSubmission("P2-CONNECTORS", "Connector Vault", "2", "L1", "gap", "UNOWNED", true, "GAP-A", "no live connectors"),
		p3,
		{ArtifactType: "edge", OrganizationID: "test-org", EdgeLabel: "parent_of", EdgeSourceID: "GOAL", EdgeTargetID: "P2"},
		{ArtifactType: "edge", OrganizationID: "test-org", EdgeLabel: "parent_of", EdgeSourceID: "P2", EdgeTargetID: "P2-DATA"},
		{ArtifactType: "edge", OrganizationID: "test-org", EdgeLabel: "parent_of", EdgeSourceID: "P2", EdgeTargetID: "P2-CONNECTORS"},
		{ArtifactType: "edge", OrganizationID: "test-org", EdgeLabel: "parent_of", EdgeSourceID: "GOAL", EdgeTargetID: "P3"},
	}}
	res, err := handleImport(ctx, env, in)
	if err != nil {
		t.Fatalf("handleImport: %v", err)
	}
	if res.Admitted != 9 {
		t.Fatalf("Admitted = %d, want 9 (5 tasks + 4 edges)", res.Admitted)
	}

	// task_graph_tasks: full listing.
	all, err := handleTasks(ctx, env, tasksInput{})
	if err != nil {
		t.Fatalf("handleTasks: %v", err)
	}
	if len(all.Tasks) != 5 {
		t.Fatalf("handleTasks full listing: got %d tasks, want 5", len(all.Tasks))
	}

	// task_graph_tasks: unowned gaps (is_gap=true, owner=UNOWNED).
	isGap := true
	gaps, err := handleTasks(ctx, env, tasksInput{IsGap: &isGap, Owner: "UNOWNED"})
	if err != nil {
		t.Fatalf("handleTasks(gaps): %v", err)
	}
	if len(gaps.Tasks) != 2 {
		t.Fatalf("unowned gaps: got %d, want 2 (P2-DATA, P2-CONNECTORS)", len(gaps.Tasks))
	}

	// task_graph_tasks: by owner.
	csuite, err := handleTasks(ctx, env, tasksInput{Owner: "C-Suite"})
	if err != nil {
		t.Fatalf("handleTasks(owner): %v", err)
	}
	if len(csuite.Tasks) != 1 || csuite.Tasks[0].TaskID != "P3" {
		t.Fatalf("owner=C-Suite: got %+v, want [P3]", csuite.Tasks)
	}

	// task_graph_task_counts: distinct gap count must be 1 (GAP-A maps to
	// two tasks) even though gap_task_count is 2.
	counts, err := handleTaskCounts(ctx, env, taskCountsInput{})
	if err != nil {
		t.Fatalf("handleTaskCounts: %v", err)
	}
	if counts.GapTaskCount != 2 {
		t.Errorf("GapTaskCount = %d, want 2", counts.GapTaskCount)
	}
	if counts.DistinctGapCount != 1 {
		t.Errorf("DistinctGapCount = %d, want 1 (GAP-A shared by P2-DATA + P2-CONNECTORS)", counts.DistinctGapCount)
	}
	if len(counts.ByPillarStatus) == 0 {
		t.Error("expected at least one (pillar,status) group")
	}

	// task_graph_add_worklink + task_graph_worklinks round-trip.
	_, err = handleAddWorklink(ctx, env, addWorklinkInput{
		TaskID: "P2-DATA", ArtifactType: "pr", ArtifactRef: "https://github.com/x/y/pull/1",
		Thread: "Backend", Note: "test worklink",
	})
	if err != nil {
		t.Fatalf("handleAddWorklink: %v", err)
	}
	links, err := handleWorklinks(ctx, env, worklinksInput{TaskID: "P2-DATA"})
	if err != nil {
		t.Fatalf("handleWorklinks: %v", err)
	}
	if len(links.Worklinks) != 1 || links.Worklinks[0].ArtifactType != "pr" || links.Worklinks[0].Thread != "Backend" {
		t.Fatalf("handleWorklinks: got %+v, want one pr worklink from Backend", links.Worklinks)
	}

	// task_graph_set_status: status changes, everything else (owner,
	// gap_label) is preserved.
	if _, err := handleSetStatus(ctx, env, setStatusInput{TaskID: "P2-DATA", Status: "in_progress"}); err != nil {
		t.Fatalf("handleSetStatus: %v", err)
	}
	afterStatus, err := handleTasks(ctx, env, tasksInput{Statuses: []string{"in_progress"}})
	if err != nil {
		t.Fatalf("handleTasks after set_status: %v", err)
	}
	var found bool
	for _, tk := range afterStatus.Tasks {
		if tk.TaskID == "P2-DATA" {
			found = true
			if tk.GapLabel != "GAP-A" {
				t.Errorf("P2-DATA.gap_label = %q after set_status, want preserved GAP-A", tk.GapLabel)
			}
		}
	}
	if !found {
		t.Error("P2-DATA not found with status=in_progress after handleSetStatus")
	}

	// task_graph_reown.
	if _, err := handleReown(ctx, env, reownInput{
		TaskID: "P3", Owner: "Backend", SessionID: "backend-session", Repo: "task-graph",
		ExpectedOwner: "C-Suite", ExpectedSessionID: "csuite-session", ExpectedRepo: "task-graph",
	}); err != nil {
		t.Fatalf("handleReown: %v", err)
	}
	reowned, err := handleTasks(ctx, env, tasksInput{Owner: "Backend"})
	if err != nil {
		t.Fatalf("handleTasks after reown: %v", err)
	}
	if len(reowned.Tasks) != 1 || reowned.Tasks[0].TaskID != "P3" {
		t.Fatalf("owner=Backend after reown: got %+v, want [P3]", reowned.Tasks)
	}
}

func TestIntegrationSchemaV1ReownCASRejectsStaleRelease(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := taskDomainEnv(t)
	defer env.Substrates.Close()

	seed := taskSubmission("CAS-TASK", "CAS task", "2", "L2", "in_progress", "owner-a", false, "", "")
	seed.Payload["session_id"] = "session-a"
	seed.Payload["repo"] = "repo-a"
	if _, err := handleImport(ctx, env, importInput{Submissions: []ArtifactSubmission{seed}}); err != nil {
		t.Fatalf("seed CAS task: %v", err)
	}

	staleRelease := reownInput{
		TaskID: "CAS-TASK", Owner: "UNOWNED",
		ExpectedOwner: "owner-a", ExpectedSessionID: "session-a", ExpectedRepo: "repo-a",
	}
	_, err := schemaV1HandleReown(ctx, env, staleRelease, func(prior NodeVersion) error {
		adapter, err := soleTaskGraphAdapter(ctx, env)
		if err != nil {
			return err
		}
		spannerAdapter, ok := adapter.(*SpannerSubstrateAdapter)
		if !ok {
			return fmt.Errorf("adapter type = %T, want *SpannerSubstrateAdapter", adapter)
		}
		client, err := spannerAdapter.getClient(ctx)
		if err != nil {
			return err
		}
		changed := make(map[string]interface{}, len(prior.Payload))
		for k, v := range prior.Payload {
			changed[k] = v
		}
		changed["owner"] = "owner-b"
		changed["session_id"] = "session-b"
		changed["repo"] = "repo-b"
		_, err = client.Apply(ctx, []*spanner.Mutation{spanner.Update("Nodes",
			[]string{"organization_id", "node_id", "version", "content"},
			[]interface{}{"test-org", "CAS-TASK", int64(prior.Version), spanner.NullJSON{Value: changed, Valid: true}},
		)})
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "compare-and-set") {
		t.Fatalf("stale schema-v1 release error = %v, want compare-and-set refusal", err)
	}

	if _, err := handleReown(ctx, env, reownInput{TaskID: "CAS-TASK", Owner: "UNOWNED"}); err == nil {
		t.Fatal("schema-v1 release of an owned task succeeded without expected tuple")
	}
	if _, err := handleReown(ctx, env, reownInput{
		TaskID: "CAS-TASK", Owner: "UNOWNED",
		ExpectedOwner: "owner-b", ExpectedSessionID: "session-b", ExpectedRepo: "repo-b",
	}); err != nil {
		t.Fatalf("release with current expected tuple: %v", err)
	}
	released, err := readNodeAcrossSubstrates(ctx, env, "", "CAS-TASK", "", "")
	if err != nil {
		t.Fatalf("read released task: %v", err)
	}
	if got := stringFromMap(released.CurrentVersion.Payload, "owner"); got != "UNOWNED" {
		t.Fatalf("released owner = %q, want UNOWNED", got)
	}
	if got := stringFromMap(released.CurrentVersion.Payload, "session_id"); got != "" {
		t.Fatalf("released session_id = %q, want empty", got)
	}
	if got := stringFromMap(released.CurrentVersion.Payload, "repo"); got != "" {
		t.Fatalf("released repo = %q, want empty", got)
	}
	for _, tc := range []struct {
		name string
		in   reownInput
	}{
		{name: "omitted expected tuple", in: reownInput{TaskID: "CAS-TASK", Owner: "owner-c", SessionID: "session-c", Repo: "repo-c"}},
		{name: "stale owner only", in: reownInput{TaskID: "CAS-TASK", Owner: "owner-c", SessionID: "session-c", Repo: "repo-c", ExpectedOwner: "owner-b"}},
		{name: "stale session only", in: reownInput{TaskID: "CAS-TASK", Owner: "owner-c", SessionID: "session-c", Repo: "repo-c", ExpectedSessionID: "session-b"}},
		{name: "stale repo only", in: reownInput{TaskID: "CAS-TASK", Owner: "owner-c", SessionID: "session-c", Repo: "repo-c", ExpectedRepo: "repo-b"}},
		{name: "stale owned tuple", in: reownInput{TaskID: "CAS-TASK", Owner: "owner-c", SessionID: "session-c", Repo: "repo-c", ExpectedOwner: "owner-b", ExpectedSessionID: "session-b", ExpectedRepo: "repo-b"}},
	} {
		t.Run("released "+tc.name, func(t *testing.T) {
			if _, err := handleReown(ctx, env, tc.in); !errors.Is(err, ErrTaskClaimCollision) {
				t.Fatalf("released schema-v1 reown error = %v, want ErrTaskClaimCollision", err)
			}
		})
	}
}

// TestIntegrationImportIdempotentReRun proves the exact property a task-plan
// cutover re-sync depends on: re-running task_graph_import against data it
// has already admitted must be a clean, re-runnable no-op — never a PK
// collision (Nodes' PK is (org, node_id, version); a naive re-import at
// version=1 would collide and abort the whole batch) and never a silently
// duplicated edge (Edges' PK is (org, edge_id), which does NOT prevent two
// active edges sharing the same source/target/label — a fresh admission
// mints a new random edge_id regardless).
func TestIntegrationImportIdempotentReRun(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDB(ctx, t)
	env := taskDomainEnv(t)
	defer env.Substrates.Close()

	subs := []ArtifactSubmission{
		taskSubmission("GOAL", "Working business OS", "0", "L2", "in_progress", "UNOWNED", false, "", ""),
		taskSubmission("P2", "PILLAR 2", "2", "L2", "in_progress", "UNOWNED", false, "", ""),
		{ArtifactType: "edge", OrganizationID: "test-org", EdgeLabel: "parent_of", EdgeSourceID: "GOAL", EdgeTargetID: "P2"},
	}

	// First run: fresh admission.
	res1, err := handleImport(ctx, env, importInput{Submissions: subs})
	if err != nil {
		t.Fatalf("first import: %v", err)
	}
	if res1.Admitted != 3 || res1.Skipped != 0 {
		t.Fatalf("first import: got admitted=%d skipped=%d, want admitted=3 skipped=0", res1.Admitted, res1.Skipped)
	}

	// Second run: byte-identical submissions — must be a total no-op, not a
	// PK-collision failure and not duplicate edges.
	res2, err := handleImport(ctx, env, importInput{Submissions: subs})
	if err != nil {
		t.Fatalf("second (identical) import must succeed as a no-op, got error: %v", err)
	}
	if res2.Admitted != 0 || res2.Skipped != 3 {
		t.Fatalf("second import: got admitted=%d skipped=%d, want admitted=0 skipped=3 (%v)", res2.Admitted, res2.Skipped, res2.SkippedIDs)
	}

	adapter, _ := env.Substrates.All(ctx)
	nodes, err := adapter[0].ListNodes(ctx, ListFilter{NodeKind: "task", Status: "active", OrganizationID: "test-org", Limit: 100})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected exactly 2 active task nodes after re-import (no duplicates), got %d", len(nodes))
	}
	edges, err := adapter[0].Neighbors(ctx, "GOAL", NeighborOpts{Direction: "out", EdgeLabels: []string{"parent_of"}})
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("expected exactly 1 active parent_of edge GOAL->P2 after re-import (no duplicate), got %d: %+v", len(edges), edges)
	}

	// Third run: ONE payload field changed (status) — must version-bump
	// that node only, not touch anything else, not skip it.
	changed := make([]ArtifactSubmission, len(subs))
	copy(changed, subs)
	changed[1] = taskSubmission("P2", "PILLAR 2", "2", "L2", "done", "UNOWNED", false, "", "")
	res3, err := handleImport(ctx, env, importInput{Submissions: changed})
	if err != nil {
		t.Fatalf("third import (one payload change): %v", err)
	}
	if res3.Admitted != 1 || res3.Skipped != 2 {
		t.Fatalf("third import: got admitted=%d skipped=%d, want admitted=1 (P2 version bump) skipped=2 (GOAL + edge)", res3.Admitted, res3.Skipped)
	}
	p2, err := readNodeAcrossSubstrates(ctx, env, "", "P2", "", "")
	if err != nil {
		t.Fatalf("read P2: %v", err)
	}
	if p2.CurrentVersion.Version != 2 {
		t.Errorf("P2 version = %d, want 2 (version-bumped, not duplicated)", p2.CurrentVersion.Version)
	}
	if p2.CurrentVersion.Payload["status"] != "done" {
		t.Errorf("P2 payload.status = %v, want done", p2.CurrentVersion.Payload["status"])
	}

	// Fourth run: a CONTRACT field changed (level) — must refuse, not
	// silently drift the node's identity.
	contractChanged := make([]ArtifactSubmission, len(subs))
	copy(contractChanged, subs)
	contractChanged[1] = taskSubmission("P2", "PILLAR 2", "2", "L1", "done", "UNOWNED", false, "", "")
	_, err = handleImport(ctx, env, importInput{Submissions: contractChanged})
	if err == nil {
		t.Fatal("expected import to refuse a contract change (level L2->L1) on re-sync, got nil error")
	}
	if !errors.Is(err, ErrContractDrift) {
		t.Errorf("expected ErrContractDrift, got: %v", err)
	}
}

// ---------------- task_graph_supersede_worklink (emulator) ----------------
//
// The in-memory Spanner used by the unit tests neither rolls back DML nor
// isolates transactions, so atomicity, first-writer-wins and the
// no-intermediate-state guarantee are proved here against the emulator.

const (
	supersedeITTask = "SUPERSEDE-TASK"
	supersedeITPred = "BND-pred"
	supersedeITSucc = "BND-succ"
)

func supersedeIntegrationEnv(t *testing.T) *Env {
	t.Helper()
	env := nativeProjectEnv(t)
	env.Config.ArtifactKinds = append(env.Config.ArtifactKinds, NewArtifactKindDescriptor(map[string]interface{}{"name": "worktree"}))
	registry, err := newArtifactKindRegistry(env.Config.ArtifactKinds)
	if err != nil {
		t.Fatalf("build artifact_kinds registry: %v", err)
	}
	env.ArtifactKinds = registry
	return env
}

// seedSupersedeTask creates one owned task with exactly one active canonical
// binding, the valid predecessor a post-merge successor replaces.
func seedSupersedeTask(t *testing.T, ctx context.Context, env *Env, taskID, predID string) supersedeWorklinkInput {
	t.Helper()
	createNativeTask(t, ctx, env, "arbor", taskID, taskID, "not_started")
	if _, err := handleClaim(ctx, env, claimInput{ProjectID: "arbor", TaskID: taskID, Owner: "owner-a", SessionID: "sess-a", Repo: "example-infra"}); err != nil {
		t.Fatalf("claim %s: %v", taskID, err)
	}
	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{ProjectID: "arbor", TaskID: taskID, ArtifactID: predID, ArtifactType: "worktree", ArtifactRef: "example-infra:branch-old:/wt/old", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "branch-old", WorktreePath: "/wt/old"}); err != nil {
		t.Fatalf("seed predecessor %s: %v", predID, err)
	}
	return supersedeWorklinkInput{
		ProjectID: "arbor", TaskID: taskID, ExpectedOwner: "owner-a", ExpectedSessionID: "sess-a", PredecessorLinkID: predID,
		ExpectedPredecessor: supersedePredecessorIdentity{ArtifactType: "worktree", ArtifactStatus: "recorded", Repo: "example-infra", Branch: "branch-old", WorktreePath: "/wt/old", Thread: "thread-a", SessionID: "sess-a"},
		Successor:           supersedeSuccessor{LinkID: supersedeITSucc, ArtifactType: "worktree", ArtifactRef: "example-infra:branch-new:/wt/new", Repo: "example-infra", Branch: "branch-new", WorktreePath: "/wt/new", Thread: "thread-a", SessionID: "sess-a"},
	}
}

func integrationLinkStatus(t *testing.T, ctx context.Context, env *Env, linkID string) (status string, found bool) {
	t.Helper()
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	res, err := adapter.ExecuteReadOnlyQuery(ctx, `SELECT artifact_status FROM `+adapter.Descriptor.WorklinkTable+` WHERE project_id = @project_id AND link_id = @link_id`, map[string]interface{}{"project_id": "arbor", "link_id": linkID})
	if err != nil {
		t.Fatalf("read %s: %v", linkID, err)
	}
	if len(res.Rows) == 0 {
		return "", false
	}
	return stringField(res.Rows[0], "artifact_status"), true
}

func TestIntegrationSupersedeWorklinkReadSurfaces(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	in := seedSupersedeTask(t, ctx, env, supersedeITTask, supersedeITPred)
	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, ArtifactID: "L-archived", ArtifactType: "commit", ArtifactRef: "x"}); err != nil {
		t.Fatalf("seed archived: %v", err)
	}
	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, LinkID: "L-archived"}); err != nil {
		t.Fatalf("archive: %v", err)
	}
	out, err := handleSupersedeWorklink(ctx, env, in)
	if err != nil || out.SupersedeReceipt == nil || out.SupersedeReceipt.RowsAffected != 2 || out.NativeMutationReceipt == nil || out.NativeMutationReceipt.Operation != "supersede_worklink" {
		t.Fatalf("supersede = %+v, %v", out, err)
	}

	// History: both rows stay readable through the task's worklink list.
	listed, err := handleWorklinks(ctx, env, worklinksInput{ProjectID: "arbor", TaskID: supersedeITTask})
	if err != nil {
		t.Fatalf("worklinks: %v", err)
	}
	ids := map[string]bool{}
	for _, w := range listed.Worklinks {
		ids[w.ArtifactID] = true
	}
	if !ids[supersedeITPred] || !ids[supersedeITSucc] || ids["L-archived"] || len(ids) != 2 {
		t.Fatalf("worklinks = %+v, want predecessor and successor as history and no archived row", listed.Worklinks)
	}
	// Active view: the coherent snapshot carries exactly the successor.
	snapshot, err := handleProjectSnapshot(ctx, env, projectSnapshotInput{ProjectID: "arbor"})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	active := map[string]bool{}
	for _, w := range snapshot.Worklinks {
		active[w.ArtifactID] = true
	}
	if len(active) != 1 || !active[supersedeITSucc] || snapshot.Collections.Worklinks.AuthorizedTotal != 1 {
		t.Fatalf("snapshot worklinks = %+v total=%d, want only the successor", snapshot.Worklinks, snapshot.Collections.Worklinks.AuthorizedTotal)
	}
	pred, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: supersedeITPred}, &snapshotPin{})
	if err != nil || pred.Status != "superseded" || pred.CurrentVersion.Payload["artifact_status"] != "superseded" || pred.CurrentVersion.Payload["branch"] != "branch-old" || pred.CurrentVersion.Payload["worktree_path"] != "/wt/old" {
		t.Fatalf("predecessor node = %+v, %v", pred, err)
	}
	succ, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: supersedeITSucc}, &snapshotPin{})
	if err != nil || succ.Status != "active" || succ.CurrentVersion.Payload["note"] != "supersedes="+supersedeITPred || succ.CurrentVersion.Payload["branch"] != "branch-new" {
		t.Fatalf("successor node = %+v, %v", succ, err)
	}
	// The superseded predecessor is the retry and read-back evidence of the
	// succession, not an erroneous row: archiving it would hide it from the
	// worklink list and break the exact retry, so the archive verb refuses it.
	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, LinkID: supersedeITPred}); err == nil || !strings.Contains(err.Error(), "task_graph_supersede_worklink") {
		t.Fatalf("archiving the superseded predecessor = %v, want a refusal naming the typed transition", err)
	}
	if status, _ := integrationLinkStatus(t, ctx, env, supersedeITPred); status != "superseded" {
		t.Fatalf("predecessor status = %q, want superseded", status)
	}
}

func TestIntegrationSupersedeWorklinkIsAtomicAcrossAFailureAfterEitherWrite(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	for _, stage := range []string{"after_retire", "after_install"} {
		t.Run(stage, func(t *testing.T) {
			task, pred := "ATOMIC-"+stage, "BND-pred-"+stage
			in := seedSupersedeTask(t, ctx, env, task, pred)
			in.Successor.LinkID = "BND-succ-" + stage
			boom := errors.New("injected failure at " + stage)
			nativeSupersedeFault = func(at string) error {
				if at == stage {
					return boom
				}
				return nil
			}
			t.Cleanup(func() { nativeSupersedeFault = nil })
			// By the injected point both writes have executed inside the
			// transaction; a non-atomic implementation would have committed one.
			if out, err := handleSupersedeWorklink(ctx, env, in); !errors.Is(err, boom) {
				t.Fatalf("supersede error = %v (%+v), want the injected failure", err, out)
			}
			nativeSupersedeFault = nil
			if status, found := integrationLinkStatus(t, ctx, env, pred); !found || status != "recorded" {
				t.Fatalf("predecessor after failed transition = %q found=%v, want it still active", status, found)
			}
			if _, found := integrationLinkStatus(t, ctx, env, in.Successor.LinkID); found {
				t.Fatal("successor exists after a failed transition")
			}
			if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || out.SupersedeReceipt.RowsAffected != 2 {
				t.Fatalf("the exact request must still commit after the failed attempt: %+v, %v", out, err)
			}
		})
	}
}

func TestIntegrationSupersedeWorklinkConcurrentIdenticalCallersConverge(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	in := seedSupersedeTask(t, ctx, env, supersedeITTask, supersedeITPred)
	const callers = 8
	type result struct {
		out WorklinkSupersedeResult
		err error
	}
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := handleSupersedeWorklink(ctx, env, in)
			results <- result{out, err}
		}()
	}
	wg.Wait()
	close(results)
	committed, retried := 0, 0
	for r := range results {
		if r.err != nil || r.out.SupersedeReceipt == nil {
			t.Fatalf("identical contender failed: %+v, %v", r.out, r.err)
		}
		switch {
		case r.out.SupersedeReceipt.RowsAffected == 2 && !r.out.SupersedeReceipt.Idempotent:
			committed++
		case r.out.SupersedeReceipt.RowsAffected == 0 && r.out.SupersedeReceipt.Idempotent:
			retried++
		default:
			t.Fatalf("unexpected receipt: %+v", r.out.SupersedeReceipt)
		}
	}
	if committed != 1 || retried != callers-1 {
		t.Fatalf("committed=%d idempotent=%d, want one winner and %d idempotent", committed, retried, callers-1)
	}
	if status, _ := integrationLinkStatus(t, ctx, env, supersedeITPred); status != "superseded" {
		t.Fatalf("predecessor status = %q", status)
	}
}

func TestIntegrationSupersedeWorklinkConflictingCallersLeaveExactlyOneWinner(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	base := seedSupersedeTask(t, ctx, env, supersedeITTask, supersedeITPred)
	ids := []string{"BND-a", "BND-b", "BND-c"}
	type outcome struct {
		id  string
		err error
	}
	done := make(chan outcome, len(ids)*2)
	var wg sync.WaitGroup
	for _, id := range ids {
		for j := 0; j < 2; j++ {
			in := base
			in.Successor.LinkID = id
			in.Successor.Branch = "branch-" + id
			in.Successor.WorktreePath = "/wt/" + id
			in.Successor.ArtifactRef = "ref-" + id
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := handleSupersedeWorklink(ctx, env, in)
				done <- outcome{in.Successor.LinkID, err}
			}()
		}
	}
	wg.Wait()
	close(done)
	winners := map[string]bool{}
	var losers []outcome
	for o := range done {
		if o.err == nil {
			winners[o.id] = true
		} else {
			losers = append(losers, o)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("winning successors = %v, want exactly one", winners)
	}
	var winner string
	for id := range winners {
		winner = id
	}
	installed := 0
	for _, id := range ids {
		if _, found := integrationLinkStatus(t, ctx, env, id); found {
			installed++
		}
	}
	if installed != 1 {
		t.Fatalf("installed successors = %d, want exactly the winner %s", installed, winner)
	}
	for _, o := range losers {
		if o.id == winner {
			t.Fatalf("an identical contender of the winner failed: %v", o.err)
		}
		if !errors.Is(o.err, ErrWorklinkSupersedeConflict) || !strings.Contains(o.err.Error(), winner) {
			t.Fatalf("loser error must be a supersede conflict naming %s: %v", winner, o.err)
		}
	}
	if status, _ := integrationLinkStatus(t, ctx, env, supersedeITPred); status != "superseded" {
		t.Fatalf("predecessor history lost: status %q", status)
	}
}

// TestIntegrationSupersedeWorklinkNeverExposesZeroOrTwoActiveBindings samples
// every task's active binding count from strong reads while many transitions
// commit; each read must see exactly one active binding for every task.
func TestIntegrationSupersedeWorklinkNeverExposesZeroOrTwoActiveBindings(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil {
		t.Fatal(err)
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const tasks = 12
	inputs := make([]supersedeWorklinkInput, tasks)
	for i := range inputs {
		id := fmt.Sprintf("OBS-%02d", i)
		inputs[i] = seedSupersedeTask(t, ctx, env, id, "BND-pred-"+id)
		inputs[i].Successor.LinkID = "BND-succ-" + id
	}
	stop := make(chan struct{})
	violations := make(chan string, 1)
	samples := 0
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			iter := client.Single().Query(ctx, spanner.Statement{SQL: `SELECT task_id, COUNT(*) AS n FROM TaskLinks
				WHERE project_id = 'arbor' AND artifact_type = 'worktree' AND ` + nativeActiveWorklinkPredicate + ` GROUP BY task_id`})
			seen := 0
			err := iter.Do(func(row *spanner.Row) error {
				var task string
				var n int64
				if err := row.Columns(&task, &n); err != nil {
					return err
				}
				seen++
				if n != 1 {
					select {
					case violations <- fmt.Sprintf("task %s exposed %d active bindings", task, n):
					default:
					}
				}
				return nil
			})
			if err != nil {
				select {
				case violations <- "sample read failed: " + err.Error():
				default:
				}
				return
			}
			if seen != tasks {
				select {
				case violations <- fmt.Sprintf("sample saw %d tasks with an active binding, want %d", seen, tasks):
				default:
				}
			}
			samples++
		}
	}()
	var writers sync.WaitGroup
	for _, in := range inputs {
		writers.Add(1)
		go func() {
			defer writers.Done()
			if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || out.SupersedeReceipt.RowsAffected != 2 {
				t.Errorf("supersede %s = %+v, %v", in.TaskID, out, err)
			}
		}()
	}
	writers.Wait()
	close(stop)
	readers.Wait()
	select {
	case v := <-violations:
		t.Fatalf("intermediate state observed: %s", v)
	default:
	}
	if samples == 0 {
		t.Fatal("no samples were taken while transitions committed")
	}
}

// TestIntegrationGenericUpdateCannotBypassTheSupersedeTransition drives the
// public task_graph_update handler on the emulator. Retiring the only active
// binding is reachable only through task_graph_supersede_worklink.
func TestIntegrationGenericUpdateCannotBypassTheSupersedeTransition(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	in := seedSupersedeTask(t, ctx, env, supersedeITTask, supersedeITPred)
	activeBindings := func() []string {
		snapshot, err := handleProjectSnapshot(ctx, env, projectSnapshotInput{ProjectID: "arbor"})
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		var ids []string
		for _, w := range snapshot.Worklinks {
			if w.TaskID == supersedeITTask && w.ArtifactType == "worktree" {
				ids = append(ids, w.ArtifactID)
			}
		}
		return ids
	}

	// Forbidden: no successor, owner/session fence or compare-and-set is involved.
	for _, payload := range []map[string]interface{}{
		{"artifact_status": "superseded"},
		{"artifact_status": "Superseded", "note": "also changed"},
	} {
		if out, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: supersedeITPred, Payload: payload}); err == nil || !strings.Contains(err.Error(), "task_graph_supersede_worklink") {
			t.Fatalf("generic update %v = %+v, %v; want a refusal naming the typed transition", payload, out, err)
		}
	}
	if status, _ := integrationLinkStatus(t, ctx, env, supersedeITPred); status != "recorded" {
		t.Fatalf("predecessor status after the refused updates = %q, want recorded", status)
	}
	if ids := activeBindings(); len(ids) != 1 || ids[0] != supersedeITPred {
		t.Fatalf("active bindings after refused updates = %v, want exactly the predecessor", ids)
	}
	got, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: supersedeITPred}, &snapshotPin{})
	if err != nil || got.CurrentVersion.Payload["note"] == "also changed" {
		t.Fatalf("a refused update partially mutated another field: %+v, %v", got.CurrentVersion.Payload, err)
	}

	// Permitted: ordinary metadata on an active binding.
	if out, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: supersedeITPred, Payload: map[string]interface{}{"pr_state": "merged", "url": "https://example.invalid/merged"}}); err != nil || out.NativeMutationReceipt == nil || out.NativeMutationReceipt.RowsAffected != 1 {
		t.Fatalf("ordinary metadata update = %+v, %v", out, err)
	}

	// The typed transition remains the sole way to supersede.
	if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || out.SupersedeReceipt.RowsAffected != 2 {
		t.Fatalf("typed supersede = %+v, %v", out, err)
	}
	// Retired history cannot be revived, archived, rewritten or re-marked.
	for _, payload := range []map[string]interface{}{
		{"artifact_status": "recorded"},
		{"artifact_status": "archived"},
		{"artifact_status": "recorded", "note": "revived"},
		{"branch": "branch-rewritten"},
		{"artifact_status": "superseded"},
	} {
		if out, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: supersedeITPred, Payload: payload}); err == nil {
			t.Fatalf("generic update of superseded history %v succeeded: %+v", payload, out)
		}
	}
	if status, _ := integrationLinkStatus(t, ctx, env, supersedeITPred); status != "superseded" {
		t.Fatalf("predecessor status = %q, want superseded", status)
	}
	if ids := activeBindings(); len(ids) != 1 || ids[0] != supersedeITSucc {
		t.Fatalf("active bindings = %v, want exactly the successor", ids)
	}
	pred, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: supersedeITPred}, &snapshotPin{})
	if err != nil || pred.CurrentVersion.Payload["branch"] != "branch-old" || pred.CurrentVersion.Payload["note"] != "" {
		t.Fatalf("history was rewritten: %+v, %v", pred.CurrentVersion.Payload, err)
	}
	// Exact retry and the typed erroneous-row archive are unaffected.
	if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || out.SupersedeReceipt.RowsAffected != 0 || !out.SupersedeReceipt.Idempotent {
		t.Fatalf("exact retry = %+v, %v", out, err)
	}
	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, ArtifactID: "L-erroneous", ArtifactType: "commit", ArtifactRef: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, LinkID: "L-erroneous"}); err != nil {
		t.Fatalf("typed erroneous archive: %v", err)
	}
}

// TestIntegrationGenericUpdateRacingTheTypedSupersedeNeverRetiresAloneWithoutASuccessor
// runs a typed supersede and generic updates naming the superseded status
// concurrently on many tasks: however they interleave, every task ends with the
// successor installed and exactly one active binding, and every generic update
// is refused.
func TestIntegrationGenericUpdateRacingTheTypedSupersedeNeverRetiresAloneWithoutASuccessor(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	const tasks = 8
	type job struct{ in supersedeWorklinkInput }
	jobs := make([]job, tasks)
	for i := range jobs {
		id := fmt.Sprintf("RACE-%02d", i)
		in := seedSupersedeTask(t, ctx, env, id, "BND-pred-"+id)
		in.Successor.LinkID = "BND-succ-" + id
		jobs[i] = job{in}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var accepted []string
	for _, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if out, err := handleSupersedeWorklink(ctx, env, j.in); err != nil || out.SupersedeReceipt.RowsAffected != 2 {
				t.Errorf("typed supersede %s = %+v, %v", j.in.TaskID, out, err)
			}
		}()
		for k := 0; k < 2; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: j.in.PredecessorLinkID, Payload: map[string]interface{}{"artifact_status": "superseded"}}); err == nil {
					mu.Lock()
					accepted = append(accepted, j.in.TaskID)
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if len(accepted) != 0 {
		t.Fatalf("generic updates into superseded were accepted for %v", accepted)
	}
	for _, j := range jobs {
		if status, found := integrationLinkStatus(t, ctx, env, j.in.Successor.LinkID); !found || status != "recorded" {
			t.Fatalf("%s: successor = %q found=%v", j.in.TaskID, status, found)
		}
		if status, _ := integrationLinkStatus(t, ctx, env, j.in.PredecessorLinkID); status != "superseded" {
			t.Fatalf("%s: predecessor = %q", j.in.TaskID, status)
		}
	}
}

// TestIntegrationSoleValidBindingCannotBeRetiredOrRewrittenThroughPublicMutations
// drives the public handlers on the emulator: generic update, link, and the
// typed erroneous-row archive. Only typed succession changes the one valid
// binding; nonidentity metadata and genuinely erroneous cleanup still work.
func TestIntegrationSoleValidBindingCannotBeRetiredOrRewrittenThroughPublicMutations(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	in := seedSupersedeTask(t, ctx, env, supersedeITTask, supersedeITPred)
	createNativeTask(t, ctx, env, "arbor", "SUPERSEDE-OTHER", "other", "not_started")
	activeBindings := func() []string {
		snapshot, err := handleProjectSnapshot(ctx, env, projectSnapshotInput{ProjectID: "arbor"})
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		var ids []string
		for _, w := range snapshot.Worklinks {
			if w.TaskID == supersedeITTask && w.ArtifactType == "worktree" {
				ids = append(ids, w.ArtifactID)
			}
		}
		return ids
	}
	payloadOf := func(id string) map[string]interface{} {
		got, err := handleGet(ctx, env, getInput{ProjectID: "arbor", NodeID: id}, &snapshotPin{})
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		return got.CurrentVersion.Payload
	}
	refused := func(label, id string, want []string, run func() error) {
		t.Helper()
		before := payloadOf(id)
		err := run()
		if err == nil || !strings.Contains(err.Error(), "task_graph_supersede_worklink") {
			t.Fatalf("%s: error = %v, want a refusal naming the typed transition", label, err)
		}
		after := payloadOf(id)
		for _, key := range []string{"artifact_status", "task_id", "repo", "branch", "worktree_path", "thread", "session_id", "note", "pr_state", "url"} {
			if before[key] != after[key] {
				t.Fatalf("%s: a refused mutation changed %s: %v -> %v", label, key, before[key], after[key])
			}
		}
		if got := activeBindings(); len(got) != len(want) || (len(want) == 1 && got[0] != want[0]) {
			t.Fatalf("%s: active bindings = %v, want %v", label, got, want)
		}
	}
	update := func(id string, payload map[string]interface{}) func() error {
		return func() error {
			_, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: id, Payload: payload})
			return err
		}
	}
	archive := func(id string) func() error {
		return func() error {
			_, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, LinkID: id})
			return err
		}
	}

	// Before succession: nothing retires or rewrites the sole valid binding.
	only := []string{supersedeITPred}
	for _, status := range []string{"archived", "Archived", " released ", "closed", "superseded"} {
		refused("update status "+status, supersedeITPred, only, update(supersedeITPred, map[string]interface{}{"artifact_status": status, "note": "alongside"}))
	}
	for name, payload := range map[string]map[string]interface{}{
		"retype": {"artifact_type": "commit"}, "task": {"task_id": "SUPERSEDE-OTHER"}, "repo": {"repo": "task-graph"},
		"branch": {"branch": "elsewhere"}, "worktree": {"worktree_path": "/wt/elsewhere"}, "thread": {"thread": "t"}, "session": {"session_id": "s"},
	} {
		refused("update "+name, supersedeITPred, only, update(supersedeITPred, payload))
	}
	refused("retype through the contract, as the public tool sends it", supersedeITPred, only, func() error {
		_, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: supersedeITPred, Contract: map[string]interface{}{"artifact_type_kind": "commit", "artifact_ref": "x"}, Payload: map[string]interface{}{}})
		return err
	})
	refused("relink through task_graph_link", supersedeITPred, only, func() error {
		_, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: supersedeITPred, TargetID: "SUPERSEDE-OTHER", EdgeLabel: "advances"})
		return err
	})
	refused("typed erroneous archive", supersedeITPred, only, archive(supersedeITPred))

	// Permitted: nonidentity metadata, the get-to-update echo, and erroneous cleanup.
	if out, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: supersedeITPred, Payload: map[string]interface{}{"branch": "branch-old", "worktree_path": "/wt/old", "thread": "thread-a", "session_id": "sess-a", "artifact_status": "recorded", "pr_state": "merged"}}); err != nil || out.NativeMutationReceipt == nil {
		t.Fatalf("echo with metadata = %+v, %v", out, err)
	}
	for _, id := range []string{"L-erroneous-a", "L-erroneous-b"} {
		if _, err := handleAddWorklink(ctx, env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, ArtifactID: id, ArtifactType: "commit", ArtifactRef: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if out, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: supersedeITTask, LinkID: "L-erroneous-a"}); err != nil || out.NativeMutationReceipt.RowsAffected != 1 {
		t.Fatalf("typed erroneous archive = %+v, %v", out, err)
	}
	if _, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: "L-erroneous-b", Payload: map[string]interface{}{"artifact_status": "archived"}}); err != nil {
		t.Fatalf("generic cleanup of an erroneous row: %v", err)
	}

	// Typed succession, then the retry and read-back evidence is immutable.
	if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || out.SupersedeReceipt.RowsAffected != 2 {
		t.Fatalf("typed supersede = %+v, %v", out, err)
	}
	after := []string{supersedeITSucc}
	refused("archive the superseded predecessor", supersedeITPred, after, archive(supersedeITPred))
	refused("revive the predecessor", supersedeITPred, after, update(supersedeITPred, map[string]interface{}{"artifact_status": "recorded"}))
	refused("archive the successor", supersedeITSucc, after, archive(supersedeITSucc))
	refused("retire the successor", supersedeITSucc, after, update(supersedeITSucc, map[string]interface{}{"artifact_status": "closed"}))
	refused("rewrite the successor branch", supersedeITSucc, after, update(supersedeITSucc, map[string]interface{}{"branch": "elsewhere"}))
	refused("rewrite the successor linkage note", supersedeITSucc, after, update(supersedeITSucc, map[string]interface{}{"note": "rewritten"}))
	if _, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: supersedeITSucc, Payload: map[string]interface{}{"pr_state": "open"}}); err != nil {
		t.Fatalf("successor metadata update: %v", err)
	}
	if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || out.SupersedeReceipt.RowsAffected != 0 || !out.SupersedeReceipt.Idempotent {
		t.Fatalf("exact retry = %+v, %v", out, err)
	}
	listed, err := handleWorklinks(ctx, env, worklinksInput{ProjectID: "arbor", TaskID: supersedeITTask})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, w := range listed.Worklinks {
		ids[w.ArtifactID] = true
	}
	if !ids[supersedeITPred] || !ids[supersedeITSucc] {
		t.Fatalf("history rows must stay listed: %v", ids)
	}
}

// TestIntegrationSuccessionRacingEveryRetirementRouteLeavesOneCanonicalBinding
// runs typed succession concurrently with each public retirement route on many
// tasks. Whatever the interleaving, no other route is accepted and every task
// ends with the successor and the superseded predecessor.
func TestIntegrationSuccessionRacingEveryRetirementRouteLeavesOneCanonicalBinding(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	const tasks = 8
	inputs := make([]supersedeWorklinkInput, tasks)
	for i := range inputs {
		id := fmt.Sprintf("ROUTE-%02d", i)
		inputs[i] = seedSupersedeTask(t, ctx, env, id, "BND-pred-"+id)
		inputs[i].Successor.LinkID = "BND-succ-" + id
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var accepted []string
	for _, in := range inputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || out.SupersedeReceipt.RowsAffected != 2 {
				t.Errorf("typed supersede %s = %+v, %v", in.TaskID, out, err)
			}
		}()
		routes := map[string]func() error{
			"generic archived": func() error {
				_, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: in.PredecessorLinkID, Payload: map[string]interface{}{"artifact_status": "archived"}})
				return err
			},
			"generic released": func() error {
				_, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: in.PredecessorLinkID, Payload: map[string]interface{}{"artifact_status": "released"}})
				return err
			},
			"generic retype": func() error {
				_, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: in.PredecessorLinkID, Payload: map[string]interface{}{"artifact_type": "commit"}})
				return err
			},
			"generic rewrite branch": func() error {
				_, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: in.PredecessorLinkID, Payload: map[string]interface{}{"branch": "elsewhere"}})
				return err
			},
			"typed archive": func() error {
				_, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: in.TaskID, LinkID: in.PredecessorLinkID})
				return err
			},
		}
		for name, route := range routes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := route(); err == nil {
					mu.Lock()
					accepted = append(accepted, in.TaskID+": "+name)
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if len(accepted) != 0 {
		t.Fatalf("routes other than typed succession were accepted: %v", accepted)
	}
	for _, in := range inputs {
		if status, found := integrationLinkStatus(t, ctx, env, in.Successor.LinkID); !found || status != "recorded" {
			t.Fatalf("%s: successor = %q found=%v", in.TaskID, status, found)
		}
		if status, _ := integrationLinkStatus(t, ctx, env, in.PredecessorLinkID); status != "superseded" {
			t.Fatalf("%s: predecessor = %q", in.TaskID, status)
		}
		if out, err := handleSupersedeWorklink(ctx, env, in); err != nil || !out.SupersedeReceipt.Idempotent {
			t.Fatalf("%s: exact retry = %+v, %v", in.TaskID, out, err)
		}
	}
}
