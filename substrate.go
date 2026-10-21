package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

// SubstrateRouter dispatches verb operations to the substrate that owns a
// given node_kind. The framework owns no storage; the router is the
// indirection between the universal verb API and the per-substrate Spanner
// tables.
//
// PRINCIPLE 7 CONFORMANCE: every behavioral value loads from
// spec-graph.config.json. ONE generic adapter (SpannerSubstrateAdapter)
// handles every substrate; per-substrate table identity is the
// SubstrateDescriptor's named table fields, not a role-key map with code-
// resident keys.
type SubstrateRouter interface {
	Adapter(ctx context.Context, kind NodeKindDescriptor) (SubstrateAdapter, error)
	All(ctx context.Context) ([]SubstrateAdapter, error)
	ByID(id string) (SubstrateAdapter, error)
	Close() error
}

// SubstrateAdapter is the per-substrate operational interface.
type SubstrateAdapter interface {
	ID() string
	Description() string
	Database() string

	// CommitAdmission runs steps 7-10 atomically: read prior signature_chain
	// (for re-admission), write the new typed row(s), mark prior version
	// superseded (if re-admission), append the ArtifactAdmissionLog row(s).
	// Variadic: passing two plans commits both in ONE Spanner RW transaction
	// (used for reciprocal edge pairs — atomic per REQUIREMENTS.md §2).
	CommitAdmission(ctx context.Context, plans ...AdmissionPlan) ([]AdmissionRecord, error)

	ReadNode(ctx context.Context, nodeID, version, snapshotID string) (Node, error)
	ReadNodeInProject(ctx context.Context, projectID, nodeID, version, snapshotID string) (Node, error)
	ReadEdge(ctx context.Context, edgeID, snapshotID string) (Edge, error)
	ListNodes(ctx context.Context, filter ListFilter) ([]Node, error)
	Neighbors(ctx context.Context, nodeID string, opts NeighborOpts) ([]Edge, error)
	History(ctx context.Context, nodeID string) ([]NodeVersion, error)

	ExecuteReadOnlyQuery(ctx context.Context, sql string, params map[string]interface{}) (QueryResult, error)

	// SearchByEmbedding runs APPROX_COSINE_DISTANCE against Nodes.embedding_v2
	// and returns the top-K nearest nodes ordered by distance. nodeKind
	// optionally restricts to one kind; "" = any kind.
	SearchByEmbedding(ctx context.Context, query []float32, nodeKind string, limit int, orgID string) ([]Node, error)

	// FindByDefinitionHash returns the (at most one) active node whose
	// definition_hash exactly matches the given hex string within the
	// organization. Found=false (no error) signals no hit. Used by
	// Layer-1 pre-admission dedup to short-circuit re-creates of byte-
	// identical contracts into an idempotent return.
	FindByDefinitionHash(ctx context.Context, orgID, definitionHash string) (Node, bool, error)

	// FindByCanonicalName returns the (at most one) active node within
	// (org, node_kind) whose contract.canonical_name equals the given
	// string. Found=false (no error) signals no hit. Used by Layer-2
	// pre-admission dedup to refuse collisions on the logical primary
	// key per AUTHORING_GUIDE §2.
	FindByCanonicalName(ctx context.Context, orgID, nodeKind, canonicalName string) (Node, bool, error)

	// FindSimilarByEmbedding runs APPROX_COSINE_DISTANCE filtered by
	// maxDistance and returns the top-N hits as SimilarNode (Node +
	// distance). Used by Layer-3 pre-admission dedup. Differs from
	// SearchByEmbedding in two ways: (a) carries the distance back so the
	// caller can populate PossibleDuplicateCandidate, (b) WHERE-clauses
	// by distance threshold so callers don't have to post-filter.
	FindSimilarByEmbedding(ctx context.Context, orgID string, query []float32, maxDistance float64, limit int) ([]SimilarNode, error)

	// TraverseClosure runs a quantified-path GRAPH_TABLE query (schema_v1) or a
	// bounded per-hop BFS over the native task/worklink/dep tables
	// (fleet_plan_native — see nativeTraverseClosure) starting at the given
	// node, following edges whose edge_label is in the allowed set, up to
	// max_depth hops. direction is "forward" (closure) or "reverse" (impact).
	// projectID is required for fleet_plan_native substrates; ignored by
	// schema_v1 substrates, which have no project namespace. Returns end
	// nodes.
	TraverseClosure(ctx context.Context, projectID, startNodeID string, edgeLabels []string, maxDepth, resultCap int, direction string) ([]Node, error)

	CheckSchema(ctx context.Context) error

	// VerifyExperimentalSchema queries the database's information_schema and
	// returns nil ONLY if the DB carries EXACTLY the spec-graph experimental
	// base tables (NodeTable + EdgeTable + AdmissionLogTable from the
	// descriptor) — no more, no less. Used by `spec-graph reset` before
	// DropDatabase to refuse on any database that doesn't fingerprint as a
	// spec-graph experimental DB. Production-tier databases carry many
	// other base tables and fail the exclusivity check; empty databases
	// (zero base tables) pass since there's nothing to lose by dropping.
	VerifyExperimentalSchema(ctx context.Context) error

	Close() error
}

// AdmissionPlan is the substrate-side input to CommitAdmission.
type AdmissionPlan struct {
	OrganizationID string
	IsEdge         bool
	IsRetirement   bool

	Node *NodeVersion
	Edge *Edge

	PriorVersion *NodeVersion
	Record       AdmissionRecord

	// NodeCompareAndSet, when present, makes authorization state part of the
	// same read/write transaction that creates the new immutable node version.
	// The expected version must still be active and every named string payload
	// field must match before any write or admission-log mutation is buffered.
	NodeCompareAndSet *NodeCompareAndSetPrecondition
}

// NodeCompareAndSetPrecondition is a generic substrate guard for a versioned
// node mutation. Domain handlers choose the payload fields that constitute
// their authorization tuple; the adapter only performs exact comparisons.
type NodeCompareAndSetPrecondition struct {
	ExpectedVersion        int
	ExpectedStatus         string
	ExpectedPayloadStrings map[string]string
}

// ListFilter is the universal filter passed to ListNodes.
type ListFilter struct {
	NodeKind       string
	Status         string
	OrganizationID string
	ProjectID      string
	SnapshotID     string
	AuthorityID    string
	Limit          int
}

// NeighborOpts is the universal neighbor-traversal options struct.
type NeighborOpts struct {
	Direction      string
	EdgeLabels     []string
	Depth          int
	SnapshotID     string
	OrganizationID string
	ProjectID      string
}

// -------------------- descriptor --------------------

// SubstrateDescriptor is one registered substrate. Every table name is a
// named config field — no role-key registry, no Go-resident keys.
//
// The schema_v1 fields (NodeTable / EdgeTable / AdmissionLogTable) are the
// graph substrate's complete persisted shape.
type SubstrateDescriptor struct {
	ID                string `json:"id"`
	Description       string `json:"description"`
	SpannerDatabase   string `json:"spanner_database,omitempty"`
	CredentialsFile   string `json:"credentials_file,omitempty"`
	CredentialsSource string `json:"credentials_source,omitempty"`
	ReadOnly          bool   `json:"read_only,omitempty"`

	// task_store_type selects the physical fleet bookkeeping store shape.
	// Empty / "schema_v1" is the migrated Nodes/Edges graph substrate.
	// "fleet_plan_native" targets the live task-plan Tasks/TaskLinks/TaskDeps
	// tables directly so task_graph_* verbs do not operate on a stale copy.
	TaskStoreType string `json:"task_store_type,omitempty"`
	ProjectColumn string `json:"project_column,omitempty"`
	TaskTable     string `json:"task_table,omitempty"`
	WorklinkTable string `json:"worklink_table,omitempty"`
	DepTable      string `json:"dep_table,omitempty"`

	// schema_v1 (legacy — still serving handlers.go / ceremony.go until Phase 5/6).
	NodeTable         string `json:"node_table,omitempty"`
	EdgeTable         string `json:"edge_table,omitempty"`
	AdmissionLogTable string `json:"admission_log_table,omitempty"`
}

// -------------------- adapter --------------------

type SpannerSubstrateAdapter struct {
	Descriptor    SubstrateDescriptor
	Credentials   gcpauth.Selector
	AllowEmulator bool
	mu            sync.Mutex
	client        *spanner.Client
}

func (a *SpannerSubstrateAdapter) ID() string          { return a.Descriptor.ID }
func (a *SpannerSubstrateAdapter) Description() string { return a.Descriptor.Description }
func (a *SpannerSubstrateAdapter) Database() string    { return a.Descriptor.SpannerDatabase }

func (a *SpannerSubstrateAdapter) getClient(ctx context.Context) (*spanner.Client, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client != nil {
		return a.client, nil
	}
	var clientOptions []option.ClientOption
	useEmulator, err := gcpauth.ValidateSpannerEmulatorHost(os.Getenv("SPANNER_EMULATOR_HOST"), a.AllowEmulator)
	if err != nil {
		return nil, fmt.Errorf("substrate %q: %w", a.Descriptor.ID, err)
	}
	if useEmulator {
		clientOptions = append(clientOptions, option.WithoutAuthentication())
	} else {
		tokenSource, err := a.Credentials.TokenSource(ctx, gcpauth.CloudPlatformScope)
		if err != nil {
			return nil, fmt.Errorf("substrate %q: google credentials: %w", a.Descriptor.ID, err)
		}
		clientOptions = append(clientOptions, option.WithTokenSource(tokenSource))
	}
	c, err := spanner.NewClient(ctx, a.Descriptor.SpannerDatabase, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("substrate %q: spanner client: %w", a.Descriptor.ID, err)
	}
	a.client = c
	return c, nil
}

func (a *SpannerSubstrateAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client != nil {
		a.client.Close()
		a.client = nil
	}
	return nil
}

func (a *SpannerSubstrateAdapter) requireWriteTables() (string, string, string, error) {
	if a.Descriptor.ReadOnly {
		return "", "", "", fmt.Errorf("substrate %q is read-only", a.Descriptor.ID)
	}
	if a.Descriptor.NodeTable == "" || a.Descriptor.EdgeTable == "" || a.Descriptor.AdmissionLogTable == "" {
		return "", "", "", fmt.Errorf("substrate %q: node_table / edge_table / admission_log_table all required for writes", a.Descriptor.ID)
	}
	return a.Descriptor.NodeTable, a.Descriptor.EdgeTable, a.Descriptor.AdmissionLogTable, nil
}

// -------------------- CheckSchema --------------------

func (a *SpannerSubstrateAdapter) CheckSchema(ctx context.Context) error {
	if a.isFleetPlanNative() {
		return a.checkFleetPlanNativeSchema(ctx)
	}
	nodeT, edgeT, logT, err := a.requireWriteTables()
	if err != nil {
		return err
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return err
	}
	stmt := spanner.Statement{
		SQL: `SELECT table_name FROM information_schema.tables WHERE table_schema = '' AND table_name IN UNNEST(@names)`,
		Params: map[string]interface{}{
			"names": []string{nodeT, edgeT, logT},
		},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	found := map[string]bool{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("check_schema query: %w", err)
		}
		var name string
		if err := row.Columns(&name); err != nil {
			return fmt.Errorf("check_schema decode: %w", err)
		}
		found[name] = true
	}
	for _, want := range []string{nodeT, edgeT, logT} {
		if !found[want] {
			return fmt.Errorf("%w: missing table %q", ErrSchemaNotApplied, want)
		}
	}
	return nil
}

// VerifyExperimentalSchema enumerates the database's base tables via
// information_schema and refuses with ErrSchemaFingerprintMismatch if the
// table set is anything other than EXACTLY {NodeTable, EdgeTable,
// AdmissionLogTable}. Empty databases (zero base tables — e.g. freshly
// created via gcloud, schema not yet applied) pass: nothing to lose by
// dropping.
//
// This is the pre-drop fingerprint gate behind task-graph reset. The
// init_database_allowlist is config-driven (a config edit can defeat it);
// this check is structural — the DB's own schema is the fingerprint.
func (a *SpannerSubstrateAdapter) VerifyExperimentalSchema(ctx context.Context) error {
	expected := map[string]bool{}
	for _, t := range []string{a.Descriptor.NodeTable, a.Descriptor.EdgeTable, a.Descriptor.AdmissionLogTable} {
		if t != "" {
			expected[t] = true
		}
	}
	if len(expected) == 0 {
		return fmt.Errorf("substrate %q: cannot verify schema — no graph tables configured", a.Descriptor.ID)
	}

	client, err := a.getClient(ctx)
	if err != nil {
		return err
	}
	stmt := spanner.Statement{
		SQL: `SELECT table_name FROM information_schema.tables
		      WHERE table_schema = '' AND table_type = 'BASE TABLE'`,
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	found := map[string]bool{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("verify schema query: %w", err)
		}
		var name string
		if err := row.Columns(&name); err != nil {
			return fmt.Errorf("verify schema decode: %w", err)
		}
		found[name] = true
	}

	// Empty DB → safe to drop. Nothing to fingerprint.
	if len(found) == 0 {
		return nil
	}

	// The fingerprint passes if EVERY found table is one the descriptor
	// expected. "Missing" expected tables are fine. "Extra" unexpected tables fail —
	// that's the production-DB-mistake guard.
	var extra []string
	for f := range found {
		if !expected[f] {
			extra = append(extra, f)
		}
	}
	if len(extra) == 0 {
		return nil
	}
	sort.Strings(extra)
	expectedList := []string{}
	for e := range expected {
		expectedList = append(expectedList, e)
	}
	sort.Strings(expectedList)
	return fmt.Errorf("%w: substrate=%q expected_base_tables=%v unexpected=%v",
		ErrSchemaFingerprintMismatch, a.Descriptor.ID, expectedList, extra)
}

// -------------------- CommitAdmission --------------------

func (a *SpannerSubstrateAdapter) CommitAdmission(ctx context.Context, plans ...AdmissionPlan) ([]AdmissionRecord, error) {
	if len(plans) == 0 {
		return nil, fmt.Errorf("CommitAdmission: no plans supplied")
	}
	nodeT, edgeT, logT, err := a.requireWriteTables()
	if err != nil {
		return nil, err
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}

	// Copy records out so we can mutate signature_chain inside the closure
	// without re-reading the plans slice.
	recs := make([]AdmissionRecord, len(plans))
	for i, p := range plans {
		recs[i] = p.Record
	}

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		for i, plan := range plans {
			fullChain := append([]string{}, recs[i].SignatureChain...)
			if plan.PriorVersion != nil && !plan.IsEdge {
				var prior []string
				var err error
				if plan.NodeCompareAndSet != nil {
					prior, err = readPriorNodeForCompareAndSetInTx(ctx, tx, nodeT, plan.OrganizationID, plan.Node.NodeID, plan.PriorVersion.Version, *plan.NodeCompareAndSet)
				} else {
					prior, err = readPriorSignatureChainInTx(ctx, tx, nodeT, plan.OrganizationID, plan.Node.NodeID, plan.PriorVersion.Version)
				}
				if err != nil {
					return fmt.Errorf("plan[%d] step 7 read prior chain: %w", i, err)
				}
				fullChain = append(prior, fullChain...)
			}
			recs[i].SignatureChain = fullChain

			if plan.IsEdge {
				if plan.IsRetirement {
					if err := retireEdgeInTx(ctx, tx, edgeT, plan); err != nil {
						return fmt.Errorf("plan[%d] step 9 retire edge: %w", i, err)
					}
				} else {
					if err := writeEdgeInTx(ctx, tx, edgeT, plan); err != nil {
						return fmt.Errorf("plan[%d] step 9 write edge: %w", i, err)
					}
				}
			} else {
				if err := writeNodeInTx(ctx, tx, nodeT, plan, fullChain); err != nil {
					return fmt.Errorf("plan[%d] step 9 write node: %w", i, err)
				}
				if plan.PriorVersion != nil {
					if err := supersedePriorInTx(ctx, tx, nodeT, plan.OrganizationID, plan.Node.NodeID, plan.PriorVersion.Version, plan.Node.Version); err != nil {
						return fmt.Errorf("plan[%d] step 9 supersede prior: %w", i, err)
					}
				}
			}

			if err := writeAdmissionLogInTx(ctx, tx, logT, recs[i]); err != nil {
				return fmt.Errorf("plan[%d] step 10 admission log: %w", i, err)
			}
		}
		return nil
	})
	if err != nil {
		return recs, fmt.Errorf("%w: %v", ErrCeremonyAtomicityBroken, err)
	}
	now := time.Now().UTC()
	for i := range recs {
		recs[i].AdmittedAt = now
	}
	return recs, nil
}

func readPriorNodeForCompareAndSetInTx(ctx context.Context, tx *spanner.ReadWriteTransaction, nodeT, orgID, nodeID string, priorVersion int, expected NodeCompareAndSetPrecondition) ([]string, error) {
	if expected.ExpectedVersion != priorVersion {
		return nil, fmt.Errorf("node compare-and-set configuration mismatch: expected version %d, plan prior version %d", expected.ExpectedVersion, priorVersion)
	}
	row, err := tx.ReadRow(ctx, nodeT, spanner.Key{orgID, nodeID, int64(priorVersion)}, []string{"status", "content", "signature_chain"})
	if err != nil {
		return nil, fmt.Errorf("node compare-and-set read version %d: %w", priorVersion, err)
	}
	var status string
	var content spanner.NullJSON
	var chain []string
	if err := row.Columns(&status, &content, &chain); err != nil {
		return nil, fmt.Errorf("node compare-and-set decode version %d: %w", priorVersion, err)
	}
	if status != expected.ExpectedStatus {
		return nil, fmt.Errorf("node compare-and-set refused: version %d status=%q, want %q", priorVersion, status, expected.ExpectedStatus)
	}
	payload, _ := content.Value.(map[string]interface{})
	keys := make([]string, 0, len(expected.ExpectedPayloadStrings))
	for key := range expected.ExpectedPayloadStrings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		want := expected.ExpectedPayloadStrings[key]
		got, _ := payload[key].(string)
		if got != want {
			return nil, fmt.Errorf("node compare-and-set refused: payload.%s=%q, want %q", key, got, want)
		}
	}
	return chain, nil
}

func readPriorSignatureChainInTx(ctx context.Context, tx *spanner.ReadWriteTransaction, nodeT, orgID, nodeID string, version int) ([]string, error) {
	row, err := tx.ReadRow(ctx, nodeT, spanner.Key{orgID, nodeID, int64(version)}, []string{"signature_chain"})
	if err != nil {
		if spanner.ErrCode(err) == 5 {
			return nil, nil
		}
		return nil, err
	}
	var chain []string
	if err := row.Columns(&chain); err != nil {
		return nil, err
	}
	return chain, nil
}

func writeNodeInTx(ctx context.Context, tx *spanner.ReadWriteTransaction, nodeT string, plan AdmissionPlan, chain []string) error {
	nv := plan.Node
	contractNullable := spanner.NullJSON{Valid: false}
	if nv.Contract != nil {
		contractNullable = spanner.NullJSON{Value: nv.Contract, Valid: true}
	}
	contentNullable := spanner.NullJSON{Valid: false}
	if nv.Payload != nil {
		contentNullable = spanner.NullJSON{Value: nv.Payload, Valid: true}
	}
	cols := []string{
		"organization_id", "node_id", "version", "node_kind", "status",
		"contract", "content", "definition_hash", "manifest_hash", "signature_chain",
		"admitted_by", "admitted_at",
		"governing_spec_ref", "source_spec_lineage_refs",
		"spec_section_ref", "spec_symbol_ref", "node_creation_contract_ref",
		"embedding_v2",
	}
	var embeddingVal interface{} = []float32(nil)
	if len(nv.EmbeddingV2) > 0 {
		embeddingVal = nv.EmbeddingV2
	}
	vals := []interface{}{
		plan.OrganizationID, nv.NodeID, int64(nv.Version), plan.Record.ArtifactType, nv.Status,
		contractNullable, contentNullable, nv.DefinitionHash, nv.ManifestHash, chain,
		plan.Record.AdmittedBy, spanner.CommitTimestamp,
		nullableString(nv.GoverningSpecRef), nv.SourceSpecLineageRefs,
		nullableString(nv.SpecSectionRef), nullableString(nv.SpecSymbolRef), nullableString(nv.NodeCreationContractRef),
		embeddingVal,
	}
	return tx.BufferWrite([]*spanner.Mutation{spanner.Insert(nodeT, cols, vals)})
}

func writeEdgeInTx(ctx context.Context, tx *spanner.ReadWriteTransaction, edgeT string, plan AdmissionPlan) error {
	e := plan.Edge
	if e.SourceVersion == 0 || e.TargetVersion == 0 {
		return fmt.Errorf("edge %q: source_version (%d) and target_version (%d) must both be set (property-graph KEY uniqueness)", e.EdgeID, e.SourceVersion, e.TargetVersion)
	}
	cols := []string{
		"organization_id", "edge_id", "source_node_id", "source_version",
		"target_node_id", "target_version", "edge_label", "status", "admitted_at",
		"reciprocal_edge_id",
	}
	vals := []interface{}{
		plan.OrganizationID, e.EdgeID, e.SourceID, int64(e.SourceVersion),
		e.TargetID, int64(e.TargetVersion), e.EdgeLabel, e.Status, spanner.CommitTimestamp,
		nullableString(e.ReciprocalEdgeID),
	}
	return tx.BufferWrite([]*spanner.Mutation{spanner.Insert(edgeT, cols, vals)})
}

// retireEdgeInTx updates the existing edge row's status to 'retired'. Edges
// don't carry versions (their PK is org+edge_id), so retirement is an UPDATE
// rather than an INSERT-new-version like nodes.
func retireEdgeInTx(ctx context.Context, tx *spanner.ReadWriteTransaction, edgeT string, plan AdmissionPlan) error {
	e := plan.Edge
	return tx.BufferWrite([]*spanner.Mutation{
		spanner.Update(edgeT,
			[]string{"organization_id", "edge_id", "status"},
			[]interface{}{plan.OrganizationID, e.EdgeID, "retired"},
		),
	})
}

func supersedePriorInTx(ctx context.Context, tx *spanner.ReadWriteTransaction, nodeT, orgID, nodeID string, priorVersion, newVersion int) error {
	return tx.BufferWrite([]*spanner.Mutation{
		spanner.Update(nodeT,
			[]string{"organization_id", "node_id", "version", "status", "superseded_by_version"},
			[]interface{}{orgID, nodeID, int64(priorVersion), "superseded", int64(newVersion)},
		),
	})
}

func writeAdmissionLogInTx(ctx context.Context, tx *spanner.ReadWriteTransaction, logT string, rec AdmissionRecord) error {
	passReport, _ := json.Marshal(rec.PassReport)
	cols := []string{
		"organization_id", "admission_id", "artifact_id", "artifact_type", "artifact_version",
		"artifact_hash", "manifest_hash", "admitted_by", "admitted_at",
		"signature", "dual_control_approver", "is_non_genesis", "pass_report",
	}
	signature := ""
	if len(rec.SignatureChain) > 0 {
		signature = rec.SignatureChain[len(rec.SignatureChain)-1]
	}
	vals := []interface{}{
		rec.OrganizationID, rec.RunID, rec.ArtifactID, rec.ArtifactType, int64(rec.ArtifactVersion),
		rec.DefinitionHash, rec.ManifestHash, rec.AdmittedBy, spanner.CommitTimestamp,
		signature, nullableString(""), rec.IsNonGenesis, spanner.NullJSON{Value: json.RawMessage(passReport), Valid: len(passReport) > 0},
	}
	return tx.BufferWrite([]*spanner.Mutation{spanner.Insert(logT, cols, vals)})
}

// -------------------- read paths --------------------

func (a *SpannerSubstrateAdapter) ReadNode(ctx context.Context, nodeID, version, _snapshotID string) (Node, error) {
	return a.ReadNodeInProject(ctx, "", nodeID, version, _snapshotID)
}

func (a *SpannerSubstrateAdapter) ReadNodeInProject(ctx context.Context, projectID, nodeID, version, _snapshotID string) (Node, error) {
	if a.isFleetPlanNative() {
		if version != "" && version != "1" {
			return Node{}, fmt.Errorf("task-plan native nodes are unversioned; got version %q", version)
		}
		return nativeReadNode(ctx, a, projectID, nodeID)
	}
	if a.Descriptor.NodeTable == "" {
		return Node{}, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return Node{}, err
	}
	var stmt spanner.Statement
	if version != "" {
		// Spanner's version column is INT64; the input may be a string from
		// the MCP layer. Parse and reject malformed.
		v, perr := strconv.ParseInt(version, 10, 64)
		if perr != nil {
			return Node{}, fmt.Errorf("read node: version %q is not an integer: %w", version, perr)
		}
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT organization_id, node_id, version, node_kind, status,
				TO_JSON_STRING(contract) AS contract_json,
				TO_JSON_STRING(content) AS content_json,
				definition_hash, manifest_hash, signature_chain,
				admitted_by, admitted_at
				FROM %s WHERE node_id = @node_id AND version = @version LIMIT 1`, a.Descriptor.NodeTable),
			Params: map[string]interface{}{"node_id": nodeID, "version": v},
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT organization_id, node_id, version, node_kind, status,
				TO_JSON_STRING(contract) AS contract_json,
				TO_JSON_STRING(content) AS content_json,
				definition_hash, manifest_hash, signature_chain,
				admitted_by, admitted_at
				FROM %s WHERE node_id = @node_id AND status = 'active' LIMIT 1`, a.Descriptor.NodeTable),
			Params: map[string]interface{}{"node_id": nodeID},
		}
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return Node{}, fmt.Errorf("node %q not found in substrate %q", nodeID, a.Descriptor.ID)
	}
	if err != nil {
		return Node{}, fmt.Errorf("read node: %w", err)
	}
	return decodeNodeRow(row)
}

func (a *SpannerSubstrateAdapter) ReadEdge(ctx context.Context, edgeID, _snapshotID string) (Edge, error) {
	if a.isFleetPlanNative() {
		return Edge{}, fmt.Errorf("task-plan native ReadEdge by edge_id is not supported; use task_graph_neighbors/worklinks")
	}
	if a.Descriptor.EdgeTable == "" {
		return Edge{}, fmt.Errorf("substrate %q: edge_table not configured", a.Descriptor.ID)
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return Edge{}, err
	}
	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT organization_id, edge_id, source_node_id, source_version,
			target_node_id, target_version, edge_label, status, admitted_at, reciprocal_edge_id
			FROM %s WHERE edge_id = @edge_id LIMIT 1`, a.Descriptor.EdgeTable),
		Params: map[string]interface{}{"edge_id": edgeID},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return Edge{}, fmt.Errorf("edge %q not found in substrate %q", edgeID, a.Descriptor.ID)
	}
	if err != nil {
		return Edge{}, fmt.Errorf("read edge: %w", err)
	}
	return decodeEdgeRow(row)
}

func (a *SpannerSubstrateAdapter) ListNodes(ctx context.Context, f ListFilter) ([]Node, error) {
	if a.isFleetPlanNative() {
		return nativeListNodes(ctx, a, f)
	}
	if a.Descriptor.NodeTable == "" {
		return nil, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`SELECT organization_id, node_id, version, node_kind, status,
		TO_JSON_STRING(contract) AS contract_json,
		TO_JSON_STRING(content) AS content_json,
		definition_hash, manifest_hash, signature_chain,
		admitted_by, admitted_at
		FROM %s WHERE 1=1`, a.Descriptor.NodeTable)
	params := map[string]interface{}{}
	if f.NodeKind != "" {
		sql += ` AND node_kind = @kind`
		params["kind"] = f.NodeKind
	}
	if f.Status != "" {
		sql += ` AND status = @status`
		params["status"] = f.Status
	}
	if f.OrganizationID != "" {
		sql += ` AND organization_id = @org`
		params["org"] = f.OrganizationID
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	sql += fmt.Sprintf(` LIMIT %d`, limit)

	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	var out []Node
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list nodes: %w", err)
		}
		n, err := decodeNodeRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func (a *SpannerSubstrateAdapter) Neighbors(ctx context.Context, nodeID string, opts NeighborOpts) ([]Edge, error) {
	if a.isFleetPlanNative() {
		return nativeNeighbors(ctx, a, nodeID, opts)
	}
	if a.Descriptor.EdgeTable == "" {
		return nil, fmt.Errorf("substrate %q: edge_table not configured", a.Descriptor.ID)
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	var where string
	switch opts.Direction {
	case "in":
		where = `target_node_id = @id`
	case "out":
		where = `source_node_id = @id`
	default:
		where = `(source_node_id = @id OR target_node_id = @id)`
	}
	params := map[string]interface{}{"id": nodeID}
	sql := fmt.Sprintf(`SELECT organization_id, edge_id, source_node_id, source_version,
		target_node_id, target_version, edge_label, status, admitted_at, reciprocal_edge_id
		FROM %s WHERE %s`, a.Descriptor.EdgeTable, where)
	if len(opts.EdgeLabels) > 0 {
		sql += ` AND edge_label IN UNNEST(@labels)`
		params["labels"] = opts.EdgeLabels
	}
	sql += ` LIMIT 200`
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	var out []Edge
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("neighbors: %w", err)
		}
		e, err := decodeEdgeRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func (a *SpannerSubstrateAdapter) History(ctx context.Context, nodeID string) ([]NodeVersion, error) {
	if a.Descriptor.NodeTable == "" {
		return nil, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`SELECT organization_id, node_id, version, node_kind, status,
		TO_JSON_STRING(contract) AS contract_json,
		TO_JSON_STRING(content) AS content_json,
		definition_hash, manifest_hash, signature_chain,
		admitted_by, admitted_at
		FROM %s WHERE node_id = @id ORDER BY version ASC`, a.Descriptor.NodeTable)
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: map[string]interface{}{"id": nodeID}})
	defer iter.Stop()
	var out []NodeVersion
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("history: %w", err)
		}
		n, err := decodeNodeRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, n.CurrentVersion)
	}
	return out, nil
}

func (a *SpannerSubstrateAdapter) SearchByEmbedding(ctx context.Context, query []float32, nodeKind string, limit int, orgID string) ([]Node, error) {
	if a.Descriptor.NodeTable == "" {
		return nil, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`
		SELECT organization_id, node_id, version, node_kind, status,
			TO_JSON_STRING(contract) AS contract_json,
			TO_JSON_STRING(content) AS content_json,
			definition_hash, manifest_hash, signature_chain,
			admitted_by, admitted_at,
			APPROX_COSINE_DISTANCE(embedding_v2, @query_vec, options => JSON '{"num_leaves_to_search": 10}') AS distance
		FROM %s
		WHERE embedding_v2 IS NOT NULL
		  AND status = 'active'
		  %s
		  %s
		ORDER BY distance
		LIMIT %d
	`, a.Descriptor.NodeTable,
		condIf(nodeKind != "", "AND node_kind = @kind"),
		condIf(orgID != "", "AND organization_id = @org"),
		limit,
	)
	params := map[string]interface{}{"query_vec": query}
	if nodeKind != "" {
		params["kind"] = nodeKind
	}
	if orgID != "" {
		params["org"] = orgID
	}
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	var out []Node
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("search query: %w", err)
		}
		n, err := decodeNodeRowWithDistance(row)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func condIf(b bool, s string) string {
	if b {
		return s
	}
	return ""
}

// FindByDefinitionHash — Layer-1 pre-admission dedup lookup. Selects at most
// one active row whose definition_hash matches exactly. (definition_hash is
// SHA-256 hex over canonical(contract+payload+identity fields) per
// stepDefinitionHash — collisions are cryptographically negligible.)
func (a *SpannerSubstrateAdapter) FindByDefinitionHash(ctx context.Context, orgID, definitionHash string) (Node, bool, error) {
	if a.Descriptor.NodeTable == "" {
		return Node{}, false, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	if definitionHash == "" {
		return Node{}, false, fmt.Errorf("FindByDefinitionHash: empty definition_hash")
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return Node{}, false, err
	}
	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT organization_id, node_id, version, node_kind, status,
			TO_JSON_STRING(contract) AS contract_json,
			TO_JSON_STRING(content) AS content_json,
			definition_hash, manifest_hash, signature_chain,
			admitted_by, admitted_at
			FROM %s
			WHERE organization_id = @org AND definition_hash = @h AND status = 'active'
			LIMIT 1`, a.Descriptor.NodeTable),
		Params: map[string]interface{}{"org": orgID, "h": definitionHash},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return Node{}, false, nil
	}
	if err != nil {
		return Node{}, false, fmt.Errorf("find by definition_hash: %w", err)
	}
	n, err := decodeNodeRow(row)
	if err != nil {
		return Node{}, false, err
	}
	return n, true, nil
}

// FindByCanonicalName — Layer-2 pre-admission dedup lookup. Selects at most
// one active row in (org, node_kind) whose contract.canonical_name equals the
// given string. JSON_VALUE on the contract column extracts the field; the
// match is case-sensitive and exact (canonical_names are snake_case literals
// per AUTHORING_GUIDE).
func (a *SpannerSubstrateAdapter) FindByCanonicalName(ctx context.Context, orgID, nodeKind, canonicalName string) (Node, bool, error) {
	if a.Descriptor.NodeTable == "" {
		return Node{}, false, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	if canonicalName == "" {
		return Node{}, false, nil
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return Node{}, false, err
	}
	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT organization_id, node_id, version, node_kind, status,
			TO_JSON_STRING(contract) AS contract_json,
			TO_JSON_STRING(content) AS content_json,
			definition_hash, manifest_hash, signature_chain,
			admitted_by, admitted_at
			FROM %s
			WHERE organization_id = @org
			  AND node_kind = @kind
			  AND status = 'active'
			  AND JSON_VALUE(contract, '$.canonical_name') = @name
			LIMIT 1`, a.Descriptor.NodeTable),
		Params: map[string]interface{}{"org": orgID, "kind": nodeKind, "name": canonicalName},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return Node{}, false, nil
	}
	if err != nil {
		return Node{}, false, fmt.Errorf("find by canonical_name: %w", err)
	}
	n, err := decodeNodeRow(row)
	if err != nil {
		return Node{}, false, err
	}
	return n, true, nil
}

// FindSimilarByEmbedding — Layer-3 pre-admission dedup lookup. Runs
// APPROX_COSINE_DISTANCE filtered by maxDistance (cosine distance < threshold)
// and returns the top-N hits with their distances. Differs from
// SearchByEmbedding by carrying the distance back so the caller can populate
// PossibleDuplicateCandidate.
func (a *SpannerSubstrateAdapter) FindSimilarByEmbedding(ctx context.Context, orgID string, query []float32, maxDistance float64, limit int) ([]SimilarNode, error) {
	if a.Descriptor.NodeTable == "" {
		return nil, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("FindSimilarByEmbedding: limit must be > 0")
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`
		SELECT organization_id, node_id, version, node_kind, status,
			TO_JSON_STRING(contract) AS contract_json,
			TO_JSON_STRING(content) AS content_json,
			definition_hash, manifest_hash, signature_chain,
			admitted_by, admitted_at,
			APPROX_COSINE_DISTANCE(embedding_v2, @query_vec, options => JSON '{"num_leaves_to_search": 10}') AS distance
		FROM %s
		WHERE embedding_v2 IS NOT NULL
		  AND status = 'active'
		  AND organization_id = @org
		  AND APPROX_COSINE_DISTANCE(embedding_v2, @query_vec, options => JSON '{"num_leaves_to_search": 10}') < @max_d
		ORDER BY distance
		LIMIT %d
	`, a.Descriptor.NodeTable, limit)
	params := map[string]interface{}{
		"query_vec": query,
		"org":       orgID,
		"max_d":     maxDistance,
	}
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	var out []SimilarNode
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("find similar by embedding: %w", err)
		}
		n, dist, err := decodeNodeRowWithDistanceValue(row)
		if err != nil {
			return nil, err
		}
		out = append(out, SimilarNode{Node: n, Distance: dist})
	}
	return out, nil
}

// decodeNodeRowWithDistanceValue is decodeNodeRowWithDistance's twin but
// returns the distance instead of discarding it. Same column order.
func decodeNodeRowWithDistanceValue(row *spanner.Row) (Node, float64, error) {
	var (
		orgID, nodeID, kind, status, defHash, manHash, admittedBy string
		version                                                   int64
		contractJSON                                              spanner.NullString
		contentJSON                                               spanner.NullString
		chain                                                     []string
		admittedAt                                                time.Time
		distance                                                  float64
	)
	if err := row.Columns(&orgID, &nodeID, &version, &kind, &status, &contractJSON, &contentJSON, &defHash, &manHash, &chain, &admittedBy, &admittedAt, &distance); err != nil {
		return Node{}, 0, fmt.Errorf("decode similar-search row: %w", err)
	}
	var contract map[string]interface{}
	if contractJSON.Valid {
		_ = json.Unmarshal([]byte(contractJSON.StringVal), &contract)
	}
	var payload map[string]interface{}
	if contentJSON.Valid {
		_ = json.Unmarshal([]byte(contentJSON.StringVal), &payload)
	}
	nv := NodeVersion{
		NodeID: nodeID, Version: int(version), Status: status,
		Contract: contract, Payload: payload,
		DefinitionHash: defHash, ManifestHash: manHash, SignatureChain: chain,
		AdmittedAt: admittedAt, AdmittedBy: admittedBy,
	}
	return Node{
		NodeID: nodeID, NodeKind: kind, OrganizationID: orgID, Status: status,
		CreatedAt: admittedAt, UpdatedAt: admittedAt, CurrentVersion: nv,
	}, distance, nil
}

// decodeNodeRowWithDistance is the same shape as decodeNodeRow but with an
// extra trailing FLOAT64 distance column (which we discard — the order in the
// result IS the distance ranking).
func decodeNodeRowWithDistance(row *spanner.Row) (Node, error) {
	var (
		orgID, nodeID, kind, status, defHash, manHash, admittedBy string
		version                                                   int64
		contractJSON                                              spanner.NullString
		contentJSON                                               spanner.NullString
		chain                                                     []string
		admittedAt                                                time.Time
		distance                                                  float64
	)
	if err := row.Columns(&orgID, &nodeID, &version, &kind, &status, &contractJSON, &contentJSON, &defHash, &manHash, &chain, &admittedBy, &admittedAt, &distance); err != nil {
		return Node{}, fmt.Errorf("decode search row: %w", err)
	}
	var contract map[string]interface{}
	if contractJSON.Valid {
		_ = json.Unmarshal([]byte(contractJSON.StringVal), &contract)
	}
	var payload map[string]interface{}
	if contentJSON.Valid {
		_ = json.Unmarshal([]byte(contentJSON.StringVal), &payload)
	}
	nv := NodeVersion{
		NodeID: nodeID, Version: int(version), Status: status,
		Contract: contract, Payload: payload,
		DefinitionHash: defHash, ManifestHash: manHash, SignatureChain: chain,
		AdmittedAt: admittedAt, AdmittedBy: admittedBy,
	}
	return Node{
		NodeID: nodeID, NodeKind: kind, OrganizationID: orgID, Status: status,
		CreatedAt: admittedAt, UpdatedAt: admittedAt, CurrentVersion: nv,
	}, nil
}

func (a *SpannerSubstrateAdapter) TraverseClosure(ctx context.Context, projectID, startNodeID string, edgeLabels []string, maxDepth, resultCap int, direction string) ([]Node, error) {
	if a.isFleetPlanNative() {
		return nativeTraverseClosure(ctx, a, projectID, startNodeID, edgeLabels, maxDepth, resultCap, direction)
	}
	if a.Descriptor.NodeTable == "" {
		return nil, fmt.Errorf("substrate %q: node_table not configured", a.Descriptor.ID)
	}
	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	// Bounded quantified-path traversal over CorpusGraph with edge-property
	// filtering inside the path pattern.
	//
	// Spanner GQL syntax: the WHERE clause goes INSIDE the parenthesized
	// edge pattern — e.g. `[e WHERE e.edge_label IN UNNEST(@labels)]->{1,N}`.
	// (Per https://docs.cloud.google.com/spanner/docs/reference/standard-sql/graph-patterns
	// — "where_clause: A WHERE clause, which filters the nodes or edges that
	// were matched.")
	//
	// The property graph uses DYNAMIC LABEL on nodes (from node_kind) and
	// edges (from edge_label), so untyped patterns `(n)` and `[e]` match
	// regardless of dynamic label, then the inline WHERE narrows by label.
	// 'end' is a GQL reserved keyword — use 'dst'.
	leftArrow, rightArrow := "-", "->"
	if direction == "reverse" {
		leftArrow, rightArrow = "<-", "-"
	}
	edgeFilter := "e.status = 'active'"
	if len(edgeLabels) > 0 {
		edgeFilter = "e.edge_label IN UNNEST(@labels) AND " + edgeFilter
	}
	sql := fmt.Sprintf(`
		GRAPH CorpusGraph
		MATCH (start {node_id: @start})%s[e WHERE %s]%s{1,%d}(dst)
		RETURN DISTINCT dst.node_id AS node_id, dst.node_kind AS node_kind, dst.status AS status,
			dst.organization_id AS organization_id, dst.admitted_at AS admitted_at
		LIMIT %d
	`, leftArrow, edgeFilter, rightArrow, maxDepth, resultCap)
	params := map[string]interface{}{"start": startNodeID, "labels": edgeLabels}
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	var out []Node
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("closure traversal: %w", err)
		}
		var (
			nodeID, kind, status, orgID string
			admittedAt                  time.Time
		)
		if err := row.Columns(&nodeID, &kind, &status, &orgID, &admittedAt); err != nil {
			return nil, fmt.Errorf("decode closure row: %w", err)
		}
		out = append(out, Node{
			NodeID:         nodeID,
			NodeKind:       kind,
			OrganizationID: orgID,
			Status:         status,
			CreatedAt:      admittedAt,
			UpdatedAt:      admittedAt,
			CurrentVersion: NodeVersion{NodeID: nodeID, Status: status, AdmittedAt: admittedAt},
		})
	}
	return out, nil
}

func (a *SpannerSubstrateAdapter) ExecuteReadOnlyQuery(ctx context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
	client, err := a.getClient(ctx)
	if err != nil {
		return QueryResult{}, err
	}
	// client.Single() is a SINGLE-USE read-only transaction: it picks its own
	// read timestamp. Two ExecuteReadOnlyQuery calls therefore observe two
	// DIFFERENT timestamps and can straddle a concurrent write. Any caller
	// that needs several reads to agree with each other must use
	// ReadOnlySnapshot instead — see its doc comment.
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	return collectQueryResult(iter)
}

// SnapshotQuerier runs one query inside an already-open read-only transaction.
// Every query issued through the same SnapshotQuerier observes the SAME Spanner
// read timestamp.
type SnapshotQuerier func(ctx context.Context, sql string, params map[string]interface{}) (QueryResult, error)

// SnapshotReader is the seam a multi-read, must-be-coherent projection reads
// through: ONE transaction, N queries, ONE read timestamp. It exists as an
// interface so a handler's coherence property is testable without Spanner —
// a fake can count how many transactions were opened and which queries ran
// inside each, which is exactly the property that regresses if someone routes
// a query back through ExecuteReadOnlyQuery.
type SnapshotReader interface {
	ReadOnlySnapshot(ctx context.Context, fn func(q SnapshotQuerier) error) (time.Time, error)
}

// TimestampSnapshotReader resumes a snapshot at one exact previously-observed
// Spanner timestamp. It is intentionally a separate capability from
// SnapshotReader: a fake or alternate reader that cannot honor an exact bound
// must cause continuation to fail closed rather than silently opening a newer
// snapshot.
type TimestampSnapshotReader interface {
	SnapshotReader
	ReadOnlySnapshotAt(ctx context.Context, readTimestamp time.Time, fn func(q SnapshotQuerier) error) (time.Time, error)
}

// ReadOnlySnapshot runs fn inside ONE multi-use Spanner read-only transaction
// and returns the read timestamp every query inside fn observed.
//
// This is the coherence primitive. A projection assembled from several
// client.Single() calls can interleave revisions and report a state that never
// existed atomically; assembled through one ReadOnlySnapshot it reports exactly
// the state that existed at the returned timestamp.
//
// The returned timestamp is only readable after a read has completed, which is
// why Timestamp() is called AFTER fn returns rather than before it runs
// (cloud.google.com/go/spanner v1.91.0 transaction.go:1213-1216).
func (a *SpannerSubstrateAdapter) ReadOnlySnapshot(ctx context.Context, fn func(q SnapshotQuerier) error) (time.Time, error) {
	return a.readOnlySnapshot(ctx, nil, fn)
}

// ReadOnlySnapshotAt runs a multi-use read-only transaction pinned to an
// exact historical timestamp. GoogleSQL's FOR SYSTEM_TIME AS OF is not used:
// the transaction's timestamp bound is the supported Spanner Go API and
// applies to every statement in the page.
func (a *SpannerSubstrateAdapter) ReadOnlySnapshotAt(ctx context.Context, readTimestamp time.Time, fn func(q SnapshotQuerier) error) (time.Time, error) {
	if readTimestamp.IsZero() {
		return time.Time{}, fmt.Errorf("snapshot read timestamp required")
	}
	return a.readOnlySnapshot(ctx, &readTimestamp, fn)
}

func (a *SpannerSubstrateAdapter) readOnlySnapshot(ctx context.Context, readTimestamp *time.Time, fn func(q SnapshotQuerier) error) (time.Time, error) {
	client, err := a.getClient(ctx)
	if err != nil {
		return time.Time{}, err
	}
	txn := client.ReadOnlyTransaction()
	if readTimestamp != nil {
		txn = txn.WithTimestampBound(spanner.ReadTimestamp(*readTimestamp))
	}
	defer txn.Close()

	q := func(ctx context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
		iter := txn.Query(ctx, spanner.Statement{SQL: sql, Params: params})
		defer iter.Stop()
		return collectQueryResult(iter)
	}
	if err := fn(q); err != nil {
		return time.Time{}, err
	}
	ts, err := txn.Timestamp()
	if err != nil {
		return time.Time{}, fmt.Errorf("read timestamp: %w", err)
	}
	if readTimestamp != nil && !ts.Equal(*readTimestamp) {
		return time.Time{}, fmt.Errorf("exact snapshot timestamp mismatch: got %s want %s", ts.UTC().Format(time.RFC3339Nano), readTimestamp.UTC().Format(time.RFC3339Nano))
	}
	return ts, nil
}

// collectQueryResult drains a row iterator into a QueryResult. Shared by the
// single-use (ExecuteReadOnlyQuery) and multi-use (ReadOnlySnapshot) read
// paths so the two decode rows identically — the snapshot path must produce
// byte-identical row maps to the per-verb path, or the same SQL would render
// two different shapes depending on which transaction ran it.
func collectQueryResult(iter *spanner.RowIterator) (QueryResult, error) {
	out := QueryResult{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return out, fmt.Errorf("query: %w", err)
		}
		if out.Columns == nil {
			out.Columns = row.ColumnNames()
		}
		m := map[string]interface{}{}
		for i, name := range row.ColumnNames() {
			var gc spanner.GenericColumnValue
			if err := row.Column(i, &gc); err != nil {
				m[name] = nil
				continue
			}
			m[name] = decodeStructpbValue(gc.Value)
		}
		out.Rows = append(out.Rows, m)
	}
	return out, nil
}

// decodeStructpbValue converts a Spanner GenericColumnValue's underlying
// google.protobuf.Value into a Go-native interface{} so callers don't
// have to know about protobuf wrappers. Strings → string, numbers →
// float64, bools → bool, null → nil, structs → map[string]interface{},
// lists → []interface{}. JSON marshaling produces the same shape either
// way; in-process Go consumers (like type-asserting handlers) get the
// native types they expect.
func decodeStructpbValue(v *structpb.Value) interface{} {
	if v == nil {
		return nil
	}
	switch k := v.Kind.(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		return k.NumberValue
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_BoolValue:
		return k.BoolValue
	case *structpb.Value_StructValue:
		out := map[string]interface{}{}
		for fname, fval := range k.StructValue.GetFields() {
			out[fname] = decodeStructpbValue(fval)
		}
		return out
	case *structpb.Value_ListValue:
		vals := k.ListValue.GetValues()
		out := make([]interface{}, len(vals))
		for i, e := range vals {
			out[i] = decodeStructpbValue(e)
		}
		return out
	}
	return nil
}

// -------------------- decoding helpers --------------------

func decodeNodeRow(row *spanner.Row) (Node, error) {
	var (
		orgID, nodeID, kind, status, defHash, manHash, admittedBy string
		version                                                   int64
		contractJSON                                              spanner.NullString
		contentJSON                                               spanner.NullString
		chain                                                     []string
		admittedAt                                                time.Time
	)
	if err := row.Columns(&orgID, &nodeID, &version, &kind, &status, &contractJSON, &contentJSON, &defHash, &manHash, &chain, &admittedBy, &admittedAt); err != nil {
		return Node{}, fmt.Errorf("decode node row: %w", err)
	}
	var contract map[string]interface{}
	if contractJSON.Valid {
		_ = json.Unmarshal([]byte(contractJSON.StringVal), &contract)
	}
	var payload map[string]interface{}
	if contentJSON.Valid {
		_ = json.Unmarshal([]byte(contentJSON.StringVal), &payload)
	}
	nv := NodeVersion{
		NodeID:         nodeID,
		Version:        int(version),
		Status:         status,
		Contract:       contract,
		Payload:        payload,
		DefinitionHash: defHash,
		ManifestHash:   manHash,
		SignatureChain: chain,
		AdmittedAt:     admittedAt,
		AdmittedBy:     admittedBy,
	}
	return Node{
		NodeID:         nodeID,
		NodeKind:       kind,
		OrganizationID: orgID,
		Status:         status,
		CreatedAt:      admittedAt,
		UpdatedAt:      admittedAt,
		CurrentVersion: nv,
	}, nil
}

func decodeEdgeRow(row *spanner.Row) (Edge, error) {
	var (
		orgID, edgeID, sourceID, targetID, label, status string
		sourceVer, targetVer                             spanner.NullInt64
		admittedAt                                       time.Time
		recipID                                          spanner.NullString
	)
	if err := row.Columns(&orgID, &edgeID, &sourceID, &sourceVer, &targetID, &targetVer, &label, &status, &admittedAt, &recipID); err != nil {
		return Edge{}, fmt.Errorf("decode edge row: %w", err)
	}
	e := Edge{
		EdgeID:         edgeID,
		OrganizationID: orgID,
		SourceID:       sourceID,
		TargetID:       targetID,
		EdgeLabel:      label,
		Status:         status,
		AdmittedAt:     admittedAt,
	}
	if sourceVer.Valid {
		e.SourceVersion = int(sourceVer.Int64)
	}
	if targetVer.Valid {
		e.TargetVersion = int(targetVer.Int64)
	}
	if recipID.Valid {
		e.ReciprocalEdgeID = recipID.StringVal
	}
	return e, nil
}

func nullableString(s string) interface{} {
	if s == "" {
		return spanner.NullString{Valid: false}
	}
	return s
}

func nullableInt64(v int) interface{} {
	if v == 0 {
		return spanner.NullInt64{Valid: false}
	}
	return int64(v)
}

// -------------------- router --------------------

type configRouter struct {
	byID map[string]*SpannerSubstrateAdapter
}

func newRouter(credentials gcpauth.Selector, allowEmulator bool, substrates []SubstrateDescriptor) SubstrateRouter {
	r := &configRouter{byID: make(map[string]*SpannerSubstrateAdapter, len(substrates))}
	for _, d := range substrates {
		r.byID[d.ID] = &SpannerSubstrateAdapter{Descriptor: d, Credentials: credentials, AllowEmulator: allowEmulator}
	}
	return r
}

func (r *configRouter) Adapter(_ context.Context, kind NodeKindDescriptor) (SubstrateAdapter, error) {
	a, ok := r.byID[kind.SubstrateID()]
	if !ok {
		return nil, fmt.Errorf("no substrate registered for id %q (kind=%q): %w", kind.SubstrateID(), kind.Name(), ErrSubstrateNotProvisioned)
	}
	return a, nil
}

func (r *configRouter) All(_ context.Context) ([]SubstrateAdapter, error) {
	out := make([]SubstrateAdapter, 0, len(r.byID))
	for _, a := range r.byID {
		out = append(out, a)
	}
	return out, nil
}

func (r *configRouter) ByID(id string) (SubstrateAdapter, error) {
	a, ok := r.byID[id]
	if !ok {
		return nil, fmt.Errorf("no substrate %q registered", id)
	}
	return a, nil
}

func (r *configRouter) Close() error {
	for _, a := range r.byID {
		_ = a.Close()
	}
	return nil
}
