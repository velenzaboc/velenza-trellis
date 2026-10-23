package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
)

// Mode names the operating mode of spec-graph. REQUIRED in config; no default.
const (
	ModeExperimentalBootstrap = "experimental_bootstrap"
	ModeProduction            = "production"
)

// SpecGraphConfig is the framework's whole config envelope. Loaded at boot
// from $SPEC_GRAPH_CONFIG (default ./spec-graph.config.json). EVERY behavioral
// value lives here. Per Vision Principle 7 (binary_is_never_sot) and per
// user direction: no defaults, no fallbacks, no hardcoded sentinel literals.
// If a required field is missing, the binary refuses to boot.
type SpecGraphConfig struct {
	// Mode REQUIRED. experimental_bootstrap | production.
	Mode string `json:"mode"`

	// GoogleCredentials is the one identity-bound credential selector shared
	// by every Spanner, Spanner Admin, and Vertex AI client in this process.
	// Per-client credential selectors are rejected below so two clients cannot
	// silently resolve different ambient identities.
	GoogleCredentials gcpauth.Selector `json:"google_credentials"`

	// ExperimentalAdmitter REQUIRED in experimental_bootstrap mode; the literal
	// string the developer wants the binary to use as admitted_by during the
	// pre-genesis experimental phase. MUST be empty in production mode.
	ExperimentalAdmitter string `json:"experimental_admitter,omitempty"`

	// ExperimentalQueryEnabled gates the spec_graph_query verb. Honored only
	// in experimental_bootstrap mode; MUST be false in production mode.
	ExperimentalQueryEnabled bool `json:"experimental_query_enabled,omitempty"`

	// DefaultOrganizationID REQUIRED in experimental_bootstrap mode. The
	// tenant scope every bootstrap-phase submission carries. MUST be empty in
	// production mode (CallerContext supplies it per LOEF §2.1).
	DefaultOrganizationID string `json:"default_organization_id,omitempty"`

	// DefaultProjectID is the logical task-universe namespace used when an
	// API call omits project_id. It is deliberately distinct from both GCP
	// project and organization_id: GCP project is infrastructure, org_id is
	// tenant scope, and project_id is the plan/task universe. Required for
	// fleet_plan_native so legacy Arbor calls keep resolving to one explicit
	// namespace instead of an unscoped table scan. Before the task-plan
	// schema cutover, a fleet_plan_native substrate may omit project_column;
	// in that mode only this default project may be addressed and any
	// non-default project_id fails closed.
	DefaultProjectID string `json:"default_project_id,omitempty"`

	// GenesisAdmitter is the literal string the application compares against
	// admitted_by to flag boot-time genesis admissions (is_non_genesis=false).
	// Whatever the dev configures — typically "GENESIS" but never hardcoded
	// in the binary. REQUIRED in production mode. Optional (and effectively
	// dead) in experimental_bootstrap mode, where the experimental admitter
	// is the only literal in play.
	GenesisAdmitter string `json:"genesis_admitter,omitempty"`

	// InitDatabaseAllowlist REQUIRED for spec-graph init and spec-graph reset
	// to operate. List of glob patterns the target database name must match.
	// Empty list (or missing field) causes init/reset to refuse.
	InitDatabaseAllowlist []string `json:"init_database_allowlist,omitempty"`

	// AllowSpannerEmulator is an explicit local-test gate. It is forbidden in
	// production and on non-loopback listeners; an ambient emulator variable
	// can never silently disable authentication for a hosted process.
	AllowSpannerEmulator bool `json:"allow_spanner_emulator,omitempty"`

	// Embedding is optional. When set, spec_graph_create / _update / _import
	// embed the node's canonical text at admission time and write it to
	// Nodes.embedding_v2; spec_graph_search embeds the query and runs
	// APPROX_COSINE_DISTANCE. When unset, both behaviors fail-closed with a
	// "no embedding configured" message.
	Embedding *EmbeddingConfig `json:"embedding,omitempty"`

	// ListenAddress is the host:port the MCP server binds to when serving
	// the Streamable HTTP transport. REQUIRED — there is no default per
	// Vision Principle 7 (binary_is_never_sot). For a single-user local
	// development setup, bind to 127.0.0.1:<port> so the server is
	// reachable only from the host. Network exposure requires the operator
	// to set 0.0.0.0:<port> deliberately and layer transport authentication
	// per StreamableHTTPOptions hooks (not configured here).
	ListenAddress string `json:"listen_address"`

	// TransportAuth makes any non-loopback HTTP exposure fail closed. The
	// secret itself never enters config; SecretEnv names the runtime variable
	// that supplies it. Local loopback development may omit this block.
	TransportAuth *TransportAuthConfig `json:"transport_auth,omitempty"`

	// SnapshotContinuation owns the dedicated HMAC key reference and token
	// lifetime for resumable project snapshots. The key bytes never enter this
	// file; KeyEnv names a base64-encoded runtime secret. This credential is
	// intentionally separate from TransportAuth's bearer credential.
	SnapshotContinuation *SnapshotContinuationConfig `json:"snapshot_continuation,omitempty"`

	// Dedup is optional. When set, spec_graph_create runs three pre-admission
	// duplicate-detection layers BEFORE the ceremony writes any row:
	//   Layer 1 — definition_hash exact match  → idempotent return (no-op admit)
	//   Layer 2 — (node_kind, canonical_name)  → ErrDuplicateCanonicalName
	//   Layer 3 — embedding cosine similarity   → ErrPossibleDuplicate(candidates)
	// When unset, no dedup runs and create proceeds straight to admission. Per
	// AUTHORING_GUIDE §2 zero_duplication: dedup is load-bearing; production
	// configs should always set this.
	Dedup *DedupPolicyConfig `json:"dedup_policy,omitempty"`

	// Optional administrator-owned review capability. Missing capability keeps
	// ordinary bookkeeping usable, but never permits protected acceptance.
	ReviewProducer *ReviewProducerConfig `json:"review_producer,omitempty"`

	Substrates  []SubstrateDescriptor  `json:"substrates"`
	NodeKinds   []NodeKindDescriptor   `json:"node_kinds"`
	EdgeLabels  []EdgeLabelDescriptor  `json:"edge_labels"`
	Projections []ProjectionDescriptor `json:"projections"`

	// TaskStatuses / DependencyKinds / ArtifactKinds are the P3-GT-FP-REGISTRIES
	// closed-set registries for the task-plan-native free-text columns
	// Tasks.status, TaskDeps.kind, and TaskLinks.artifact_type — same
	// config-driven shape as NodeKinds/EdgeLabels above. See registries.go.
	TaskStatuses    []TaskStatusDescriptor     `json:"task_statuses,omitempty"`
	DependencyKinds []DependencyKindDescriptor `json:"dependency_kinds,omitempty"`
	ArtifactKinds   []ArtifactKindDescriptor   `json:"artifact_kinds,omitempty"`
}

type TransportAuthConfig struct {
	Mode      string `json:"mode"`
	SecretEnv string `json:"secret_env"`
}

type SnapshotContinuationConfig struct {
	Mode       string `json:"mode"`
	KeyEnv     string `json:"key_env"`
	TTLSeconds int    `json:"ttl_seconds"`
}

// DedupPolicyConfig governs pre-admission duplicate detection. Every field is
// REQUIRED when Dedup is set — no defaults. Per Vision Principle 7
// (binary_is_never_sot) threshold/limit/exclusion live in config, not the
// binary.
type DedupPolicyConfig struct {
	// EmbeddingSimilarityThreshold is the maximum cosine DISTANCE (not
	// similarity) at which a candidate is considered a possible duplicate.
	// gemini-embedding-001 produces cosine distance in [0, 2]; identical
	// content → 0, opposite → 2. A threshold of 0.08 corresponds to
	// "similarity > 0.92". Caller-tunable per node_kind via overrides if
	// needed in future; today one global threshold applies.
	EmbeddingSimilarityThreshold float64 `json:"embedding_similarity_threshold"`

	// CandidateLimit caps the top-N candidates surfaced when the embedding
	// search finds matches under the threshold. Bounds the size of
	// ErrPossibleDuplicate's detail payload.
	CandidateLimit int `json:"candidate_limit"`

	// SkipKinds disables dedup entirely for the listed node_kinds. Edges
	// are always exempt regardless (parallel edges with the same endpoints
	// are legitimately distinct by label per
	// 010_multi_role_same_target_edges_are_distinct_by_label). Use this
	// list for node_kinds where dedup is structurally inappropriate
	// (e.g. audit_record, snapshot).
	SkipKinds []string `json:"skip_kinds,omitempty"`
}

// EmbeddingConfig carries the per-tenant Vertex AI embedding setup. Every
// field is required when Embedding is set — no defaults. Authentication uses
// the same closed credential selector as the substrate's Spanner client.
type EmbeddingConfig struct {
	Provider          string `json:"provider"`       // "vertex_ai"
	Project           string `json:"project"`        // GCP project id
	Location          string `json:"location"`       // e.g. "us-central1"
	Model             string `json:"model"`          // e.g. "gemini-embedding-001"
	Dimensions        int    `json:"dimensions"`     // e.g. 3072
	MaxTextBytes      int    `json:"max_text_bytes"` // truncate input above this
	CredentialsFile   string `json:"credentials_file,omitempty"`
	CredentialsSource string `json:"credentials_source,omitempty"`
}

// ProjectionDescriptor is the per-projection config row.
type ProjectionDescriptor struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	OwningSpec  string `json:"owning_spec"`
}

// Validate refuses any config that's incomplete for its declared mode. No
// defaults. No fallbacks. If a value is missing, Validate returns an error
// and loadConfig fails — the binary will not boot.
func (c SpecGraphConfig) Validate() error {
	if err := validateReviewConfiguration(c); err != nil {
		return err
	}
	if c.Mode == "" {
		return fmt.Errorf("mode is required (experimental_bootstrap | production)")
	}
	switch c.Mode {
	case ModeExperimentalBootstrap, ModeProduction:
		// allowed
	default:
		return fmt.Errorf("invalid mode %q: must be %q | %q", c.Mode, ModeExperimentalBootstrap, ModeProduction)
	}
	if err := c.GoogleCredentials.Validate("google_credentials", c.Mode == ModeProduction); err != nil {
		return err
	}
	if c.ListenAddress == "" {
		return fmt.Errorf("listen_address is required (host:port for the Streamable HTTP MCP transport, e.g. \"127.0.0.1:8765\")")
	}
	if c.TransportAuth != nil {
		if c.TransportAuth.Mode != "bearer_env" {
			return fmt.Errorf("transport_auth.mode must be bearer_env")
		}
		if strings.TrimSpace(c.TransportAuth.SecretEnv) == "" {
			return fmt.Errorf("transport_auth.secret_env is required")
		}
	}
	if c.SnapshotContinuation != nil {
		if c.SnapshotContinuation.Mode != "hmac_sha256_base64_env" {
			return fmt.Errorf("snapshot_continuation.mode must be hmac_sha256_base64_env")
		}
		if strings.TrimSpace(c.SnapshotContinuation.KeyEnv) == "" {
			return fmt.Errorf("snapshot_continuation.key_env is required")
		}
		if c.SnapshotContinuation.TTLSeconds <= 0 || c.SnapshotContinuation.TTLSeconds > 3600 {
			return fmt.Errorf("snapshot_continuation.ttl_seconds must be between 1 and 3600")
		}
		if c.TransportAuth != nil && c.SnapshotContinuation.KeyEnv == c.TransportAuth.SecretEnv {
			return fmt.Errorf("snapshot_continuation.key_env must differ from transport_auth.secret_env")
		}
	}
	if !strings.HasPrefix(c.ListenAddress, "127.0.0.1:") && !strings.HasPrefix(c.ListenAddress, "localhost:") && c.TransportAuth == nil {
		return fmt.Errorf("transport_auth is required for non-loopback listen_address")
	}
	if c.AllowSpannerEmulator && c.Mode != ModeExperimentalBootstrap {
		return fmt.Errorf("allow_spanner_emulator is forbidden outside experimental_bootstrap mode")
	}
	if c.AllowSpannerEmulator && !strings.HasPrefix(c.ListenAddress, "127.0.0.1:") && !strings.HasPrefix(c.ListenAddress, "localhost:") {
		return fmt.Errorf("allow_spanner_emulator requires a loopback listen_address")
	}

	if c.Mode == ModeExperimentalBootstrap {
		if c.ExperimentalAdmitter == "" {
			return fmt.Errorf("experimental_admitter required in experimental_bootstrap mode (the literal string the binary will use as admitted_by)")
		}
		if c.DefaultOrganizationID == "" {
			return fmt.Errorf("default_organization_id required in experimental_bootstrap mode (no CallerContext envelope to source it from)")
		}
	}

	if c.Mode == ModeProduction {
		if c.ExperimentalAdmitter != "" {
			return fmt.Errorf("experimental_admitter must be empty in production mode (got %q)", c.ExperimentalAdmitter)
		}
		if c.DefaultOrganizationID != "" {
			return fmt.Errorf("default_organization_id must be empty in production mode (org_id resolved from CallerContext)")
		}
		if c.ExperimentalQueryEnabled {
			return fmt.Errorf("experimental_query_enabled must be false in production mode")
		}
		if c.GenesisAdmitter == "" {
			return fmt.Errorf("genesis_admitter required in production mode (the literal string the binary will compare admitted_by against to flag genesis admissions)")
		}
	}

	// Embedding validation. If set, every field must be present — no defaults.
	if c.Embedding != nil {
		e := c.Embedding
		if e.Provider == "" {
			return fmt.Errorf("embedding.provider required when embedding is set")
		}
		if e.Project == "" {
			return fmt.Errorf("embedding.project required when embedding is set")
		}
		if e.Location == "" {
			return fmt.Errorf("embedding.location required when embedding is set")
		}
		if e.Model == "" {
			return fmt.Errorf("embedding.model required when embedding is set")
		}
		if e.Dimensions <= 0 {
			return fmt.Errorf("embedding.dimensions required when embedding is set (>0)")
		}
		if e.MaxTextBytes <= 0 {
			return fmt.Errorf("embedding.max_text_bytes required when embedding is set (>0)")
		}
		if e.Provider != "vertex_ai" {
			return fmt.Errorf("embedding.provider must be vertex_ai")
		}
		if err := vertexai.ValidateEndpointComponents(e.Location, e.Project, e.Model); err != nil {
			return fmt.Errorf("embedding endpoint: %w", err)
		}
		if e.CredentialsFile != "" || e.CredentialsSource != "" {
			return fmt.Errorf("embedding: legacy per-client credential selector is forbidden; use google_credentials")
		}
	}

	// Dedup policy validation. Every field is required when Dedup is set —
	// no defaults. Per Vision Principle 7: the policy values must come from
	// config, not from the binary.
	if c.Dedup != nil {
		d := c.Dedup
		if d.EmbeddingSimilarityThreshold <= 0 || d.EmbeddingSimilarityThreshold > 2 {
			return fmt.Errorf("dedup_policy.embedding_similarity_threshold required when dedup_policy is set (cosine distance in (0, 2])")
		}
		if d.CandidateLimit <= 0 {
			return fmt.Errorf("dedup_policy.candidate_limit required when dedup_policy is set (>0)")
		}
	}

	// Descriptor validation. Each node_kind and edge_label declares the
	// mechanical fields the binary consumes at runtime; protocol-extra
	// metadata flows through verbatim. See kinds.go / edges.go for the
	// binary_is_never_sot design intent.
	for i, k := range c.NodeKinds {
		if err := k.Validate(i); err != nil {
			return err
		}
	}
	for i, l := range c.EdgeLabels {
		if err := l.Validate(i); err != nil {
			return err
		}
	}
	for i, s := range c.TaskStatuses {
		if err := s.Validate(i); err != nil {
			return err
		}
	}
	for i, k := range c.DependencyKinds {
		if err := k.Validate(i); err != nil {
			return err
		}
	}
	for i, k := range c.ArtifactKinds {
		if err := k.Validate(i); err != nil {
			return err
		}
	}
	// Cross-item construction: this is also where a double-claimed literal
	// (two members' absorbs lists — or a member's own name colliding with
	// another member's absorbs entry — naming the same legacy value) is
	// caught, by reusing the exact registry constructors the runtime uses.
	// Config validation and runtime registry construction must never
	// disagree on what's admissible.
	if _, err := newTaskStatusRegistry(c.TaskStatuses); err != nil {
		return err
	}
	if _, err := newDependencyKindRegistry(c.DependencyKinds); err != nil {
		return err
	}
	if _, err := newArtifactKindRegistry(c.ArtifactKinds); err != nil {
		return err
	}

	// Substrate validation. Every substrate must declare its identity and DB.
	// Non-read-only substrates must declare node_table / edge_table /
	// admission_log_table — the binary writes via these named fields, not via
	// any role-key registry.
	for i, s := range c.Substrates {
		if s.ID == "" {
			return fmt.Errorf("substrates[%d].id required", i)
		}
		if s.SpannerDatabase == "" {
			return fmt.Errorf("substrates[%d] (id=%q): spanner_database required", i, s.ID)
		}
		if s.CredentialsFile != "" || s.CredentialsSource != "" {
			return fmt.Errorf("substrates[%d] (id=%q): legacy per-client credential selector is forbidden; use google_credentials", i, s.ID)
		}
		switch s.TaskStoreType {
		case "", "schema_v1":
			// The legacy migrated graph store below requires Nodes/Edges/log.
		case "fleet_plan_native":
			if c.DefaultProjectID == "" {
				return fmt.Errorf("default_project_id required for fleet_plan_native (logical task namespace; e.g. \"arbor\")")
			}
			if s.TaskTable == "" {
				return fmt.Errorf("substrates[%d] (id=%q): task_table required for fleet_plan_native", i, s.ID)
			}
			if s.WorklinkTable == "" {
				return fmt.Errorf("substrates[%d] (id=%q): worklink_table required for fleet_plan_native", i, s.ID)
			}
			if s.DepTable == "" {
				return fmt.Errorf("substrates[%d] (id=%q): dep_table required for fleet_plan_native", i, s.ID)
			}
			continue
		default:
			return fmt.Errorf("substrates[%d] (id=%q): invalid task_store_type %q", i, s.ID, s.TaskStoreType)
		}
		if !s.ReadOnly {
			if s.NodeTable == "" {
				return fmt.Errorf("substrates[%d] (id=%q): node_table required (not read-only)", i, s.ID)
			}
			if s.EdgeTable == "" {
				return fmt.Errorf("substrates[%d] (id=%q): edge_table required (not read-only)", i, s.ID)
			}
			if s.AdmissionLogTable == "" {
				return fmt.Errorf("substrates[%d] (id=%q): admission_log_table required (not read-only)", i, s.ID)
			}
		}
	}

	return nil
}

// loadConfig reads the config envelope from $VELENZA_TRELLIS_CONFIG or
// $TASK_GRAPH_CONFIG (default ./task-graph.config.json). If the file is
// missing, the binary fails to boot. If the file exists but Validate() rejects
// it, the binary fails to boot. There are no fallbacks.
func loadConfig() (SpecGraphConfig, error) {
	path := resolveConfigPath(os.Getenv)
	data, err := os.ReadFile(path)
	if err != nil {
		return SpecGraphConfig{}, fmt.Errorf("read config %q: %w", path, err)
	}
	if err := gcpauth.ValidateConfigCredentialShape(data); err != nil {
		return SpecGraphConfig{}, fmt.Errorf("validate credential shape in config %q: %w", path, err)
	}
	var cfg SpecGraphConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return SpecGraphConfig{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return SpecGraphConfig{}, fmt.Errorf("validate config %q: %w", path, err)
	}
	return cfg, nil
}
