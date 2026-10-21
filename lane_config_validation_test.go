package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLaneConfigValidationRejectsEachOwnedBoundary(t *testing.T) {
	experimental := func() SpecGraphConfig {
		cfg := validIdentityBoundConfig()
		cfg.Embedding = nil
		return cfg
	}
	production := func() SpecGraphConfig {
		cfg := experimental()
		cfg.Mode = ModeProduction
		cfg.ExperimentalAdmitter = ""
		cfg.DefaultOrganizationID = ""
		cfg.GenesisAdmitter = "GENESIS"
		return cfg
	}

	tests := []struct {
		name string
		cfg  func() SpecGraphConfig
		want string
	}{
		{"mode_missing", func() SpecGraphConfig { c := experimental(); c.Mode = ""; return c }, "mode is required"},
		{"mode_unknown", func() SpecGraphConfig { c := experimental(); c.Mode = "future"; return c }, "invalid mode"},
		{"listen_missing", func() SpecGraphConfig { c := experimental(); c.ListenAddress = ""; return c }, "listen_address is required"},
		{"transport_mode", func() SpecGraphConfig {
			c := experimental()
			c.TransportAuth = &TransportAuthConfig{Mode: "plain", SecretEnv: "X"}
			return c
		}, "mode must be bearer_env"},
		{"transport_env", func() SpecGraphConfig {
			c := experimental()
			c.TransportAuth = &TransportAuthConfig{Mode: "bearer_env", SecretEnv: " "}
			return c
		}, "secret_env is required"},
		{"snapshot_continuation_mode", func() SpecGraphConfig {
			c := experimental()
			c.SnapshotContinuation = &SnapshotContinuationConfig{Mode: "raw_env", KeyEnv: "SNAPSHOT_KEY", TTLSeconds: 300}
			return c
		}, "mode must be hmac_sha256_base64_env"},
		{"snapshot_continuation_env", func() SpecGraphConfig {
			c := experimental()
			c.SnapshotContinuation = &SnapshotContinuationConfig{Mode: "hmac_sha256_base64_env", KeyEnv: " ", TTLSeconds: 300}
			return c
		}, "key_env is required"},
		{"snapshot_continuation_ttl", func() SpecGraphConfig {
			c := experimental()
			c.SnapshotContinuation = &SnapshotContinuationConfig{Mode: "hmac_sha256_base64_env", KeyEnv: "SNAPSHOT_KEY"}
			return c
		}, "ttl_seconds must be between 1 and 3600"},
		{"snapshot_continuation_transport_reuse", func() SpecGraphConfig {
			c := experimental()
			c.TransportAuth = &TransportAuthConfig{Mode: "bearer_env", SecretEnv: "SHARED_SECRET"}
			c.SnapshotContinuation = &SnapshotContinuationConfig{Mode: "hmac_sha256_base64_env", KeyEnv: "SHARED_SECRET", TTLSeconds: 300}
			return c
		}, "key_env must differ"},
		{"remote_without_auth", func() SpecGraphConfig { c := experimental(); c.ListenAddress = "0.0.0.0:8080"; return c }, "transport_auth is required"},
		{"experimental_admitter", func() SpecGraphConfig { c := experimental(); c.ExperimentalAdmitter = ""; return c }, "experimental_admitter required"},
		{"experimental_org", func() SpecGraphConfig { c := experimental(); c.DefaultOrganizationID = ""; return c }, "default_organization_id required"},
		{"production_admitter", func() SpecGraphConfig { c := production(); c.ExperimentalAdmitter = "legacy"; return c }, "experimental_admitter must be empty"},
		{"production_org", func() SpecGraphConfig { c := production(); c.DefaultOrganizationID = "legacy"; return c }, "default_organization_id must be empty"},
		{"production_query", func() SpecGraphConfig { c := production(); c.ExperimentalQueryEnabled = true; return c }, "experimental_query_enabled must be false"},
		{"production_genesis", func() SpecGraphConfig { c := production(); c.GenesisAdmitter = ""; return c }, "genesis_admitter required"},
		{"embedding_provider_missing", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.Provider = ""
			return c
		}, "embedding.provider required"},
		{"embedding_project_missing", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.Project = ""
			return c
		}, "embedding.project required"},
		{"embedding_location_missing", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.Location = ""
			return c
		}, "embedding.location required"},
		{"embedding_model_missing", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.Model = ""
			return c
		}, "embedding.model required"},
		{"embedding_dimensions", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.Dimensions = 0
			return c
		}, "embedding.dimensions required"},
		{"embedding_text_limit", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.MaxTextBytes = 0
			return c
		}, "embedding.max_text_bytes required"},
		{"embedding_provider", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.Provider = "other"
			return c
		}, "must be vertex_ai"},
		{"embedding_endpoint", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.Location = "UPPER"
			return c
		}, "embedding endpoint"},
		{"embedding_legacy_file", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.CredentialsFile = "key.json"
			return c
		}, "legacy per-client credential selector"},
		{"embedding_legacy_source", func() SpecGraphConfig {
			c := experimental()
			c.Embedding = validEmbedding()
			c.Embedding.CredentialsSource = "ambient"
			return c
		}, "legacy per-client credential selector"},
		{"dedup_threshold_zero", func() SpecGraphConfig { c := experimental(); c.Dedup = &DedupPolicyConfig{CandidateLimit: 1}; return c }, "embedding_similarity_threshold required"},
		{"dedup_threshold_high", func() SpecGraphConfig {
			c := experimental()
			c.Dedup = &DedupPolicyConfig{EmbeddingSimilarityThreshold: 2.1, CandidateLimit: 1}
			return c
		}, "embedding_similarity_threshold required"},
		{"dedup_limit", func() SpecGraphConfig {
			c := experimental()
			c.Dedup = &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1}
			return c
		}, "candidate_limit required"},
		{"node_kind_descriptor", func() SpecGraphConfig {
			c := experimental()
			c.NodeKinds = []NodeKindDescriptor{NewNodeKindDescriptor(map[string]any{})}
			return c
		}, "node_kinds[0]"},
		{"edge_descriptor", func() SpecGraphConfig {
			c := experimental()
			c.EdgeLabels = []EdgeLabelDescriptor{NewEdgeLabelDescriptor(map[string]any{})}
			return c
		}, "edge_labels[0]"},
		{"task_status_descriptor", func() SpecGraphConfig { c := experimental(); c.TaskStatuses = []TaskStatusDescriptor{{}}; return c }, "task_statuses[0]"},
		{"dependency_descriptor", func() SpecGraphConfig {
			c := experimental()
			c.DependencyKinds = []DependencyKindDescriptor{{}}
			return c
		}, "dependency_kinds[0]"},
		{"artifact_descriptor", func() SpecGraphConfig { c := experimental(); c.ArtifactKinds = []ArtifactKindDescriptor{{}}; return c }, "artifact_kinds[0]"},
		{"substrate_id", func() SpecGraphConfig { c := experimental(); c.Substrates[0].ID = ""; return c }, "id required"},
		{"substrate_database", func() SpecGraphConfig { c := experimental(); c.Substrates[0].SpannerDatabase = ""; return c }, "spanner_database required"},
		{"substrate_legacy_file", func() SpecGraphConfig { c := experimental(); c.Substrates[0].CredentialsFile = "key.json"; return c }, "legacy per-client credential selector"},
		{"substrate_legacy_source", func() SpecGraphConfig { c := experimental(); c.Substrates[0].CredentialsSource = "ambient"; return c }, "legacy per-client credential selector"},
		{"native_default_project", func() SpecGraphConfig { c := experimental(); c.DefaultProjectID = ""; return c }, "default_project_id required"},
		{"native_task_table", func() SpecGraphConfig { c := experimental(); c.Substrates[0].TaskTable = ""; return c }, "task_table required"},
		{"native_worklink_table", func() SpecGraphConfig { c := experimental(); c.Substrates[0].WorklinkTable = ""; return c }, "worklink_table required"},
		{"native_dep_table", func() SpecGraphConfig { c := experimental(); c.Substrates[0].DepTable = ""; return c }, "dep_table required"},
		{"store_type", func() SpecGraphConfig { c := experimental(); c.Substrates[0].TaskStoreType = "unknown"; return c }, "invalid task_store_type"},
		{"schema_node_table", func() SpecGraphConfig { c := schemaConfig(experimental()); c.Substrates[0].NodeTable = ""; return c }, "node_table required"},
		{"schema_edge_table", func() SpecGraphConfig { c := schemaConfig(experimental()); c.Substrates[0].EdgeTable = ""; return c }, "edge_table required"},
		{"schema_log_table", func() SpecGraphConfig {
			c := schemaConfig(experimental())
			c.Substrates[0].AdmissionLogTable = ""
			return c
		}, "admission_log_table required"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.cfg().Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate error = %v, want substring %q", err, test.want)
			}
		})
	}

	if err := experimental().Validate(); err != nil {
		t.Fatalf("valid experimental config rejected: %v", err)
	}
	if err := production().Validate(); err != nil {
		t.Fatalf("valid production config rejected: %v", err)
	}
}

func validEmbedding() *EmbeddingConfig {
	return &EmbeddingConfig{
		Provider: "vertex_ai", Project: "example-gcp-project", Location: "us-central1",
		Model: "gemini-embedding-001", Dimensions: 32, MaxTextBytes: 1024,
	}
}

func schemaConfig(cfg SpecGraphConfig) SpecGraphConfig {
	cfg.Substrates[0].TaskStoreType = "schema_v1"
	cfg.Substrates[0].TaskTable = ""
	cfg.Substrates[0].WorklinkTable = ""
	cfg.Substrates[0].DepTable = ""
	cfg.Substrates[0].NodeTable = "Nodes"
	cfg.Substrates[0].EdgeTable = "Edges"
	cfg.Substrates[0].AdmissionLogTable = "ArtifactAdmissionLog"
	return cfg
}

func TestLaneLoadConfigCoversFileParseValidationAndSuccess(t *testing.T) {
	t.Setenv("TASK_GRAPH_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "read config") {
		t.Fatalf("missing config error = %v", err)
	}

	writeConfig := func(name string, body []byte) string {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	invalidJSON := writeConfig("invalid.json", []byte(`{"mode":`))
	t.Setenv("TASK_GRAPH_CONFIG", invalidJSON)
	if _, err := loadConfig(); err == nil {
		t.Fatal("invalid JSON must fail")
	}

	invalidShape := writeConfig("shape.json", []byte(`{"mode":"experimental_bootstrap","credentials_file":"key.json"}`))
	t.Setenv("TASK_GRAPH_CONFIG", invalidShape)
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "credential shape") {
		t.Fatalf("credential-shape error = %v", err)
	}

	invalidConfig := writeConfig("validation.json", []byte(`{}`))
	t.Setenv("TASK_GRAPH_CONFIG", invalidConfig)
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "validate config") {
		t.Fatalf("config validation error = %v", err)
	}

	valid := validIdentityBoundConfig()
	valid.Embedding = nil
	body, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	validPath := writeConfig("valid.json", body)
	t.Setenv("TASK_GRAPH_CONFIG", validPath)
	loaded, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ListenAddress != valid.ListenAddress || loaded.DefaultProjectID != "arbor" {
		t.Fatalf("loaded config = %#v", loaded)
	}
}
