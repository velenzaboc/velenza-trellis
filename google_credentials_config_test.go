package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

func TestConfigRequiresOneCentralIdentityBoundGoogleSelector(t *testing.T) {
	cfg := validIdentityBoundConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("central selector rejected: %v", err)
	}

	cfg.GoogleCredentials = gcpauth.Selector{}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "google_credentials") {
		t.Fatalf("missing central selector error = %v", err)
	}
}

func TestConfigRejectsStalePerClientCredentialSelectors(t *testing.T) {
	for _, mutate := range []func(*SpecGraphConfig){
		func(cfg *SpecGraphConfig) { cfg.Embedding.CredentialsSource = "application_default" },
		func(cfg *SpecGraphConfig) { cfg.Embedding.CredentialsFile = "/tmp/key.json" },
		func(cfg *SpecGraphConfig) { cfg.Substrates[0].CredentialsSource = "application_default" },
		func(cfg *SpecGraphConfig) { cfg.Substrates[0].CredentialsFile = "/tmp/key.json" },
	} {
		cfg := validIdentityBoundConfig()
		mutate(&cfg)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "legacy per-client credential selector") {
			t.Fatalf("stale selector error = %v", err)
		}
	}
}

func TestConfigClosesSpannerEmulatorBypass(t *testing.T) {
	cfg := validIdentityBoundConfig()
	cfg.AllowSpannerEmulator = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("loopback experimental emulator rejected: %v", err)
	}

	cfg.Mode = ModeProduction
	cfg.ExperimentalAdmitter = ""
	cfg.DefaultOrganizationID = ""
	cfg.GenesisAdmitter = "GENESIS"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "forbidden outside experimental_bootstrap") {
		t.Fatalf("production emulator error = %v", err)
	}

	cfg = validIdentityBoundConfig()
	cfg.AllowSpannerEmulator = true
	cfg.ListenAddress = "0.0.0.0:8080"
	cfg.TransportAuth = &TransportAuthConfig{Mode: "bearer_env", SecretEnv: "TASK_GRAPH_SHARED_SECRET"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "requires a loopback") {
		t.Fatalf("non-loopback emulator error = %v", err)
	}
}

func TestShippedConfigsUseExactCentralIdentityAndNoPerClientSelectors(t *testing.T) {
	for _, path := range []string{
		"task-graph.config.json",
		"task-graph.post-cutover.config.json",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := gcpauth.ValidateConfigCredentialShape(data); err != nil {
				t.Fatalf("credential shape: %v", err)
			}
			var cfg SpecGraphConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				t.Fatal(err)
			}
			if err := cfg.GoogleCredentials.Validate("google_credentials", cfg.Mode == ModeProduction); err != nil {
				t.Fatalf("google credential topology = %v", err)
			}
			if cfg.GoogleCredentials.CredentialsIdentity == "" {
				t.Fatal("google_credentials.credentials_identity is empty")
			}
			if cfg.GoogleCredentials.CredentialsProfile != gcpauth.CredentialsProfileImpersonatedServiceAccount &&
				cfg.GoogleCredentials.CredentialsProfile != gcpauth.CredentialsProfileAttachedServiceAccount {
				t.Fatalf("credentials_profile = %q", cfg.GoogleCredentials.CredentialsProfile)
			}
			if cfg.Embedding != nil && (cfg.Embedding.CredentialsFile != "" || cfg.Embedding.CredentialsSource != "") {
				t.Fatal("embedding retained a per-client credential selector")
			}
			for _, substrate := range cfg.Substrates {
				if substrate.CredentialsFile != "" || substrate.CredentialsSource != "" {
					t.Fatalf("substrate %q retained a per-client credential selector", substrate.ID)
				}
			}
		})
	}
}

func validIdentityBoundConfig() SpecGraphConfig {
	return SpecGraphConfig{
		Mode:                  ModeExperimentalBootstrap,
		ExperimentalAdmitter:  "EXPERIMENTAL_BOOTSTRAP",
		DefaultOrganizationID: "example-org",
		DefaultProjectID:      "arbor",
		ListenAddress:         "127.0.0.1:8790",
		GoogleCredentials: gcpauth.Selector{
			CredentialsSource:   gcpauth.CredentialsSourceApplicationDefault,
			CredentialsProfile:  gcpauth.CredentialsProfileImpersonatedServiceAccount,
			CredentialsIdentity: "task-graph-operator@example-gcp-project.iam.gserviceaccount.com",
		},
		Embedding: &EmbeddingConfig{
			Provider: "vertex_ai", Project: "example-gcp-project", Location: "us-central1",
			Model: "gemini-embedding-001", Dimensions: 3072, MaxTextBytes: 32768,
		},
		NodeKinds: []NodeKindDescriptor{
			NewNodeKindDescriptor(map[string]any{"name": "task", "substrate_id": "fleet", "artifact_type": "task", "version_scope": "stable_id_preserved"}),
		},
		Substrates: []SubstrateDescriptor{{
			ID: "fleet", SpannerDatabase: "projects/p/instances/i/databases/d",
			TaskStoreType: fleetPlanNativeStoreType, TaskTable: "Tasks", WorklinkTable: "TaskLinks", DepTable: "TaskDeps",
		}},
	}
}
