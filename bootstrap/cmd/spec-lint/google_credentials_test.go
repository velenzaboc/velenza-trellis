package main

import (
	"strings"
	"testing"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

func TestSpecLintUsesRootGoogleCredentialTopology(t *testing.T) {
	cfg := &Config{
		Mode: "experimental_bootstrap",
		GoogleCredentials: gcpauth.Selector{
			CredentialsSource:   gcpauth.CredentialsSourceApplicationDefault,
			CredentialsProfile:  gcpauth.CredentialsProfileImpersonatedServiceAccount,
			CredentialsIdentity: "task-graph-operator@example-gcp-project.iam.gserviceaccount.com",
		},
	}
	if err := cfg.ValidateGoogleCredentialTopology(); err != nil {
		t.Fatal(err)
	}
	cfg.Mode = ""
	if err := cfg.ValidateGoogleCredentialTopology(); err == nil || !strings.Contains(err.Error(), "mode required") {
		t.Fatalf("missing mode error = %v", err)
	}
}

func TestSpecLintRejectsLegacyFlashCredentialSelector(t *testing.T) {
	flash := &FlashConfig{
		Provider: "vertex_ai", Project: "example-gcp-project", Location: "us-central1", Model: "gemini-2.5-flash",
		CredentialsSource: "application_default", MaxOutputTokens: 256,
		Prompts: &FlashPromptsConfig{R6ProseCheckPath: "fixture.txt"},
	}
	if err := flash.Validate(); err == nil || !strings.Contains(err.Error(), "legacy per-client credential selector") {
		t.Fatalf("legacy flash selector error = %v", err)
	}
}

func TestSpecLintLoadConfigRejectsHiddenCredentialSelector(t *testing.T) {
	data := []byte(`{"mode":"experimental_bootstrap","google_credentials":{"credentials_source":"application_default","credentials_profile":"impersonated_service_account","credentials_identity":"task-graph-operator@example-gcp-project.iam.gserviceaccount.com"},"pipeline":{"flash":{"credentials_file":"/tmp/hidden.json"}}}`)
	if _, err := LoadConfig(data); err == nil || !strings.Contains(err.Error(), "credentials_file") {
		t.Fatalf("LoadConfig() error = %v, want hidden credential rejection", err)
	}
}
