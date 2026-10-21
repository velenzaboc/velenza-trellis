package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

func TestDirectoryLoaderUsesOnlySortedSubmissionFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, value interface{}) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("z.json", Submission{StableID: "z"})
	write("a.json", Submission{StableID: "a"})
	write("_validator.json", Submission{StableID: "ignored"})
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "nested.json"), 0o700); err != nil {
		t.Fatal(err)
	}

	subs, err := LoadSubmissions(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{subs[0].StableID, subs[1].StableID}
	if !reflect.DeepEqual(got, []string{"a", "z"}) {
		t.Fatalf("directory order = %v", got)
	}
	if _, err := LoadSubmissions(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("missing input was accepted")
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSubmissions(dir); err == nil || !strings.Contains(err.Error(), "bad.json") {
		t.Fatalf("malformed directory member error = %v", err)
	}
}

func TestConfigAndFlashValidationFailClosed(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	valid := &Config{Mode: "experimental_bootstrap", GoogleCredentials: gcpauth.Selector{
		CredentialsSource: gcpauth.CredentialsSourceApplicationDefault, CredentialsProfile: gcpauth.CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: identity,
	}}
	if err := valid.ValidateGoogleCredentialTopology(); err != nil {
		t.Fatal(err)
	}
	valid.Mode = "unknown"
	if err := valid.ValidateGoogleCredentialTopology(); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("invalid mode error = %v", err)
	}
	if err := (*Config)(nil).ValidateGoogleCredentialTopology(); err == nil {
		t.Fatal("nil config accepted")
	}

	flash := &FlashConfig{Provider: "vertex_ai", Project: "example-gcp-project", Location: "us-central1", Model: "gemini-2.5-flash", MaxOutputTokens: 8, Temperature: 0, Prompts: &FlashPromptsConfig{R6ProseCheckPath: "prompt.txt"}}
	if err := flash.Validate(); err != nil {
		t.Fatal(err)
	}
	flash.Temperature = 3
	if err := flash.Validate(); err == nil || !strings.Contains(err.Error(), "[0, 2]") {
		t.Fatalf("temperature error = %v", err)
	}
	flash.Temperature = 0
	flash.CredentialsSource = "application_default"
	if err := flash.Validate(); err == nil || !strings.Contains(err.Error(), "per-client") {
		t.Fatalf("legacy selector error = %v", err)
	}
}

func TestFlashValidationRejectsEveryRequiredComponent(t *testing.T) {
	valid := func() *FlashConfig {
		return &FlashConfig{Provider: "vertex_ai", Project: "example-gcp-project", Location: "us-central1", Model: "gemini-2.5-flash", MaxOutputTokens: 8, Temperature: 0, Prompts: &FlashPromptsConfig{R6ProseCheckPath: "prompt.txt"}}
	}
	for _, tc := range []struct {
		name string
		mut  func(*FlashConfig)
		want string
	}{
		{name: "nil", mut: func(*FlashConfig) {}, want: "pipeline.flash required"},
		{name: "provider", mut: func(f *FlashConfig) { f.Provider = "" }, want: "provider required"},
		{name: "project", mut: func(f *FlashConfig) { f.Project = "" }, want: "project required"},
		{name: "location", mut: func(f *FlashConfig) { f.Location = "" }, want: "location required"},
		{name: "model", mut: func(f *FlashConfig) { f.Model = "" }, want: "model required"},
		{name: "provider_kind", mut: func(f *FlashConfig) { f.Provider = "other" }, want: "must be vertex_ai"},
		{name: "endpoint", mut: func(f *FlashConfig) { f.Location = "evil/path" }, want: "endpoint"},
		{name: "tokens", mut: func(f *FlashConfig) { f.MaxOutputTokens = 0 }, want: "max_output_tokens"},
		{name: "prompts", mut: func(f *FlashConfig) { f.Prompts = nil }, want: "prompts required"},
		{name: "prompt_path", mut: func(f *FlashConfig) { f.Prompts.R6ProseCheckPath = "" }, want: "r6_prose_check_path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "nil" {
				if err := (*FlashConfig)(nil).Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("nil validation error = %v", err)
				}
				return
			}
			flash := valid()
			tc.mut(flash)
			if err := flash.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestEveryRuleReportsItsStableName(t *testing.T) {
	rules := AllRules()
	got := make([]string, 0, len(rules))
	for _, rule := range rules {
		got = append(got, rule.Name())
	}
	want := []string{"R3_kind_in_registry", "R4_required_contract_fields", "R5_required_payload_fields", "R6_no_prose", "R7_canonical_name_pattern", "R8_no_unintended_duplicate", "R10_edge_endpoint_kinds", "R12_self_loop", "R13_source_reference", "R15_no_admission_artifacts"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rule names = %v, want %v", got, want)
	}
}

func TestLoadConfigAndDefaultPromptReaderRejectMalformedInput(t *testing.T) {
	config := []byte(`{"mode":"experimental_bootstrap","google_credentials":{"credentials_source":"application_default","credentials_profile":"impersonated_service_account","credentials_identity":"task-graph-operator@example-gcp-project.iam.gserviceaccount.com"}}`)
	loaded, err := LoadConfig(config)
	if err != nil || loaded.Mode != "experimental_bootstrap" {
		t.Fatalf("LoadConfig = %#v, %v", loaded, err)
	}
	if _, err := LoadConfig([]byte(`{"google_credentials":{},"google_credentials":{}}`)); err == nil {
		t.Fatal("LoadConfig accepted duplicate keys")
	}
	path := filepath.Join(t.TempDir(), "prompt.txt")
	if err := os.WriteFile(path, []byte("RULE"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := defaultReadFile(path)
	if err != nil || string(got) != "RULE" {
		t.Fatalf("defaultReadFile = %q, %v", got, err)
	}
	if _, err := defaultReadFile(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("defaultReadFile accepted missing path")
	}
}
