package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
)

func TestRunFailsClosedBeforeAnyVertexConstruction(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.json")
	if err := os.WriteFile(input, []byte(`{"node_kind":"section","stable_id":"section_x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"mode":"experimental_bootstrap"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing flags", want: "required"},
		{name: "unknown flag", args: []string{"--unknown"}, want: "flag provided but not defined"},
		{name: "missing config", args: []string{"--config", filepath.Join(dir, "missing"), "--input", input}, want: "read config"},
		{name: "missing input", args: []string{"--config", config, "--input", filepath.Join(dir, "missing")}, want: "load submissions"},
		{name: "missing flash", args: []string{"--config", config, "--input", input}, want: "pipeline.flash required"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := run(tt.args, &stdout, &stderr); got != 2 {
				t.Fatalf("run() exit = %d, want 2; stderr=%s", got, stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.want) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tt.want)
			}
		})
	}
}

func TestRunBuildsValidatedRuntimeAndReportsVerdict(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	input := filepath.Join(dir, "input.json")
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	configJSON := `{
  "mode":"experimental_bootstrap",
  "google_credentials":{"credentials_source":"application_default","credentials_profile":"impersonated_service_account","credentials_identity":"` + identity + `"},
  "node_kinds":[{"name":"section","required_contract_fields":["canonical_name"]}],
  "pipeline":{"flash":{"provider":"vertex_ai","project":"example-gcp-project","location":"us-central1","model":"gemini-2.5-flash","max_output_tokens":8,"temperature":0,"prompts":{"r6_prose_check_path":"test:prompt"}}}
}`
	if err := os.WriteFile(config, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, []byte(`{"node_kind":"section","stable_id":"section_x","contract":{"canonical_name":"section_x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	origAuth, origFlash := constructVertexAuth, constructFlashGenerator
	constructVertexAuth = func(context.Context, vertexai.TokenSourceFactory) (*vertexai.Auth, error) {
		return &vertexai.Auth{}, nil
	}
	constructFlashGenerator = func(*vertexai.Auth, vertexai.FlashConfig) (Generator, error) {
		return &mockGenerator{}, nil
	}
	t.Cleanup(func() {
		constructVertexAuth, constructFlashGenerator = origAuth, origFlash
	})

	var stdout, stderr bytes.Buffer
	if got := run([]string{"--config", config, "--input", input, "--concept-domain", "task_graph"}, &stdout, &stderr); got != 0 {
		t.Fatalf("run() exit = %d, stderr=%s", got, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"verdict": "pass"`) || !strings.Contains(stdout.String(), `"concept_domain": "task_graph"`) {
		t.Fatalf("report = %s", stdout.String())
	}

	if err := os.WriteFile(input, []byte(`{"node_kind":"unknown","stable_id":"section_x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if got := run([]string{"--config", config, "--input", input}, &stdout, &stderr); got != 1 {
		t.Fatalf("run() exit = %d, want failing report; stderr=%s", got, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"verdict": "fail"`) || !strings.Contains(stdout.String(), "R3_kind_in_registry") {
		t.Fatalf("failing report = %s", stdout.String())
	}
}

func TestRunReportsVertexConstructionFailures(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	input := filepath.Join(dir, "input.json")
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	if err := os.WriteFile(config, []byte(`{"mode":"experimental_bootstrap","google_credentials":{"credentials_source":"application_default","credentials_profile":"impersonated_service_account","credentials_identity":"`+identity+`"},"pipeline":{"flash":{"provider":"vertex_ai","project":"example-gcp-project","location":"us-central1","model":"gemini-2.5-flash","max_output_tokens":8,"temperature":0,"prompts":{"r6_prose_check_path":"test:prompt"}}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, []byte(`{"node_kind":"section","stable_id":"section_x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	origAuth, origFlash := constructVertexAuth, constructFlashGenerator
	t.Cleanup(func() { constructVertexAuth, constructFlashGenerator = origAuth, origFlash })
	constructVertexAuth = func(context.Context, vertexai.TokenSourceFactory) (*vertexai.Auth, error) {
		return nil, errors.New("credential rejected")
	}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--config", config, "--input", input}, &stdout, &stderr); got != 2 || !strings.Contains(stderr.String(), "vertex auth") {
		t.Fatalf("auth failure: exit=%d stderr=%q", got, stderr.String())
	}
	constructVertexAuth = func(context.Context, vertexai.TokenSourceFactory) (*vertexai.Auth, error) {
		return &vertexai.Auth{}, nil
	}
	constructFlashGenerator = func(*vertexai.Auth, vertexai.FlashConfig) (Generator, error) {
		return nil, errors.New("client rejected")
	}
	stderr.Reset()
	if got := run([]string{"--config", config, "--input", input}, &stdout, &stderr); got != 2 || !strings.Contains(stderr.String(), "vertex client") {
		t.Fatalf("flash failure: exit=%d stderr=%q", got, stderr.String())
	}
}
