package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

func TestResolveTrellisConfigPathHonorsVelenzaTrellisConfigAndFallback(t *testing.T) {
	env := map[string]string{
		"VELENZA_TRELLIS_CONFIG": "/etc/velenza/trellis.json",
		"TASK_GRAPH_CONFIG":      "/etc/velenza/legacy-task-graph.json",
	}
	got := resolveConfigPath(func(k string) string { return env[k] })
	if got != "/etc/velenza/trellis.json" {
		t.Fatalf("resolveConfigPath with VELENZA_TRELLIS_CONFIG set = %q, want %q", got, "/etc/velenza/trellis.json")
	}

	env["VELENZA_TRELLIS_CONFIG"] = "   "
	got = resolveConfigPath(func(k string) string { return env[k] })
	if got != "/etc/velenza/legacy-task-graph.json" {
		t.Fatalf("resolveConfigPath with only TASK_GRAPH_CONFIG set = %q, want %q", got, "/etc/velenza/legacy-task-graph.json")
	}

	delete(env, "TASK_GRAPH_CONFIG")
	got = resolveConfigPath(func(k string) string { return env[k] })
	if got != "task-graph.config.json" {
		t.Fatalf("resolveConfigPath with neither env var set = %q, want %q", got, "task-graph.config.json")
	}

	configSrc, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	if !strings.Contains(string(configSrc), "resolveConfigPath(") {
		t.Fatalf("config.go loadConfig() does not delegate to resolveConfigPath")
	}
}

func TestShippedAndExampleTrellisConfigsHaveZeroInternalLiteralsAndValidate(t *testing.T) {
	join := func(parts ...string) string { return strings.Join(parts, "") }
	forbidden := []string{
		join("jeev", "es-", "486", "102"),
		join("jeev", "es-rg-spanner-prod-", "4d0e", "4c43"),
		join("no", "13", "productions"),
		join("no", "13", "core"),
		join("merid", "ian-staging"),
		join("/Users/bran", "don"),
	}
	paths := []string{
		"velenza-trellis.config.example.json",
		"task-graph.config.json",
		"task-graph.post-cutover.config.json",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			raw := string(data)
			for _, lit := range forbidden {
				if strings.Contains(raw, lit) {
					t.Errorf("%s contains forbidden internal literal %q", path, lit)
				}
			}
			if err := gcpauth.ValidateConfigCredentialShape(data); err != nil {
				t.Fatalf("%s credential shape invalid: %v", path, err)
			}
			var cfg SpecGraphConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("validate %s: %v", path, err)
			}
			if len(cfg.TaskStatuses) != 7 || len(cfg.DependencyKinds) != 3 || len(cfg.ArtifactKinds) != 27 {
				t.Fatalf(
					"%s registry counts = (%d, %d, %d), want (7, 3, 27)",
					path, len(cfg.TaskStatuses), len(cfg.DependencyKinds), len(cfg.ArtifactKinds),
				)
			}
		})
	}
}

func TestTrellisUsageBannerAndCLIAlignment(t *testing.T) {
	banner := usageText()
	for _, required := range []string{
		"velenza-trellis",
		"task-graph",
		"VELENZA_TRELLIS_CONFIG",
		"TASK_GRAPH_CONFIG",
		"arbor-info",
	} {
		if !strings.Contains(banner, required) {
			t.Errorf("usageText() missing required token %q in:\n%s", required, banner)
		}
	}
	if strings.Contains(banner, "spec-graph") {
		t.Errorf("usageText() still contains stale 'spec-graph' reference:\n%s", banner)
	}

	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if strings.Contains(string(mainSrc), "spec-graph mcp") || strings.Contains(string(mainSrc), "spec-graph init") {
		t.Errorf("main.go still contains stale spec-graph header comments")
	}
}

func TestTrellisArborDescriptorAndCLISubcommand(t *testing.T) {
	cfg := validIdentityBoundConfig()
	cfg.TaskStatuses = []TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]any{"name": "not_started", "legal_next": []any{"in_progress"}}),
		NewTaskStatusDescriptor(map[string]any{"name": "in_progress", "legal_next": []any{"done"}}),
		NewTaskStatusDescriptor(map[string]any{"name": "done", "terminal": true}),
	}
	cfg.DependencyKinds = []DependencyKindDescriptor{
		NewDependencyKindDescriptor(map[string]any{"name": "gates", "edge_family": "dep", "blocking": true}),
	}
	cfg.ArtifactKinds = []ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]any{"name": "pr", "absorbs": []any{"pr"}}),
		NewArtifactKindDescriptor(map[string]any{"name": "commit", "absorbs": []any{"commit"}}),
	}

	desc := BuildTrellisArborDescriptor(cfg)
	if desc.Layer != 3 {
		t.Fatalf("desc.Layer = %d, want 3", desc.Layer)
	}
	if desc.CanonicalBinary != "velenza-trellis" || desc.LegacyAlias != "task-graph" {
		t.Fatalf("unexpected binary names: %+v", desc)
	}
	if len(desc.UmbrellaSubcommands) != 2 || desc.UmbrellaSubcommands[0] != "inr trellis" || desc.UmbrellaSubcommands[1] != "inr task" {
		t.Fatalf("unexpected UmbrellaSubcommands: %+v", desc.UmbrellaSubcommands)
	}
	wantEnv := []string{"VELENZA_TRELLIS_CONFIG", "TASK_GRAPH_CONFIG", "ARBOR_GCP_PROJECT", "ARBOR_SPANNER_INSTANCE"}
	if len(desc.ConfigEnvVars) != len(wantEnv) {
		t.Fatalf("unexpected ConfigEnvVars length: got %v, want %v", desc.ConfigEnvVars, wantEnv)
	}
	for i, w := range wantEnv {
		if desc.ConfigEnvVars[i] != w {
			t.Fatalf("desc.ConfigEnvVars[%d] = %q, want %q", i, desc.ConfigEnvVars[i], w)
		}
	}
	if desc.TaskStatusCount != 3 || desc.DependencyKindCount != 1 || desc.ArtifactKindCount != 2 {
		t.Fatalf("unexpected registry counts in descriptor: %+v", desc)
	}

	var out bytes.Buffer
	code, err := runArborInfoCLI(context.Background(), []string{"--config", "task-graph.config.json"}, &out)
	if err != nil || code != 0 {
		t.Fatalf("runArborInfoCLI failed: code=%d err=%v", code, err)
	}
	var parsed TrellisArborDescriptor
	if err := json.Unmarshal(out.Bytes(), &parsed); err != nil {
		t.Fatalf("unmarshal arbor-info JSON: %v (raw=%s)", err, out.String())
	}
	if parsed.CanonicalBinary != "velenza-trellis" || parsed.TaskStatusCount != 7 || parsed.ArtifactKindCount != 27 {
		t.Fatalf("unexpected parsed arbor-info output: %+v", parsed)
	}
}

func TestValidateFGBindingAcceptsInarchBlake3ChangesetIDAndGitSHA1(t *testing.T) {
	// REQ-TRELLIS-BLAKE3-CHANGESET-ID
	var config struct {
		ArtifactKinds []ArtifactKindDescriptor `json:"artifact_kinds"`
	}
	cfgBytes, err := os.ReadFile("task-graph.config.json")
	if err != nil {
		t.Fatalf("read task-graph.config.json: %v", err)
	}
	if err := json.Unmarshal(cfgBytes, &config); err != nil {
		t.Fatalf("parse task-graph.config.json: %v", err)
	}
	reg, err := newArtifactKindRegistry(config.ArtifactKinds)
	if err != nil {
		t.Fatalf("newArtifactKindRegistry: %v", err)
	}

	baseRow := WorklinkRow{
		ProjectID:    "arbor",
		TaskID:       "ARBOR-POLISH-TRELLIS",
		ArtifactID:   "link-1",
		ArtifactType: "binding",
		ArtifactRef:  "task-graph/REQUIREMENTS.md#section-2",
		Repo:         "velenza-inarch",
		Thread:       "arbor-thread",
		SessionID:    "session-1",
		WorktreePath: "/workspace/arbor/worktrees/arbor-polish-trellis",
		Branch:       "main",
	}
	evidence := FGBindingEvidence{
		Host:             "workstation-a",
		AccountOrHarness: "antigravity",
		PRorSpecRef:      "https://github.com/velenzaboc/velenza-trellis/pull/1",
		CheckpointRef:    "/tmp/cp.json#sha256=" + strings.Repeat("c", 64),
		MeasuredAt:       "2026-10-20T09:00:00Z",
	}

	// 1. 40-char Git SHA-1 must pass.
	gitRow := baseRow
	gitRow.CommitSHA = strings.Repeat("a", 40)
	envGit, err := ValidateFGBinding(context.Background(), reg, gitRow, evidence)
	if err != nil {
		t.Fatalf("40-char Git SHA-1 rejected: %v", err)
	}
	if envGit.CommitSHA != gitRow.CommitSHA {
		t.Fatalf("got CommitSHA %q, want %q", envGit.CommitSHA, gitRow.CommitSHA)
	}

	// 2. 64-char Inarch BLAKE3 changeset ID must pass both ValidateFGBinding and landed JSON schema.
	blake3Row := baseRow
	blake3Row.CommitSHA = strings.Repeat("b", 64)
	envBlake3, err := ValidateFGBinding(context.Background(), reg, blake3Row, evidence)
	if err != nil {
		t.Fatalf("64-char Inarch BLAKE3 changeset ID rejected: %v", err)
	}
	if envBlake3.CommitSHA != blake3Row.CommitSHA {
		t.Fatalf("got CommitSHA %q, want %q", envBlake3.CommitSHA, blake3Row.CommitSHA)
	}
	b, err := json.Marshal(envBlake3)
	if err != nil {
		t.Fatalf("marshal blake3 envelope: %v", err)
	}
	var mapped map[string]string
	if err := json.Unmarshal(b, &mapped); err != nil {
		t.Fatalf("unmarshal blake3 envelope: %v", err)
	}
	assertFGBindingSchema(t, mapped)

	// 3. Invalid lengths (39, 41, 63, 65) must still fail closed with invalid_commit_sha.
	for _, badLen := range []int{39, 41, 63, 65} {
		badRow := baseRow
		badRow.CommitSHA = strings.Repeat("d", badLen)
		got, err := ValidateFGBinding(context.Background(), reg, badRow, evidence)
		assertFGBindingRejection(t, got, err, "invalid_commit_sha", "commit_sha")
	}
}

func TestTrellisOpenSourceLicenseAndReadmeAndCLIHelpHygiene(t *testing.T) {
	// REQ-TRELLIS-OPEN-SOURCE-HYGIENE & REQ-TRELLIS-ENV-ALIGNMENT
	lic, err := os.ReadFile("LICENSE")
	if err != nil {
		t.Fatalf("read LICENSE: %v", err)
	}
	if !strings.Contains(string(lic), "Apache License") || !strings.Contains(string(lic), "Version 2.0") {
		t.Errorf("LICENSE missing Apache-2.0 header")
	}

	for _, docFile := range []string{"NOTICE", "CONTRIBUTING.md", ".github/workflows/ci.yml"} {
		if _, err := os.Stat(docFile); err != nil {
			t.Errorf("required open-source file %s missing: %v", docFile, err)
		}
	}

	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	readmeStr := string(readme)
	join := func(parts ...string) string { return strings.Join(parts, "") }
	for _, forbidden := range []string{
		join("jeev", "es-", "486", "102"),
		join("jeev", "es-rg-spanner-prod-", "4d0e", "4c43"),
		join("/Users/bran", "don"),
	} {
		if strings.Contains(readmeStr, forbidden) {
			t.Errorf("README.md contains forbidden internal literal %q", forbidden)
		}
	}
	for _, required := range []string{
		"velenza-trellis",
		"arbor-info",
		"Velenza Arbor Stack",
		"VELENZA_TRELLIS_CONFIG",
	} {
		if !strings.Contains(readmeStr, required) {
			t.Errorf("README.md missing required token %q", required)
		}
	}

	for _, cmdFile := range []string{"cmd_init.go", "cmd_reset.go"} {
		src, err := os.ReadFile(cmdFile)
		if err != nil {
			t.Fatalf("read %s: %v", cmdFile, err)
		}
		if strings.Contains(string(src), "spec-graph init") || strings.Contains(string(src), "spec-graph reset") {
			t.Errorf("%s still contains stale 'spec-graph init/reset' CLI strings", cmdFile)
		}
	}
}

func TestOpenSourceHygiene(t *testing.T) {
	join := func(parts ...string) string {
		return strings.Join(parts, "")
	}
	forbidden := []string{
		join("jeev", "es"),
		join("486", "102"),
		join("4d0e", "4c43"),
		join("no", "13"),
		join("ini", "tech"),
		join("ba", "os"),
		join("meri", "dian"),
		join("ragg", "edy"),
		join("kop", "ia"),
		join("velenza-", "core"),
		join("kernel-", "staging"),
		join("fleet-", "plan"),
		join("arc-", "runner"),
		join("arc", "server"),
		join("arc", "runner"),
		join("mac", "mini"),
		join("mac", "book"),
		join("bran", "don"),
		join("open", "scion"),
		join("2965", "825"),
		join("users/", "lab"),
		join("/home/", "lab"),
		join("cu", "be"),
		join("pitch", "pen"),
		join("100.", "86."),
		join("100.", "104."),
		join("100.", "110."),
		join("100.", "101."),
		join("100.", "66."),
		join("velenzaboc/", "code-graph"),
		join("velenzaboc/", "spec-graph"),
		join("velenzaboc/", "task-graph"),
	}

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == ".inarch" || d.Name() == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lower := strings.ToLower(string(data))
		for _, tok := range forbidden {
			if strings.Contains(lower, strings.ToLower(tok)) {
				t.Errorf("file %s contains forbidden internal coordinate %q", path, tok)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
}
