package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

type TrellisArborDescriptor struct {
	Layer               int      `json:"layer"`
	CanonicalBinary     string   `json:"canonical_binary"`
	LegacyAlias         string   `json:"legacy_alias"`
	UmbrellaSubcommands []string `json:"umbrella_subcommands"`
	ConfigEnvVars       []string `json:"config_env_vars"`
	Mode                string   `json:"mode"`
	TaskStatusCount     int      `json:"task_status_count"`
	DependencyKindCount int      `json:"dependency_kind_count"`
	ArtifactKindCount   int      `json:"artifact_kind_count"`
}

func resolveConfigPath(getenv func(string) string) string {
	if v := strings.TrimSpace(getenv("VELENZA_TRELLIS_CONFIG")); v != "" {
		return v
	}
	if v := strings.TrimSpace(getenv("TASK_GRAPH_CONFIG")); v != "" {
		return v
	}
	return "task-graph.config.json"
}

func usageText() string {
	return `velenza-trellis (task-graph) — Layer 3 of the Velenza Arbor Stack (fleet plan-to-done MCP server)

usage:
  velenza-trellis mcp                       run the Streamable HTTP MCP service (alias: task-graph mcp)
  velenza-trellis init                      apply bootstrap/schema_v1.sql to an allowlisted experimental DB
  velenza-trellis reset --confirm           drop + recreate an allowlisted experimental DB
  velenza-trellis fg-validator-runner FILE  run the read-only FG checks over frozen snapshot JSON
  velenza-trellis arbor-info [--config FILE] emit the Layer 3 Velenza Arbor Stack JSON descriptor

Config file: $VELENZA_TRELLIS_CONFIG or $TASK_GRAPH_CONFIG (default ./task-graph.config.json).`
}

func BuildTrellisArborDescriptor(cfg SpecGraphConfig) TrellisArborDescriptor {
	return TrellisArborDescriptor{
		Layer:               3,
		CanonicalBinary:     "velenza-trellis",
		LegacyAlias:         "task-graph",
		UmbrellaSubcommands: []string{"inr trellis", "inr task"},
		ConfigEnvVars:       []string{"VELENZA_TRELLIS_CONFIG", "TASK_GRAPH_CONFIG", "ARBOR_GCP_PROJECT", "ARBOR_SPANNER_INSTANCE"},
		Mode:                cfg.Mode,
		TaskStatusCount:     len(cfg.TaskStatuses),
		DependencyKindCount: len(cfg.DependencyKinds),
		ArtifactKindCount:   len(cfg.ArtifactKinds),
	}
}

func runArborInfoCLI(_ context.Context, args []string, out io.Writer) (int, error) {
	configPath := resolveConfigPath(os.Getenv)
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--config":
			if i+1 >= len(args) {
				return 2, fmt.Errorf("--config requires a file path")
			}
			configPath = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--config="):
			configPath = strings.TrimPrefix(args[i], "--config=")
		default:
			return 2, fmt.Errorf("unknown arbor-info argument %q", args[i])
		}
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return 1, fmt.Errorf("read config %q: %w", configPath, err)
	}
	if err := gcpauth.ValidateConfigCredentialShape(data); err != nil {
		return 1, fmt.Errorf("validate credential shape in config %q: %w", configPath, err)
	}
	var cfg SpecGraphConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return 1, fmt.Errorf("parse config %q: %w", configPath, err)
	}
	if err := cfg.Validate(); err != nil {
		return 1, fmt.Errorf("validate config %q: %w", configPath, err)
	}
	desc := BuildTrellisArborDescriptor(cfg)
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(desc); err != nil {
		return 1, err
	}
	return 0, nil
}
