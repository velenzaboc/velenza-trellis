package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
)

// Submission is the input shape spec-lint validates against the R-rules.
// Matches spec-graph's ArtifactSubmission JSON shape (snake_case tags
// per spec-graph REQUIREMENTS.md §12.1) so files authored for
// spec_graph_import can be linted directly without re-shaping.
type Submission struct {
	ArtifactType   string                 `json:"artifact_type,omitempty"`
	OrganizationID string                 `json:"organization_id,omitempty"`
	NodeKind       string                 `json:"node_kind,omitempty"`
	EdgeLabel      string                 `json:"edge_label,omitempty"`
	StableID       string                 `json:"stable_id,omitempty"`
	Contract       map[string]interface{} `json:"contract,omitempty"`
	Payload        map[string]interface{} `json:"payload,omitempty"`
	Submitter      string                 `json:"submitter,omitempty"`
	GoverningSpec  string                  `json:"governing_spec,omitempty"`
	SpecSection    string                  `json:"spec_section,omitempty"`

	// Edge-only fields (some submissions are edges, not nodes).
	SourceID     string `json:"source_id,omitempty"`
	TargetID     string `json:"target_id,omitempty"`
	SourceKind   string `json:"source_kind,omitempty"`
	TargetKind   string `json:"target_kind,omitempty"`

	// SourceFile is set by the loader when a submission was read from
	// disk; rules use it to populate Finding.SourceFile.
	SourceFile string `json:"-"`
}

// IsEdge reports whether this submission is an edge (carries an
// edge_label) rather than a node.
func (s Submission) IsEdge() bool { return s.EdgeLabel != "" }

// ConceptTable is the second input shape spec-lint accepts (emitted
// by the pipeline's Stage 2c — consolidated dedup output, before
// lint). The loader detects it by the presence of a top-level
// "concepts" array and flattens each Concept to a Submission for
// rule application.
type ConceptTable struct {
	ConceptDomain string    `json:"concept_domain,omitempty"`
	Concepts      []Concept `json:"concepts"`
	Edges         []Edge    `json:"edges,omitempty"`
}

// Concept is one node-to-be inside a ConceptTable.
type Concept struct {
	CanonicalName string                 `json:"canonical_name"`
	NodeKind      string                 `json:"node_kind"`
	Contract      map[string]interface{} `json:"contract,omitempty"`
	Payload       map[string]interface{} `json:"payload,omitempty"`
	SourceRefs    []string               `json:"source_refs,omitempty"`
}

// Edge is one edge-to-be inside a ConceptTable.
type Edge struct {
	EdgeLabel  string `json:"edge_label"`
	SourceName string `json:"source_name"`
	TargetName string `json:"target_name"`
}

// CanonicalName resolves per OOD_GRAPH_PROTOCOL_v1 §3 identity-tuple
// resolution rule: stable_id wins, then contract.canonical_name, then
// contract.canonical_err_name (sentinels).
func (s Submission) CanonicalName() string {
	if s.StableID != "" {
		return s.StableID
	}
	if s.Contract != nil {
		if v, ok := s.Contract["canonical_name"].(string); ok && v != "" {
			return v
		}
		if v, ok := s.Contract["canonical_err_name"].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// KindDescriptor declares the per-node-kind contract for a Submission.
// Loaded from spec-graph config; the linter mirrors spec-graph's
// existing closed-registry policy.
type KindDescriptor struct {
	Name                   string   `json:"name"`
	RequiredContractFields []string `json:"required_contract_fields,omitempty"`
	RequiredPayloadFields  []string `json:"required_payload_fields,omitempty"`
}

// EdgeLabelDescriptor declares the per-edge-label contract.
type EdgeLabelDescriptor struct {
	Name            string   `json:"name"`
	SourceKinds     []string `json:"source_kinds,omitempty"`
	TargetKinds     []string `json:"target_kinds,omitempty"`
	ForbidSelfLoops bool     `json:"forbid_self_loops,omitempty"`
}

// Config is the subset of spec-graph.config.json the linter needs.
//
// Two distinct concerns share one struct because both come from the same
// config file:
//   - NodeKinds + EdgeLabels: the closed registry R3/R4/R5/R10/R12 audit
//     against.
//   - Pipeline: the Flash credentials + prompt paths that R6 uses for
//     semantic prose detection.
//
// FlashRuntime is constructed in main.go after the file parse and
// attached to the in-memory Config; it is NOT a JSON-decoded field.
type Config struct {
	Mode              string           `json:"mode"`
	GoogleCredentials gcpauth.Selector `json:"google_credentials"`
	NodeKinds  []KindDescriptor      `json:"node_kinds"`
	EdgeLabels []EdgeLabelDescriptor `json:"edge_labels"`
	Pipeline   *PipelineConfig       `json:"pipeline,omitempty"`

	// Flash carries the live Generator + ctx for R6. Populated by main
	// after LoadConfig returns. NEVER serialized.
	Flash *FlashRuntime `json:"-"`
}

// PipelineConfig is the slice of spec-graph.config.json's pipeline
// section spec-lint reads. Today only flash is consumed; cluster and
// other pipeline subsections are ignored.
type PipelineConfig struct {
	Flash *FlashConfig `json:"flash"`
}

// FlashConfig is the Vertex AI Gemini Flash setup. Mirrors
// spec-dedup-verify's FlashConfig — same JSON shape, same validation
// discipline (every field REQUIRED, no defaults).
type FlashConfig struct {
	Provider          string              `json:"provider"`
	Project           string              `json:"project"`
	Location          string              `json:"location"`
	Model             string              `json:"model"`
	CredentialsFile   string              `json:"credentials_file,omitempty"`
	CredentialsSource string              `json:"credentials_source,omitempty"`
	MaxOutputTokens   int                 `json:"max_output_tokens"`
	Temperature       float64             `json:"temperature"`
	Prompts           *FlashPromptsConfig `json:"prompts"`
}

// FlashPromptsConfig declares the per-template file paths. Templates
// are loaded from disk on EVERY R6 evaluation (no in-memory cache) so
// editing the .txt file changes behavior without a binary restart.
// Other prompt paths (same_concept_query_path, canonical_name_selection_path)
// are spec-dedup-verify's concern and are ignored here.
type FlashPromptsConfig struct {
	R6ProseCheckPath string `json:"r6_prose_check_path"`
}

// FlashRuntime bundles the Generator + ctx + config snapshot R6 needs
// at Check() time. Built once in main, passed via Config so the Rule
// interface signature does not change.
type FlashRuntime struct {
	Ctx context.Context
	Gen Generator
	Cfg *FlashConfig
}

// Generator is the FlashClient surface R6 calls. Defined as an
// interface so tests can supply a deterministic mock without touching
// Vertex AI. Mirrors spec-dedup-verify's Generator definition.
type Generator interface {
	Generate(ctx context.Context, prompt string) (string, error)
}

func (c *Config) ValidateGoogleCredentialTopology() error {
	if c == nil {
		return fmt.Errorf("config required")
	}
	switch c.Mode {
	case "experimental_bootstrap", "production":
		// Closed below.
	case "":
		return fmt.Errorf("mode required")
	default:
		return fmt.Errorf("mode %q is unsupported", c.Mode)
	}
	if err := c.GoogleCredentials.Validate("google_credentials", c.Mode == "production"); err != nil {
		return err
	}
	return nil
}

// ValidateFlash enforces the pipeline.flash contract. Called by main
// before constructing FlashRuntime; tests that skip Flash supply their
// own runtime directly.
func (f *FlashConfig) Validate() error {
	if f == nil {
		return fmt.Errorf("pipeline.flash required")
	}
	if f.Provider == "" {
		return fmt.Errorf("pipeline.flash.provider required")
	}
	if f.Project == "" {
		return fmt.Errorf("pipeline.flash.project required")
	}
	if f.Location == "" {
		return fmt.Errorf("pipeline.flash.location required")
	}
	if f.Model == "" {
		return fmt.Errorf("pipeline.flash.model required")
	}
	if f.Provider != "vertex_ai" {
		return fmt.Errorf("pipeline.flash.provider must be vertex_ai")
	}
	if err := vertexai.ValidateEndpointComponents(f.Location, f.Project, f.Model); err != nil {
		return fmt.Errorf("pipeline.flash endpoint: %w", err)
	}
	if f.CredentialsFile != "" || f.CredentialsSource != "" {
		return fmt.Errorf("pipeline.flash legacy per-client credential selector is forbidden; use google_credentials")
	}
	if f.MaxOutputTokens <= 0 {
		return fmt.Errorf("pipeline.flash.max_output_tokens required (>0)")
	}
	if f.Temperature < 0 || f.Temperature > 2 {
		return fmt.Errorf("pipeline.flash.temperature required in [0, 2]")
	}
	if f.Prompts == nil {
		return fmt.Errorf("pipeline.flash.prompts required")
	}
	if f.Prompts.R6ProseCheckPath == "" {
		return fmt.Errorf("pipeline.flash.prompts.r6_prose_check_path required")
	}
	return nil
}

// KindByName returns the descriptor for a node_kind or nil.
func (c *Config) KindByName(name string) *KindDescriptor {
	for i := range c.NodeKinds {
		if c.NodeKinds[i].Name == name {
			return &c.NodeKinds[i]
		}
	}
	return nil
}

// EdgeLabelByName returns the descriptor for an edge_label or nil.
func (c *Config) EdgeLabelByName(name string) *EdgeLabelDescriptor {
	for i := range c.EdgeLabels {
		if c.EdgeLabels[i].Name == name {
			return &c.EdgeLabels[i]
		}
	}
	return nil
}

// LoadConfig parses a JSON config file into a Config.
func LoadConfig(data []byte) (*Config, error) {
	if err := gcpauth.ValidateConfigCredentialShape(data); err != nil {
		return nil, err
	}
	c := &Config{}
	if err := json.Unmarshal(data, c); err != nil {
		return nil, err
	}
	return c, nil
}
