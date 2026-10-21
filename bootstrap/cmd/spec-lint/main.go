package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
)

// spec-lint runs the deterministic R-rules against one or more
// submission JSON files (or a directory of them) and emits a structured
// findings report. Output JSON shape matches the C8 validator findings
// file so downstream consumers (drift-fix dispatch, validator
// aggregator) accept either source.
//
// Usage:
//
//	spec-lint --config <path> --input <file-or-dir> [--output <path>]
//	spec-lint --config <path> --input <dir> --concept-domain <name>
//
// Exit codes:
//
//	0  pass — no findings
//	1  fail — one or more findings
//	2  invocation error (bad flag, missing file)
var (
	constructVertexAuth = func(ctx context.Context, factory vertexai.TokenSourceFactory) (*vertexai.Auth, error) {
		return vertexai.NewAuth(ctx, factory)
	}
	constructFlashGenerator = func(auth *vertexai.Auth, cfg vertexai.FlashConfig) (Generator, error) {
		return vertexai.NewFlashClient(auth, cfg)
	}
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run owns the command's observable behavior while returning its exit code so
// every fail-closed invocation path is directly testable without subprocesses.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("spec-lint", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to spec-graph config JSON (required)")
	inputPath := flags.String("input", "", "path to submission JSON file or directory (required)")
	outputPath := flags.String("output", "", "path to write findings JSON (default: stdout)")
	conceptDomain := flags.String("concept-domain", "", "optional concept_domain tag for the report")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *configPath == "" || *inputPath == "" {
		fmt.Fprintln(stderr, "spec-lint: --config and --input are required")
		flags.Usage()
		return 2
	}

	cfgData, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "spec-lint: read config: %v\n", err)
		return 2
	}
	cfg, err := LoadConfig(cfgData)
	if err != nil {
		fmt.Fprintf(stderr, "spec-lint: parse config: %v\n", err)
		return 2
	}

	subs, err := LoadSubmissions(*inputPath)
	if err != nil {
		fmt.Fprintf(stderr, "spec-lint: load submissions: %v\n", err)
		return 2
	}

	// Build the Flash runtime R6 needs and attach to cfg before rule
	// dispatch. Required (not optional): R6 is part of AllRules() and
	// fail-closes if Flash is absent.
	if cfg.Pipeline == nil || cfg.Pipeline.Flash == nil {
		fmt.Fprintln(stderr, "spec-lint: config.pipeline.flash required (R6 prose detection is Flash-backed)")
		return 2
	}
	if err := cfg.ValidateGoogleCredentialTopology(); err != nil {
		fmt.Fprintf(stderr, "spec-lint: google credentials invalid: %v\n", err)
		return 2
	}
	if err := cfg.Pipeline.Flash.Validate(); err != nil {
		fmt.Fprintf(stderr, "spec-lint: pipeline.flash invalid: %v\n", err)
		return 2
	}
	ctx := context.Background()
	auth, err := constructVertexAuth(ctx, cfg.GoogleCredentials.RequestTokenSourceFactory(gcpauth.CloudPlatformScope))
	if err != nil {
		fmt.Fprintf(stderr, "spec-lint: vertex auth: %v\n", err)
		return 2
	}
	gen, err := constructFlashGenerator(auth, vertexai.FlashConfig{
		Project:         cfg.Pipeline.Flash.Project,
		Location:        cfg.Pipeline.Flash.Location,
		Model:           cfg.Pipeline.Flash.Model,
		MaxOutputTokens: cfg.Pipeline.Flash.MaxOutputTokens,
		Temperature:     cfg.Pipeline.Flash.Temperature,
	})
	if err != nil {
		fmt.Fprintf(stderr, "spec-lint: vertex client: %v\n", err)
		return 2
	}
	cfg.Flash = &FlashRuntime{Ctx: ctx, Gen: gen, Cfg: cfg.Pipeline.Flash}

	findings := RunRules(subs, cfg, AllRules())

	report := Report{
		ConceptDomain:  *conceptDomain,
		NodesInspected: len(subs),
		Findings:       findings,
	}
	for _, f := range findings {
		switch f.Severity {
		case "fail":
			report.SummaryCounts.Fail++
		case "warn":
			report.SummaryCounts.Warn++
		}
	}
	if report.SummaryCounts.Fail == 0 {
		report.Verdict = "pass"
	} else {
		report.Verdict = "fail"
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(stderr, "spec-lint: marshal report: %v\n", err)
		return 2
	}
	if *outputPath != "" {
		if err := os.WriteFile(*outputPath, out, 0o644); err != nil {
			fmt.Fprintf(stderr, "spec-lint: write output: %v\n", err)
			return 2
		}
	} else {
		fmt.Fprintln(stdout, string(out))
	}

	if report.Verdict == "fail" {
		return 1
	}
	return 0
}

// LoadSubmissions reads input from either a single JSON file or a
// directory of JSON files. Two input shapes are recognized:
//
//  1. Submission shape — top-level keys include node_kind / edge_label
//     / contract / payload. The historical input format for direct
//     linting of spec_graph_import-style submission files.
//
//  2. ConceptTable shape — top-level keys are concept_domain +
//     concepts (array) [+ edges (array)]. Emitted by the pipeline's
//     Stage 2c (consolidated dedup output, before lint). Each concept
//     is flattened to a Submission for rule application.
//
// Directory-scan rule (Submission shape only): files whose name
// begins with "_" are skipped (authoring/validator artifacts, not
// submissions). ConceptTable input is single-file only.
func LoadSubmissions(path string) ([]Submission, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return loadFromDir(path)
	}
	return loadFromFileDetectShape(path)
}

// loadFromFileDetectShape reads the file, detects whether the JSON is
// a ConceptTable or a Submission, and returns the appropriate
// Submission slice. ConceptTable shape is detected by the presence of
// a "concepts" top-level key with array type.
func loadFromFileDetectShape(path string) ([]Submission, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Probe top-level shape via a tolerant unmarshal into a generic
	// map. If the file isn't a JSON object the conversion fails and we
	// fall through to Submission-shape parsing (which will produce its
	// own error).
	var probe map[string]interface{}
	if err := json.Unmarshal(data, &probe); err == nil {
		if _, isConceptTable := probe["concepts"].([]interface{}); isConceptTable {
			return loadConceptTable(data, path)
		}
	}
	var sub Submission
	if err := json.Unmarshal(data, &sub); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	sub.SourceFile = path
	return []Submission{sub}, nil
}

// loadConceptTable parses the JSON as ConceptTable and flattens its
// concepts into Submissions for rule application. The
// canonical_name → stable_id mapping is the bridge between the
// pipeline's identity model and the existing R-rule code's identity
// model (R7/R8 both read .CanonicalName(), which honors StableID
// first).
func loadConceptTable(data []byte, path string) ([]Submission, error) {
	var ct ConceptTable
	if err := json.Unmarshal(data, &ct); err != nil {
		return nil, fmt.Errorf("parse ConceptTable %s: %w", path, err)
	}
	subs := make([]Submission, 0, len(ct.Concepts)+len(ct.Edges))
	for _, c := range ct.Concepts {
		subs = append(subs, Submission{
			NodeKind:   c.NodeKind,
			StableID:   c.CanonicalName,
			Contract:   c.Contract,
			Payload:    c.Payload,
			SourceFile: path,
		})
	}
	for _, e := range ct.Edges {
		subs = append(subs, Submission{
			EdgeLabel:  e.EdgeLabel,
			SourceID:   e.SourceName,
			TargetID:   e.TargetName,
			SourceFile: path,
		})
	}
	return subs, nil
}

func loadFromDir(dir string) ([]Submission, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasPrefix(n, "_") || !strings.HasSuffix(n, ".json") {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	subs := make([]Submission, 0, len(names))
	for _, n := range names {
		sub, err := loadOneFile(filepath.Join(dir, n))
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", n, err)
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func loadOneFile(path string) (Submission, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Submission{}, err
	}
	var sub Submission
	if err := json.Unmarshal(data, &sub); err != nil {
		return Submission{}, fmt.Errorf("parse %s: %w", path, err)
	}
	sub.SourceFile = path
	return sub, nil
}
