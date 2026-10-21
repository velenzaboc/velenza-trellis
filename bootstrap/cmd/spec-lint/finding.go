// Package main implements spec-lint, the deterministic R-rule linter for
// spec-graph concept submissions. The linter validates submission JSON
// against the R-rules declared by the Doc-Graph-Validator agent definition
// BEFORE those submissions reach the graph, eliminating the
// admit-then-discover-violation round-trip that produces churn in the
// authoring pipeline.
//
// Output JSON shape matches the C8 validator findings format so a single
// downstream consumer (drift-fix dispatch, validator report aggregator)
// can read either source.
package main

// Finding is one rule violation against one field of one node submission.
// Schema matches the C8 validator findings JSON so author and validator
// outputs are interchangeable downstream.
type Finding struct {
	Severity      string `json:"severity"`       // "fail" | "warn"
	Rule          string `json:"rule"`           // e.g. "R6_no_prose"
	GraphObjectID string `json:"graph_object_id,omitempty"`
	SourceFile    string `json:"source_file,omitempty"`
	CanonicalName string `json:"canonical_name,omitempty"`
	ObjectClass   string `json:"object_class,omitempty"`
	FieldPath     string `json:"field_path"` // dotted path into the submission
	Observed      string `json:"observed,omitempty"`
	Expected      string `json:"expected,omitempty"`
	Detail        string `json:"detail,omitempty"`
}

// Report is the top-level lint output, parallel to the C8 validator
// verdict-and-findings file shape.
type Report struct {
	ConceptDomain  string    `json:"concept_domain,omitempty"`
	NodesInspected int       `json:"nodes_inspected"`
	Verdict        string    `json:"verdict"` // "pass" | "fail"
	SummaryCounts  Counts    `json:"summary_counts"`
	Findings       []Finding `json:"findings"`
}

// Counts is a per-severity rollup of the report's findings.
type Counts struct {
	Fail int `json:"fail"`
	Warn int `json:"warn"`
}
