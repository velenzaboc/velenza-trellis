package main

import "fmt"

// R10EdgeEndpointKinds validates that an edge submission's source kind
// and target kind both appear in the edge_label descriptor's
// source_kinds and target_kinds lists. Only applies to edge submissions
// (those carrying edge_label). Empty source_kinds / target_kinds in the
// descriptor means "any kind is admissible" — the linter mirrors
// spec-graph's validateEdgeEndpoints behavior in handlers_test.go.
type R10EdgeEndpointKinds struct{}

func (r *R10EdgeEndpointKinds) Name() string { return "R10_edge_endpoint_kinds" }

func (r *R10EdgeEndpointKinds) Check(sub Submission, cfg *Config) []Finding {
	if cfg == nil || !sub.IsEdge() {
		return nil
	}
	desc := cfg.EdgeLabelByName(sub.EdgeLabel)
	if desc == nil {
		return nil // R3 catches the registry miss
	}
	var findings []Finding
	if len(desc.SourceKinds) > 0 && sub.SourceKind != "" && !contains(desc.SourceKinds, sub.SourceKind) {
		findings = append(findings, Finding{
			Severity:   "fail",
			Rule:       "R10_edge_endpoint_kinds",
			SourceFile: sub.SourceFile,
			FieldPath:  "source_id",
			Observed:   sub.SourceKind,
			Expected:   fmt.Sprintf("source_kind in %v for edge_label %q", desc.SourceKinds, sub.EdgeLabel),
			Detail: fmt.Sprintf(
				"source kind %q not in source_kinds for edge_label %q",
				sub.SourceKind, sub.EdgeLabel),
		})
	}
	if len(desc.TargetKinds) > 0 && sub.TargetKind != "" && !contains(desc.TargetKinds, sub.TargetKind) {
		findings = append(findings, Finding{
			Severity:   "fail",
			Rule:       "R10_edge_endpoint_kinds",
			SourceFile: sub.SourceFile,
			FieldPath:  "target_id",
			Observed:   sub.TargetKind,
			Expected:   fmt.Sprintf("target_kind in %v for edge_label %q", desc.TargetKinds, sub.EdgeLabel),
			Detail: fmt.Sprintf(
				"target kind %q not in target_kinds for edge_label %q",
				sub.TargetKind, sub.EdgeLabel),
		})
	}
	return findings
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
