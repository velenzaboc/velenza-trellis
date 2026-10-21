package main

import "fmt"

// R3KindInRegistry validates that the submission's node_kind (or
// edge_label, for edge submissions) appears in the loaded Config's
// closed registry. Per the validator definition: spec-graph admission
// enforces this structurally; the linter runs it pre-admission so
// authoring drift surfaces before the round-trip.
type R3KindInRegistry struct{}

func (r *R3KindInRegistry) Name() string { return "R3_kind_in_registry" }

func (r *R3KindInRegistry) Check(sub Submission, cfg *Config) []Finding {
	if cfg == nil {
		return nil
	}
	if sub.IsEdge() {
		if cfg.EdgeLabelByName(sub.EdgeLabel) == nil {
			return []Finding{{
				Severity:      "fail",
				Rule:          "R3_kind_in_registry",
				SourceFile:    sub.SourceFile,
				CanonicalName: sub.CanonicalName(),
				FieldPath:     "edge_label",
				Observed:      sub.EdgeLabel,
				Expected:      "edge_label registered in config.edge_labels",
				Detail:        fmt.Sprintf("edge_label %q is not in the registry", sub.EdgeLabel),
			}}
		}
		return nil
	}
	if sub.NodeKind == "" {
		return []Finding{{
			Severity:   "fail",
			Rule:       "R3_kind_in_registry",
			SourceFile: sub.SourceFile,
			FieldPath:  "node_kind",
			Detail:     "node_kind is empty",
		}}
	}
	if cfg.KindByName(sub.NodeKind) == nil {
		return []Finding{{
			Severity:      "fail",
			Rule:          "R3_kind_in_registry",
			SourceFile:    sub.SourceFile,
			CanonicalName: sub.CanonicalName(),
			ObjectClass:   sub.NodeKind,
			FieldPath:     "node_kind",
			Observed:      sub.NodeKind,
			Expected:      "node_kind registered in config.node_kinds",
			Detail:        fmt.Sprintf("node_kind %q is not in the registry", sub.NodeKind),
		}}
	}
	return nil
}
