package main

import "fmt"

// R12SelfLoop validates that an edge submission whose edge_label
// descriptor sets forbid_self_loops=true does NOT have source_id ==
// target_id. Per the validator definition: spec_graph_link enforces
// this structurally; the linter runs it pre-admission.
type R12SelfLoop struct{}

func (r *R12SelfLoop) Name() string { return "R12_self_loop" }

func (r *R12SelfLoop) Check(sub Submission, cfg *Config) []Finding {
	if cfg == nil || !sub.IsEdge() {
		return nil
	}
	if sub.SourceID == "" || sub.TargetID == "" || sub.SourceID != sub.TargetID {
		return nil
	}
	desc := cfg.EdgeLabelByName(sub.EdgeLabel)
	if desc == nil || !desc.ForbidSelfLoops {
		return nil
	}
	return []Finding{{
		Severity:   "fail",
		Rule:       "R12_self_loop",
		SourceFile: sub.SourceFile,
		FieldPath:  "source_id == target_id",
		Observed:   sub.SourceID,
		Detail: fmt.Sprintf(
			"self-loop on edge_label %q is forbidden (forbid_self_loops=true)",
			sub.EdgeLabel),
	}}
}
