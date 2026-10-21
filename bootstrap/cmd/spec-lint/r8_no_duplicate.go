package main

import "fmt"

// R8NoDuplicate is a BatchRule that operates across the full submission
// set. Two submissions sharing (node_kind, canonical_name) for the same
// concept are flagged as duplicates: the older is canonical, the newer
// is the duplicate. Per the validator definition, near-duplicate
// detection (different canonical_names but same semantic claim) requires
// a vector index — out of scope for the linter; that lives in
// spec_graph_search.
type R8NoDuplicate struct{}

func (r *R8NoDuplicate) Name() string { return "R8_no_unintended_duplicate" }

func (r *R8NoDuplicate) Check(sub Submission, cfg *Config) []Finding { return nil }

func (r *R8NoDuplicate) CheckBatch(subs []Submission, cfg *Config) []Finding {
	type key struct{ kind, name string }
	seen := map[key]Submission{}
	var findings []Finding
	for _, s := range subs {
		if s.IsEdge() {
			continue
		}
		name := s.CanonicalName()
		if name == "" {
			continue
		}
		k := key{kind: s.NodeKind, name: name}
		if prior, ok := seen[k]; ok {
			findings = append(findings, Finding{
				Severity:      "fail",
				Rule:          "R8_no_unintended_duplicate",
				SourceFile:    s.SourceFile,
				CanonicalName: name,
				ObjectClass:   s.NodeKind,
				FieldPath:     "canonical_name",
				Observed:      name,
				Detail: fmt.Sprintf(
					"duplicate (node_kind=%q, canonical_name=%q); prior submission in %q",
					s.NodeKind, name, prior.SourceFile),
			})
			continue
		}
		seen[k] = s
	}
	return findings
}
