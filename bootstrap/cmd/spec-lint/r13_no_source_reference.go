package main

import (
	"strings"
)

// R13NoSourceReference flags submissions whose payload carries a
// `source_reference` key or whose payload string values point back to
// specs-local/*.ood.json files by path or doc_id. The graph is
// canonical, not derivative — provenance lives on metadata, not
// embedded in the concept content.
type R13NoSourceReference struct{}

func (r *R13NoSourceReference) Name() string { return "R13_source_reference" }

// r13PathFragments are signals that a string value embeds a file-path
// reference back to the source spec corpus.
var r13PathFragments = []string{
	"specs-local/",
	"/workspace/specs-local",
	".ood.json",
}

func (r *R13NoSourceReference) Check(sub Submission, cfg *Config) []Finding {
	var findings []Finding
	check := func(prefix string, obj map[string]interface{}) {
		if obj == nil {
			return
		}
		if _, ok := obj["source_reference"]; ok {
			findings = append(findings, Finding{
				Severity:      "fail",
				Rule:          "R13_source_reference",
				SourceFile:    sub.SourceFile,
				CanonicalName: sub.CanonicalName(),
				ObjectClass:   sub.NodeKind,
				FieldPath:     prefix + ".source_reference",
				Detail:        "source_reference key is forbidden; provenance belongs on metadata, not embedded content",
			})
		}
		for k, v := range obj {
			if s, ok := v.(string); ok {
				for _, frag := range r13PathFragments {
					if strings.Contains(s, frag) {
						findings = append(findings, Finding{
							Severity:      "fail",
							Rule:          "R13_source_reference",
							SourceFile:    sub.SourceFile,
							CanonicalName: sub.CanonicalName(),
							ObjectClass:   sub.NodeKind,
							FieldPath:     prefix + "." + k,
							Observed:      s,
							Detail:        "value references a specs-local source file path; the graph is canonical",
						})
						break
					}
				}
			}
		}
	}
	check("contract", sub.Contract)
	check("payload", sub.Payload)
	return findings
}
