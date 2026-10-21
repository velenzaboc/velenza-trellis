package main

import (
	"fmt"
	"regexp"
)

// R7CanonicalNamePattern validates that the resolved canonical_name
// matches the per-kind regex declared in the validator definition:
//   section: ^[a-z][a-z0-9_.]*$
//   sentinel.canonical_err_name: ^Err[A-Z][A-Za-z0-9]*$
//   all other kinds: ^[a-z][a-z0-9_]*$
type R7CanonicalNamePattern struct{}

func (r *R7CanonicalNamePattern) Name() string { return "R7_canonical_name_pattern" }

var (
	r7SectionPattern  = regexp.MustCompile(`^[a-z][a-z0-9_.]*$`)
	r7SentinelPattern = regexp.MustCompile(`^Err[A-Z][A-Za-z0-9]*$`)
	r7DefaultPattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

func (r *R7CanonicalNamePattern) Check(sub Submission, cfg *Config) []Finding {
	if sub.IsEdge() {
		return nil
	}
	name := sub.CanonicalName()
	if name == "" {
		return nil // R3/R4 catch missing identity separately
	}
	var pattern *regexp.Regexp
	var patternDesc string
	switch sub.NodeKind {
	case "section":
		pattern = r7SectionPattern
		patternDesc = "^[a-z][a-z0-9_.]*$"
	case "sentinel":
		pattern = r7SentinelPattern
		patternDesc = "^Err[A-Z][A-Za-z0-9]*$"
	default:
		pattern = r7DefaultPattern
		patternDesc = "^[a-z][a-z0-9_]*$"
	}
	if !pattern.MatchString(name) {
		return []Finding{{
			Severity:      "fail",
			Rule:          "R7_canonical_name_pattern",
			SourceFile:    sub.SourceFile,
			CanonicalName: name,
			ObjectClass:   sub.NodeKind,
			FieldPath:     "canonical_name",
			Observed:      name,
			Expected:      patternDesc,
			Detail:        fmt.Sprintf("canonical_name %q does not match %s for kind %q", name, patternDesc, sub.NodeKind),
		}}
	}
	return nil
}
