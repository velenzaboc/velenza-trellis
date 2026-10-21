package main

import "fmt"

// R15NoAdmissionArtifacts flags submissions whose node_kind is one of
// the admission-ceremony-emitted classes (audit_record, snapshot). Per
// the validator definition: these are spec-graph-emitted by the
// admission ceremony, not author-authored. An author submitting one
// indicates a malformed concept or a process violation.
type R15NoAdmissionArtifacts struct{}

func (r *R15NoAdmissionArtifacts) Name() string { return "R15_no_admission_artifacts" }

var r15ForbiddenAuthorKinds = map[string]struct{}{
	"audit_record": {},
	"snapshot":     {},
}

func (r *R15NoAdmissionArtifacts) Check(sub Submission, cfg *Config) []Finding {
	if sub.IsEdge() {
		return nil
	}
	if _, forbidden := r15ForbiddenAuthorKinds[sub.NodeKind]; !forbidden {
		return nil
	}
	return []Finding{{
		Severity:      "fail",
		Rule:          "R15_no_admission_artifacts",
		SourceFile:    sub.SourceFile,
		CanonicalName: sub.CanonicalName(),
		ObjectClass:   sub.NodeKind,
		FieldPath:     "node_kind",
		Observed:      sub.NodeKind,
		Detail: fmt.Sprintf(
			"node_kind %q is spec-graph-emitted by the admission ceremony; not author-authored",
			sub.NodeKind),
	}}
}
