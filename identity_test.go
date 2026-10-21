package main

import (
	"strings"
	"testing"
)

// identity_test.go exercises the §3 grammar parsers/serializers. Every
// canonical-form input round-trips byte-equivalently through Parse ➜ String;
// malformed inputs return errors naming the rejected condition.

// ---------------- ValidateUUID ----------------

func TestValidateUUID(t *testing.T) {
	good := []string{
		"767c2399-87f9-4859-ac64-afb575bdb263", // ood_graph_protocol id
		"05c50ca8-5d1b-4eed-ba8a-4c21c5d01475", // volt canonical
		"00000000-0000-0000-0000-000000000000",
	}
	for _, s := range good {
		if err := ValidateUUID(s); err != nil {
			t.Errorf("ValidateUUID(%q) unexpectedly errored: %v", s, err)
		}
	}
	bad := []struct {
		s, reason string
	}{
		{"", "empty"},
		{"767c2399-87f9-4859-ac64-afb575bdb26", "missing last char"},
		{"767C2399-87F9-4859-AC64-AFB575BDB263", "uppercase"},
		{"767c2399_87f9_4859_ac64_afb575bdb263", "underscores not hyphens"},
		{"767c2399-87f9-4859-ac64-afb575bdb2638", "extra char"},
	}
	for _, c := range bad {
		if err := ValidateUUID(c.s); err == nil {
			t.Errorf("ValidateUUID(%q) should have failed (%s)", c.s, c.reason)
		}
	}
}

// ---------------- SectionPath ----------------

func TestValidateSectionPath(t *testing.T) {
	good := []string{
		"$",
		"$.sections",
		"$.sections.s3_symbol_identity_and_addressing",
		"$.sections.s3_symbol_identity_and_addressing.identity_vocabulary",
		"$.non_goals[0]",
		"$.non_goals[42]",
		"$.sections.s2.object_classes.doc_object",
		"$.sections.s4_edge_model.edge_label_taxonomy.labels.depends_on.consumed_by[0]",
	}
	for _, p := range good {
		if err := ValidateSectionPath(p); err != nil {
			t.Errorf("ValidateSectionPath(%q) unexpectedly errored: %v", p, err)
		}
	}
	bad := []struct {
		p, reason string
	}{
		{"", "empty"},
		{"sections.s1", "missing root"},
		{"$.", "trailing dot empty segment"},
		{"$.Sections", "PascalCase not snake_case"},
		{"$.sections-foo", "hyphen not allowed"},
		{"$.sections[01]", "leading zero array index"},
		{"$.sections[", "unterminated bracket"},
		{"$.sections[-1]", "negative array index"},
		{"$.sections.0badkey", "key starts with digit"},
		{"$.sections..s1", "empty segment between dots"},
	}
	for _, c := range bad {
		if err := ValidateSectionPath(c.p); err == nil {
			t.Errorf("ValidateSectionPath(%q) should have failed (%s)", c.p, c.reason)
		}
	}
}

func TestNormalizeSectionPath_Identity(t *testing.T) {
	// Per §3.section_path_grammar normalization, the grammar is already
	// byte-canonical; well-formed inputs round-trip unchanged.
	cases := []string{
		"$",
		"$.sections.s3_symbol_identity_and_addressing",
		"$.non_goals[0]",
	}
	for _, p := range cases {
		got, err := NormalizeSectionPath(p)
		if err != nil {
			t.Fatalf("NormalizeSectionPath(%q): %v", p, err)
		}
		if got != p {
			t.Errorf("NormalizeSectionPath(%q) = %q, want byte-equivalent", p, got)
		}
	}
}

// ---------------- VersionCoordinate ----------------

func TestVersionCoordinate_RoundTrip(t *testing.T) {
	cases := []string{
		"unversioned",
		"docobject:767c2399-87f9-4859-ac64-afb575bdb263",
		"docobject:767c2399-87f9-4859-ac64-afb575bdb263@v1",
		"docobject:767c2399-87f9-4859-ac64-afb575bdb263@v42",
		"spec:05c50ca8-5d1b-4eed-ba8a-4c21c5d01475@v33",
		"source:46954a80-78eb-4351-8be7-6e7d7eba8f5b:my_artifact_name@token1",
		"source:46954a80-78eb-4351-8be7-6e7d7eba8f5b:branch%2Fmain@sha256_abc123",
	}
	for _, s := range cases {
		vc, err := ParseVersionCoordinate(s)
		if err != nil {
			t.Fatalf("ParseVersionCoordinate(%q): %v", s, err)
		}
		got := vc.String()
		if got != s {
			t.Errorf("VersionCoordinate round-trip mismatch:\n  input:  %q\n  output: %q", s, got)
		}
	}
}

func TestVersionCoordinate_RejectsAliases(t *testing.T) {
	// Per §3.version_coordinate_grammar.normalization: current/latest aliases
	// forbidden; empty version forbidden; leading zeros forbidden.
	bad := []struct{ s, reason string }{
		{"", "empty"},
		{"current", "current alias"},
		{"latest", "latest alias"},
		{"docobject:current", "docobject current"},
		{"docobject:767c2399-87f9-4859-ac64-afb575bdb263@v0", "v0 rejected (versions are positive)"},
		{"docobject:767c2399-87f9-4859-ac64-afb575bdb263@v01", "leading zero"},
		{"docobject:not-a-uuid@v1", "invalid uuid"},
		{"spec:767c2399-87f9-4859-ac64-afb575bdb263", "spec without @v required"},
		{"docobject:", "docobject without id"},
	}
	for _, c := range bad {
		if _, err := ParseVersionCoordinate(c.s); err == nil {
			t.Errorf("ParseVersionCoordinate(%q) should have failed (%s)", c.s, c.reason)
		}
	}
}

// ---------------- GraphAddress ----------------

func TestGraphAddress_RoundTrip(t *testing.T) {
	// Build representative addresses spanning the protocol's example shapes.
	cases := []GraphAddress{
		{
			ObjectClass:       "doc_object",
			SourceAuthorityID: "46954a80-78eb-4351-8be7-6e7d7eba8f5b",
			SourceRefKind:     "id",
			SourceRef:         "767c2399-87f9-4859-ac64-afb575bdb263",
			VersionCoordinate: "docobject:767c2399-87f9-4859-ac64-afb575bdb263",
			SectionPath:       "$",
		},
		{
			ObjectClass:       "section",
			SourceAuthorityID: "767c2399-87f9-4859-ac64-afb575bdb263",
			SourceRefKind:     "path",
			SourceRef:         "$.sections.s3_symbol_identity_and_addressing",
			VersionCoordinate: "docobject:767c2399-87f9-4859-ac64-afb575bdb263@v1",
			SectionPath:       "$.sections.s3_symbol_identity_and_addressing",
		},
		{
			ObjectClass:       "sentinel",
			SourceAuthorityID: "3b929ea6-e771-451b-a51f-e3556f116915",
			SourceRefKind:     "name",
			SourceRef:         "ErrOODGraphEdgeLabelInvalid",
			VersionCoordinate: "spec:767c2399-87f9-4859-ac64-afb575bdb263@v1",
			SectionPath:       "$.sections.s15_sentinels.sentinels[0]",
		},
	}
	for _, ga := range cases {
		serialized := ga.String()
		parsed, err := ParseGraphAddress(serialized)
		if err != nil {
			t.Fatalf("ParseGraphAddress(%q): %v", serialized, err)
		}
		reSerialized := parsed.String()
		if reSerialized != serialized {
			t.Errorf("GraphAddress round-trip mismatch:\n  original:  %q\n  re-serial: %q", serialized, reSerialized)
		}
		// Field equality round-trip.
		if parsed.ObjectClass != ga.ObjectClass ||
			parsed.SourceAuthorityID != ga.SourceAuthorityID ||
			parsed.SourceRefKind != ga.SourceRefKind ||
			parsed.SourceRef != ga.SourceRef ||
			parsed.VersionCoordinate != ga.VersionCoordinate ||
			parsed.SectionPath != ga.SectionPath {
			t.Errorf("GraphAddress field-equality round-trip failed:\n  before: %+v\n  after:  %+v", ga, parsed)
		}
	}
}

func TestGraphAddress_RejectsMalformed(t *testing.T) {
	bad := []struct{ s, reason string }{
		{"", "empty"},
		{"https://example/foo", "wrong scheme"},
		{"oodg://doc_object/767c2399-87f9-4859-ac64-afb575bdb263/id/abc", "missing version + fragment"},
		{"oodg://doc_object/767c2399-87f9-4859-ac64-afb575bdb263/id/abc?version=docobject:767c2399-87f9-4859-ac64-afb575bdb263", "missing fragment"},
		{"oodg://doc_object/767c2399-87f9-4859-ac64-afb575bdb263/id/abc#$", "missing version query"},
		{"oodg://doc_object/767c2399-87f9-4859-ac64-afb575bdb263/badkind/abc?version=unversioned#$", "invalid source_ref_kind"},
		{"oodg://doc_object/NOTAUUID/id/abc?version=unversioned#$", "non-uuid authority"},
		{"oodg://doc_object/767c2399-87f9-4859-ac64-afb575bdb263/id/abc?version=current#$", "current alias in version"},
		{"oodg://doc_object/767c2399-87f9-4859-ac64-afb575bdb263/id/abc?version=unversioned&extra=1#$", "additional query param"},
		{"oodg://doc_object/767c2399-87f9-4859-ac64-afb575bdb263/id/abc?version=unversioned#bad path", "invalid section_path fragment"},
	}
	for _, c := range bad {
		if _, err := ParseGraphAddress(c.s); err == nil {
			t.Errorf("ParseGraphAddress(%q) should have failed (%s)", c.s, c.reason)
		} else if !strings.Contains(err.Error(), "graph_address") && !strings.Contains(err.Error(), "version") && !strings.Contains(err.Error(), "section_path") {
			t.Errorf("ParseGraphAddress(%q) failed but error message doesn't name the rejected condition: %v", c.s, err)
		}
	}
}
