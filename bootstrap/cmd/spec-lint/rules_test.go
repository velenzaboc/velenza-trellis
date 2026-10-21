package main

import (
	"context"
	"strings"
	"testing"
)

// alwaysCleanGenerator returns "VERDICT: clean" for every Flash call —
// the default for tests that don't care about R6 behavior but that
// run AllRules() (which includes R6) against a submission whose
// strings make it past the identifier-shape pre-filter.
type alwaysCleanGenerator struct{}

func (alwaysCleanGenerator) Generate(_ context.Context, _ string) (string, error) {
	return "VERDICT: clean\nREASON: ok", nil
}

func testConfig() *Config {
	// The R6 rule needs cfg.Flash. Install a runtime that fakes prompt
	// load (so we don't hit disk) and returns clean for every value
	// the pre-filter does not short-circuit.
	readFileFunc = func(string) ([]byte, error) { return []byte("noop"), nil }
	return &Config{
		NodeKinds: []KindDescriptor{
			{Name: "axiom", RequiredContractFields: []string{"asserts"}, RequiredPayloadFields: []string{"title"}},
			{Name: "section", RequiredPayloadFields: []string{"statement"}},
			{Name: "invariant"},
			{Name: "sentinel", RequiredContractFields: []string{"canonical_err_name"}},
		},
		EdgeLabels: []EdgeLabelDescriptor{
			{Name: "depends_on", SourceKinds: []string{"axiom", "section"}, TargetKinds: []string{"axiom", "section"}},
			{Name: "raises", SourceKinds: []string{"section", "invariant"}, TargetKinds: []string{"sentinel"}},
			{Name: "audits", SourceKinds: []string{"audit_record"}, TargetKinds: []string{"axiom", "section"}, ForbidSelfLoops: true},
		},
		Flash: &FlashRuntime{
			Ctx: context.Background(),
			Gen: alwaysCleanGenerator{},
			Cfg: &FlashConfig{Prompts: &FlashPromptsConfig{R6ProseCheckPath: "test:noop"}},
		},
	}
}

// ---------------- R3 ----------------

func TestR3_NodeKindInRegistry(t *testing.T) {
	r := &R3KindInRegistry{}
	sub := Submission{NodeKind: "axiom", StableID: "x"}
	if got := r.Check(sub, testConfig()); len(got) != 0 {
		t.Errorf("expected clean, got %d findings", len(got))
	}
}

func TestR3_NodeKindMissingFromRegistry(t *testing.T) {
	r := &R3KindInRegistry{}
	sub := Submission{NodeKind: "wrong_kind", StableID: "x"}
	got := r.Check(sub, testConfig())
	if len(got) != 1 || got[0].Rule != "R3_kind_in_registry" {
		t.Errorf("expected 1 R3 finding, got %+v", got)
	}
}

func TestR3_EdgeLabelInRegistry(t *testing.T) {
	r := &R3KindInRegistry{}
	sub := Submission{EdgeLabel: "depends_on", SourceID: "a", TargetID: "b"}
	if got := r.Check(sub, testConfig()); len(got) != 0 {
		t.Errorf("expected clean edge, got %+v", got)
	}
}

func TestR3_EdgeLabelMissingFromRegistry(t *testing.T) {
	r := &R3KindInRegistry{}
	sub := Submission{EdgeLabel: "bogus_label", SourceID: "a", TargetID: "b"}
	got := r.Check(sub, testConfig())
	if len(got) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(got))
	}
	if !strings.Contains(got[0].Detail, "bogus_label") {
		t.Errorf("expected detail to name the label, got %q", got[0].Detail)
	}
}

// ---------------- R4 ----------------

func TestR4_AllContractFieldsPresent(t *testing.T) {
	r := &R4RequiredContractFields{}
	sub := Submission{NodeKind: "axiom", StableID: "x", Contract: map[string]interface{}{"asserts": "y"}}
	if got := r.Check(sub, testConfig()); len(got) != 0 {
		t.Errorf("expected clean, got %+v", got)
	}
}

func TestR4_MissingContractField(t *testing.T) {
	r := &R4RequiredContractFields{}
	sub := Submission{NodeKind: "axiom", StableID: "x", Contract: map[string]interface{}{}}
	got := r.Check(sub, testConfig())
	if len(got) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(got))
	}
	if got[0].FieldPath != "contract.asserts" {
		t.Errorf("expected field_path=contract.asserts, got %q", got[0].FieldPath)
	}
}

// ---------------- R5 ----------------

func TestR5_AllPayloadFieldsPresent(t *testing.T) {
	r := &R5RequiredPayloadFields{}
	sub := Submission{NodeKind: "axiom", StableID: "x", Payload: map[string]interface{}{"title": "y"}}
	if got := r.Check(sub, testConfig()); len(got) != 0 {
		t.Errorf("expected clean, got %+v", got)
	}
}

func TestR5_MissingPayloadField(t *testing.T) {
	r := &R5RequiredPayloadFields{}
	sub := Submission{NodeKind: "axiom", StableID: "x", Payload: map[string]interface{}{}}
	got := r.Check(sub, testConfig())
	if len(got) != 1 || got[0].FieldPath != "payload.title" {
		t.Errorf("expected payload.title finding, got %+v", got)
	}
}

// ---------------- R7 ----------------

func TestR7_DefaultPatternMatch(t *testing.T) {
	r := &R7CanonicalNamePattern{}
	sub := Submission{NodeKind: "axiom", StableID: "valid_snake_case_name"}
	if got := r.Check(sub, nil); len(got) != 0 {
		t.Errorf("expected clean, got %+v", got)
	}
}

func TestR7_DefaultPatternRejectsCamelCase(t *testing.T) {
	r := &R7CanonicalNamePattern{}
	sub := Submission{NodeKind: "axiom", StableID: "InvalidCamelCase"}
	if got := r.Check(sub, nil); len(got) != 1 {
		t.Errorf("expected 1 R7 finding for CamelCase, got %+v", got)
	}
}

func TestR7_SectionAllowsDots(t *testing.T) {
	r := &R7CanonicalNamePattern{}
	sub := Submission{NodeKind: "section", StableID: "s14.access_control"}
	if got := r.Check(sub, nil); len(got) != 0 {
		t.Errorf("expected dot-in-section clean, got %+v", got)
	}
}

func TestR7_SentinelRequiresErrPrefix(t *testing.T) {
	r := &R7CanonicalNamePattern{}
	good := Submission{NodeKind: "sentinel", Contract: map[string]interface{}{"canonical_err_name": "ErrSomething"}}
	if got := r.Check(good, nil); len(got) != 0 {
		t.Errorf("expected sentinel ErrX clean, got %+v", got)
	}
	bad := Submission{NodeKind: "sentinel", Contract: map[string]interface{}{"canonical_err_name": "no_err_prefix"}}
	if got := r.Check(bad, nil); len(got) != 1 {
		t.Errorf("expected sentinel-no-Err-prefix finding, got %+v", got)
	}
}

// ---------------- R8 ----------------

func TestR8_DuplicateDetection(t *testing.T) {
	r := &R8NoDuplicate{}
	subs := []Submission{
		{NodeKind: "axiom", StableID: "node_a", SourceFile: "001.json"},
		{NodeKind: "axiom", StableID: "node_a", SourceFile: "002.json"},
		{NodeKind: "axiom", StableID: "node_b", SourceFile: "003.json"},
	}
	got := r.CheckBatch(subs, testConfig())
	if len(got) != 1 {
		t.Fatalf("expected 1 duplicate finding, got %+v", got)
	}
	if !strings.Contains(got[0].Detail, "001.json") {
		t.Errorf("duplicate finding should reference prior submission (001.json), got %q", got[0].Detail)
	}
}

func TestR8_DifferentKindsSameNameNotDuplicate(t *testing.T) {
	r := &R8NoDuplicate{}
	subs := []Submission{
		{NodeKind: "axiom", StableID: "shared_name", SourceFile: "a.json"},
		{NodeKind: "invariant", StableID: "shared_name", SourceFile: "b.json"},
	}
	if got := r.CheckBatch(subs, testConfig()); len(got) != 0 {
		t.Errorf("different kinds same name should not duplicate, got %+v", got)
	}
}

// ---------------- R10 ----------------

func TestR10_ValidEdgeKinds(t *testing.T) {
	r := &R10EdgeEndpointKinds{}
	sub := Submission{EdgeLabel: "depends_on", SourceID: "a", TargetID: "b", SourceKind: "axiom", TargetKind: "section"}
	if got := r.Check(sub, testConfig()); len(got) != 0 {
		t.Errorf("expected clean, got %+v", got)
	}
}

func TestR10_InvalidSourceKind(t *testing.T) {
	r := &R10EdgeEndpointKinds{}
	sub := Submission{EdgeLabel: "raises", SourceID: "a", TargetID: "b", SourceKind: "axiom", TargetKind: "sentinel"}
	got := r.Check(sub, testConfig())
	if len(got) != 1 {
		t.Errorf("expected source_kind violation for raises-from-axiom, got %+v", got)
	}
}

func TestR10_InvalidTargetKind(t *testing.T) {
	r := &R10EdgeEndpointKinds{}
	sub := Submission{EdgeLabel: "raises", SourceID: "a", TargetID: "b", SourceKind: "section", TargetKind: "axiom"}
	got := r.Check(sub, testConfig())
	if len(got) != 1 {
		t.Errorf("expected target_kind violation for raises-to-axiom, got %+v", got)
	}
}

// ---------------- R12 ----------------

func TestR12_SelfLoopForbidden(t *testing.T) {
	r := &R12SelfLoop{}
	sub := Submission{EdgeLabel: "audits", SourceID: "x", TargetID: "x"}
	if got := r.Check(sub, testConfig()); len(got) != 1 {
		t.Errorf("expected self-loop violation, got %+v", got)
	}
}

func TestR12_SelfLoopAllowedWhenNotForbidden(t *testing.T) {
	r := &R12SelfLoop{}
	sub := Submission{EdgeLabel: "depends_on", SourceID: "x", TargetID: "x"}
	if got := r.Check(sub, testConfig()); len(got) != 0 {
		t.Errorf("expected clean (label allows self-loops), got %+v", got)
	}
}

// ---------------- R13 ----------------

func TestR13_SourceReferenceKey(t *testing.T) {
	r := &R13NoSourceReference{}
	sub := Submission{NodeKind: "axiom", StableID: "x", Payload: map[string]interface{}{"source_reference": "anywhere"}}
	if got := r.Check(sub, nil); len(got) == 0 {
		t.Error("expected R13 violation on source_reference key")
	}
}

func TestR13_SpecsLocalPathInValue(t *testing.T) {
	r := &R13NoSourceReference{}
	sub := Submission{NodeKind: "axiom", StableID: "x",
		Payload: map[string]interface{}{"note": "see /workspace/specs-local/VOLT_v1.ood.json"}}
	if got := r.Check(sub, nil); len(got) == 0 {
		t.Error("expected R13 violation on specs-local path in value")
	}
}

func TestR13_OodJsonSuffix(t *testing.T) {
	r := &R13NoSourceReference{}
	sub := Submission{NodeKind: "axiom", StableID: "x",
		Payload: map[string]interface{}{"derived_from": "FOO.ood.json"}}
	if got := r.Check(sub, nil); len(got) == 0 {
		t.Error("expected R13 violation on .ood.json suffix")
	}
}

func TestR13_CleanValue(t *testing.T) {
	r := &R13NoSourceReference{}
	sub := Submission{NodeKind: "axiom", StableID: "x",
		Payload: map[string]interface{}{"title": "a clean title"}}
	if got := r.Check(sub, nil); len(got) != 0 {
		t.Errorf("expected clean, got %+v", got)
	}
}

// ---------------- R15 ----------------

func TestR15_AuditRecordRejected(t *testing.T) {
	r := &R15NoAdmissionArtifacts{}
	sub := Submission{NodeKind: "audit_record", StableID: "x"}
	if got := r.Check(sub, nil); len(got) != 1 {
		t.Errorf("expected R15 violation on audit_record, got %+v", got)
	}
}

func TestR15_SnapshotRejected(t *testing.T) {
	r := &R15NoAdmissionArtifacts{}
	sub := Submission{NodeKind: "snapshot", StableID: "x"}
	if got := r.Check(sub, nil); len(got) != 1 {
		t.Errorf("expected R15 violation on snapshot, got %+v", got)
	}
}

func TestR15_NormalKindAccepted(t *testing.T) {
	r := &R15NoAdmissionArtifacts{}
	sub := Submission{NodeKind: "axiom", StableID: "x"}
	if got := r.Check(sub, nil); len(got) != 0 {
		t.Errorf("expected clean, got %+v", got)
	}
}

// ---------------- RunRules integration ----------------

func TestRunRules_AggregatesAcrossRules(t *testing.T) {
	subs := []Submission{
		{NodeKind: "axiom", StableID: "node_a", Contract: map[string]interface{}{"asserts": "yes"},
			Payload: map[string]interface{}{"title": "clean title"}},
		{NodeKind: "axiom", StableID: "node_a", Contract: map[string]interface{}{"asserts": "yes"},
			Payload: map[string]interface{}{"title": "clean title"}}, // dup → R8
		{NodeKind: "bogus", StableID: "node_c"}, // missing contract/payload → R3 fires (kind not in registry)
	}
	findings := RunRules(subs, testConfig(), AllRules())
	rules := map[string]bool{}
	for _, f := range findings {
		rules[f.Rule] = true
	}
	if !rules["R8_no_unintended_duplicate"] {
		t.Error("expected R8 finding")
	}
	if !rules["R3_kind_in_registry"] {
		t.Error("expected R3 finding")
	}
}
