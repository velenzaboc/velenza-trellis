package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// mockGenerator records every prompt it receives and returns a pre-set
// response. Tests use it to assert (a) which strings invoked Flash and
// (b) how the parsed verdict propagates back into CheckR6String.
type mockGenerator struct {
	prompts   []string
	responses []string // FIFO; one per Generate call
	errs      []error  // FIFO; if non-nil for a slot, returned instead
}

func (m *mockGenerator) Generate(_ context.Context, prompt string) (string, error) {
	idx := len(m.prompts)
	m.prompts = append(m.prompts, prompt)
	if idx < len(m.errs) && m.errs[idx] != nil {
		return "", m.errs[idx]
	}
	if idx >= len(m.responses) {
		return "", fmt.Errorf("mockGenerator: no response for call #%d", idx)
	}
	return m.responses[idx], nil
}

// installFakePromptOnce overrides the readFileFunc indirection for the
// duration of one test so prompt rendering never touches disk. Restores
// the default on test cleanup.
func installFakePromptOnce(t *testing.T, body string) {
	t.Helper()
	orig := readFileFunc
	readFileFunc = func(string) ([]byte, error) { return []byte(body), nil }
	t.Cleanup(func() { readFileFunc = orig })
}

// minimalFlashCfg returns the cfg + runtime + mock the rule needs.
// Prompt body is a no-op Go template — the test exercises CheckR6String
// behavior, not template rendering, which has its own test.
func minimalFlashCfg(t *testing.T, gen Generator) *Config {
	t.Helper()
	installFakePromptOnce(t, "PROMPT for {{.Value}}")
	return &Config{
		Flash: &FlashRuntime{
			Ctx: context.Background(),
			Gen: gen,
			Cfg: &FlashConfig{
				Prompts: &FlashPromptsConfig{
					R6ProseCheckPath: "test:prompt.txt",
				},
			},
		},
	}
}

// ---------------- pre-filter (no Flash call) ----------------

// TestR6_PreFilter_SkipsIdentifierShapes covers every shape the spec
// accepts without further inspection. Each MUST NOT trigger a Flash
// call AND MUST return clean.
func TestR6_PreFilter_SkipsIdentifierShapes(t *testing.T) {
	cases := map[string]string{
		"empty":            "",
		"whitespace_only":  "   \t\n  ",
		"snake_case":       "matryoshka_brick",
		"dotted_path":      "core.driver.spanner_query",
		"kebab_case":       "single-source-of-truth",
		"uuid":             "bf4a8e2d-7c1b-4f93-a652-9d3e8b7f5c0e",
		"sha256_hex":       "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"file_path":        "/workspace/specs-local/VOLT_v34.ood.json",
		"version_tag":      "v34",
		"regex_literal":    "^[a-z][a-z0-9_]*$",
		"url":              "https://api.voyageai.com/v1/rerank",
	}
	gen := &mockGenerator{}
	cfg := minimalFlashCfg(t, gen)
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			got := CheckR6String(cfg, value, "payload.x", "node_x", "section")
			if got != "" {
				t.Errorf("expected clean (pre-filter), got violation: %s", got)
			}
		})
	}
	if len(gen.prompts) != 0 {
		t.Errorf("pre-filter should skip Flash entirely; got %d Flash calls", len(gen.prompts))
	}
}

// ---------------- Flash verdict propagation ----------------

// TestR6_FlashVerdict_CleanPropagates covers a multi-word value Flash
// classifies as a single declarative clause. Pre-filter does NOT
// short-circuit it (whitespace present); Flash is called once.
func TestR6_FlashVerdict_CleanPropagates(t *testing.T) {
	gen := &mockGenerator{
		responses: []string{"VERDICT: clean\nREASON: single_clause"},
	}
	cfg := minimalFlashCfg(t, gen)
	got := CheckR6String(cfg, "the grant is active", "payload.x", "node_x", "section")
	if got != "" {
		t.Errorf("expected clean (Flash verdict), got %q", got)
	}
	if len(gen.prompts) != 1 {
		t.Fatalf("expected exactly 1 Flash call, got %d", len(gen.prompts))
	}
}

// TestR6_FlashVerdict_ProsePropagates covers a value Flash classifies
// as prose. The returned detail MUST include "prose" and the Flash
// reason so the downstream agent can act on it without re-inspecting
// the raw value.
func TestR6_FlashVerdict_ProsePropagates(t *testing.T) {
	gen := &mockGenerator{
		responses: []string{"VERDICT: prose\nREASON: two finite verbs joined by 'and'"},
	}
	cfg := minimalFlashCfg(t, gen)
	got := CheckR6String(cfg, "the gate fires and the row commits", "payload.x", "node_x", "section")
	if got == "" {
		t.Fatal("expected prose violation, got clean")
	}
	if !strings.Contains(got, "prose") {
		t.Errorf("expected detail to name 'prose' verdict, got %q", got)
	}
	if !strings.Contains(got, "two finite verbs") {
		t.Errorf("expected detail to include Flash reason, got %q", got)
	}
}

// ---------------- fail-closed paths ----------------

// TestR6_FlashError_FailsClosed: Flash transport error → violation
// (NOT a silent pass). Vision Principle 6.
func TestR6_FlashError_FailsClosed(t *testing.T) {
	gen := &mockGenerator{
		errs: []error{fmt.Errorf("network unreachable")},
	}
	cfg := minimalFlashCfg(t, gen)
	got := CheckR6String(cfg, "the gate fires and the row commits", "payload.x", "node_x", "section")
	if got == "" {
		t.Fatal("expected fail-closed violation on Flash transport error, got clean")
	}
	if !strings.Contains(got, "flash call failed") {
		t.Errorf("expected detail to name flash call failure, got %q", got)
	}
}

// TestR6_FlashUnparseable_FailsClosed: garbage response (no VERDICT
// line) → violation.
func TestR6_FlashUnparseable_FailsClosed(t *testing.T) {
	gen := &mockGenerator{
		responses: []string{"I think probably it is fine ok"},
	}
	cfg := minimalFlashCfg(t, gen)
	got := CheckR6String(cfg, "the gate fires and the row commits", "payload.x", "node_x", "section")
	if got == "" {
		t.Fatal("expected fail-closed violation on unparseable response, got clean")
	}
	if !strings.Contains(got, "unparseable") {
		t.Errorf("expected detail to name unparseable response, got %q", got)
	}
}

// TestR6_BadVerdictToken_FailsClosed: VERDICT present but with a token
// outside {clean, prose}. Distinct from unparseable so the linter can
// distinguish prompt-following defects from transport defects.
func TestR6_BadVerdictToken_FailsClosed(t *testing.T) {
	gen := &mockGenerator{
		responses: []string{"VERDICT: maybe\nREASON: idk"},
	}
	cfg := minimalFlashCfg(t, gen)
	got := CheckR6String(cfg, "the gate fires and the row commits", "payload.x", "node_x", "section")
	if got == "" {
		t.Fatal("expected fail-closed violation on bad verdict token, got clean")
	}
	if !strings.Contains(got, "unparseable") {
		t.Errorf("expected detail to name unparseable response, got %q", got)
	}
}

// TestR6_NilFlashRuntime_FailsClosed: caller forgot to populate
// cfg.Flash before invoking Check. Violation with caller-defect detail
// — NOT a silent pass.
func TestR6_NilFlashRuntime_FailsClosed(t *testing.T) {
	cfg := &Config{} // no Flash runtime
	got := CheckR6String(cfg, "the gate fires and the row commits", "payload.x", "node_x", "section")
	if got == "" {
		t.Fatal("expected fail-closed violation when cfg.Flash is nil, got clean")
	}
	if !strings.Contains(got, "flash runtime not configured") {
		t.Errorf("expected caller-defect detail, got %q", got)
	}
}

// ---------------- response parser ----------------

func TestParseR6Response_Clean(t *testing.T) {
	v, r, err := ParseR6Response("VERDICT: clean\nREASON: single_clause")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "clean" {
		t.Errorf("expected verdict=clean, got %q", v)
	}
	if r != "single_clause" {
		t.Errorf("expected reason=single_clause, got %q", r)
	}
}

func TestParseR6Response_Prose(t *testing.T) {
	v, _, err := ParseR6Response("VERDICT: prose\nREASON: multiple sentences")
	if err != nil || v != "prose" {
		t.Errorf("expected (prose, nil), got (%q, %v)", v, err)
	}
}

func TestParseR6Response_CaseAndWhitespaceTolerant(t *testing.T) {
	v, _, err := ParseR6Response("  verdict:  CLEAN  \n  reason:  ok  ")
	if err != nil || v != "clean" {
		t.Errorf("expected (clean, nil), got (%q, %v)", v, err)
	}
}

func TestParseR6Response_NoVerdict(t *testing.T) {
	if _, _, err := ParseR6Response("REASON: only reason here"); err == nil {
		t.Error("expected error on missing VERDICT line")
	}
}

func TestParseR6Response_BadToken(t *testing.T) {
	if _, _, err := ParseR6Response("VERDICT: yes\nREASON: x"); err == nil {
		t.Error("expected error on token outside {clean,prose}")
	}
}

// ---------------- end-to-end via Rule interface ----------------

// TestR6_Rule_VisitsContractAndPayload covers the full rule: every
// string in contract + payload visited; identifier-shaped strings
// skip Flash; prose-shaped strings trigger Flash + emit findings.
func TestR6_Rule_VisitsContractAndPayload(t *testing.T) {
	// Two strings need Flash: contract.statement and payload.description.
	// Mock responds clean to the first, prose to the second.
	gen := &mockGenerator{
		responses: []string{
			"VERDICT: clean\nREASON: single_clause",
			"VERDICT: prose\nREASON: multi-clause",
		},
	}
	cfg := minimalFlashCfg(t, gen)
	cfg.NodeKinds = []KindDescriptor{{Name: "section"}}

	sub := Submission{
		NodeKind: "section",
		StableID: "node_x",
		Contract: map[string]interface{}{
			"canonical_name": "node_x",                  // identifier — skipped
			"statement":      "the grant is active",     // → Flash → clean
		},
		Payload: map[string]interface{}{
			"description": "the gate fires and the row commits because the verdict ratified the proposal",
			"kind":        "section",                    // identifier — skipped
		},
	}
	r := &R6NoProse{}
	findings := r.Check(sub, cfg)
	if len(findings) != 1 {
		t.Fatalf("expected exactly 1 finding (payload.description), got %d: %+v", len(findings), findings)
	}
	if findings[0].FieldPath != "payload.description" {
		t.Errorf("expected payload.description finding, got %q", findings[0].FieldPath)
	}
	if len(gen.prompts) != 2 {
		t.Errorf("expected exactly 2 Flash calls (two prose-eligible strings), got %d", len(gen.prompts))
	}
}
