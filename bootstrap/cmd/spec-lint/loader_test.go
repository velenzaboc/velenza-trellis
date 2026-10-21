package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadSubmissions_SubmissionShape_PreservesLegacyBehavior verifies
// that direct submission JSON still loads correctly — the loader's
// shape-detection must not regress the historical path.
func TestLoadSubmissions_SubmissionShape_PreservesLegacyBehavior(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.json")
	sub := Submission{NodeKind: "axiom", StableID: "ax_1",
		Contract: map[string]interface{}{"asserts": "y"},
		Payload:  map[string]interface{}{"title": "T"}}
	data, _ := json.Marshal(sub)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	subs, err := LoadSubmissions(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected 1 submission, got %d", len(subs))
	}
	if subs[0].NodeKind != "axiom" || subs[0].StableID != "ax_1" {
		t.Errorf("submission fields not preserved: %+v", subs[0])
	}
	if subs[0].SourceFile != path {
		t.Errorf("source_file not set: %q", subs[0].SourceFile)
	}
}

// TestLoadSubmissions_ConceptTableShape_FlattensConcepts verifies the
// new ConceptTable input mode: the file's "concepts" array becomes a
// flat Submission slice, one per concept, with canonical_name mapped
// to stable_id so R7/R8 resolve identity correctly.
func TestLoadSubmissions_ConceptTableShape_FlattensConcepts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concepts.json")
	ct := ConceptTable{
		ConceptDomain: "test_domain",
		Concepts: []Concept{
			{CanonicalName: "node_a", NodeKind: "axiom",
				Contract: map[string]interface{}{"asserts": "y"}},
			{CanonicalName: "node_b", NodeKind: "section",
				Payload: map[string]interface{}{"statement": "S"}},
		},
	}
	data, _ := json.Marshal(ct)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	subs, err := LoadSubmissions(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 2 {
		t.Fatalf("expected 2 flattened submissions, got %d", len(subs))
	}
	// Order preserved from the concepts array.
	if subs[0].StableID != "node_a" || subs[0].NodeKind != "axiom" {
		t.Errorf("concept[0] not flattened correctly: %+v", subs[0])
	}
	if subs[1].StableID != "node_b" || subs[1].NodeKind != "section" {
		t.Errorf("concept[1] not flattened correctly: %+v", subs[1])
	}
	// Every flattened submission carries the SourceFile so findings
	// can attribute back to the table.
	for i, s := range subs {
		if s.SourceFile != path {
			t.Errorf("concept[%d] missing source_file: %q", i, s.SourceFile)
		}
	}
}

// TestLoadSubmissions_ConceptTableShape_FlattensEdges verifies that
// edges in a ConceptTable also become Submissions (with EdgeLabel set
// so IsEdge() reports true).
func TestLoadSubmissions_ConceptTableShape_FlattensEdges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concepts.json")
	ct := ConceptTable{
		Concepts: []Concept{{CanonicalName: "n1", NodeKind: "axiom"}},
		Edges: []Edge{
			{EdgeLabel: "references", SourceName: "n1", TargetName: "n2"},
		},
	}
	data, _ := json.Marshal(ct)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	subs, err := LoadSubmissions(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 2 {
		t.Fatalf("expected 2 submissions (1 node + 1 edge), got %d", len(subs))
	}
	// Edge submission second.
	if !subs[1].IsEdge() {
		t.Errorf("edge submission should report IsEdge=true: %+v", subs[1])
	}
	if subs[1].EdgeLabel != "references" || subs[1].SourceID != "n1" || subs[1].TargetID != "n2" {
		t.Errorf("edge fields not preserved: %+v", subs[1])
	}
}

// TestLoadSubmissions_ConceptTableShape_AppliesRulesEndToEnd verifies
// the integration: load a ConceptTable, run AllRules, observe rule
// findings against the flattened concepts.
func TestLoadSubmissions_ConceptTableShape_AppliesRulesEndToEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concepts.json")
	ct := ConceptTable{
		Concepts: []Concept{
			// Two with same (kind, name) — R8 should fire.
			{CanonicalName: "dup", NodeKind: "axiom"},
			{CanonicalName: "dup", NodeKind: "axiom"},
			// One with bogus kind — R3 should fire.
			{CanonicalName: "bogus_kind_node", NodeKind: "not_in_registry"},
		},
	}
	data, _ := json.Marshal(ct)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	subs, err := LoadSubmissions(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	findings := RunRules(subs, cfg, AllRules())
	rules := map[string]bool{}
	for _, f := range findings {
		rules[f.Rule] = true
	}
	if !rules["R8_no_unintended_duplicate"] {
		t.Error("R8 should fire on duplicate canonical_name")
	}
	if !rules["R3_kind_in_registry"] {
		t.Error("R3 should fire on bogus kind")
	}
}
