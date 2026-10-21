package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// R6NoProse implements Rule for R6_no_prose. It recursively inspects
// every string value inside the submission's contract + payload and
// decides — per value — whether the string is a single declarative
// clause / identifier / enum (clean) or prose (violation).
//
// Detection method: a cheap identifier-shape pre-filter clears obvious
// non-prose without an LLM call (~80% of typical values). Everything
// else is referred to Gemini Flash with the r6_prose_check.txt prompt
// rendered per call. Flash error / unparseable response = fail-closed
// (treat as violation) per Vision Principle 6.
//
// History: the previous implementation was a stack of seven
// regex/length heuristics with a 200-character ceiling. That approach
// (a) false-flagged legitimate single-clause statements that exceeded
// 200 chars, (b) silently masked violations beyond the first detector
// to fire (per the Stage 4 agent's "latent_violations_caught_pre_rewrite"
// finding on volt_secret_detection), and (c) embedded an arbitrary
// numeric threshold the spec does not actually mandate. Architecture
// directive: "use gemini flash for cheap checks instead of a fucking
// arbitrary number." Refactor replaces the heuristic stack with one
// Flash call per ambiguous string.
type R6NoProse struct{}

func (r *R6NoProse) Name() string { return "R6_no_prose" }

func (r *R6NoProse) Check(sub Submission, cfg *Config) []Finding {
	var findings []Finding
	visit := func(prefix string, v interface{}) {}
	visit = func(prefix string, v interface{}) {
		switch t := v.(type) {
		case string:
			if detail := CheckR6String(cfg, t, prefix, sub.CanonicalName(), sub.NodeKind); detail != "" {
				findings = append(findings, Finding{
					Severity:      "fail",
					Rule:          "R6_no_prose",
					SourceFile:    sub.SourceFile,
					CanonicalName: sub.CanonicalName(),
					ObjectClass:   sub.NodeKind,
					FieldPath:     prefix,
					Observed:      t,
					Detail:        detail,
				})
			}
		case map[string]interface{}:
			for k, child := range t {
				visit(prefix+"."+k, child)
			}
		case []interface{}:
			for i, child := range t {
				visit(prefix+"["+strconv.Itoa(i)+"]", child)
			}
		}
	}
	if sub.Contract != nil {
		for k, v := range sub.Contract {
			visit("contract."+k, v)
		}
	}
	if sub.Payload != nil {
		for k, v := range sub.Payload {
			visit("payload."+k, v)
		}
	}
	return findings
}

// CheckR6String decides whether one string value violates R6.
// Returns "" if clean; returns a human-readable detail if violation.
//
// Decision ladder:
//  1. Cheap identifier-shape pre-filter (FREE — no LLM call). Skips
//     empty strings, single-token identifiers, and structural shapes
//     (snake_case, dotted_path, UUID, kebab-case, version tags, file
//     paths, regex literals). These are the well-formed shapes the
//     spec explicitly accepts; calling Flash on them wastes a call.
//  2. Flash call with the r6_prose_check.txt prompt rendered per
//     evaluation. Verdict "clean" → return "". Verdict "prose" → return
//     detail (reason from Flash).
//  3. Flash unreachable / unparseable / error → FAIL CLOSED (return
//     violation with the underlying error in detail). Vision Principle 6.
//
// cfg.Flash MUST be populated by main before any call; CheckR6String
// returns a violation if it is nil (caller invocation defect, not a
// silent pass).
func CheckR6String(cfg *Config, value, fieldPath, canonicalName, nodeKind string) string {
	if isIdentifierShape(value) {
		return ""
	}
	if cfg == nil || cfg.Flash == nil || cfg.Flash.Gen == nil || cfg.Flash.Cfg == nil {
		return "R6 flash runtime not configured (caller defect): cfg.Flash must be populated before rule evaluation"
	}
	prompt, err := renderR6Prompt(cfg.Flash.Cfg.Prompts.R6ProseCheckPath, value, fieldPath, canonicalName, nodeKind)
	if err != nil {
		return fmt.Sprintf("R6 flash prompt render failed (fail-closed): %v", err)
	}
	raw, err := cfg.Flash.Gen.Generate(cfg.Flash.Ctx, prompt)
	if err != nil {
		return fmt.Sprintf("R6 flash call failed (fail-closed): %v", err)
	}
	verdict, reason, parseErr := ParseR6Response(raw)
	if parseErr != nil {
		return fmt.Sprintf("R6 flash response unparseable (fail-closed): %v; raw=%q", parseErr, truncateForDetail(raw))
	}
	if verdict == "prose" {
		if reason == "" {
			return "R6 flash verdict: prose"
		}
		return "R6 flash verdict: prose — " + reason
	}
	return ""
}

// isIdentifierShape returns true if the value matches one of the
// well-formed shapes the spec accepts without further inspection.
//
// Recognized shapes (any one is sufficient to skip Flash):
//   - empty string
//   - no whitespace at all (single token; covers snake_case,
//     dotted.path.identifier, kebab-case, UUID, sha256:hex, regex
//     literals, file paths, version tags, urls)
//
// Multi-token values always go to Flash even if they look identifier-
// adjacent — "list of identifiers separated by commas" is a judgment
// call Flash makes better than a regex would.
func isIdentifierShape(value string) bool {
	s := strings.TrimSpace(value)
	if s == "" {
		return true
	}
	if !strings.ContainsAny(s, " \t\n\r") {
		return true
	}
	return false
}

// r6PromptTemplateCache is intentionally absent. The prompt file is
// re-read from disk on EVERY R6 evaluation per Vision Principle 7
// (binary_is_never_sot): editing the .txt file MUST change behavior on
// the next call without a binary restart.
//
// Cost trade: at ~30 R6 candidates per concept domain and ~3 KB per
// prompt template, the re-read cost is sub-millisecond and dwarfed by
// the Flash call's network latency. Caching is an optimization the
// architecture explicitly forbids in exchange for editability.

// renderR6Prompt loads the canonical prompt body from disk fresh and
// appends the per-call VALUE + CONTEXT section. The .txt file carries
// ONLY the rule body and the response-format spec — no value markers.
// Both this in-process detector AND the Spanner-side audit recipe at
// bootstrap/audit/r6.sql cite that one file (VOLT §s1_2 reuse_by_reference):
// the audit recipe substitutes the file contents into a placeholder at
// invocation time; this detector appends the value section in Go. A
// prompt edit propagates to both surfaces with no second edit.
func renderR6Prompt(path, value, fieldPath, canonicalName, nodeKind string) (string, error) {
	raw, err := readFileFunc(path)
	if err != nil {
		return "", fmt.Errorf("read prompt %q: %w", path, err)
	}
	var buf bytes.Buffer
	buf.Write(raw)
	if !bytes.HasSuffix(raw, []byte("\n")) {
		buf.WriteByte('\n')
	}
	buf.WriteString("\nVALUE UNDER AUDIT (delimited by <<<>>>):\n<<<")
	buf.WriteString(value)
	buf.WriteString(">>>\n\nCONTEXT (informational only — do NOT include in your verdict logic):\n  field_path:     ")
	buf.WriteString(fieldPath)
	buf.WriteString("\n  canonical_name: ")
	buf.WriteString(canonicalName)
	buf.WriteString("\n  node_kind:      ")
	buf.WriteString(nodeKind)
	buf.WriteByte('\n')
	return buf.String(), nil
}

// readFileFunc is the os.ReadFile indirection tests override to supply
// in-memory prompt content without touching disk.
var readFileFunc = defaultReadFile

func defaultReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// r6VerdictLine + r6ReasonLine are case-insensitive prefix matchers
// for the two-line response shape.
var (
	r6VerdictLine = regexp.MustCompile(`(?i)^\s*VERDICT\s*:\s*(.*)$`)
	r6ReasonLine  = regexp.MustCompile(`(?i)^\s*REASON\s*:\s*(.*)$`)
)

// ParseR6Response parses the Flash response. Expected shape:
//
//	VERDICT: clean | prose
//	REASON: <one short sentence>
//
// Whitespace and case tolerant on both the labels and on the verdict
// token itself. Any other token in VERDICT is a parse error.
func ParseR6Response(raw string) (verdict, reason string, err error) {
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if m := r6VerdictLine.FindStringSubmatch(line); m != nil {
			v := strings.ToLower(strings.TrimSpace(m[1]))
			switch v {
			case "clean", "prose":
				verdict = v
			case "":
				return "", "", fmt.Errorf("VERDICT line empty")
			default:
				return "", "", fmt.Errorf("verdict token %q not in {clean,prose}", v)
			}
			continue
		}
		if m := r6ReasonLine.FindStringSubmatch(line); m != nil {
			reason = strings.TrimSpace(m[1])
			continue
		}
	}
	if verdict == "" {
		return "", "", fmt.Errorf("no VERDICT line in response")
	}
	return verdict, reason, nil
}

// truncateForDetail keeps the Finding.Detail string under control when
// raw Flash output is dumped for diagnostic context.
func truncateForDetail(s string) string {
	const max = 280
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
