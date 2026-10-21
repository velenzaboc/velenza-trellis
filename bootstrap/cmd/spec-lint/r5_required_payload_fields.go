package main

import "fmt"

// R5RequiredPayloadFields validates that every field declared in the
// kind's `required_payload_fields` is present in the submission's
// payload. Edge submissions skip this rule.
type R5RequiredPayloadFields struct{}

func (r *R5RequiredPayloadFields) Name() string { return "R5_required_payload_fields" }

func (r *R5RequiredPayloadFields) Check(sub Submission, cfg *Config) []Finding {
	if cfg == nil || sub.IsEdge() {
		return nil
	}
	kd := cfg.KindByName(sub.NodeKind)
	if kd == nil {
		return nil
	}
	var findings []Finding
	for _, required := range kd.RequiredPayloadFields {
		if _, ok := sub.Payload[required]; !ok {
			findings = append(findings, Finding{
				Severity:      "fail",
				Rule:          "R5_required_payload_fields",
				SourceFile:    sub.SourceFile,
				CanonicalName: sub.CanonicalName(),
				ObjectClass:   sub.NodeKind,
				FieldPath:     "payload." + required,
				Expected:      fmt.Sprintf("required payload field %q present", required),
				Detail:        fmt.Sprintf("kind %q requires payload.%s; not found", sub.NodeKind, required),
			})
		}
	}
	return findings
}
