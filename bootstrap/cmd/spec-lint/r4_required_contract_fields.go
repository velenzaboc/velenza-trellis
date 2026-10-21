package main

import "fmt"

// R4RequiredContractFields validates that every field declared in the
// kind's `required_contract_fields` is present in the submission's
// contract. Edge submissions skip this rule (edges don't have contract).
type R4RequiredContractFields struct{}

func (r *R4RequiredContractFields) Name() string { return "R4_required_contract_fields" }

func (r *R4RequiredContractFields) Check(sub Submission, cfg *Config) []Finding {
	if cfg == nil || sub.IsEdge() {
		return nil
	}
	kd := cfg.KindByName(sub.NodeKind)
	if kd == nil {
		return nil // R3 catches missing-from-registry separately
	}
	var findings []Finding
	for _, required := range kd.RequiredContractFields {
		if _, ok := sub.Contract[required]; !ok {
			findings = append(findings, Finding{
				Severity:      "fail",
				Rule:          "R4_required_contract_fields",
				SourceFile:    sub.SourceFile,
				CanonicalName: sub.CanonicalName(),
				ObjectClass:   sub.NodeKind,
				FieldPath:     "contract." + required,
				Expected:      fmt.Sprintf("required contract field %q present", required),
				Detail:        fmt.Sprintf("kind %q requires contract.%s; not found", sub.NodeKind, required),
			})
		}
	}
	return findings
}
