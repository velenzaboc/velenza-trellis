package main

// Rule is the interface every R-rule satisfies. Per-submission rules
// implement Check; batch rules (R8 duplicate detection across the input
// set) implement CheckBatch and use the trivial Check pass-through.
type Rule interface {
	Name() string
	Check(sub Submission, cfg *Config) []Finding
}

// BatchRule is a Rule that also has visibility across the full
// submission set — needed by R8 for duplicate detection.
type BatchRule interface {
	Rule
	CheckBatch(subs []Submission, cfg *Config) []Finding
}

// AllRules is the registry of every implemented R-rule. main.go runs
// every rule against every submission.
func AllRules() []Rule {
	return []Rule{
		&R3KindInRegistry{},
		&R4RequiredContractFields{},
		&R5RequiredPayloadFields{},
		&R6NoProse{},
		&R7CanonicalNamePattern{},
		&R8NoDuplicate{},
		&R10EdgeEndpointKinds{},
		&R12SelfLoop{},
		&R13NoSourceReference{},
		&R15NoAdmissionArtifacts{},
	}
}

// RunRules executes every per-submission rule against every submission
// and every batch rule against the whole set. Returns the aggregated
// findings.
func RunRules(subs []Submission, cfg *Config, rules []Rule) []Finding {
	var findings []Finding
	for _, r := range rules {
		for _, s := range subs {
			findings = append(findings, r.Check(s, cfg)...)
		}
		if br, ok := r.(BatchRule); ok {
			findings = append(findings, br.CheckBatch(subs, cfg)...)
		}
	}
	return findings
}
