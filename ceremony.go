package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
)

// AdmitArtifact runs the 10-step admission ceremony required by
// REQUIREMENTS.md §3.1.
//
// Steps 1-6 run in-process here. Steps 7-10 are handed to the substrate
// adapter's CommitAdmission, which opens a Spanner ReadWriteTransaction and
// commits atomically (single-admission-path per REQUIREMENTS.md §3.6).
//
// In experimental_bootstrap mode (REQUIREMENTS.md §3.1), several steps run
// with relaxed inputs:
//   - admitted_by: burned-in literal ExperimentalAdmitterSentinel from config
//   - signature: SHA-256 placeholder over the canonical signature input vector
//     (not a Cloud KMS sign — that comes at production cutover)
//   - pre/post sign passes: tolerate an empty pass_registry
//
// definition_hash and manifest_hash are computed for real (SHA-256 over
// canonical JSON) in both modes.
func AdmitArtifact(ctx context.Context, env *Env, sub ArtifactSubmission) (AdmissionRecord, error) {
	if env == nil {
		return AdmissionRecord{}, fmt.Errorf("nil env")
	}

	runID := uuid.NewString()
	rec := AdmissionRecord{
		RunID:          runID,
		OrganizationID: sub.OrganizationID,
	}

	// Step 1: schema_validate
	if err := stepSchemaValidate(ctx, env, sub); err != nil {
		return rec, fmt.Errorf("step 1 schema_validate: %w", err)
	}

	// Step 2: resolve_admitter
	admitter, err := stepResolveAdmitter(ctx, env, sub)
	if err != nil {
		return rec, fmt.Errorf("step 2 resolve_admitter: %w", err)
	}
	rec.AdmittedBy = admitter

	// Step 3: pre_sign_passes
	preReport, err := stepRunPasses(ctx, env, sub, "pre_sign")
	if err != nil {
		return rec, fmt.Errorf("step 3 pre_sign_passes: %w", err)
	}
	rec.PassReport = append(rec.PassReport, preReport...)

	// Step 4: definition_hash
	defHash, err := stepDefinitionHash(ctx, env, sub)
	if err != nil {
		return rec, fmt.Errorf("step 4 definition_hash: %w", err)
	}
	rec.DefinitionHash = defHash

	// Step 5: manifest_hash
	manHash, err := stepManifestHash(ctx, env, rec)
	if err != nil {
		return rec, fmt.Errorf("step 5 manifest_hash: %w", err)
	}
	rec.ManifestHash = manHash

	// Step 6: kms_sign — placeholder in bootstrap mode
	rec.AdmittedAt = env.Now().UTC()
	signature, err := stepSign(ctx, env, sub, &rec)
	if err != nil {
		return rec, fmt.Errorf("step 6 kms_sign: %w", err)
	}
	rec.SignatureChain = []string{signature}

	// Step 8: post_sign_passes (step 7 happens inside CommitAdmission)
	postReport, err := stepRunPasses(ctx, env, sub, "post_sign")
	if err != nil {
		return rec, fmt.Errorf("step 8 post_sign_passes: %w", err)
	}
	rec.PassReport = append(rec.PassReport, postReport...)

	// Determine artifact identity
	if sub.PriorVersion != nil {
		rec.ArtifactVersion = sub.PriorVersion.Version + 1
	} else {
		rec.ArtifactVersion = 1
	}
	rec.ArtifactType = sub.ArtifactType
	if sub.StableID == "" {
		sub.StableID = uuid.NewString()
	}

	// is_non_genesis: TRUE iff admitted by a real principal. FALSE when the
	// admitter matches either the configured genesis_admitter (production
	// boot-time path) or the configured experimental_admitter (bootstrap
	// path). Both literals live in config; the binary holds neither.
	// REQUIREMENTS.md §3.2 + §3.8.
	rec.IsNonGenesis = true
	if env.Config.GenesisAdmitter != "" && rec.AdmittedBy == env.Config.GenesisAdmitter {
		rec.IsNonGenesis = false
	}
	if env.Config.ExperimentalAdmitter != "" && rec.AdmittedBy == env.Config.ExperimentalAdmitter {
		rec.IsNonGenesis = false
	}
	rec.ArtifactID = sub.StableID

	// Resolve substrate
	var adapter SubstrateAdapter
	if sub.NodeKind != "" {
		desc, err := env.Kinds.Resolve(ctx, sub.NodeKind)
		if err != nil {
			return rec, err
		}
		adapter, err = env.Substrates.Adapter(ctx, desc)
		if err != nil {
			return rec, err
		}
	} else if sub.EdgeLabel != "" {
		// Edges route via the source endpoint's substrate. For bootstrap (one
		// substrate), All() returns that one adapter.
		all, err := env.Substrates.All(ctx)
		if err != nil {
			return rec, err
		}
		if len(all) == 0 {
			return rec, fmt.Errorf("no substrates registered; add to spec-graph.config.json")
		}
		adapter = all[0]
	} else {
		return rec, fmt.Errorf("submission must have either node_kind or edge_label")
	}

	// Assemble the AdmissionPlan
	plan := AdmissionPlan{
		OrganizationID: sub.OrganizationID,
		Record:         rec,
		PriorVersion:   sub.PriorVersion,
		IsRetirement:   sub.IsRetirement,
	}
	if sub.EdgeLabel != "" {
		plan.IsEdge = true
		plan.Edge = &Edge{
			EdgeID:        sub.StableID,
			SourceID:      sub.EdgeSourceID,
			SourceVersion: sub.EdgeSourceVersion,
			TargetID:      sub.EdgeTargetID,
			TargetVersion: sub.EdgeTargetVersion,
			EdgeLabel:     sub.EdgeLabel,
			Status:        "active",
		}
		if sub.IsRetirement {
			plan.Edge.Status = "retired"
		}
	} else {
		plan.Node = &NodeVersion{
			NodeID:                  sub.StableID,
			Version:                 rec.ArtifactVersion,
			Status:                  "active",
			Contract:                sub.Contract,
			Payload:                 sub.Payload,
			DefinitionHash:          defHash,
			ManifestHash:            manHash,
			AdmittedAt:              rec.AdmittedAt,
			AdmittedBy:              admitter,
			AdmissionRunID:          runID,
			GoverningSpecRef:        sub.GoverningSpec,
			SpecSectionRef:          sub.SpecSection,
			NodeCreationContractRef: "",
		}
		if sub.IsRetirement {
			plan.Node.Status = "retired"
		}
		// Embed at admission time if configured. Retirement skips the embed
		// (nothing semantic to capture; the row's content is unchanged).
		if env.Embed != nil && !sub.IsRetirement {
			text, err := canonicalEmbedText(sub)
			if err != nil {
				return rec, fmt.Errorf("canonical embed text: %w", err)
			}
			vec, err := env.Embed.Embed(ctx, text, vertexai.TaskTypeRetrievalDocument)
			if err != nil {
				return rec, fmt.Errorf("embed node: %w", err)
			}
			plan.Node.EmbeddingV2 = vec
		}
	}

	// Steps 7, 9, 10: CommitAdmission (single plan)
	finals, err := adapter.CommitAdmission(ctx, plan)
	if err != nil {
		return rec, err
	}
	if len(finals) == 0 {
		return rec, fmt.Errorf("CommitAdmission returned no records")
	}
	return finals[0], nil
}

// PrepareAdmissionPlan runs steps 1-8 of the ceremony in memory and returns
// the resulting plan WITHOUT committing. Callers that need to admit multiple
// artifacts atomically (e.g. reciprocal edge pairs) use this to prepare each
// plan, then pass the slice to adapter.CommitAdmission for one-tx commit.
//
// The returned AdmissionPlan carries the resolved AdmissionRecord (with
// definition_hash, manifest_hash, signature placeholder, pass_report) and
// the assembled Node or Edge. Callers may mutate Edge.ReciprocalEdgeID
// (the only field reciprocity cross-references) before committing.
func PrepareAdmissionPlan(ctx context.Context, env *Env, sub ArtifactSubmission) (AdmissionPlan, error) {
	if env == nil {
		return AdmissionPlan{}, fmt.Errorf("nil env")
	}
	runID := uuid.NewString()
	rec := AdmissionRecord{
		RunID:          runID,
		OrganizationID: sub.OrganizationID,
	}

	if err := stepSchemaValidate(ctx, env, sub); err != nil {
		return AdmissionPlan{}, fmt.Errorf("step 1 schema_validate: %w", err)
	}
	admitter, err := stepResolveAdmitter(ctx, env, sub)
	if err != nil {
		return AdmissionPlan{}, fmt.Errorf("step 2 resolve_admitter: %w", err)
	}
	rec.AdmittedBy = admitter

	preReport, err := stepRunPasses(ctx, env, sub, "pre_sign")
	if err != nil {
		return AdmissionPlan{}, fmt.Errorf("step 3 pre_sign_passes: %w", err)
	}
	rec.PassReport = append(rec.PassReport, preReport...)

	defHash, err := stepDefinitionHash(ctx, env, sub)
	if err != nil {
		return AdmissionPlan{}, fmt.Errorf("step 4 definition_hash: %w", err)
	}
	rec.DefinitionHash = defHash

	manHash, err := stepManifestHash(ctx, env, rec)
	if err != nil {
		return AdmissionPlan{}, fmt.Errorf("step 5 manifest_hash: %w", err)
	}
	rec.ManifestHash = manHash

	rec.AdmittedAt = env.Now().UTC()
	signature, err := stepSign(ctx, env, sub, &rec)
	if err != nil {
		return AdmissionPlan{}, fmt.Errorf("step 6 kms_sign: %w", err)
	}
	rec.SignatureChain = []string{signature}

	postReport, err := stepRunPasses(ctx, env, sub, "post_sign")
	if err != nil {
		return AdmissionPlan{}, fmt.Errorf("step 8 post_sign_passes: %w", err)
	}
	rec.PassReport = append(rec.PassReport, postReport...)

	if sub.PriorVersion != nil {
		rec.ArtifactVersion = sub.PriorVersion.Version + 1
	} else {
		rec.ArtifactVersion = 1
	}
	rec.ArtifactType = sub.ArtifactType
	if sub.StableID == "" {
		sub.StableID = uuid.NewString()
	}
	rec.ArtifactID = sub.StableID

	rec.IsNonGenesis = true
	if env.Config.GenesisAdmitter != "" && rec.AdmittedBy == env.Config.GenesisAdmitter {
		rec.IsNonGenesis = false
	}
	if env.Config.ExperimentalAdmitter != "" && rec.AdmittedBy == env.Config.ExperimentalAdmitter {
		rec.IsNonGenesis = false
	}

	plan := AdmissionPlan{
		OrganizationID: sub.OrganizationID,
		Record:         rec,
		PriorVersion:   sub.PriorVersion,
		IsRetirement:   sub.IsRetirement,
	}
	if sub.EdgeLabel != "" {
		plan.IsEdge = true
		plan.Edge = &Edge{
			EdgeID:        sub.StableID,
			SourceID:      sub.EdgeSourceID,
			SourceVersion: sub.EdgeSourceVersion,
			TargetID:      sub.EdgeTargetID,
			TargetVersion: sub.EdgeTargetVersion,
			EdgeLabel:     sub.EdgeLabel,
			Status:        "active",
		}
		if sub.IsRetirement {
			plan.Edge.Status = "retired"
		}
	} else {
		plan.Node = &NodeVersion{
			NodeID:                  sub.StableID,
			Version:                 rec.ArtifactVersion,
			Status:                  "active",
			Contract:                sub.Contract,
			Payload:                 sub.Payload,
			DefinitionHash:          defHash,
			ManifestHash:            manHash,
			AdmittedAt:              rec.AdmittedAt,
			AdmittedBy:              admitter,
			AdmissionRunID:          runID,
			GoverningSpecRef:        sub.GoverningSpec,
			SpecSectionRef:          sub.SpecSection,
			NodeCreationContractRef: "",
		}
		if sub.IsRetirement {
			plan.Node.Status = "retired"
		}
		if env.Embed != nil && !sub.IsRetirement {
			text, err := canonicalEmbedText(sub)
			if err != nil {
				return AdmissionPlan{}, fmt.Errorf("canonical embed text: %w", err)
			}
			vec, err := env.Embed.Embed(ctx, text, vertexai.TaskTypeRetrievalDocument)
			if err != nil {
				return AdmissionPlan{}, fmt.Errorf("embed node: %w", err)
			}
			plan.Node.EmbeddingV2 = vec
		}
	}
	return plan, nil
}

// ---------------- step implementations ----------------

func stepSchemaValidate(_ context.Context, env *Env, sub ArtifactSubmission) error {
	// Experimental bootstrap mode: minimal structural validation (no
	// per-artifact_type JSON Schema registry yet).
	if env.Config.Mode == ModeExperimentalBootstrap {
		if sub.NodeKind == "" && sub.EdgeLabel == "" {
			return fmt.Errorf("submission has neither node_kind nor edge_label")
		}
		return nil
	}
	return SchemaPendingDetail{
		Step:      "schema_validate",
		Needs:     "schema_registry table + per-artifact_type JSON schema rows",
		OwnerSpec: "Node Identity & Admission spec (per REQUIREMENTS.md §3.1 ceremony step 1)",
	}
}

func stepResolveAdmitter(_ context.Context, env *Env, sub ArtifactSubmission) (string, error) {
	if env.Config.Mode == ModeExperimentalBootstrap && env.Config.ExperimentalAdmitter != "" {
		return env.Config.ExperimentalAdmitter, nil
	}
	if sub.Submitter != "" {
		return sub.Submitter, nil
	}
	return "", SchemaPendingDetail{
		Step:      "resolve_admitter",
		Needs:     "CallerContext propagation — or set experimental_admitter for bootstrap mode (REQUIREMENTS.md §3.8)",
		OwnerSpec: "Node Identity & Admission spec",
	}
}

func stepRunPasses(_ context.Context, env *Env, _ ArtifactSubmission, phase string) ([]PassResult, error) {
	// In experimental bootstrap mode: empty pass_registry is OK; we just emit a
	// marker that says the phase ran with zero registered passes.
	if env.Config.Mode == ModeExperimentalBootstrap {
		return []PassResult{{
			PassName: fmt.Sprintf("%s_phase_empty_registry_bootstrap", phase),
			Outcome:  "skipped",
			Evidence: "compiler_pass_registry empty in experimental_bootstrap mode",
		}}, nil
	}
	return nil, SchemaPendingDetail{
		Step:      fmt.Sprintf("%s_passes", phase),
		Needs:     "compiler_pass_registry KernelConfig row + per-pass implementations",
		OwnerSpec: "compiler_pass_registry config (KernelConfig)",
	}
}

func stepDefinitionHash(_ context.Context, _ *Env, sub ArtifactSubmission) (string, error) {
	return definitionHashOf(sub)
}

// definitionHashOf is the ONE derivation of a submission's definition_hash.
//
// It is a package-level function rather than ceremony-step-shaped because it
// has two callers with different shapes: the ceremony step above, and the
// dedup gate, which runs AHEAD of the ceremony so Layer 1 can short-circuit on
// an exact definition_hash match. Both must produce the identical digest for
// that short-circuit to be sound — the gate's whole premise is that the
// ceremony's later recompute will agree with it.
//
// It exists BECAUSE that premise used to rest on nothing. The dedup gate
// carried its own copy of this field map (computeDefinitionHashForDedup,
// dedup.go), hand-declared to match. The two agreed only by coincidence of
// having been written together: nothing bound the field lists, so adding one
// identity-bearing field to the ceremony's map and not the gate's would have
// silently broken the short-circuit — the gate would miss an exact duplicate
// and the ceremony would then admit a second version of an artifact already
// present. A field list that must not drift belongs in exactly one place.
func definitionHashOf(sub ArtifactSubmission) (string, error) {
	// Canonical JSON over the submission's identity-bearing fields.
	canon := map[string]interface{}{
		"artifact_type":      sub.ArtifactType,
		"organization_id":    sub.OrganizationID,
		"node_kind":          sub.NodeKind,
		"edge_label":         sub.EdgeLabel,
		"stable_id":          sub.StableID,
		"contract":           sub.Contract,
		"payload":            sub.Payload,
		"governing_spec_ref": sub.GoverningSpec,
		"spec_section_ref":   sub.SpecSection,
	}
	b, err := canonicalJSON(canon)
	if err != nil {
		return "", fmt.Errorf("canonical JSON: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func stepManifestHash(_ context.Context, _ *Env, rec AdmissionRecord) (string, error) {
	canon := map[string]interface{}{
		"pass_report":     rec.PassReport,
		"definition_hash": rec.DefinitionHash,
	}
	b, err := canonicalJSON(canon)
	if err != nil {
		return "", fmt.Errorf("canonical JSON: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func stepSign(_ context.Context, env *Env, sub ArtifactSubmission, rec *AdmissionRecord) (string, error) {
	if env.Config.Mode == ModeExperimentalBootstrap {
		// Placeholder signature: SHA-256 over the canonical signature input
		// vector (six fields per REQUIREMENTS.md §2). The placeholder constant
		// option (c) per REQUIREMENTS.md §10.2 — a stable derivation, not a
		// real KMS-issued signature. Cutover replaces this with a Cloud KMS
		// asymmetric sign.
		canon := map[string]interface{}{
			"artifact_type":   sub.ArtifactType,
			"organization_id": sub.OrganizationID,
			"artifact_id":     sub.StableID,
			"admitted_at":     rec.AdmittedAt.Format(time.RFC3339Nano),
			"definition_hash": rec.DefinitionHash,
			"manifest_hash":   rec.ManifestHash,
		}
		b, err := canonicalJSON(canon)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(b)
		return "EXPERIMENTAL_BOOTSTRAP:" + hex.EncodeToString(sum[:]), nil
	}
	return "", SchemaPendingDetail{
		Step:      "kms_sign",
		Needs:     "KMS key resource id + per-artifact_type key restriction",
		OwnerSpec: "Node Identity & Admission spec",
	}
}

// ---------------- helpers ----------------

// canonicalJSON marshals v with sorted map keys at every level so the output
// is byte-deterministic.
func canonicalJSON(v interface{}) ([]byte, error) {
	// Round-trip through generic to sort keys deterministically. UseNumber is
	// essential here: the generic interface path must not coerce INT64 values
	// through float64, which cannot represent integers above 2^53 exactly.
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var generic interface{}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	return marshalCanonical(generic)
}

func marshalCanonical(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var buf []byte
		buf = append(buf, '{')
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			kb, _ := json.Marshal(k)
			buf = append(buf, kb...)
			buf = append(buf, ':')
			vb, err := marshalCanonical(t[k])
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		buf = append(buf, '}')
		return buf, nil
	case []interface{}:
		var buf []byte
		buf = append(buf, '[')
		for i, e := range t {
			if i > 0 {
				buf = append(buf, ',')
			}
			eb, err := marshalCanonical(e)
			if err != nil {
				return nil, err
			}
			buf = append(buf, eb...)
		}
		buf = append(buf, ']')
		return buf, nil
	case json.Number:
		// json.Number came from encoding/json's validated decoder above. Marshal
		// once more to retain the package's validity checks before emitting the
		// exact numeric token unchanged.
		b, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		return json.Marshal(v)
	}
}
