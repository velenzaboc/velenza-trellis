package main

import (
	"context"
	"encoding/json"
	"fmt"
)

// NodeKindRegistry is the closed-set authority for node_kind values. The
// registry loads from spec-graph.config.json's node_kinds section today;
// from KernelConfig once the corpus is queryable as graph rows. The binary
// holds no node_kind enum.
type NodeKindRegistry interface {
	// Resolve returns the kind descriptor or ErrUnknownNodeKind. Used at
	// admission to route to the owning substrate and to look up applicable
	// compiler passes.
	Resolve(ctx context.Context, kind string) (NodeKindDescriptor, error)

	// All returns every admissible node_kind. Used by spec_graph_list when no
	// kind filter is supplied.
	All(ctx context.Context) ([]NodeKindDescriptor, error)
}

// NodeKindDescriptor wraps the raw config map for one node_kind entry.
//
// Design intent (binary_is_never_sot, Arbor Grand Vision §2): the binary
// holds field ACCESSORS for the mechanical fields it consumes at runtime
// (Name, SubstrateID, ArtifactType, VersionScope) but stores the raw map
// verbatim so the protocol's per-class metadata (class_id, projection_role,
// source_authority, sibling_owner, what_it_represents,
// graph_owned_fields_boundary, stable_reference_target,
// non_ownership_boundaries, and any future protocol fields) flows through
// to MCP read verbs and downstream authoring/validating agents intact.
//
// Adding new protocol metadata to the registry is a config edit; the binary
// needs no recompile. Removing or renaming a mechanical field still requires
// a binary change because the accessor goes away or changes contract — that
// is the small surface the binary legitimately holds.
type NodeKindDescriptor struct {
	raw map[string]interface{}
}

// NewNodeKindDescriptor constructs a descriptor from a raw map. Used by
// tests and by any caller that wants to build a descriptor programmatically.
// Callers should invoke Validate before use to confirm required fields exist
// with correct types; config-loaded descriptors are validated by
// SpecGraphConfig.Validate.
func NewNodeKindDescriptor(raw map[string]interface{}) NodeKindDescriptor {
	if raw == nil {
		raw = map[string]interface{}{}
	}
	return NodeKindDescriptor{raw: raw}
}

// UnmarshalJSON loads the descriptor verbatim from JSON. Every key in the
// source object becomes a key in the underlying map; no field is filtered or
// renamed.
func (d *NodeKindDescriptor) UnmarshalJSON(b []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	d.raw = m
	return nil
}

// MarshalJSON renders the descriptor back to JSON verbatim — every protocol
// field round-trips through MCP read verbs and projection artifacts.
func (d NodeKindDescriptor) MarshalJSON() ([]byte, error) {
	if d.raw == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(d.raw)
}

// Raw returns the full underlying config map. MCP read verbs use this to
// expose the complete protocol metadata. The returned map is the live backing
// store; callers should treat it as read-only.
func (d NodeKindDescriptor) Raw() map[string]interface{} {
	if d.raw == nil {
		return map[string]interface{}{}
	}
	return d.raw
}

// Name is the canonical node_kind name (required).
func (d NodeKindDescriptor) Name() string { return d.optionalString("name") }

// SubstrateID names the substrate that owns this kind's rows (required for
// admission routing).
func (d NodeKindDescriptor) SubstrateID() string { return d.optionalString("substrate_id") }

// ArtifactType tags this kind's admission rows (required).
func (d NodeKindDescriptor) ArtifactType() string { return d.optionalString("artifact_type") }

// VersionScope describes the version model for this kind. Per
// §2.class_record_required_fields. Examples: "stable_id_preserved",
// "version_row_scope", "owner_spec_version_scope".
func (d NodeKindDescriptor) VersionScope() string { return d.optionalString("version_scope") }

// Projections lists projection_role values the projection layer recognizes
// for this kind. Optional.
func (d NodeKindDescriptor) Projections() []string { return d.optionalStrings("projections") }

// PreSignPasses / PostSignPasses are compiler-pass-registry references.
// Optional.
func (d NodeKindDescriptor) PreSignPasses() []string  { return d.optionalStrings("pre_sign_passes") }
func (d NodeKindDescriptor) PostSignPasses() []string { return d.optionalStrings("post_sign_passes") }

// DualControlRequired flags kinds whose admission requires two-principal
// approval. Optional, defaults to false.
func (d NodeKindDescriptor) DualControlRequired() bool {
	return d.optionalBool("dual_control_required")
}

// Validate refuses descriptors missing required mechanical fields or
// carrying type errors on those fields. Optional protocol-metadata fields
// are NOT validated by the binary — that is the registry's job and the
// validator agent's job.
func (d NodeKindDescriptor) Validate(index int) error {
	if _, ok := d.raw["name"].(string); !ok || d.Name() == "" {
		return fmt.Errorf("node_kinds[%d]: name required and must be a non-empty string", index)
	}
	if _, ok := d.raw["substrate_id"].(string); !ok || d.SubstrateID() == "" {
		return fmt.Errorf("node_kinds[%d] (name=%q): substrate_id required and must be a non-empty string", index, d.Name())
	}
	if _, ok := d.raw["artifact_type"].(string); !ok || d.ArtifactType() == "" {
		return fmt.Errorf("node_kinds[%d] (name=%q): artifact_type required and must be a non-empty string", index, d.Name())
	}
	if _, ok := d.raw["version_scope"].(string); !ok || d.VersionScope() == "" {
		return fmt.Errorf("node_kinds[%d] (name=%q): version_scope required and must be a non-empty string (per OOD_GRAPH_PROTOCOL_v1 §2.class_record_required_fields)", index, d.Name())
	}
	return nil
}

// optionalString returns the string at key, or "" if missing or wrong-typed.
// Validate enforces required-field presence separately.
func (d NodeKindDescriptor) optionalString(key string) string {
	if d.raw == nil {
		return ""
	}
	s, _ := d.raw[key].(string)
	return s
}

// optionalStrings returns the string slice at key, or nil if missing or
// wrong-typed. Non-string array elements are skipped.
func (d NodeKindDescriptor) optionalStrings(key string) []string {
	if d.raw == nil {
		return nil
	}
	v, ok := d.raw[key]
	if !ok || v == nil {
		return nil
	}
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// optionalBool returns the bool at key, or false if missing or wrong-typed.
func (d NodeKindDescriptor) optionalBool(key string) bool {
	if d.raw == nil {
		return false
	}
	b, _ := d.raw[key].(bool)
	return b
}

// fileKindRegistry holds the kinds loaded from config. Empty registry is
// valid; verbs fail-closed with a clear message.
type fileKindRegistry struct {
	byName map[string]NodeKindDescriptor
}

func newKindRegistry(kinds []NodeKindDescriptor) NodeKindRegistry {
	r := &fileKindRegistry{byName: make(map[string]NodeKindDescriptor, len(kinds))}
	for _, k := range kinds {
		r.byName[k.Name()] = k
	}
	return r
}

func (r *fileKindRegistry) Resolve(_ context.Context, kind string) (NodeKindDescriptor, error) {
	d, ok := r.byName[kind]
	if !ok {
		return NodeKindDescriptor{}, fmt.Errorf("%w: %q (add to spec-graph.config.json node_kinds, or set SPEC_GRAPH_CONFIG)", ErrUnknownNodeKind, kind)
	}
	return d, nil
}

func (r *fileKindRegistry) All(_ context.Context) ([]NodeKindDescriptor, error) {
	out := make([]NodeKindDescriptor, 0, len(r.byName))
	for _, d := range r.byName {
		out = append(out, d)
	}
	return out, nil
}
