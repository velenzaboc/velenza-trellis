package main

import (
	"context"
	"encoding/json"
	"fmt"
)

// EdgeLabelRegistry is the closed-set authority for edge_label values. The
// registry loads from spec-graph.config.json's edge_labels section today;
// from KernelConfig once the corpus is queryable as graph rows.
type EdgeLabelRegistry interface {
	Resolve(ctx context.Context, label string) (EdgeLabelDescriptor, error)
	All(ctx context.Context) ([]EdgeLabelDescriptor, error)
}

// EdgeLabelDescriptor wraps the raw config map for one edge_label entry.
//
// Same binary_is_never_sot design as NodeKindDescriptor: the binary holds
// field ACCESSORS for the mechanical fields it consumes at admission and
// validation (Name, SourceKinds, TargetKinds, ReciprocalLabel, EdgeFamily,
// VersionPinned, ForbidSelfLoops, DiagnosticOnly) but the underlying map
// carries the protocol's full per-label metadata (direction,
// authored_or_derived, transitive_closure_meaning, consumed_by, etc.) for
// MCP read verbs and downstream agents.
type EdgeLabelDescriptor struct {
	raw map[string]interface{}
}

// NewEdgeLabelDescriptor constructs a descriptor from a raw map.
func NewEdgeLabelDescriptor(raw map[string]interface{}) EdgeLabelDescriptor {
	if raw == nil {
		raw = map[string]interface{}{}
	}
	return EdgeLabelDescriptor{raw: raw}
}

// UnmarshalJSON loads the descriptor verbatim from JSON.
func (d *EdgeLabelDescriptor) UnmarshalJSON(b []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	d.raw = m
	return nil
}

// MarshalJSON renders the descriptor back to JSON verbatim.
func (d EdgeLabelDescriptor) MarshalJSON() ([]byte, error) {
	if d.raw == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(d.raw)
}

// Raw returns the full underlying config map.
func (d EdgeLabelDescriptor) Raw() map[string]interface{} {
	if d.raw == nil {
		return map[string]interface{}{}
	}
	return d.raw
}

// Name is the canonical edge label (required).
func (d EdgeLabelDescriptor) Name() string { return d.optionalString("name") }

// SourceKinds names the legal source endpoint classes. An empty slice means
// "any class admitted" (per the existing protocol convention — strict
// per-label endpoint constraints are enforced by §14 conformance contract,
// not by the binary).
func (d EdgeLabelDescriptor) SourceKinds() []string { return d.optionalStrings("source_kinds") }

// TargetKinds names the legal target endpoint classes.
func (d EdgeLabelDescriptor) TargetKinds() []string { return d.optionalStrings("target_kinds") }

// ReciprocalLabel names the inverse label (if the protocol declares
// required reciprocity). Edge creation auto-creates the mirror edge in the
// same Spanner ReadWriteTransaction when this is set.
func (d EdgeLabelDescriptor) ReciprocalLabel() string { return d.optionalString("reciprocal_label") }

// EdgeFamily groups labels for closure_profile-driven traversal (required).
// Per §4.edge_label_taxonomy.labels.*.family.
func (d EdgeLabelDescriptor) EdgeFamily() string { return d.optionalString("edge_family") }

// DiagnosticOnly flags edges that record materialization-time observations
// but are excluded from authoritative closure.
func (d EdgeLabelDescriptor) DiagnosticOnly() bool { return d.optionalBool("diagnostic_only") }

// VersionPinned signals that the edge requires explicit source_version and
// target_version coordinates (e.g. supersedes).
func (d EdgeLabelDescriptor) VersionPinned() bool { return d.optionalBool("version_pinned") }

// ForbidSelfLoops refuses any edge whose source_id equals its target_id.
// Per-label policy declared in config; no binary default. Per §4.self_edges,
// the dependency/version/implementation/raise/invariant/assumption families
// all forbid self-loops; declares/contains/references/interop_reference
// permit them.
func (d EdgeLabelDescriptor) ForbidSelfLoops() bool { return d.optionalBool("forbid_self_loops") }

// Validate refuses descriptors missing required mechanical fields or
// carrying type errors.
func (d EdgeLabelDescriptor) Validate(index int) error {
	if _, ok := d.raw["name"].(string); !ok || d.Name() == "" {
		return fmt.Errorf("edge_labels[%d]: name required and must be a non-empty string", index)
	}
	if _, ok := d.raw["edge_family"].(string); !ok || d.EdgeFamily() == "" {
		return fmt.Errorf("edge_labels[%d] (name=%q): edge_family required and must be a non-empty string (per OOD_GRAPH_PROTOCOL_v1 §4.edge_label_taxonomy)", index, d.Name())
	}
	// source_kinds and target_kinds are optional (empty = any), but if
	// provided must be a JSON array — not a string or other type.
	if v, ok := d.raw["source_kinds"]; ok && v != nil {
		if _, isArr := v.([]interface{}); !isArr {
			return fmt.Errorf("edge_labels[%d] (name=%q): source_kinds must be a JSON array when set", index, d.Name())
		}
	}
	if v, ok := d.raw["target_kinds"]; ok && v != nil {
		if _, isArr := v.([]interface{}); !isArr {
			return fmt.Errorf("edge_labels[%d] (name=%q): target_kinds must be a JSON array when set", index, d.Name())
		}
	}
	return nil
}

// optionalString returns the string at key, or "".
func (d EdgeLabelDescriptor) optionalString(key string) string {
	if d.raw == nil {
		return ""
	}
	s, _ := d.raw[key].(string)
	return s
}

// optionalStrings returns the string slice at key.
func (d EdgeLabelDescriptor) optionalStrings(key string) []string {
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

// optionalBool returns the bool at key, or false.
func (d EdgeLabelDescriptor) optionalBool(key string) bool {
	if d.raw == nil {
		return false
	}
	b, _ := d.raw[key].(bool)
	return b
}

// fileEdgeRegistry holds the labels loaded from config.
type fileEdgeRegistry struct {
	byName map[string]EdgeLabelDescriptor
}

func newEdgeRegistry(labels []EdgeLabelDescriptor) EdgeLabelRegistry {
	r := &fileEdgeRegistry{byName: make(map[string]EdgeLabelDescriptor, len(labels))}
	for _, l := range labels {
		r.byName[l.Name()] = l
	}
	return r
}

func (r *fileEdgeRegistry) Resolve(_ context.Context, label string) (EdgeLabelDescriptor, error) {
	d, ok := r.byName[label]
	if !ok {
		return EdgeLabelDescriptor{}, fmt.Errorf("%w: %q (add to spec-graph.config.json edge_labels)", ErrUnknownEdgeLabel, label)
	}
	return d, nil
}

func (r *fileEdgeRegistry) All(_ context.Context) ([]EdgeLabelDescriptor, error) {
	out := make([]EdgeLabelDescriptor, 0, len(r.byName))
	for _, d := range r.byName {
		out = append(out, d)
	}
	return out, nil
}

// validateEdgeEndpoints refuses an edge admission whose resolved endpoints
// don't satisfy the edge_label descriptor's structural constraints.
//
// Three checks:
//
//  1. src.NodeKind must be in desc.SourceKinds() (when SourceKinds is non-empty).
//  2. tgt.NodeKind must be in desc.TargetKinds() (when TargetKinds is non-empty).
//  3. When desc.ForbidSelfLoops() is true, src.NodeID must differ from tgt.NodeID.
//
// Empty SourceKinds / TargetKinds means "any class admitted" — the config
// row declared no restriction for that side. ForbidSelfLoops defaults to
// false; only labels that opt in via config will refuse self-loops.
func validateEdgeEndpoints(desc EdgeLabelDescriptor, src, tgt Node) error {
	if srcKinds := desc.SourceKinds(); len(srcKinds) > 0 && !containsString(srcKinds, src.NodeKind) {
		return fmt.Errorf("%w: edge_label=%q source_id=%q has node_kind=%q, allowed source_kinds=%v",
			ErrEdgeKindMismatch, desc.Name(), src.NodeID, src.NodeKind, srcKinds)
	}
	if tgtKinds := desc.TargetKinds(); len(tgtKinds) > 0 && !containsString(tgtKinds, tgt.NodeKind) {
		return fmt.Errorf("%w: edge_label=%q target_id=%q has node_kind=%q, allowed target_kinds=%v",
			ErrEdgeKindMismatch, desc.Name(), tgt.NodeID, tgt.NodeKind, tgtKinds)
	}
	if desc.ForbidSelfLoops() && src.NodeID == tgt.NodeID {
		return fmt.Errorf("%w: edge_label=%q source_id=target_id=%q",
			ErrEdgeSelfLoopForbidden, desc.Name(), src.NodeID)
	}
	return nil
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
