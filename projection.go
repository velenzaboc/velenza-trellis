package main

import (
	"context"
	"fmt"
)

// ProjectionRegistry is the closed-set authority for task projection names.
type ProjectionRegistry interface {
	Resolve(ctx context.Context, name string) (Projection, error)
	All(ctx context.Context) ([]string, error)
}

// Projection renders a node (or a closure rooted at one) into a target
// representation. The contract is deterministic: the same graph state yields
// the same output bytes.
type Projection interface {
	Name() string
	OwningSpec() string
	Render(ctx context.Context, root Node, closure []Node) ([]byte, error)
}

// projectionRenderer is the signature each built-in projection implementation
// satisfies. Registered by name in builtinProjectionRenderers below.
type projectionRenderer func(ctx context.Context, root Node, closure []Node) ([]byte, error)

// builtinProjectionRenderers maps projection name → implementation. Adding a
// new projection means: (a) configure it in task-graph.config.json
// projections, (b) add a renderer here. Projections in config without a
// renderer fail at Render time with SchemaPendingDetail naming what's needed.
var builtinProjectionRenderers = map[string]projectionRenderer{
	"task_canonical_json": renderTaskCanonicalJSON,
}

// renderTaskCanonicalJSON projects a task node as canonical structured JSON.
// Deterministic — same node state always produces the same bytes.
func renderTaskCanonicalJSON(_ context.Context, root Node, _ []Node) ([]byte, error) {
	canon := map[string]interface{}{
		"node_id":         root.NodeID,
		"node_kind":       root.NodeKind,
		"organization_id": root.OrganizationID,
		"status":          root.Status,
		"version":         root.CurrentVersion.Version,
		"contract":        root.CurrentVersion.Contract,
		"payload":         root.CurrentVersion.Payload,
		"definition_hash": root.CurrentVersion.DefinitionHash,
		"manifest_hash":   root.CurrentVersion.ManifestHash,
		"admitted_by":     root.CurrentVersion.AdmittedBy,
		"admitted_at":     root.CurrentVersion.AdmittedAt,
	}
	return canonicalJSON(canon)
}

// fileProjection is the framework-phase Projection. Name + owning_spec come
// from config; the renderer is bound by name to builtinProjectionRenderers.
// Names without a registered renderer return SchemaPendingDetail.
type fileProjection struct {
	descriptor ProjectionDescriptor
}

func (p fileProjection) Name() string       { return p.descriptor.Name }
func (p fileProjection) OwningSpec() string { return p.descriptor.OwningSpec }
func (p fileProjection) Render(ctx context.Context, root Node, closure []Node) ([]byte, error) {
	if r, ok := builtinProjectionRenderers[p.descriptor.Name]; ok {
		return r(ctx, root, closure)
	}
	return nil, SchemaPendingDetail{
		Step:      fmt.Sprintf("render projection %q", p.descriptor.Name),
		Needs:     "projection algorithm implementation (add to builtinProjectionRenderers in projection.go)",
		OwnerSpec: p.descriptor.OwningSpec,
	}
}

// fileProjectionRegistry holds the projections loaded from config.
type fileProjectionRegistry struct {
	byName map[string]Projection
}

func newProjectionRegistry(descs []ProjectionDescriptor) ProjectionRegistry {
	r := &fileProjectionRegistry{byName: make(map[string]Projection, len(descs))}
	for _, d := range descs {
		r.byName[d.Name] = fileProjection{descriptor: d}
	}
	return r
}

func (r *fileProjectionRegistry) Resolve(_ context.Context, name string) (Projection, error) {
	p, ok := r.byName[name]
	if !ok {
		return nil, fmt.Errorf("unknown projection %q (add to task-graph.config.json projections)", name)
	}
	return p, nil
}

func (r *fileProjectionRegistry) All(_ context.Context) ([]string, error) {
	out := make([]string, 0, len(r.byName))
	for n := range r.byName {
		out = append(out, n)
	}
	return out, nil
}
