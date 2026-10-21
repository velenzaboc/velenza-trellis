package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestNodeKindDescriptorPreservesAndEnforcesConfiguredContract(t *testing.T) {
	raw := map[string]interface{}{
		"name": "task", "substrate_id": "fleet", "artifact_type": "task", "version_scope": "stable_id_preserved",
		"projections": []interface{}{"task_canonical_json", float64(9)}, "pre_sign_passes": []interface{}{"validate"}, "post_sign_passes": []interface{}{"record"}, "dual_control_required": true,
	}
	desc := NewNodeKindDescriptor(raw)
	if err := desc.Validate(0); err != nil {
		t.Fatal(err)
	}
	if got := []string{desc.Name(), desc.SubstrateID(), desc.ArtifactType(), desc.VersionScope()}; !reflect.DeepEqual(got, []string{"task", "fleet", "task", "stable_id_preserved"}) {
		t.Fatalf("required accessors = %v", got)
	}
	if !reflect.DeepEqual(desc.Projections(), []string{"task_canonical_json"}) || !reflect.DeepEqual(desc.PreSignPasses(), []string{"validate"}) || !reflect.DeepEqual(desc.PostSignPasses(), []string{"record"}) || !desc.DualControlRequired() {
		t.Fatalf("optional accessors did not preserve contract: %#v", desc.Raw())
	}
	encoded, err := json.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip NodeKindDescriptor
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || !reflect.DeepEqual(roundTrip.Raw(), raw) {
		t.Fatalf("descriptor round trip = %#v, %v", roundTrip.Raw(), err)
	}
	registry := newKindRegistry([]NodeKindDescriptor{desc})
	if _, err := registry.Resolve(context.Background(), "missing"); !errors.Is(err, ErrUnknownNodeKind) {
		t.Fatalf("missing kind error = %v", err)
	}
	if all, err := registry.All(context.Background()); err != nil || len(all) != 1 || all[0].Name() != "task" {
		t.Fatalf("All = %#v, %v", all, err)
	}
}

func TestEdgeDescriptorPreservesAndEnforcesConfiguredContract(t *testing.T) {
	raw := map[string]interface{}{
		"name": "gates", "edge_family": "dependency", "source_kinds": []interface{}{"task"}, "target_kinds": []interface{}{"task"},
		"reciprocal_label": "gated_by", "diagnostic_only": true, "version_pinned": true, "forbid_self_loops": true,
	}
	desc := NewEdgeLabelDescriptor(raw)
	if err := desc.Validate(0); err != nil {
		t.Fatal(err)
	}
	if got := []string{desc.Name(), desc.ReciprocalLabel(), desc.EdgeFamily()}; !reflect.DeepEqual(got, []string{"gates", "gated_by", "dependency"}) || !desc.DiagnosticOnly() || !desc.VersionPinned() || !desc.ForbidSelfLoops() {
		t.Fatalf("accessors lost edge contract: %#v", desc.Raw())
	}
	encoded, err := json.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip EdgeLabelDescriptor
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || !reflect.DeepEqual(roundTrip.Raw(), raw) {
		t.Fatalf("descriptor round trip = %#v, %v", roundTrip.Raw(), err)
	}
	registry := newEdgeRegistry([]EdgeLabelDescriptor{desc})
	if _, err := registry.Resolve(context.Background(), "missing"); !errors.Is(err, ErrUnknownEdgeLabel) {
		t.Fatalf("missing edge label error = %v", err)
	}
	if err := validateEdgeEndpoints(desc, Node{NodeID: "same", NodeKind: "task"}, Node{NodeID: "same", NodeKind: "task"}); !errors.Is(err, ErrEdgeSelfLoopForbidden) {
		t.Fatalf("self-loop error = %v", err)
	}
}
