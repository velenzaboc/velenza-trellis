package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTaskCanonicalJSONProjectionRendersConfiguredTaskProjection(t *testing.T) {
	registry := newProjectionRegistry([]ProjectionDescriptor{{
		Name: "task_canonical_json", OwningSpec: "task-graph requirements §2",
	}})
	projection, err := registry.Resolve(context.Background(), "task_canonical_json")
	if err != nil {
		t.Fatal(err)
	}
	root := Node{
		NodeID: "TASK-1", NodeKind: "task", OrganizationID: "example-org", Status: "active",
		CurrentVersion: NodeVersion{Version: 3, Contract: map[string]interface{}{"canonical_name": "TASK-1"}, Payload: map[string]interface{}{"title": "Gate"}, DefinitionHash: "definition", ManifestHash: "manifest", AdmittedBy: "principal", AdmittedAt: time.Unix(10, 0).UTC()},
	}
	data, err := projection.Render(context.Background(), root, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got["node_id"] != "TASK-1" || got["node_kind"] != "task" {
		t.Fatalf("projection = %#v", got)
	}
	if projection.Name() != "task_canonical_json" || projection.OwningSpec() != "task-graph requirements §2" {
		t.Fatalf("projection descriptor = %q / %q", projection.Name(), projection.OwningSpec())
	}
}

func TestProjectionRegistryRejectsUnknownConfiguredAlgorithm(t *testing.T) {
	registry := newProjectionRegistry([]ProjectionDescriptor{{Name: "not_implemented", OwningSpec: "owner"}})
	projection, err := registry.Resolve(context.Background(), "not_implemented")
	if err != nil {
		t.Fatal(err)
	}
	_, err = projection.Render(context.Background(), Node{}, nil)
	if !errors.Is(err, ErrSchemaPending) {
		t.Fatalf("Render error = %v, want ErrSchemaPending", err)
	}
	if _, err := registry.Resolve(context.Background(), "missing"); err == nil {
		t.Fatal("Resolve accepted an unknown projection")
	}
	all, err := registry.All(context.Background())
	if err != nil || len(all) != 1 || all[0] != "not_implemented" {
		t.Fatalf("All = %v, %v", all, err)
	}
}
