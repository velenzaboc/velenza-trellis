package main

import (
	"context"
	"strings"
	"testing"
)

func canceledNativeReadContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func requireNativeReadFailure(t *testing.T, err error, contains string) {
	t.Helper()
	if err == nil {
		t.Fatalf("operation succeeded, want error containing %q", contains)
	}
	if contains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(contains)) {
		t.Fatalf("error = %v, want substring %q", err, contains)
	}
}

func nativeReadAdapterWithDescriptor(adapter *SpannerSubstrateAdapter, descriptor SubstrateDescriptor) *SpannerSubstrateAdapter {
	return &SpannerSubstrateAdapter{
		Descriptor:    descriptor,
		Credentials:   adapter.Credentials,
		AllowEmulator: adapter.AllowEmulator,
		client:        adapter.client,
	}
}

func TestNativeReadListAndSearchFailClosed(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	adapter := nativeCoverageAdapter(t, env)
	canceled := canceledNativeReadContext()

	if _, err := nativeListNodes(context.Background(), adapter, ListFilter{}); err == nil || !strings.Contains(err.Error(), "project_id required") {
		t.Fatalf("unscoped list error = %v", err)
	}

	listCases := []struct {
		name   string
		filter ListFilter
	}{
		{"default limit task query", ListFilter{ProjectID: "arbor", NodeKind: "task"}},
		{"artifact query", ListFilter{ProjectID: "arbor", NodeKind: "artifact", Limit: 7}},
		{"mixed query starts with tasks", ListFilter{ProjectID: "arbor", Limit: 7}},
	}
	for _, tc := range listCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeListNodes(canceled, adapter, tc.filter)
			requireNativeReadFailure(t, err, "canceled")
		})
	}

	if _, err := adapter.ListNodes(canceled, ListFilter{ProjectID: "arbor", NodeKind: "task", Limit: 3}); err == nil {
		t.Fatal("adapter native list ignored canceled context")
	}
	unknownList, err := nativeListNodes(context.Background(), adapter, ListFilter{ProjectID: "arbor", NodeKind: "unknown", Limit: 3})
	if err != nil || len(unknownList) != 0 {
		t.Fatalf("unknown-kind list = %+v, %v", unknownList, err)
	}

	if _, err := nativeSearchNodes(context.Background(), adapter, "arbor", " \t ", "", 5); err == nil || !strings.Contains(err.Error(), "no searchable terms") {
		t.Fatalf("blank native search error = %v", err)
	}
	searchCases := []struct {
		name     string
		query    string
		nodeKind string
		limit    int
		want     string
	}{
		{"default limit task query", "Alpha ALPHA evidence", "task", 0, "native task search"},
		{"mixed search starts with tasks", "alpha evidence", "", 4, "native task search"},
		{"artifact query", "branch evidence", "artifact", 4, "native worklink search"},
	}
	for _, tc := range searchCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeSearchNodes(canceled, adapter, "arbor", tc.query, tc.nodeKind, tc.limit)
			requireNativeReadFailure(t, err, tc.want)
			if !strings.Contains(strings.ToLower(err.Error()), "canceled") {
				t.Fatalf("search error did not preserve query failure: %v", err)
			}
		})
	}
	unknownSearch, err := nativeSearchNodes(context.Background(), adapter, "arbor", "alpha", "unknown", 5)
	if err != nil || len(unknownSearch) != 0 {
		t.Fatalf("unknown-kind search = %+v, %v", unknownSearch, err)
	}
}

func TestNativeReadIdentityAndAdapterBoundaries(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	adapter := nativeCoverageAdapter(t, env)
	canceled := canceledNativeReadContext()

	if _, err := adapter.ReadNodeInProject(context.Background(), "arbor", "TASK", "2", ""); err == nil || !strings.Contains(err.Error(), "unversioned") {
		t.Fatalf("versioned native read error = %v", err)
	}
	if _, err := adapter.ReadNodeInProject(canceled, "arbor", "TASK", "1", ""); err == nil {
		t.Fatal("adapter native read ignored canceled context")
	}
	if _, err := nativeReadNode(canceled, adapter, "arbor", "TASK"); err == nil {
		t.Fatal("native read ignored canceled context")
	}
	if _, found, err := nativeReadTaskNode(canceled, adapter, "arbor", "TASK"); err == nil || found {
		t.Fatalf("canceled task read = found %v, error %v", found, err)
	}
	if _, found, err := nativeReadArtifactNode(canceled, adapter, "arbor", "LINK"); err == nil || found {
		t.Fatalf("canceled artifact read = found %v, error %v", found, err)
	}

	if _, err := adapter.ReadEdge(context.Background(), "edge", ""); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("native ReadEdge error = %v", err)
	}
	if _, err := adapter.History(context.Background(), "TASK"); err == nil || !strings.Contains(err.Error(), "node_table not configured") {
		t.Fatalf("native History error = %v", err)
	}
	if _, err := adapter.SearchByEmbedding(context.Background(), []float32{1}, "task", 5, "task-plan"); err == nil || !strings.Contains(err.Error(), "node_table not configured") {
		t.Fatalf("native embedding search error = %v", err)
	}
}

func TestNativeReadNeighborQueriesFailClosed(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	adapter := nativeCoverageAdapter(t, env)
	canceled := canceledNativeReadContext()

	if _, err := nativeNeighbors(context.Background(), adapter, "TASK", NeighborOpts{}); err == nil || !strings.Contains(err.Error(), "project_id required") {
		t.Fatalf("unscoped neighbors error = %v", err)
	}

	cases := []struct {
		name string
		opts NeighborOpts
	}{
		{"incoming advances", NeighborOpts{ProjectID: "arbor", Direction: "in", EdgeLabels: []string{"advances"}}},
		{"outgoing advances", NeighborOpts{ProjectID: "arbor", Direction: "out", EdgeLabels: []string{"advances"}}},
		{"outgoing parent", NeighborOpts{ProjectID: "arbor", Direction: "out", EdgeLabels: []string{"parent_of"}}},
		{"incoming child", NeighborOpts{ProjectID: "arbor", Direction: "in", EdgeLabels: []string{"child_of"}}},
		{"dependency gates", NeighborOpts{ProjectID: "arbor", Direction: "both", EdgeLabels: []string{"gates"}}},
		{"dependency gated by", NeighborOpts{ProjectID: "arbor", Direction: "both", EdgeLabels: []string{"gated_by"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := nativeNeighbors(canceled, adapter, "TASK", tc.opts); err == nil {
				t.Fatal("native neighbors ignored canceled context")
			}
		})
	}
	if _, err := adapter.Neighbors(canceled, "TASK", NeighborOpts{ProjectID: "arbor", Direction: "out", EdgeLabels: []string{"parent_of"}}); err == nil {
		t.Fatal("adapter native neighbors ignored canceled context")
	}
}

func TestNativeReadTraversalBoundaries(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	adapter := nativeCoverageAdapter(t, env)
	background := context.Background()

	if _, err := nativeTraverseClosure(background, adapter, "", "TASK", nil, 2, 5, "forward"); err == nil || !strings.Contains(err.Error(), "project_id required") {
		t.Fatalf("unscoped traversal error = %v", err)
	}
	if _, err := nativeTraverseClosure(background, adapter, "arbor", "", nil, 2, 5, "forward"); err == nil || !strings.Contains(err.Error(), "node_id required") {
		t.Fatalf("identity-free traversal error = %v", err)
	}

	for _, tc := range []struct {
		name      string
		maxDepth  int
		resultCap int
		direction string
	}{
		{"zero depth", 0, 5, "forward"},
		{"zero result cap", 5, 0, "forward"},
		{"reverse zero depth", 0, 5, "reverse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes, err := nativeTraverseClosure(background, adapter, "arbor", "TASK", nil, tc.maxDepth, tc.resultCap, tc.direction)
			if err != nil || len(nodes) != 0 {
				t.Fatalf("bounded traversal = %+v, %v", nodes, err)
			}
		})
	}

	canceled := canceledNativeReadContext()
	if _, err := nativeTraverseClosure(canceled, adapter, "arbor", "TASK", []string{"advances"}, 2, 5, "forward"); err == nil {
		t.Fatal("forward traversal ignored neighbor query failure")
	}
	if _, err := adapter.TraverseClosure(canceled, "arbor", "TASK", []string{"gates"}, 2, 5, "reverse"); err == nil {
		t.Fatal("adapter reverse traversal ignored neighbor query failure")
	}
}

func TestNativeReadMissingTablesFailClosed(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	ctx := context.Background()
	adapter := nativeCoverageAdapter(t, env)

	taskDescriptor := adapter.Descriptor
	taskDescriptor.TaskTable = "MissingTasks"
	badTasks := nativeReadAdapterWithDescriptor(adapter, taskDescriptor)
	if _, err := nativeListNodes(ctx, badTasks, ListFilter{ProjectID: "arbor", NodeKind: "task"}); err == nil {
		t.Fatal("task list against missing table succeeded")
	}
	if _, err := nativeSearchNodes(ctx, badTasks, "arbor", "existing", "task", 5); err == nil || !strings.Contains(err.Error(), "native task search") {
		t.Fatalf("task search error = %v", err)
	}
	if _, _, err := nativeReadTaskNode(ctx, badTasks, "arbor", "TASK"); err == nil {
		t.Fatal("task read against missing table succeeded")
	}

	linkDescriptor := adapter.Descriptor
	linkDescriptor.WorklinkTable = "MissingTaskLinks"
	badLinks := nativeReadAdapterWithDescriptor(adapter, linkDescriptor)
	if _, err := nativeListNodes(ctx, badLinks, ListFilter{ProjectID: "arbor", NodeKind: "artifact"}); err == nil {
		t.Fatal("artifact list against missing table succeeded")
	}
	if _, err := nativeSearchNodes(ctx, badLinks, "arbor", "evidence", "artifact", 5); err == nil || !strings.Contains(err.Error(), "native worklink search") {
		t.Fatalf("artifact search error = %v", err)
	}
	if _, _, err := nativeReadArtifactNode(ctx, badLinks, "arbor", "LINK"); err == nil {
		t.Fatal("artifact read against missing table succeeded")
	}
	if _, err := nativeNeighbors(ctx, badLinks, "TASK", NeighborOpts{ProjectID: "arbor", EdgeLabels: []string{"advances"}}); err == nil {
		t.Fatal("neighbors against missing worklink table succeeded")
	}

	depDescriptor := adapter.Descriptor
	depDescriptor.DepTable = "MissingTaskDeps"
	badDeps := nativeReadAdapterWithDescriptor(adapter, depDescriptor)
	if _, err := nativeNeighbors(ctx, badDeps, "TASK", NeighborOpts{ProjectID: "arbor", EdgeLabels: []string{"gates"}}); err == nil {
		t.Fatal("neighbors against missing dependency table succeeded")
	}
}
