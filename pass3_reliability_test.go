package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestPass3EmbeddingModelUnification(t *testing.T) {
	for _, path := range []string{"task-graph.config.json", "task-graph.post-cutover.config.json"} {
		t.Setenv("TASK_GRAPH_CONFIG", path)
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("loadConfig(%q) failed: %v", path, err)
		}
		if cfg.Embedding == nil {
			t.Fatalf("%s: cfg.Embedding is nil", path)
		}
		if cfg.Embedding.Model != "gemini-embedding-2-preview" {
			t.Errorf("%s: Embedding.Model = %q, want %q", path, cfg.Embedding.Model, "gemini-embedding-2-preview")
		}
		if cfg.Embedding.Dimensions != 3072 {
			t.Errorf("%s: Embedding.Dimensions = %d, want 3072", path, cfg.Embedding.Dimensions)
		}
	}
}

func TestPass3SnapshotUnnestChunking(t *testing.T) {
	if got := snapshotUnnestIDsLimit(); got != 1000 {
		t.Fatalf("snapshotUnnestIDsLimit() = %d, want 1000", got)
	}

	ids := make([]string, 2500)
	for i := range ids {
		ids[i] = fmt.Sprintf("TASK-%04d", i)
	}
	chunks := chunkSnapshotIDs(ids, 1000)
	if len(chunks) != 3 {
		t.Fatalf("len(chunks) = %d, want 3", len(chunks))
	}
	if len(chunks[0]) != 1000 || len(chunks[1]) != 1000 || len(chunks[2]) != 500 {
		t.Fatalf("chunk lengths = [%d, %d, %d], want [1000, 1000, 500]", len(chunks[0]), len(chunks[1]), len(chunks[2]))
	}

	adapter := &SpannerSubstrateAdapter{
		Descriptor: SubstrateDescriptor{
			ID:            "task_graph_iter1",
			TaskTable:     "Tasks",
			DepTable:      "TaskDeps",
			WorklinkTable: "TaskLinks",
			ProjectColumn: "project_id",
		},
	}

	childrenOfRoot := make([]string, 2400)
	for i := range childrenOfRoot {
		childrenOfRoot[i] = fmt.Sprintf("CHILD-%04d", i)
	}

	querier := func(ctx context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
		for k, v := range params {
			if slice, ok := v.([]string); ok {
				if len(slice) > 1000 {
					t.Errorf("query %q param %q has %d elements, exceeding Spanner UNNEST safe chunk limit 1000", sql, k, len(slice))
				}
			}
		}
		switch {
		case strings.Contains(sql, "id = @root_task_id"):
			return QueryResult{Rows: []map[string]interface{}{{"id": "ROOT"}}}, nil
		case strings.Contains(sql, "parent_id IN UNNEST(@parent_ids)"):
			pids, _ := params["parent_ids"].([]string)
			var rows []map[string]interface{}
			for _, pid := range pids {
				if pid == "ROOT" {
					for _, cid := range childrenOfRoot {
						rows = append(rows, map[string]interface{}{"id": cid})
					}
				}
			}
			return QueryResult{Rows: rows}, nil
		case strings.Contains(sql, "id IN UNNEST(@scope_task_ids)"):
			sids, _ := params["scope_task_ids"].([]string)
			rows := make([]map[string]interface{}, 0, len(sids))
			for _, sid := range sids {
				rows = append(rows, map[string]interface{}{"id": sid})
			}
			return QueryResult{Rows: rows}, nil
		default:
			return QueryResult{}, fmt.Errorf("unexpected query: %s", sql)
		}
	}

	membership, err := discoverSnapshotMembership(context.Background(), adapter, querier, "arbor", snapshotScopeExecutionSubtree, "ROOT", 5000)
	if err != nil {
		t.Fatalf("discoverSnapshotMembership failed: %v", err)
	}
	if !membership.Complete {
		t.Errorf("membership.Complete = false, want true (missing=%v)", membership.MissingTaskIDs)
	}
	if len(membership.TaskIDs) != 2401 {
		t.Errorf("len(membership.TaskIDs) = %d, want 2401", len(membership.TaskIDs))
	}
}
