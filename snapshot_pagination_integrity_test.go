package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type paginationIntegrityMissingScopeReader struct {
	readAt time.Time
}

func (r *paginationIntegrityMissingScopeReader) ReadOnlySnapshot(_ context.Context, fn func(SnapshotQuerier) error) (time.Time, error) {
	return r.run(fn)
}

func (r *paginationIntegrityMissingScopeReader) ReadOnlySnapshotAt(_ context.Context, ts time.Time, fn func(SnapshotQuerier) error) (time.Time, error) {
	if !ts.Equal(r.readAt) {
		return time.Time{}, fmt.Errorf("timestamp drift: got %s want %s", ts, r.readAt)
	}
	return r.run(fn)
}

func (r *paginationIntegrityMissingScopeReader) run(fn func(SnapshotQuerier) error) (time.Time, error) {
	q := func(_ context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
		switch {
		case strings.Contains(sql, "id = @root_task_id"):
			return rowsResult(map[string]interface{}{"id": "ROOT"}), nil
		case strings.Contains(sql, "parent_id IN UNNEST"):
			return QueryResult{}, nil
		case strings.Contains(sql, "SELECT id FROM") && strings.Contains(sql, "id IN UNNEST(@scope_task_ids)"):
			ids, _ := params["scope_task_ids"].([]string)
			rows := make([]map[string]interface{}, 0, len(ids))
			for _, id := range ids {
				if id == "ROOT" || id == "EXISTS" || id == "EXISTS-2" {
					rows = append(rows, map[string]interface{}{"id": id})
				}
			}
			return rowsResult(rows...), nil
		case strings.Contains(sql, "AS task_id"):
			return rowsResult(map[string]interface{}{
				"project_id": "arbor", "task_id": "ROOT", "pillar": "P", "status": "in_progress",
			}), nil
		case strings.Contains(sql, "AS dependency_id"):
			if _, resumed := params["after_dependency_id"]; resumed {
				return rowsResult(map[string]interface{}{
					"project_id": "arbor", "dependency_id": "DEP-3", "task_id": "ROOT", "depends_on_task_id": "EXISTS-2", "kind": "gates",
				}), nil
			}
			return rowsResult(
				map[string]interface{}{"project_id": "arbor", "dependency_id": "DEP-1", "task_id": "ROOT", "depends_on_task_id": "MISSING", "kind": "gates"},
				map[string]interface{}{"project_id": "arbor", "dependency_id": "DEP-2", "task_id": "ROOT", "depends_on_task_id": "EXISTS", "kind": "gates"},
			), nil
		case strings.Contains(sql, "COUNT(*) AS n") && strings.Contains(sql, "TaskDeps"):
			return countResult("3"), nil
		case strings.Contains(sql, "COUNT(*) AS n") && strings.Contains(sql, "TaskLinks"):
			return countResult("0"), nil
		case strings.Contains(sql, "COUNT(*) AS n"):
			return countResult("1"), nil
		case strings.Contains(sql, "COUNT(*) AS gap_task_count"):
			return rowsResult(map[string]interface{}{"gap_task_count": "0", "distinct_gap_count": "0"}), nil
		default:
			return QueryResult{}, nil
		}
	}
	if err := fn(q); err != nil {
		return time.Time{}, err
	}
	return r.readAt, nil
}

func TestScopedPaginationPreservesMissingPrerequisiteAcrossPages(t *testing.T) {
	readAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	reader := &paginationIntegrityMissingScopeReader{readAt: readAt}
	env := snapshotTestEnv()
	env.Now = func() time.Time { return readAt.Add(time.Minute) }

	first, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{
		Limit: 2, Scope: snapshotScopeExecutionSubtree, RootTaskID: "ROOT",
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Scope.Complete || len(first.Scope.MissingTaskIDs) != 1 || first.Scope.MissingTaskIDs[0] != "MISSING" {
		t.Fatalf("first page did not expose missing prerequisite: %+v", first.Scope)
	}

	second, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{
		Limit: 2, Scope: snapshotScopeExecutionSubtree, RootTaskID: "ROOT", Continuation: first.Continuation,
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.Scope.Complete || len(second.Scope.MissingTaskIDs) != 1 || second.Scope.MissingTaskIDs[0] != "MISSING" {
		t.Fatalf("final page lost the earlier missing prerequisite: %+v", second.Scope)
	}
}

type paginationIntegrityEmptyOrderingReader struct {
	readAt time.Time
}

func (r *paginationIntegrityEmptyOrderingReader) ReadOnlySnapshot(_ context.Context, fn func(SnapshotQuerier) error) (time.Time, error) {
	return r.run(fn)
}

func (r *paginationIntegrityEmptyOrderingReader) ReadOnlySnapshotAt(_ context.Context, ts time.Time, fn func(SnapshotQuerier) error) (time.Time, error) {
	if !ts.Equal(r.readAt) {
		return time.Time{}, fmt.Errorf("timestamp drift: got %s want %s", ts, r.readAt)
	}
	return r.run(fn)
}

func (r *paginationIntegrityEmptyOrderingReader) run(fn func(SnapshotQuerier) error) (time.Time, error) {
	q := func(_ context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
		switch {
		case strings.Contains(sql, "AS task_id"):
			if _, resumed := params["after_task_id"]; resumed {
				return rowsResult(map[string]interface{}{"project_id": "arbor", "task_id": "TASK-2", "pillar": "P", "status": "done"}), nil
			}
			return rowsResult(map[string]interface{}{"project_id": "arbor", "task_id": "TASK-1", "pillar": "", "status": "in_progress"}), nil
		case strings.Contains(sql, "COUNT(*) AS n") && (strings.Contains(sql, "TaskDeps") || strings.Contains(sql, "TaskLinks")):
			return countResult("0"), nil
		case strings.Contains(sql, "COUNT(*) AS n"):
			return countResult("2"), nil
		case strings.Contains(sql, "COUNT(*) AS gap_task_count"):
			return rowsResult(map[string]interface{}{"gap_task_count": "0", "distinct_gap_count": "0"}), nil
		default:
			return QueryResult{}, nil
		}
	}
	if err := fn(q); err != nil {
		return time.Time{}, err
	}
	return r.readAt, nil
}

func TestSnapshotPaginationAcceptsEmptyTaskOrderingField(t *testing.T) {
	readAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	reader := &paginationIntegrityEmptyOrderingReader{readAt: readAt}
	env := snapshotTestEnv()
	env.Now = func() time.Time { return readAt.Add(time.Minute) }

	first, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Continuation == "" {
		t.Fatal("first page omitted continuation")
	}
	if _, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 1, Continuation: first.Continuation}); err != nil {
		t.Fatalf("valid continuation after empty ordering field was rejected: %v", err)
	}
}

func TestSnapshotContinuationAcceptsSpannerHourOffsetTimestamp(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	readAt := now.Add(-time.Minute)
	token, err := encodeSnapshotContinuation(key, snapshotContinuation{
		Version: snapshotContinuationVersion, ProjectID: "arbor", Scope: snapshotScopeProject,
		ReadTimestamp: readAt.Format(time.RFC3339Nano), Revision: snapshotRevision(readAt),
		Limit: 1, ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
		LinksAfter: []string{"2026-07-03 09:52:46.616876-07", "LNK-0001"}, LinksTotal: 2, LinksCollected: 1,
	})
	if err != nil {
		t.Fatalf("encode continuation: %v", err)
	}
	if _, err := decodeSnapshotContinuation(token, key, "arbor", snapshotScopeProject, "", 1, now); err != nil {
		t.Fatalf("live Spanner timestamp cursor was rejected: %v", err)
	}
}

func TestSnapshotContinuationValidatesIdentityBearingTupleParts(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	base := snapshotContinuation{
		Version: snapshotContinuationVersion, ProjectID: "arbor", Scope: snapshotScopeProject,
		ReadTimestamp: now.Add(-time.Minute).Format(time.RFC3339Nano), Revision: snapshotRevision(now.Add(-time.Minute)),
		Limit: 1, ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
		TasksAfter: []string{"", "", "TASK-1"}, TasksTotal: 2, TasksCollected: 1,
	}
	token, err := encodeSnapshotContinuation(key, base)
	if err != nil {
		t.Fatalf("encode valid empty ordering fields: %v", err)
	}
	if _, err := decodeSnapshotContinuation(token, key, "arbor", snapshotScopeProject, "", 1, now); err != nil {
		t.Fatalf("nullable ordering fields were rejected: %v", err)
	}

	base.TasksAfter[2] = ""
	token, err = encodeSnapshotContinuation(key, base)
	if err != nil {
		t.Fatalf("encode empty identity tuple: %v", err)
	}
	if _, err := decodeSnapshotContinuation(token, key, "arbor", snapshotScopeProject, "", 1, now); err == nil {
		t.Fatal("empty task identity in ordering tuple was accepted")
	}
}

func TestSnapshotContinuationCarriesOnlyValidScopedMissingIDs(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	base := snapshotContinuation{
		Version: snapshotContinuationVersion, ProjectID: "arbor", Scope: snapshotScopeExecutionSubtree, RootTaskID: "ROOT",
		ReadTimestamp: now.Add(-time.Minute).Format(time.RFC3339Nano), Revision: snapshotRevision(now.Add(-time.Minute)),
		Limit: 1, ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano), ScopeMissingTaskIDs: []string{"MISSING"},
	}
	token, err := encodeSnapshotContinuation(key, base)
	if err != nil {
		t.Fatalf("encode scoped continuation: %v", err)
	}
	decoded, err := decodeSnapshotContinuation(token, key, "arbor", snapshotScopeExecutionSubtree, "ROOT", 1, now)
	if err != nil {
		t.Fatalf("decode scoped continuation: %v", err)
	}
	if len(decoded.ScopeMissingTaskIDs) != 1 || decoded.ScopeMissingTaskIDs[0] != "MISSING" {
		t.Fatalf("scope missing task IDs were not preserved: %#v", decoded.ScopeMissingTaskIDs)
	}

	for name, ids := range map[string][]string{
		"empty":     {""},
		"duplicate": {"MISSING", "MISSING"},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := base
			invalid.ScopeMissingTaskIDs = ids
			encoded, encodeErr := encodeSnapshotContinuation(key, invalid)
			if encodeErr != nil {
				t.Fatalf("encode invalid scoped continuation: %v", encodeErr)
			}
			if _, decodeErr := decodeSnapshotContinuation(encoded, key, "arbor", snapshotScopeExecutionSubtree, "ROOT", 1, now); decodeErr == nil {
				t.Fatalf("invalid scope missing IDs were accepted: %#v", ids)
			}
		})
	}
}

func TestSpannerHourOffsetCursorBindsTypedStrictWorklinkKeyset(t *testing.T) {
	sql, params := nativeSnapshotWorklinksPageQuery(snapshotTestAdapter(), "arbor", 2, []string{"2026-07-03 09:52:46.616876-07", "LNK-0001"})
	wantPredicate := "created_at > @after_created_at OR (created_at = @after_created_at AND link_id > @after_artifact_id)"
	if !strings.Contains(sql, wantPredicate) {
		t.Fatalf("worklink continuation predicate is not the strict unique tuple: %s", sql)
	}
	want := time.Date(2026, 7, 3, 16, 52, 46, 616876000, time.UTC)
	if got, ok := params["after_created_at"].(time.Time); !ok || !got.Equal(want) {
		t.Fatalf("worklink continuation timestamp = %#v, want typed UTC %s", params["after_created_at"], want.Format(time.RFC3339Nano))
	}
	if params["after_artifact_id"] != "LNK-0001" {
		t.Fatalf("worklink continuation identity = %#v", params["after_artifact_id"])
	}
}
