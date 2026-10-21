package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

type paginatedSnapshotReader struct {
	readAt time.Time
	at     time.Time
}

func (r *paginatedSnapshotReader) ReadOnlySnapshot(_ context.Context, fn func(SnapshotQuerier) error) (time.Time, error) {
	return r.run(fn)
}

func (r *paginatedSnapshotReader) ReadOnlySnapshotAt(_ context.Context, ts time.Time, fn func(SnapshotQuerier) error) (time.Time, error) {
	r.at = ts
	got, err := r.run(fn)
	if err != nil {
		return time.Time{}, err
	}
	if !got.Equal(ts) {
		return time.Time{}, fmt.Errorf("timestamp drift")
	}
	return got, nil
}

func (r *paginatedSnapshotReader) run(fn func(SnapshotQuerier) error) (time.Time, error) {
	q := func(_ context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
		if strings.Contains(sql, "AS task_id") {
			if params["after_task_id"] != nil {
				return rowsResult(map[string]interface{}{"task_id": "T-3", "pillar": "3", "status": "done"}), nil
			}
			return rowsResult(
				map[string]interface{}{"task_id": "T-1", "pillar": "3", "status": "done"},
				map[string]interface{}{"task_id": "T-2", "pillar": "3", "status": "done"},
			), nil
		}
		if strings.Contains(sql, "COUNT(*) AS n") {
			if strings.Contains(sql, "TaskDeps") || strings.Contains(sql, "TaskLinks") {
				return countResult("0"), nil
			}
			return countResult("3"), nil
		}
		if strings.Contains(sql, "COUNT(*) AS gap_task_count") {
			return rowsResult(map[string]interface{}{"gap_task_count": "0", "distinct_gap_count": "0"}), nil
		}
		return QueryResult{}, nil
	}
	if err := fn(q); err != nil {
		return time.Time{}, err
	}
	return r.readAt, nil
}

func TestSnapshotContinuationRejectsTamperingAndBindingDrift(t *testing.T) {
	key := []byte("unit-test cursor key with sufficient entropy")
	now := time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC)
	continuation, err := encodeSnapshotContinuation(key, snapshotContinuation{
		Version:       snapshotContinuationVersion,
		ProjectID:     "arbor",
		Scope:         snapshotScopeProject,
		ReadTimestamp: now.Add(-time.Minute).Format(time.RFC3339Nano),
		Revision:      snapshotRevision(now.Add(-time.Minute)),
		Limit:         2,
		ExpiresAt:     now.Add(time.Minute).Format(time.RFC3339Nano),
		TasksAfter:    []string{"3", "in_progress", "TASK-2"},
	})
	if err != nil {
		t.Fatalf("encode continuation: %v", err)
	}
	if _, err := decodeSnapshotContinuation(continuation, key, "arbor", snapshotScopeProject, "", 2, now); err != nil {
		t.Fatalf("decode valid continuation: %v", err)
	}
	if _, err := decodeSnapshotContinuation(continuation+"x", key, "arbor", snapshotScopeProject, "", 2, now); err == nil {
		t.Fatal("tampered continuation was accepted")
	}
	if _, err := decodeSnapshotContinuation(continuation, key, "other-project", snapshotScopeProject, "", 2, now); err == nil {
		t.Fatal("cross-project continuation was accepted")
	}
	if _, err := decodeSnapshotContinuation(continuation, key, "arbor", snapshotScopeProject, "", 3, now); err == nil {
		t.Fatal("changed page limit continuation was accepted")
	}
	if _, err := decodeSnapshotContinuation(continuation, key, "arbor", snapshotScopeProject, "", 2, now.Add(2*time.Minute)); err == nil {
		t.Fatal("expired continuation was accepted")
	}
	if _, err := decodeSnapshotContinuation(continuation, key, "arbor", snapshotScopeExecutionSubtree, "TASK-1", 2, now); err == nil {
		t.Fatal("cross-scope continuation was accepted")
	}
	rotatedKey := []byte("rotated unit-test cursor key material")
	if _, err := decodeSnapshotContinuation(continuation, rotatedKey, "arbor", snapshotScopeProject, "", 2, now); err == nil {
		t.Fatal("continuation signed by the prior key survived rotation")
	}
	scoped, err := encodeSnapshotContinuation(key, snapshotContinuation{
		Version: snapshotContinuationVersion, ProjectID: "arbor",
		Scope: snapshotScopeAcceptanceDependencyClosure, RootTaskID: "ROOT-1",
		ReadTimestamp: now.Add(-time.Minute).Format(time.RFC3339Nano),
		Revision:      snapshotRevision(now.Add(-time.Minute)), Limit: 2,
		ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("encode scoped continuation: %v", err)
	}
	if _, err := decodeSnapshotContinuation(scoped, key, "arbor", snapshotScopeAcceptanceDependencyClosure, "ROOT-1", 2, now); err != nil {
		t.Fatalf("decode scoped continuation: %v", err)
	}
	if _, err := decodeSnapshotContinuation(scoped, key, "arbor", snapshotScopeAcceptanceDependencyClosure, "ROOT-2", 2, now); err == nil {
		t.Fatal("cross-root continuation was accepted")
	}
}

func TestSnapshotContinuationRejectsUnknownVersion(t *testing.T) {
	key := []byte("unit-test cursor key with sufficient entropy")
	now := time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC)
	continuation, err := encodeSnapshotContinuation(key, snapshotContinuation{
		Version:       "unknown",
		ProjectID:     "arbor",
		Scope:         snapshotScopeProject,
		ReadTimestamp: now.Format(time.RFC3339Nano),
		Revision:      snapshotRevision(now),
		Limit:         2,
		ExpiresAt:     now.Add(time.Minute).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("encode unknown version continuation: %v", err)
	}
	if _, err := decodeSnapshotContinuation(continuation, key, "arbor", snapshotScopeProject, "", 2, now); err == nil {
		t.Fatal("unknown continuation version was accepted")
	}
}

func TestProjectSnapshotContinuationResumesExactTimestamp(t *testing.T) {
	readAt := time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC)
	reader := &paginatedSnapshotReader{readAt: readAt}
	env := snapshotTestEnv()
	env.SnapshotContinuationKey = []byte("01234567890123456789012345678901")
	env.SnapshotContinuationTTL = time.Hour
	first, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.Complete || first.Continuation == "" || len(first.Tasks) != 2 {
		t.Fatalf("first page = complete=%v continuation=%q tasks=%d", first.Complete, first.Continuation, len(first.Tasks))
	}
	second, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 2, Continuation: first.Continuation})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if !second.Complete || second.Continuation != "" || len(second.Tasks) != 1 || second.Tasks[0].TaskID != "T-3" {
		t.Fatalf("second page = complete=%v continuation=%q tasks=%+v", second.Complete, second.Continuation, second.Tasks)
	}
	if !reader.at.Equal(readAt) || second.ReadTimestamp != first.ReadTimestamp || second.Revision != first.Revision {
		t.Fatalf("resume changed snapshot timestamp: first=%s/%s second=%s/%s at=%s", first.ReadTimestamp, first.Revision, second.ReadTimestamp, second.Revision, reader.at)
	}
}

func TestSnapshotPageQueriesUseStrictUniqueOrderingTuple(t *testing.T) {
	adapter := snapshotTestAdapter()
	tasksSQL, taskParams := nativeSnapshotTasksPageQuery(adapter, "arbor", tasksInput{}, 2, []string{"3", "in_progress", "TASK-2"})
	if !strings.Contains(tasksSQL, "(pillar > @after_pillar") || taskParams["after_task_id"] != "TASK-2" {
		t.Fatalf("task continuation query does not use the full strict ordering tuple: %s / %#v", tasksSQL, taskParams)
	}
	depsSQL, depParams := nativeSnapshotDependenciesPageQuery(adapter, "arbor", 2, []string{"TASK-2", "TASK-1", "DEP-2"})
	if !strings.Contains(depsSQL, "(task_id > @after_task_id") || depParams["after_dependency_id"] != "DEP-2" {
		t.Fatalf("dependency continuation query does not use the full strict ordering tuple: %s / %#v", depsSQL, depParams)
	}
	linksSQL, linkParams := nativeSnapshotWorklinksPageQuery(adapter, "arbor", 2, []string{"2026-09-08T00:00:00Z", "LNK-2"})
	if !strings.Contains(linksSQL, "(created_at > @after_created_at") || linkParams["after_artifact_id"] != "LNK-2" {
		t.Fatalf("worklink continuation query does not use the full strict ordering tuple: %s / %#v", linksSQL, linkParams)
	}
	if got, ok := linkParams["after_created_at"].(time.Time); !ok || !got.Equal(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("worklink continuation timestamp = %#v, want typed time.Time 2026-09-08T00:00:00Z", linkParams["after_created_at"])
	}
}

func TestNewPagedCollectionStateZeroRowsWithPositiveTotalIsIncomplete(t *testing.T) {
	st := newPagedCollectionState(0, 0, 3, 2, "")
	if st.Complete {
		t.Fatalf("zero-row page with authorized_total=3 reported complete: %+v", st)
	}
	if !st.Capped || st.Collected != 0 || st.AuthorizedTotal != 3 {
		t.Fatalf("zero-row page lost exact incomplete state: %+v", st)
	}
}

func TestProjectSnapshotFailsClosedOnFirstPageNoProgress(t *testing.T) {
	reader := &fakeSnapshotReader{
		readAt: time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC),
		results: map[string]QueryResult{
			"COUNT(*) AS n": rowsResult(map[string]interface{}{"n": "3"}),
		},
	}
	_, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 2})
	if err == nil || !strings.Contains(err.Error(), "made no progress") {
		t.Fatalf("zero-row first page with positive total was accepted: %v", err)
	}
}

func TestProjectSnapshotFailsClosedWithoutContinuationKey(t *testing.T) {
	env := snapshotTestEnv()
	env.SnapshotContinuationKey = nil
	reader := &fakeSnapshotReader{
		readAt: time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC),
		results: map[string]QueryResult{
			"AS task_id": rowsResult(
				map[string]interface{}{"task_id": "T-1", "pillar": "3", "status": "done"},
			),
			"COUNT(*) AS n": countResult("3"),
		},
	}
	_, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 2})
	if err == nil || !strings.Contains(err.Error(), "signing key unavailable") {
		t.Fatalf("incomplete snapshot without owner key was accepted: %v", err)
	}
}

func TestProjectSnapshotContinuationRejectsAuthorizedTotalDrift(t *testing.T) {
	env := snapshotTestEnv()
	now := env.Now().UTC()
	readAt := now.Add(-time.Minute)
	encoded, err := encodeSnapshotContinuation(env.SnapshotContinuationKey, snapshotContinuation{
		Version: snapshotContinuationVersion, ProjectID: "arbor", Scope: snapshotScopeProject,
		ReadTimestamp: readAt.Format(time.RFC3339Nano), Revision: snapshotRevision(readAt),
		Limit: 2, ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
		TasksAfter: []string{"3", "done", "T-2"}, TasksTotal: 4, TasksCollected: 2,
	})
	if err != nil {
		t.Fatalf("encode continuation: %v", err)
	}
	reader := &paginatedSnapshotReader{readAt: readAt}
	_, err = nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 2, Continuation: encoded})
	if err == nil || !strings.Contains(err.Error(), "authorized totals mismatch") {
		t.Fatalf("continuation with drifted total was accepted: %v", err)
	}
}

func TestCanonicalJSONPreservesInt64BeyondFloatPrecision(t *testing.T) {
	const exact int64 = 9007199254740993
	b, err := canonicalJSON(map[string]interface{}{"value": exact})
	if err != nil {
		t.Fatalf("canonical JSON: %v", err)
	}
	var decoded map[string]json.Number
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("decode canonical JSON: %v", err)
	}
	if got := decoded["value"].String(); got != "9007199254740993" {
		t.Fatalf("canonical JSON changed exact INT64: got %q", got)
	}
}
