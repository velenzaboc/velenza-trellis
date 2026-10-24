//go:build integration

package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

type firstQueryPausingSnapshotReader struct {
	delegate    SnapshotReader
	firstRead   chan struct{}
	release     chan struct{}
	signalOnce  sync.Once
	releaseOnce sync.Once
}

func newFirstQueryPausingSnapshotReader(delegate SnapshotReader) *firstQueryPausingSnapshotReader {
	return &firstQueryPausingSnapshotReader{
		delegate:  delegate,
		firstRead: make(chan struct{}),
		release:   make(chan struct{}),
	}
}

func (r *firstQueryPausingSnapshotReader) ReadOnlySnapshot(ctx context.Context, fn func(SnapshotQuerier) error) (time.Time, error) {
	return r.delegate.ReadOnlySnapshot(ctx, func(q SnapshotQuerier) error {
		queryCount := 0
		return fn(func(ctx context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
			result, err := q(ctx, sql, params)
			if err != nil {
				return result, err
			}
			queryCount++
			if queryCount == 1 {
				r.signalOnce.Do(func() { close(r.firstRead) })
				select {
				case <-r.release:
				case <-ctx.Done():
					return QueryResult{}, ctx.Err()
				}
			}
			return result, nil
		})
	})
}

func (r *firstQueryPausingSnapshotReader) releaseReads() {
	r.releaseOnce.Do(func() { close(r.release) })
}

type projectSnapshotCallResult struct {
	snapshot ProjectSnapshotResult
	err      error
}

func waitForSnapshotCall(ch <-chan projectSnapshotCallResult) (projectSnapshotCallResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		return projectSnapshotCallResult{}, fmt.Errorf("snapshot goroutine did not terminate: %w", ctx.Err())
	}
}

func taskIDs(snapshot ProjectSnapshotResult) map[string]bool {
	ids := make(map[string]bool, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		ids[task.TaskID] = true
	}
	return ids
}

func TestIntegrationReadOnlySnapshotExcludesMidflightWriteAndReturnsPinnedTimestamp(t *testing.T) {
	requireEmulator(t)
	setupCtx := context.Background()
	setupEmulatorDBWithSchema(setupCtx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	defer env.Substrates.Close()

	createNativeTask(t, setupCtx, env, "arbor", "SNAPSHOT-A", "Snapshot A", "not_started")
	createNativeTask(t, setupCtx, env, "arbor", "SNAPSHOT-B", "Snapshot B", "not_started")

	adapter, err := soleNativeFleetPlanAdapter(setupCtx, env)
	if err != nil {
		t.Fatalf("resolve native adapter: %v", err)
	}
	client, err := adapter.getClient(setupCtx)
	if err != nil {
		t.Fatalf("open native adapter client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reader := newFirstQueryPausingSnapshotReader(adapter)
	defer reader.releaseReads()
	resultCh := make(chan projectSnapshotCallResult, 1)
	go func() {
		snapshot, callErr := nativeHandleProjectSnapshot(
			ctx,
			env,
			adapter,
			reader,
			projectSnapshotInput{ProjectID: "arbor"},
		)
		resultCh <- projectSnapshotCallResult{snapshot: snapshot, err: callErr}
	}()

	select {
	case <-reader.firstRead:
		// The real adapter has drained its first query and fixed the read-only
		// transaction's timestamp. Commit a third task before the handler's
		// remaining reads, without substituting fixtures for either operation.
	case early := <-resultCh:
		reader.releaseReads()
		t.Fatalf("snapshot returned before the first-query barrier: %+v, %v", early.snapshot, early.err)
	case <-ctx.Done():
		reader.releaseReads()
		if _, joinErr := waitForSnapshotCall(resultCh); joinErr != nil {
			t.Fatalf("first-query barrier timed out and cleanup failed: %v; %v", ctx.Err(), joinErr)
		}
		t.Fatalf("first-query barrier timed out: %v", ctx.Err())
	}

	thirdTaskTimestamp := time.Now().UTC()
	commitTimestamp, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert(
			"Tasks",
			[]string{"project_id", "id", "title", "pillar", "status", "owner", "is_gap", "created_at", "updated_at"},
			[]interface{}{"arbor", "SNAPSHOT-C", "Snapshot C", "P", "not_started", "UNOWNED", false, thirdTaskTimestamp, thirdTaskTimestamp},
		),
	})
	reader.releaseReads()
	if err != nil {
		cancel()
		if _, joinErr := waitForSnapshotCall(resultCh); joinErr != nil {
			t.Fatalf("commit third task: %v; snapshot cleanup: %v", err, joinErr)
		}
		t.Fatalf("commit third task: %v", err)
	}

	call, err := waitForSnapshotCall(resultCh)
	if err != nil {
		cancel()
		reader.releaseReads()
		if _, joinErr := waitForSnapshotCall(resultCh); joinErr != nil {
			t.Fatalf("%v; snapshot cleanup: %v", err, joinErr)
		}
		t.Fatal(err)
	}
	if call.err != nil {
		t.Fatalf("coherent project snapshot: %v", call.err)
	}
	gotIDs := taskIDs(call.snapshot)
	if len(gotIDs) != 2 || !gotIDs["SNAPSHOT-A"] || !gotIDs["SNAPSHOT-B"] || gotIDs["SNAPSHOT-C"] {
		t.Fatalf("snapshot tasks = %v, want exactly SNAPSHOT-A and SNAPSHOT-B", gotIDs)
	}
	if got := call.snapshot.Collections.Tasks.AuthorizedTotal; got != 2 {
		t.Fatalf("snapshot authorized task total = %d, want 2", got)
	}
	readTimestamp, err := time.Parse(time.RFC3339Nano, call.snapshot.ReadTimestamp)
	if err != nil {
		t.Fatalf("parse snapshot read timestamp %q: %v", call.snapshot.ReadTimestamp, err)
	}
	if readTimestamp.IsZero() || !readTimestamp.Before(commitTimestamp) {
		t.Fatalf("snapshot read timestamp = %s, want nonzero and before third commit %s", readTimestamp, commitTimestamp)
	}

	later, err := nativeHandleProjectSnapshot(
		ctx,
		env,
		adapter,
		adapter,
		projectSnapshotInput{ProjectID: "arbor"},
	)
	if err != nil {
		t.Fatalf("later project snapshot: %v", err)
	}
	laterIDs := taskIDs(later)
	if len(laterIDs) != 3 || !laterIDs["SNAPSHOT-A"] || !laterIDs["SNAPSHOT-B"] || !laterIDs["SNAPSHOT-C"] {
		t.Fatalf("later snapshot tasks = %v, want all three committed tasks", laterIDs)
	}
	if got := later.Collections.Tasks.AuthorizedTotal; got != 3 {
		t.Fatalf("later snapshot authorized task total = %d, want 3", got)
	}
}
