package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// snapshot_coherence_test.go is the regression guard for P3-GT-FP-SNAPSHOT.
//
// The property under test is NOT "the result carries a read_timestamp field" —
// a field-presence assertion stays green after someone routes a read back
// through ExecuteReadOnlyQuery, which is the exact defect this work closed.
// The property is HOW MANY READ-ONLY TRANSACTIONS the snapshot opened and
// WHICH statements ran inside each, which fakeSnapshotReader observes directly.
//
// No Spanner. The handler reaches the store only through the injected
// SnapshotReader, and every coherence value it publishes is a pure function of
// rows already read.

// ---------------- fake reader ----------------

// fakeSnapshotReader records one entry per ReadOnlySnapshot call, each holding
// the statements issued inside that transaction. Two entries means two read
// timestamps means the snapshot can mix revisions.
type fakeSnapshotReader struct {
	transactions [][]string
	results      map[string]QueryResult
	readAt       time.Time
	err          error
}

type scriptedSnapshotReader struct {
	transactions int
	statements   []string
	readAt       time.Time
	query        func(sql string, params map[string]interface{}) (QueryResult, error)
}

func (s *scriptedSnapshotReader) ReadOnlySnapshot(ctx context.Context, fn func(q SnapshotQuerier) error) (time.Time, error) {
	s.transactions++
	q := func(_ context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
		s.statements = append(s.statements, sql)
		return s.query(sql, params)
	}
	if err := fn(q); err != nil {
		return time.Time{}, err
	}
	return s.readAt, nil
}

func (f *fakeSnapshotReader) ReadOnlySnapshot(ctx context.Context, fn func(q SnapshotQuerier) error) (time.Time, error) {
	if f.err != nil {
		return time.Time{}, f.err
	}
	idx := len(f.transactions)
	f.transactions = append(f.transactions, nil)
	q := func(ctx context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
		f.transactions[idx] = append(f.transactions[idx], sql)
		// COUNT(*) rows are table-specific. Keep this deterministic instead of
		// relying on map iteration or a broad FROM marker that also matches the
		// collection row query.
		if strings.Contains(sql, "COUNT(*) AS n") {
			if strings.Contains(sql, "TaskDeps") || strings.Contains(sql, "TaskLinks") {
				return countResult("0"), nil
			}
		}
		bestMarker := ""
		var best QueryResult
		for marker, res := range f.results {
			if strings.Contains(sql, marker) && len(marker) > len(bestMarker) {
				bestMarker, best = marker, res
			}
		}
		if bestMarker != "" {
			return best, nil
		}
		return QueryResult{}, nil
	}
	if err := fn(q); err != nil {
		return time.Time{}, err
	}
	return f.readAt, nil
}

// statementCount totals the statements across every transaction the fake saw.
func (f *fakeSnapshotReader) statementCount() int {
	n := 0
	for _, txn := range f.transactions {
		n += len(txn)
	}
	return n
}

// snapshotTestAdapter is a descriptor-only adapter: it carries the table names
// and the (absent) project column the SQL builders need, and NO Spanner client
// and NO credentials. That is deliberate — getClient on this descriptor fails
// immediately with "explicit credentials source required", so any read the
// handler tries to make OUTSIDE the injected SnapshotReader fails loudly rather
// than dialing out. The absent ProjectColumn also mirrors the live shape of
// BOTH consumers of this binary today (task-plan and example-consumer), so these tests
// exercise the pre-cutover path they actually run.
func snapshotTestAdapter() *SpannerSubstrateAdapter {
	return &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{
		ID:            "snapshot-test",
		TaskStoreType: fleetPlanNativeStoreType,
		TaskTable:     "Tasks",
		WorklinkTable: "TaskLinks",
		DepTable:      "TaskDeps",
	}}
}

func snapshotTestEnv() *Env {
	return &Env{
		Config: SpecGraphConfig{DefaultProjectID: "arbor", DefaultOrganizationID: "example-org"},
		Now:    func() time.Time { return time.Date(2026, 8, 17, 9, 0, 0, 0, time.UTC) },
		// Test-only key mirrors the dedicated runtime-resolved continuation
		// credential without reading process environment in unit tests.
		SnapshotContinuationKey: []byte("01234567890123456789012345678901"),
		SnapshotContinuationTTL: 5 * time.Minute,
	}
}

func rowsResult(rows ...map[string]interface{}) QueryResult {
	return QueryResult{Rows: rows}
}

func countResult(n string) QueryResult {
	return rowsResult(map[string]interface{}{"n": n})
}

// ---------------- the coherence property ----------------

// TestProjectSnapshotReadsInExactlyOneTransaction is THE test for
// P3-GT-FP-SNAPSHOT. Before the fix, nativeHandleProjectSnapshot issued five
// independent client.Single() queries — five read timestamps — so a concurrent
// write between any two of them produced a snapshot describing a state that
// never existed. This asserts every statement lands in ONE transaction.
func TestProjectSnapshotReadsInExactlyOneTransaction(t *testing.T) {
	reader := &fakeSnapshotReader{
		readAt: time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC),
		results: map[string]QueryResult{
			"AS task_id":       rowsResult(map[string]interface{}{"task_id": "T-1", "pillar": "3", "status": "done"}),
			"AS dependency_id": rowsResult(map[string]interface{}{"dependency_id": "DEP-1", "task_id": "T-1", "depends_on_task_id": "T-0"}),
			"AS artifact_id":   rowsResult(map[string]interface{}{"artifact_id": "LNK-1", "created_at": "2026-08-17T00:00:00Z"}),
			"COUNT(*) AS n":    countResult("1"),
		},
	}
	out, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if len(reader.transactions) != 1 {
		t.Fatalf("snapshot opened %d read-only transactions, want exactly 1 — %d transactions means %d read timestamps, so the snapshot can interleave revisions (P3-GT-FP-SNAPSHOT). Statements per transaction: %v",
			len(reader.transactions), len(reader.transactions), len(reader.transactions), transactionShapes(reader))
	}
	// Every collection AND every total must be inside that one transaction. A
	// total counted outside it would make "capped" indistinguishable from "the
	// store moved", which is the same defect one layer down.
	const wantStatements = 8
	if got := reader.statementCount(); got != wantStatements {
		t.Fatalf("snapshot issued %d statements, want %d (tasks, counts-group, counts-gap, dependencies, worklinks, and one authorized-total per capped collection); a missing statement means a collection is not being read coherently: %v",
			got, wantStatements, reader.transactions[0])
	}
	if out.ReadTimestamp != "2026-08-17T08:30:00Z" {
		t.Fatalf("read_timestamp = %q, want the transaction's read timestamp 2026-08-17T08:30:00Z", out.ReadTimestamp)
	}
	if out.Scope.Mode != snapshotScopeProject || !out.Scope.Complete {
		t.Fatalf("default scope = %+v, want complete project scope", out.Scope)
	}
}

func transactionShapes(f *fakeSnapshotReader) []int {
	out := make([]int, 0, len(f.transactions))
	for _, txn := range f.transactions {
		out = append(out, len(txn))
	}
	return out
}

// TestProjectSnapshotRevisionDerivesFromTheReadTimestamp pins revision to the
// single read timestamp rather than to the wall clock, so two snapshots
// carrying the same revision genuinely describe the same state.
func TestProjectSnapshotRevisionDerivesFromTheReadTimestamp(t *testing.T) {
	readAt := time.Date(2026, 8, 17, 8, 30, 0, 123456789, time.UTC)
	reader := &fakeSnapshotReader{readAt: readAt, results: map[string]QueryResult{"COUNT(*) AS n": countResult("0")}}
	env := snapshotTestEnv()

	out, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if want := snapshotRevision(readAt); out.Revision != want {
		t.Fatalf("revision = %q, want %q", out.Revision, want)
	}
	if out.Revision == out.GeneratedAt {
		t.Fatalf("revision must not be the wall clock — generated_at is when the response was assembled, revision is when the store was read")
	}
	if out.GeneratedAt != "2026-08-17T09:00:00Z" {
		t.Fatalf("generated_at = %q, want the env wall clock 2026-08-17T09:00:00Z", out.GeneratedAt)
	}
	// The read timestamp must be the transaction's, NOT env.Now() — a handler
	// that stamped both from the clock would look right and mean nothing.
	if out.ReadTimestamp == out.GeneratedAt {
		t.Fatalf("read_timestamp %q equals generated_at — read_timestamp must come from the transaction, not the clock", out.ReadTimestamp)
	}
}

// TestProjectSnapshotCapStateAndCursor covers the cap/cursor half: an uncapped
// collection publishes no cursor, a capped one publishes the last row's full
// ordering tuple.
func TestProjectSnapshotCapStateAndCursor(t *testing.T) {
	tasks := rowsResult(
		map[string]interface{}{"task_id": "T-1", "pillar": "3", "status": "done"},
		map[string]interface{}{"task_id": "T-2", "pillar": "3", "status": "in_progress"},
	)
	reader := &fakeSnapshotReader{
		readAt: time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC),
		results: map[string]QueryResult{
			"AS task_id":    tasks,
			"COUNT(*) AS n": countResult("7"),
		},
	}
	out, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 2})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	ts := out.Collections.Tasks
	if ts.Returned != 2 || ts.AuthorizedTotal != 7 || ts.Limit != 2 {
		t.Fatalf("tasks collection state = %+v, want returned=2 authorized_total=7 limit=2", ts)
	}
	if !ts.Capped {
		t.Fatalf("tasks returned 2 of an authorized total of 7 at limit 2 — must report capped=true")
	}
	var cursor []string
	if err := json.Unmarshal([]byte(ts.NextCursor), &cursor); err != nil {
		t.Fatalf("tasks next_cursor %q is not a JSON array: %v", ts.NextCursor, err)
	}
	// The cursor must carry the FULL ORDER BY tuple (pillar, status, id). A
	// cursor missing a leading term cannot resume a multi-column order.
	want := []string{"3", "in_progress", "T-2"}
	if len(cursor) != len(want) {
		t.Fatalf("tasks next_cursor = %v, want the full ordering tuple %v", cursor, want)
	}
	for i := range want {
		if cursor[i] != want[i] {
			t.Fatalf("tasks next_cursor = %v, want %v", cursor, want)
		}
	}

	// Dependencies returned zero rows against a total of 7 — not capped,
	// because nothing was truncated by the limit; and no cursor, because there
	// is no last row to resume from.
	dep := out.Collections.Dependencies
	if dep.Returned != 0 || dep.Capped || dep.NextCursor != "" {
		t.Fatalf("dependencies collection state = %+v, want returned=0 capped=false and no cursor", dep)
	}
}

// TestProjectSnapshotContentHashesAreDeterministicAndSensitive: identical
// content at two different read timestamps must hash identically (that is what
// lets a consumer skip a re-render), and any content change must move the hash.
func TestProjectSnapshotContentHashesAreDeterministicAndSensitive(t *testing.T) {
	build := func(taskID string, readAt time.Time) ProjectSnapshotResult {
		reader := &fakeSnapshotReader{
			readAt: readAt,
			results: map[string]QueryResult{
				"AS task_id":    rowsResult(map[string]interface{}{"task_id": taskID, "pillar": "3", "status": "done"}),
				"COUNT(*) AS n": countResult("1"),
			},
		}
		out, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{})
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		return out
	}

	a := build("T-1", time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC))
	b := build("T-1", time.Date(2026, 8, 17, 9, 45, 0, 0, time.UTC))
	c := build("T-2", time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC))

	if a.ContentHashes.Snapshot != b.ContentHashes.Snapshot {
		t.Fatalf("identical content at two read timestamps hashed differently (%s vs %s) — the hash must answer 'did the content change', not 'was this read again'",
			a.ContentHashes.Snapshot, b.ContentHashes.Snapshot)
	}
	if a.Revision == b.Revision {
		t.Fatalf("two different read timestamps produced the same revision %q", a.Revision)
	}
	if a.ContentHashes.Snapshot == c.ContentHashes.Snapshot {
		t.Fatalf("changing a task id did not move the snapshot hash (%s)", a.ContentHashes.Snapshot)
	}
	if a.ContentHashes.Tasks == c.ContentHashes.Tasks {
		t.Fatalf("changing a task id did not move the tasks hash (%s)", a.ContentHashes.Tasks)
	}
	if a.ContentHashes.Worklinks != c.ContentHashes.Worklinks {
		t.Fatalf("changing a TASK moved the WORKLINKS hash — the per-collection hashes must be independent")
	}
}

// TestProjectSnapshotCountsCarryTheCanonicalStatusFold: the snapshot's counts
// go through the same task_statuses fold as task_graph_task_counts, and the
// fold is inside the counts content hash, so a registry that places a literal
// differently publishes a different counts hash over the same rows. The
// shipped registry absorbs no legacy spelling any more, so a completed row is
// unrecognized under it and done under a registry that still absorbs it.
func TestProjectSnapshotCountsCarryTheCanonicalStatusFold(t *testing.T) {
	groupSQL, _ := nativeTaskCountsGroupQuery(snapshotTestAdapter(), "arbor")
	build := func(reg TaskStatusRegistry) ProjectSnapshotResult {
		reader := &fakeSnapshotReader{
			readAt: time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC),
			results: map[string]QueryResult{
				"COUNT(*) AS n": countResult("0"),
				groupSQL:        rowsResult(map[string]interface{}{"pillar": "ASSESS", "status": "completed", "n": "1"}),
			},
		}
		env := snapshotTestEnv()
		env.TaskStatuses = reg
		out, err := nativeHandleProjectSnapshot(context.Background(), env, snapshotTestAdapter(), reader, projectSnapshotInput{})
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		return out
	}

	shipped := build(realConfigTaskStatusRegistry(t))
	got := shipped.Counts.ByPillarCanonicalStatus
	if len(got) != 1 || got[0].Pillar != "ASSESS" || got[0].CanonicalStatus != unrecognizedTaskStatus || got[0].Terminal || got[0].Count != 1 {
		t.Fatalf("snapshot canonical counts = %+v, want one non-terminal ASSESS/%s group of 1", got, unrecognizedTaskStatus)
	}

	legacy, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done", "terminal": true, "absorbs": []interface{}{"done", "completed"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "archived", "terminal": true}),
	})
	if err != nil {
		t.Fatalf("build legacy registry: %v", err)
	}
	moved := build(legacy)
	if len(moved.Counts.ByPillarCanonicalStatus) != 1 || moved.Counts.ByPillarCanonicalStatus[0].CanonicalStatus != "done" {
		t.Fatalf("legacy-registry snapshot canonical counts = %+v, want completed under done", moved.Counts.ByPillarCanonicalStatus)
	}
	if shipped.ContentHashes.Counts == moved.ContentHashes.Counts {
		t.Fatalf("the canonical fold moved completed from unrecognized to done but the counts hash %s did not change", shipped.ContentHashes.Counts)
	}
}

// ---------------- unique-total-order guards ----------------

// TestSnapshotCollectionOrderingIsAUniqueTotalOrder guards the cursor
// soundness precondition. TaskLinks.created_at carries no uniqueness
// guarantee and (task_id, depends_on_task_id) is not the TaskDeps key, so
// ordering by those alone lets ties reorder between reads — which would make
// both the content hash and the cursor unsound. P3-GT-PAGINATION resumes from
// these cursors, so this is load-bearing for the lane this one gates.
func TestSnapshotCollectionOrderingIsAUniqueTotalOrder(t *testing.T) {
	adapter := snapshotTestAdapter()

	depSQL, _ := nativeSnapshotDependenciesQuery(adapter, "arbor", 10)
	if !strings.Contains(depSQL, "ORDER BY task_id, depends_on_task_id, dep_id") {
		t.Fatalf("dependencies ORDER BY is not a unique total order — dep_id (the key part) must be the final tie-break.\nSQL: %s", depSQL)
	}

	linkSQL, _ := nativeSnapshotWorklinksQuery(adapter, "arbor", 10)
	if !strings.Contains(linkSQL, "ORDER BY created_at, link_id") {
		t.Fatalf("worklinks ORDER BY is not a unique total order — created_at is not unique, link_id must be the final tie-break.\nSQL: %s", linkSQL)
	}

	taskSQL, _ := nativeTasksQuery(adapter, "arbor", tasksInput{}, 10)
	if !strings.Contains(taskSQL, "ORDER BY pillar, status, id") {
		t.Fatalf("tasks ORDER BY is not a unique total order — id (the key part) must be the final tie-break.\nSQL: %s", taskSQL)
	}
}

// TestSnapshotTotalsRespectTheSameFiltersAsTheRows: an authorized total counted
// without the archived-row filters would exceed the rows for a reason that is
// not the cap, and every uncapped snapshot would falsely report capped=true.
func TestSnapshotTotalsRespectTheSameFiltersAsTheRows(t *testing.T) {
	adapter := snapshotTestAdapter()

	depTotal, _ := nativeSnapshotTotalsQuery(adapter, "arbor", adapter.Descriptor.DepTable, `COALESCE(kind, 'gates') != 'archived'`)
	if !strings.Contains(depTotal, "COALESCE(kind, 'gates') != 'archived'") {
		t.Fatalf("dependencies total omits the archived filter the row query applies.\nSQL: %s", depTotal)
	}
	linkTotal, _ := nativeSnapshotTotalsQuery(adapter, "arbor", adapter.Descriptor.WorklinkTable, `COALESCE(artifact_status, 'recorded') != 'archived'`)
	if !strings.Contains(linkTotal, "COALESCE(artifact_status, 'recorded') != 'archived'") {
		t.Fatalf("worklinks total omits the archived filter the row query applies.\nSQL: %s", linkTotal)
	}
}

// TestSnapshotSQLBuildersAreSharedWithThePerVerbPath: the snapshot's tasks and
// counts statements must be the SAME builders task_graph_tasks and
// task_graph_task_counts use, or the two surfaces drift silently on the next
// filter edit and a dashboard disagrees with a list call over identical data.
func TestSnapshotSQLBuildersAreSharedWithThePerVerbPath(t *testing.T) {
	adapter := snapshotTestAdapter()
	reader := &fakeSnapshotReader{
		readAt:  time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC),
		results: map[string]QueryResult{"COUNT(*) AS n": countResult("0")},
	}
	if _, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), adapter, reader, projectSnapshotInput{Limit: 5000}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	issued := reader.transactions[0]

	wantTaskSQL, _ := nativeTasksQuery(adapter, "arbor", tasksInput{}, 5000)
	wantGroupSQL, _ := nativeTaskCountsGroupQuery(adapter, "arbor")
	wantGapSQL, _ := nativeTaskCountsGapQuery(adapter, "arbor")
	for _, want := range []string{wantTaskSQL, wantGroupSQL, wantGapSQL} {
		found := false
		for _, got := range issued {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("snapshot did not issue the shared builder statement — it has its own inlined copy, which will drift.\nwant: %s\nissued: %v", want, issued)
		}
	}
}

// TestSnapshotRejectsAnOversizedLimit keeps the cap bounded; an unbounded
// snapshot is how a "coherent" read turns into a transaction that never
// finishes.
func TestSnapshotRejectsAnOversizedLimit(t *testing.T) {
	reader := &fakeSnapshotReader{readAt: time.Now()}
	_, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{Limit: 10001})
	if err == nil {
		t.Fatal("limit 10001 was accepted; want rejection")
	}
	if len(reader.transactions) != 0 {
		t.Fatalf("limit was validated AFTER opening a transaction (%d opened); reject before reading", len(reader.transactions))
	}
}

func TestScopedExecutionSnapshotIsCoherentAndStructurallyBounded(t *testing.T) {
	reader := &scriptedSnapshotReader{
		readAt: time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC),
	}
	reader.query = func(sql string, params map[string]interface{}) (QueryResult, error) {
		switch {
		case strings.Contains(sql, "id = @root_task_id"):
			return rowsResult(map[string]interface{}{"id": "EXECUTION"}), nil
		case strings.Contains(sql, "parent_id IN UNNEST"):
			parents, _ := params["parent_ids"].([]string)
			if len(parents) == 1 && parents[0] == "EXECUTION" {
				return rowsResult(map[string]interface{}{"id": "LANE"}), nil
			}
			return QueryResult{}, nil
		case strings.Contains(sql, "SELECT id FROM Tasks"):
			return rowsResult(map[string]interface{}{"id": "EXECUTION"}, map[string]interface{}{"id": "LANE"}), nil
		case strings.Contains(sql, "id AS task_id"):
			return rowsResult(
				map[string]interface{}{"project_id": "arbor", "task_id": "EXECUTION", "title": "Execution board", "pillar": "MVP", "status": "in_progress", "owner": "Conductor"},
				map[string]interface{}{"project_id": "arbor", "task_id": "LANE", "title": "Lane", "pillar": "MVP-EXECUTION", "status": "not_started", "owner": "LANE-X", "parent_id": "EXECUTION"},
			), nil
		case strings.Contains(sql, "COUNT(*) AS n") && (strings.Contains(sql, "TaskDeps") || strings.Contains(sql, "TaskLinks")):
			return countResult("0"), nil
		case strings.Contains(sql, "COUNT(*) AS n"):
			return countResult("2"), nil
		default:
			return QueryResult{}, nil
		}
	}

	out, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{
		Scope: snapshotScopeExecutionSubtree, RootTaskID: "EXECUTION", Limit: 50,
	})
	if err != nil {
		t.Fatalf("scoped snapshot: %v", err)
	}
	if reader.transactions != 1 {
		t.Fatalf("scoped snapshot opened %d transactions, want 1", reader.transactions)
	}
	if out.Scope.Mode != snapshotScopeExecutionSubtree || out.Scope.RootTaskID != "EXECUTION" || !out.Scope.Complete || out.Scope.Capped {
		t.Fatalf("scope metadata = %+v, want complete uncapped execution subtree", out.Scope)
	}
	if out.Scope.MembershipCount != 2 || len(out.Tasks) != 2 {
		t.Fatalf("scope membership/tasks = %d/%d, want 2/2", out.Scope.MembershipCount, len(out.Tasks))
	}
	for _, statement := range reader.statements {
		if strings.Contains(statement, "FROM Tasks") || strings.Contains(statement, "FROM TaskDeps") || strings.Contains(statement, "FROM TaskLinks") {
			if !strings.Contains(statement, "scope_task_ids") && !strings.Contains(statement, "parent_id IN UNNEST") && !strings.Contains(statement, "id = @root_task_id") {
				t.Fatalf("scoped statement lacks server-side scope predicate: %s", statement)
			}
		}
	}
}

func TestScopedAcceptanceSnapshotReportsMissingDependencyInOneTransaction(t *testing.T) {
	reader := &scriptedSnapshotReader{
		readAt: time.Date(2026, 8, 17, 8, 30, 0, 0, time.UTC),
	}
	reader.query = func(sql string, params map[string]interface{}) (QueryResult, error) {
		switch {
		case strings.Contains(sql, "id = @root_task_id"):
			return rowsResult(map[string]interface{}{"id": "MVP"}), nil
		case strings.Contains(sql, "parent_id IN UNNEST"):
			return rowsResult(map[string]interface{}{"id": "GATE"}), nil
		case strings.Contains(sql, "scope_frontier"):
			frontier, _ := params["scope_frontier"].([]string)
			if len(frontier) == 1 && frontier[0] == "GATE" {
				return rowsResult(map[string]interface{}{"dep_id": "DEP-1", "task_id": "GATE", "depends_on_task_id": "MISSING"}), nil
			}
			return QueryResult{}, nil
		case strings.Contains(sql, "SELECT id FROM Tasks"):
			return rowsResult(map[string]interface{}{"id": "MVP"}, map[string]interface{}{"id": "GATE"}), nil
		case strings.Contains(sql, "id AS task_id"):
			return rowsResult(
				map[string]interface{}{"project_id": "arbor", "task_id": "MVP", "title": "MVP", "pillar": "MVP", "status": "in_progress", "owner": "Conductor"},
				map[string]interface{}{"project_id": "arbor", "task_id": "GATE", "title": "Gate", "pillar": "MVP", "status": "blocked", "owner": "Conductor", "parent_id": "MVP"},
			), nil
		case strings.Contains(sql, "AS dependency_id"):
			return rowsResult(map[string]interface{}{"project_id": "arbor", "dependency_id": "DEP-1", "task_id": "GATE", "depends_on_task_id": "MISSING", "kind": "gates"}), nil
		case strings.Contains(sql, "COUNT(*) AS n") && strings.Contains(sql, "TaskLinks"):
			return countResult("0"), nil
		case strings.Contains(sql, "COUNT(*) AS n") && strings.Contains(sql, "TaskDeps"):
			return countResult("1"), nil
		case strings.Contains(sql, "COUNT(*) AS n"):
			return countResult("2"), nil
		default:
			return QueryResult{}, nil
		}
	}

	out, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{
		Scope: snapshotScopeAcceptanceDependencyClosure, RootTaskID: "MVP", Limit: 50,
	})
	if err != nil {
		t.Fatalf("acceptance snapshot: %v", err)
	}
	if reader.transactions != 1 {
		t.Fatalf("acceptance snapshot opened %d transactions, want 1", reader.transactions)
	}
	if out.Scope.Mode != snapshotScopeAcceptanceDependencyClosure || out.Scope.Complete {
		t.Fatalf("scope metadata = %+v, want incomplete acceptance closure", out.Scope)
	}
	if len(out.Scope.MissingTaskIDs) != 1 || out.Scope.MissingTaskIDs[0] != "MISSING" {
		t.Fatalf("missing_task_ids = %v, want [MISSING]", out.Scope.MissingTaskIDs)
	}
	if len(out.Dependencies) != 1 || out.Dependencies[0].DependsOnTaskID != "MISSING" {
		t.Fatalf("dependencies = %+v, want the unresolved dependency edge preserved", out.Dependencies)
	}
}

func TestSnapshotScopeValidationRejectsAmbiguousInputsBeforeReading(t *testing.T) {
	reader := &scriptedSnapshotReader{readAt: time.Now()}
	_, err := nativeHandleProjectSnapshot(context.Background(), snapshotTestEnv(), snapshotTestAdapter(), reader, projectSnapshotInput{
		RootTaskID: "ROOT",
	})
	if err == nil || !strings.Contains(err.Error(), "requires a scoped snapshot mode") {
		t.Fatalf("ambiguous root-only snapshot error = %v, want explicit scope validation", err)
	}
	if reader.transactions != 0 {
		t.Fatalf("ambiguous scope opened %d transactions, want 0", reader.transactions)
	}
}

// ---------------- pure helper guards ----------------

func TestEncodeCursorSurvivesSeparatorCharactersInValues(t *testing.T) {
	// A delimiter-joined cursor would be ambiguous for these values. The
	// canonical-JSON encoding must round-trip them exactly.
	parts := []string{`a|b`, `c","d`, `e,f`}
	var got []string
	if err := json.Unmarshal([]byte(encodeCursor(parts)), &got); err != nil {
		t.Fatalf("cursor did not round-trip: %v", err)
	}
	if len(got) != len(parts) {
		t.Fatalf("cursor round-tripped to %v, want %v", got, parts)
	}
	for i := range parts {
		if got[i] != parts[i] {
			t.Fatalf("cursor round-tripped to %v, want %v", got, parts)
		}
	}
}

func TestNewCollectionStateDoesNotReportCappedWhenTheCollectionIsComplete(t *testing.T) {
	// Returned == Limit but the total is exactly that many: nothing was
	// truncated, so publishing capped=true would send a consumer after a page
	// that does not exist.
	st := newCollectionState(5, 5, 5, `["x"]`)
	if st.Capped {
		t.Fatalf("returned=5 total=5 limit=5 reported capped=true; nothing was truncated")
	}
	if st.NextCursor != "" {
		t.Fatalf("an uncapped collection published cursor %q", st.NextCursor)
	}
}
