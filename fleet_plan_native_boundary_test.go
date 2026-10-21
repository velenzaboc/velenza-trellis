package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

func TestNativeProjectionRowDecodersPreserveWireSemantics(t *testing.T) {
	tasks := nativeTaskRowsFrom(QueryResult{Rows: []map[string]interface{}{{
		"project_id": "p", "task_id": "T-1", "title": "title", "pillar": "trust", "level": "L2",
		"status": "open", "owner": "owner", "parent_id": "P-1", "is_gap": "true", "is_critical_path": true,
		"gap_label": "gap", "note": "note", "created_at": "2026-09-08T00:00:00Z", "updated_at": "2026-09-09T00:00:00Z", "session_id": "sess", "repo": "task-graph",
	}}})
	if len(tasks) != 1 || tasks[0].TaskID != "T-1" || !tasks[0].IsGap || !tasks[0].IsCriticalPath || tasks[0].SessionID != "sess" || tasks[0].Repo != "task-graph" || tasks[0].CreatedAt != "2026-09-08T00:00:00Z" {
		t.Fatalf("task projection lost stored fields: %#v", tasks)
	}

	deps := nativeDependencyRowsFrom(QueryResult{Rows: []map[string]interface{}{{
		"project_id": "p", "dependency_id": "D-1", "task_id": "T-1", "depends_on_task_id": "T-0",
		"kind": "gates", "note": "blocks", "created_at": "2026-09-09T00:00:00Z",
	}}})
	if len(deps) != 1 || deps[0].DependencyID != "D-1" || deps[0].DependsOnTaskID != "T-0" {
		t.Fatalf("dependency projection lost stored fields: %#v", deps)
	}

	links := nativeSnapshotWorklinkRowsFrom(QueryResult{Rows: []map[string]interface{}{{
		"project_id": "p", "artifact_id": "L-1", "task_id": "T-1", "artifact_type": "pull_request",
		"artifact_ref": "https://example.test/pr/1", "thread": "thread-1", "note": "ready", "created_at": "2026-09-09T00:00:00Z", "session_id": "sess", "repo": "task-graph", "branch": "main", "commit_sha": "abc", "pr_number": "42", "pr_state": "open", "worktree_path": "C:/wt", "url": "https://example.test/pr/1",
	}}})
	if len(links) != 1 || links[0].ArtifactID != "L-1" || links[0].ArtifactRef == "" || links[0].SessionID != "sess" || links[0].PRNumber != 42 || links[0].Branch != "main" || links[0].WorktreePath == "" {
		t.Fatalf("worklink projection lost stored fields: %#v", links)
	}
}

func TestNativeSnapshotTaskQueryCarriesCreatedAtInsideCoherentRead(t *testing.T) {
	adapter := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{TaskTable: "Tasks"}}
	sql, _ := nativeSnapshotTasksPageQueryScoped(adapter, "arbor", tasksInput{}, 10, []string{"T-1"}, nil)
	if !strings.Contains(sql, "CAST(created_at AS STRING) AS created_at") {
		t.Fatalf("snapshot task query omits created_at: %s", sql)
	}
}

func TestNativeScalarBoundariesRemainLosslessOrFailClosed(t *testing.T) {
	if got := boolField(map[string]interface{}{"b": "true"}, "b"); !got {
		t.Fatal("string true must decode as true")
	}
	for _, value := range []interface{}{nil, "false", 1} {
		if boolField(map[string]interface{}{"b": value}, "b") {
			t.Fatalf("non-boolean value %#v must fail closed", value)
		}
	}
	if got := intField(map[string]interface{}{"n": "9223372036854775807"}, "n"); got == 0 {
		t.Fatal("valid Spanner INT64 wire value must decode")
	}
	if got := intField(map[string]interface{}{"n": "not-a-number"}, "n"); got != 0 {
		t.Fatalf("invalid integer wire value = %d, want zero", got)
	}
	if got := stringField(map[string]interface{}{"s": nil}, "s"); got != "" {
		t.Fatalf("NULL string = %q, want empty", got)
	}

	for _, tc := range []struct {
		in   interface{}
		want interface{}
	}{
		{int(7), int64(7)}, {int64(8), int64(8)}, {float64(9.8), int64(9)}, {"10", int64(10)},
	} {
		if got := int64FromAny(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("int64FromAny(%#v) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
	if got := int64FromAny("x"); got != (spanner.NullInt64{Valid: false}) {
		t.Fatalf("invalid integer must become SQL NULL, got %#v", got)
	}
	for _, tc := range []struct {
		value interface{}
		want  int
	}{
		{nil, 0}, {int(5), 5}, {int64(6), 6}, {float64(7.9), 7}, {"8", 8}, {"bad", 0},
	} {
		if got := intFromMap(map[string]interface{}{"n": tc.value}, "n"); got != tc.want {
			t.Fatalf("intFromMap(%#v) = %d, want %d", tc.value, got, tc.want)
		}
	}
	if got := intFromMap(nil, "n"); got != 0 {
		t.Fatalf("nil map = %d, want zero", got)
	}
}

func TestNativeNullableWriteValuesAndTimeParsing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input map[string]interface{}
		want  spanner.NullBool
	}{
		{"nil map", nil, spanner.NullBool{Valid: false}},
		{"absent", map[string]interface{}{}, spanner.NullBool{Valid: false}},
		{"bool", map[string]interface{}{"v": true}, spanner.NullBool{Bool: true, Valid: true}},
		{"string", map[string]interface{}{"v": "false"}, spanner.NullBool{Bool: false, Valid: true}},
		{"invalid", map[string]interface{}{"v": 3}, spanner.NullBool{Valid: false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nullBoolFromMap(tc.input, "v"); got != tc.want {
				t.Fatalf("nullBoolFromMap = %#v, want %#v", got, tc.want)
			}
		})
	}
	if got := nullInt64FromMap(map[string]interface{}{"n": "42"}, "n"); got != int64(42) {
		t.Fatalf("valid nullable integer = %#v", got)
	}
	if got := nullInt64FromMap(map[string]interface{}{"n": "bad"}, "n"); got != (spanner.NullInt64{Valid: false}) {
		t.Fatalf("invalid nullable integer = %#v", got)
	}
	if got := nativeParam(nil); got != (spanner.NullString{Valid: false}) {
		t.Fatalf("nil param = %#v", got)
	}
	if got := nativeParam(3); got != 3 {
		t.Fatalf("non-string param changed: %#v", got)
	}
	if got := nativeParam(""); got != (spanner.NullString{Valid: false}) {
		t.Fatalf("empty string param = %#v", got)
	}

	want := time.Date(2026, 9, 9, 1, 2, 3, 4, time.UTC)
	if got := parseRFC3339(want.Format(time.RFC3339Nano)); !got.Equal(want) {
		t.Fatalf("RFC3339 timestamp = %v, want %v", got, want)
	}
	if !parseRFC3339("2026-09-09 01:02:03.000000004+00:00").Equal(want) {
		t.Fatal("Spanner timestamp representation must parse")
	}
	for _, in := range []string{"", "not-a-time"} {
		if !parseRFC3339(in).IsZero() {
			t.Fatalf("invalid timestamp %q must fail closed", in)
		}
	}
}

func TestNativeTaskCountsAndQueryBuildersRespectProjectScope(t *testing.T) {
	adapter := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{
		TaskTable: "Tasks", WorklinkTable: "TaskLinks", DepTable: "TaskDeps", ProjectColumn: "project_id",
	}}
	counts, countsErr := nativeTaskCountsFrom(context.Background(), nil, QueryResult{Rows: []map[string]interface{}{
		{"pillar": "trust", "status": "open", "n": "2"}, {"pillar": "trust", "status": "done", "n": float64(3)},
	}}, QueryResult{Rows: []map[string]interface{}{{"gap_task_count": "4", "distinct_gap_count": "2"}}})
	if countsErr != nil {
		t.Fatalf("counts decode: %v", countsErr)
	}
	if len(counts.ByPillarStatus) != 2 || counts.ByPillarStatus[1].Count != 3 || counts.GapTaskCount != 4 || counts.DistinctGapCount != 2 {
		t.Fatalf("counts decode = %#v", counts)
	}
	if len(counts.ByPillarCanonicalStatus) != 1 || counts.ByPillarCanonicalStatus[0].Count != 5 || len(counts.UnrecognizedStatusLiterals) != 2 {
		t.Fatalf("with no registry every row must count as unrecognized, never be dropped: %#v", counts)
	}
	if empty, emptyErr := nativeTaskCountsFrom(context.Background(), nil, QueryResult{}, QueryResult{}); emptyErr != nil || len(empty.ByPillarStatus) != 0 || empty.GapTaskCount != 0 || len(empty.ByPillarCanonicalStatus) != 0 {
		t.Fatalf("empty results must remain empty: %#v", empty)
	}
	groupSQL, groupParams := nativeTaskCountsGroupQuery(adapter, "project-a")
	gapSQL, gapParams := nativeTaskCountsGapQuery(adapter, "project-a")
	totalSQL, totalParams := nativeSnapshotTotalsQuery(adapter, "project-a", "Tasks", "status != 'archived'")
	for _, query := range []string{groupSQL, gapSQL, totalSQL} {
		if !containsAll(query, "project_id", "@project_id") {
			t.Fatalf("query must constrain the configured project namespace: %s", query)
		}
	}
	for _, params := range []map[string]interface{}{groupParams, gapParams, totalParams} {
		if params["project_id"] != "project-a" {
			t.Fatalf("project parameter lost: %#v", params)
		}
	}
}

func TestNativeProjectColumnsAndArtifactProjectionRemainTenantScoped(t *testing.T) {
	postCutover := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ProjectColumn: "project_id"}}
	if got := postCutover.projectSelectExpr(); got != "project_id AS project_id" {
		t.Fatalf("project select expression = %q", got)
	}
	if got := postCutover.projectPredicate(); got != "project_id = @project_id" {
		t.Fatalf("project predicate = %q", got)
	}
	if got := postCutover.projectWhereAnd(); got != "project_id = @project_id AND " {
		t.Fatalf("project where prefix = %q", got)
	}
	if got := postCutover.projectInsertColumns(); got != "project_id, " {
		t.Fatalf("project insert columns = %q", got)
	}
	if got := postCutover.projectInsertValues(); got != "@project_id, " {
		t.Fatalf("project insert values = %q", got)
	}
	preCutover := &SpannerSubstrateAdapter{}
	if preCutover.projectPredicate() != "TRUE" || preCutover.projectWhereAnd() != "" || preCutover.projectInsertColumns() != "" || preCutover.projectInsertValues() != "" {
		t.Fatalf("pre-cutover adapter must not emit a nonexistent project column")
	}

	artifact := nativeArtifactNodeFromRow(map[string]interface{}{
		"link_id": "L-1", "project_id": "project-a", "task_id": "T-1", "artifact_type": "pull_request", "artifact_ref": "pr/1",
		"created_at": "2026-09-09T01:02:03Z", "pr_number": "19", "artifact_status": "archived", "commit_sha": "abc",
	})
	if artifact.NodeID != "L-1" || artifact.ProjectID != "project-a" || artifact.Status != "archived" {
		t.Fatalf("artifact identity/status projection = %#v", artifact)
	}
	if artifact.CurrentVersion.Contract["project_id"] != "project-a" || artifact.CurrentVersion.Payload["pr_number"] != 19 {
		t.Fatalf("artifact tenant/provenance projection = %#v", artifact.CurrentVersion)
	}
	if edge := nativeEdge("project-a", "E-1", "gates", "T-1", "T-0", "2026-09-09T01:02:03Z"); edge.ProjectID != "project-a" || edge.Status != "active" || edge.ReciprocalEdgeID != "" {
		t.Fatalf("native edge projection = %#v", edge)
	}
	if edge := nativeReciprocalEdge("project-a", "E-1", "gates", "T-1", "T-0", "bad"); edge.ReciprocalEdgeID != "E-1" || !edge.AdmittedAt.IsZero() {
		t.Fatalf("reciprocal edge projection = %#v", edge)
	}
}

func TestNativeMutationReceiptUsesConfiguredActorOrClosedDefault(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
	defaultReceipt := nativeMutationReceipt(&Env{}, "project-a", "archive_worklink", "artifact", "L-1", "TaskLinks", 1, now)
	if defaultReceipt.MutatedBy != "task-plan-native" || defaultReceipt.ProjectID != "project-a" || defaultReceipt.RowsAffected != 1 {
		t.Fatalf("default receipt = %#v", defaultReceipt)
	}
	configuredReceipt := nativeMutationReceipt(&Env{Config: SpecGraphConfig{ExperimentalAdmitter: "operator"}}, "project-a", "create", "task", "T-1", "Tasks", 2, now)
	if configuredReceipt.MutatedBy != "operator" || configuredReceipt.Operation != "create" || !configuredReceipt.MutatedAt.Equal(now) {
		t.Fatalf("configured receipt = %#v", configuredReceipt)
	}
}

func containsAll(s string, values ...string) bool {
	for _, value := range values {
		if !strings.Contains(s, value) {
			return false
		}
	}
	return true
}
