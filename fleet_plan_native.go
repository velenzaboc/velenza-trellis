package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

const fleetPlanNativeStoreType = "fleet_plan_native"

const (
	snapshotScopeProject                     = "project"
	snapshotScopeExecutionSubtree            = "execution_subtree"
	snapshotScopeAcceptanceDependencyClosure = "acceptance_dependency_closure"
)

var nativeDependencyNamespace = uuid.MustParse("74b0ebf8-7aa5-5f87-8c5d-cac027e52721")

func (a *SpannerSubstrateAdapter) isFleetPlanNative() bool {
	return a.Descriptor.TaskStoreType == fleetPlanNativeStoreType
}

func projectIDOrDefault(env *Env, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if env != nil && env.Config.DefaultProjectID != "" {
		return env.Config.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project_id required (or configure default_project_id)")
}

func resolveNativeProjectID(env *Env, a *SpannerSubstrateAdapter, explicit string) (string, error) {
	projectID, err := projectIDOrDefault(env, explicit)
	if err != nil {
		return "", err
	}
	if a == nil || a.Descriptor.ProjectColumn != "" {
		return projectID, nil
	}
	defaultProject := ""
	if env != nil {
		defaultProject = env.Config.DefaultProjectID
	}
	if projectID != defaultProject {
		return "", fmt.Errorf("project_id %q requires task-plan project namespace cutover; current runtime config has no project_column and only supports default project_id %q", projectID, defaultProject)
	}
	return projectID, nil
}

func (a *SpannerSubstrateAdapter) hasProjectColumn() bool {
	return a != nil && a.Descriptor.ProjectColumn != ""
}

func (a *SpannerSubstrateAdapter) projectSelectExpr() string {
	if a.hasProjectColumn() {
		return a.Descriptor.ProjectColumn + ` AS project_id`
	}
	return `@project_id AS project_id`
}

func (a *SpannerSubstrateAdapter) projectPredicate() string {
	if a.hasProjectColumn() {
		return a.Descriptor.ProjectColumn + ` = @project_id`
	}
	return `TRUE`
}

func (a *SpannerSubstrateAdapter) projectWhereAnd() string {
	if a.hasProjectColumn() {
		return a.Descriptor.ProjectColumn + ` = @project_id AND `
	}
	return ``
}

func (a *SpannerSubstrateAdapter) projectInsertColumns() string {
	if a.hasProjectColumn() {
		return a.Descriptor.ProjectColumn + `, `
	}
	return ``
}

func (a *SpannerSubstrateAdapter) projectInsertValues() string {
	if a.hasProjectColumn() {
		return `@project_id, `
	}
	return ``
}

func soleNativeFleetPlanAdapter(ctx context.Context, env *Env) (*SpannerSubstrateAdapter, error) {
	all, err := env.Substrates.All(ctx)
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no substrates registered")
	}
	a, ok := all[0].(*SpannerSubstrateAdapter)
	if !ok {
		return nil, fmt.Errorf("task_graph native task-plan requires SpannerSubstrateAdapter")
	}
	if !a.isFleetPlanNative() {
		return nil, nil
	}
	return a, nil
}

func (a *SpannerSubstrateAdapter) checkFleetPlanNativeSchema(ctx context.Context) error {
	client, err := a.getClient(ctx)
	if err != nil {
		return err
	}
	want := []string{a.Descriptor.TaskTable, a.Descriptor.WorklinkTable, a.Descriptor.DepTable}
	stmt := spanner.Statement{
		SQL: `SELECT table_name FROM information_schema.tables
			WHERE table_schema = '' AND table_name IN UNNEST(@names)`,
		Params: map[string]interface{}{"names": want},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	found := map[string]bool{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("task-plan schema query: %w", err)
		}
		var name string
		if err := row.Columns(&name); err != nil {
			return fmt.Errorf("task-plan schema decode: %w", err)
		}
		found[name] = true
	}
	for _, table := range want {
		if !found[table] {
			return fmt.Errorf("%w: missing task-plan table %q", ErrSchemaNotApplied, table)
		}
	}
	if !a.hasProjectColumn() {
		return nil
	}
	projectCol := a.Descriptor.ProjectColumn
	colStmt := spanner.Statement{
		SQL: `SELECT table_name, column_name FROM information_schema.columns
			WHERE table_schema = '' AND table_name IN UNNEST(@tables) AND column_name = @project_col`,
		Params: map[string]interface{}{"tables": want, "project_col": projectCol},
	}
	colIter := client.Single().Query(ctx, colStmt)
	defer colIter.Stop()
	colFound := map[string]bool{}
	for {
		row, err := colIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("task-plan project column query: %w", err)
		}
		var tableName, columnName string
		if err := row.Columns(&tableName, &columnName); err != nil {
			return fmt.Errorf("task-plan project column decode: %w", err)
		}
		if columnName == projectCol {
			colFound[tableName] = true
		}
	}
	for _, table := range want {
		if !colFound[table] {
			return fmt.Errorf("%w: missing task-plan project column %q on table %q", ErrSchemaNotApplied, projectCol, table)
		}
	}
	return nil
}

// nativeTasksQuery builds the task-listing SQL + params. Extracted from
// nativeHandleTasks so the task_graph_tasks verb and the project-snapshot
// projection issue the IDENTICAL statement — inlining a second copy in the
// snapshot path is how the two silently drift on the next filter edit.
//
// ORDER BY pillar, status, id is a UNIQUE TOTAL ORDER because id is the table's
// own key part; that is what makes tasks_cursor resumable.
func nativeTasksQuery(adapter *SpannerSubstrateAdapter, projectID string, in tasksInput, limit int) (string, map[string]interface{}) {
	return nativeTasksQueryScoped(adapter, projectID, in, limit, nil)
}

func nativeTasksQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, in tasksInput, limit int, scopeTaskIDs []string) (string, map[string]interface{}) {
	return nativeSnapshotTasksPageQueryScoped(adapter, projectID, in, limit, scopeTaskIDs, nil)
}

// nativeSnapshotTasksPageQuery adds a strict lexicographic seek predicate to
// the task collection's unique ordering tuple. Offset pagination is forbidden:
// it becomes both expensive and gap-prone as the collection grows.
func nativeSnapshotTasksPageQuery(adapter *SpannerSubstrateAdapter, projectID string, in tasksInput, limit int, after []string) (string, map[string]interface{}) {
	return nativeSnapshotTasksPageQueryScoped(adapter, projectID, in, limit, nil, after)
}

func nativeSnapshotTasksPageQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, in tasksInput, limit int, scopeTaskIDs, after []string) (string, map[string]interface{}) {
	sql := `SELECT ` + adapter.projectSelectExpr() + `, id AS task_id, title, pillar, layer AS level, status, owner,
		parent_id, is_gap, is_critical_path, gap_label, note, CAST(created_at AS STRING) AS created_at, CAST(updated_at AS STRING) AS updated_at, session_id, repo
		FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectPredicate()
	params := map[string]interface{}{"project_id": projectID}
	sql, params = addSnapshotTaskScope(sql, params, "id", scopeTaskIDs)
	if in.Pillar != "" {
		sql += ` AND pillar = @pillar`
		params["pillar"] = in.Pillar
	}
	if len(in.Statuses) > 0 {
		sql += ` AND status IN UNNEST(@statuses)`
		params["statuses"] = in.Statuses
	}
	if in.Owner != "" {
		sql += ` AND owner = @owner`
		params["owner"] = in.Owner
	}
	if in.IsGap != nil {
		sql += ` AND is_gap = @is_gap`
		params["is_gap"] = *in.IsGap
	}
	if in.GapLabel != "" {
		sql += ` AND gap_label = @gap_label`
		params["gap_label"] = in.GapLabel
	}
	if in.Level != "" {
		sql += ` AND layer = @level`
		params["level"] = in.Level
	}
	if len(after) == 3 {
		sql += ` AND (pillar > @after_pillar OR (pillar = @after_pillar AND (status > @after_status OR (status = @after_status AND id > @after_task_id))))`
		params["after_pillar"] = after[0]
		params["after_status"] = after[1]
		params["after_task_id"] = after[2]
	}
	sql += fmt.Sprintf(` ORDER BY pillar, status, id LIMIT %d`, limit)
	return sql, params
}

// nativeTaskRowsFrom maps a nativeTasksQuery result set to TaskRows. It
// returns a nil slice (not an empty one) for zero rows, preserving
// task_graph_tasks' pre-extraction `"tasks": null` wire shape.
func nativeTaskRowsFrom(res QueryResult) []TaskRow {
	var out []TaskRow
	for _, row := range res.Rows {
		out = append(out, TaskRow{
			ProjectID:      stringField(row, "project_id"),
			TaskID:         stringField(row, "task_id"),
			Title:          stringField(row, "title"),
			Pillar:         stringField(row, "pillar"),
			Level:          stringField(row, "level"),
			Status:         stringField(row, "status"),
			Owner:          stringField(row, "owner"),
			SessionID:      stringField(row, "session_id"),
			Repo:           stringField(row, "repo"),
			ParentID:       stringField(row, "parent_id"),
			IsGap:          boolField(row, "is_gap"),
			IsCriticalPath: boolField(row, "is_critical_path"),
			GapLabel:       stringField(row, "gap_label"),
			Note:           stringField(row, "note"),
			CreatedAt:      stringField(row, "created_at"),
			UpdatedAt:      stringField(row, "updated_at"),
		})
	}
	return out
}

func nativeHandleTasks(ctx context.Context, env *Env, in tasksInput) (TaskListResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return TaskListResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return TaskListResult{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 500
	}
	sql, params := nativeTasksQuery(adapter, projectID, in, limit)
	res, err := adapter.ExecuteReadOnlyQuery(ctx, sql, params)
	if err != nil {
		return TaskListResult{}, fmt.Errorf("task_graph_tasks: %w", err)
	}
	return TaskListResult{Tasks: nativeTaskRowsFrom(res)}, nil
}

// nativeTaskCountsGroupQuery / nativeTaskCountsGapQuery are the two statements
// task_graph_task_counts issues, extracted so the snapshot can run them inside
// its own transaction rather than re-deriving them.
func nativeTaskCountsGroupQuery(adapter *SpannerSubstrateAdapter, projectID string) (string, map[string]interface{}) {
	return nativeTaskCountsGroupQueryScoped(adapter, projectID, nil)
}

func nativeTaskCountsGroupQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, scopeTaskIDs []string) (string, map[string]interface{}) {
	return `SELECT pillar, status, COUNT(*) AS n
		FROM ` + adapter.Descriptor.TaskTable + `
		WHERE ` + adapter.projectPredicate() + snapshotTaskScopePredicate("id", scopeTaskIDs) + `
		GROUP BY pillar, status
		ORDER BY pillar, status`, snapshotTaskScopeParams(projectID, scopeTaskIDs)
}

func nativeTaskCountsGapQuery(adapter *SpannerSubstrateAdapter, projectID string) (string, map[string]interface{}) {
	return nativeTaskCountsGapQueryScoped(adapter, projectID, nil)
}

func nativeTaskCountsGapQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, scopeTaskIDs []string) (string, map[string]interface{}) {
	return `SELECT COUNT(*) AS gap_task_count,
		COUNT(DISTINCT gap_label) AS distinct_gap_count
		FROM ` + adapter.Descriptor.TaskTable + `
		WHERE ` + adapter.projectWhereAnd() + `is_gap = TRUE` + snapshotTaskScopePredicate("id", scopeTaskIDs), snapshotTaskScopeParams(projectID, scopeTaskIDs)
}

// nativeTaskCountsFrom folds the two count result sets into TaskCountsResult,
// including the canonical-status fold over reg.
func nativeTaskCountsFrom(ctx context.Context, reg TaskStatusRegistry, groupRes, gapRes QueryResult) (TaskCountsResult, error) {
	out := TaskCountsResult{}
	for _, row := range groupRes.Rows {
		out.ByPillarStatus = append(out.ByPillarStatus, PillarStatusCount{
			Pillar: stringField(row, "pillar"),
			Status: stringField(row, "status"),
			Count:  intField(row, "n"),
		})
	}
	if len(gapRes.Rows) > 0 {
		out.GapTaskCount = intField(gapRes.Rows[0], "gap_task_count")
		out.DistinctGapCount = intField(gapRes.Rows[0], "distinct_gap_count")
	}
	return withCanonicalStatusCounts(ctx, reg, out)
}

// nativeEnvTaskCounts is the one place the native count surfaces fold through
// the Env's task_statuses registry, so task_graph_task_counts and the project
// snapshot cannot report counts against different registries.
func nativeEnvTaskCounts(ctx context.Context, env *Env, groupRes, gapRes QueryResult) (TaskCountsResult, error) {
	return nativeTaskCountsFrom(ctx, env.TaskStatuses, groupRes, gapRes)
}

func nativeHandleTaskCounts(ctx context.Context, env *Env, explicitProjectID string) (TaskCountsResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return TaskCountsResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, explicitProjectID)
	if err != nil {
		return TaskCountsResult{}, err
	}
	groupSQL, groupParams := nativeTaskCountsGroupQuery(adapter, projectID)
	groupRes, err := adapter.ExecuteReadOnlyQuery(ctx, groupSQL, groupParams)
	if err != nil {
		return TaskCountsResult{}, fmt.Errorf("task_graph_task_counts: group query: %w", err)
	}
	gapSQL, gapParams := nativeTaskCountsGapQuery(adapter, projectID)
	gapRes, err := adapter.ExecuteReadOnlyQuery(ctx, gapSQL, gapParams)
	if err != nil {
		return TaskCountsResult{}, fmt.Errorf("task_graph_task_counts: gap query: %w", err)
	}
	counts, err := nativeEnvTaskCounts(ctx, env, groupRes, gapRes)
	if err != nil {
		return TaskCountsResult{}, fmt.Errorf("task_graph_task_counts: %w", err)
	}
	return counts, nil
}

func nativeHandleWorklinks(ctx context.Context, env *Env, in worklinksInput) (WorklinkListResult, error) {
	if in.TaskID == "" {
		return WorklinkListResult{}, fmt.Errorf("task_id required")
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return WorklinkListResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return WorklinkListResult{}, err
	}
	sql := `SELECT ` + adapter.projectSelectExpr() + `, link_id AS artifact_id, artifact_type, artifact_ref, thread,
		note, CAST(created_at AS STRING) AS created_at
		FROM ` + adapter.Descriptor.WorklinkTable + `
		WHERE ` + adapter.projectWhereAnd() + `task_id = @task_id
		AND COALESCE(artifact_status, 'recorded') != 'archived'
		ORDER BY created_at`
	res, err := adapter.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": projectID, "task_id": in.TaskID})
	if err != nil {
		return WorklinkListResult{}, fmt.Errorf("task_graph_worklinks: %w", err)
	}
	out := WorklinkListResult{}
	for _, row := range res.Rows {
		out.Worklinks = append(out.Worklinks, WorklinkRow{
			ProjectID:    stringField(row, "project_id"),
			ArtifactID:   stringField(row, "artifact_id"),
			TaskID:       in.TaskID,
			ArtifactType: stringField(row, "artifact_type"),
			ArtifactRef:  stringField(row, "artifact_ref"),
			Thread:       stringField(row, "thread"),
			Note:         stringField(row, "note"),
			CreatedAt:    stringField(row, "created_at"),
		})
	}
	return out, nil
}

// nativeSnapshotDependenciesQuery builds the snapshot's TaskDeps statement.
//
// ORDER BY appends dep_id to make the order a UNIQUE TOTAL ORDER. Ordering by
// (task_id, depends_on_task_id) alone is NOT unique — dep_id is the table's key
// part, not that pair — so ties could reorder between two reads, which would
// make both the content hash and the cursor unsound. This is load-bearing for
// P3-GT-PAGINATION, which resumes from the cursor this emits.
func nativeSnapshotDependenciesQuery(adapter *SpannerSubstrateAdapter, projectID string, limit int) (string, map[string]interface{}) {
	return nativeSnapshotDependenciesQueryScoped(adapter, projectID, limit, nil)
}

func nativeSnapshotDependenciesQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, limit int, scopeTaskIDs []string) (string, map[string]interface{}) {
	return nativeSnapshotDependenciesPageQueryScoped(adapter, projectID, limit, scopeTaskIDs, nil)
}

func nativeSnapshotDependenciesPageQuery(adapter *SpannerSubstrateAdapter, projectID string, limit int, after []string) (string, map[string]interface{}) {
	return nativeSnapshotDependenciesPageQueryScoped(adapter, projectID, limit, nil, after)
}

func nativeSnapshotDependenciesPageQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, limit int, scopeTaskIDs, after []string) (string, map[string]interface{}) {
	where := adapter.projectPredicate() + ` AND COALESCE(kind, 'gates') != 'archived'`
	params := snapshotTaskScopeParams(projectID, scopeTaskIDs)
	where += snapshotTaskScopePredicate("task_id", scopeTaskIDs)
	if len(after) == 3 {
		where += ` AND (task_id > @after_task_id OR (task_id = @after_task_id AND (depends_on_task_id > @after_depends_on_task_id OR (depends_on_task_id = @after_depends_on_task_id AND dep_id > @after_dependency_id))))`
		params["after_task_id"] = after[0]
		params["after_depends_on_task_id"] = after[1]
		params["after_dependency_id"] = after[2]
	}
	return fmt.Sprintf(`SELECT %s, dep_id AS dependency_id, task_id, depends_on_task_id,
		kind, note, CAST(created_at AS STRING) AS created_at
		FROM %s WHERE %s
		ORDER BY task_id, depends_on_task_id, dep_id LIMIT %d`,
		adapter.projectSelectExpr(), adapter.Descriptor.DepTable, where, limit), params
}

// nativeSnapshotWorklinksQuery builds the snapshot's TaskLinks statement.
//
// ORDER BY appends link_id for the same reason: created_at carries no
// uniqueness guarantee, so two links written in the same microsecond could
// swap places between reads.
func nativeSnapshotWorklinksQuery(adapter *SpannerSubstrateAdapter, projectID string, limit int) (string, map[string]interface{}) {
	return nativeSnapshotWorklinksQueryScoped(adapter, projectID, limit, nil)
}

func nativeSnapshotWorklinksQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, limit int, scopeTaskIDs []string) (string, map[string]interface{}) {
	return nativeSnapshotWorklinksPageQueryScoped(adapter, projectID, limit, scopeTaskIDs, nil)
}

func nativeSnapshotWorklinksPageQuery(adapter *SpannerSubstrateAdapter, projectID string, limit int, after []string) (string, map[string]interface{}) {
	return nativeSnapshotWorklinksPageQueryScoped(adapter, projectID, limit, nil, after)
}

func nativeSnapshotWorklinksPageQueryScoped(adapter *SpannerSubstrateAdapter, projectID string, limit int, scopeTaskIDs, after []string) (string, map[string]interface{}) {
	where := adapter.projectPredicate() + ` AND ` + nativeActiveWorklinkPredicate
	params := snapshotTaskScopeParams(projectID, scopeTaskIDs)
	where += snapshotTaskScopePredicate("task_id", scopeTaskIDs)
	if len(after) == 2 {
		where += ` AND (created_at > @after_created_at OR (created_at = @after_created_at AND link_id > @after_artifact_id))`
		// TaskLinks.created_at is a Spanner TIMESTAMP. Keep the continuation
		// carrier typed at the query boundary; binding the cursor's display
		// string makes Spanner reject the statement (or invites a lossy cast).
		// A malformed direct builder input is represented as NULL so it cannot
		// match any row; authenticated continuations reject it earlier.
		if parsed := parseRFC3339(after[0]); !parsed.IsZero() {
			params["after_created_at"] = parsed.UTC()
		} else {
			params["after_created_at"] = spanner.NullTime{Valid: false}
		}
		params["after_artifact_id"] = after[1]
	}
	return fmt.Sprintf(`SELECT %s, link_id AS artifact_id, task_id, artifact_type, artifact_ref,
		thread, note, CAST(created_at AS STRING) AS created_at, session_id, repo, branch, commit_sha, pr_number, pr_state, worktree_path, url
		FROM %s WHERE %s
		ORDER BY created_at, link_id LIMIT %d`,
		adapter.projectSelectExpr(), adapter.Descriptor.WorklinkTable, where, limit), params
}

// nativeSnapshotTotalsQuery counts the rows a collection holds in this project
// IN FULL, independent of the cap. Run inside the snapshot transaction so the
// total is coherent with the capped rows returned beside it: total > returned
// then means "capped", never "the store moved between two reads".
func nativeSnapshotTotalsQuery(adapter *SpannerSubstrateAdapter, projectID, table, extraPredicate string) (string, map[string]interface{}) {
	return nativeSnapshotTotalsQueryScoped(adapter, projectID, table, extraPredicate, nil)
}

func nativeSnapshotTotalsQueryScoped(adapter *SpannerSubstrateAdapter, projectID, table, extraPredicate string, scopeTaskIDs []string) (string, map[string]interface{}) {
	sql := `SELECT COUNT(*) AS n FROM ` + table + ` WHERE ` + adapter.projectPredicate()
	if extraPredicate != "" {
		sql += ` AND ` + extraPredicate
	}
	if table == adapter.Descriptor.TaskTable {
		sql += snapshotTaskScopePredicate("id", scopeTaskIDs)
	} else {
		sql += snapshotTaskScopePredicate("task_id", scopeTaskIDs)
	}
	return sql, snapshotTaskScopeParams(projectID, scopeTaskIDs)
}

func snapshotTaskScopePredicate(column string, scopeTaskIDs []string) string {
	if scopeTaskIDs == nil {
		return ""
	}
	if len(scopeTaskIDs) == 0 {
		return " AND FALSE"
	}
	return " AND " + column + " IN UNNEST(@scope_task_ids)"
}

func snapshotTaskScopeParams(projectID string, scopeTaskIDs []string) map[string]interface{} {
	params := map[string]interface{}{"project_id": projectID}
	if scopeTaskIDs != nil {
		params["scope_task_ids"] = scopeTaskIDs
	}
	return params
}

func addSnapshotTaskScope(sql string, params map[string]interface{}, column string, scopeTaskIDs []string) (string, map[string]interface{}) {
	sql += snapshotTaskScopePredicate(column, scopeTaskIDs)
	if scopeTaskIDs != nil {
		params["scope_task_ids"] = scopeTaskIDs
	}
	return sql, params
}

func nativeDependencyRowsFrom(res QueryResult) []DependencyRow {
	out := make([]DependencyRow, 0, len(res.Rows))
	for _, row := range res.Rows {
		out = append(out, DependencyRow{
			ProjectID: stringField(row, "project_id"), DependencyID: stringField(row, "dependency_id"),
			TaskID: stringField(row, "task_id"), DependsOnTaskID: stringField(row, "depends_on_task_id"),
			Kind: stringField(row, "kind"), Note: stringField(row, "note"), CreatedAt: stringField(row, "created_at"),
		})
	}
	return out
}

func nativeSnapshotWorklinkRowsFrom(res QueryResult) []WorklinkRow {
	out := make([]WorklinkRow, 0, len(res.Rows))
	for _, row := range res.Rows {
		out = append(out, WorklinkRow{
			ProjectID: stringField(row, "project_id"), ArtifactID: stringField(row, "artifact_id"), TaskID: stringField(row, "task_id"),
			ArtifactType: stringField(row, "artifact_type"), ArtifactRef: stringField(row, "artifact_ref"),
			Thread: stringField(row, "thread"), Note: stringField(row, "note"), CreatedAt: stringField(row, "created_at"),
			SessionID: stringField(row, "session_id"), Repo: stringField(row, "repo"), Branch: stringField(row, "branch"), CommitSHA: stringField(row, "commit_sha"),
			PRNumber: int64(intField(row, "pr_number")), PRState: stringField(row, "pr_state"), WorktreePath: stringField(row, "worktree_path"), URL: stringField(row, "url"),
		})
	}
	return out
}

type snapshotMembership struct {
	Mode           string
	RootTaskID     string
	TaskIDs        []string
	Complete       bool
	Capped         bool
	MissingTaskIDs []string
}

func validateSnapshotScope(in projectSnapshotInput) (string, error) {
	mode := in.Scope
	if mode == "" {
		mode = snapshotScopeProject
	}
	switch mode {
	case snapshotScopeProject:
		if in.RootTaskID != "" {
			return "", fmt.Errorf("root_task_id requires a scoped snapshot mode")
		}
	case snapshotScopeExecutionSubtree, snapshotScopeAcceptanceDependencyClosure:
		if in.RootTaskID == "" {
			return "", fmt.Errorf("root_task_id required for snapshot scope %q", mode)
		}
	default:
		return "", fmt.Errorf("unknown snapshot scope %q", mode)
	}
	return mode, nil
}

func snapshotUniqueIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func snapshotIDsFromRows(res QueryResult, column string) []string {
	ids := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		ids = append(ids, stringField(row, column))
	}
	return snapshotUniqueIDs(ids)
}

func snapshotMissingIDs(all, found []string) []string {
	foundSet := make(map[string]struct{}, len(found))
	for _, id := range found {
		foundSet[id] = struct{}{}
	}
	missing := make([]string, 0)
	for _, id := range snapshotUniqueIDs(all) {
		if _, ok := foundSet[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func snapshotScopeRootQuery(adapter *SpannerSubstrateAdapter) string {
	return `SELECT id FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectPredicate() + ` AND id = @root_task_id`
}

func snapshotScopeChildrenQuery(adapter *SpannerSubstrateAdapter, limit int) string {
	return fmt.Sprintf(`SELECT id FROM %s WHERE %s AND parent_id IN UNNEST(@parent_ids) ORDER BY id LIMIT %d`, adapter.Descriptor.TaskTable, adapter.projectPredicate(), limit)
}

func snapshotScopeDependenciesQuery(adapter *SpannerSubstrateAdapter, limit int) string {
	return fmt.Sprintf(`SELECT dep_id, task_id, depends_on_task_id FROM %s WHERE %s AND task_id IN UNNEST(@scope_frontier) AND COALESCE(kind, 'gates') != 'archived' ORDER BY task_id, depends_on_task_id, dep_id LIMIT %d`, adapter.Descriptor.DepTable, adapter.projectPredicate(), limit)
}

func snapshotScopeExistingIDsQuery(adapter *SpannerSubstrateAdapter) string {
	return `SELECT id FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectPredicate() + ` AND id IN UNNEST(@scope_task_ids)`
}

func snapshotDependencyTargets(res QueryResult) []string {
	ids := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		ids = append(ids, stringField(row, "depends_on_task_id"))
	}
	return snapshotUniqueIDs(ids)
}

// discoverSnapshotMembership performs bounded BFS through the same
// read-only transaction used for the final snapshot. It intentionally does
// not use task_graph_closure: the native task-plan store has no property
// graph, and direct neighbors would observe separate read timestamps.
func discoverSnapshotMembership(ctx context.Context, adapter *SpannerSubstrateAdapter, q SnapshotQuerier, projectID string, mode, rootTaskID string, limit int) (snapshotMembership, error) {
	out := snapshotMembership{Mode: mode, RootTaskID: rootTaskID, Complete: true}
	if mode == snapshotScopeProject {
		return out, nil
	}
	rootRes, err := q(ctx, snapshotScopeRootQuery(adapter), map[string]interface{}{"project_id": projectID, "root_task_id": rootTaskID})
	if err != nil {
		return out, fmt.Errorf("scope root: %w", err)
	}
	if len(rootRes.Rows) == 0 {
		out.Complete = false
		out.MissingTaskIDs = []string{rootTaskID}
		return out, nil
	}
	ids := []string{rootTaskID}
	seen := map[string]struct{}{rootTaskID: {}}
	frontier := []string{rootTaskID}
	maxMembership := limit
	if maxMembership <= 0 {
		maxMembership = 5000
	}
	if maxMembership > 10000 {
		maxMembership = 10000
	}
	addIDs := func(next []string) {
		for _, id := range next {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if mode == snapshotScopeExecutionSubtree {
		for len(frontier) > 0 && !out.Capped {
			remaining := maxMembership - len(ids)
			if remaining <= 0 {
				out.Capped = true
				out.Complete = false
				break
			}
			var children []string
			for _, chunk := range chunkSnapshotIDs(frontier, snapshotUnnestIDsLimit()) {
				rem := maxMembership - (len(ids) + len(children))
				if rem <= 0 {
					out.Capped = true
					out.Complete = false
					break
				}
				childRes, qErr := q(ctx, snapshotScopeChildrenQuery(adapter, rem+1), map[string]interface{}{"project_id": projectID, "parent_ids": chunk})
				if qErr != nil {
					return out, fmt.Errorf("scope children: %w", qErr)
				}
				chunkChildren := snapshotIDsFromRows(childRes, "id")
				if len(chunkChildren) > rem {
					chunkChildren = chunkChildren[:rem]
					out.Capped = true
					out.Complete = false
				}
				children = append(children, chunkChildren...)
				if out.Capped {
					break
				}
			}
			children = snapshotUniqueIDs(children)
			if len(children) > remaining {
				children = children[:remaining]
				out.Capped = true
				out.Complete = false
			}
			addIDs(children)
			frontier = children
		}
	} else {
		gateRes, qErr := q(ctx, snapshotScopeChildrenQuery(adapter, maxMembership+1), map[string]interface{}{"project_id": projectID, "parent_ids": []string{rootTaskID}})
		if qErr != nil {
			return out, fmt.Errorf("acceptance gates: %w", qErr)
		}
		gates := snapshotIDsFromRows(gateRes, "id")
		if len(gates) > maxMembership-1 {
			gates = gates[:maxMembership-1]
			out.Capped = true
			out.Complete = false
		}
		addIDs(gates)
		frontier = gates
		for len(frontier) > 0 && !out.Capped {
			remaining := maxMembership - len(ids)
			if remaining <= 0 {
				out.Capped = true
				out.Complete = false
				break
			}
			var deps []string
			for _, chunk := range chunkSnapshotIDs(frontier, snapshotUnnestIDsLimit()) {
				depRes, qErr := q(ctx, snapshotScopeDependenciesQuery(adapter, remaining+1), map[string]interface{}{"project_id": projectID, "scope_frontier": chunk})
				if qErr != nil {
					return out, fmt.Errorf("acceptance dependencies: %w", qErr)
				}
				deps = append(deps, snapshotDependencyTargets(depRes)...)
			}
			deps = snapshotUniqueIDs(deps)
			newDeps := make([]string, 0, len(deps))
			for _, id := range deps {
				if _, ok := seen[id]; !ok {
					newDeps = append(newDeps, id)
				}
			}
			if len(newDeps) > remaining {
				newDeps = newDeps[:remaining]
				out.Capped = true
				out.Complete = false
			}
			addIDs(newDeps)
			frontier = newDeps
		}
	}
	out.TaskIDs = snapshotUniqueIDs(ids)
	if len(out.TaskIDs) == 0 {
		return out, nil
	}
	var foundIDs []string
	for _, chunk := range chunkSnapshotIDs(out.TaskIDs, snapshotUnnestIDsLimit()) {
		foundRes, err := q(ctx, snapshotScopeExistingIDsQuery(adapter), map[string]interface{}{"project_id": projectID, "scope_task_ids": chunk})
		if err != nil {
			return out, fmt.Errorf("scope task existence: %w", err)
		}
		foundIDs = append(foundIDs, snapshotIDsFromRows(foundRes, "id")...)
	}
	out.MissingTaskIDs = snapshotMissingIDs(out.TaskIDs, foundIDs)
	if len(out.MissingTaskIDs) > 0 {
		out.Complete = false
	}
	return out, nil
}

// nativeHandleProjectSnapshot emits ONE transactionally coherent projection of
// a task-plan project.
//
// Every read below runs inside a SINGLE SnapshotReader transaction, so all of
// them observe one Spanner read timestamp. The predecessor issued five
// independent client.Single() queries, each picking its own timestamp, so a
// concurrent write landing between any two of them produced a snapshot mixing
// revisions — a state that never existed atomically (P3-GT-FP-SNAPSHOT).
//
// reader is taken as a parameter rather than reached for through adapter so
// the coherence property is testable without Spanner: a fake reader observes
// how many transactions were opened and which statements ran inside each.
func nativeHandleProjectSnapshot(ctx context.Context, env *Env, adapter *SpannerSubstrateAdapter, reader SnapshotReader, in projectSnapshotInput) (ProjectSnapshotResult, error) {
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return ProjectSnapshotResult{}, err
	}
	mode, err := validateSnapshotScope(in)
	if err != nil {
		return ProjectSnapshotResult{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 5000
	}
	if limit > 10000 {
		return ProjectSnapshotResult{}, fmt.Errorf("limit must be <= 10000")
	}
	now := time.Now().UTC()
	if env != nil && env.Now != nil {
		now = env.Now().UTC()
	}
	var continuation snapshotContinuation
	resuming := in.Continuation != ""
	if resuming {
		key := []byte(nil)
		if env != nil {
			key = env.SnapshotContinuationKey
		}
		var err error
		continuation, err = decodeSnapshotContinuation(in.Continuation, key, projectID, mode, in.RootTaskID, limit, now)
		if err != nil {
			return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation: %w", err)
		}
		if continuation.TasksCollected > continuation.TasksTotal || continuation.DepsCollected > continuation.DepsTotal || continuation.LinksCollected > continuation.LinksTotal {
			return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation collected rows exceed authorized totals")
		}
	}

	var (
		taskRes, groupRes, gapRes, depRes, linkRes QueryResult
		taskTotalRes, depTotalRes, linkTotalRes    QueryResult
	)
	scope := snapshotMembership{Mode: mode, RootTaskID: in.RootTaskID, Complete: true}
	readFn := func(q SnapshotQuerier) error {
		var scopeErr error
		if mode != snapshotScopeProject {
			scope, scopeErr = discoverSnapshotMembership(ctx, adapter, q, projectID, mode, in.RootTaskID, limit)
			if scopeErr != nil {
				return scopeErr
			}
		}
		var scopeTaskIDs []string
		if mode != snapshotScopeProject {
			scopeTaskIDs = scope.TaskIDs
		}
		type read struct {
			label  string
			sql    string
			params map[string]interface{}
			into   *QueryResult
		}
		taskAfter, depAfter, linkAfter := continuation.TasksAfter, continuation.DepsAfter, continuation.LinksAfter
		taskLimit, depLimit, linkLimit := limit, limit, limit
		if resuming && continuation.TasksCollected >= continuation.TasksTotal {
			taskLimit = 0
		}
		if resuming && continuation.DepsCollected >= continuation.DepsTotal {
			depLimit = 0
		}
		if resuming && continuation.LinksCollected >= continuation.LinksTotal {
			linkLimit = 0
		}
		taskSQL, taskParams := nativeSnapshotTasksPageQueryScoped(adapter, projectID, tasksInput{}, taskLimit, scopeTaskIDs, taskAfter)
		groupSQL, groupParams := nativeTaskCountsGroupQueryScoped(adapter, projectID, scopeTaskIDs)
		gapSQL, gapParams := nativeTaskCountsGapQueryScoped(adapter, projectID, scopeTaskIDs)
		depSQL, depParams := nativeSnapshotDependenciesPageQueryScoped(adapter, projectID, depLimit, scopeTaskIDs, depAfter)
		linkSQL, linkParams := nativeSnapshotWorklinksPageQueryScoped(adapter, projectID, linkLimit, scopeTaskIDs, linkAfter)
		taskTotalSQL, taskTotalParams := nativeSnapshotTotalsQueryScoped(adapter, projectID, adapter.Descriptor.TaskTable, "", scopeTaskIDs)
		depTotalSQL, depTotalParams := nativeSnapshotTotalsQueryScoped(adapter, projectID, adapter.Descriptor.DepTable, `COALESCE(kind, 'gates') != 'archived'`, scopeTaskIDs)
		linkTotalSQL, linkTotalParams := nativeSnapshotTotalsQueryScoped(adapter, projectID, adapter.Descriptor.WorklinkTable, nativeActiveWorklinkPredicate, scopeTaskIDs)

		for _, r := range []read{
			{"tasks", taskSQL, taskParams, &taskRes},
			{"counts group", groupSQL, groupParams, &groupRes},
			{"counts gap", gapSQL, gapParams, &gapRes},
			{"dependencies", depSQL, depParams, &depRes},
			{"worklinks", linkSQL, linkParams, &linkRes},
			{"tasks total", taskTotalSQL, taskTotalParams, &taskTotalRes},
			{"dependencies total", depTotalSQL, depTotalParams, &depTotalRes},
			{"worklinks total", linkTotalSQL, linkTotalParams, &linkTotalRes},
		} {
			res, qErr := q(ctx, r.sql, r.params)
			if qErr != nil {
				return fmt.Errorf("%s: %w", r.label, qErr)
			}
			// A collection completed on an earlier page contributes no rows to
			// this page. The SQL uses LIMIT 0 for that case; clear defensively as
			// well so alternate SnapshotReader implementations cannot reintroduce
			// duplicates by ignoring the limit.
			if resuming && ((r.label == "tasks" && continuation.TasksCollected >= continuation.TasksTotal) ||
				(r.label == "dependencies" && continuation.DepsCollected >= continuation.DepsTotal) ||
				(r.label == "worklinks" && continuation.LinksCollected >= continuation.LinksTotal)) {
				res.Rows = nil
			}
			*r.into = res
		}
		if mode != snapshotScopeProject {
			// A structural subtree intentionally includes dependency edges from
			// scoped tasks to external prerequisites. Resolve those references in
			// this same transaction so the scope can report exact missing IDs
			// instead of silently treating an omitted task as complete.
			dependencyTargets := snapshotDependencyTargets(depRes)
			if len(dependencyTargets) > 0 {
				var foundDepIDs []string
				for _, chunk := range chunkSnapshotIDs(dependencyTargets, snapshotUnnestIDsLimit()) {
					foundRes, qErr := q(ctx, snapshotScopeExistingIDsQuery(adapter), map[string]interface{}{"project_id": projectID, "scope_task_ids": chunk})
					if qErr != nil {
						return fmt.Errorf("scope dependency existence: %w", qErr)
					}
					foundDepIDs = append(foundDepIDs, snapshotIDsFromRows(foundRes, "id")...)
				}
				missing := snapshotMissingIDs(dependencyTargets, foundDepIDs)
				if len(missing) > 0 {
					scope.MissingTaskIDs = append(snapshotUniqueIDs(scope.MissingTaskIDs), missing...)
					scope.MissingTaskIDs = snapshotUniqueIDs(scope.MissingTaskIDs)
					scope.Complete = false
				}
			}
		}
		return nil
	}
	var readTimestamp time.Time
	if resuming {
		ts, parseErr := time.Parse(time.RFC3339Nano, continuation.ReadTimestamp)
		if parseErr != nil {
			return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation read timestamp: %w", parseErr)
		}
		at, ok := reader.(TimestampSnapshotReader)
		if !ok {
			return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation requires exact timestamp snapshot reader")
		}
		readTimestamp, err = at.ReadOnlySnapshotAt(ctx, ts, readFn)
		if err == nil && !readTimestamp.Equal(ts) {
			return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation read timestamp mismatch")
		}
	} else {
		readTimestamp, err = reader.ReadOnlySnapshot(ctx, readFn)
	}
	if err != nil {
		return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: %w", err)
	}
	if resuming && mode != snapshotScopeProject {
		// Dependency targets are checked one page at a time. Carry all earlier
		// missing prerequisites forward inside the authenticated continuation so
		// later pages cannot turn an incomplete fixed-timestamp scope complete.
		priorMissing := append([]string(nil), continuation.ScopeMissingTaskIDs...)
		scope.MissingTaskIDs = snapshotUniqueIDs(append(priorMissing, scope.MissingTaskIDs...))
		if len(scope.MissingTaskIDs) > 0 {
			scope.Complete = false
		}
	}

	tasks := nativeTaskRowsFrom(taskRes)
	dependencies := nativeDependencyRowsFrom(depRes)
	worklinks := nativeSnapshotWorklinkRowsFrom(linkRes)
	counts, err := nativeEnvTaskCounts(ctx, env, groupRes, gapRes)
	if err != nil {
		return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: counts: %w", err)
	}

	out := ProjectSnapshotResult{
		ProjectID:     projectID,
		StoreType:     fleetPlanNativeStoreType,
		GeneratedAt:   env.Now().UTC().Format(time.RFC3339Nano),
		Revision:      snapshotRevision(readTimestamp),
		ReadTimestamp: readTimestamp.UTC().Format(time.RFC3339Nano),
		Limit:         limit,
		Scope: SnapshotScope{
			Mode:            scope.Mode,
			RootTaskID:      scope.RootTaskID,
			Complete:        scope.Complete,
			Capped:          scope.Capped,
			MembershipCount: len(scope.TaskIDs),
			MissingTaskIDs:  snapshotUniqueIDs(scope.MissingTaskIDs),
		},
		Tasks:        tasks,
		Dependencies: dependencies,
		Worklinks:    worklinks,
		Counts:       counts,
	}
	taskTotal, depTotal, linkTotal := snapshotTotal(taskTotalRes), snapshotTotal(depTotalRes), snapshotTotal(linkTotalRes)
	if resuming && (taskTotal != continuation.TasksTotal || depTotal != continuation.DepsTotal || linkTotal != continuation.LinksTotal) {
		return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation authorized totals mismatch")
	}
	out.Collections = SnapshotCollections{
		Tasks:        newPagedCollectionState(continuation.TasksCollected, len(tasks), taskTotal, limit, taskCursor(tasks)),
		Dependencies: newPagedCollectionState(continuation.DepsCollected, len(dependencies), depTotal, limit, dependencyCursor(dependencies)),
		Worklinks:    newPagedCollectionState(continuation.LinksCollected, len(worklinks), linkTotal, limit, worklinkCursor(worklinks)),
	}
	// A resumed non-empty collection must advance whenever it still has rows.
	// Refusing an empty page prevents a malformed or stale cursor from being
	// mistaken for a complete inventory.
	if (continuation.TasksCollected < taskTotal && len(tasks) == 0) ||
		(continuation.DepsCollected < depTotal && len(dependencies) == 0) ||
		(continuation.LinksCollected < linkTotal && len(worklinks) == 0) {
		return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: snapshot made no progress")
	}
	if (out.Collections.Tasks.Capped && out.Collections.Tasks.NextCursor == "") ||
		(out.Collections.Dependencies.Capped && out.Collections.Dependencies.NextCursor == "") ||
		(out.Collections.Worklinks.Capped && out.Collections.Worklinks.NextCursor == "") {
		return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: incomplete collection cursor unavailable")
	}
	out.Complete = out.Collections.Tasks.Complete && out.Collections.Dependencies.Complete && out.Collections.Worklinks.Complete
	if !out.Complete {
		key := []byte(nil)
		var ttl time.Duration
		if env != nil {
			key = env.SnapshotContinuationKey
			ttl = env.SnapshotContinuationTTL
		}
		if len(key) >= 32 {
			if ttl <= 0 {
				return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation lifetime unavailable")
			}
			cont := snapshotContinuation{
				Version: snapshotContinuationVersion, ProjectID: projectID, Scope: mode, RootTaskID: in.RootTaskID,
				ReadTimestamp: out.ReadTimestamp, Revision: out.Revision, Limit: limit,
				ExpiresAt:           now.Add(ttl).Format(time.RFC3339Nano),
				ScopeMissingTaskIDs: append([]string(nil), out.Scope.MissingTaskIDs...),
				TasksTotal:          taskTotal, DepsTotal: depTotal, LinksTotal: linkTotal,
				TasksCollected: out.Collections.Tasks.Collected, DepsCollected: out.Collections.Dependencies.Collected, LinksCollected: out.Collections.Worklinks.Collected,
			}
			if out.Collections.Tasks.Capped {
				_ = json.Unmarshal([]byte(out.Collections.Tasks.NextCursor), &cont.TasksAfter)
			}
			if out.Collections.Dependencies.Capped {
				_ = json.Unmarshal([]byte(out.Collections.Dependencies.NextCursor), &cont.DepsAfter)
			}
			if out.Collections.Worklinks.Capped {
				_ = json.Unmarshal([]byte(out.Collections.Worklinks.NextCursor), &cont.LinksAfter)
			}
			encoded, encErr := encodeSnapshotContinuation(key, cont)
			if encErr != nil {
				return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation: %w", encErr)
			}
			out.Continuation = encoded
		} else {
			// An incomplete page without an authenticated continuation cannot be
			// resumed safely. Do not emit a false-complete or unusable partial
			// snapshot while the owner key contract is absent.
			return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: continuation signing key unavailable")
		}
	}
	if mode == snapshotScopeProject {
		out.Scope.MembershipCount = out.Collections.Tasks.AuthorizedTotal
	}
	hashes, err := snapshotContentHashes(tasks, dependencies, worklinks, out.Counts)
	if err != nil {
		return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot: content hashes: %w", err)
	}
	out.ContentHashes = hashes
	return out, nil
}

func snapshotTotal(res QueryResult) int {
	if len(res.Rows) == 0 {
		return 0
	}
	return intField(res.Rows[0], "n")
}

func nativeHandleAddWorklink(ctx context.Context, env *Env, in addWorklinkInput) (MutationWriteResult, error) {
	if in.TaskID == "" || in.ArtifactType == "" || in.ArtifactRef == "" {
		return MutationWriteResult{}, fmt.Errorf("task_id, artifact_type, artifact_ref required")
	}
	if namesWriterHandoverHistory(in.Note) {
		return MutationWriteResult{}, errWriterHandoverHistory()
	}
	if err := requireArtifactKindRegistry(env); err != nil {
		return MutationWriteResult{}, err
	}
	if _, err := env.ArtifactKinds.Resolve(ctx, in.ArtifactType); err != nil {
		return MutationWriteResult{}, err
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	linkID := in.ArtifactID
	if linkID == "" {
		linkID = "LNK-" + uuid.NewString()
	}
	now := env.Now().UTC()
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	columns, linkParams := nativeBuildWorklinkInsert(linkID, in.TaskID, in.ArtifactType, in.ArtifactRef, nativeWorklinkInsertFields(in), now)
	linkParams["project_id"] = projectID
	placeholders := make([]string, len(columns))
	for i, col := range columns {
		placeholders[i] = "@" + col
	}
	var rows int64
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := nativeRequireTaskInProject(ctx, tx, adapter, projectID, in.TaskID); err != nil {
			return err
		}
		stmt := spanner.Statement{
			SQL: `INSERT INTO ` + adapter.Descriptor.WorklinkTable + `
				(` + adapter.projectInsertColumns() + strings.Join(columns, ", ") + `)
				VALUES (` + adapter.projectInsertValues() + strings.Join(placeholders, ", ") + `)`,
			Params: linkParams,
		}
		n, err := tx.Update(ctx, stmt)
		rows = n
		return err
	})
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("insert task-plan worklink: %w", err)
	}
	return nativeMutationWriteResult(nativeMutationReceipt(env, projectID, "insert_worklink", "artifact", linkID, adapter.Descriptor.WorklinkTable, rows, now)), nil
}

func nativeHandleArchiveWorklink(ctx context.Context, env *Env, in archiveWorklinkInput) (MutationWriteResult, error) {
	if in.TaskID == "" || in.LinkID == "" {
		return MutationWriteResult{}, fmt.Errorf("task_id, link_id required")
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil {
		return MutationWriteResult{}, err
	}
	if adapter == nil {
		return MutationWriteResult{}, fmt.Errorf("task_graph_archive_worklink requires fleet_plan_native store")
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	mutatedAt := env.Now().UTC()
	var rows int64
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		rows = 0
		if err := nativeRequireTaskInProject(ctx, tx, adapter, projectID, in.TaskID); err != nil {
			return err
		}
		stored, linkExists, err := nativeReadWorklinkStateTx(ctx, tx, adapter, projectID, in.LinkID)
		if err != nil {
			return err
		}
		alreadyArchived, err := validateNativeWorklinkArchiveTarget(in.TaskID, in.LinkID, stored.TaskID, stored.Status, true, linkExists)
		if err != nil {
			return err
		}
		if alreadyArchived {
			return nil
		}
		// Archive is for erroneous rows. A superseded predecessor is the retry and
		// read-back evidence of a succession, and the sole valid active binding is
		// replaced only by typed succession; both are decided in this transaction.
		if strings.EqualFold(strings.TrimSpace(stored.Status), worklinkSupersededStatus) {
			return errArchiveSupersededHistory(in.LinkID)
		}
		if namesWriterHandoverHistory(stored.Note) {
			return errWriterHandoverHistory()
		}
		taskLinks, err := nativeReadTaskWorklinksTx(ctx, tx, adapter, projectID, in.TaskID)
		if err != nil {
			return err
		}
		if nativeIsSoleValidActiveBinding(stored, taskLinks) {
			return errSoleValidBinding(stored)
		}
		update := spanner.Statement{
			SQL: `UPDATE ` + adapter.Descriptor.WorklinkTable + `
				SET artifact_status = 'archived'
				WHERE ` + adapter.projectWhereAnd() + `link_id = @link_id AND task_id = @task_id
				AND (artifact_status IS NULL OR artifact_status != 'archived')`,
			Params: map[string]interface{}{"project_id": projectID, "link_id": in.LinkID, "task_id": in.TaskID},
		}
		rows, err = tx.Update(ctx, update)
		return err
	})
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("archive task-plan worklink: %w", err)
	}
	return nativeMutationWriteResult(nativeMutationReceipt(env, projectID, "archive_worklink", "artifact", in.LinkID, adapter.Descriptor.WorklinkTable, rows, mutatedAt)), nil
}

const (
	// worklinkBindingArtifactType is the canonical artifact type of a branch or
	// worktree binding. Only this type participates in a supersede transition.
	worklinkBindingArtifactType = "worktree"
	// worklinkSupersededStatus marks a valid binding that a successor replaced.
	// It is history, not an erroneous row: archived stays reserved for that.
	worklinkSupersededStatus  = "superseded"
	worklinkRecordedStatus    = "recorded"
	worklinkSupersedeNote     = "supersedes="
	writerHandoverHistoryNote = "writer_handover_history="
	writerHandoverHistoryType = "finding"
)

// nativeNamesSupersededStatus reports whether a caller-supplied artifact_status
// names the superseded lifecycle, in any case or padding: readers of the row
// compare the status case-insensitively, so every spelling retires the binding.
func nativeNamesSupersededStatus(value interface{}) bool {
	s, ok := value.(string)
	return ok && strings.EqualFold(strings.TrimSpace(s), worklinkSupersededStatus)
}

// errGenericUpdateSuperseded is returned by task_graph_update when it is asked
// to mint the superseded lifecycle or to touch a row that already carries it.
// superseded is reachable only through task_graph_supersede_worklink, whose
// transaction fences the owner and session, compares the predecessor and
// installs the successor; a superseded row is immutable history.
func errGenericUpdateSuperseded(linkID string) error {
	if linkID == "" {
		return fmt.Errorf("artifact_status=%s is set only by task_graph_supersede_worklink, not by a generic worklink update", worklinkSupersededStatus)
	}
	return fmt.Errorf("worklink %q is superseded history: it is immutable through a generic update, and %s is set only by task_graph_supersede_worklink", linkID, worklinkSupersededStatus)
}

// A VALID ACTIVE BINDING is an active worktree, binding or branch row that
// carries branch, worktree, thread and session: the row the identity guard
// counts and task_graph_supersede_worklink replaces. When it is the ONLY such
// row of its task and repository, retiring or rewriting it outside typed
// succession would leave the task with no binding or a counterfeit successor,
// so neither the generic update nor the erroneous-row archive may do it. Rows
// that are not canonical bindings (evidence, legacy rows without thread or
// session) and duplicates beside another valid binding remain ordinary
// erroneous-row cleanup.
func nativeIsValidActiveBinding(w nativeWorklinkState) bool {
	return nativeWorklinkIsActiveBinding(w) &&
		strings.TrimSpace(w.Branch) != "" && strings.TrimSpace(w.WorktreePath) != "" &&
		strings.TrimSpace(w.Thread) != "" && strings.TrimSpace(w.SessionID) != ""
}

func nativeIsSoleValidActiveBinding(w nativeWorklinkState, taskLinks []nativeWorklinkState) bool {
	if !nativeIsValidActiveBinding(w) {
		return false
	}
	valid := 0
	for _, other := range taskLinks {
		if nativeIsValidActiveBinding(other) && strings.EqualFold(other.Repo, w.Repo) {
			valid++
		}
	}
	return valid == 1
}

// nativeNamesInactiveStatus reports whether a caller-supplied artifact_status
// names a status that takes a binding out of use, in any case or padding.
func nativeNamesInactiveStatus(value interface{}) bool {
	s, ok := value.(string)
	return ok && nativeInactiveWorklinkStatuses[strings.ToLower(strings.TrimSpace(s))]
}

// nativeRewritesBindingIdentity reports whether a generic update would change
// a coordinate that identifies the binding. A value equal to the stored one is
// the get-to-update echo and is not a change; empty values are skipped exactly
// as nativeBuildWorklinkUpdate skips them.
func nativeRewritesBindingIdentity(stored nativeWorklinkState, fields map[string]interface{}) bool {
	for key, current := range map[string]string{
		"task_id": stored.TaskID, "artifact_type": stored.ArtifactType, "repo": stored.Repo, "branch": stored.Branch,
		"worktree_path": stored.WorktreePath, "thread": stored.Thread, "session_id": stored.SessionID,
	} {
		if requested, ok := fields[key].(string); ok && requested != "" && requested != current {
			return true
		}
	}
	return false
}

func errSoleValidBinding(w nativeWorklinkState) error {
	return fmt.Errorf("worklink %q is the sole valid active binding of task %q for repository %q: it is retired or rewritten only by task_graph_supersede_worklink, never by a generic update or the erroneous-row archive", w.LinkID, w.TaskID, w.Repo)
}

func errSupersedeLinkage() error {
	return fmt.Errorf("a note beginning %q is the supersede linkage that task_graph_supersede_worklink alone writes; it is immutable through a generic update", worklinkSupersedeNote)
}

func errArchiveSupersededHistory(linkID string) error {
	return fmt.Errorf("worklink %q is superseded history, the retry and read-back evidence of a succession, not an erroneous row; it cannot be archived (succession is task_graph_supersede_worklink)", linkID)
}

func namesWriterHandoverHistory(note string) bool {
	return strings.HasPrefix(strings.TrimSpace(note), writerHandoverHistoryNote)
}

func errWriterHandoverHistory() error {
	return fmt.Errorf("a note beginning %q is immutable task_graph_reown writer_handover evidence; generic create, update, link and archive routes cannot mint or change it", writerHandoverHistoryNote)
}

// nativeGuardGenericWorklinkUpdate is the fence a generic update passes inside
// the transaction that would write. It refuses, with nothing written, an update
// that would forge or rewrite the supersede linkage note, or that would retire
// or rewrite the sole valid active binding.
func nativeGuardGenericWorklinkUpdate(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, projectID string, stored nativeWorklinkState, fields map[string]interface{}) error {
	if namesWriterHandoverHistory(stored.Note) {
		return errWriterHandoverHistory()
	}
	if note, ok := fields["note"].(string); ok && namesWriterHandoverHistory(note) {
		return errWriterHandoverHistory()
	}
	if note, ok := fields["note"].(string); ok && note != "" && note != stored.Note &&
		(strings.HasPrefix(note, worklinkSupersedeNote) || strings.HasPrefix(stored.Note, worklinkSupersedeNote)) {
		return errSupersedeLinkage()
	}
	if !nativeIsValidActiveBinding(stored) || (!nativeNamesInactiveStatus(fields["artifact_status"]) && !nativeRewritesBindingIdentity(stored, fields)) {
		return nil
	}
	taskLinks, err := nativeReadTaskWorklinksTx(ctx, tx, a, projectID, stored.TaskID)
	if err != nil {
		return err
	}
	if nativeIsSoleValidActiveBinding(stored, taskLinks) {
		return errSoleValidBinding(stored)
	}
	return nil
}

// nativeBindingArtifactTypes are the artifact types that reserve a branch or
// worktree for a task. It mirrors the identity guard's protected set.
var nativeBindingArtifactTypes = map[string]bool{"worktree": true, "binding": true, "branch": true}

// nativeInactiveWorklinkStatusNames is the single lifecycle set shared by
// in-memory binding checks and snapshot SQL. Both paths normalize case and
// surrounding whitespace before membership testing.
var nativeInactiveWorklinkStatusNames = []string{"archived", "superseded", "released", "closed"}

var nativeInactiveWorklinkStatuses = func() map[string]bool {
	statuses := make(map[string]bool, len(nativeInactiveWorklinkStatusNames))
	for _, status := range nativeInactiveWorklinkStatusNames {
		statuses[status] = true
	}
	return statuses
}()

// nativeActiveWorklinkPredicate selects the ACTIVE worklinks a consumer may
// count. Inactive rows stay readable as task history through
// task_graph_worklinks and task_graph_get.
var nativeActiveWorklinkPredicate = func() string {
	quoted := make([]string, 0, len(nativeInactiveWorklinkStatusNames))
	for _, status := range nativeInactiveWorklinkStatusNames {
		quoted = append(quoted, "'"+status+"'")
	}
	return `(artifact_status IS NULL OR LOWER(TRIM(artifact_status)) NOT IN (` + strings.Join(quoted, ", ") + `))`
}()

// ErrWorklinkSupersedeConflict is the sentinel for a supersede request that
// lost to, or contradicts, a transition another caller already committed.
var ErrWorklinkSupersedeConflict = errors.New("worklink supersede conflict")

// WorklinkSupersedeConflict reports the committed winner so the losing caller
// can reconcile by exact read-back instead of issuing a second successor.
type WorklinkSupersedeConflict struct {
	TaskID            string
	PredecessorLinkID string
	WinnerLinkID      string
	Reason            string
}

func (e WorklinkSupersedeConflict) Error() string {
	if e.WinnerLinkID != "" {
		return fmt.Sprintf("%s: predecessor %q of task %q is already superseded by %q; %s", ErrWorklinkSupersedeConflict, e.PredecessorLinkID, e.TaskID, e.WinnerLinkID, e.Reason)
	}
	return fmt.Sprintf("%s: predecessor %q of task %q: %s", ErrWorklinkSupersedeConflict, e.PredecessorLinkID, e.TaskID, e.Reason)
}

func (e WorklinkSupersedeConflict) Unwrap() error { return ErrWorklinkSupersedeConflict }

// nativeSupersedeFault is a test seam: it is called inside the transaction at
// the named stage and any error aborts and rolls the whole transition back.
// Production leaves it nil.
var nativeSupersedeFault func(stage string) error

// errSupersedeLostRace means another transition retired the predecessor after
// this transaction read it as active. The caller re-reads once and reports the
// committed winner (or converges on an identical transition).
var errSupersedeLostRace = errors.New("predecessor was superseded concurrently")

func validateSupersedeWorklinkInput(in supersedeWorklinkInput) error {
	p, s := in.ExpectedPredecessor, in.Successor
	for _, field := range []struct{ name, value string }{
		{"task_id", in.TaskID}, {"expected_owner", in.ExpectedOwner}, {"expected_session_id", in.ExpectedSessionID}, {"predecessor_link_id", in.PredecessorLinkID},
		{"expected_predecessor.artifact_type", p.ArtifactType}, {"expected_predecessor.artifact_status", p.ArtifactStatus}, {"expected_predecessor.repo", p.Repo},
		{"expected_predecessor.branch", p.Branch}, {"expected_predecessor.worktree_path", p.WorktreePath}, {"expected_predecessor.thread", p.Thread}, {"expected_predecessor.session_id", p.SessionID},
		{"successor.link_id", s.LinkID}, {"successor.artifact_type", s.ArtifactType}, {"successor.artifact_ref", s.ArtifactRef}, {"successor.repo", s.Repo},
		{"successor.branch", s.Branch}, {"successor.worktree_path", s.WorktreePath}, {"successor.thread", s.Thread}, {"successor.session_id", s.SessionID},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s required", field.name)
		}
	}
	switch {
	case in.ExpectedOwner == "UNOWNED":
		return fmt.Errorf("expected_owner must name the owning principal, not UNOWNED")
	case p.ArtifactType != worklinkBindingArtifactType:
		return fmt.Errorf("expected_predecessor.artifact_type must be %s", worklinkBindingArtifactType)
	case s.ArtifactType != worklinkBindingArtifactType:
		return fmt.Errorf("successor.artifact_type must be %s", worklinkBindingArtifactType)
	case p.ArtifactStatus != worklinkRecordedStatus:
		return fmt.Errorf("expected_predecessor.artifact_status must be %s", worklinkRecordedStatus)
	case s.LinkID == in.PredecessorLinkID:
		return fmt.Errorf("successor.link_id must differ from predecessor_link_id")
	case s.Repo != p.Repo:
		return fmt.Errorf("successor.repo must equal the predecessor repository")
	case s.Thread != p.Thread:
		return fmt.Errorf("successor.thread must equal the predecessor thread")
	case s.SessionID != p.SessionID || s.SessionID != in.ExpectedSessionID:
		return fmt.Errorf("successor.session_id must equal the predecessor and task session_id")
	case s.Branch == p.Branch && s.WorktreePath == p.WorktreePath:
		return fmt.Errorf("successor must differ from the predecessor branch or worktree_path")
	}
	return nil
}

// nativeWorklinkState is one TaskLinks row as the supersede transition reads it.
type nativeWorklinkState struct {
	LinkID, TaskID, ArtifactType, ArtifactRef, Thread, Note string
	SessionID, Repo, Branch, WorktreePath, Status           string
}

const nativeWorklinkStateColumns = `link_id, task_id, artifact_type, artifact_ref, thread, note, session_id, repo, branch, worktree_path, artifact_status`

func nativeScanWorklinkState(row *spanner.Row) (nativeWorklinkState, error) {
	var out nativeWorklinkState
	var thread, note, session, repo, branch, worktree, status spanner.NullString
	if err := row.Columns(&out.LinkID, &out.TaskID, &out.ArtifactType, &out.ArtifactRef, &thread, &note, &session, &repo, &branch, &worktree, &status); err != nil {
		return nativeWorklinkState{}, err
	}
	out.Thread, out.Note, out.SessionID = nullStringValue(thread), nullStringValue(note), nullStringValue(session)
	out.Repo, out.Branch, out.WorktreePath, out.Status = nullStringValue(repo), nullStringValue(branch), nullStringValue(worktree), nullStringValue(status)
	if out.Status == "" {
		out.Status = worklinkRecordedStatus
	}
	return out, nil
}

func nativeReadWorklinkStateTx(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, projectID, linkID string) (nativeWorklinkState, bool, error) {
	iter := tx.Query(ctx, spanner.Statement{
		SQL:    `SELECT ` + nativeWorklinkStateColumns + ` FROM ` + a.Descriptor.WorklinkTable + ` WHERE ` + a.projectWhereAnd() + `link_id = @link_id LIMIT 1`,
		Params: map[string]interface{}{"project_id": projectID, "link_id": linkID},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return nativeWorklinkState{}, false, nil
	}
	if err != nil {
		return nativeWorklinkState{}, false, fmt.Errorf("read worklink %q in project %q: %w", linkID, projectID, err)
	}
	state, err := nativeScanWorklinkState(row)
	if err != nil {
		return nativeWorklinkState{}, false, fmt.Errorf("decode worklink %q in project %q: %w", linkID, projectID, err)
	}
	return state, true, nil
}

func nativeReadTaskWorklinksTx(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, projectID, taskID string) ([]nativeWorklinkState, error) {
	iter := tx.Query(ctx, spanner.Statement{
		SQL:    `SELECT ` + nativeWorklinkStateColumns + ` FROM ` + a.Descriptor.WorklinkTable + ` WHERE ` + a.projectWhereAnd() + `task_id = @task_id`,
		Params: map[string]interface{}{"project_id": projectID, "task_id": taskID},
	})
	defer iter.Stop()
	var out []nativeWorklinkState
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read worklinks of task %q in project %q: %w", taskID, projectID, err)
		}
		state, err := nativeScanWorklinkState(row)
		if err != nil {
			return nil, fmt.Errorf("decode worklinks of task %q in project %q: %w", taskID, projectID, err)
		}
		out = append(out, state)
	}
}

func nativeWorklinkIsActiveBinding(w nativeWorklinkState) bool {
	return nativeBindingArtifactTypes[strings.ToLower(w.ArtifactType)] && !nativeInactiveWorklinkStatuses[strings.ToLower(strings.TrimSpace(w.Status))]
}

// nativePredecessorMismatch names the first exact-match field on which the
// stored predecessor differs from the caller's expectation; status is checked
// separately because a completed transition legitimately changes it.
func nativePredecessorMismatch(w nativeWorklinkState, want supersedePredecessorIdentity) string {
	for _, f := range []struct{ name, got, want string }{
		{"artifact_type", w.ArtifactType, want.ArtifactType}, {"repo", w.Repo, want.Repo}, {"branch", w.Branch, want.Branch},
		{"worktree_path", w.WorktreePath, want.WorktreePath}, {"thread", w.Thread, want.Thread}, {"session_id", w.SessionID, want.SessionID},
	} {
		if f.got != f.want {
			return f.name
		}
	}
	return ""
}

func nativeSuccessorMatches(w nativeWorklinkState, in supersedeWorklinkInput) bool {
	s := in.Successor
	return w.LinkID == s.LinkID && w.TaskID == in.TaskID && w.ArtifactType == s.ArtifactType && w.ArtifactRef == s.ArtifactRef && w.Repo == s.Repo &&
		w.Branch == s.Branch && w.WorktreePath == s.WorktreePath && w.Thread == s.Thread && w.SessionID == s.SessionID &&
		w.Note == worklinkSupersedeNote+in.PredecessorLinkID && w.Status == worklinkRecordedStatus
}

// nativeHandleSupersedeWorklink retires one valid active binding worklink and
// installs its exact successor in ONE read-write transaction, so no reader can
// observe zero or two active bindings. The caller is a compare-and-set request:
// the task owner/session, the predecessor's every coordinate, and the absence of
// any other active binding for the task and repository are all verified inside
// the transaction. An identical retry converges with rows_affected zero; any
// other request against an already superseded predecessor fails closed and names
// the winner.
func nativeHandleSupersedeWorklink(ctx context.Context, env *Env, in supersedeWorklinkInput) (WorklinkSupersedeResult, error) {
	if err := validateSupersedeWorklinkInput(in); err != nil {
		return WorklinkSupersedeResult{}, err
	}
	if err := requireArtifactKindRegistry(env); err != nil {
		return WorklinkSupersedeResult{}, err
	}
	if _, err := env.ArtifactKinds.Resolve(ctx, worklinkBindingArtifactType); err != nil {
		return WorklinkSupersedeResult{}, err
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil {
		return WorklinkSupersedeResult{}, err
	}
	if adapter == nil {
		return WorklinkSupersedeResult{}, fmt.Errorf("task_graph_supersede_worklink requires fleet_plan_native store")
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return WorklinkSupersedeResult{}, err
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		return WorklinkSupersedeResult{}, err
	}
	mutatedAt := env.Now().UTC()
	var rows int64
	var idempotent bool
	fault := func(stage string) error {
		if nativeSupersedeFault == nil {
			return nil
		}
		return nativeSupersedeFault(stage)
	}
	transition := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		// The function is re-run when Spanner aborts the transaction.
		rows, idempotent = 0, false

		taskIter := tx.Query(ctx, spanner.Statement{SQL: `SELECT owner, session_id FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectWhereAnd() + `id = @id LIMIT 1`, Params: map[string]interface{}{"project_id": projectID, "id": in.TaskID}})
		taskRow, readErr := taskIter.Next()
		taskIter.Stop()
		if readErr == iterator.Done {
			return fmt.Errorf("task %q not found in project %q", in.TaskID, projectID)
		}
		if readErr != nil {
			return fmt.Errorf("read task %q in project %q: %w", in.TaskID, projectID, readErr)
		}
		var owner string
		var session spanner.NullString
		if err := taskRow.Columns(&owner, &session); err != nil {
			return fmt.Errorf("decode task %q in project %q: %w", in.TaskID, projectID, err)
		}
		if owner != in.ExpectedOwner {
			return fmt.Errorf("task %q owner does not match expected_owner", in.TaskID)
		}
		if nullStringValue(session) != in.ExpectedSessionID {
			return fmt.Errorf("task %q session_id does not match expected_session_id", in.TaskID)
		}

		pred, found, err := nativeReadWorklinkStateTx(ctx, tx, adapter, projectID, in.PredecessorLinkID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("worklink %q not found in project %q", in.PredecessorLinkID, projectID)
		}
		if pred.TaskID != in.TaskID {
			return fmt.Errorf("worklink %q belongs to task %q, not requested task %q", in.PredecessorLinkID, pred.TaskID, in.TaskID)
		}
		succ, succExists, err := nativeReadWorklinkStateTx(ctx, tx, adapter, projectID, in.Successor.LinkID)
		if err != nil {
			return err
		}
		taskLinks, err := nativeReadTaskWorklinksTx(ctx, tx, adapter, projectID, in.TaskID)
		if err != nil {
			return err
		}

		if succExists {
			// The only acceptable existing successor is the exact one this
			// request would install, after its own transition committed.
			onlyActive := true
			for _, w := range taskLinks {
				if nativeWorklinkIsActiveBinding(w) && strings.EqualFold(w.Repo, in.Successor.Repo) && w.LinkID != succ.LinkID {
					onlyActive = false
				}
			}
			if pred.Status != worklinkSupersededStatus || nativePredecessorMismatch(pred, in.ExpectedPredecessor) != "" || !nativeSuccessorMatches(succ, in) || !onlyActive {
				return fmt.Errorf("successor link %q collides with an existing worklink that does not match the requested transition", in.Successor.LinkID)
			}
			idempotent = true
			return nil
		}

		if pred.Status == worklinkSupersededStatus {
			conflict := WorklinkSupersedeConflict{TaskID: in.TaskID, PredecessorLinkID: in.PredecessorLinkID, Reason: "a different successor was rejected"}
			for _, w := range taskLinks {
				if w.Note == worklinkSupersedeNote+in.PredecessorLinkID && w.ArtifactType == worklinkBindingArtifactType {
					conflict.WinnerLinkID = w.LinkID
				}
			}
			if conflict.WinnerLinkID == "" {
				conflict.Reason = "it is superseded without a recorded successor"
			}
			return conflict
		}
		if pred.Status != worklinkRecordedStatus {
			return fmt.Errorf("predecessor %q is not active (artifact_status %q)", in.PredecessorLinkID, pred.Status)
		}
		if field := nativePredecessorMismatch(pred, in.ExpectedPredecessor); field != "" {
			return fmt.Errorf("predecessor %s does not match expected_predecessor", field)
		}
		active := 0
		for _, w := range taskLinks {
			if nativeWorklinkIsActiveBinding(w) && strings.EqualFold(w.Repo, pred.Repo) {
				active++
			}
		}
		if active != 1 {
			return fmt.Errorf("task %q has %d active binding worklinks for repository %q; exactly the predecessor is required", in.TaskID, active, pred.Repo)
		}

		if err := fault("before_retire"); err != nil {
			return err
		}
		// The retirement is itself a compare-and-set on the status this transaction
		// read, so the first writer wins even when the store does not serialize the
		// two transactions' reads.
		retired, err := tx.Update(ctx, spanner.Statement{
			SQL: `UPDATE ` + adapter.Descriptor.WorklinkTable + ` SET artifact_status = @status
				WHERE ` + adapter.projectWhereAnd() + `link_id = @link_id AND task_id = @task_id
				AND (artifact_status IS NULL OR artifact_status = @recorded)`,
			Params: map[string]interface{}{"project_id": projectID, "link_id": in.PredecessorLinkID, "task_id": in.TaskID, "status": worklinkSupersededStatus, "recorded": worklinkRecordedStatus},
		})
		if err != nil {
			return fmt.Errorf("retire predecessor %q: %w", in.PredecessorLinkID, err)
		}
		if retired == 0 {
			return errSupersedeLostRace
		}
		if retired != 1 {
			return fmt.Errorf("retire predecessor %q affected %d rows, want 1", in.PredecessorLinkID, retired)
		}
		if err := fault("after_retire"); err != nil {
			return err
		}
		s := in.Successor
		columns, params := nativeBuildWorklinkInsert(s.LinkID, in.TaskID, s.ArtifactType, s.ArtifactRef, map[string]interface{}{
			"thread": s.Thread, "session_id": s.SessionID, "repo": s.Repo, "branch": s.Branch, "worktree_path": s.WorktreePath,
			"note": worklinkSupersedeNote + in.PredecessorLinkID,
		}, mutatedAt)
		params["project_id"] = projectID
		placeholders := make([]string, len(columns))
		for i, col := range columns {
			placeholders[i] = "@" + col
		}
		installed, err := tx.Update(ctx, spanner.Statement{
			SQL: `INSERT INTO ` + adapter.Descriptor.WorklinkTable + `
				(` + adapter.projectInsertColumns() + strings.Join(columns, ", ") + `)
				VALUES (` + adapter.projectInsertValues() + strings.Join(placeholders, ", ") + `)`,
			Params: params,
		})
		if spanner.ErrCode(err) == codes.AlreadyExists {
			return errSupersedeLostRace
		}
		if err != nil {
			return fmt.Errorf("install successor %q: %w", s.LinkID, err)
		}
		if installed != 1 {
			return fmt.Errorf("install successor %q affected %d rows, want 1", s.LinkID, installed)
		}
		if err := fault("after_install"); err != nil {
			return err
		}
		rows = retired + installed
		return nil
	}
	// A lost race is resolved by one fresh read-write pass: it observes the
	// winner's committed state and either converges on the identical transition
	// or fails closed naming the winner. The second pass cannot lose again to the
	// same winner, so the loop is bounded.
	for attempt := 0; attempt < 3; attempt++ {
		_, err = client.ReadWriteTransaction(ctx, transition)
		if !errors.Is(err, errSupersedeLostRace) {
			break
		}
	}
	if err != nil {
		return WorklinkSupersedeResult{}, fmt.Errorf("supersede task-plan worklink: %w", err)
	}
	native := nativeMutationReceipt(env, projectID, "supersede_worklink", "artifact", in.Successor.LinkID, adapter.Descriptor.WorklinkTable, rows, mutatedAt)
	native.Evidence = fmt.Sprintf("task_id=%s;predecessor_link_id=%s;successor_link_id=%s;idempotent=%t;epoch=%s", in.TaskID, in.PredecessorLinkID, in.Successor.LinkID, idempotent, mutatedAt.Format(time.RFC3339Nano))
	return WorklinkSupersedeResult{
		NativeMutationReceipt: &native,
		SupersedeReceipt: &WorklinkSupersedeReceipt{
			ReceiptKind: "worklink_supersede", Issuer: "task-graph", ProjectID: projectID, TaskID: in.TaskID,
			PredecessorLinkID: in.PredecessorLinkID, SuccessorLinkID: in.Successor.LinkID, PredecessorStatus: worklinkSupersededStatus,
			Idempotent: idempotent, RowsAffected: rows, Epoch: mutatedAt,
		},
	}, nil
}

func validateNativeWorklinkArchiveTarget(taskID, linkID, ownedTaskID, artifactStatus string, taskExists, linkExists bool) (bool, error) {
	if !taskExists {
		return false, fmt.Errorf("task %q not found", taskID)
	}
	if !linkExists {
		return false, fmt.Errorf("worklink %q not found", linkID)
	}
	if ownedTaskID != taskID {
		return false, fmt.Errorf("worklink %q belongs to task %q, not requested task %q", linkID, ownedTaskID, taskID)
	}
	return artifactStatus == "archived", nil
}

func nativeHandleSetStatus(ctx context.Context, env *Env, in setStatusInput) (MutationWriteResult, error) {
	if in.TaskID == "" || in.Status == "" {
		return MutationWriteResult{}, fmt.Errorf("task_id, status required")
	}
	return nativeUpdateTaskFields(ctx, env, in.ProjectID, in.TaskID, map[string]interface{}{"status": in.Status})
}

type writerHandoverHistory struct {
	Kind                  string `json:"kind"`
	Version               int    `json:"version"`
	ProjectID             string `json:"project_id"`
	TaskID                string `json:"task_id"`
	BindingLinkID         string `json:"binding_link_id"`
	ArtifactType          string `json:"artifact_type"`
	ArtifactStatus        string `json:"artifact_status"`
	ArtifactRef           string `json:"artifact_ref"`
	Repo                  string `json:"repo"`
	Branch                string `json:"branch"`
	WorktreePath          string `json:"worktree_path"`
	FromThread            string `json:"from_thread"`
	FromSessionID         string `json:"from_session_id"`
	ToThread              string `json:"to_thread"`
	ToSessionID           string `json:"to_session_id"`
	ExpectedTaskOwner     string `json:"expected_task_owner"`
	ExpectedTaskSessionID string `json:"expected_task_session_id"`
	ExpectedTaskRepo      string `json:"expected_task_repo"`
	TargetTaskOwner       string `json:"target_task_owner"`
	TargetTaskSessionID   string `json:"target_task_session_id"`
	TargetTaskRepo        string `json:"target_task_repo"`
	PriorNote             string `json:"prior_note,omitempty"`
}

// nativeWriterHandoverFault is a test seam used only to prove that every
// failure between the history, task and binding writes rolls the transaction
// back. Production leaves it nil.
var nativeWriterHandoverFault func(stage string) error
var errWriterHandoverLostRace = errors.New("writer_handover lost a concurrent race")

func nativeWriterHandoverHistory(projectID string, in reownInput, binding nativeWorklinkState) (string, string) {
	w := in.WriterHandover
	history := writerHandoverHistory{
		Kind: "writer_handover", Version: 1, ProjectID: projectID, TaskID: in.TaskID,
		BindingLinkID: w.LinkID, ArtifactType: w.ArtifactType, ArtifactStatus: w.ArtifactStatus,
		ArtifactRef: binding.ArtifactRef, Repo: w.Repo, Branch: w.Branch, WorktreePath: w.WorktreePath,
		FromThread: w.Thread, FromSessionID: w.SessionID, ToThread: w.TargetThread, ToSessionID: in.SessionID,
		ExpectedTaskOwner: in.ExpectedOwner, ExpectedTaskSessionID: in.ExpectedSessionID, ExpectedTaskRepo: in.ExpectedRepo,
		TargetTaskOwner: in.Owner, TargetTaskSessionID: in.SessionID, TargetTaskRepo: in.Repo, PriorNote: binding.Note,
	}
	b, _ := json.Marshal(history) // a fixed scalar-only struct cannot fail to marshal
	note := writerHandoverHistoryNote + string(b)
	sum := sha256.Sum256([]byte(note))
	return "HND-" + hex.EncodeToString(sum[:])[:60], note
}

func nativeWriterBindingMatchesExpected(stored nativeWorklinkState, in reownInput) bool {
	w := in.WriterHandover
	return stored.LinkID == w.LinkID && stored.TaskID == in.TaskID && stored.ArtifactType == w.ArtifactType &&
		stored.Status == w.ArtifactStatus && stored.Repo == w.Repo && stored.Branch == w.Branch &&
		stored.WorktreePath == w.WorktreePath && stored.Thread == w.Thread && stored.SessionID == w.SessionID
}

func nativeWriterBindingMatchesTarget(stored nativeWorklinkState, in reownInput) bool {
	w := in.WriterHandover
	return stored.LinkID == w.LinkID && stored.TaskID == in.TaskID && stored.ArtifactType == w.ArtifactType &&
		stored.Status == w.ArtifactStatus && stored.Repo == w.Repo && stored.Branch == w.Branch &&
		stored.WorktreePath == w.WorktreePath && stored.Thread == w.TargetThread && stored.SessionID == in.SessionID
}

func nativeWriterHistoryMatches(stored nativeWorklinkState, historyID, historyNote string, in reownInput) bool {
	w := in.WriterHandover
	return stored.LinkID == historyID && stored.TaskID == in.TaskID && stored.ArtifactType == writerHandoverHistoryType &&
		stored.ArtifactRef == "writer_handover:"+w.LinkID+":"+historyID && stored.Status == worklinkRecordedStatus &&
		stored.Repo == w.Repo && stored.Branch == w.Branch && stored.WorktreePath == w.WorktreePath &&
		stored.Thread == w.Thread && stored.SessionID == w.SessionID && stored.Note == historyNote
}

// nativeNormalizeRepository is byte-for-byte equivalent in behavior to the
// installed work-identity.mjs normalizeRepository consumer: trim, normalize
// separators, remove trailing slashes and a .git suffix, then compare the last
// path/colon component case-insensitively. Handover uses it only to group
// competing writers; the selected binding still matches every stored/requested
// coordinate exactly and no stored repository value is rewritten.
func nativeNormalizeRepository(value string) string {
	cleaned := strings.TrimSpace(value)
	if cleaned == "" {
		return ""
	}
	cleaned = strings.ReplaceAll(cleaned, `\`, "/")
	cleaned = strings.TrimRight(cleaned, "/")
	if strings.HasSuffix(strings.ToLower(cleaned), ".git") {
		cleaned = cleaned[:len(cleaned)-len(".git")]
	}
	pathParts := strings.Split(cleaned, "/")
	lastPath := cleaned
	for i := len(pathParts) - 1; i >= 0; i-- {
		if pathParts[i] != "" {
			lastPath = pathParts[i]
			break
		}
	}
	colonParts := strings.Split(lastPath, ":")
	return strings.ToLower(colonParts[len(colonParts)-1])
}

func nativeWriterBindingCount(taskLinks []nativeWorklinkState, repo string) int {
	count := 0
	normalizedRepo := nativeNormalizeRepository(repo)
	for _, link := range taskLinks {
		// Writer handover must reject an active identity-bearing competitor even
		// when its branch/path registration is incomplete. Rows without writer
		// thread/session are legacy evidence and do not identify a competing
		// writer. This stricter count belongs only to handover selection; the
		// pre-existing supersede lifecycle keeps its valid-binding definition.
		if nativeWorklinkIsActiveBinding(link) && nativeNormalizeRepository(link.Repo) == normalizedRepo &&
			strings.TrimSpace(link.Thread) != "" && strings.TrimSpace(link.SessionID) != "" {
			count++
		}
	}
	return count
}

func nativeTaskTupleMatches(owner, sessionID, repo, wantOwner, wantSessionID, wantRepo string) bool {
	return owner == wantOwner && sessionID == wantSessionID && repo == wantRepo
}

func nativeHandleReown(ctx context.Context, env *Env, in reownInput) (MutationWriteResult, error) {
	if err := validateReownInput(in); err != nil {
		return MutationWriteResult{}, err
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	if in.WriterHandover != nil {
		if err := requireArtifactKindRegistry(env); err != nil {
			return MutationWriteResult{}, err
		}
		if _, err := env.ArtifactKinds.Resolve(ctx, writerHandoverHistoryType); err != nil {
			return MutationWriteResult{}, err
		}
	}
	now := env.Now().UTC()
	var rows int64
	var idempotent bool
	var historyID, historyNote string
	transition := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		rows, idempotent, historyID, historyNote = 0, false, "", ""
		iter := tx.Query(ctx, spanner.Statement{SQL: `SELECT owner, session_id, repo FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectWhereAnd() + `id = @id LIMIT 1`, Params: map[string]interface{}{"project_id": projectID, "id": in.TaskID}})
		row, e := iter.Next()
		iter.Stop()
		if e == iterator.Done {
			return fmt.Errorf("task %q not found in project %q", in.TaskID, projectID)
		}
		if e != nil {
			return e
		}
		var owner string
		var session, repo spanner.NullString
		if e := row.Columns(&owner, &session, &repo); e != nil {
			return e
		}
		currentSession, currentRepo := nullStringValue(session), nullStringValue(repo)
		expectedErr := validateReownExpectedTuple(in, owner, currentSession, currentRepo)
		if in.WriterHandover == nil && expectedErr != nil {
			return expectedErr
		}
		if in.WriterHandover != nil && expectedErr != nil && !nativeTaskTupleMatches(owner, currentSession, currentRepo, in.Owner, in.SessionID, in.Repo) {
			return expectedErr
		}

		taskLinks, e := nativeReadTaskWorklinksTx(ctx, tx, adapter, projectID, in.TaskID)
		if e != nil {
			return e
		}
		if in.WriterHandover == nil {
			taskChanges := !nativeTaskTupleMatches(owner, currentSession, currentRepo, in.Owner, in.SessionID, in.Repo)
			for _, link := range taskLinks {
				if !nativeIsValidActiveBinding(link) {
					continue
				}
				if taskChanges || link.SessionID != in.SessionID || !strings.EqualFold(link.Repo, in.Repo) {
					return fmt.Errorf("task %q has a protected active binding; use writer_handover so task and writer identity change coherently", in.TaskID)
				}
			}
			n, e := tx.Update(ctx, spanner.Statement{SQL: `UPDATE ` + adapter.Descriptor.TaskTable + ` SET owner = @owner, session_id = @session_id, repo = @repo, updated_at = @updated_at WHERE ` + adapter.projectWhereAnd() + `id = @id`, Params: map[string]interface{}{"project_id": projectID, "id": in.TaskID, "owner": in.Owner, "session_id": in.SessionID, "repo": in.Repo, "updated_at": now}})
			rows = n
			return e
		}

		w := in.WriterHandover
		binding, found, e := nativeReadWorklinkStateTx(ctx, tx, adapter, projectID, w.LinkID)
		if e != nil {
			return e
		}
		if !found {
			return fmt.Errorf("writer_handover worklink %q not found in project %q", w.LinkID, projectID)
		}
		if binding.TaskID != in.TaskID {
			return fmt.Errorf("writer_handover worklink %q belongs to task %q, not requested task %q", w.LinkID, binding.TaskID, in.TaskID)
		}
		if got := nativeWriterBindingCount(taskLinks, w.Repo); got != 1 || !nativeIsValidActiveBinding(binding) {
			return fmt.Errorf("task %q has %d active writer-identity binding worklinks for repository %q; exactly one valid writer_handover.link_id is required", in.TaskID, got, w.Repo)
		}
		historyID, historyNote = nativeWriterHandoverHistory(projectID, in, binding)
		history, historyFound, e := nativeReadWorklinkStateTx(ctx, tx, adapter, projectID, historyID)
		if e != nil {
			return e
		}
		taskAtTarget := nativeTaskTupleMatches(owner, currentSession, currentRepo, in.Owner, in.SessionID, in.Repo)
		bindingAtTarget := nativeWriterBindingMatchesTarget(binding, in)
		if historyFound {
			if !nativeWriterHistoryMatches(history, historyID, historyNote, in) || !taskAtTarget || !bindingAtTarget {
				return fmt.Errorf("writer_handover history %q collides with state that does not match the exact completed request", historyID)
			}
			idempotent = true
			return nil
		}
		// A current task tuple that differs from expected is accepted only as
		// an exact response-loss retry proven by the immutable history above.
		// Split recovery instead supplies the already-reowned tuple as the
		// expected tuple and therefore reaches this point with expectedErr nil.
		if expectedErr != nil {
			return expectedErr
		}
		if !nativeWriterBindingMatchesExpected(binding, in) {
			return fmt.Errorf("writer_handover worklink %q does not match its exact historical tuple", w.LinkID)
		}

		columns, historyParams := nativeBuildWorklinkInsert(historyID, in.TaskID, writerHandoverHistoryType, "writer_handover:"+w.LinkID+":"+historyID, map[string]interface{}{
			"thread": w.Thread, "note": historyNote, "session_id": w.SessionID, "repo": w.Repo,
			"branch": w.Branch, "worktree_path": w.WorktreePath,
		}, now)
		historyParams["project_id"] = projectID
		placeholders := make([]string, len(columns))
		for i, column := range columns {
			placeholders[i] = "@" + column
		}
		inserted, e := tx.Update(ctx, spanner.Statement{
			SQL: `INSERT INTO ` + adapter.Descriptor.WorklinkTable + ` (` + adapter.projectInsertColumns() + strings.Join(columns, ", ") + `)
				VALUES (` + adapter.projectInsertValues() + strings.Join(placeholders, ", ") + `)`,
			Params: historyParams,
		})
		if spanner.ErrCode(e) == codes.AlreadyExists {
			return errWriterHandoverLostRace
		}
		if e != nil {
			return fmt.Errorf("insert writer_handover history %q: %w", historyID, e)
		}
		if inserted != 1 {
			return fmt.Errorf("insert writer_handover history %q affected %d rows, want 1", historyID, inserted)
		}
		rows += inserted
		if nativeWriterHandoverFault != nil {
			if e := nativeWriterHandoverFault("after_history"); e != nil {
				return e
			}
		}
		if !taskAtTarget {
			n, e := tx.Update(ctx, spanner.Statement{
				SQL:    `UPDATE ` + adapter.Descriptor.TaskTable + ` SET owner = @owner, session_id = @session_id, repo = @repo, updated_at = @updated_at WHERE ` + adapter.projectWhereAnd() + `id = @id`,
				Params: map[string]interface{}{"project_id": projectID, "id": in.TaskID, "owner": in.Owner, "session_id": in.SessionID, "repo": in.Repo, "updated_at": now},
			})
			if e != nil {
				return e
			}
			if n != 1 {
				return fmt.Errorf("update task %q affected %d rows, want 1", in.TaskID, n)
			}
			rows += n
		}
		if nativeWriterHandoverFault != nil {
			if e := nativeWriterHandoverFault("after_task"); e != nil {
				return e
			}
		}
		n, e := tx.Update(ctx, spanner.Statement{
			SQL: `UPDATE ` + adapter.Descriptor.WorklinkTable + ` SET thread = @thread, session_id = @session_id
				WHERE ` + adapter.projectWhereAnd() + `link_id = @link_id AND task_id = @task_id
				AND artifact_type = @artifact_type AND repo = @repo AND branch = @branch AND worktree_path = @worktree_path
				AND thread = @from_thread AND session_id = @from_session_id
				AND (artifact_status IS NULL OR artifact_status = @artifact_status)`,
			Params: map[string]interface{}{
				"project_id": projectID, "link_id": w.LinkID, "task_id": in.TaskID,
				"artifact_type": w.ArtifactType, "artifact_status": w.ArtifactStatus, "repo": w.Repo,
				"branch": w.Branch, "worktree_path": w.WorktreePath, "from_thread": w.Thread,
				"from_session_id": w.SessionID, "thread": w.TargetThread, "session_id": in.SessionID,
			},
		})
		if e != nil {
			return e
		}
		if n != 1 {
			return fmt.Errorf("update writer_handover worklink %q affected %d rows, want 1", w.LinkID, n)
		}
		rows += n
		if nativeWriterHandoverFault != nil {
			if e := nativeWriterHandoverFault("after_writer"); e != nil {
				return e
			}
		}
		return nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		_, err = client.ReadWriteTransaction(ctx, transition)
		if !errors.Is(err, errWriterHandoverLostRace) {
			break
		}
	}
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("reown task-plan task: %w", err)
	}
	rec := nativeMutationReceipt(env, projectID, "reown_task", "task", in.TaskID, adapter.Descriptor.TaskTable, rows, now)
	rec.Evidence = fmt.Sprintf("owner=%s;session_id=%s;repo=%s;expected_owner=%s;expected_session_id=%s;expected_repo=%s;epoch=%s", in.Owner, in.SessionID, in.Repo, in.ExpectedOwner, in.ExpectedSessionID, in.ExpectedRepo, now.Format(time.RFC3339Nano))
	out := nativeMutationWriteResult(rec)
	if in.WriterHandover != nil {
		w := in.WriterHandover
		out.WriterHandoverReceipt = &WriterHandoverReceipt{
			ReceiptKind: "writer_handover", Issuer: "task-graph", ProjectID: projectID, TaskID: in.TaskID,
			BindingLinkID: w.LinkID, HistoryLinkID: historyID, FromThread: w.Thread, FromSessionID: w.SessionID,
			ToThread: w.TargetThread, ToSessionID: in.SessionID, Idempotent: idempotent, RowsAffected: rows, Epoch: now,
		}
		rec.Evidence = fmt.Sprintf("%s;writer_link_id=%s;writer_history_link_id=%s;target_thread=%s;idempotent=%t", rec.Evidence, w.LinkID, historyID, w.TargetThread, idempotent)
		out.NativeMutationReceipt = &rec
	}
	return out, nil
}

// nativeHandleClaim performs a first-writer-wins task claim in one native
// Spanner transaction. The read and conditional update share the transaction,
// so concurrent claimers cannot both observe an unowned task. Existing fields
// are preserved; only owner, session_id, repo, and updated_at are written.
func nativeHandleClaim(ctx context.Context, env *Env, in claimInput) (MutationWriteResult, error) {
	if err := validateClaimInput(in); err != nil {
		return MutationWriteResult{}, err
	}
	if in.Binding != nil {
		if in.Binding.Repo != "" && in.Binding.Repo != in.Repo || in.Binding.SessionID != "" && in.Binding.SessionID != in.SessionID {
			return MutationWriteResult{}, fmt.Errorf("binding repo/session_id must match claim fields")
		}
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	now := env.Now().UTC()
	var rows int64
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{SQL: `SELECT owner, session_id, repo FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectWhereAnd() + `id = @id LIMIT 1`, Params: map[string]interface{}{"project_id": projectID, "id": in.TaskID}}
		iter := tx.Query(ctx, stmt)
		row, readErr := iter.Next()
		iter.Stop()
		if readErr == iterator.Done {
			return fmt.Errorf("task %q not found in project %q", in.TaskID, projectID)
		}
		if readErr != nil {
			return fmt.Errorf("read task %q for claim: %w", in.TaskID, readErr)
		}
		var currentOwner string
		var currentSession, currentRepo spanner.NullString
		if err := row.Columns(&currentOwner, &currentSession, &currentRepo); err != nil {
			return fmt.Errorf("decode task %q for claim: %w", in.TaskID, err)
		}
		decision, decisionErr := taskClaimDecision(in.TaskID, currentOwner, nullStringValue(currentSession), nullStringValue(currentRepo), in.Owner, in.SessionID, in.Repo)
		if decisionErr != nil {
			return decisionErr
		}
		if err := validateClaimReceiptBinding(decision, in.Binding); err != nil {
			return err
		}
		if decision == "idempotent" {
			rows = 0
			return nil
		}
		update := spanner.Statement{SQL: `UPDATE ` + adapter.Descriptor.TaskTable + ` SET owner = @owner, session_id = @session_id, repo = @repo, updated_at = @updated_at WHERE ` + adapter.projectWhereAnd() + `id = @id AND (owner = '' OR owner = 'UNOWNED')`, Params: map[string]interface{}{"project_id": projectID, "id": in.TaskID, "owner": in.Owner, "session_id": in.SessionID, "repo": in.Repo, "updated_at": now}}
		n, err := tx.Update(ctx, update)
		rows = n
		return err
	})
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("claim task-plan task: %w", err)
	}
	rec := nativeMutationReceipt(env, projectID, "claim_task", "task", in.TaskID, adapter.Descriptor.TaskTable, rows, now)
	rec.Evidence = fmt.Sprintf("owner=%s;session_id=%s;repo=%s;epoch=%s", in.Owner, in.SessionID, in.Repo, now.Format(time.RFC3339Nano))
	if in.Binding != nil {
		rec.Evidence += fmt.Sprintf(";branch=%s;worktree_path=%s;host=%s;account=%s;harness=%s;thread=%s", in.Binding.Branch, in.Binding.WorktreePath, in.Binding.Host, in.Binding.Account, in.Binding.Harness, in.Binding.Thread)
	}
	out := nativeMutationWriteResult(rec)
	claim := &ClaimReceipt{ReceiptKind: "claim", Issuer: "task-graph", ProjectID: projectID, TaskID: in.TaskID, Owner: in.Owner, SessionID: in.SessionID, Repo: in.Repo, Epoch: now, RowsAffected: rows}
	if in.Binding != nil {
		claim.Branch, claim.WorktreePath, claim.Host, claim.Account, claim.Harness, claim.Thread = in.Binding.Branch, in.Binding.WorktreePath, in.Binding.Host, in.Binding.Account, in.Binding.Harness, in.Binding.Thread
	}
	out.ClaimReceipt = claim
	return out, nil
}

func validateClaimInput(in claimInput) error {
	if strings.TrimSpace(in.TaskID) == "" || strings.TrimSpace(in.Owner) == "" || strings.TrimSpace(in.SessionID) == "" || strings.TrimSpace(in.Repo) == "" || strings.EqualFold(strings.TrimSpace(in.Owner), "UNOWNED") {
		return fmt.Errorf("task_id, owner, session_id, repo required")
	}
	return nil
}

// validateClaimReceiptBinding prevents the server from issuing two receipts
// that attest different ephemeral execution coordinates for one durable claim.
// Tasks persists owner/session_id/repo, so an idempotent retry can prove that
// tuple. Branch/worktree/host/account/harness/thread are intentionally outside
// the Tasks schema and may therefore be attested only by the transaction that
// actually changes the task from UNOWNED to owned. The caller records those
// coordinates durably through task_graph_add_worklink after the claim.
func validateClaimReceiptBinding(decision string, binding *ClaimBinding) error {
	if decision != "idempotent" || binding == nil {
		return nil
	}
	if binding.Branch != "" || binding.WorktreePath != "" || binding.Host != "" || binding.Account != "" || binding.Harness != "" || binding.Thread != "" {
		return fmt.Errorf("idempotent claim retry cannot re-attest branch, worktree_path, host, account, harness, or thread; omit optional binding and read the registered worklink")
	}
	return nil
}

func nullStringValue(v spanner.NullString) string {
	if v.Valid {
		return v.StringVal
	}
	return ""
}

// taskClaimDecision centralizes the first-writer-wins decision so its edge
// cases remain testable without a database. It returns "claim", "idempotent",
// or a typed collision error.
func taskClaimDecision(taskID, currentOwner, currentSession, currentRepo, owner, sessionID, repo string) (string, error) {
	if currentOwner == owner && currentSession == sessionID && currentRepo == repo {
		return "idempotent", nil
	}
	if currentOwner != "" && currentOwner != "UNOWNED" {
		return "", TaskClaimCollisionDetail{TaskID: taskID, Owner: currentOwner, SessionID: currentSession, Repo: currentRepo}
	}
	return "claim", nil
}

func nativeHandleCreate(ctx context.Context, env *Env, in createInput) (MutationWriteResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	switch in.NodeKind {
	case "task":
		taskID := firstNonEmpty(in.StableID, stringFromMap(in.Contract, "canonical_name"), stringFromMap(in.Contract, "task_ref"))
		if taskID == "" {
			return MutationWriteResult{}, fmt.Errorf("task stable_id or contract.canonical_name required")
		}
		payloadParentID := stringFromMap(in.Payload, "parent_id")
		if in.ParentRef != "" && payloadParentID != "" && in.ParentRef != payloadParentID {
			return MutationWriteResult{}, fmt.Errorf("parent_ref %q does not match payload.parent_id %q", in.ParentRef, payloadParentID)
		}
		parentID := firstNonEmpty(in.ParentRef, payloadParentID)
		if parentID == taskID {
			return MutationWriteResult{}, fmt.Errorf("task %q cannot be its own parent", taskID)
		}
		if err := requireTaskStatusRegistry(env); err != nil {
			return MutationWriteResult{}, err
		}
		statusLiteral := stringFromMap(in.Payload, "status")
		if statusLiteral == "" {
			return MutationWriteResult{}, fmt.Errorf("task status required")
		}
		if _, err := env.TaskStatuses.Resolve(ctx, statusLiteral); err != nil {
			return MutationWriteResult{}, err
		}
		now := env.Now().UTC()
		client, err := adapter.getClient(ctx)
		if err != nil {
			return MutationWriteResult{}, err
		}
		columns, taskParams := nativeBuildTaskInsert(taskID, parentID, in.Contract, in.Payload, now)
		taskParams["project_id"] = projectID
		placeholders := make([]string, len(columns))
		for i, col := range columns {
			placeholders[i] = "@" + col
		}
		var rows int64
		_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if parentID != "" {
				if err := nativeRequireTaskInProject(ctx, tx, adapter, projectID, parentID); err != nil {
					return err
				}
			}
			stmt := spanner.Statement{
				SQL: `INSERT INTO ` + adapter.Descriptor.TaskTable + `
					(` + adapter.projectInsertColumns() + strings.Join(columns, ", ") + `)
					VALUES (` + adapter.projectInsertValues() + strings.Join(placeholders, ", ") + `)`,
				Params: taskParams,
			}
			n, err := tx.Update(ctx, stmt)
			rows = n
			return err
		})
		if err != nil {
			return MutationWriteResult{}, fmt.Errorf("insert task-plan task: %w", err)
		}
		return nativeMutationWriteResult(nativeMutationReceipt(env, projectID, "insert_task", "task", taskID, adapter.Descriptor.TaskTable, rows, now)), nil
	case "artifact":
		return nativeHandleAddWorklink(ctx, env, nativeArtifactCreateInput(projectID, in.StableID, in.Contract, in.Payload))
	default:
		return MutationWriteResult{}, fmt.Errorf("task-plan native supports task/artifact node_kind, got %q", in.NodeKind)
	}
}

func nativeHandleUpdate(ctx context.Context, env *Env, in updateInput) (MutationWriteResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	if in.NodeID == "" {
		return MutationWriteResult{}, fmt.Errorf("node_id required")
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	// Writer-handover history is immutable evidence. Check its reserved note
	// through the smallest possible projection before expanding a generic
	// Node. The write path repeats this check inside its transaction, so a
	// concurrent handover cannot race this preflight into a mutation. A reserved
	// evidence row also wins an otherwise ambiguous cross-table ID collision.
	worklinkFound, reserved, err := nativeWorklinkHistoryGuard(ctx, adapter, projectID, in.NodeID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	if reserved {
		return MutationWriteResult{}, errWriterHandoverHistory()
	}
	taskFound, err := nativeNodeIDExists(ctx, adapter, projectID, adapter.Descriptor.TaskTable, "id", in.NodeID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	if taskFound {
		return nativeUpdateTaskFields(ctx, env, projectID, in.NodeID, in.Payload)
	}
	if worklinkFound {
		return nativeUpdateWorklinkFields(ctx, env, projectID, in.NodeID, in.Contract, in.Payload)
	}
	return MutationWriteResult{}, fmt.Errorf("node %q not found in project %q in task-plan native store", in.NodeID, projectID)
}

func nativeWorklinkHistoryGuard(ctx context.Context, adapter *SpannerSubstrateAdapter, projectID, linkID string) (bool, bool, error) {
	res, err := adapter.ExecuteReadOnlyQuery(ctx,
		`SELECT note FROM `+adapter.Descriptor.WorklinkTable+` WHERE `+adapter.projectWhereAnd()+`link_id = @link_id LIMIT 1`,
		map[string]interface{}{"project_id": projectID, "link_id": linkID},
	)
	if err != nil {
		return false, false, fmt.Errorf("read worklink history guard: %w", err)
	}
	if len(res.Rows) == 0 {
		return false, false, nil
	}
	return true, namesWriterHandoverHistory(stringFromMap(res.Rows[0], "note")), nil

}

func nativeNodeIDExists(ctx context.Context, adapter *SpannerSubstrateAdapter, projectID, table, idColumn, nodeID string) (bool, error) {
	res, err := adapter.ExecuteReadOnlyQuery(ctx,
		`SELECT `+idColumn+` FROM `+table+` WHERE `+adapter.projectWhereAnd()+idColumn+` = @node_id LIMIT 1`,
		map[string]interface{}{"project_id": projectID, "node_id": nodeID},
	)
	if err != nil {
		return false, fmt.Errorf("read native node existence: %w", err)
	}
	return len(res.Rows) != 0, nil
}

func nativeHandleLink(ctx context.Context, env *Env, in linkInput) (MutationWriteResult, error) {
	if in.SourceID == "" || in.TargetID == "" || in.EdgeLabel == "" {
		return MutationWriteResult{}, fmt.Errorf("source_id, target_id, edge_label required")
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, in.ProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	switch in.EdgeLabel {
	case "advances":
		return nativeLinkWorklinkToTask(ctx, env, projectID, in.SourceID, in.TargetID)
	case "parent_of":
		return nativeLinkParent(ctx, env, projectID, in.SourceID, in.TargetID)
	case "child_of":
		return nativeLinkParent(ctx, env, projectID, in.TargetID, in.SourceID)
	case "gates":
		return nativeInsertDep(ctx, env, projectID, in.TargetID, in.SourceID)
	case "gated_by":
		return nativeInsertDep(ctx, env, projectID, in.SourceID, in.TargetID)
	default:
		return MutationWriteResult{}, fmt.Errorf("task-plan native link unsupported edge_label %q", in.EdgeLabel)
	}
}

func nativeLinkWorklinkToTask(ctx context.Context, env *Env, projectID, linkID, taskID string) (MutationWriteResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := nativeRequireTaskInProject(ctx, tx, adapter, projectID, taskID); err != nil {
			return err
		}
		if err := nativeRequireWorklinkInProject(ctx, tx, adapter, projectID, linkID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return MutationWriteResult{}, err
	}
	return nativeUpdateWorklinkFields(ctx, env, projectID, linkID, nil, map[string]interface{}{"task_id": taskID})
}

func nativeLinkParent(ctx context.Context, env *Env, projectID, parentID, childID string) (MutationWriteResult, error) {
	return nativeUpdateTaskFields(ctx, env, projectID, childID, map[string]interface{}{"parent_id": parentID})
}

// nativeTaskFieldColumns maps every task-plan-native task payload field this
// store accepts to its Tasks table column. BOTH the CREATE (nativeBuildTaskInsert)
// and UPDATE (nativeBuildTaskUpdate) native write paths are keyed off this ONE
// map so they cannot silently diverge on which payload fields persist — see
// P3-GT-FP-CREATE-DROPS-FIELDS, where CREATE's INSERT independently named its
// own field list and drifted out of sync with UPDATE's allow-list:
// is_critical_path, session_id, repo, and the payload.level alias for layer
// were all accepted by UPDATE and silently dropped by CREATE. Adding a field
// here alone is NOT enough to make it create-able — nativeBuildTaskInsert
// still has to read it — but TestNativeTaskCreateUpdateFieldParity iterates
// this map and fails loudly if the two builders ever disagree on it again.
//
// "layer" and "level" both target the layer column deliberately: UPDATE has
// long accepted "level" as a caller-facing alias for "layer" (matching the
// contract.level naming used elsewhere), and CREATE now honors the same
// alias — see nativeBuildTaskInsert.
var nativeTaskFieldColumns = map[string]string{
	"title":            "title",
	"pillar":           "pillar",
	"layer":            "layer",
	"level":            "layer",
	"status":           "status",
	"owner":            "owner",
	"parent_id":        "parent_id",
	"is_gap":           "is_gap",
	"gap_label":        "gap_label",
	"note":             "note",
	"session_id":       "session_id",
	"repo":             "repo",
	"is_critical_path": "is_critical_path",
}

// nativeBuildTaskUpdate renders the SET-clause assignments and params for a
// task-plan-native task UPDATE from a caller-supplied fields map, keyed off
// nativeTaskFieldColumns. Pure/no I/O so it is directly unit-testable without
// a Spanner emulator. A field not present in nativeTaskFieldColumns is
// silently ignored — see gt-w1-fp-create-fields.md for why this PR does not
// change that to a hard rejection (the get→update round trip carries
// read-only echo keys like fleet_created_at that were never meant to be
// written, and the create tool's own docstring declares payload open-ended).
func nativeBuildTaskUpdate(fields map[string]interface{}) (assignments []string, params map[string]interface{}) {
	params = map[string]interface{}{}
	for key, val := range fields {
		col, ok := nativeTaskFieldColumns[key]
		if !ok {
			continue
		}
		assignments = append(assignments, col+" = @"+col)
		params[col] = nativeTaskUpdateParam(col, val)
	}
	return assignments, params
}

// nativeTaskUpdateParam preserves the SQL type of an explicit null for the
// one nullable BOOL in the Tasks mutation surface. The generic nativeParam
// cannot infer a nil value's target column and therefore uses NullString;
// binding that value to is_critical_path either fails at Spanner's type gate
// or tempts callers to replace the null with false before updating.
func nativeTaskUpdateParam(column string, value interface{}) interface{} {
	if column == "is_critical_path" && value == nil {
		return spanner.NullBool{Valid: false}
	}
	return nativeParam(value)
}

func nativeRequestedParent(fields map[string]interface{}) (string, bool, error) {
	value, present := fields["parent_id"]
	if !present {
		return "", false, nil
	}
	if value == nil {
		return "", true, nil
	}
	parentID, ok := value.(string)
	if !ok {
		return "", true, fmt.Errorf("task parent_id must be a string or null, got %T", value)
	}
	return parentID, true, nil
}

func nativeGuardTaskOwnershipTuple(fields map[string]interface{}, owner, sessionID, repo string) error {
	for _, field := range []struct {
		name    string
		current string
	}{
		{name: "owner", current: owner},
		{name: "session_id", current: sessionID},
		{name: "repo", current: repo},
	} {
		value, present := fields[field.name]
		if !present {
			continue
		}
		requested, ok := value.(string)
		if !ok || requested != field.current {
			return fmt.Errorf("task %s may change only through task_graph_claim or task_graph_reown", field.name)
		}
	}
	return nil
}

// nativeValidateParentAssignmentTx proves that parentID exists in the same
// project and that following its parent chain cannot reach childID. The reads
// share the transaction with the eventual child update, so a Spanner retry
// revalidates the tree against the winning concurrent write.
func nativeValidateParentAssignmentTx(ctx context.Context, tx *spanner.ReadWriteTransaction, adapter *SpannerSubstrateAdapter, projectID, childID, parentID string) error {
	if parentID == "" {
		return nil
	}
	visited := map[string]bool{childID: true}
	for current := parentID; current != ""; {
		if visited[current] {
			return fmt.Errorf("setting task %q parent to %q would create a parent cycle", childID, parentID)
		}
		visited[current] = true
		iter := tx.Query(ctx, spanner.Statement{
			SQL:    `SELECT parent_id FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectWhereAnd() + `id = @id LIMIT 1`,
			Params: map[string]interface{}{"project_id": projectID, "id": current},
		})
		row, err := iter.Next()
		iter.Stop()
		if err == iterator.Done {
			return fmt.Errorf("task %q not found in project %q", current, projectID)
		}
		if err != nil {
			return fmt.Errorf("read parent chain task %q in project %q: %w", current, projectID, err)
		}
		var next spanner.NullString
		if err := row.Columns(&next); err != nil {
			return fmt.Errorf("decode parent of task %q: %w", current, err)
		}
		current = nullStringValue(next)
	}
	return nil
}

func nativeUpdateTaskFields(ctx context.Context, env *Env, explicitProjectID, taskID string, fields map[string]interface{}) (MutationWriteResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, explicitProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	if len(fields) == 0 {
		return MutationWriteResult{}, fmt.Errorf("no task fields supplied")
	}
	requestedParent, checkParentWrite, err := nativeRequestedParent(fields)
	if err != nil {
		return MutationWriteResult{}, err
	}
	// status is a SECOND write path into Tasks.status alongside
	// nativeHandleCreate — task_graph_set_status, task_graph_reown (which
	// never touches status but shares this function), task_graph_update, and
	// the parent-link helpers all funnel status changes through here via
	// nativeTaskFieldColumns. Gate it here so every path is covered by one
	// check, not just task_graph_set_status.
	//
	// The registry checks themselves run INSIDE the transaction below
	// (checkTaskStatusWrite), after the row's current status is read: a
	// status-preserving round trip (incoming == stored, legacy literal or
	// not) must never fail, and only a genuine status CHANGE is gated on
	// canonical membership + the transition graph. Validating the incoming
	// literal with Resolve up front — the pre-2026-08-20 shape — froze every
	// legacy-status row solid (see checkTaskStatusWrite in registries.go).
	// What CAN fail fast here: a status key that is not a non-empty string
	// (previously written through unvalidated — "" or a non-string reached
	// the UPDATE statement untouched by any registry), and a missing
	// registry (fail closed, per requireTaskStatusRegistry).
	var newStatusLiteral string
	var checkStatusWrite bool
	if v, ok := fields["status"]; ok {
		s, isString := v.(string)
		if !isString || s == "" {
			return MutationWriteResult{}, fmt.Errorf("task status must be a non-empty string, got %T (%v)", v, v)
		}
		if err := requireTaskStatusRegistry(env); err != nil {
			return MutationWriteResult{}, err
		}
		newStatusLiteral = s
		checkStatusWrite = true
	}
	assignments, fieldParams := nativeBuildTaskUpdate(fields)
	params := map[string]interface{}{"project_id": projectID, "id": taskID, "updated_at": env.Now().UTC()}
	for col, val := range fieldParams {
		params[col] = val
	}
	if len(assignments) == 0 {
		return MutationWriteResult{}, fmt.Errorf("no supported task fields supplied")
	}
	assignments = append(assignments, "updated_at = @updated_at")
	sql := `UPDATE ` + adapter.Descriptor.TaskTable + ` SET ` + strings.Join(assignments, ", ") + ` WHERE ` + adapter.projectWhereAnd() + `id = @id`
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	var rows int64
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		readStmt := spanner.Statement{
			SQL:    `SELECT owner, session_id, repo, status FROM ` + adapter.Descriptor.TaskTable + ` WHERE ` + adapter.projectWhereAnd() + `id = @id LIMIT 1`,
			Params: map[string]interface{}{"project_id": projectID, "id": taskID},
		}
		iter := tx.Query(ctx, readStmt)
		row, readErr := iter.Next()
		iter.Stop()
		if readErr == iterator.Done {
			return fmt.Errorf("task %q not found in project %q", taskID, projectID)
		}
		if readErr != nil {
			return fmt.Errorf("read task %q in project %q for update: %w", taskID, projectID, readErr)
		}
		var currentOwner, currentStatus string
		var currentSession, currentRepo spanner.NullString
		if err := row.Columns(&currentOwner, &currentSession, &currentRepo, &currentStatus); err != nil {
			return fmt.Errorf("decode task %q for update: %w", taskID, err)
		}
		if err := nativeGuardTaskOwnershipTuple(fields, currentOwner, nullStringValue(currentSession), nullStringValue(currentRepo)); err != nil {
			return err
		}
		if checkStatusWrite {
			if err := checkTaskStatusWrite(ctx, env.TaskStatuses, taskID, currentStatus, newStatusLiteral); err != nil {
				return err
			}
		}
		if checkParentWrite {
			if err := nativeValidateParentAssignmentTx(ctx, tx, adapter, projectID, taskID, requestedParent); err != nil {
				return err
			}
		}
		n, err := tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
		rows = n
		return err
	})
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("update task-plan task: %w", err)
	}
	if rows == 0 {
		return MutationWriteResult{}, fmt.Errorf("task %q not found in project %q", taskID, projectID)
	}
	return nativeMutationWriteResult(nativeMutationReceipt(env, projectID, "update_task", "task", taskID, adapter.Descriptor.TaskTable, rows, timeFromParam(params["updated_at"]))), nil
}

// nativeWorklinkFieldColumns maps every task-plan-native worklink field this
// store accepts to its TaskLinks table column. BOTH the CREATE
// (nativeBuildWorklinkInsert) and UPDATE (nativeBuildWorklinkUpdate) native
// write paths are keyed off this ONE map so they cannot silently diverge on
// which fields persist — the worklink-lane analogue of
// nativeTaskFieldColumns, and the fix for P3-GT-FP-WORKLINK-FIELD-PARITY.
// Before it existed, CREATE's INSERT named its own literal eight-column list
// while UPDATE carried a thirteen-entry allow-list PLUS a pr_number special
// case, so session_id, repo, branch, commit_sha, pr_number, pr_state,
// worktree_path and url were all accepted by UPDATE and silently dropped by
// CREATE. TestNativeWorklinkCreateUpdateFieldParity iterates this map and
// fails loudly if the two builders ever disagree on it again.
//
// ⛔ pr_number is deliberately IN this map rather than special-cased beside
// it. Keeping it outside is exactly what made the original defect hard to
// count: reading only the allow-list yields SEVEN dropped fields and misses
// pr_number, so a parity test iterating an allow-list that excluded it would
// have reproduced the same blind spot it exists to close. The builders
// branch on the target COLUMN being pr_number (an INT64) rather than on
// membership, the same way the task-lane test branches on is_gap /
// is_critical_path being bool.
//
// THREE TaskLinks columns are deliberately ABSENT and are not a drop:
//   - link_id and created_at are identity and server-minted; UPDATE has
//     never accepted either, and CREATE resolves them itself.
//   - artifact_status is LIFECYCLE, not content. CREATE always writes
//     'recorded' (nativeBuildWorklinkInsert) and archival goes through
//     nativeHandleArchiveWorklink's own validated statement. Admitting it to
//     this map would let a caller mint an already-archived link on create —
//     a behavior change, not a parity fix. UPDATE still accepts it, handled
//     explicitly in nativeBuildWorklinkUpdate so the carve-out is visible at
//     both ends rather than implied by an omission.
var nativeWorklinkFieldColumns = map[string]string{
	"task_id":       "task_id",
	"artifact_type": "artifact_type",
	"artifact_ref":  "artifact_ref",
	"thread":        "thread",
	"note":          "note",
	"session_id":    "session_id",
	"repo":          "repo",
	"branch":        "branch",
	"commit_sha":    "commit_sha",
	"pr_number":     "pr_number",
	"pr_state":      "pr_state",
	"worktree_path": "worktree_path",
	"url":           "url",
}

// nativeWorklinkInsertFields projects an addWorklinkInput into the
// payload-shaped map nativeBuildWorklinkInsert reads. Empty values are
// OMITTED rather than carried as "", so the same projection also feeds the
// migrated-mode artifact payload in handleAddWorklink without stamping empty
// strings onto keys the caller never supplied.
//
// pr_number is included only when non-zero: the wire field is `int` with
// `omitempty`, so a decoded 0 is indistinguishable from absent, and PR
// numbers start at 1. Absent means SQL NULL — see
// nativeBuildWorklinkInsert's NULL-not-zero contract.
func nativeWorklinkInsertFields(in addWorklinkInput) map[string]interface{} {
	fields := map[string]interface{}{}
	for key, val := range map[string]string{
		"thread":        in.Thread,
		"note":          in.Note,
		"session_id":    in.SessionID,
		"repo":          in.Repo,
		"branch":        in.Branch,
		"commit_sha":    in.CommitSHA,
		"pr_state":      in.PRState,
		"worktree_path": in.WorktreePath,
		"url":           in.URL,
	} {
		if val != "" {
			fields[key] = val
		}
	}
	if in.PRNumber != 0 {
		fields["pr_number"] = in.PRNumber
	}
	return fields
}

// nativeArtifactCreateInput projects a task_graph_create(node_kind="artifact")
// contract+payload into the addWorklinkInput the native worklink writer
// consumes. This is the SECOND create entrypoint into TaskLinks, and it
// carried the identical drop: it read only task_id, thread and note out of an
// open-ended payload map, so every provenance key a caller supplied there was
// discarded without a word. Pure, so the payload-to-column coverage is
// testable without a Spanner emulator.
func nativeArtifactCreateInput(projectID, stableID string, contract, payload map[string]interface{}) addWorklinkInput {
	return addWorklinkInput{
		ProjectID:    projectID,
		TaskID:       stringFromMap(payload, "task_id"),
		ArtifactID:   firstNonEmpty(stableID, stringFromMap(contract, "canonical_name")),
		ArtifactType: stringFromMap(contract, "artifact_type_kind"),
		ArtifactRef:  stringFromMap(contract, "artifact_ref"),
		Thread:       stringFromMap(payload, "thread"),
		Note:         stringFromMap(payload, "note"),
		SessionID:    stringFromMap(payload, "session_id"),
		Repo:         stringFromMap(payload, "repo"),
		Branch:       stringFromMap(payload, "branch"),
		CommitSHA:    stringFromMap(payload, "commit_sha"),
		PRNumber:     intFromMap(payload, "pr_number"),
		PRState:      stringFromMap(payload, "pr_state"),
		WorktreePath: stringFromMap(payload, "worktree_path"),
		URL:          stringFromMap(payload, "url"),
	}
}

// nativeBuildWorklinkInsert renders the column list and param map for
// inserting a task-plan-native TaskLinks row. linkID, taskID, artifactType
// and artifactRef are the identity and required-input values
// nativeHandleAddWorklink has already resolved and validated (task existence
// checked inside the caller's transaction, artifact_type resolved against the
// closed artifact-kind registry) — they are threaded as arguments rather than
// read from fields, the same split nativeBuildTaskInsert uses for taskID and
// parentID. now is the server-minted created_at. Every other column is read
// from fields, keyed against the same names nativeWorklinkFieldColumns
// declares write-able on UPDATE, so this builder cannot silently diverge from
// nativeBuildWorklinkUpdate again — see
// TestNativeWorklinkCreateUpdateFieldParity (P3-GT-FP-WORKLINK-FIELD-PARITY).
//
// pr_number absent writes SQL NULL, never 0: the column is a nullable INT64
// and UPDATE already treats an absent key as "leave the column alone", so a
// freshly created link must be indistinguishable from one nobody has attached
// a PR to. This is the direct analogue of the NULL-not-false decision
// TestNativeBuildTaskInsertCriticalPathAbsentIsNull pins on the task lane.
//
// project_id is NOT included here: the caller adds it to the returned params
// and to the column list via projectInsertColumns()/projectInsertValues().
func nativeBuildWorklinkInsert(linkID, taskID, artifactType, artifactRef string, fields map[string]interface{}, now time.Time) (columns []string, params map[string]interface{}) {
	columns = []string{
		"link_id", "task_id", "artifact_type", "artifact_ref", "thread", "note",
		"session_id", "repo", "branch", "commit_sha", "pr_number", "pr_state",
		"worktree_path", "url", "created_at", "artifact_status",
	}
	params = map[string]interface{}{
		"link_id":         linkID,
		"task_id":         taskID,
		"artifact_type":   artifactType,
		"artifact_ref":    artifactRef,
		"thread":          nullString(stringFromMap(fields, "thread")),
		"note":            nullString(stringFromMap(fields, "note")),
		"session_id":      nullString(stringFromMap(fields, "session_id")),
		"repo":            nullString(stringFromMap(fields, "repo")),
		"branch":          nullString(stringFromMap(fields, "branch")),
		"commit_sha":      nullString(stringFromMap(fields, "commit_sha")),
		"pr_number":       nullInt64FromMap(fields, "pr_number"),
		"pr_state":        nullString(stringFromMap(fields, "pr_state")),
		"worktree_path":   nullString(stringFromMap(fields, "worktree_path")),
		"url":             nullString(stringFromMap(fields, "url")),
		"created_at":      now,
		"artifact_status": nullString("recorded"),
	}
	return columns, params
}

// nativeBuildWorklinkUpdate renders the SET-clause assignments and params for
// a task-plan-native TaskLinks UPDATE from a caller-supplied fields map,
// keyed off nativeWorklinkFieldColumns. Pure/no I/O so it is directly
// unit-testable without a Spanner emulator. A field not present in
// nativeWorklinkFieldColumns is silently ignored, matching
// nativeBuildTaskUpdate — the get→update round trip carries read-only echo
// keys (project_id, fleet_created_at) that were never meant to be written.
//
// The two value-shaped rules below are PRE-EXISTING behavior this refactor
// preserves verbatim, not new policy:
//   - a string-shaped field whose value is "" or nil is SKIPPED (leave the
//     column alone), which is why clearing a string column is not expressible
//     through this verb;
//   - pr_number is applied whenever its KEY is present, regardless of value,
//     because the empty-value guard above is string-shaped and would swallow
//     a legitimate clear on an INT64 column. int64FromAny renders nil or an
//     unparseable value as SQL NULL.
func nativeBuildWorklinkUpdate(fields map[string]interface{}) (assignments []string, params map[string]interface{}) {
	params = map[string]interface{}{}
	for key, val := range fields {
		col, ok := nativeWorklinkFieldColumns[key]
		if !ok {
			continue
		}
		if col == "pr_number" {
			assignments = append(assignments, col+" = @"+col)
			params[col] = int64FromAny(val)
			continue
		}
		if val == "" || val == nil {
			continue
		}
		assignments = append(assignments, col+" = @"+col)
		params[col] = nativeParam(val)
	}
	// artifact_status is lifecycle, not content — see the carve-out note on
	// nativeWorklinkFieldColumns. UPDATE has always accepted it and continues
	// to; CREATE never does.
	// The superseded lifecycle is never emitted here: nativeUpdateWorklinkFields
	// refuses it explicitly first, and this guard keeps the builder from ever
	// rendering it even if a caller skips that check.
	if val, ok := fields["artifact_status"]; ok && val != "" && val != nil && !nativeNamesSupersededStatus(val) {
		assignments = append(assignments, "artifact_status = @artifact_status")
		params["artifact_status"] = nativeParam(val)
	}
	return assignments, params
}

func nativeUpdateWorklinkFields(ctx context.Context, env *Env, explicitProjectID, linkID string, contract, payload map[string]interface{}) (MutationWriteResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, explicitProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	fields := map[string]interface{}{}
	for k, v := range payload {
		fields[k] = v
	}
	if contract != nil {
		fields["artifact_type"] = stringFromMap(contract, "artifact_type_kind")
		fields["artifact_ref"] = stringFromMap(contract, "artifact_ref")
	}
	if nativeNamesSupersededStatus(fields["artifact_status"]) {
		return MutationWriteResult{}, errGenericUpdateSuperseded("")
	}
	// artifact_type is the second write path into TaskLinks.artifact_type
	// (nativeHandleAddWorklink is the first) — task_graph_update on an
	// artifact node reaches this function directly. Both paths must gate
	// against the same closed registry or the registry silently re-opens
	// itself through whichever path forgot the check.
	if v, ok := fields["artifact_type"].(string); ok && v != "" {
		if err := requireArtifactKindRegistry(env); err != nil {
			return MutationWriteResult{}, err
		}
		if _, err := env.ArtifactKinds.Resolve(ctx, v); err != nil {
			return MutationWriteResult{}, err
		}
	}
	assignments, fieldParams := nativeBuildWorklinkUpdate(fields)
	params := map[string]interface{}{"project_id": projectID, "link_id": linkID}
	for col, val := range fieldParams {
		params[col] = val
	}
	if len(assignments) == 0 {
		return MutationWriteResult{}, fmt.Errorf("no supported worklink fields supplied")
	}
	sql := `UPDATE ` + adapter.Descriptor.WorklinkTable + ` SET ` + strings.Join(assignments, ", ") + ` WHERE ` + adapter.projectWhereAnd() + `link_id = @link_id`
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	var rows int64
	missing := false
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		// A superseded row is retired history. Reading it in the transaction that
		// would write it makes the refusal atomic with, and serialized against, a
		// concurrent typed supersede; nothing is written on refusal.
		rows, missing = 0, false
		stored, found, err := nativeReadWorklinkStateTx(ctx, tx, adapter, projectID, linkID)
		if err != nil {
			return err
		}
		if !found {
			missing = true
			return nil
		}
		if strings.EqualFold(strings.TrimSpace(stored.Status), worklinkSupersededStatus) {
			return errGenericUpdateSuperseded(linkID)
		}
		if err := nativeGuardGenericWorklinkUpdate(ctx, tx, adapter, projectID, stored, fields); err != nil {
			return err
		}
		n, err := tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
		rows = n
		return err
	})
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("update task-plan worklink: %w", err)
	}
	if missing || rows == 0 {
		return MutationWriteResult{}, fmt.Errorf("worklink %q not found in project %q", linkID, projectID)
	}
	return nativeMutationWriteResult(nativeMutationReceipt(env, projectID, "update_worklink", "artifact", linkID, adapter.Descriptor.WorklinkTable, rows, env.Now().UTC())), nil
}

// nativeInsertDep creates (or reactivates) a "gates" TaskDeps row: taskID
// depends on dependsOnTaskID. Enforces, in order: legal endpoints (no
// self-loop — resolved node existence is checked further down), uniqueness
// (an existing ACTIVE row for the same ordered pair under a DIFFERENT kind
// refuses rather than silently overwriting), legal transition (archived ->
// gates, matching dependencyKindTransitionLegal), and acyclicity (refuses if
// this edge would close a cycle in the active BLOCKING dependency graph —
// see wouldCreateDependencyCycle).
func nativeInsertDep(ctx context.Context, env *Env, explicitProjectID, taskID, dependsOnTaskID string) (MutationWriteResult, error) {
	if taskID == dependsOnTaskID {
		return MutationWriteResult{}, fmt.Errorf("%w: task_id=%q", ErrDependencySelfLoop, taskID)
	}
	if err := requireDependencyKindRegistry(env); err != nil {
		return MutationWriteResult{}, err
	}
	gatesDesc, err := env.DependencyKinds.Resolve(ctx, "gates")
	if err != nil {
		return MutationWriteResult{}, err
	}
	archivedDesc, err := env.DependencyKinds.Resolve(ctx, "archived")
	if err != nil {
		return MutationWriteResult{}, err
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	projectID, err := resolveNativeProjectID(env, adapter, explicitProjectID)
	if err != nil {
		return MutationWriteResult{}, err
	}
	depID := nativeDependencyID(projectID, dependsOnTaskID, taskID)
	now := env.Now().UTC()
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	var rows int64
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := nativeRequireTaskInProject(ctx, tx, adapter, projectID, taskID); err != nil {
			return err
		}
		if err := nativeRequireTaskInProject(ctx, tx, adapter, projectID, dependsOnTaskID); err != nil {
			return err
		}
		existing := spanner.Statement{
			SQL: `SELECT dep_id, kind, created_at FROM ` + adapter.Descriptor.DepTable + `
				WHERE ` + adapter.projectWhereAnd() + `task_id = @task_id AND depends_on_task_id = @depends_on_task_id
				ORDER BY CASE WHEN kind = 'archived' THEN 1 ELSE 0 END, created_at, dep_id LIMIT 1`,
			Params: map[string]interface{}{
				"project_id": projectID, "task_id": taskID, "depends_on_task_id": dependsOnTaskID,
			},
		}
		iter := tx.Query(ctx, existing)
		row, readErr := iter.Next()
		iter.Stop()
		if readErr == nil {
			var existingID string
			var existingKind spanner.NullString
			var createdAt spanner.NullTime
			if err := row.Columns(&existingID, &existingKind, &createdAt); err != nil {
				return fmt.Errorf("decode existing task-plan dep: %w", err)
			}
			depID = existingID
			isArchived := existingKind.Valid && existingKind.StringVal == "archived"
			if !isArchived {
				// An ACTIVE row already exists for this ordered pair. Same
				// kind ("gates", the only kind this function ever writes
				// fresh) is the existing idempotent no-op. A different
				// kind is a genuine conflict, not silently overwritten —
				// ErrDependencyKindConflict per the "never a silent
				// rewrite" requirement; caller archives the existing edge
				// first if replacing it is really intended.
				existingCanonical, found := env.DependencyKinds.Canonicalize(ctx, existingKind.StringVal)
				if found && existingCanonical != gatesDesc.Name() {
					return fmt.Errorf("%w: task_id=%q depends_on_task_id=%q existing kind=%q (canonical %q)",
						ErrDependencyKindConflict, taskID, dependsOnTaskID, existingKind.StringVal, existingCanonical)
				}
				rows = 0
				return nil
			}
			// Reactivating an archived row is transitioning archived ->
			// gates, and closes exactly the cycle a fresh insert would
			// (the edge becomes active either way) — run the same
			// acyclicity check before flipping it back on.
			if !dependencyKindTransitionLegal(archivedDesc, gatesDesc) {
				return fmt.Errorf("%w: %q -> %q", ErrIllegalDependencyTransition, archivedDesc.Name(), gatesDesc.Name())
			}
			if gatesDesc.Blocking() {
				existingEdges, err := nativeFetchActiveBlockingEdges(ctx, tx, adapter, env, projectID)
				if err != nil {
					return err
				}
				if would, cycle := wouldCreateDependencyCycle(existingEdges, taskID, dependsOnTaskID); would {
					return fmt.Errorf("%w: reactivating %s -> %s would close cycle %v", ErrDependencyCycle, taskID, dependsOnTaskID, cycle)
				}
			}
			reactivate := spanner.Statement{
				SQL: `UPDATE ` + adapter.Descriptor.DepTable + ` SET kind = 'gates', created_at = @created_at
					WHERE ` + adapter.projectWhereAnd() + `dep_id = @dep_id AND kind = 'archived'`,
				Params: map[string]interface{}{"project_id": projectID, "dep_id": depID, "created_at": now},
			}
			rows, err = tx.Update(ctx, reactivate)
			return err
		}
		if readErr != iterator.Done {
			return fmt.Errorf("read existing task-plan dep: %w", readErr)
		}
		if gatesDesc.Blocking() {
			existingEdges, err := nativeFetchActiveBlockingEdges(ctx, tx, adapter, env, projectID)
			if err != nil {
				return err
			}
			if would, cycle := wouldCreateDependencyCycle(existingEdges, taskID, dependsOnTaskID); would {
				return fmt.Errorf("%w: inserting %s -> %s would close cycle %v", ErrDependencyCycle, taskID, dependsOnTaskID, cycle)
			}
		}
		stmt := spanner.Statement{
			SQL: `INSERT INTO ` + adapter.Descriptor.DepTable + `
				(` + adapter.projectInsertColumns() + `dep_id, task_id, depends_on_task_id, kind, note, created_at)
				VALUES (` + adapter.projectInsertValues() + `@dep_id, @task_id, @depends_on_task_id, @kind, @note, @created_at)`,
			Params: map[string]interface{}{
				"project_id":         projectID,
				"dep_id":             depID,
				"task_id":            taskID,
				"depends_on_task_id": dependsOnTaskID,
				"kind":               gatesDesc.Name(),
				"note":               spanner.NullString{Valid: false},
				"created_at":         now,
			},
		}
		n, err := tx.Update(ctx, stmt)
		rows = n
		return err
	})
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("insert task-plan dep: %w", err)
	}
	return nativeMutationWriteResult(nativeMutationReceipt(env, projectID, "insert_dep", "edge", depID, adapter.Descriptor.DepTable, rows, now)), nil
}

// nativeFetchActiveBlockingEdges reads every non-archived TaskDeps row in
// the project and returns the subset whose kind canonicalizes to a
// dependency_kinds member with blocking=true, as pure depEdge values for
// wouldCreateDependencyCycle. A row whose kind literal does not canonicalize
// to ANY registered member (possible only for drift introduced after this
// task's live-value audit — every literal measured live as of 2026-08-16 is
// exhaustively covered, see task-graph.config.json dependency_kinds) is
// skipped rather than failing the whole check closed: acyclicity is a
// property of the blocking subgraph, and silently excluding one
// unrecognized row is a safer degradation than refusing every dependency
// write in the project until that drift is investigated separately.
func nativeFetchActiveBlockingEdges(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, env *Env, projectID string) ([]depEdge, error) {
	stmt := spanner.Statement{
		SQL: `SELECT task_id, depends_on_task_id, kind FROM ` + a.Descriptor.DepTable + `
			WHERE ` + a.projectWhereAnd() + `COALESCE(kind, 'gates') != 'archived'`,
		Params: map[string]interface{}{"project_id": projectID},
	}
	iter := tx.Query(ctx, stmt)
	defer iter.Stop()
	var out []depEdge
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fetch active dependency edges: %w", err)
		}
		var taskID, dependsOnTaskID string
		var kind spanner.NullString
		if err := row.Columns(&taskID, &dependsOnTaskID, &kind); err != nil {
			return nil, fmt.Errorf("decode dependency edge: %w", err)
		}
		kindLiteral := "gates"
		if kind.Valid {
			kindLiteral = kind.StringVal
		}
		canonical, found := env.DependencyKinds.Canonicalize(ctx, kindLiteral)
		if !found {
			continue
		}
		desc, err := env.DependencyKinds.Resolve(ctx, canonical)
		if err != nil || !desc.Blocking() {
			continue
		}
		out = append(out, depEdge{TaskID: taskID, DependsOnTaskID: dependsOnTaskID})
	}
	return out, nil
}

func nativeDependencyID(projectID, gatesSourceID, gatesTargetID string) string {
	identity := strings.Join([]string{projectID, gatesSourceID, gatesTargetID, "gates"}, "\x00")
	return "DEP-" + uuid.NewSHA1(nativeDependencyNamespace, []byte(identity)).String()
}

func nativeHandleUnlink(ctx context.Context, env *Env, edgeID string) (MutationWriteResult, error) {
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil || adapter == nil {
		return MutationWriteResult{}, err
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		return MutationWriteResult{}, err
	}
	projectID := env.Config.DefaultProjectID
	mutatedAt := env.Now().UTC()
	var rows int64
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		projectSelect := `@default_project_id AS project_id`
		if adapter.hasProjectColumn() {
			projectSelect = adapter.Descriptor.ProjectColumn + ` AS project_id`
		}
		stmt := spanner.Statement{
			SQL: `SELECT ` + projectSelect + `, kind FROM ` + adapter.Descriptor.DepTable + `
				WHERE dep_id = @dep_id LIMIT 2`,
			Params: map[string]interface{}{"default_project_id": projectID, "dep_id": edgeID},
		}
		iter := tx.Query(ctx, stmt)
		defer iter.Stop()
		row, readErr := iter.Next()
		if readErr == iterator.Done {
			return fmt.Errorf("edge %q not found in task-plan native store", edgeID)
		}
		if readErr != nil {
			return fmt.Errorf("read task-plan dep %q: %w", edgeID, readErr)
		}
		var kind spanner.NullString
		if err := row.Columns(&projectID, &kind); err != nil {
			return fmt.Errorf("decode task-plan dep %q: %w", edgeID, err)
		}
		if _, nextErr := iter.Next(); nextErr != iterator.Done {
			if nextErr != nil {
				return fmt.Errorf("check task-plan dep identity %q: %w", edgeID, nextErr)
			}
			return fmt.Errorf("edge_id %q is ambiguous across task-plan projects", edgeID)
		}
		if kind.Valid && kind.StringVal == "archived" {
			return nil
		}
		where := `dep_id = @dep_id`
		params := map[string]interface{}{"dep_id": edgeID, "project_id": projectID}
		if adapter.hasProjectColumn() {
			where = adapter.Descriptor.ProjectColumn + ` = @project_id AND ` + where
		}
		update := spanner.Statement{
			SQL: `UPDATE ` + adapter.Descriptor.DepTable + ` SET kind = 'archived'
				WHERE ` + where + ` AND COALESCE(kind, 'gates') != 'archived'`,
			Params: params,
		}
		rows, err = tx.Update(ctx, update)
		return err
	})
	if err != nil {
		return MutationWriteResult{}, fmt.Errorf("archive task-plan dep: %w", err)
	}
	return nativeMutationWriteResult(nativeMutationReceipt(env, projectID, "archive_dep", "edge", edgeID, adapter.Descriptor.DepTable, rows, mutatedAt)), nil
}

func nativeRequireTaskInProject(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, projectID, taskID string) error {
	stmt := spanner.Statement{
		SQL:    `SELECT id FROM ` + a.Descriptor.TaskTable + ` WHERE ` + a.projectWhereAnd() + `id = @id LIMIT 1`,
		Params: map[string]interface{}{"project_id": projectID, "id": taskID},
	}
	iter := tx.Query(ctx, stmt)
	defer iter.Stop()
	if _, err := iter.Next(); err == iterator.Done {
		return fmt.Errorf("task %q not found in project %q", taskID, projectID)
	} else if err != nil {
		return fmt.Errorf("read task %q in project %q: %w", taskID, projectID, err)
	}
	return nil
}

func nativeRequireWorklinkInProject(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, projectID, linkID string) error {
	stmt := spanner.Statement{
		SQL:    `SELECT link_id FROM ` + a.Descriptor.WorklinkTable + ` WHERE ` + a.projectWhereAnd() + `link_id = @id LIMIT 1`,
		Params: map[string]interface{}{"project_id": projectID, "id": linkID},
	}
	iter := tx.Query(ctx, stmt)
	defer iter.Stop()
	if _, err := iter.Next(); err == iterator.Done {
		return fmt.Errorf("worklink %q not found in project %q", linkID, projectID)
	} else if err != nil {
		return fmt.Errorf("read worklink %q in project %q: %w", linkID, projectID, err)
	}
	return nil
}

func nativeReadNode(ctx context.Context, a *SpannerSubstrateAdapter, projectID, nodeID string) (Node, error) {
	if n, found, err := nativeReadTaskNode(ctx, a, projectID, nodeID); err != nil || found {
		return n, err
	}
	if n, found, err := nativeReadArtifactNode(ctx, a, projectID, nodeID); err != nil || found {
		return n, err
	}
	return Node{}, fmt.Errorf("node %q not found in project %q in substrate %q", nodeID, projectID, a.Descriptor.ID)
}

func nativeReadTaskNode(ctx context.Context, a *SpannerSubstrateAdapter, projectID, taskID string) (Node, bool, error) {
	sql := `SELECT ` + a.projectSelectExpr() + `, id, title, pillar, layer, status, owner, parent_id, is_gap,
		gap_label, note, CAST(created_at AS STRING) AS created_at,
		CAST(updated_at AS STRING) AS updated_at, session_id, repo, is_critical_path
		FROM ` + a.Descriptor.TaskTable + ` WHERE ` + a.projectWhereAnd() + `id = @id LIMIT 1`
	res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": projectID, "id": taskID})
	if err != nil {
		return Node{}, false, err
	}
	if len(res.Rows) == 0 {
		return Node{}, false, nil
	}
	return nativeTaskNodeFromRow(res.Rows[0]), true, nil
}

func nativeReadArtifactNode(ctx context.Context, a *SpannerSubstrateAdapter, projectID, linkID string) (Node, bool, error) {
	sql := `SELECT ` + a.projectSelectExpr() + `, link_id, task_id, artifact_type, artifact_ref, thread, note,
		CAST(created_at AS STRING) AS created_at, session_id, repo, branch,
		commit_sha, pr_number, pr_state, worktree_path, url, artifact_status
		FROM ` + a.Descriptor.WorklinkTable + ` WHERE ` + a.projectWhereAnd() + `link_id = @id LIMIT 1`
	res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": projectID, "id": linkID})
	if err != nil {
		return Node{}, false, err
	}
	if len(res.Rows) == 0 {
		return Node{}, false, nil
	}
	return nativeArtifactNodeFromRow(res.Rows[0]), true, nil
}

func nativeListNodes(ctx context.Context, a *SpannerSubstrateAdapter, f ListFilter) ([]Node, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	if f.ProjectID == "" {
		return nil, fmt.Errorf("project_id required for task-plan native list")
	}
	var out []Node
	if f.NodeKind == "" || f.NodeKind == "task" {
		sql := fmt.Sprintf(`SELECT %s, id, title, pillar, layer, status, owner, parent_id, is_gap,
			gap_label, note, CAST(created_at AS STRING) AS created_at,
			CAST(updated_at AS STRING) AS updated_at, session_id, repo, is_critical_path
			FROM %s WHERE %s ORDER BY pillar, status, id LIMIT %d`, a.projectSelectExpr(), a.Descriptor.TaskTable, a.projectPredicate(), limit)
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": f.ProjectID})
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			out = append(out, nativeTaskNodeFromRow(row))
		}
	}
	if f.NodeKind == "" || f.NodeKind == "artifact" {
		remaining := limit - len(out)
		if remaining <= 0 {
			return out, nil
		}
		sql := fmt.Sprintf(`SELECT %s, link_id, task_id, artifact_type, artifact_ref, thread, note,
			CAST(created_at AS STRING) AS created_at, session_id, repo, branch,
			commit_sha, pr_number, pr_state, worktree_path, url, artifact_status
			FROM %s WHERE %s ORDER BY created_at LIMIT %d`, a.projectSelectExpr(), a.Descriptor.WorklinkTable, a.projectPredicate(), remaining)
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": f.ProjectID})
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			out = append(out, nativeArtifactNodeFromRow(row))
		}
	}
	return out, nil
}

// nativeSearchTerms lowercases the query and splits it into whitespace-delimited
// terms, deduplicated in first-seen order. The cap bounds the generated SQL: a
// query is intent, not a document, and eight terms is already far past what a
// ranked STRPOS match can usefully discriminate.
func nativeSearchTerms(query string) []string {
	const maxTerms = 8
	seen := map[string]bool{}
	var out []string
	for _, t := range strings.Fields(strings.ToLower(query)) {
		if seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
		if len(out) == maxTerms {
			break
		}
	}
	return out
}

// nativeSearchHaystackExpr renders the lowercased concatenation of the given
// STRING columns, NULL-safe, space-joined so a term cannot straddle two column
// values.
func nativeSearchHaystackExpr(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = `COALESCE(` + c + `, '')`
	}
	return `LOWER(` + strings.Join(parts, ` || ' ' || `) + `)`
}

func nativeWorklinkSearchHaystackExpr() string {
	return nativeSearchHaystackExpr([]string{
		"link_id", "task_id", "artifact_type", "artifact_ref", "thread", "session_id", "note", "branch", "url",
	})
}

// nativeSearchScoreExpr renders the per-row match score: the count of query
// terms (@t0..@tN) found in the haystack. STRPOS rather than LIKE so term
// bytes are matched literally — no wildcard semantics to escape. The score is
// computed in Spanner; the client only orders by it.
func nativeSearchScoreExpr(haystackExpr string, termCount int) string {
	parts := make([]string, termCount)
	for i := 0; i < termCount; i++ {
		parts[i] = fmt.Sprintf(`IF(STRPOS(%s, @t%d) > 0, 1, 0)`, haystackExpr, i)
	}
	return strings.Join(parts, ` + `)
}

// nativeSearchNodes is the fleet_plan_native implementation of
// task_graph_search. The native store has no node table and no embedding
// column, so search is a Spanner-side ranked term match over the live task and
// worklink rows: rows matching at least one term, ordered by how many terms
// they match. Tasks fill the limit first, artifacts take the remainder —
// the same precedence nativeListNodes establishes for this store.
func nativeSearchNodes(ctx context.Context, a *SpannerSubstrateAdapter, projectID, query, nodeKind string, limit int) ([]Node, error) {
	if limit <= 0 {
		limit = 15
	}
	terms := nativeSearchTerms(query)
	if len(terms) == 0 {
		return nil, fmt.Errorf("query has no searchable terms")
	}
	params := map[string]interface{}{"project_id": projectID}
	for i, t := range terms {
		params[fmt.Sprintf("t%d", i)] = t
	}
	var out []Node
	if nodeKind == "" || nodeKind == "task" {
		score := nativeSearchScoreExpr(nativeSearchHaystackExpr([]string{
			"id", "title", "pillar", "layer", "status", "owner", "gap_label", "note",
		}), len(terms))
		sql := fmt.Sprintf(`SELECT %s, id, title, pillar, layer, status, owner, parent_id, is_gap,
			gap_label, note, CAST(created_at AS STRING) AS created_at,
			CAST(updated_at AS STRING) AS updated_at, session_id, repo, is_critical_path,
			(%s) AS match_score
			FROM %s WHERE %s(%s) > 0
			ORDER BY match_score DESC, id LIMIT %d`,
			a.projectSelectExpr(), score, a.Descriptor.TaskTable, a.projectWhereAnd(), score, limit)
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, params)
		if err != nil {
			return nil, fmt.Errorf("native task search: %w", err)
		}
		for _, row := range res.Rows {
			out = append(out, nativeTaskNodeFromRow(row))
		}
	}
	if nodeKind == "" || nodeKind == "artifact" {
		remaining := limit - len(out)
		if remaining <= 0 {
			return out, nil
		}
		score := nativeSearchScoreExpr(nativeWorklinkSearchHaystackExpr(), len(terms))
		sql := fmt.Sprintf(`SELECT %s, link_id, task_id, artifact_type, artifact_ref, thread, note,
			CAST(created_at AS STRING) AS created_at, session_id, repo, branch,
			commit_sha, pr_number, pr_state, worktree_path, url, artifact_status,
			(%s) AS match_score
			FROM %s WHERE %s(%s) > 0
			ORDER BY match_score DESC, link_id LIMIT %d`,
			a.projectSelectExpr(), score, a.Descriptor.WorklinkTable, a.projectWhereAnd(), score, remaining)
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, params)
		if err != nil {
			return nil, fmt.Errorf("native worklink search: %w", err)
		}
		for _, row := range res.Rows {
			out = append(out, nativeArtifactNodeFromRow(row))
		}
	}
	return out, nil
}

func nativeNeighbors(ctx context.Context, a *SpannerSubstrateAdapter, nodeID string, opts NeighborOpts) ([]Edge, error) {
	if opts.ProjectID == "" {
		return nil, fmt.Errorf("project_id required for task-plan native neighbors")
	}
	var out []Edge
	want := func(label string) bool {
		if len(opts.EdgeLabels) == 0 {
			return true
		}
		for _, l := range opts.EdgeLabels {
			if l == label {
				return true
			}
		}
		return false
	}
	includeIn := opts.Direction == "" || opts.Direction == "both" || opts.Direction == "in"
	includeOut := opts.Direction == "" || opts.Direction == "both" || opts.Direction == "out"
	if includeIn && want("advances") {
		sql := `SELECT link_id, task_id, CAST(created_at AS STRING) AS created_at
			FROM ` + a.Descriptor.WorklinkTable + ` WHERE ` + a.projectWhereAnd() + `task_id = @id LIMIT 200`
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": opts.ProjectID, "id": nodeID})
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			out = append(out, nativeEdge(opts.ProjectID, "advances:"+stringField(row, "link_id"), "advances", stringField(row, "link_id"), stringField(row, "task_id"), stringField(row, "created_at")))
		}
	}
	if includeOut && want("advances") {
		sql := `SELECT link_id, task_id, CAST(created_at AS STRING) AS created_at
			FROM ` + a.Descriptor.WorklinkTable + ` WHERE ` + a.projectWhereAnd() + `link_id = @id LIMIT 200`
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": opts.ProjectID, "id": nodeID})
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			out = append(out, nativeEdge(opts.ProjectID, "advances:"+stringField(row, "link_id"), "advances", stringField(row, "link_id"), stringField(row, "task_id"), stringField(row, "created_at")))
		}
	}
	if includeOut && want("parent_of") {
		sql := `SELECT id, parent_id, CAST(updated_at AS STRING) AS updated_at
			FROM ` + a.Descriptor.TaskTable + ` WHERE ` + a.projectWhereAnd() + `parent_id = @id LIMIT 200`
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": opts.ProjectID, "id": nodeID})
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			out = append(out, nativeEdge(opts.ProjectID, "parent_of:"+nodeID+":"+stringField(row, "id"), "parent_of", nodeID, stringField(row, "id"), stringField(row, "updated_at")))
		}
	}
	if includeIn && want("child_of") {
		sql := `SELECT id, parent_id, CAST(updated_at AS STRING) AS updated_at
			FROM ` + a.Descriptor.TaskTable + ` WHERE ` + a.projectWhereAnd() + `id = @id AND parent_id IS NOT NULL LIMIT 1`
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": opts.ProjectID, "id": nodeID})
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			out = append(out, nativeEdge(opts.ProjectID, "child_of:"+nodeID+":"+stringField(row, "parent_id"), "child_of", nodeID, stringField(row, "parent_id"), stringField(row, "updated_at")))
		}
	}
	if want("gates") || want("gated_by") {
		sql := `SELECT dep_id, task_id, depends_on_task_id, CAST(created_at AS STRING) AS created_at
			FROM ` + a.Descriptor.DepTable + ` WHERE ` + a.projectWhereAnd() + `(task_id = @id OR depends_on_task_id = @id)
			AND COALESCE(kind, 'gates') != 'archived' LIMIT 200`
		res, err := a.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"project_id": opts.ProjectID, "id": nodeID})
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			taskID := stringField(row, "task_id")
			dependsOn := stringField(row, "depends_on_task_id")
			depID := stringField(row, "dep_id")
			if want("gated_by") && ((includeOut && taskID == nodeID) || (includeIn && dependsOn == nodeID)) {
				out = append(out, nativeReciprocalEdge(opts.ProjectID, depID, "gated_by", taskID, dependsOn, stringField(row, "created_at")))
			}
			if want("gates") && ((includeOut && dependsOn == nodeID) || (includeIn && taskID == nodeID)) {
				out = append(out, nativeReciprocalEdge(opts.ProjectID, depID, "gates", dependsOn, taskID, stringField(row, "created_at")))
			}
		}
	}
	return out, nil
}

// nativeTraverseClosure is the fleet_plan_native equivalent of
// SpannerSubstrateAdapter.TraverseClosure. The generic implementation runs a
// single GRAPH_TABLE query over a substrate's node_table/edge_table; a
// fleet_plan_native substrate has neither (it declares task_table /
// worklink_table / dep_table instead — see task-graph.config.json), so there
// is no property-graph view to query in one shot. Instead this walks the
// graph one hop at a time via nativeNeighbors, starting from startNodeID and
// expanding the frontier up to maxDepth hops or resultCap discovered nodes,
// whichever comes first. direction "forward" (closure) follows out-edges;
// "reverse" (impact / blast radius) follows in-edges.
func nativeTraverseClosure(ctx context.Context, a *SpannerSubstrateAdapter, projectID, startNodeID string, edgeLabels []string, maxDepth, resultCap int, direction string) ([]Node, error) {
	if projectID == "" {
		return nil, fmt.Errorf("project_id required for task-plan native traversal")
	}
	if startNodeID == "" {
		return nil, fmt.Errorf("node_id required for task-plan native traversal")
	}
	hopDirection := "out"
	if direction == "reverse" {
		hopDirection = "in"
	}
	visited := map[string]bool{startNodeID: true}
	frontier := []string{startNodeID}
	var out []Node
	for depth := 0; depth < maxDepth && len(frontier) > 0 && len(out) < resultCap; depth++ {
		var next []string
		for _, current := range frontier {
			edges, err := nativeNeighbors(ctx, a, current, NeighborOpts{
				Direction:  hopDirection,
				EdgeLabels: edgeLabels,
				ProjectID:  projectID,
			})
			if err != nil {
				return nil, err
			}
			for _, e := range edges {
				other := e.TargetID
				if other == current {
					other = e.SourceID
				}
				if visited[other] {
					continue
				}
				visited[other] = true
				node, err := nativeReadNode(ctx, a, projectID, other)
				if err != nil {
					// A dependency/worklink row can point at a node that was
					// since retired or never admitted (a dangling edge) —
					// skip it rather than failing the whole traversal.
					continue
				}
				out = append(out, node)
				next = append(next, other)
				if len(out) >= resultCap {
					break
				}
			}
			if len(out) >= resultCap {
				break
			}
		}
		frontier = next
	}
	return out, nil
}

func nativeTaskNodeFromRow(row map[string]interface{}) Node {
	id := stringField(row, "id")
	projectID := stringField(row, "project_id")
	created := parseTimeField(row, "created_at")
	updated := parseTimeField(row, "updated_at")
	payload := map[string]interface{}{
		"project_id":       projectID,
		"title":            stringField(row, "title"),
		"pillar":           stringField(row, "pillar"),
		"status":           stringField(row, "status"),
		"owner":            stringField(row, "owner"),
		"parent_id":        stringField(row, "parent_id"),
		"is_gap":           boolField(row, "is_gap"),
		"gap_label":        stringField(row, "gap_label"),
		"note":             stringField(row, "note"),
		"fleet_created_at": stringField(row, "created_at"),
		"fleet_updated_at": stringField(row, "updated_at"),
		"session_id":       stringField(row, "session_id"),
		"repo":             stringField(row, "repo"),
		"is_critical_path": nativeNullableBoolField(row, "is_critical_path"),
	}
	return Node{
		NodeID:         id,
		NodeKind:       "task",
		OrganizationID: "task-plan",
		ProjectID:      projectID,
		Status:         "active",
		CreatedAt:      created,
		UpdatedAt:      updated,
		CurrentVersion: NodeVersion{
			NodeID:     id,
			Version:    1,
			Status:     "active",
			Contract:   map[string]interface{}{"canonical_name": id, "task_ref": id, "level": stringField(row, "layer"), "project_id": projectID},
			Payload:    payload,
			AdmittedAt: created,
			AdmittedBy: "task-plan-native",
		},
	}
}

// nativeNullableBoolField preserves the tri-state semantics of nullable BOOL
// columns when a task-plan row is projected into a generic node payload.
// SQL NULL becomes JSON null; concrete booleans remain booleans. The string
// forms are retained for compatibility with adapters that surface BOOL query
// expressions as "true"/"false" strings.
func nativeNullableBoolField(row map[string]interface{}, column string) interface{} {
	value, ok := row[column]
	if !ok || value == nil {
		return nil
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		if typed == "true" {
			return true
		}
		if typed == "false" {
			return false
		}
	}
	// Preserve an unexpected representation so a later write fails visibly
	// at the typed substrate boundary instead of silently converting it.
	return value
}

func nativeArtifactNodeFromRow(row map[string]interface{}) Node {
	id := stringField(row, "link_id")
	projectID := stringField(row, "project_id")
	created := parseTimeField(row, "created_at")
	payload := map[string]interface{}{
		"project_id":       projectID,
		"task_id":          stringField(row, "task_id"),
		"thread":           stringField(row, "thread"),
		"note":             stringField(row, "note"),
		"fleet_created_at": stringField(row, "created_at"),
		"session_id":       stringField(row, "session_id"),
		"repo":             stringField(row, "repo"),
		"branch":           stringField(row, "branch"),
		"commit_sha":       stringField(row, "commit_sha"),
		"pr_number":        intField(row, "pr_number"),
		"pr_state":         stringField(row, "pr_state"),
		"worktree_path":    stringField(row, "worktree_path"),
		"url":              stringField(row, "url"),
		"artifact_status":  stringField(row, "artifact_status"),
	}
	status := "active"
	switch stringField(row, "artifact_status") {
	case "archived":
		status = "archived"
	case worklinkSupersededStatus:
		status = worklinkSupersededStatus
	}
	return Node{
		NodeID:         id,
		NodeKind:       "artifact",
		OrganizationID: "task-plan",
		ProjectID:      projectID,
		Status:         status,
		CreatedAt:      created,
		UpdatedAt:      created,
		CurrentVersion: NodeVersion{
			NodeID:     id,
			Version:    1,
			Status:     status,
			Contract:   map[string]interface{}{"canonical_name": id, "artifact_type_kind": stringField(row, "artifact_type"), "artifact_ref": stringField(row, "artifact_ref"), "project_id": projectID},
			Payload:    payload,
			AdmittedAt: created,
			AdmittedBy: "task-plan-native",
		},
	}
}

func nativeEdge(projectID, edgeID, label, sourceID, targetID, admittedAt string) Edge {
	return Edge{
		EdgeID:         edgeID,
		OrganizationID: "task-plan",
		ProjectID:      projectID,
		SourceID:       sourceID,
		TargetID:       targetID,
		EdgeLabel:      label,
		Status:         "active",
		AdmittedAt:     parseRFC3339(admittedAt),
	}
}

func nativeReciprocalEdge(projectID, edgeID, label, sourceID, targetID, admittedAt string) Edge {
	edge := nativeEdge(projectID, edgeID, label, sourceID, targetID, admittedAt)
	edge.ReciprocalEdgeID = edgeID
	return edge
}

func nativeMutationReceipt(env *Env, projectID, operation, artifactType, artifactID, table string, rows int64, at time.Time) NativeMutationReceipt {
	mutatedBy := env.Config.ExperimentalAdmitter
	if mutatedBy == "" {
		mutatedBy = "task-plan-native"
	}
	return NativeMutationReceipt{
		ReceiptType:     "native_mutation_receipt",
		StoreType:       fleetPlanNativeStoreType,
		Operation:       operation,
		ArtifactType:    artifactType,
		OrganizationID:  "task-plan",
		ProjectID:       projectID,
		ArtifactID:      artifactID,
		ArtifactVersion: 1,
		MutatedAt:       at,
		MutatedBy:       mutatedBy,
		Table:           table,
		RowsAffected:    rows,
		Evidence:        "live task-plan table mutation; no admission ceremony executed",
	}
}

func nullString(s string) spanner.NullString {
	if s == "" {
		return spanner.NullString{Valid: false}
	}
	return spanner.NullString{StringVal: s, Valid: true}
}

func nativeParam(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return spanner.NullString{Valid: false}
	case string:
		return nullString(t)
	default:
		return t
	}
}

func stringFromMap(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func boolFromMap(m map[string]interface{}, key string) bool {
	if m == nil {
		return false
	}
	if v, ok := m[key].(bool); ok {
		return v
	}
	if v, ok := m[key].(string); ok {
		return v == "true"
	}
	return false
}

// nullBoolFromMap reads a nullable boolean-valued payload field, tolerating
// a native Go bool or the "true"/"false" string forms boolFromMap also
// accepts. Unlike boolFromMap it returns spanner.NullBool{Valid:false} — SQL
// NULL — when the key is absent, rather than defaulting to false. That
// distinction matters here: is_critical_path is a NULLABLE column (unlike
// is_gap, which is NOT NULL and always defaults false), and UPDATE already
// treats an absent key as "leave the column untouched" (NULL or whatever it
// already was) rather than "set it false" — see nativeBuildTaskUpdate. This
// keeps CREATE's absent-field behavior consistent with that.
func nullBoolFromMap(m map[string]interface{}, key string) spanner.NullBool {
	if m == nil {
		return spanner.NullBool{Valid: false}
	}
	v, ok := m[key]
	if !ok {
		return spanner.NullBool{Valid: false}
	}
	switch t := v.(type) {
	case bool:
		return spanner.NullBool{Bool: t, Valid: true}
	case string:
		return spanner.NullBool{Bool: t == "true", Valid: true}
	default:
		return spanner.NullBool{Valid: false}
	}
}

// nativeBuildTaskInsert renders the column list and param map for inserting
// a task-plan-native task row. taskID and parentID are the identity and
// relationship values nativeHandleCreate has already resolved and validated
// (parent_ref vs payload.parent_id cross-check, self-parent rejection,
// existence checked inside the caller's transaction) — they are not part of
// the payload-field parity surface nativeTaskFieldColumns declares. now is
// the server-minted created_at/updated_at timestamp. Every other column is
// read from payload (falling back to contract for layer, matching the
// pre-existing contract.level convention), keyed against the same field
// names nativeTaskFieldColumns declares write-able on UPDATE, so this
// builder cannot silently diverge from nativeBuildTaskUpdate again — see
// TestNativeTaskCreateUpdateFieldParity and gt-w1-fp-create-fields.md
// (P3-GT-FP-CREATE-DROPS-FIELDS).
//
// project_id is NOT included here: the caller (nativeHandleCreate) adds it
// to the returned params and to the column list via
// adapter.projectInsertColumns()/projectInsertValues(), the same way the
// pre-refactor INSERT did — project-column handling is a substrate-cutover
// concern orthogonal to which task fields persist.
// nullInt64FromMap reads a nullable integer-valued field. An absent or nil
// key renders as SQL NULL (spanner.NullInt64{Valid:false}) rather than 0, so
// "no PR attached" and "PR #0" are not conflated on a nullable INT64 column.
// A present value is normalized by int64FromAny, which itself falls back to
// SQL NULL on an unparseable value.
func nullInt64FromMap(m map[string]interface{}, key string) interface{} {
	if m == nil {
		return spanner.NullInt64{Valid: false}
	}
	v, ok := m[key]
	if !ok || v == nil {
		return spanner.NullInt64{Valid: false}
	}
	return int64FromAny(v)
}

// intFromMap reads an integer-valued payload field, tolerating the float64
// form a JSON decode produces alongside the int/int64 native forms and the
// decimal-string form. An absent, nil or unparseable value reads as 0, which
// nativeWorklinkInsertFields treats as "not supplied".
func intFromMap(m map[string]interface{}, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	if n, ok := int64FromAny(v).(int64); ok {
		return int(n)
	}
	return 0
}

func nativeBuildTaskInsert(taskID, parentID string, contract, payload map[string]interface{}, now time.Time) (columns []string, params map[string]interface{}) {
	columns = []string{
		"id", "title", "pillar", "layer", "status", "owner", "parent_id",
		"is_gap", "gap_label", "note", "session_id", "repo", "is_critical_path",
		"created_at", "updated_at",
	}
	params = map[string]interface{}{
		"id":               taskID,
		"title":            nullString(stringFromMap(payload, "title")),
		"pillar":           stringFromMap(payload, "pillar"),
		"layer":            firstNonEmpty(stringFromMap(contract, "level"), stringFromMap(payload, "layer"), stringFromMap(payload, "level")),
		"status":           stringFromMap(payload, "status"),
		"owner":            nullString(stringFromMap(payload, "owner")),
		"parent_id":        nullString(parentID),
		"is_gap":           boolFromMap(payload, "is_gap"),
		"gap_label":        nullString(stringFromMap(payload, "gap_label")),
		"note":             nullString(stringFromMap(payload, "note")),
		"session_id":       nullString(stringFromMap(payload, "session_id")),
		"repo":             nullString(stringFromMap(payload, "repo")),
		"is_critical_path": nullBoolFromMap(payload, "is_critical_path"),
		"created_at":       now,
		"updated_at":       now,
	}
	return columns, params
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func int64FromAny(v interface{}) interface{} {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err == nil {
			return n
		}
	}
	return spanner.NullInt64{Valid: false}
}

func parseTimeField(row map[string]interface{}, key string) time.Time {
	return parseRFC3339(stringField(row, key))
}

func parseRFC3339(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999999-07:00", s); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999999-07", s); err == nil {
		return t
	}
	return time.Time{}
}

func timeFromParam(v interface{}) time.Time {
	if t, ok := v.(time.Time); ok {
		return t
	}
	return time.Time{}
}
