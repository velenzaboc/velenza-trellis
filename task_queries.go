package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// task_queries.go implements the task-domain-specific verb surface: the
// six task_graph_* verbs that reproduce the task-plan SQL recipe set
// (see MASTER_PLAN.md "Query/Mutation recipes") against the schema_v1
// Nodes/Edges substrate, where a task's business fields (status, pillar,
// owner, is_gap, gap_label, note, title) live inside the JSON `content`
// (payload) column rather than as typed columns. The generic task_graph_list
// verb cannot filter on those — it only filters on node_kind and the
// admission-lifecycle Nodes.status (active/superseded), not the task's own
// business status — so these verbs build targeted SQL over
// adapter.ExecuteReadOnlyQuery instead.
//
// Three reads: task_graph_tasks, task_graph_task_counts, task_graph_worklinks.
// Five writes: task_graph_add_worklink, task_graph_archive_worklink,
// task_graph_set_status, task_graph_reown, task_graph_claim.
// Schema-v1 writes reuse the generic ceremony (AdmitArtifact / admitEdge).
// Native writes return native mutation receipts; archive_worklink is native-only.

// ---------------- task_graph_tasks ----------------

type tasksInput struct {
	ProjectID string   `json:"project_id,omitempty" jsonschema:"logical task-universe namespace; omitted resolves to config.default_project_id"`
	Pillar    string   `json:"pillar,omitempty" jsonschema:"filter: exact payload.pillar match"`
	Statuses  []string `json:"statuses,omitempty" jsonschema:"filter: payload.status IN this set; omit for no status filter"`
	Owner     string   `json:"owner,omitempty" jsonschema:"filter: exact payload.owner match (e.g. a thread name, or UNOWNED)"`
	IsGap     *bool    `json:"is_gap,omitempty" jsonschema:"filter: payload.is_gap equals this value; omit for no filter"`
	GapLabel  string   `json:"gap_label,omitempty" jsonschema:"filter: exact payload.gap_label match"`
	Level     string   `json:"level,omitempty" jsonschema:"filter: exact contract.level match"`
	Limit     int      `json:"limit,omitempty" jsonschema:"max rows returned; default 500"`
}

// TaskRow is one task node's business fields, flattened out of contract +
// payload for direct consumption — the shape MASTER_PLAN.md's Tasks-table
// SQL recipes return.
type TaskRow struct {
	ProjectID      string `json:"project_id"`
	TaskID         string `json:"task_id"`
	Title          string `json:"title"`
	Pillar         string `json:"pillar"`
	Level          string `json:"level"`
	Status         string `json:"status"`
	Owner          string `json:"owner"`
	SessionID      string `json:"session_id,omitempty"`
	Repo           string `json:"repo,omitempty"`
	ParentID       string `json:"parent_id,omitempty"`
	IsGap          bool   `json:"is_gap"`
	IsCriticalPath bool   `json:"is_critical_path"`
	GapLabel       string `json:"gap_label,omitempty"`
	Note           string `json:"note,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
}

type TaskListResult struct {
	Tasks []TaskRow `json:"tasks"`
}

// handleTasks reproduces MASTER_PLAN.md recipes (a) full listing, (b) tasks
// by owner, (c) unowned gaps, (e) blocked/in_progress — all via one
// filterable query, since they are all "SELECT ... FROM Tasks WHERE <preds>
// ORDER BY pillar, status, id" with different predicates.
func handleTasks(ctx context.Context, env *Env, in tasksInput) (TaskListResult, error) {
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return TaskListResult{}, err
	} else if adapter != nil {
		return nativeHandleTasks(ctx, env, in)
	}
	adapter, err := soleTaskGraphAdapter(ctx, env)
	if err != nil {
		return TaskListResult{}, err
	}
	orgID := env.Config.DefaultOrganizationID
	if orgID == "" {
		return TaskListResult{}, fmt.Errorf("default_organization_id required")
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 500
	}

	sql := `SELECT node_id,
		JSON_VALUE(content, '$.title') AS title,
		JSON_VALUE(content, '$.pillar') AS pillar,
		JSON_VALUE(contract, '$.level') AS level,
		JSON_VALUE(content, '$.status') AS status,
		JSON_VALUE(content, '$.owner') AS owner,
		JSON_VALUE(content, '$.is_gap') AS is_gap,
		JSON_VALUE(content, '$.gap_label') AS gap_label,
		JSON_VALUE(content, '$.note') AS note
	FROM Nodes
	WHERE organization_id = @org AND node_kind = 'task' AND status = 'active'`
	params := map[string]interface{}{"org": orgID}
	if in.Pillar != "" {
		sql += ` AND JSON_VALUE(content, '$.pillar') = @pillar`
		params["pillar"] = in.Pillar
	}
	if len(in.Statuses) > 0 {
		sql += ` AND JSON_VALUE(content, '$.status') IN UNNEST(@statuses)`
		params["statuses"] = in.Statuses
	}
	if in.Owner != "" {
		sql += ` AND JSON_VALUE(content, '$.owner') = @owner`
		params["owner"] = in.Owner
	}
	if in.IsGap != nil {
		// SAFE_CAST rather than a raw string/bool comparison: GoogleSQL's
		// documented JSON_VALUE contract returns STRING ('true'/'false');
		// SAFE_CAST(... AS BOOL) accepts that STRING form and is also a
		// harmless identity cast on backends that surface the boolean
		// scalar as BOOL directly — portable across real Spanner and the
		// emulator without assuming either typing.
		sql += ` AND SAFE_CAST(JSON_VALUE(content, '$.is_gap') AS BOOL) = @is_gap`
		params["is_gap"] = *in.IsGap
	}
	if in.GapLabel != "" {
		sql += ` AND JSON_VALUE(content, '$.gap_label') = @gap_label`
		params["gap_label"] = in.GapLabel
	}
	if in.Level != "" {
		sql += ` AND JSON_VALUE(contract, '$.level') = @level`
		params["level"] = in.Level
	}
	sql += fmt.Sprintf(` ORDER BY pillar, status, node_id LIMIT %d`, limit)

	res, err := adapter.ExecuteReadOnlyQuery(ctx, sql, params)
	if err != nil {
		return TaskListResult{}, fmt.Errorf("task_graph_tasks: %w", err)
	}
	out := TaskListResult{}
	for _, row := range res.Rows {
		out.Tasks = append(out.Tasks, TaskRow{
			TaskID:   stringField(row, "node_id"),
			Title:    stringField(row, "title"),
			Pillar:   stringField(row, "pillar"),
			Level:    stringField(row, "level"),
			Status:   stringField(row, "status"),
			Owner:    stringField(row, "owner"),
			IsGap:    boolField(row, "is_gap"),
			GapLabel: stringField(row, "gap_label"),
			Note:     stringField(row, "note"),
		})
	}
	return out, nil
}

// ---------------- task_graph_task_counts ----------------

type taskCountsInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace; omitted resolves to config.default_project_id"`
}

type PillarStatusCount struct {
	Pillar string `json:"pillar"`
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// TaskCountsResult reproduces MASTER_PLAN.md recipe (a)'s grouped count
// query PLUS recipe (c)'s "true distinct-gap count" note (COUNT(DISTINCT
// gap_label) among is_gap=true rows — the 4 named gaps A/B/C/D, distinct
// from GapTaskCount which counts the 5 concrete gap TASKS, since GAP-A maps
// to two tasks).
type TaskCountsResult struct {
	ByPillarStatus []PillarStatusCount `json:"by_pillar_status"`
	// ByPillarCanonicalStatus is ByPillarStatus folded onto the task_statuses
	// registry, so a dashboard neither double counts a status spelled several
	// ways nor misclassifies terminal work. Per pillar it sums to the raw
	// counts exactly.
	ByPillarCanonicalStatus []PillarCanonicalStatusCount `json:"by_pillar_canonical_status"`
	// UnrecognizedStatusLiterals lists, sorted and de-duplicated, every stored
	// status literal no task_statuses member absorbs. Their rows are counted
	// under canonical_status "unrecognized", never dropped.
	UnrecognizedStatusLiterals []string `json:"unrecognized_status_literals"`
	GapTaskCount               int      `json:"gap_task_count"`
	DistinctGapCount           int      `json:"distinct_gap_count"`
}

// PillarCanonicalStatusCount is one (pillar, canonical task status) group.
// CanonicalStatus is a task_statuses member name, or unrecognizedTaskStatus
// when no member absorbs the stored literal. Terminal is the member's
// config-declared terminal flag, and is false for unrecognized.
type PillarCanonicalStatusCount struct {
	Pillar          string `json:"pillar"`
	CanonicalStatus string `json:"canonical_status"`
	Terminal        bool   `json:"terminal"`
	Count           int    `json:"count"`
}

// canonicalStatusCounts folds raw (pillar, status literal) groups onto the
// task_statuses registry. Each literal is Canonicalize'd to its member, and
// the terminal flag is read from that member's config descriptor. A literal
// no member absorbs, including an empty or NULL status, is counted under
// unrecognizedTaskStatus and returned in the literal list. A nil registry
// claims nothing, so every row is unrecognized. Groups are ordered by pillar
// and then canonical status, which keeps the counts content hash
// deterministic.
func canonicalStatusCounts(ctx context.Context, reg TaskStatusRegistry, raw []PillarStatusCount) ([]PillarCanonicalStatusCount, []string, error) {
	type groupKey struct{ pillar, status string }
	groups := map[groupKey]*PillarCanonicalStatusCount{}
	unrecognized := map[string]bool{}
	for _, row := range raw {
		member, terminal, recognized := unrecognizedTaskStatus, false, false
		if reg != nil {
			if canonical, found := reg.Canonicalize(ctx, row.Status); found {
				desc, err := reg.Resolve(ctx, canonical)
				if err != nil {
					return nil, nil, fmt.Errorf("status literal %q canonicalizes to %q, which does not resolve: %w", row.Status, canonical, err)
				}
				member, terminal, recognized = desc.Name(), desc.Terminal(), true
			}
		}
		if !recognized {
			unrecognized[row.Status] = true
		}
		key := groupKey{pillar: row.Pillar, status: member}
		if g, ok := groups[key]; ok {
			g.Count += row.Count
			continue
		}
		groups[key] = &PillarCanonicalStatusCount{Pillar: row.Pillar, CanonicalStatus: member, Terminal: terminal, Count: row.Count}
	}
	out := make([]PillarCanonicalStatusCount, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Pillar != out[j].Pillar {
			return out[i].Pillar < out[j].Pillar
		}
		return out[i].CanonicalStatus < out[j].CanonicalStatus
	})
	literals := make([]string, 0, len(unrecognized))
	for literal := range unrecognized {
		literals = append(literals, literal)
	}
	sort.Strings(literals)
	return out, literals, nil
}

// withCanonicalStatusCounts fills the canonical-status fields of out from its
// raw ByPillarStatus groups.
func withCanonicalStatusCounts(ctx context.Context, reg TaskStatusRegistry, out TaskCountsResult) (TaskCountsResult, error) {
	groups, literals, err := canonicalStatusCounts(ctx, reg, out.ByPillarStatus)
	if err != nil {
		return TaskCountsResult{}, err
	}
	out.ByPillarCanonicalStatus, out.UnrecognizedStatusLiterals = groups, literals
	return out, nil
}

func handleTaskCounts(ctx context.Context, env *Env, in taskCountsInput) (TaskCountsResult, error) {
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return TaskCountsResult{}, err
	} else if adapter != nil {
		return nativeHandleTaskCounts(ctx, env, in.ProjectID)
	}
	adapter, err := soleTaskGraphAdapter(ctx, env)
	if err != nil {
		return TaskCountsResult{}, err
	}
	orgID := env.Config.DefaultOrganizationID
	if orgID == "" {
		return TaskCountsResult{}, fmt.Errorf("default_organization_id required")
	}
	params := map[string]interface{}{"org": orgID}

	groupSQL := `SELECT JSON_VALUE(content, '$.pillar') AS pillar,
		JSON_VALUE(content, '$.status') AS status,
		COUNT(*) AS n
	FROM Nodes
	WHERE organization_id = @org AND node_kind = 'task' AND status = 'active'
	GROUP BY pillar, status
	ORDER BY pillar, status`
	groupRes, err := adapter.ExecuteReadOnlyQuery(ctx, groupSQL, params)
	if err != nil {
		return TaskCountsResult{}, fmt.Errorf("task_graph_task_counts: group query: %w", err)
	}
	out := TaskCountsResult{}
	for _, row := range groupRes.Rows {
		out.ByPillarStatus = append(out.ByPillarStatus, PillarStatusCount{
			Pillar: stringField(row, "pillar"),
			Status: stringField(row, "status"),
			Count:  intField(row, "n"),
		})
	}

	// NULLIF(...,'') per task-plan's SQL semantics: COUNT(DISTINCT gap_label)
	// against the source Tasks table excludes NULL (a gap task with no named
	// gap_label, e.g. a freshly-discovered gap not yet assigned to A/B/C/D).
	// A migrated node with no gap_label carries JSON_VALUE(...) = '' (empty
	// string, not SQL NULL — Nodes.content has no representation of "key
	// present with JSON null" vs "key absent" that JSON_VALUE distinguishes
	// from ''); NULLIF folds '' back to NULL so COUNT(DISTINCT) excludes it
	// the same way the source recipe does.
	gapSQL := `SELECT COUNT(*) AS gap_task_count,
		COUNT(DISTINCT NULLIF(JSON_VALUE(content, '$.gap_label'), '')) AS distinct_gap_count
	FROM Nodes
	WHERE organization_id = @org AND node_kind = 'task' AND status = 'active'
	  AND SAFE_CAST(JSON_VALUE(content, '$.is_gap') AS BOOL) = TRUE`
	gapRes, err := adapter.ExecuteReadOnlyQuery(ctx, gapSQL, params)
	if err != nil {
		return TaskCountsResult{}, fmt.Errorf("task_graph_task_counts: gap query: %w", err)
	}
	if len(gapRes.Rows) > 0 {
		out.GapTaskCount = intField(gapRes.Rows[0], "gap_task_count")
		out.DistinctGapCount = intField(gapRes.Rows[0], "distinct_gap_count")
	}
	out, err = withCanonicalStatusCounts(ctx, env.TaskStatuses, out)
	if err != nil {
		return TaskCountsResult{}, fmt.Errorf("task_graph_task_counts: %w", err)
	}
	return out, nil
}

// ---------------- task_graph_worklinks ----------------

type worklinksInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace; omitted resolves to config.default_project_id"`
	TaskID    string `json:"task_id" jsonschema:"the task node_id to fetch advancing artifacts for"`
}

type WorklinkRow struct {
	ProjectID    string `json:"project_id"`
	ArtifactID   string `json:"artifact_id"`
	TaskID       string `json:"task_id,omitempty"`
	ArtifactType string `json:"artifact_type"`
	ArtifactRef  string `json:"artifact_ref"`
	Thread       string `json:"thread,omitempty"`
	Note         string `json:"note,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	Repo         string `json:"repo,omitempty"`
	Branch       string `json:"branch,omitempty"`
	CommitSHA    string `json:"commit_sha,omitempty"`
	PRNumber     int64  `json:"pr_number,omitempty"`
	PRState      string `json:"pr_state,omitempty"`
	WorktreePath string `json:"worktree_path,omitempty"`
	URL          string `json:"url,omitempty"`
}

type projectSnapshotInput struct {
	ProjectID    string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace; omitted resolves to config.default_project_id"`
	Limit        int    `json:"limit,omitempty" jsonschema:"maximum tasks, dependencies, and worklinks per collection; default 5000"`
	Continuation string `json:"continuation,omitempty" jsonschema:"opaque authenticated continuation returned by a prior partial project snapshot; binds project, scope, root, fixed read timestamp, ordering positions and page contract"`
	// Scope is deliberately opt-in. The empty value preserves the historical
	// project-wide snapshot contract; scoped modes are server-owned projections
	// rather than client-side filters over a capped project read.
	Scope      string `json:"scope,omitempty" jsonschema:"project | execution_subtree | acceptance_dependency_closure"`
	RootTaskID string `json:"root_task_id,omitempty" jsonschema:"required for execution_subtree and acceptance_dependency_closure"`
}

type DependencyRow struct {
	ProjectID       string `json:"project_id"`
	DependencyID    string `json:"dependency_id"`
	TaskID          string `json:"task_id"`
	DependsOnTaskID string `json:"depends_on_task_id"`
	Kind            string `json:"kind,omitempty"`
	Note            string `json:"note,omitempty"`
	CreatedAt       string `json:"created_at,omitempty"`
}

// ProjectSnapshotResult is ONE transactionally coherent projection of a
// project: every collection below was read inside a single Spanner read-only
// transaction, so they all describe the state at ReadTimestamp and cannot mix
// revisions (P3-GT-FP-SNAPSHOT).
type ProjectSnapshotResult struct {
	ProjectID string `json:"project_id"`
	StoreType string `json:"store_type"`
	// GeneratedAt is the wall clock when this response was assembled. It is
	// NOT the coherence coordinate — use ReadTimestamp/Revision for that.
	GeneratedAt string `json:"generated_at"`
	// Revision is the opaque token identifying the state this snapshot
	// describes. Equal revisions describe the same state; compare, do not parse.
	Revision string `json:"revision"`
	// ReadTimestamp is the single Spanner read timestamp every collection below
	// was read at.
	ReadTimestamp string `json:"read_timestamp"`
	// Complete is false whenever one or more collections have additional rows.
	// A partial page is evidence of one coherent read, never an authoritative
	// complete portfolio graph.
	Complete     bool   `json:"complete"`
	Continuation string `json:"continuation,omitempty"`
	// Limit is the per-collection cap in force for this snapshot.
	Limit         int                 `json:"limit"`
	Scope         SnapshotScope       `json:"scope"`
	ContentHashes SnapshotHashes      `json:"content_hashes"`
	Collections   SnapshotCollections `json:"collections"`
	Tasks         []TaskRow           `json:"tasks"`
	Dependencies  []DependencyRow     `json:"dependencies"`
	Worklinks     []WorklinkRow       `json:"worklinks"`
	Counts        TaskCountsResult    `json:"counts"`
}

func handleProjectSnapshot(ctx context.Context, env *Env, in projectSnapshotInput) (ProjectSnapshotResult, error) {
	adapter, err := nativeTaskQueryAdapter(ctx, env)
	if err != nil {
		return ProjectSnapshotResult{}, err
	}
	if adapter == nil {
		return ProjectSnapshotResult{}, fmt.Errorf("task_graph_project_snapshot requires fleet_plan_native store")
	}
	// The adapter is passed twice on purpose: once as the SQL-shape authority
	// (descriptor + project-column helpers) and once as the SnapshotReader the
	// coherent read runs through. Splitting the two is what lets a test drive
	// the handler with a fake reader and no Spanner client.
	return nativeHandleProjectSnapshot(ctx, env, adapter, adapter, in)
}

type WorklinkListResult struct {
	Worklinks []WorklinkRow `json:"worklinks"`
}

// handleWorklinks reproduces MASTER_PLAN.md recipe (d): every artifact
// advancing a given task, via the "advances" edge (artifact -> task).
func handleWorklinks(ctx context.Context, env *Env, in worklinksInput) (WorklinkListResult, error) {
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return WorklinkListResult{}, err
	} else if adapter != nil {
		return nativeHandleWorklinks(ctx, env, in)
	}
	if in.TaskID == "" {
		return WorklinkListResult{}, fmt.Errorf("task_id required")
	}
	adapter, err := soleTaskGraphAdapter(ctx, env)
	if err != nil {
		return WorklinkListResult{}, err
	}
	orgID := env.Config.DefaultOrganizationID
	if orgID == "" {
		return WorklinkListResult{}, fmt.Errorf("default_organization_id required")
	}

	sql := `SELECT a.node_id AS artifact_id,
		JSON_VALUE(a.contract, '$.artifact_type_kind') AS artifact_type,
		JSON_VALUE(a.contract, '$.artifact_ref') AS artifact_ref,
		JSON_VALUE(a.content, '$.thread') AS thread,
		JSON_VALUE(a.content, '$.note') AS note,
		JSON_VALUE(a.content, '$.fleet_created_at') AS created_at
	FROM Edges e
	JOIN Nodes a
	  ON a.organization_id = e.organization_id
	 AND a.node_id = e.source_node_id
	 AND a.version = e.source_version
	WHERE e.organization_id = @org
	  AND e.target_node_id = @task_id
	  AND e.edge_label = 'advances'
	  AND e.status = 'active'
	  AND a.status = 'active'
	ORDER BY created_at`
	res, err := adapter.ExecuteReadOnlyQuery(ctx, sql, map[string]interface{}{"org": orgID, "task_id": in.TaskID})
	if err != nil {
		return WorklinkListResult{}, fmt.Errorf("task_graph_worklinks: %w", err)
	}
	out := WorklinkListResult{}
	for _, row := range res.Rows {
		out.Worklinks = append(out.Worklinks, WorklinkRow{
			ArtifactID:   stringField(row, "artifact_id"),
			ArtifactType: stringField(row, "artifact_type"),
			ArtifactRef:  stringField(row, "artifact_ref"),
			Thread:       stringField(row, "thread"),
			Note:         stringField(row, "note"),
			CreatedAt:    stringField(row, "created_at"),
		})
	}
	return out, nil
}

// ---------------- task_graph_add_worklink ----------------

// addWorklinkInput is the caller-facing surface of task_graph_add_worklink.
//
// The provenance fields below session_id are NOT decoration: every one of
// them is a real, live TaskLinks column that task_graph_update has always
// accepted and persisted, while the create path had no way to accept them at
// all — so a caller could only record a worklink's repo/branch/commit by
// writing it and then immediately updating it. That asymmetry is
// P3-GT-FP-WORKLINK-FIELD-PARITY, the worklink-lane sibling of
// P3-GT-FP-CREATE-DROPS-FIELDS. Widening this struct is the caller-facing
// half of closing it; nativeWorklinkFieldColumns is the persistence half.
type addWorklinkInput struct {
	ProjectID    string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace; omitted resolves to config.default_project_id"`
	TaskID       string `json:"task_id" jsonschema:"the task node_id this artifact advances"`
	ArtifactID   string `json:"artifact_id,omitempty" jsonschema:"stable id for the new artifact node; auto-minted (uuid) when omitted"`
	ArtifactType string `json:"artifact_type" jsonschema:"pr | circuit | spec | dispatch | finding | commit | binding | ..."`
	ArtifactRef  string `json:"artifact_ref" jsonschema:"e.g. a PR URL, circuit name, or spec section"`
	Thread       string `json:"thread,omitempty" jsonschema:"the fleet thread that produced this artifact"`
	Note         string `json:"note,omitempty"`
	SessionID    string `json:"session_id,omitempty" jsonschema:"the session that produced this artifact"`
	Repo         string `json:"repo,omitempty" jsonschema:"repository the artifact lives in, e.g. task-graph"`
	Branch       string `json:"branch,omitempty" jsonschema:"branch the work landed on"`
	CommitSHA    string `json:"commit_sha,omitempty" jsonschema:"commit sha this artifact resolves to"`
	PRNumber     int    `json:"pr_number,omitempty" jsonschema:"pull-request number; omitted or 0 records SQL NULL"`
	PRState      string `json:"pr_state,omitempty" jsonschema:"pull-request state, e.g. open | merged | closed"`
	WorktreePath string `json:"worktree_path,omitempty" jsonschema:"absolute path of the worktree the work was done in"`
	URL          string `json:"url,omitempty" jsonschema:"canonical URL for the artifact"`
}

type archiveWorklinkInput struct {
	ProjectID string `json:"project_id,omitempty" jsonschema:"logical task-universe namespace; omitted resolves to config.default_project_id"`
	TaskID    string `json:"task_id" jsonschema:"the owning task id; archive fails if the link belongs to another task"`
	LinkID    string `json:"link_id" jsonschema:"stable TaskLinks link id to archive"`
}

// supersedeWorklinkInput is the caller-facing surface of
// task_graph_supersede_worklink: the one lifecycle transition that retires a
// VALID active binding worklink (typically a merged branch) and installs its
// successor in the same transaction. It is deliberately not a generic update:
// every field of the predecessor and successor is bound exactly, so the caller
// is a compare-and-set request and never the authority for the current state.
type supersedeWorklinkInput struct {
	ProjectID           string                       `json:"project_id,omitempty" jsonschema:"logical task-universe namespace; omitted resolves to config.default_project_id"`
	TaskID              string                       `json:"task_id" jsonschema:"the owning task id; the predecessor must belong to this task"`
	ExpectedOwner       string                       `json:"expected_owner" jsonschema:"the task's current owner; a mismatch fails closed"`
	ExpectedSessionID   string                       `json:"expected_session_id" jsonschema:"the task's current session_id; a mismatch fails closed"`
	PredecessorLinkID   string                       `json:"predecessor_link_id" jsonschema:"stable link id of the active binding worklink being superseded"`
	ExpectedPredecessor supersedePredecessorIdentity `json:"expected_predecessor" jsonschema:"exact fields the predecessor must currently carry"`
	Successor           supersedeSuccessor           `json:"successor" jsonschema:"the exact successor binding worklink to install"`
}

type supersedePredecessorIdentity struct {
	ArtifactType   string `json:"artifact_type" jsonschema:"must be worktree"`
	ArtifactStatus string `json:"artifact_status" jsonschema:"must be recorded, the only active status"`
	Repo           string `json:"repo"`
	Branch         string `json:"branch"`
	WorktreePath   string `json:"worktree_path"`
	Thread         string `json:"thread"`
	SessionID      string `json:"session_id"`
}

type supersedeSuccessor struct {
	LinkID       string `json:"link_id" jsonschema:"stable id of the successor worklink; an identical retry converges on it"`
	ArtifactType string `json:"artifact_type" jsonschema:"must be worktree"`
	ArtifactRef  string `json:"artifact_ref"`
	Repo         string `json:"repo" jsonschema:"must equal the predecessor repository"`
	Branch       string `json:"branch"`
	WorktreePath string `json:"worktree_path"`
	Thread       string `json:"thread" jsonschema:"must equal the predecessor thread"`
	SessionID    string `json:"session_id" jsonschema:"must equal the predecessor and task session_id"`
}

// WorklinkSupersedeReceipt names both identities of one supersede transition.
// It is issued only by the transaction that observed or committed the state it
// reports; RowsAffected is 2 for the first commit and 0 for an identical retry.
type WorklinkSupersedeReceipt struct {
	ReceiptKind       string    `json:"receipt_kind"`
	Issuer            string    `json:"issuer"`
	ProjectID         string    `json:"project_id"`
	TaskID            string    `json:"task_id"`
	PredecessorLinkID string    `json:"predecessor_link_id"`
	SuccessorLinkID   string    `json:"successor_link_id"`
	PredecessorStatus string    `json:"predecessor_status"`
	Idempotent        bool      `json:"idempotent"`
	RowsAffected      int64     `json:"rows_affected"`
	Epoch             time.Time `json:"epoch"`
}

// WorklinkSupersedeResult is the write envelope of task_graph_supersede_worklink.
type WorklinkSupersedeResult struct {
	NativeMutationReceipt *NativeMutationReceipt    `json:"native_mutation_receipt,omitempty"`
	SupersedeReceipt      *WorklinkSupersedeReceipt `json:"supersede_receipt,omitempty"`
}

// handleAddWorklink reproduces MASTER_PLAN.md's "INSERT a new work-link"
// mutation: mint one artifact node, then link it to the task via an
// 'advances' edge. Two ceremony calls composed — the same as a caller doing
// task_graph_create(node_kind=artifact) followed by
// task_graph_link(edge_label=advances) by hand.
func handleAddWorklink(ctx context.Context, env *Env, in addWorklinkInput) (MutationWriteResult, error) {
	if namesWriterHandoverHistory(in.Note) {
		return MutationWriteResult{}, errWriterHandoverHistory()
	}
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleAddWorklink(ctx, env, in)
	}
	if in.TaskID == "" || in.ArtifactType == "" || in.ArtifactRef == "" {
		return MutationWriteResult{}, fmt.Errorf("task_id, artifact_type, artifact_ref required")
	}
	desc, err := env.Kinds.Resolve(ctx, "artifact")
	if err != nil {
		return MutationWriteResult{}, err
	}
	artifactID := in.ArtifactID
	if artifactID == "" {
		artifactID = "LNK-" + uuid.NewString()
	}
	sub := ArtifactSubmission{
		ArtifactType:   desc.ArtifactType(),
		OrganizationID: env.Config.DefaultOrganizationID,
		NodeKind:       "artifact",
		StableID:       artifactID,
		Contract: map[string]interface{}{
			"canonical_name":     artifactID,
			"artifact_type_kind": in.ArtifactType,
			"artifact_ref":       in.ArtifactRef,
		},
		Payload: map[string]interface{}{
			"thread":           in.Thread,
			"note":             in.Note,
			"fleet_created_at": time.Now().UTC().Format(time.RFC3339),
		},
	}
	// The migrated (doc-graph) mode stores the artifact node's payload as
	// JSON rather than as TaskLinks columns, so it has no column list to
	// drift out of sync with — but it MUST still carry the provenance fields
	// the widened addWorklinkInput now accepts, or the same field silently
	// vanishes here instead of on the native path. One projection
	// (nativeWorklinkInsertFields) feeds both modes so there is exactly one
	// place a new field has to be added.
	for k, v := range nativeWorklinkInsertFields(in) {
		sub.Payload[k] = v
	}
	if _, err := AdmitArtifact(ctx, env, sub); err != nil {
		return MutationWriteResult{}, fmt.Errorf("admit artifact node: %w", err)
	}
	rec, err := admitEdge(ctx, env, artifactID, in.TaskID, "advances")
	if err != nil {
		return MutationWriteResult{}, err
	}
	return admissionWriteResult(rec), nil
}

func handleArchiveWorklink(ctx context.Context, env *Env, in archiveWorklinkInput) (MutationWriteResult, error) {
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleArchiveWorklink(ctx, env, in)
	}
	return MutationWriteResult{}, fmt.Errorf("task_graph_archive_worklink requires fleet_plan_native store")
}

func handleSupersedeWorklink(ctx context.Context, env *Env, in supersedeWorklinkInput) (WorklinkSupersedeResult, error) {
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return WorklinkSupersedeResult{}, err
	} else if adapter != nil {
		return nativeHandleSupersedeWorklink(ctx, env, in)
	}
	return WorklinkSupersedeResult{}, fmt.Errorf("task_graph_supersede_worklink requires fleet_plan_native store")
}

// ---------------- task_graph_set_status / task_graph_reown ----------------

type setStatusInput struct {
	ProjectID string `json:"project_id,omitempty"`
	TaskID    string `json:"task_id"`
	Status    string `json:"status" jsonschema:"done | in_progress | blocked | gap | not_started | discovered | ..."`
}

// handleSetStatus reproduces MASTER_PLAN.md's "UPDATE a task's status"
// mutation: re-admit the task with the SAME contract (identity is
// immutable) and every payload field preserved except status.
func handleSetStatus(ctx context.Context, env *Env, in setStatusInput) (MutationWriteResult, error) {
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleSetStatus(ctx, env, in)
	}
	if in.TaskID == "" || in.Status == "" {
		return MutationWriteResult{}, fmt.Errorf("task_id, status required")
	}
	rec, err := mergeTaskPayload(ctx, env, in.ProjectID, in.TaskID, func(payload map[string]interface{}) {
		payload["status"] = in.Status
	})
	if err != nil {
		return MutationWriteResult{}, err
	}
	return admissionWriteResult(rec), nil
}

type reownInput struct {
	ProjectID         string               `json:"project_id,omitempty"`
	TaskID            string               `json:"task_id"`
	Owner             string               `json:"owner" jsonschema:"a fleet thread name, or UNOWNED"`
	SessionID         string               `json:"session_id,omitempty" jsonschema:"new session binding; required for owned handover"`
	Repo              string               `json:"repo,omitempty" jsonschema:"new repository binding; required for owned handover"`
	ExpectedOwner     string               `json:"expected_owner,omitempty" jsonschema:"current owner required for an owned-task handover"`
	ExpectedSessionID string               `json:"expected_session_id,omitempty" jsonschema:"current session required for an owned-task handover"`
	ExpectedRepo      string               `json:"expected_repo,omitempty" jsonschema:"current repository required for an owned-task handover"`
	WriterHandover    *writerHandoverInput `json:"writer_handover,omitempty" jsonschema:"native-only exact protected-worktree writer handover performed atomically with the task tuple"`
}

// writerHandoverInput names the sole existing protected worktree writer by
// its exact historical coordinates. The target thread is explicit because a
// task owner is a worker identity while TaskLinks.thread is the Codex/Claude
// conversation identity; they are deliberately not interchangeable.
type writerHandoverInput struct {
	LinkID         string `json:"link_id" jsonschema:"stable id of the existing active worktree binding"`
	ArtifactType   string `json:"artifact_type" jsonschema:"must be worktree"`
	ArtifactStatus string `json:"artifact_status" jsonschema:"must be recorded"`
	Repo           string `json:"repo"`
	Branch         string `json:"branch"`
	WorktreePath   string `json:"worktree_path"`
	Thread         string `json:"thread" jsonschema:"exact historical writer thread"`
	SessionID      string `json:"session_id" jsonschema:"exact historical writer session"`
	TargetThread   string `json:"target_thread" jsonschema:"new writer conversation identity; distinct from task owner"`
}

// WriterHandoverReceipt is issued only from the native transaction that
// committed or exactly observed the coherent task+writer state. The history
// link is a deterministic immutable finding row, so an exact response-loss
// retry can prove it is a zero-write retry.
type WriterHandoverReceipt struct {
	ReceiptKind   string    `json:"receipt_kind"`
	Issuer        string    `json:"issuer"`
	ProjectID     string    `json:"project_id"`
	TaskID        string    `json:"task_id"`
	BindingLinkID string    `json:"binding_link_id"`
	HistoryLinkID string    `json:"history_link_id"`
	FromThread    string    `json:"from_thread"`
	FromSessionID string    `json:"from_session_id"`
	ToThread      string    `json:"to_thread"`
	ToSessionID   string    `json:"to_session_id"`
	Idempotent    bool      `json:"idempotent"`
	RowsAffected  int64     `json:"rows_affected"`
	Epoch         time.Time `json:"epoch"`
}

// claimInput binds an unowned task to one worker session. Claim is deliberately
// narrower than reown: the native store performs a conditional first-writer-
// wins update and never treats updated_at as a lease or expiry mechanism.
type claimInput struct {
	ProjectID string        `json:"project_id,omitempty"`
	TaskID    string        `json:"task_id"`
	Owner     string        `json:"owner" jsonschema:"the claiming fleet thread name"`
	SessionID string        `json:"session_id" jsonschema:"the claiming session identifier"`
	Repo      string        `json:"repo,omitempty" jsonschema:"repository associated with the claim"`
	Binding   *ClaimBinding `json:"binding,omitempty" jsonschema:"optional branch/worktree and execution identity evidence"`
}

type ClaimBinding struct {
	Repo         string `json:"repo,omitempty"`
	Branch       string `json:"branch,omitempty"`
	WorktreePath string `json:"worktree_path,omitempty"`
	Host         string `json:"host,omitempty"`
	Account      string `json:"account,omitempty"`
	Harness      string `json:"harness,omitempty"`
	Thread       string `json:"thread,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
}

func handleClaim(ctx context.Context, env *Env, in claimInput) (MutationWriteResult, error) {
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleClaim(ctx, env, in)
	}
	return MutationWriteResult{}, fmt.Errorf("task_graph_claim requires fleet_plan_native store")
}

func validateReownInput(in reownInput) error {
	if strings.TrimSpace(in.TaskID) == "" || strings.TrimSpace(in.Owner) == "" {
		return fmt.Errorf("task_id, owner required")
	}
	if in.Owner == "UNOWNED" {
		if strings.TrimSpace(in.SessionID) != "" || strings.TrimSpace(in.Repo) != "" {
			return fmt.Errorf("session_id and repo must be empty when owner is UNOWNED")
		}
		if in.WriterHandover != nil {
			return fmt.Errorf("writer_handover requires an owned target")
		}
		return nil
	}
	if strings.TrimSpace(in.SessionID) == "" || strings.TrimSpace(in.Repo) == "" {
		return fmt.Errorf("session_id, repo required for owned handover")
	}
	if in.WriterHandover != nil {
		w := in.WriterHandover
		for _, field := range []struct{ name, value string }{
			{"writer_handover.link_id", w.LinkID}, {"writer_handover.artifact_type", w.ArtifactType},
			{"writer_handover.artifact_status", w.ArtifactStatus}, {"writer_handover.repo", w.Repo},
			{"writer_handover.branch", w.Branch}, {"writer_handover.worktree_path", w.WorktreePath},
			{"writer_handover.thread", w.Thread}, {"writer_handover.session_id", w.SessionID},
			{"writer_handover.target_thread", w.TargetThread},
		} {
			if strings.TrimSpace(field.value) == "" {
				return fmt.Errorf("%s required", field.name)
			}
		}
		if w.ArtifactType != worklinkBindingArtifactType {
			return fmt.Errorf("writer_handover.artifact_type must be %s", worklinkBindingArtifactType)
		}
		if w.ArtifactStatus != worklinkRecordedStatus {
			return fmt.Errorf("writer_handover.artifact_status must be %s", worklinkRecordedStatus)
		}
		if w.Repo != in.Repo {
			return fmt.Errorf("writer_handover.repo must equal the target task repo")
		}
	}
	return nil
}

// handleReown reproduces MASTER_PLAN.md's "RE-OWN a gap / task" mutation.
func handleReown(ctx context.Context, env *Env, in reownInput) (MutationWriteResult, error) {
	if err := validateReownInput(in); err != nil {
		return MutationWriteResult{}, err
	}
	if adapter, err := nativeTaskQueryAdapter(ctx, env); err != nil {
		return MutationWriteResult{}, err
	} else if adapter != nil {
		return nativeHandleReown(ctx, env, in)
	}
	if in.WriterHandover != nil {
		return MutationWriteResult{}, fmt.Errorf("writer_handover requires fleet_plan_native store")
	}
	rec, err := schemaV1HandleReown(ctx, env, in, nil)
	if err != nil {
		return MutationWriteResult{}, err
	}
	return admissionWriteResult(rec), nil
}

func reownExpectedTupleMatches(in reownInput, owner, sessionID, repo string) bool {
	return in.ExpectedOwner == owner && in.ExpectedSessionID == sessionID && in.ExpectedRepo == repo
}

func validateReownExpectedTuple(in reownInput, owner, sessionID, repo string) error {
	if owner == "" || owner == "UNOWNED" {
		if in.Owner == "UNOWNED" && in.ExpectedOwner == "" && in.ExpectedSessionID == "" && in.ExpectedRepo == "" {
			return nil
		}
		return TaskClaimCollisionDetail{TaskID: in.TaskID, Owner: owner, SessionID: sessionID, Repo: repo}
	}
	if strings.TrimSpace(in.ExpectedOwner) == "" || strings.TrimSpace(in.ExpectedSessionID) == "" || strings.TrimSpace(in.ExpectedRepo) == "" {
		return fmt.Errorf("expected_owner, expected_session_id, expected_repo required for an owned-task handover")
	}
	if !reownExpectedTupleMatches(in, owner, sessionID, repo) {
		return TaskClaimCollisionDetail{TaskID: in.TaskID, Owner: owner, SessionID: sessionID, Repo: repo}
	}
	return nil
}

// schemaV1HandleReown prepares the ordinary admission plan, then attaches a
// transaction-owned compare-and-set over the current immutable version and
// its owner/session/repository authorization tuple. beforeCommit is a test
// seam used to deterministically change the observed row between preparation
// and CommitAdmission; production callers pass nil.
func schemaV1HandleReown(ctx context.Context, env *Env, in reownInput, beforeCommit func(NodeVersion) error) (AdmissionRecord, error) {
	projectID, err := resolveProjectID(env, in.ProjectID)
	if err != nil {
		return AdmissionRecord{}, err
	}
	prior, err := readNodeAcrossSubstrates(ctx, env, projectID, in.TaskID, "", "")
	if err != nil {
		return AdmissionRecord{}, fmt.Errorf("read task %q: %w", in.TaskID, err)
	}
	if prior.NodeKind != "task" {
		return AdmissionRecord{}, fmt.Errorf("node %q has node_kind=%q, want task", in.TaskID, prior.NodeKind)
	}
	currentOwner := stringFromMap(prior.CurrentVersion.Payload, "owner")
	currentSession := stringFromMap(prior.CurrentVersion.Payload, "session_id")
	currentRepo := stringFromMap(prior.CurrentVersion.Payload, "repo")
	if err := validateReownExpectedTuple(in, currentOwner, currentSession, currentRepo); err != nil {
		return AdmissionRecord{}, err
	}

	payload := make(map[string]interface{}, len(prior.CurrentVersion.Payload))
	for k, v := range prior.CurrentVersion.Payload {
		payload[k] = v
	}
	payload["owner"] = in.Owner
	payload["session_id"] = in.SessionID
	payload["repo"] = in.Repo

	desc, err := env.Kinds.Resolve(ctx, prior.NodeKind)
	if err != nil {
		return AdmissionRecord{}, err
	}
	adapter, err := env.Substrates.Adapter(ctx, desc)
	if err != nil {
		return AdmissionRecord{}, err
	}
	plan, err := PrepareAdmissionPlan(ctx, env, ArtifactSubmission{
		ArtifactType:   desc.ArtifactType(),
		OrganizationID: env.Config.DefaultOrganizationID,
		NodeKind:       prior.NodeKind,
		StableID:       in.TaskID,
		Contract:       prior.CurrentVersion.Contract,
		Payload:        payload,
		PriorVersion:   &prior.CurrentVersion,
	})
	if err != nil {
		return AdmissionRecord{}, err
	}
	plan.NodeCompareAndSet = &NodeCompareAndSetPrecondition{
		ExpectedVersion: prior.CurrentVersion.Version,
		ExpectedStatus:  "active",
		ExpectedPayloadStrings: map[string]string{
			"owner":      currentOwner,
			"session_id": currentSession,
			"repo":       currentRepo,
		},
	}
	if beforeCommit != nil {
		if err := beforeCommit(prior.CurrentVersion); err != nil {
			return AdmissionRecord{}, fmt.Errorf("before schema-v1 reown commit: %w", err)
		}
	}
	recs, err := adapter.CommitAdmission(ctx, plan)
	if err != nil {
		return AdmissionRecord{}, err
	}
	if len(recs) == 0 {
		return AdmissionRecord{}, fmt.Errorf("CommitAdmission returned no records")
	}
	return recs[0], nil
}

// mergeTaskPayload reads a task node's current version, applies mutate to a
// COPY of its payload (every other field preserved), and re-admits it as a
// version bump with the SAME contract per node_version_chain_axiom. Refuses
// non-task nodes — set_status/reown are task-domain mutations, not generic
// node updates (task_graph_update already covers the generic case).
func mergeTaskPayload(ctx context.Context, env *Env, projectID, taskID string, mutate func(payload map[string]interface{})) (AdmissionRecord, error) {
	resolvedProjectID, err := resolveProjectID(env, projectID)
	if err != nil {
		return AdmissionRecord{}, err
	}
	prior, err := readNodeAcrossSubstrates(ctx, env, resolvedProjectID, taskID, "", "")
	if err != nil {
		return AdmissionRecord{}, fmt.Errorf("read task %q: %w", taskID, err)
	}
	if prior.NodeKind != "task" {
		return AdmissionRecord{}, fmt.Errorf("node %q has node_kind=%q, want task", taskID, prior.NodeKind)
	}
	payload := map[string]interface{}{}
	for k, v := range prior.CurrentVersion.Payload {
		payload[k] = v
	}
	mutate(payload)

	desc, err := env.Kinds.Resolve(ctx, prior.NodeKind)
	if err != nil {
		return AdmissionRecord{}, err
	}
	sub := ArtifactSubmission{
		ArtifactType:   desc.ArtifactType(),
		OrganizationID: env.Config.DefaultOrganizationID,
		NodeKind:       prior.NodeKind,
		StableID:       taskID,
		Contract:       prior.CurrentVersion.Contract,
		Payload:        payload,
		PriorVersion:   &prior.CurrentVersion,
	}
	return AdmitArtifact(ctx, env, sub)
}

// ---------------- shared helpers ----------------

// soleTaskGraphAdapter returns the single registered substrate adapter.
// task-graph runs one substrate (task_graph_iter1) during the experimental
// bootstrap phase per task-graph.config.json.
func soleTaskGraphAdapter(ctx context.Context, env *Env) (SubstrateAdapter, error) {
	all, err := env.Substrates.All(ctx)
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no substrates registered")
	}
	return all[0], nil
}

// boolField reads a boolean-valued column, tolerating either representation
// JSON_VALUE(...) AS BOOL might surface it in: a native Go bool (decoded
// from a Spanner BOOL/structpb.Value_BoolValue) or the string "true"/"false"
// (decoded from a Spanner STRING) — see the SAFE_CAST comment in
// handleTasks for why both are possible.
func boolField(row map[string]interface{}, col string) bool {
	switch v := row[col].(type) {
	case bool:
		return v
	case string:
		return v == "true"
	default:
		return false
	}
}

// stringField reads a string column from an ExecuteReadOnlyQuery row map,
// tolerating nil (SQL NULL decodes to nil via decodeStructpbValue).
func stringField(row map[string]interface{}, col string) string {
	if v, ok := row[col].(string); ok {
		return v
	}
	return ""
}

// intField reads an integer-valued column. Spanner transmits INT64 over the
// wire as a JSON STRING (avoiding float64 precision loss for the full int64
// range), so decodeStructpbValue produces a Go string, not a number, for
// INT64/COUNT(*) results — only genuine FLOAT64 columns decode to float64.
func intField(row map[string]interface{}, col string) int {
	switch v := row[col].(type) {
	case string:
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0
		}
		return n
	case float64:
		return int(v)
	default:
		return 0
	}
}

var nativeTaskQueryAdapter = soleNativeFleetPlanAdapter
