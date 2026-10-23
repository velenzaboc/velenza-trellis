package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
)

// snapshotPin holds the current snapshot id for the MCP session. Every read
// verb operates within it for coherence (per OOD Graph §10 snapshot-pinning
// contract). spec_graph_use rebinds it.
type snapshotPin struct {
	SnapshotID string
}

func cmdMCP(ctx context.Context, _ []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	transportSecret, err := resolveTransportSecret(cfg.TransportAuth)
	if err != nil {
		return fmt.Errorf("load transport auth: %w", err)
	}
	snapshotContinuationKey, snapshotContinuationTTL, err := resolveSnapshotContinuation(cfg.SnapshotContinuation, transportSecret)
	if err != nil {
		return fmt.Errorf("load snapshot continuation: %w", err)
	}

	taskStatuses, err := newTaskStatusRegistry(cfg.TaskStatuses)
	if err != nil {
		return fmt.Errorf("build task_statuses registry: %w", err)
	}
	dependencyKinds, err := newDependencyKindRegistry(cfg.DependencyKinds)
	if err != nil {
		return fmt.Errorf("build dependency_kinds registry: %w", err)
	}
	artifactKinds, err := newArtifactKindRegistry(cfg.ArtifactKinds)
	if err != nil {
		return fmt.Errorf("build artifact_kinds registry: %w", err)
	}

	env := &Env{
		Config:                  cfg,
		Kinds:                   newKindRegistry(cfg.NodeKinds),
		Edges:                   newEdgeRegistry(cfg.EdgeLabels),
		Projections:             newProjectionRegistry(cfg.Projections),
		Substrates:              newRouter(cfg.GoogleCredentials, cfg.AllowSpannerEmulator, cfg.Substrates),
		Now:                     time.Now,
		TaskStatuses:            taskStatuses,
		DependencyKinds:         dependencyKinds,
		ArtifactKinds:           artifactKinds,
		SnapshotContinuationKey: snapshotContinuationKey,
		SnapshotContinuationTTL: snapshotContinuationTTL,
	}
	defer env.Substrates.Close()

	if cfg.Embedding != nil {
		auth, err := vertexai.NewAuth(ctx, cfg.GoogleCredentials.RequestTokenSourceFactory(gcpauth.CloudPlatformScope))
		if err != nil {
			return fmt.Errorf("build vertex auth: %w", err)
		}
		env.Embed, err = vertexai.NewEmbedClient(auth, vertexai.EmbedConfig{
			Project:      cfg.Embedding.Project,
			Location:     cfg.Embedding.Location,
			Model:        cfg.Embedding.Model,
			Dimensions:   cfg.Embedding.Dimensions,
			MaxTextBytes: cfg.Embedding.MaxTextBytes,
		})
		if err != nil {
			return fmt.Errorf("build vertex embedding client: %w", err)
		}
	}

	pin := &snapshotPin{}

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "task-graph",
		Version: "v0.1.0",
	}, nil)

	// ---------------- state ----------------

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_use",
			Description: `Pin a snapshot. Every other read verb operates within it for coherence. Omit snapshot_id to report the current pin. In experimental_bootstrap mode pins are no-ops on read (only one logical snapshot exists).`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in useInput) (*mcp.CallToolResult, Snapshot, error) {
			out, err := handleUse(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	// ---------------- nodes (write) ----------------

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_create",
			Description: `Create one task or artifact. In schema_v1 mode this admits through the full admission ceremony and returns admission_record; in fleet_plan_native mode this writes the live task-plan table and returns native_mutation_receipt. node_kind is 'task' | 'artifact' (config.node_kinds). project_id is the logical task-universe namespace; omit it only to use config.default_project_id ("arbor" for existing Arbor tasks). Before task-plan schema cutover, only omitted/default project_id="arbor" is accepted; non-default projects fail closed. After cutover, task-graph.post-cutover.config.json enables project_column="project_id" and non-overlapping namespaces. contract carries the identity fields (project_id, canonical_name, task_ref, level | project_id, artifact_type_kind, artifact_ref); payload carries the mutable atomic state (status, pillar, layer, is_gap, is_critical_path, …). For a native task create, supply its optional parent through parent_ref or payload.parent_id; matching dual inputs are accepted, mismatches and self-parenting are rejected, and the resolved parent must exist in the same project. Parent validation and child insertion are atomic; omitting both inputs stores NULL. Every string value is OOD-conformant by construction — no prose, reference-by-stable-id, atomic single-clause values.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in createInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleCreate(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_update",
			Description: `Update an existing node's mutable state. In schema_v1 mode this re-admits as a version bump and returns admission_record; in fleet_plan_native mode this updates the live task-plan row and returns native_mutation_receipt. node_id is the stable canonical_name inside project_id; omit project_id only to use config.default_project_id. Before cutover, only omitted/default project_id="arbor" is accepted; non-default projects fail closed. contract MUST be byte-equivalent to the prior version's (identity is immutable); only payload evolves. This is how a task moves not_started → in_progress → done. Status semantics: passing the row's OWN current status back unchanged (the get→update round trip) always succeeds, even when the stored value is a pre-registry legacy literal (completed/resolved/audit_probe/…) — a status-preserving update never fails on status validation. CHANGING the status requires a canonical task_statuses member name and a legal transition from the current status's canonical member; legacy literals and novel values are rejected as change targets. In fleet_plan_native mode a worklink's artifact_status=superseded is NEVER accepted here (in any case or padding): it is set only by task_graph_supersede_worklink, and a worklink that is already superseded is immutable history, so any update of it is refused with nothing written. The same refusal, naming task_graph_supersede_worklink, applies to retiring (archived, released, closed) or rewriting the identity (artifact_type, task_id, repo, branch, worktree_path, thread, session_id) of the sole valid active worktree/binding/branch binding of a task and repository, and to changing or forging a note that begins supersedes=; unchanged values, ordinary metadata, evidence rows and legacy or duplicate rows keep their behavior.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in updateInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleUpdate(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	// ---------------- edges (write) ----------------

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_link",
			Description: `Create or mutate a typed relationship between two nodes by their stable node_ids (canonical_names). In schema_v1 mode this admits edge rows and returns admission_record; in fleet_plan_native mode this mutates TaskLinks/Tasks/TaskDeps and returns native_mutation_receipt. project_id scopes both endpoints; cross-project links fail closed because endpoints must resolve inside the same logical task universe. Before cutover, only omitted/default project_id="arbor" is accepted because the live tables are not project-keyed yet. edge_label is one of: advances (artifact→task), owned_by (task→thread-task), parent_of↔child_of (plan tree), gates↔gated_by (dependency). Validated against config.edge_labels: endpoint kinds against the label's source_kinds/target_kinds, self-loop policy enforced. Reciprocity (parent_of/child_of, gates/gated_by) creates the mirror edge in the SAME Spanner ReadWriteTransaction.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in linkInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleLink(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_unlink",
			Description: `Archive an edge by the exact edge_id returned from task_graph_neighbors. In schema_v1 mode this flips the edge and reciprocal mirror active→archived in one transaction. In fleet_plan_native mode one TaskDeps dep_id is the shared identity of gates↔gated_by; unlink marks that physical row archived so both projections disappear atomically. Idempotent.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in unlinkInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleUnlink(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	// ---------------- reads ----------------

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_get",
			Description: `Fetch one node by project_id + node_id (canonical_name). Omitted project_id resolves to config.default_project_id. Before cutover, only omitted/default project_id="arbor" is accepted. Returns the active version with its full contract + payload.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in getInput) (*mcp.CallToolResult, Node, error) {
			out, err := handleGet(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_list",
			Description: `List nodes inside one project_id. Filter by node_kind ('task' | 'artifact') and/or status. Default status filter is 'active'. Omitted project_id resolves to config.default_project_id. Before cutover, only omitted/default project_id="arbor" is accepted.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in listInput) (*mcp.CallToolResult, NodeListResult, error) {
			out, err := handleList(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_search",
			Description: `Find tasks/artifacts by intent, not exact id. In schema_v1 mode this embeds the query (gemini-embedding-2-preview, 3072d) and runs APPROX_COSINE_DISTANCE over node embeddings. In fleet_plan_native mode this runs a Spanner-side ranked term match over the live Tasks and TaskLinks rows (id/title/pillar/status/owner/gap_label/note and the worklink columns, including session_id), ordered by how many query terms match — the native store has no node table or embeddings. project_id scopes native search; omitted resolves to config.default_project_id.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in searchInput) (*mcp.CallToolResult, NodeListResult, error) {
			out, err := handleSearch(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_neighbors",
			Description: `List edges incident to a project_id + node_id. direction=out|in|both (default both); optionally filter by edge_labels. Before cutover, only omitted/default project_id="arbor" is accepted. This is how you read a task's linked artifacts (advances), its children (parent_of), its owner (owned_by), and its dependency edges (gates/gated_by).`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in neighborsInput) (*mcp.CallToolResult, EdgeListResult, error) {
			out, err := handleNeighbors(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_closure",
			Description: `Forward transitive closure from a node_id, walking edges of the named closure_profile (edge_family literal — dep | ref | impl | struct). Bounded by max_depth + result_cap. E.g. the full subtree under a pillar (struct), or everything a task transitively gates (dep).`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in closureInput) (*mcp.CallToolResult, NodeListResult, error) {
			out, err := handleClosure(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_impact",
			Description: `Reverse transitive closure (impact / blast radius) from a node_id across all edge families. E.g. every task blocked (directly or transitively) by an unfinished blocker.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in impactInput) (*mcp.CallToolResult, NodeListResult, error) {
			out, err := handleImpact(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_query",
			Description: `Ad-hoc read-only GoogleSQL / GQL query over the task graph. Gated by experimental_bootstrap mode with experimental_query_enabled=true. SELECT, GRAPH, MATCH, or WITH queries only.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in queryInput) (*mcp.CallToolResult, QueryResult, error) {
			out, err := handleQuery(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_import",
			Description: `Bulk admission of multiple task/artifact/edge submissions in topological order. Pass submissions inline or as a JSON file path. All-or-none per-substrate — single-admission-path applies to the batch per REQUIREMENTS.md §3.6. This is the migration + reseed surface.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in importInput) (*mcp.CallToolResult, ImportResult, error) {
			out, err := handleImport(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_health",
			Description: `Readiness + schema state + registry sizes (substrates, node_kinds, edge_labels, projections, and the closed task-plan registries task_statuses, dependency_kinds, artifact_kinds) + mode + active snapshot pin. Run at session start to surface immediate blockers. overall_ready is false when a substrate is unreachable or its schema is unapplied, AND when a fleet_plan_native substrate is configured while any of the three closed registries has zero members — that server can accept no write, because every gated write fails closed with unknown task status / unknown dependency kind / unknown artifact kind. A schema_v1 deployment does not consult those registries and stays ready without them; their sizes are reported either way.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in healthInput) (*mcp.CallToolResult, HealthResult, error) {
			out, err := handleHealth(ctx, env, in, pin)
			return textResult(out, err), out, err
		},
	)

	// ---------------- task-domain query/mutation surface ----------------
	//
	// task_graph_list/_neighbors/_closure/_impact/_query above are the
	// GENERIC graph verbs. The six verbs below reproduce the exact task-plan
	// SQL recipe set (MASTER_PLAN.md "Query/Mutation recipes") — the
	// Conductor's + every fleet thread's day-to-day plan-tracking surface —
	// against a task's business fields (status/pillar/owner/is_gap/gap_label)
	// living inside the Nodes.content JSON payload.

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_tasks",
			Description: `List tasks in one project_id, filtered by pillar/status/owner/is_gap/gap_label/level. Omit project_id only to use config.default_project_id. Before cutover, only omitted/default project_id="arbor" is accepted. Reproduces "all tasks by pillar+status" (full listing), "tasks for a given thread/owner", "the UNOWNED gaps" (is_gap=true, owner=UNOWNED), and "tasks blocked/in_progress" (statuses=["blocked","in_progress"]). Omit all filters for every active task in that project.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in tasksInput) (*mcp.CallToolResult, TaskListResult, error) {
			out, err := handleTasks(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_project_snapshot",
			Description: `Return one TRANSACTIONALLY COHERENT, bounded project projection: tasks, dependency edges, active worklinks and grouped counts, all read inside a SINGLE Spanner read-only transaction, so they describe one state and cannot mix revisions. Carries revision (opaque; compare, do not parse), read_timestamp (the single timestamp every collection was read at), content_hashes per collection, per-collection cap state (returned, collected, authorized_total, capped, complete, next_cursor), and scope metadata. The default scope is project-wide. Opt-in scope=execution_subtree with root_task_id returns the structural parent subtree; scope=acceptance_dependency_closure with root_task_id returns the root, its direct children, and their transitive prerequisite dependencies. Scoped membership is discovered server-side inside the same fixed-timestamp transaction; scope.complete=false and scope.missing_task_ids report incomplete or unresolved membership and remain cumulative across dependency pages. When a collection remains capped, continuation is an authenticated opaque token binding the project, scope, root, fixed read timestamp, ordering positions, cumulative scoped missing prerequisites, totals and page limit; supply it unchanged to resume. A zero-row page with a positive authorized_total, malformed or stale cursor, scope drift, or missing continuation key is refused rather than reported complete. generated_at is the wall clock of assembly and is NOT the coherence coordinate. The consumer supplies no SQL. Omitted project_id resolves to config.default_project_id; pre-cutover runtimes accept only that default namespace.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in projectSnapshotInput) (*mcp.CallToolResult, ProjectSnapshotResult, error) {
			out, err := handleProjectSnapshot(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_task_counts",
			Description: `Project-scoped task counts grouped by (pillar, status) — reproduces "all tasks by pillar+status" as counts — plus by_pillar_canonical_status (the same rows folded onto the task_statuses registry members through their absorbs lists, each group carrying its member's config-declared terminal flag; a status literal no member absorbs is counted under canonical_status "unrecognized" and listed in unrecognized_status_literals, never dropped), gap_task_count (the concrete gap tasks, e.g. GAP-A maps to two tasks) and distinct_gap_count (COUNT(DISTINCT gap_label) — the true named-gap count). Before cutover, only omitted/default project_id="arbor" is accepted.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in taskCountsInput) (*mcp.CallToolResult, TaskCountsResult, error) {
			out, err := handleTaskCounts(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_worklinks",
			Description: `List every artifact (pr/circuit/spec/dispatch/finding/commit/binding) advancing a given project_id + task_id, via its 'advances' edges. Before cutover, only omitted/default project_id="arbor" is accepted. Reproduces "all work linked to a given task".`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in worklinksInput) (*mcp.CallToolResult, WorklinkListResult, error) {
			out, err := handleWorklinks(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_add_worklink",
			Description: `Record one work artifact against a project_id + task. In schema_v1 mode this mints an artifact node, links it via an 'advances' edge, and returns admission_record; in fleet_plan_native mode this inserts TaskLinks and returns native_mutation_receipt. Before cutover, only omitted/default project_id="arbor" is accepted. Reproduces "INSERT a new work-link".`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in addWorklinkInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleAddWorklink(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_archive_worklink",
			Description: `Archive one erroneous native TaskLinks worklink by exact project_id + task_id + stable link_id. The task and link must exist in the project and the link must belong to that task; wrong-task and cross-project requests fail closed. A successful first archive sets artifact_status=archived and returns a native_mutation_receipt; re-archiving the same owned link is an idempotent success with rows_affected=0. Archive is for erroneous rows only: it refuses a superseded predecessor (the retry and read-back evidence of a succession) and the sole valid active worktree/binding/branch binding of a task and repository, which only task_graph_supersede_worklink replaces. This operation is available only in fleet_plan_native mode and does not expose arbitrary query or deletion.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in archiveWorklinkInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleArchiveWorklink(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_supersede_worklink",
			Description: `Atomically retire one VALID active binding worklink (typically the merged branch of a task) and install its exact successor under the same task, owner, session and thread, for post-merge work. One Spanner read-write transaction verifies the task owner and session (expected_owner, expected_session_id); the predecessor's exact artifact_type=worktree, artifact_status=recorded, repo, branch, worktree_path, thread and session_id; that the predecessor is the ONLY active binding worklink (worktree, binding or branch) of the task for that repository; and that the successor link_id is unused. It then sets the predecessor artifact_status=superseded (retained as history: still readable through task_graph_worklinks and task_graph_get, absent from task_graph_project_snapshot) and inserts the successor with note "supersedes=<predecessor_link_id>", so no reader can observe zero or two active bindings. Returns a native_mutation_receipt (operation supersede_worklink, rows_affected 2) and a supersede_receipt naming both link ids. An identical retry is an idempotent success with rows_affected 0; any other request against an already superseded predecessor fails closed and names the winning successor, so concurrent contenders converge on one. Distinct from task_graph_archive_worklink, which is only for erroneous rows. Available only in fleet_plan_native mode; it does not expose arbitrary query, update or deletion.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in supersedeWorklinkInput) (*mcp.CallToolResult, WorklinkSupersedeResult, error) {
			out, err := handleSupersedeWorklink(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_set_status",
			Description: `Update a project-scoped task's status, preserving every other payload field (title/pillar/owner/is_gap/gap_label/note) and the immutable contract. Before cutover, only omitted/default project_id="arbor" is accepted. Returns admission_record in schema_v1 mode or native_mutation_receipt in fleet_plan_native mode. Reproduces "UPDATE a task's status". Setting a task to its own current status is always a legal no-op, even when the stored value is a pre-registry legacy literal; a status CHANGE must name a canonical task_statuses member and satisfy the transition graph from the current status's canonical member.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in setStatusInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleSetStatus(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_reown",
			Description: `Re-own a project-scoped task/gap to a different thread (or UNOWNED), preserving every non-binding payload field. A new owned target requires session_id and repo. For an already owned task, also supply expected_owner, expected_session_id, and expected_repo for an owner-authorized atomic compare-and-set handover, including release to UNOWNED. Recovery to UNOWNED requires session_id and repo to be omitted and clears both stored fields. In fleet_plan_native mode, optional writer_handover atomically moves the sole protected worktree writer with the task: supply the exact existing link_id, artifact_type=worktree, artifact_status=recorded, repo, branch, worktree_path, historical thread/session_id, and explicit target_thread; the target writer session is the outer session_id. It preserves the binding id and execution coordinates, records immutable retry evidence, repairs an already-reowned split, and refuses ambiguity or a plain re-own that would leave task and writer identity incoherent. Before cutover, only omitted/default project_id="arbor" is accepted. Returns admission_record in schema_v1 mode or native_mutation_receipt plus binding evidence and, when requested, writer_handover_receipt in fleet_plan_native mode.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in reownInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleReown(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "task_graph_claim",
			Description: `Atomically claim a project-scoped task for the first available worker. The claim succeeds only when owner is empty or UNOWNED; concurrent claimers are resolved first-writer-wins. A retry by the same owner, session_id, and repo is idempotent. Optional binding adds branch/worktree_path/host/account/harness/thread evidence only to the receipt for the transaction that first acquires the claim; because those coordinates are not stored in Tasks, an idempotent retry must omit them and read the registered worklink instead. On collision, the error reports the current owner, session_id, and repo. No expiring lease is created.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in claimInput) (*mcp.CallToolResult, MutationWriteResult, error) {
			out, err := handleClaim(ctx, env, in)
			return textResult(out, err), out, err
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{Name:"task_graph_review_produce", Description:`Run a fresh administrator-pinned independent no-tools reviewer on the exact recorded native leaf/controller/binding and immutable frozen source. Only the service's actual successful process output can atomically create exclusive immutable review origin and publish its bound acceptance. Missing capability, caller findings/commands, stale or ambiguous inputs refuse. This does not execute or attest RED/GREEN tests.`},
		func(ctx context.Context, _ *mcp.CallToolRequest, in reviewInput) (*mcp.CallToolResult, map[string]interface{}, error) {
			out,err := handleReviewProduce(ctx,env,in)
			return textResult(out,err),out,err
		},
	)
	mcp.AddTool(server,
		&mcp.Tool{Name:"task_graph_review_get", Description:`Resolve an exact review_ref from the exclusive native evidence table. Verify current leaf/controller/binding/slot/frozen source and producer generation, actual captured process output and strict result. Generic notes, local receipts, version1 and reviewer labels never establish origin. Wrong scope, stale/replayed/tampered evidence or missing capability refuses.`},
		func(ctx context.Context, _ *mcp.CallToolRequest, in reviewInput) (*mcp.CallToolResult, map[string]interface{}, error) {
			out,err := handleReviewGet(ctx,env,in)
			return textResult(out,err),out,err
		},
	)

	// Streamable HTTP transport per the MCP 2025-11-25 spec. The SDK's
	// StreamableHTTPHandler is an http.Handler; we serve it from a plain
	// net/http.Server bound to cfg.ListenAddress.
	//
	// Stateless=true: spec-graph holds no per-session state (the snapshotPin
	// closure captured above is the only mutable, and it's process-global, not
	// session-scoped). Stateless mode skips Mcp-Session-Id validation and
	// closes the temporary session after each request — simpler, and lets the
	// process be restarted under the client without breaking sessions.
	//
	// DNS-rebinding protection is on by default (any request arriving via a
	// localhost address with a non-localhost Host header is 403'd). The
	// localhost bind (127.0.0.1) is also enforced by net.Listen on the address
	// the config supplies — we recommend 127.0.0.1:<port> in
	// spec-graph.config.json. Operators who deliberately bind 0.0.0.0 should
	// layer their own auth on top.
	mcpHTTPHandler := mcp.NewStreamableHTTPHandler(
		func(_ *http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{
			Stateless: true,
		},
	)
	httpHandler := transportHandler(mcpHTTPHandler, transportSecret)
	srv := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           httpHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful shutdown on context cancellation. ctx is cancelled by main's
	// signal handler on SIGINT / SIGTERM, which lets in-flight tool calls
	// finish before the process exits.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	fmt.Fprintf(os.Stderr, "task-graph: listening on %s (Streamable HTTP MCP)\n", cfg.ListenAddress)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}

func resolveTransportSecret(config *TransportAuthConfig) (string, error) {
	if config == nil {
		return "", nil
	}
	secret := strings.TrimSpace(os.Getenv(config.SecretEnv))
	if secret == "" {
		return "", fmt.Errorf("configured secret environment variable %q is empty", config.SecretEnv)
	}
	return secret, nil
}

func resolveSnapshotContinuation(config *SnapshotContinuationConfig, bearerSecret string) ([]byte, time.Duration, error) {
	if config == nil {
		return nil, 0, nil
	}
	encoded := strings.TrimSpace(os.Getenv(config.KeyEnv))
	if encoded == "" {
		return nil, 0, fmt.Errorf("configured key environment variable %q is empty", config.KeyEnv)
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, 0, fmt.Errorf("configured key environment variable %q is not standard base64: %w", config.KeyEnv, err)
	}
	if len(key) < sha256.Size {
		return nil, 0, fmt.Errorf("configured key environment variable %q decodes to %d bytes; at least %d required", config.KeyEnv, len(key), sha256.Size)
	}
	if bearerSecret != "" && continuationKeyMatchesBearer(key, bearerSecret) {
		return nil, 0, fmt.Errorf("snapshot continuation key must differ from transport bearer secret")
	}
	return key, time.Duration(config.TTLSeconds) * time.Second, nil
}

func continuationKeyMatchesBearer(key []byte, bearerSecret string) bool {
	raw := []byte(bearerSecret)
	if len(key) == len(raw) && subtle.ConstantTimeCompare(key, raw) == 1 {
		return true
	}
	decoded, err := base64.StdEncoding.DecodeString(bearerSecret)
	return err == nil && len(key) == len(decoded) && subtle.ConstantTimeCompare(key, decoded) == 1
}

func transportHandler(mcpHandler http.Handler, bearerSecret string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json; charset=utf-8")
		w.Header().Set("cache-control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"task-graph"}`))
	})
	mux.Handle("/", requireBearer(mcpHandler, bearerSecret))
	return mux
}

func requireBearer(next http.Handler, secret string) http.Handler {
	if secret == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided, hasBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !hasBearer || len(provided) != len(secret) || subtle.ConstantTimeCompare([]byte(provided), []byte(secret)) != 1 {
			w.Header().Set("cache-control", "no-store")
			w.Header().Set("www-authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func textResult(v interface{}, err error) *mcp.CallToolResult {
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
		}
	}
	b, mErr := json.MarshalIndent(v, "", "  ")
	if mErr != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: mErr.Error()}},
		}
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(b)}},
	}
}
