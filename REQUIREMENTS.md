# task-graph — Requirements

The MCP server the Conductor and every fleet thread use to read and write
Velenza BOC fleet plan-to-done state — tasks, artifacts, and the edges between them
— as a queryable Spanner graph, replacing direct `gcloud spanner ... execute-sql`
calls against the `task-plan` database.

## 1. Purpose

task-graph is a retarget of spec-graph's generic node-graph framework (15
verbs, 10-step admission ceremony, closed-registry node_kinds/edge_labels,
one generic `SpannerSubstrateAdapter`, property-graph closure/impact,
snapshot-pinned reads, fail-closed posture) onto the fleet task-tracking
domain. Every architectural property that made spec-graph a sound Node
Identity & Admission implementation carries over unchanged; only the
domain — what a "node" and an "edge" *mean* — changes.

The framework serves two node_kinds:

- **`task`** — one fleet plan-to-done unit of work at any tree level (goal,
  pillar, critical_path, milestone, work_task, gap, or worker_thread),
  addressed by its snake/kebab-case `canonical_name` (e.g. `P0-KERNEL`,
  `THREAD-BACKEND`, `GAP-VOLT-S17-3-ORD9-TIER-FLOOR`).
- **`artifact`** — one work artifact (pr, circuit, spec, dispatch, finding,
  commit, binding) that advanced a task.

And four edge relationships (six `edge_labels` registry entries — two are
reciprocal pairs): `advances` (artifact→task), `owned_by` (task→
thread-task — declared in config, not populated by the migration; see §6),
`parent_of`↔`child_of` (the plan tree), `gates`↔`gated_by` (task
dependencies — declared in config, no source data populated it as of the
initial migration).

## 2. Data model

Every native task-plan task universe is scoped by `project_id`, a logical
project namespace. `project_id` is not the GCP project id (`example-gcp-project`)
and not the tenant `organization_id`; it is the first-class identity of a
non-overlapping task universe. The existing Velenza BOC plan defaults to `arbor`
because the live rows are Velenza BOC fleet work; `fleet` names the bookkeeping
service, not the project. Omitted API `project_id` values resolve to the
configured `default_project_id` for backward-compatible Velenza BOC operation.

Every task/artifact node carries an immutable **contract** (identity) and a
mutable **payload** (business state) — never a plain embedded id string for
a cross-entity relationship; those are always typed edges
(`task_graph_link` / `task_graph_add_worklink`), per
`OOD_GRAPH_PROTOCOL_v1` §3 (edges-carry-meaning).

**task** contract: `project_id`, `canonical_name`, `task_ref` (the latter two equal the task's id,
e.g. `P2-DATA`), `level` (the source `layer` field verbatim — `L0`/`L1`/
`L2`/`P0`/`spec`/`kernel`; not a strict enum in the source data).
**task** payload: `title`, `pillar`, `status`, `owner`, `session_id`, `repo`,
`is_gap` (bool), `gap_label`, `note`, `fleet_created_at`,
`fleet_updated_at`.

**artifact** contract: `project_id`, `canonical_name` (the artifact's own id, e.g.
`LNK-0001`), `artifact_type_kind` (pr/circuit/spec/dispatch/finding/commit/
binding), `artifact_ref` (a PR URL, circuit name, spec section, …).
**artifact** payload: `thread`, `note`, `fleet_created_at`.

The plan tree is `parent_of` edges (parent task → child task); `task_graph_link`
auto-admits the `child_of` mirror in the same transaction (reciprocity is
declared on the edge_label, not hand-coded per call). A task's advancing
artifacts are `advances` edges (artifact → task).

## 3. Substrate

ONE live substrate during this phase (`task_graph_iter1` in
`task-graph.config.json`) — the canonical `task-plan` Spanner database in
instance `example-spanner-instance`, project `example-gcp-project`. The
configured store type is `fleet_plan_native`, so the day-to-day task verbs
operate directly on `Tasks`, `TaskLinks`, and `TaskDeps`.

The checked-in `task-graph.config.json` is the pre-cutover runtime config for
the current live schema. It carries `default_project_id="arbor"` but omits
`project_column`; that is intentional deployment safety. In this mode,
omitted `project_id` and explicit `project_id="arbor"` continue to read/write
the existing un-namespaced live tables, while any non-default project id fails
closed because the schema cannot yet prove project isolation.

The checked-in `task-graph.post-cutover.config.json` is the explicit
post-cutover runtime config. It adds `project_column="project_id"` while
keeping the production table names. Use it only after the migration/cutover
has made those production-named tables project-keyed. The intended
post-cutover live schema is:

- `Tasks` primary key `(project_id, id)`.
- `TaskLinks` primary key `(project_id, link_id)` with `(project_id, task_id)`
  referencing `Tasks`.
- `TaskDeps` primary key `(project_id, dep_id)` with both task endpoints
  referencing `Tasks` inside the same `project_id`.

The sanctioned migration/default story is
`migrations/001_fleet_plan_project_namespace.sql`: create project-keyed
replacement tables, backfill current rows to `project_id='arbor'`, verify
counts, then cut over in a maintenance window. The task-graph binary must not
hand-apply that DDL; operators apply it through the sanctioned Spanner DDL/DML
path. The safe order is:

1. Merge source with the default pre-cutover config.
2. Pull main, rebuild, and restart only if desired; this remains compatible
   with the current live schema and limits default-project access to Velenza BOC.
3. Apply the replacement-table DDL and Velenza BOC backfill DML from the migration.
4. Verify old/new row counts match.
5. Cut the replacement tables over to the production `Tasks`/`TaskLinks`/
   `TaskDeps` names.
6. Switch runtime config to `task-graph.post-cutover.config.json`, rebuild,
   restart, and smoke against `project_id="arbor"` plus a throwaway
   non-default project.

The older dedicated `task-graph` database remains a schema_v1 migrated-copy
mode: `Nodes` (JSON `contract` + `content` columns, one row per node
version, active-version UNIQUE NULL_FILTERED INDEX), `Edges`,
`ArtifactAdmissionLog`, plus `CREATE PROPERTY GRAPH CorpusGraph` for
`GRAPH_TABLE`/`MATCH` traversal. `task-graph init` still refuses any
database not matching `init_database_allowlist` (`["task-graph"]`) so schema
DDL cannot be applied to the live `task-plan` database by accident.

## 4. Verb surface

**Generic graph verbs** (14 — the framework, domain-agnostic):
`task_graph_use`, `task_graph_create`, `task_graph_update`, `task_graph_link`,
`task_graph_unlink`, `task_graph_get`, `task_graph_list`, `task_graph_search`,
`task_graph_neighbors`, `task_graph_closure`, `task_graph_impact`,
`task_graph_query` (ad-hoc read-only SQL/GQL, gated by
`experimental_query_enabled`), `task_graph_import` (bulk topologically-ordered
admission — the migration + reseed surface), `task_graph_health`.

**Task-domain verbs** (10 — `task_queries.go`; in `fleet_plan_native` mode a
task's business fields are the live `Tasks` columns; in schema_v1 migrated
mode they live inside `Nodes.content` JSON):

| Verb | Reproduces |
|---|---|
| `task_graph_tasks` | filter by project_id/pillar/status/owner/is_gap/gap_label/level — covers full listing, by-owner, unowned-gaps, blocked/in_progress |
| `task_graph_project_snapshot` | one bounded project projection containing tasks, dependency edges, active worklinks, grouped counts, and snapshot metadata for dashboards and other read models |
| `task_graph_task_counts` | project-scoped grouped (pillar,status) counts + `by_pillar_canonical_status` (the same rows folded onto `task_statuses` members, terminal flag from config, per-pillar sums equal to the raw counts; literals no member absorbs count under `unrecognized` and are listed in `unrecognized_status_literals`) + `gap_task_count` (concrete gap tasks) + `distinct_gap_count` (`COUNT(DISTINCT gap_label)`, NULL-safe) |
| `task_graph_worklinks` | every artifact advancing a task inside one project (`TaskLinks` in live mode, `advances` edges in migrated mode) |
| `task_graph_add_worklink` | insert one project-scoped live `TaskLinks` row, or mint an artifact node + `advances` edge in migrated mode |
| `task_graph_archive_worklink` | archive one erroneous native `TaskLinks` row by exact project/task/link identity; idempotent re-archive, no generic delete/query surface |
| `task_graph_supersede_worklink` | retire one VALID active `worktree` binding row and install its exact successor for the same task, owner, session, thread and repository in ONE transaction (see §5); it is the valid-lifecycle transition that `task_graph_archive_worklink` deliberately cannot represent |
| `task_graph_set_status` | update a task's status inside one project, every other field preserved |
| `task_graph_reown` | atomically compare-and-set a task's owner/session/repository binding inside one project, preserving every non-binding field; an owned current task requires its exact expected tuple even when releasing to `UNOWNED`, which clears session/repository; the optional native-only `writer_handover` moves the sole existing protected worktree writer in the same transaction |
| `task_graph_claim` | atomically claim an empty/`UNOWNED` task for one owner/session/repository tuple; concurrent claimers resolve first-writer-wins, exact durable-tuple retries are idempotent, and only the acquiring transaction may attest optional execution coordinates |

Total: **24 verbs**.

`task_graph_project_snapshot` is the reusable application read boundary. It
accepts a logical `project_id`, a bounded row limit, and an optional typed
scope. The default `project` scope preserves the project-wide projection;
`execution_subtree` requires `root_task_id` and follows `Tasks.parent_id`;
`acceptance_dependency_closure` requires `root_task_id`, includes that root
and its direct structural children, then follows active `TaskDeps` from each
child to its prerequisites. Scoped membership discovery and the final task,
dependency, worklink, count, and total reads all run inside one read-only
transaction. The response identifies the scope and reports membership
completeness, cap state, and unresolved task references. Consumers never
submit SQL and do not need one query per task to recover dependencies or work
evidence. When any returned collection is capped, the response carries an
opaque HMAC-authenticated continuation binding the project, scope, root task,
page limit, authorized totals, strict ordering tuples, cumulative scoped
missing-prerequisite IDs, expiry, and original Spanner read timestamp.
Resumption re-discovers scoped membership and reads all collections at that
exact timestamp; scoped completeness is monotonic across dependency pages.
Tampering, tenant/project drift, scope or
root drift, changed page size, changed totals, expiry, missing progress, and a
reader that cannot honor the timestamp all fail closed. The continuation key
is resolved from the dedicated `snapshot_continuation.key_env` runtime secret;
the transport bearer credential cannot be reused.

## 5. Admission ceremony

For schema_v1 graph mode, unchanged from spec-graph: the 10-step ceremony (`ceremony.go`) —
schema_validate → resolve_admitter → pre_sign_passes → definition_hash →
manifest_hash → kms_sign (placeholder in `experimental_bootstrap` mode) →
signature_chain_append → post_sign_passes → typed_row_insert →
admission_log_insert. `task_graph_import` prepares every submission's plan
in memory first (`PrepareAdmissionPlan`); any prepare failure aborts the
whole batch before any substrate write (all-or-none); successfully prepared
plans commit together in ONE Spanner ReadWriteTransaction via the variadic
`CommitAdmission` — see `TestIntegrationImportAtomicAllOrNone` /
`TestIntegrationImportHappyPathBatch` in `integration_test.go`.

For `fleet_plan_native` mode, conductor bookkeeping verbs are the typed
access layer over the live operational tables. They do not create a second
admission copy of task state; `task_graph_set_status`, `task_graph_reown`,
`task_graph_add_worklink`, `task_graph_archive_worklink`,
`task_graph_supersede_worklink`, `task_graph_create`,
`task_graph_update`, `task_graph_link`, and `task_graph_claim` mutate `Tasks` /
`TaskLinks` / `TaskDeps` through the server's Spanner client. Every native mutation resolves exactly one
`project_id` first and predicates all endpoint reads/writes on that project;
cross-project links/deps fail closed because one endpoint is not found in the
chosen project. Worklink archive additionally requires the named task to exist
in that project and the named link to belong to that exact task. It sets
`artifact_status='archived'`, returns a `native_mutation_receipt`, and returns
a successful zero-row receipt when the same owned link is already archived.
Unknown links, wrong-task links, and cross-project links are denied. Archive is
for erroneous rows: it refuses, in the transaction that would write, a
superseded predecessor and the sole valid active binding (defined below), so it
is never a shortcut for replacing a binding.

Worklink supersede is the valid-lifecycle counterpart of archive. A merged
branch's binding is not an erroneous row, so it is never archived to make room
for post-merge work; the successor for the same task, owner, session, thread and
repository is installed by `task_graph_supersede_worklink`:

- **Inputs are a compare-and-set request, never authority.** The request names
  `project_id`, `task_id`, `expected_owner`, `expected_session_id`, the exact
  `predecessor_link_id`, the predecessor's exact `artifact_type` (`worktree`),
  `artifact_status` (`recorded`), `repo`, `branch`, `worktree_path`, `thread` and
  `session_id`, and the exact successor (`link_id`, `worktree` type, ref, repo,
  branch, worktree path, thread, session). The successor must keep the
  predecessor's repository, thread and session and must differ in branch or
  worktree. The project is resolved server-side and every read and write is
  predicated on it.
- **One read-write transaction** verifies the task exists in the project and
  carries the expected owner and session; that the predecessor exists, belongs to
  the task, is active and matches every expected field; that it is the ONLY
  active binding row (`worktree`, `binding` or `branch`, status not archived,
  superseded, released or closed) of the task for that repository; and that the
  successor link id is unused. It then sets the predecessor
  `artifact_status='superseded'` through a status compare-and-set and inserts the
  successor with `note='supersedes=<predecessor_link_id>'`. No reader observes
  zero or two active bindings, and a failure after either write rolls both back.
- **The predecessor is retained as history.** It keeps every coordinate, stays
  listed by `task_graph_worklinks`, is readable through `task_graph_get` with
  node status `superseded`, and is excluded from `task_graph_project_snapshot`,
  whose worklinks are the ACTIVE rows (a superseded row is not counted by an
  active-binding consumer any more than an archived one).
- **Receipts.** The result carries a `native_mutation_receipt` (operation
  `supersede_worklink`, artifact id the successor, `rows_affected` 2) and a
  `supersede_receipt` naming both link ids with `idempotent`. An identical retry
  that finds the completed transition succeeds with `rows_affected` 0. Any other
  request against an already superseded predecessor fails closed with a
  supersede conflict naming the winning successor, so concurrent identical
  callers converge on one successor and concurrent conflicting callers leave at
  most one winner without losing predecessor history.
- **It is the only way to retire or replace a valid binding.** A *valid active
  binding* is an active `worktree`, `binding` or `branch` row that carries
  branch, worktree, thread and session; the *sole* valid active binding is the
  only such row of its task and repository, exactly the row this verb replaces.
  Neither `task_graph_update` (nor `task_graph_link advances`) nor the
  erroneous-row archive may retire it (`archived`, `released`, `closed` or
  `superseded`, in any case or padding) or rewrite its identity (artifact type,
  task, repository, branch, worktree, thread, session), and a value equal to the
  stored one is the get-to-update echo and is allowed. `task_graph_update` also
  refuses `artifact_status=superseded` on any row, every update of a worklink
  that is already superseded (immutable history: it must not be revived,
  archived, re-linked or rewritten), and any change to a note beginning
  `supersedes=` or a new note claiming that prefix, because the linkage is the
  retry and read-back evidence. Every refusal is decided in the same transaction
  that would write, names this verb, is serialized against a concurrent
  supersede, and writes nothing, including any other supplied field. Ordinary
  metadata (note, url, pr_state, commit, PR number), erroneous evidence rows,
  legacy rows without thread or session, and a duplicate beside another valid
  binding keep the pre-existing generic update and archive behavior.
- The operation exists only in `fleet_plan_native` mode and adds no generic
  update, delete or query surface. It does not change the archive verb, which
  remains limited to erroneous rows.

Task re-own can additionally move the writer identity of the existing protected
worktree without changing its binding identity or execution coordinates:

- **The optional `writer_handover` is an exact second compare-and-set.** It
  names the binding link id, `worktree` type, `recorded` status, repository,
  branch, worktree path, historical thread and historical session, plus an
  explicit target thread. The task's target session is the writer's target
  session; the task owner is a worker identity and is never inferred to be the
  target thread. The extension is accepted only in `fleet_plan_native` mode.
- **One read-write transaction owns both rows.** It verifies the current task
  tuple, the independently supplied historical binding tuple, and that the
  selected row is the only valid active binding for the task and repository.
  Repository aliases are grouped with the installed work-identity consumer's
  normalization (trimmed slash form, optional `.git`, final path/colon segment,
  case-insensitive), so a URL-form competitor cannot survive beside a short
  repository name. The selected row's supplied and stored repository bytes
  still match exactly and no coordinate is rewritten.
  It updates `Tasks.owner/session_id/repo` and only the selected binding's
  `thread/session_id`, preserving its link id, task, artifact reference,
  repository, branch, worktree path and ordinary metadata. A plain re-own that
  would leave a protected binding incoherent is refused. An already-reowned
  task with the exact target tuple may repair a still-historical binding.
- **Immutable request evidence makes response-loss retry exact.** The same
  transaction inserts a deterministic `finding` row whose note begins
  `writer_handover_history=` and records the exact before/after request. An
  identical retry requires both that exact evidence and the coherent target
  rows, then returns `rows_affected=0` and `idempotent=true`. A different
  request, an evidence-id collision, an ambiguous or malformed binding set, or
  a concurrent loser fails closed without a mixed task/writer tuple.
  `task_graph_add_worklink`, generic artifact create/import, update, link and
  archive refuse to mint, change, relink or retire the reserved evidence rows
  or prefix. Existing `superseded` history retains its distinct branch/worktree
  succession meaning.
- **Receipts identify the durable transition.** The result includes the native
  mutation receipt and `writer_handover_receipt`, naming the stable binding and
  history link ids, old and new writer coordinates, row count and idempotency.

Native dependency identity is one `TaskDeps.dep_id` for the normalized
`(project_id, gates_source_id, gates_target_id, gates)` relationship. Both
`gates` and `gated_by` neighbor projections return that exact `dep_id` and
name it as their reciprocal identity. Link is a transactional find-or-create,
so repeated creation through either reciprocal label is idempotent. Unlink
accepts the exact neighbor-returned `dep_id` and marks the one physical row
`kind='archived'` in a single transaction; both reciprocal projections then
disappear together, and re-unlink is an idempotent zero-row mutation.

## 6. Migration history

The fleet's plan-to-done tracker lives in the `task-plan` Spanner database
(`Tasks`, `TaskLinks`, `TaskDeps` — see the frozen
`/opt/velenza/MASTER_PLAN.md` for historical recipes only). A
previous iteration migrated a point-in-time copy into the schema_v1
node/edge model via `task_graph_import`, pulling the live `task-plan` rows
at migration time (not the frozen MASTER_PLAN.md snapshot, which had
already gone stale relative to the live table by the time of migration).
That initial migration admitted 55 task nodes, 30 artifact nodes, 53
`parent_of` edges (+ 53 auto-admitted `child_of` mirrors), and 30 `advances`
edges. Every read recipe from `MASTER_PLAN.md`'s "Query/Mutation recipes"
section was verified to reproduce byte-for-byte against the live
`task-plan` SQL at migration time (see `TESTING.md` §Parity), but that
database is no longer the conductor bookkeeping target.

`owned_by` remains a declared graph relationship for schema_v1 mode; live
mode keeps `owner` as the canonical `Tasks.owner` string. `gates`/
`gated_by` map to live `TaskDeps` rows in native mode.

## 7. Testing discipline

Same three tiers as spec-graph (`make test` / `make test-integration`) —
see `TESTING.md`. `t.Skip` is forbidden per CLAUDE.md
`feedback_no_t_skip_in_tests`.

## 8. Out of scope

- **Not a migration tool for future cutovers.** `task_graph_import` remains
  the generic schema_v1 bulk-admission verb. Live conductor bookkeeping
  happens through the native task-domain verbs instead.
- **Not a kernel substitute.** No circuits, no signal dispatch, no real KMS
  signing — `experimental_bootstrap` mode's placeholder signature is
  explicitly not a production cryptographic guarantee.
- **Not the plan's source of truth for humans.** The graph is; render it via
  `task_graph_tasks` / `task_graph_task_counts` / `task_graph_worklinks`,
  never re-paraphrase plan state into a file or memory (per CLAUDE.md
  `⛔ PLAN/TASKS/PROGRESS LIVE IN THE task-plan SPANNER GRAPH`).
