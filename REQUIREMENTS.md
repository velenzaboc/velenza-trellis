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

**Task-domain verbs** (14 — `task_queries.go` and the bounded native review
and execution producers; in `fleet_plan_native` mode a
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
| `task_graph_review_produce` | execute one server-controlled independent no-tools review of exact frozen native task/slot/source inputs, then atomically insert exclusive immutable origin evidence and publish its bound acceptance reference; native mode only |
| `task_graph_review_get` | resolve that exclusive origin by exact project/review/task/slot/input identity, validate its strict kind and current frozen generation, and refuse legacy notes, stale inputs or unavailable producer capability; native mode only |
| `task_graph_tdd_produce` | record a bounded native execution lineage action: start against existing owned context, actual RED/GREEN through the pinned native-Go executor, or independently audited reopen; atomic exclusive evidence and existing Task/slot publication, never generic runner authority |
| `task_graph_tdd_get` | resolve exact immutable execution/lineage evidence against current task/binding/slot/source generation, including explicitly retained unchanged-production proof after an audited fixture-only reopen; native mode only |

Total: **28 verbs**.

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
`TaskLinks` / `TaskDeps` through the server's Spanner client. Each of these bookkeeping mutations resolves exactly one
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

### Independent review origin in the existing native service

`task_graph_review_produce` and `task_graph_review_get` extend the existing
configured native service with one exclusive immutable evidence table. They
own software review origin. Circuit validation keeps its existing hash-based
runtime handoff below; neither operation is a circuit release prerequisite.
They do not create another task/queue ledger, signer, identity framework, workflow
service or scheduler. The table is additional evidence, not a second admission
copy of `Tasks`, `TaskLinks` or `TaskDeps`. The writer-handover and supersede
contracts above, including their historical identifiers and note meanings,
remain unchanged. Neither operation invokes reown or stores review under
`writer_handover_history=`.

**Typed wire identity.** Both operations accept exactly these JSON fields:

| Field | Meaning |
|---|---|
| `project_id` | logical native task project, resolved by the existing adapter |
| `task_id` | actual leaf Task whose frozen work is reviewed and whose validation approval is recorded |
| `execution_task_id` | optional equality assertion for the existing controller; it must equal `EXECUTION-QUEUE-<binding.session_id>` |
| `slot_id` | optional equality assertion only when the selected existing slot already records that ID; never required or invented |
| `slot_key` | optional equality assertion of the deterministic server-derived slot identity defined below |
| `binding_link_id` | exact current canonical native worktree binding |
| `kind` | `validate_tests` or `production`, with no substitution between them |
| `revision` | `slot.tests.revision` for `validate_tests`, or actual frozen `slot.revision` for `production`; verify immutable source |
| `base_revision` | required only for `production`; exact recorded `slot.base_revision` and verified production comparison base |
| `epoch`, `context_sha256` | exact project-queue generation of that slot |
| `review_id` | project-scoped request/retry identity, not caller authority |

Typed get may also receive `execution_project_id`, `input_sha256` and
`producer_generation` from a returned reference as equality assertions. They
must equal actual server-resolved state; they never select origin or producer
authority. A production response/reference includes its `base_revision`;
validation responses do not invent a production base.

Unknown fields, including commands, executable/argument/environment overrides,
findings files, supplied findings, reviewer labels and `verified`, are refused.
The actual layout is TWO distinct Tasks. The leaf `task_id` owns its canonical
worklink and `note.validation_acceptance`. The existing controller Task
`EXECUTION-QUEUE-<binding.session_id>` owns `note.queue_assignments` (array) or
historical `note.queue_assignment` (singleton). Never rehome those slots onto
the leaf. Resolve the leaf in its exact requested project and the controller in
the configured default project, matching the existing gate's omitted-project
controller lookup. These two review operations explicitly bind both scoped row
identities; existing single-project bookkeeping handlers and cross-project edge
refusals remain unchanged. The controller payload session must equal the actual
binding session. Caller-selected controller namespaces or different explicit
controller IDs refuse.

Select exactly one active real slot by leaf project/task, normalized repository
and exact recorded physical worktree coordinates of the canonical leaf binding;
verify its project-queue scope and frozen source generation. Missing, duplicate,
ambiguous or contradictory slots/bindings refuse. No mandatory `slot_id` exists
in current slots. Preserve an existing explicit ID and validate it if asserted.
The service derives `slot_key` without writing new assignment state: SHA-256 of
the UTF-8 length-prefixed scalar tuple `velenza-review-slot/1`, leaf project,
leaf task, controller project, controller task, binding link ID, normalized
repository, exact recorded worktree, binding branch, thread and session. Frame
each scalar as its decimal UTF-8 byte length, `:`, then its exact UTF-8 bytes,
with no separator or padding between framed scalars. This is a locator/digest,
not origin proof. Queue stage is not a new slot identity; current stage/action
authorization stays with the existing queue gate. Shared child sessions remain
usable and never authenticate an individual reviewer or prove independence.

The administrator's source checkout can live on another host/path from the
recorded worktree. Keep immutable repository acquisition separate from exact
registered binding/slot coordinates; do not stat a remote path as if it were a
local source checkout or substitute the administrator's path into the binding.

The response to either operation is a JSON object with `schema_version` equal
to `velenza_review_result/1`, the exact request identity fields above,
`input_sha256`, `producer_generation`, `result` (the actual strict child result),
`review_ref` and `idempotent`. `review_ref` is the structured identity
`{project_id,review_id,task_id,execution_project_id,execution_task_id,slot_key,
binding_link_id,kind,revision,epoch,context_sha256,input_sha256,producer_generation}`
plus production `base_revision` and any genuinely existing `slot_id`. It is resolved through the
typed get operation, never treated as a signature or accepted inline.
`idempotent=true` identifies an exact completed retry/readback. Generic node
version1, `admitted_by`, ordinary mutation receipts, timestamps and local paths
do not authenticate this result.

**Administrator configuration and source.** The native substrate descriptor
adds `review_evidence_table`, explicitly naming the dedicated table. The
server-only `review_producer` configuration contains `generation`, absolute
`executable`, `executable_sha256`, fixed `args`, positive bounded
`timeout_milliseconds` and `max_output_bytes`, and `repositories` entries
`{repo,path,requirements_path}`. Repository roots and the pinned executable are
administrator-controlled, not request-derived. This uses existing digests for
byte identity and introduces no signing claim or new cryptographic choice.
Validate the complete configured descriptor and producer before use. An
unconfigured producer/table may leave ordinary bookkeeping available, but both
review operations and affected protected progression refuse. A configured
missing table or unavailable producer never falls back to notes or local files.

Acquire files through verified immutable source, including committed Git
objects rather than dirty working-tree bytes. Executable `slot.tests.files`
contains only the frozen executable test files: every such file must have named
coverage. Bind governing-owner bytes and hashes separately through
`slot.governing_files` and the configured requirements path; an owner document
is not an uncovered pretend test. Resolve any Inarch revision only
through its existing authoritative verified-source path; an unavailable or
unverified provider refuses. Check every selected test/owner hash and legal
relative path/type against the frozen revision. Absolute/parent escape paths,
missing source, mismatched hashes and caller revision labels are not accepted
as source evidence. Supply the full governing requirement and assertion/source
bundle as data; mutable notes alone do not establish source authority.

For production, require the actual frozen code head `slot.revision`, comparison
base `slot.base_revision`, and complete declared `slot.production_files`
path/hash scope. Verify their immutable relationship and acquire the FULL
committed base-to-head diff and all committed changed-file bodies/hashes. Compare
the complete actual diff path set with declared scope; no changed file may be
silently omitted. Retain the earlier frozen test revision/files and acquire the
full governing owner separately. Missing/wrong head/base, undeclared or incomplete
scope, missing bodies and hash mismatches refuse. A test-only commit or kind
label is not production review. Dirty local bytes never replace committed input.

**Real producer boundary.** Launch a fresh independent no-tools reviewer
process using the administrator's pinned executable/configuration and fixed
argv, directly, without a shell. The caller selects no executable, flags or
environment. Reuse the sanctioned independent review invocation with tools
disabled. A shared MCP bearer authorizes requests; it does not prove a distinct
human/agent identity. Capture the actual child stdin/stdout/stderr/exit through
service-owned resources. Never reread a caller-writable receipt or accept the
local helper's record mode as execution. Missing launch, timeout, nonzero exit,
truncation, oversize, malformed/duplicate/ambiguous JSON or incomplete coverage
refuses publication. Inputs are untrusted data, not reviewer instructions.

The child's stdin JSON bundle contains `kind`, `input_sha256`, both exact Task
identities and binding/slot identity, `test_files` (committed executable paths,
hashes and contents), and separate `governing_files` (full owner paths, hashes
and contents). For production it additionally contains
`production={base_revision,revision,diff,files:[{path,sha256,content}]}` holding
the complete committed production input. Governing hashes, frozen tests and
the complete production input all contribute to `input_sha256`. Precisely,
remove only `input_sha256` from this flat stdin object, serialize it with Go
`encoding/json.Marshal` (sorted object keys, default JSON string/HTML escaping,
no whitespace/newline), and hash those UTF-8 bytes with SHA-256. The controlled
test child recomputes this digest before emitting any successful result, so
omitting owner or production bytes from the digest cannot pass a positive.
The stdin projection contains frozen inputs only, not the retry `review_id`,
raw mutable Task notes, prior acceptance output or unrelated metadata.
Its stdout is one strict object with `schema_version` equal to
`velenza_independent_review/1`, matching `kind` and `input_sha256`, complete
`requirements`, `coverage`, `blocking`, `production_boundary`, `tautologies`,
`fixture_supplied_authority` and `summary`. `blocking` is an array; acceptance
requires it empty, `production_boundary=true`, `tautologies=false` and
`fixture_supplied_authority=false`. Coverage maps each requirement to named
`path`, `case`, `assertion`, actual `boundary` and `scenario` (`positive`,
`refusal` or `exception`); explain inapplicable scenarios in the review rather
than silently omit requirements. Compare this mapping with the real committed
assertions and governing requirement scope. A zero count or different reviewer
string alone is insufficient. The server records what actually ran and returned;
this establishes process origin, not infallibility of a model's judgment.
Production output also names matching `production_base_revision`,
`production_revision` and complete `reviewed_files=[{path,sha256}]`; incomplete
production coverage or a different head/base refuses even if test coverage is
complete and the zero-blocker label is well formed.

**Exclusive immutable storage.** Key review evidence by exact
`(project_id,review_id)`. The candidate physical shape is `project_id`,
`review_id`, `task_id`, `execution_project_id`, `execution_task_id`, `slot_key`,
`kind`, `input_sha256`, `binding_json`,
`result_json`, `producer_generation` and `created_at`; `binding_json` contains
the exact bound request, frozen source and protected generation, while
`result_json` preserves the actual strict result. Table identifiers remain in
the validated substrate descriptor. The candidate test DDL provisions this
shape only inside isolated tests. Actual DDL belongs to the sanctioned schema
deployment path, never MCP query, import or a hand-applied live SQL command.
Only successful service-controlled producer completion inserts origin rows.
Generic create/import/update/link/archive/query cannot mint or change them.
Never backfill authority from valid-looking legacy Task/TaskLinks notes.

**One publication transaction.** Resolve coherent inputs before review; run
the external process OUTSIDE a retried database transaction. Then one short
native transaction rereads and compares the exact leaf AND controller rows,
selected slot, canonical binding and frozen/protected input generation. Insert
immutable origin evidence, publish the actual server-produced LEAF
`note.validation_acceptance` with `review_ref`, and publish the CONTROLLER's
selected slot `tests.acceptance={task_id,sha256,review_ref}` atomically for
`validate_tests`. That acceptance `task_id` is the separate leaf identity;
preserve the existing validation
receipt schema/consumer hash convention for compatibility, while origin always
requires typed resolution. For `production`, publish the controller's selected slot
`acceptance` with its `revision`, `blocking=0` and exact `review_ref`. Local
evidence paths and hashes remain historical data, never origin substitutes.
Preserve unrelated metadata in BOTH Tasks and all disjoint slots. The test fault
seams `after_evidence_insert`, `after_leaf_publication` and
`after_execution_publication` must occur after the named actual writes;
failure at any seam rolls back origin, leaf approval and controller acceptance.
An exact completed retry
reads the same result without rerunning the reviewer; changed input, conflicting
identity/collision, stale generation or ambiguous state refuses without overwrite.
Concurrent identical requests converge on one accepted origin; different requests
cannot win the same identity. Transaction retries never rerun an external review.

**Protect actual software mutation boundaries.** Generic Task create/import/update and
every note-writing transaction refuse caller manufacture, alteration or deletion
of software leaf root `validation_acceptance`; software controller slot `tests.acceptance`, `acceptance`,
`tdd_red_receipt`, `tests.tdd_audit_receipt`, `tdd_green_receipt` and
`reopen_history`. Apply the same rule to array and legacy singleton slots at
their exact structural paths, including an array-valued `reopen_history`. A
changed surrounding note cannot erase the
protection. Unchanged protected get/update echo and ordinary metadata/project
queue bookkeeping remain allowed; arbitrary prose containing "review" does
not become protected authority. Typed producer publication is the narrow owner
of software review acceptance. Existing held TDD labels/files are historical data and do
not become valid server origin; this service does not implement the TDD helper.
Refuse generic protected-field writes with `REVIEW_ORIGIN_REQUIRED` before
generic admission/backend or shape failures can masquerade as an origin guard.
Correctly shaped caller-authored, cross-bound and replayed approvals/references
remain untrusted. A path/hash to a plausible local receipt does not prove origin.

**Preserve the existing circuit bookkeeping boundary.** Higher native
WORKFLOW `project_queues.validation_evidence` and `runtime_assignments` also
use `validation_acceptance` and `tests.acceptance` for circuits. Their existing
file-hash handoff must remain recordable without a Git revision, CI, PR,
software merge, configured software reviewer or software evidence table.
Those fields are workflow metadata, never software review origin, executed
runtime validation, signatures, admission or post-admission proof.

The narrow generic preservation door is an existing native Task update, checked
in the actual mutation transaction. Plain Task/slot creation stays available;
create/import cannot grant an exemption from a caller-supplied kind or note.
Resolve the actual leaf in the exact project, its sole coherent canonical
worktree binding, and the configured-default-project
`EXECUTION-QUEUE-<binding.session_id>` controller with matching recorded session.
There must be exactly one prior registered active slot for that physical
worktree/repository; match its leaf/project/binding coordinates, including any
existing slot ID. Resolve the actual delivery Task through its recorded
`project_queue` scope and validate its full circuit plan: schema, exact project
and delivery ID, epoch/context, all five distinct circuit queue owners, and
`admission.ceremony=node.save` plus a nonempty post-admission check. Missing,
contradictory, duplicate, cross-project, software or caller-labelled-only context
refuses. A proposed note cannot mint its own prior assignment context.

Only the circuit leaf's `validation_acceptance` and that circuit slot's
`tests.acceptance` use this door. The validation review is recorded while the
registered slot is `review-validation`; its normal transition to `author-circuit`
may publish the hash reference in the same controller update. Validate the
existing shared receipt schema, project/leaf/context, nonempty distinct
author/reviewer, zero blockers, complete files/requirements/coverage/evidence,
and the three boundary flags against the registered hash-only test handoff.
`tests.acceptance={task_id,sha256}` must resolve that same leaf's current
validation receipt and its exact JSON.stringify hash. Changed file hashes need
renewed review. A hash mismatch, other leaf/project/context/files or replay
from another path refuses. The controller transition preserves disjoint slots
and unrelated metadata. No Git test revision is required or inferred.

Any changed before/after software `review_ref` remains protected, even if a
caller relabels a plan or slot as circuit. An unchanged software slot may coexist
with a legitimate disjoint circuit slot; its origin/reference is preserved.
Circuit receipts cannot carry software `review_ref`, overwrite software
publication, create exclusive origin rows, substitute candidate acceptance for
software review, or satisfy software progression. Software consumers always
resolve exact origin through `task_graph_review_get`; labels and legacy notes
never grant that capability. Schema-v1/generic imports cannot manufacture this
native context. Ordinary metadata and unchanged protected echo keep working.

Existing circuit `candidate_files`/`candidate_acceptance`/`validation_evidence`
bookkeeping remains usable for `validate-circuit` and `admit-circuit` under the
native hash-based runtime contract. The owner oracle/validator, lawful names,
stable references, required signatures, `node.save`, protected typed rows,
ArtifactAdmissionLog and post-admission verification remain mandatory. This
preservation door introduces no circuit release service or software gate.

A fixture-only reopened test generation can retain separately verified prior
production proof when its actual production bytes remain unchanged and its
test-only audit/reopen lineage is proven by the legitimate typed execution/
lineage producer. Do not impose a blanket new production review after every
reopen. That producer and helper adapter are a separate required dependency
before combined activation; this unit neither fabricates it nor treats review
origin as executed RED/GREEN evidence.

**Typed reads and compatibility.** Typed get reads the configured exclusive
table directly and validates the strict stored schema, producer capability,
kind, complete binding and actual current recorded frozen source. Wrong project,
leaf/controller project or Task, controller/binding session, selected slot,
binding, revision/base, epoch, context or kind; tampered/incomplete output;
stale/replayed generation; and missing producer/table capability refuse. Generic
node-ID collisions and ordinary worklinks never mask or replace exclusive
evidence. Software tools/readers or a rollback that lacks this capability must fail
closed at software consuming hooks; no legacy-note bootstrap trust is permitted. An old
generic writer cannot mint rows in the new exclusive table. Ordinary tasks and
existing writer/succession history keep their semantics and usability.

### Native execution and reopen origin (candidate relay)

`task_graph_tdd_produce/get` complete the separate execution dependency above;
they do not make independent review equivalent to executed tests. They extend
this existing native service, not the kernel, an authorization ledger, signer,
identity service or scheduler. The trusted installed helper is a thin client;
its own receipt, command, reviewer label or verification flag never proves
execution. Actual native executor qualification and the helper adapter remain
required before combined activation. The accepted native-Go provider currently
has only allocation/time/exit/completion receipts; until it implements the full
source/argv/process contract below, it is unsupported and MUST refuse.

**Wire and current context.** Produce accepts only `project_id`, `task_id`,
`binding_link_id`, optional equality assertions `execution_task_id`/`slot_id`,
`epoch`, `context_sha256`, `action` (`start`, `red`, `green`, `reopen`),
`request_id`, `revision`, and, except start, `generation_id`, `expected_queue`
and `previous_ref`. Reopen additionally requires nonempty `reason` (untrusted
intent, not classification). Unknown command/argv/executable/environment,
receipt/file/result, `verified`, fixture-kind, approval or reviewer fields
refuse. A request selects intent and exact immutable source, never authority.
Both operations are native-only. Resolve the existing claimed leaf, its sole
current canonical worktree binding and coherent owner/session/repository,
default-project session controller, and complete real software project queue
plan/context. Do not infer an individual identity from a shared bearer/session.

Start requires a verified committed base `revision` and a valid pinned executor
capability handshake. It may have no slot. It inserts start lineage and
publishes only leaf `note.tdd_execution_ref`; it creates no Task, owner claim,
binding or assignment, overwrites no slot, and grants no generic runner access.
If a slot exists it must be unique and coherent; a contradictory or capped
lookup refuses. Existing coordinator bookkeeping may subsequently register an
ordinary build-tests slot. Every later action requires exactly one current
active slot matching leaf project/task, normalized repository and exact
physical binding worktree (including any genuinely existing slot ID). Its
actual queue, project scope, source and generation must match the request.
The same binding-derived slot key as review origin is used, including start
when it is a locator without an assignment. No new caller-created slot ID.

Response schema `velenza_tdd_result/1` contains `action`, `idempotent`, actual
`result`, and `tdd_ref`. A reference has `project_id`, `evidence_id` (the
request ID), `task_id`, `execution_project_id`, `execution_task_id`, `slot_key`,
`binding_link_id`, `action`, server-derived `generation_id`, optional
`parent_evidence_id`, `revision`, `test_revision`, `production_revision`,
`base_revision`, `epoch`, `context_sha256`, `input_sha256`,
`producer_generation`, and only a genuinely existing optional `slot_id`.
Get accepts exactly a returned reference as equality assertions. Missing,
wrong-project/task/binding/controller/session/slot/stage/generation/source,
stale/replayed or tampered reference refuses. Digests bind bytes, not identity
or signatures. A missing producer/table/provider or unsupported protocol refuses
protected progression while ordinary reads/documents remain available.

**Exclusive evidence and publication.** The descriptor adds a distinct
`tdd_evidence_table`, exclusive from ordinary and review tables. Candidate
columns are `project_id,evidence_id,task_id,execution_project_id,
execution_task_id,slot_key,action,generation_id,parent_evidence_id,input_sha256,
binding_json,result_json,producer_generation,created_at`; primary key is exact
`(project_id,evidence_id)`. Binding includes exact request/context, immutable
source materialization, actual captured executor/audit process and current
generation. No generic route writes this table; no historical label backfill.
This is immutable origin/lineage evidence; current task/queue state remains in
the existing Tasks/TaskLinks, not a parallel task-state ledger. DDL stays on the
sanctioned schema path; tests bootstrap only isolated substrates.

Execute the native process or independent reopen audit OUTSIDE any retried
database transaction. Then one short native transaction rereads exact current
leaf/controller/binding/slot, frozen source, current lineage and protected
references; compares the expected generation; inserts immutable evidence;
publishes leaf `tdd_execution_ref`; and publishes the selected slot's existing
flat RED/audit/GREEN/reopen projections atomically. Preserve both Tasks'
unrelated metadata and every disjoint slot. Reuse existing nil-only
`reviewProducerFault` with stages `tdd_after_evidence_insert`,
`tdd_after_leaf_publication`, `tdd_after_execution_publication`, after actual
writes. Only the real-emulator tier proves rollback at all three seams.
Start has no assignment write. Identical completed retry reads the same result
without executing again; changed request/collision/stale generation refuses.
Concurrent exact retries converge on one origin; transaction retries never
rerun external work. Generic create/import/update and note writers refuse
manufacture/change/deletion of `tdd_execution_ref` and all existing protected
slot fields, including whole-note/slot deletion and array/singleton forms.
Unchanged protected echoes, ordinary metadata and circuit handoffs remain usable.

**Administrator executor and exact source.** `tdd_executor` uses the existing
pinned-process pattern: `generation`, absolute `executable`,
`executable_sha256`, fixed `args`, bounded `timeout_milliseconds` and
`max_output_bytes`, administrator-selected absolute `go_path`, and fixed
`packages` (initial native-Go capability `['./...']`). Verified repository
acquisition reuses `review_producer.repositories`; source/owner lookup must not
depend on a dirty checkout or caller file. Git objects must be immutable and
verified; Inarch requires its authoritative verified provider or refuses.
Acquire the entire regular-file source tree (including go.mod, dependencies'
lock inputs, embedded files and required tests/owner), reject symlink/type/path
escapes, and exclude `.git`. The service constructs immutable materialization:
committed production/base layer plus exact committed locked test generation.
A synthetic Git repository created after transfer cannot replace original
revision identity. Every path/body/hash and the layer revision identities bind
the source digest. No arbitrary shell, executable, argv, package pattern or
caller environment; actual provider goPath remains root-owned and pinned.
The process receives a fixed administrator environment, not caller secrets.

Executor stdin is one strict `velenza_tdd_executor_request/1` object:
`operation` (`capabilities` for start, otherwise `go_test_json`), `action`,
`identity` (the resolved frozen scope/lineage, not request/retry ID),
`source={base_revision,test_revision,production_revision,
files:[{path,sha256,content}],sha256}`, `argv` (fixed qualified go_path followed
by `test,-json,-count=1` and configured packages), and `input_sha256`.
Source SHA is SHA256 of encoding/json.Marshal of source with only its sha256
removed; input SHA uses the same existing canonical byte rule after removing
only input_sha256. Source files are complete, sorted paths, actual committed
regular files. The executor must verify the received bytes and exact argv
before starting any Go process, retain descendant custody and use its guarded
allocation. The service captures actual wrapper stdout/stderr/exit/signal and
timeout. No local receipt path or claimed summary substitutes for this process.

Stdout is one strict `velenza_tdd_executor_result/1` object with exact
`producer_generation,input_sha256,source_sha256,argv`, capabilities
`['native-go-test-json','immutable-source-v1','exact-argv-v1',
'descendant-custody-v1']`, `completed`, `process_started`, actual `exit_code`,
empty `signal`, `go_test_json` (the complete Go event stream), and
`provider_receipt={schema_version:velenza_native_go_receipt/1,allocation_id,
source_sha256,argv_sha256,exit_code,completed,started_at,finished_at}`.
Capabilities must come from the actually invoked pinned process. The native
provider must attest exact materialized source/argv and actual child completion,
not merely allocation/time/exit. Wrapper OS exit must equal the reported Go
exit for execution (RED1/GREEN0); start is a0 capability handshake with no test
execution. Malformed/duplicate/oversize/truncated output, missing capability,
wrong binding/source/argv, launch failure, timeout/signal or incomplete custody
refuses. A controlled test executable models this protocol, proves the actual
service process boundary and storage wiring, and is explicitly NOT native
provider qualification or real test-execution evidence for a deployed system.

**RED, review and GREEN.** RED requires actual build-tests and freezes the
committed test revision/files/author/requirements. The service independently
parses the captured Go JSON: at least one actual test run and assertion-failing
test, package completion, no build-fail/compile error, zero-test/skip-only run,
signal or startup failure. Nonzero exit alone is never RED. It publishes a
typed ref in `slot.tdd_red_receipt` and moves only that slot to validate-tests.
When this Task/binding has exclusive TDD start lineage, validate_tests review
must resolve its current RED, identical test hashes/source/generation and
actual slot; neither removal of a label/reference nor old mutable receipts
opts out. Ordinary non-TDD review/bookkeeping remains supported. The real
review input/output and stored origin bind `tdd_red_ref`; publication adds
`tests.tdd_audit_receipt={review_ref,tdd_ref}` alongside existing tests.acceptance.
Coordinator bookkeeping can then record build-code; it does not create origin.

GREEN requires actual build-code, current RED, exact independent validate_tests
origin/audit, unchanged locked tests and immutable production revision. Parse
actual nonzero test-run/pass completion with no fail/build-fail/skip-only,
signal or mismatch and actual exit0. Publish `slot.tdd_green_receipt` and move
the slot to test-code. This does not prove mutation sensitivity, final review,
installation or release; their existing testing/merge checks remain required.

**Reopen and the unchanged-production exception.** Reopen requires current
test-code, exact current GREEN/lineage, immutable repaired test revision and
nonempty intent reason. A fresh actual independent no-tools reviewer consumes
`kind:tdd_reopen`, full committed old/new diff and changed bodies/hashes, full
governing owner, actual prior GREEN/test-review/production-origin references,
and complete old/new production blob manifests. Its strict output schema
`velenza_tdd_reopen_review/1` binds kind/input digest, zero blocking findings,
`classification:fixture_only|production_changed`, complete `reviewed_files`,
`production_files`, boundary flags and summary. The service separately verifies
all immutable production blobs and the complete diff: no caller fixture flag,
file suffix, omitted path or reviewer label can relabel changed production as
unchanged fixtures. Incomplete/contradictory audit refuses publication.

An approved reopen derives a fresh generation, appends its typed reference to
array `slot.reopen_history`, returns build-tests, updates the actual test
manifest from immutable source, and invalidates affected RED/test-review/audit/
GREEN/production-acceptance projections. It never overwrites historical origin.
If every prior independently reviewed production blob is unchanged and actual
audit proves a test-only repair, its immutable lineage may retain that prior
production reference. `task_graph_tdd_get` specifically verifies the historical
review origin and exact unchanged production blobs in the current generation
before returning `retained_production_ref`; ordinary stale review_get does not
gain a bootstrap exception. Production changes invalidate retention. Fresh RED
and exact new test review/audit are still required. For a fixture-only reopened
generation, RED materializes repaired committed tests against the independently
recorded preimplementation production base, while GREEN materializes them
against the unchanged verified production revision. Original revision IDs and
both layer manifests are bound; no empty production commit or invented ancestor
is required, and no blanket new production review follows a test-only repair.

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
