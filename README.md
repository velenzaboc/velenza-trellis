# velenza-trellis (`task-graph`) — Layer 3 of the Velenza Arbor Stack

`velenza-trellis` (compatible CLI/MCP alias: `task-graph`, Unified CLI: `inr trellis` / `inr task`) is **Layer 3 of the Velenza Arbor Stack**: the Plan-to-Done Execution & Evidence Graph MCP server and CLI that binds multi-agent tasks, dependencies, and cryptographically verified worklinks (commits, 64-char Inarch BLAKE3 changeset IDs, worktrees, pull requests, and TDD checkpoints) in a queryable Spanner graph.

It shares the node-graph admission framework with Layer 2 ([`velenza-canon` / `spec-graph`](https://github.com/velenzaboc/velenza-canon)): same 10-step admission ceremony, closed `node_kinds` / `edge_labels` / `task_statuses` / `dependency_kinds` / `artifact_kinds` registries, `SpannerSubstrateAdapter`, snapshot-pinned reads, and fail-closed posture. See `REQUIREMENTS.md` for the full data model and migration history.

## The Velenza Arbor Stack & Cross-Layer Integration

`velenza-trellis` is **Layer 3** of the open-source **Velenza Arbor Stack**, a 4-layer neuro-symbolic software engineering stack for concurrent AI coding agents:

| Layer | Repository | Canonical Binary | Compatibility Alias | Umbrella CLI | Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Layer 0** | [`velenzaboc/velenza-inarch`](https://github.com/velenzaboc/velenza-inarch) | `velenza-inarch` | `inr` | `inr` | **The Graft Union & Rootstock** — Checkout-free content-addressed VCS (`BonsaiChangeset`, 64-char BLAKE3 changeset IDs, client-side rebase, JIT leases) |
| **Layer 1** | [`velenzaboc/velenza-cambium`](https://github.com/velenzaboc/velenza-cambium) | `velenza-cambium` | `code-graph` | `inr cambium` / `inr code` | **The Living Vascular Code Tissue** — CPG/SSA/VTA semantic code graph and deterministic production hazard analyzer (`code_graph_hazards`) |
| **Layer 2** | [`velenzaboc/velenza-canon`](https://github.com/velenzaboc/velenza-canon) | `velenza-canon` | `spec-graph` | `inr canon` / `inr spec` | **The Normative Root Canon** — Versioned architectural specification graph and requirement-to-symbol governance (`trace-code`) |
| **Layer 3** | [`velenzaboc/velenza-trellis`](https://github.com/velenzaboc/velenza-trellis) | `velenza-trellis` | `task-graph` | `inr trellis` / `inr task` | **The Structural Growth Trellis** — Multi-agent execution DAG, TDD receipts, and cryptographic worklink provenance (`fg-validator-runner`) |

### How `velenza-trellis` Integrates Across the Stack

1. **With Layer 0 ([`velenza-inarch`](https://github.com/velenzaboc/velenza-inarch)):**
   - **Native 64-Char BLAKE3 Changeset Worklinks:** `velenza-trellis` (`ValidateFGBinding` and `fg-validator-runner`) natively accepts both 64-character lowercase hex `velenza-inarch` BLAKE3 `ChangesetId` digests and 40-character Git SHA-1 commit hashes on `TaskLinks` worklinks, binding every completed task directly to its immutable changeset in `velenza-inarch`.
   - **Swarm Lease & Worktree Coordination:** Autonomous agents claim tasks via `task_graph_claim` / `task_graph_reown` (which record owner, session, branch, and worktree bindings) paired with path-scoped JIT leases minted by `inr lease checkout`.
   - **Umbrella CLI Delegation:** `inr trellis <subcommand>` (or `inr task <subcommand>`) delegates directly to `velenza-trellis`, propagating `ARBOR_GCP_PROJECT` and `ARBOR_SPANNER_INSTANCE` and resolving `$VELENZA_TRELLIS_CONFIG` / `$TASK_GRAPH_CONFIG`.
2. **With Layer 1 ([`velenza-cambium`](https://github.com/velenzaboc/velenza-cambium)):**
   - **Symbol Snapshot & Hazard Evidence:** Changeset SHAs recorded on `velenza-trellis` task worklinks correspond 1:1 to `Snapshots.commit_sha` (`STRING(64)`) in `velenza-cambium`, allowing reviewers and gate runners to query symbol diffs (`code_graph_diff`), semantic drift (`code_graph_drift`), and SSA/VTA concurrency hazards (`code_graph_hazards`) for any task's landed changeset.
3. **With Layer 2 ([`velenza-canon`](https://github.com/velenzaboc/velenza-canon)):**
   - **Normative Requirement Closure & Shared Admission Engine:** `velenza-trellis` shares the 10-step admission ceremony and closed-registry engine with `velenza-canon`. Tasks track execution of normative specification nodes in `velenza-canon`, while `velenza-canon trace-code` verifies that the code landed under a `velenza-trellis` task satisfies its `//spec:` and `//volt:` traceability anchors.

## Build

```bash
go build -o bin/velenza-trellis .
go build -o bin/task-graph .
```

## Subcommands

| Subcommand | Purpose |
|---|---|
| `velenza-trellis mcp` (`task-graph mcp`) | Run as MCP Streamable HTTP server on the config's `listen_address` |
| `velenza-trellis init` (`task-graph init`) | Apply `bootstrap/schema_v1.sql` to the configured database via the Spanner Admin API. Idempotent. Refuses any database not matching `init_database_allowlist` (`["task-graph"]`). |
| `velenza-trellis reset --confirm` (`task-graph reset --confirm`) | Drop + recreate the database, then re-apply schema. Refuses without `--confirm`. |
| `velenza-trellis fg-validator-runner FILE` | Run read-only FG work-identity binding checks over frozen snapshot JSON (accepts both 40-char Git SHA-1 and 64-char Inarch BLAKE3 changeset IDs). |
| `velenza-trellis arbor-info [--config FILE]` | Emit the Layer 3 Velenza Arbor Stack JSON descriptor (`inr trellis arbor-info` / `inr task arbor-info`). |

## Verb surface (24)

**State pin:** `task_graph_use`

**Node lifecycle (write):** `task_graph_create`, `task_graph_update`

**Edges (write):** `task_graph_link`, `task_graph_unlink`

**Generic reads:** `task_graph_get`, `task_graph_list`, `task_graph_search`,
`task_graph_neighbors`, `task_graph_closure`, `task_graph_impact`,
`task_graph_query` (ad-hoc read-only SQL/GQL)

**Bulk + ops:** `task_graph_import` (topologically-ordered batch admission —
the migration/reseed surface), `task_graph_health` (readiness probe)

**Task-domain verbs** (the task-plan SQL recipe set — in live mode these
operate directly on `task-plan`'s `Tasks`, `TaskLinks`, and `TaskDeps`
tables; in legacy migrated mode they operate on `Nodes` / `Edges`):

- `task_graph_tasks` — filter by pillar/status/owner/is_gap/gap_label/level
- `task_graph_project_snapshot` — the authoritative coherent native binding read: bounded tasks (including owner/session/repo) + dependencies + worklinks (including repo/branch/commit/PR/worktree/url) + counts; optional `scope=execution_subtree` or `scope=acceptance_dependency_closure` with `root_task_id` provides a server-owned MVP subtree/closure; capped collections return an authenticated continuation that resumes the same Spanner read timestamp and preserves cumulative missing-prerequisite scope state
- `task_graph_task_counts` — grouped (pillar,status) counts, the same counts per canonical `task_statuses` member with its terminal flag (`by_pillar_canonical_status`; literals no member absorbs count as `unrecognized` and are listed in `unrecognized_status_literals`), + distinct-gap count
- `task_graph_worklinks` — artifacts advancing a task
- `task_graph_add_worklink` — record an artifact against a task
- `task_graph_archive_worklink` — archive one erroneous native worklink by exact project/task/link identity; it refuses a superseded predecessor and the sole valid active binding, which only `task_graph_supersede_worklink` replaces
- `task_graph_supersede_worklink` — atomically retire one VALID active binding worklink (`worktree`, typically a merged branch) as `artifact_status=superseded` and install its exact successor under the same task, owner, session and thread; exact-field compare-and-set, one transaction, idempotent identical retry, first-writer-wins for conflicting contenders. The predecessor stays readable history; `task_graph_project_snapshot` carries only active rows. `task_graph_update` cannot retire or rewrite the sole valid binding, cannot mint `superseded`, and cannot touch a superseded worklink or the supersede linkage note, so this verb is the sole way to succeed a binding
- `task_graph_set_status` — update a task's status, everything else preserved
- `task_graph_reown` — compare-and-set a task's owner/session/repository binding while preserving every non-binding field; its optional native-only `writer_handover` also moves the sole protected worktree writer atomically and supports repair of an already-reowned task
- `task_graph_claim` — atomically claim an unowned task for an owner/session/repo; retries require the same durable tuple, and only the first successful claim receipt may attest optional branch/worktree execution coordinates

Owned-task handovers through `task_graph_reown` must provide the new
`session_id` and `repo` plus `expected_owner`, `expected_session_id`, and
`expected_repo`; recovery to `UNOWNED` omits and clears session/repository and
requires the expected tuple when the current task is owned. Claims and native handovers return
binding evidence (owner, session, repository, and server epoch) in the mutation
receipt. When a protected worktree must move with the task, `writer_handover`
names its exact link/type/status/repo/branch/path/thread/session and an explicit
`target_thread`. The target session is the outer `session_id`; one native
transaction updates both rows and records immutable deterministic history.
Exact retries return zero writes, while ambiguous bindings, mismatched history,
and plain re-owns that would leave task and writer identity split fail closed.

## Config

The binary reads `$VELENZA_TRELLIS_CONFIG` or `$TASK_GRAPH_CONFIG` (default `./task-graph.config.json`).
Every closed registry (substrates, node_kinds, edge_labels, projections, task_statuses, dependency_kinds, artifact_kinds)
is a section of this file — `binary_is_never_sot`: adding to a registry is a
JSON edit + restart, never a Go edit.

The checked-in runtime config targets the Spanner database `task-plan`
(configured via `projects/${ARBOR_GCP_PROJECT}/instances/${ARBOR_SPANNER_INSTANCE}/databases/task-plan`),
using the native tables `Tasks`, `TaskLinks`, and `TaskDeps`.
It deliberately omits `project_column` so a source merge, rebuild, and MCP
restart remain coherent before the live schema cutover. In this pre-cutover
mode, omitted `project_id` and explicit `project_id="arbor"` resolve to the
backward-compatible Arbor namespace; any non-default project id fails closed
instead of reading/writing the un-namespaced live tables.

After the sanctioned schema cutover, run with
`VELENZA_TRELLIS_CONFIG=task-graph.post-cutover.config.json` (or `TASK_GRAPH_CONFIG=task-graph.post-cutover.config.json`). That config enables
`project_column="project_id"` and activates first-class non-overlapping
project namespaces against production-named `Tasks`, `TaskLinks`, and
`TaskDeps`.

Resumable snapshots use a dedicated runtime credential, never the transport
bearer secret. Configure it only when the operator has provisioned the named
environment variable:

```json
"snapshot_continuation": {
  "mode": "hmac_sha256_base64_env",
  "key_env": "TASK_GRAPH_SNAPSHOT_CONTINUATION_KEY_B64",
  "ttl_seconds": 300
}
```

The environment value is standard-base64 encoded key material that decodes to
at least 32 bytes. The key environment name must differ from
`transport_auth.secret_env`, and boot also rejects equal resolved secrets.
Omitting the block keeps uncapped snapshots available; a capped snapshot then
fails closed instead of returning an unusable partial page. Provisioning the
secret, changing checked-in runtime config, rebuilding, and restarting remain
separate operator actions.

Live schema cutover requires the project-keyed migration in
`migrations/001_fleet_plan_project_namespace.sql`: create replacement tables,
backfill existing rows to `arbor`, verify row counts, cut the replacement
tables over to the production names, then switch config/rebuild/restart and
smoke. The target schema is `Tasks` key `(project_id, id)`, `TaskLinks` key
`(project_id, link_id)`, and `TaskDeps` key `(project_id, dep_id)`.

## Sentinels

`ErrSchemaPending`, `ErrSubstrateNotProvisioned`, `ErrRegistryNotLoaded`,
`ErrUnknownNodeKind`, `ErrUnknownEdgeLabel`, `ErrReciprocityViolation`,
`ErrClosureWouldDangle`, `ErrContractDrift`, `ErrCeremonyAtomicityBroken`,
`ErrSnapshotNotPinned`, `ErrModeMismatch`, `ErrForbiddenDatabaseTarget`,
`ErrSchemaNotApplied`, `ErrImportDependencyUnresolved`,
`ErrSchemaFingerprintMismatch`, `ErrEdgeKindMismatch`,
`ErrEdgeSelfLoopForbidden`, `ErrDuplicateCanonicalName`,
`ErrPossibleDuplicate`.

## Repo layout

| File | Responsibility |
|---|---|
| `main.go` | CLI entry: `mcp` / `init` / `reset` / `fg-validator-runner` / `arbor-info` subcommand dispatch |
| `trellis_arbor.go` | Layer 3 Velenza Arbor Stack descriptor + `resolveConfigPath` (`$VELENZA_TRELLIS_CONFIG` / `$TASK_GRAPH_CONFIG`) |
| `mcp.go` | MCP server + 24 verb registrations |
| `handlers.go` | Generic verb handlers + shared helpers (`readNodeAcrossSubstrates`, `admitEdge`, `topologicallyOrder`, …) |
| `task_queries.go` | The 10 task-domain verbs |
| `ceremony.go` | The 10-step admission ceremony |
| `substrate.go` | `SubstrateRouter` + generic `SpannerSubstrateAdapter` (schema_v1 — the one this substrate uses) |
| `kinds.go` / `edges.go` / `projection.go` | Closed-registry interfaces + file-loaded implementations |
| `config.go` | `SpecGraphConfig` envelope + `Validate()` |
| `cmd_init.go` / `cmd_reset.go` | DDL apply + DB reset subcommands |
| `types.go` | `Node`, `NodeVersion`, `Edge`, `AdmissionRecord`, `NativeMutationReceipt`, result wrappers |
| `errors.go` | Typed sentinels |
| `bootstrap/schema_v1.sql` | DDL: `Nodes` / `Edges` / `ArtifactAdmissionLog` / `CorpusGraph` property graph |
| `velenza-trellis.config.example.json` | Open-source example config template |
| `task-graph.config.json` | Default pre-cutover config — task-plan schema, substrate, node_kinds, edge_labels |
| `task-graph.post-cutover.config.json` | Explicit post-cutover config — enables `project_column="project_id"` after schema cutover |
| `TESTING.md` | Unit/integration test discipline + the manual smoke walk |
| `REQUIREMENTS.md` | The data model, verb surface, and migration history |

## License

Licensed under the Apache License, Version 2.0. See `LICENSE` for details.
