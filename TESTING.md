# task-graph testing

## Test discipline (REQUIREMENTS.md §7)

Each capability requires before merge:

- **Unit test** of orchestration shape with the substrate mocked. Lives in `*_test.go` alongside the file under test. Run via `make test-unit` (also the default `make test`).
- **Integration test** against a Spanner emulator. Lives in test files behind the `integration` build tag. Run via `make test-integration` — the shared `scripts/test-integration.sh` entrypoint starts its own pinned emulator in Docker, runs the complete integration-only source tier, and verifies owned cleanup.
- **Manual smoke test** documented below. Run end-to-end once after every meaningful change against the real `task-graph` database.

`t.Skip` is forbidden per CLAUDE.md `feedback_no_t_skip_in_tests`. Build-tag exclusion (`//go:build integration`) is not a runtime skip — it's a compile-time variant; tests under the tag still run explicitly via `go test -tags=integration` and the Makefile.

The integration entrypoint uses actual `go list` membership with and without
`-tags=integration`, then a temporary standard-library Go AST helper to select
every valid test in the tagged-only files. Its sorted, exact parent-test regex
includes audit and compatibility tests regardless of naming prefix, and runs
all their subtests. Untagged unit fixtures stay in `make test-unit`; they are
not run against an already listening external emulator. Empty, malformed or
duplicate selections fail before emulator creation.

Local invocation needs Go, Bash, Docker, curl and `timeout`, with the emulator
endpoints `127.0.0.1:9010` and `127.0.0.1:9020` available. An occupied endpoint
causes refusal without touching its owner. The emulator image is pinned by
digest, readiness is bounded to 30 attempts, and success, failure or interruption
removes only the exact owned container ID and verifies that it is gone. Cleanup
failure is reported as failure; subprocess failures retain their exit status.

No CI identity variables are required. Scratch storage is a unique native
temporary directory under optional `RUNNER_TEMP`, otherwise `TMPDIR` or the
system temporary directory. When both `GITHUB_RUN_ID` and `GITHUB_RUN_ATTEMPT`
are available, the existing CI container label is retained; local invocations
use a unique name. Make and the CI integration step invoke this same entrypoint
through explicit Bash; the script does not require a filesystem executable bit.

### A note on emulator instance setup

The integration test process provisions `test-instance` through the Spanner
Instance Admin client before creating its test database. The client follows
`SPANNER_EMULATOR_HOST`, disables authentication on that route, and treats
only `ALREADY_EXISTS` as an idempotent success. This keeps both
`make test-integration` and the reusable coverage workflow self-contained;
neither path depends on a separately configured `gcloud` invocation.

## Integration test scope (emulator gaps)

The Spanner emulator supports DDL apply, mutations, standard SQL queries (including `JSON_VALUE`/`SAFE_CAST`/`GROUP BY`/joins — everything the task-domain verbs use), and the typed-row write path. It does NOT (as of 2026) support:

- `CREATE PROPERTY GRAPH` + DYNAMIC LABEL
- `APPROX_COSINE_DISTANCE` / vector indexes
- `GRAPH CorpusGraph MATCH ...` traversal queries

Therefore the integration tests cover the admission ceremony + substrate write/read path + reciprocal-edge atomicity + RFC 6902 diff + the seven task-domain verbs (`TestIntegrationTaskDomainVerbs` plus native project-isolation coverage). Property-graph traversal and vector search are verified against real Spanner via the manual smoke walk below.

## Manual smoke test — the task-domain verb walk

Run from the repo root after building (`go build -o bin/task-graph .`), against the real `task-plan` database.

### Prereqs

The daemon impersonates `task-graph-local@example-gcp-project.iam.gserviceaccount.com`
— one database (`task-plan`) plus `aiplatform.endpoints.predict`, nothing else.
Two carrier shapes reach it, and the device one is preferred.

**Device-sourced (preferred — no human token in the refresh path).** The
workstation's own `workstation-<host>@` key is the impersonation SOURCE; the effective
identity is still the narrow SA above. This is what keeps one human OAuth expiry
from taking every workstation offline at once, as it did on 2026-09-06.

```bash
umask 077
jq -n --slurpfile k ~/.config/velenza-backup/workstation-$(hostname -s).json '{
  type: "impersonated_service_account",
  service_account_impersonation_url: "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/task-graph-local@example-gcp-project.iam.gserviceaccount.com:generateAccessToken",
  delegates: [], source_credentials: $k[0]
}' > ~/.config/gcloud/application_default_credentials.json
chmod 600 ~/.config/gcloud/application_default_credentials.json
```

**Human-sourced (fallback).** Still accepted; needs a browser and expires.

```bash
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE CLOUDSDK_CONFIG GCE_METADATA_HOST
gcloud auth application-default login \
  --impersonate-service-account task-graph-local@example-gcp-project.iam.gserviceaccount.com
```

Either way, verify with a real call — **a port answering 405 is not health**, and a
daemon will serve happily for hours on a dead credential:

```bash
gcloud spanner databases execute-sql task-plan \
  --instance=example-spanner-instance \
  --project=example-gcp-project \
  --sql='SELECT 1'
# then, against the running daemon, task_graph_health must return overall_ready: true
```

⚠ The daemon reads ADC **once at startup**. After changing the carrier, restart it
(`launchctl kickstart -k gui/$(id -u)/com.velenza.task-graph`) or it keeps using the
old credential.

The config binds every Google client to that exact service-account identity.
Local execution reads only the owner-protected well-known gcloud ADC carrier
written by the impersonated login command; it never follows
`GOOGLE_APPLICATION_CREDENTIALS` or an alternate `CLOUDSDK_CONFIG` root. The
process rejects generic human ADC, service-account key JSON, symlinked or
group/world-readable carriers, impersonation delegates, a different target,
and Cloud SDK or metadata-host overrides.

task-graph has no hosted deployment. The Cloud Run service, its
`task-graph-runtime` identity and its transport secret were retired on
2026-09-06 — this is a local MCP and the impersonated carrier above is its only
credential path. The `attached_service_account` profile still exists in
`internal/gcpauth` because other services use it, and its unit tests still name
the retired identity as a fixture string; that is a test fixture, not a live
identity.

### Step 0 — schema

```bash
./bin/task-graph init
```

Expected: `task-graph init` remains restricted to the legacy `task-graph`
database by `init_database_allowlist`; do not run it against live
`task-plan`.

### Step 1 — boot the MCP server and call health

```bash
./bin/task-graph mcp
# via your MCP client:
task_graph_health
```

Expected before cutover: substrate reachable at `databases/task-plan`,
native schema present (`Tasks`, `TaskLinks`, `TaskDeps`), registry sizes =
1 substrates / 2 node_kinds / 6 edge_labels / 1 projections / 7
task_statuses / 3 dependency_kinds / 27 artifact_kinds,
mode=experimental_bootstrap, `overall_ready: true`. The default runtime
config deliberately has no `project_column`, so health must not require
`project_id` on the current live tables.

`task_statuses` / `dependency_kinds` / `artifact_kinds` are the closed
registries gating every `fleet_plan_native` write. A **zero** on any of them
here is a blocker, not a curiosity: this config declares a
`fleet_plan_native` substrate, so an empty registry means every gated write
fails closed and `overall_ready` reports `false` with the offending registry
named in `blocking_issues`. If you see that, the server is reading a config
without those sections — check `TASK_GRAPH_CONFIG`. (A `schema_v1`
deployment does not consult these registries and stays ready without them;
their sizes are still reported.)

Expected after cutover: run with
`TASK_GRAPH_CONFIG=task-graph.post-cutover.config.json`; health must require
project column `project_id` on all three production-named tables.

### Step 2 — read the live plan

```
task_graph_task_counts(project_id="arbor")
task_graph_tasks(project_id="arbor", is_gap=true, owner="UNOWNED")
task_graph_worklinks(project_id="arbor", task_id="P3-SPECS")
```

Expected: non-zero counts from live `task-plan`; `task_graph_query` counts
match direct Spanner reads. Before cutover, compare to the current
un-namespaced tables (`SELECT COUNT(*) FROM Tasks`, `SELECT COUNT(*) FROM
TaskLinks`, and `TaskLinks WHERE task_id='<id>'`). After cutover, compare to
project-scoped reads filtered by `project_id='arbor'` (`SELECT COUNT(*) FROM
Tasks WHERE project_id='arbor'`, `SELECT COUNT(*) FROM TaskLinks WHERE
project_id='arbor'`, and `TaskLinks WHERE project_id='arbor' AND
task_id='<id>'`).

### Step 3 — record a work-link, update status, re-own

```
task_graph_add_worklink(project_id="arbor", task_id="<a real task id>", artifact_type="finding", artifact_ref="smoke test", thread="smoke", session_id="smoke-sess", repo="task-graph", branch="smoke-branch", commit_sha="<any sha>", pr_number=1, pr_state="open", worktree_path="/tmp/smoke", url="https://example.invalid/smoke")
task_graph_get(project_id="arbor", node_id="<the returned artifact id>")  # confirms the provenance fields PERSISTED, not just were accepted
task_graph_archive_worklink(project_id="arbor", task_id="<same id>", link_id="<the returned artifact id>")
# A valid active worktree binding is superseded, never archived, by its successor (see REQUIREMENTS.md section 5):
# task_graph_supersede_worklink(project_id="arbor", task_id="<owned id>", expected_owner=..., expected_session_id=..., predecessor_link_id=..., expected_predecessor={...}, successor={...})
task_graph_set_status(project_id="arbor", task_id="<same id>", status="in_progress")
task_graph_reown(project_id="arbor", task_id="<same id>", owner="smoke-test-thread", session_id="smoke-sess", repo="task-graph")
task_graph_worklinks(project_id="arbor", task_id="<same id>")  # confirms the new work-link
task_graph_tasks(project_id="arbor", owner="smoke-test-thread")  # confirms the re-own, and that title/pillar/is_gap/gap_label/note survived

# First-writer claim (native task-plan):
task_graph_claim(project_id="arbor", task_id="<unowned id>", owner="smoke-test-thread", session_id="smoke-sess", repo="task-graph")
# An owned-task handover requires the exact current binding:
task_graph_reown(project_id="arbor", task_id="<same id>", owner="new-thread", session_id="new-sess", repo="task-graph", expected_owner="smoke-test-thread", expected_session_id="smoke-sess", expected_repo="task-graph")
# A task with one protected worktree moves that writer in the same transaction.
# target_thread is explicit; its target session is the outer session_id:
task_graph_reown(project_id="arbor", task_id="<same id>", owner="new-worker", session_id="new-session", repo="task-graph", expected_owner="old-worker", expected_session_id="old-session", expected_repo="task-graph", writer_handover={"link_id":"<existing binding id>","artifact_type":"worktree","artifact_status":"recorded","repo":"task-graph","branch":"<existing branch>","worktree_path":"<existing path>","thread":"<old thread UUID>","session_id":"old-session","target_thread":"<new thread UUID>"})
```

Expected: each write returns a `native_mutation_receipt` inside the write
result envelope (not an `admission_record`, because no admission ceremony ran);
the archived worklink disappears from `task_graph_worklinks`, re-archiving it
returns a successful receipt with zero rows affected, and the task fields not
touched by set_status/reown are preserved. The writer handover returns a
`writer_handover_receipt`; an exact retry returns `rows_affected=0`, and the
stable binding keeps its link id, branch and path while its thread/session move.

The `task_graph_get` read-back is the load-bearing step of the add_worklink
line, not decoration: `task_graph_worklinks` projects only seven of the sixteen
`TaskLinks` columns, so it CANNOT show whether the provenance fields landed —
only `task_graph_get` on the link id surfaces `session_id` / `repo` / `branch` /
`commit_sha` / `pr_number` / `pr_state` / `worktree_path` / `url`. Those eight
were accepted by `task_graph_update` and silently discarded by
`task_graph_add_worklink` until P3-GT-FP-WORKLINK-FIELD-PARITY, and a smoke walk
that never reads them back cannot tell the two states apart.

### Step 4 — closure / property graph

```
task_graph_neighbors(project_id="arbor", node_id="<a pillar id, e.g. P2>", direction="out", edge_labels=["parent_of"])
task_graph_closure(node_id="GOAL", closure_profile="struct", max_depth=5)
```

Expected: `neighbors` returns the pillar's direct children; `closure` walks the full plan subtree under `GOAL` via `parent_of`/`child_of` (`edge_family=struct`).

For a disposable dependency, call `task_graph_link` twice with the same
project/source/target/label and confirm both receipts carry one `artifact_id`
with the second reporting `rows_affected=0`. Read either reciprocal projection
with `task_graph_neighbors`, pass its exact `edge_id` to `task_graph_unlink`,
and confirm both projections disappear; a second unlink reports zero rows.

### Pass criteria

All four steps complete without error. Re-running Step 2/3/4 against the
live graph never regresses (idempotent reads; writes mutate the single live
task-plan store, not a copied graph).

## Parity — the historical task-plan migration proof

The initial `task_graph_import` migration (see `REQUIREMENTS.md` §6) was
verified by running the same recipe against BOTH `task-plan` (raw SQL) and
`task-graph` (the corresponding verb), at the same point in time, and
diffing the results:

| Recipe | task-plan SQL | task-graph verb | Result |
|---|---|---|---|
| (a) grouped counts | `SELECT pillar,status,COUNT(*) FROM Tasks WHERE project_id='arbor' GROUP BY pillar,status` | `task_graph_task_counts(project_id="arbor").by_pillar_status` | exact match, all groups |
| (b) tasks by owner | `SELECT id,title,status FROM Tasks WHERE project_id='arbor' AND owner='Conversation'` | `task_graph_tasks(project_id="arbor", owner="Conversation")` | exact match |
| (c) unowned gaps | `SELECT gap_label,id,owner,title,note FROM Tasks WHERE project_id='arbor' AND is_gap=true AND owner='UNOWNED'` | `task_graph_tasks(project_id="arbor", is_gap=true, owner="UNOWNED")` | exact match |
| (c) distinct gap count | `SELECT COUNT(DISTINCT gap_label) FROM Tasks WHERE project_id='arbor' AND is_gap=true` | `task_graph_task_counts(project_id="arbor").distinct_gap_count` | exact match — see the `NULLIF` note in `task_queries.go`; a naive `JSON_VALUE(...)='true'` comparison undercounted by treating NULL `gap_label`s as one shared `''` |
| (d) worklinks | `SELECT artifact_type,artifact_ref,thread,note,created_at FROM TaskLinks WHERE project_id='arbor' AND task_id='P3-SPECS'` | `task_graph_worklinks(project_id="arbor", task_id="P3-SPECS")` | exact match |
| (e) blocked/in_progress | `SELECT id,title,owner,status FROM Tasks WHERE project_id='arbor' AND status IN ('in_progress','blocked')` | `task_graph_tasks(project_id="arbor", statuses=["in_progress","blocked"])` | exact match |

Mutation recipes (insert work-link, update status, re-own) are covered by
`TestIntegrationTaskDomainVerbs` against the emulator rather than by
mutating the historical migrated data.

## Project namespace cutover smoke

Run only after `migrations/001_fleet_plan_project_namespace.sql` has been
applied, row counts have been verified, and the replacement tables have been
cut over to the production names:

```bash
TASK_GRAPH_CONFIG=task-graph.post-cutover.config.json ./bin/task-graph mcp
```

Then repeat the task-domain verb walk with `project_id="arbor"` and prove a
non-default namespace is isolated by creating two throwaway tasks with the
same local id in two non-default project namespaces, listing each project
separately, and confirming a cross-project dependency/link request fails closed.
