-- TASKGRAPH-PROJECT-NAMESPACE
-- Purpose: add a first-class logical project namespace to task-plan native
-- tables. This file is a sanctioned migration artifact only; do not pipe it
-- blindly into gcloud because it mixes DDL and DML verification steps.
--
-- Default/backfill project_id: "arbor".
-- Rationale: the existing store tracks Arbor fleet plan work. "fleet" names
-- the bookkeeping system; "arbor" names the logical task universe.
--
-- Required cutover property:
--   Tasks are keyed by (project_id, id), TaskLinks by (project_id, link_id),
--   and TaskDeps by (project_id, dep_id). This is what permits two logical
--   projects to both contain task id "GOAL" without overlap.

-- 1. Create project-keyed replacement tables.
CREATE TABLE Tasks_project_namespace (
  project_id STRING(64) NOT NULL,
  id STRING(64) NOT NULL,
  title STRING(512) NOT NULL,
  pillar STRING(16) NOT NULL,
  layer STRING(8),
  status STRING(32) NOT NULL,
  owner STRING(64) NOT NULL,
  parent_id STRING(64),
  is_gap BOOL NOT NULL,
  note STRING(MAX),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  gap_label STRING(16),
  session_id STRING(64),
  repo STRING(80),
  is_critical_path BOOL,
  started_at TIMESTAMP,
  done_at TIMESTAMP
) PRIMARY KEY (project_id, id);

CREATE TABLE TaskLinks_project_namespace (
  project_id STRING(64) NOT NULL,
  link_id STRING(64) NOT NULL,
  task_id STRING(64) NOT NULL,
  artifact_type STRING(32) NOT NULL,
  artifact_ref STRING(512) NOT NULL,
  thread STRING(64),
  note STRING(MAX),
  created_at TIMESTAMP NOT NULL,
  session_id STRING(64),
  repo STRING(80),
  branch STRING(200),
  commit_sha STRING(64),
  pr_number INT64,
  pr_state STRING(20),
  worktree_path STRING(300),
  url STRING(500),
  artifact_status STRING(30),
  CONSTRAINT FK_TaskLinks_ProjectTask FOREIGN KEY (project_id, task_id) REFERENCES Tasks_project_namespace (project_id, id)
) PRIMARY KEY (project_id, link_id);

CREATE TABLE TaskDeps_project_namespace (
  project_id STRING(64) NOT NULL,
  dep_id STRING(40) NOT NULL,
  task_id STRING(64) NOT NULL,
  depends_on_task_id STRING(64) NOT NULL,
  kind STRING(20),
  note STRING(MAX),
  created_at TIMESTAMP,
  CONSTRAINT FK_TaskDeps_ProjectTask FOREIGN KEY (project_id, task_id) REFERENCES Tasks_project_namespace (project_id, id),
  CONSTRAINT FK_TaskDeps_ProjectDependsOn FOREIGN KEY (project_id, depends_on_task_id) REFERENCES Tasks_project_namespace (project_id, id)
) PRIMARY KEY (project_id, dep_id);

-- 2. Backfill existing Arbor rows into the replacement tables.
INSERT INTO Tasks_project_namespace (
  project_id, id, title, pillar, layer, status, owner, parent_id, is_gap,
  note, created_at, updated_at, gap_label, session_id, repo,
  is_critical_path, started_at, done_at
)
SELECT 'arbor', id, title, pillar, layer, status, owner, parent_id, is_gap,
  note, created_at, updated_at, gap_label, session_id, repo,
  is_critical_path, started_at, done_at
FROM Tasks;

INSERT INTO TaskLinks_project_namespace (
  project_id, link_id, task_id, artifact_type, artifact_ref, thread, note,
  created_at, session_id, repo, branch, commit_sha, pr_number, pr_state,
  worktree_path, url, artifact_status
)
SELECT 'arbor', link_id, task_id, artifact_type, artifact_ref, thread, note,
  created_at, session_id, repo, branch, commit_sha, pr_number, pr_state,
  worktree_path, url, artifact_status
FROM TaskLinks;

INSERT INTO TaskDeps_project_namespace (
  project_id, dep_id, task_id, depends_on_task_id, kind, note, created_at
)
SELECT 'arbor', dep_id, task_id, depends_on_task_id, kind, note, created_at
FROM TaskDeps;

-- 3. Verify before any destructive swap. All three counts must match.
SELECT 'Tasks' AS table_name, (SELECT COUNT(*) FROM Tasks) AS old_count,
  (SELECT COUNT(*) FROM Tasks_project_namespace WHERE project_id = 'arbor') AS new_count;
SELECT 'TaskLinks' AS table_name, (SELECT COUNT(*) FROM TaskLinks) AS old_count,
  (SELECT COUNT(*) FROM TaskLinks_project_namespace WHERE project_id = 'arbor') AS new_count;
SELECT 'TaskDeps' AS table_name, (SELECT COUNT(*) FROM TaskDeps) AS old_count,
  (SELECT COUNT(*) FROM TaskDeps_project_namespace WHERE project_id = 'arbor') AS new_count;

-- 4. Cutover in a maintenance window after counts match. Spanner primary
-- keys cannot be altered in place; preserve the old tables until
-- post-cutover smoke passes.
--
-- Recommended operator sequence:
--   a. Merge source with default task-graph.config.json. That config omits
--      project_column and remains compatible with the current live schema.
--   b. Pull main, rebuild, and restart only if desired; before cutover the
--      running service supports only omitted/default project_id="arbor" and
--      rejects non-default projects fail-closed.
--   c. Export/backup task-plan.
--   d. Apply the CREATE TABLE statements above with UpdateDatabaseDdl.
--   e. Run the three INSERT INTO ... SELECT statements with execute-sql.
--   f. Run the three verification SELECT statements and confirm equality.
--   g. Rename old tables aside and replacement tables into place, or recreate
--      production-named tables with the composite PKs and copy from the
--      replacement tables if table rename is not available in the target
--      Spanner release.
--   h. Switch the runtime to task-graph.post-cutover.config.json
--      (TASK_GRAPH_CONFIG=task-graph.post-cutover.config.json), rebuild, and
--      restart task-graph. That config sets default_project_id="arbor" and
--      project_column="project_id".
--   i. Run TESTING.md's smoke walk using project_id="arbor", then create two
--      test tasks with the same id in two non-Arbor projects in a throwaway
--      database before enabling multi-project live writes.
