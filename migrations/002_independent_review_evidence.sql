-- Candidate only. Apply reviewed merged bytes through the sanctioned schema
-- deployment path after native project namespacing. No legacy-note backfill.
-- Configure review_evidence_table explicitly only with this exclusive table;
-- never alias a generic Task/TaskLinks/TaskDeps or admission table to it.
-- The service account is the sole writer. Generic tools cannot write this
-- schema. Keep historical rows immutable; no cascade on task/binding changes.
CREATE TABLE IndependentReviewEvidence (
  project_id STRING(64) NOT NULL,
  review_id STRING(64) NOT NULL,
  task_id STRING(64) NOT NULL,
  execution_project_id STRING(64) NOT NULL,
  execution_task_id STRING(128) NOT NULL,
  slot_key STRING(64) NOT NULL,
  kind STRING(32) NOT NULL,
  input_sha256 STRING(64) NOT NULL,
  binding_json STRING(MAX) NOT NULL,
  result_json STRING(MAX) NOT NULL,
  producer_generation STRING(128) NOT NULL,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (project_id, review_id);
