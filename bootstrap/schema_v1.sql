-- spec-graph experimental schema v1
--
-- Applied by `spec-graph init` against the configured experimental Spanner
-- database (default: corpus-graph-iter1). REFUSES against example-staging
-- by the corpus-graph-* allowlist enforced in cmd_init.go.
--
-- This schema captures the universal Node primitive + typed Edge model + the
-- ArtifactAdmissionLog. ONE Nodes table during the experimental phase;
-- per-node_kind typed tables emerge at production cutover per
-- REQUIREMENTS.md §3.2.

-- ---------- Nodes ----------

CREATE TABLE Nodes (
  organization_id            STRING(64)  NOT NULL,
  node_id                    STRING(64)  NOT NULL,
  version                    INT64       NOT NULL,
  node_kind                  STRING(128) NOT NULL,
  status                     STRING(32)  NOT NULL,
  contract                   JSON,
  content                    JSON,
  definition_hash            STRING(128),
  manifest_hash              STRING(128),
  signature_chain            ARRAY<STRING(MAX)>,
  admitted_by                STRING(256) NOT NULL,
  admitted_at                TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
  superseded_by_version      INT64,
  governing_spec_ref         STRING(MAX),
  source_spec_lineage_refs   ARRAY<STRING(MAX)>,
  spec_section_ref           STRING(MAX),
  spec_symbol_ref            STRING(MAX),
  node_creation_contract_ref STRING(MAX),
  -- active_marker is the indexable handle for "this version is the active one";
  -- the UNIQUE NULL_FILTERED INDEX below enforces single-active-version-per-stable-id.
  active_marker              STRING(64) AS (IF(status = 'active', node_id, NULL)) STORED,
  -- embedding_v2: per example-infra convention (gemini-embedding-001, 3072d).
  -- Populated at admission time by ceremony.go when Config.Embedding is set.
  -- spec_graph_search uses APPROX_COSINE_DISTANCE against this column.
  embedding_v2               ARRAY<FLOAT32>(vector_length=>3072),
) PRIMARY KEY (organization_id, node_id, version);

CREATE UNIQUE NULL_FILTERED INDEX NodesActiveVersionUniqueIdx
  ON Nodes (organization_id, active_marker);

CREATE INDEX NodesKindIdx        ON Nodes (organization_id, node_kind, status);
CREATE INDEX NodesAdmittedAtIdx  ON Nodes (organization_id, admitted_at);

CREATE VECTOR INDEX IF NOT EXISTS Nodes_embedding_v2_idx
  ON Nodes (embedding_v2)
  WHERE embedding_v2 IS NOT NULL
  OPTIONS (distance_type = 'COSINE', num_leaves = 100);


-- ---------- Edges ----------

CREATE TABLE Edges (
  organization_id    STRING(64)  NOT NULL,
  edge_id            STRING(64)  NOT NULL,
  source_node_id     STRING(64)  NOT NULL,
  source_version     INT64       NOT NULL,
  target_node_id     STRING(64)  NOT NULL,
  target_version     INT64       NOT NULL,
  edge_label         STRING(128) NOT NULL,
  status             STRING(32)  NOT NULL,
  admitted_at        TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
  reciprocal_edge_id STRING(64),
) PRIMARY KEY (organization_id, edge_id);

CREATE INDEX EdgesSourceIdx ON Edges (organization_id, source_node_id, edge_label);
CREATE INDEX EdgesTargetIdx ON Edges (organization_id, target_node_id, edge_label);

-- ---------- ArtifactAdmissionLog (REQUIREMENTS.md §3.2) ----------

CREATE TABLE ArtifactAdmissionLog (
  organization_id        STRING(64)  NOT NULL,
  admission_id           STRING(64)  NOT NULL,
  artifact_id            STRING(64)  NOT NULL,
  artifact_type          STRING(128) NOT NULL,
  artifact_version       INT64       NOT NULL,
  artifact_hash          STRING(128) NOT NULL,
  manifest_hash          STRING(128) NOT NULL,
  admitted_by            STRING(256) NOT NULL,
  admitted_at            TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
  signature              STRING(MAX) NOT NULL,
  dual_control_approver  STRING(256),
  -- is_non_genesis distinguishes ceremony-by-real-principal admissions (TRUE)
  -- from boot-time GENESIS admissions and experimental_bootstrap-mode
  -- admissions (FALSE). REQUIREMENTS.md §3.2 + §3.8: the experimental
  -- admitter literal is config-driven, so the application writes this
  -- column explicitly rather than deriving it from a hardcoded literal.
  is_non_genesis         BOOL        NOT NULL,
  pass_report            JSON,
) PRIMARY KEY (organization_id, admission_id);

CREATE INDEX ArtifactAdmissionLogArtifactIdx
  ON ArtifactAdmissionLog (organization_id, artifact_id, artifact_version);

-- ---------- Property Graph ----------
--
-- REQUIREMENTS.md §3.2 requires CREATE PROPERTY GRAPH CorpusGraph over Nodes
-- + Edges so GRAPH_TABLE / MATCH queries work.
--
-- DYNAMIC LABEL pattern per
-- /opt/velenza/specs-local/supporting/reference/spanner-graph-capabilities-2026-05-22.md:
--   - ONE node table (Nodes), keyed by its PK (org, node_id, version)
--   - DYNAMIC LABEL projects node_kind as the per-row graph label
--   - Edges reference Nodes by (org, source_node_id, source_version) and
--     (org, target_node_id, target_version). Version-pinning is what makes
--     the source/destination KEY statically uniqueness-verifiable for the
--     property graph (the reference doc's required design).
--   - DYNAMIC LABEL projects edge_label as the per-row graph label

CREATE OR REPLACE PROPERTY GRAPH CorpusGraph
  NODE TABLES (
    Nodes
      KEY (organization_id, node_id, version)
      PROPERTIES ALL COLUMNS
      DYNAMIC LABEL (node_kind)
  )
  EDGE TABLES (
    Edges
      KEY (organization_id, edge_id)
      SOURCE KEY (organization_id, source_node_id, source_version)
        REFERENCES Nodes (organization_id, node_id, version)
      DESTINATION KEY (organization_id, target_node_id, target_version)
        REFERENCES Nodes (organization_id, node_id, version)
      PROPERTIES ALL COLUMNS
      DYNAMIC LABEL (edge_label)
  );

