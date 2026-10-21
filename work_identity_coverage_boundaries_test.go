package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/velenzaboc/velenza-trellis/internal/vertexai"
)

type coverageAdapterFake struct {
	queryFn     func(string, map[string]interface{}) (QueryResult, error)
	readNode    Node
	readErr     error
	commit      []AdmissionRecord
	commitErr   error
	byHash      Node
	byHashFound bool
	byHashErr   error
	byName      Node
	byNameFound bool
	byNameErr   error
	similar     []SimilarNode
	similarErr  error
}

func (*coverageAdapterFake) ID() string                                     { return "coverage" }
func (*coverageAdapterFake) Description() string                            { return "coverage" }
func (*coverageAdapterFake) Database() string                               { return "coverage" }
func (*coverageAdapterFake) Close() error                                   { return nil }
func (*coverageAdapterFake) CheckSchema(context.Context) error              { return nil }
func (*coverageAdapterFake) VerifyExperimentalSchema(context.Context) error { return nil }
func (a *coverageAdapterFake) CommitAdmission(context.Context, ...AdmissionPlan) ([]AdmissionRecord, error) {
	return a.commit, a.commitErr
}
func (a *coverageAdapterFake) ReadNode(context.Context, string, string, string) (Node, error) {
	return a.readNode, a.readErr
}
func (a *coverageAdapterFake) ReadNodeInProject(context.Context, string, string, string, string) (Node, error) {
	return a.readNode, a.readErr
}
func (*coverageAdapterFake) ReadEdge(context.Context, string, string) (Edge, error) {
	return Edge{}, errors.New("unused read edge")
}
func (*coverageAdapterFake) ListNodes(context.Context, ListFilter) ([]Node, error) { return nil, nil }
func (*coverageAdapterFake) Neighbors(context.Context, string, NeighborOpts) ([]Edge, error) {
	return nil, nil
}
func (*coverageAdapterFake) History(context.Context, string) ([]NodeVersion, error) { return nil, nil }
func (a *coverageAdapterFake) ExecuteReadOnlyQuery(_ context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
	if a.queryFn == nil {
		return QueryResult{}, nil
	}
	return a.queryFn(sql, params)
}
func (*coverageAdapterFake) SearchByEmbedding(context.Context, []float32, string, int, string) ([]Node, error) {
	return nil, nil
}
func (*coverageAdapterFake) TraverseClosure(context.Context, string, string, []string, int, int, string) ([]Node, error) {
	return nil, nil
}
func (a *coverageAdapterFake) FindByDefinitionHash(context.Context, string, string) (Node, bool, error) {
	return a.byHash, a.byHashFound, a.byHashErr
}
func (a *coverageAdapterFake) FindByCanonicalName(context.Context, string, string, string) (Node, bool, error) {
	return a.byName, a.byNameFound, a.byNameErr
}
func (a *coverageAdapterFake) FindSimilarByEmbedding(context.Context, string, []float32, float64, int) ([]SimilarNode, error) {
	return a.similar, a.similarErr
}

type coverageKindsFake struct{ err error }

func (k coverageKindsFake) Resolve(_ context.Context, name string) (NodeKindDescriptor, error) {
	if k.err != nil {
		return NodeKindDescriptor{}, k.err
	}
	return NewNodeKindDescriptor(map[string]interface{}{"name": name, "artifact_type": name, "substrate_id": "coverage"}), nil
}
func (coverageKindsFake) All(context.Context) ([]NodeKindDescriptor, error) { return nil, nil }

func schemaCoverageEnv(adapter SubstrateAdapter) *Env {
	return &Env{
		Config: SpecGraphConfig{
			Mode:                  ModeExperimentalBootstrap,
			ExperimentalAdmitter:  "TEST",
			DefaultOrganizationID: "org",
			DefaultProjectID:      "arbor",
		},
		Kinds:      coverageKindsFake{},
		Substrates: coverageRouter{all: []SubstrateAdapter{adapter}},
		Now:        func() time.Time { return time.Date(2026, 9, 12, 13, 0, 0, 0, time.UTC) },
	}
}

func forceSchemaQueryPath(t *testing.T) {
	t.Helper()
	previous := nativeTaskQueryAdapter
	nativeTaskQueryAdapter = func(context.Context, *Env) (*SpannerSubstrateAdapter, error) { return nil, nil }
	t.Cleanup(func() { nativeTaskQueryAdapter = previous })
}

func TestWorkIdentitySchemaQueryFailuresAndFilters(t *testing.T) {
	forceSchemaQueryPath(t)
	ctx := context.Background()
	boom := errors.New("query failed")

	adapter := &coverageAdapterFake{queryFn: func(sql string, params map[string]interface{}) (QueryResult, error) {
		switch {
		case strings.Contains(sql, "COUNT(DISTINCT"):
			return QueryResult{Rows: []map[string]interface{}{{"gap_task_count": "2", "distinct_gap_count": "1"}}}, nil
		case strings.Contains(sql, "COUNT(*) AS n"):
			return QueryResult{Rows: []map[string]interface{}{{"pillar": "P", "status": "blocked", "n": "3"}}}, nil
		case strings.Contains(sql, "FROM Edges"):
			return QueryResult{Rows: []map[string]interface{}{{"artifact_id": "a", "artifact_type": "commit", "artifact_ref": "sha", "thread": "thread", "note": "note", "created_at": "now"}}}, nil
		default:
			return QueryResult{Rows: []map[string]interface{}{{"node_id": "task", "title": "title", "pillar": "P", "level": "task", "status": "blocked", "owner": "owner", "is_gap": "true", "gap_label": "G", "note": "note"}}}, nil
		}
	}}
	env := schemaCoverageEnv(adapter)
	isGap := true
	tasks, err := handleTasks(ctx, env, tasksInput{Pillar: "P", Statuses: []string{"blocked"}, Owner: "owner", IsGap: &isGap, GapLabel: "G", Level: "task", Limit: 1})
	if err != nil || len(tasks.Tasks) != 1 || !tasks.Tasks[0].IsGap {
		t.Fatalf("filtered tasks = %+v, %v", tasks, err)
	}
	counts, err := handleTaskCounts(ctx, env, taskCountsInput{})
	if err != nil || counts.GapTaskCount != 2 || counts.DistinctGapCount != 1 {
		t.Fatalf("counts = %+v, %v", counts, err)
	}
	registryEnv := schemaCoverageEnv(adapter)
	registryEnv.TaskStatuses = realConfigTaskStatusRegistry(t)
	canonical, err := handleTaskCounts(ctx, registryEnv, taskCountsInput{})
	wantCanonical := []PillarCanonicalStatusCount{{Pillar: "P", CanonicalStatus: "blocked", Terminal: false, Count: 3}}
	if err != nil || !reflect.DeepEqual(canonical.ByPillarCanonicalStatus, wantCanonical) || len(canonical.UnrecognizedStatusLiterals) != 0 {
		t.Fatalf("schema_v1 canonical counts = %+v (unrecognized %q), %v; want %+v", canonical.ByPillarCanonicalStatus, canonical.UnrecognizedStatusLiterals, err, wantCanonical)
	}
	links, err := handleWorklinks(ctx, env, worklinksInput{TaskID: "task"})
	if err != nil || len(links.Worklinks) != 1 {
		t.Fatalf("worklinks = %+v, %v", links, err)
	}

	for _, tc := range []struct {
		name string
		run  func(*Env) error
	}{
		{"tasks", func(e *Env) error { _, err := handleTasks(ctx, e, tasksInput{}); return err }},
		{"counts group", func(e *Env) error { _, err := handleTaskCounts(ctx, e, taskCountsInput{}); return err }},
		{"worklinks", func(e *Env) error { _, err := handleWorklinks(ctx, e, worklinksInput{TaskID: "task"}); return err }},
	} {
		t.Run(tc.name+" query", func(t *testing.T) {
			e := schemaCoverageEnv(&coverageAdapterFake{queryFn: func(string, map[string]interface{}) (QueryResult, error) { return QueryResult{}, boom }})
			if err := tc.run(e); !errors.Is(err, boom) {
				t.Fatalf("error = %v, want query failure", err)
			}
		})
	}

	t.Run("counts gap query", func(t *testing.T) {
		calls := 0
		e := schemaCoverageEnv(&coverageAdapterFake{queryFn: func(string, map[string]interface{}) (QueryResult, error) {
			calls++
			if calls == 2 {
				return QueryResult{}, boom
			}
			return QueryResult{}, nil
		}})
		_, err := handleTaskCounts(ctx, e, taskCountsInput{})
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want gap query failure", err)
		}
	})

	for _, tc := range []struct {
		name string
		run  func(*Env) error
	}{
		{"tasks organization", func(e *Env) error { _, err := handleTasks(ctx, e, tasksInput{}); return err }},
		{"counts organization", func(e *Env) error { _, err := handleTaskCounts(ctx, e, taskCountsInput{}); return err }},
		{"worklinks organization", func(e *Env) error { _, err := handleWorklinks(ctx, e, worklinksInput{TaskID: "task"}); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := schemaCoverageEnv(adapter)
			e.Config.DefaultOrganizationID = ""
			requireCoverageError(t, tc.run(e), "default_organization_id required")
		})
	}
	if _, err := handleWorklinks(ctx, env, worklinksInput{}); err == nil {
		t.Fatal("schema worklinks without task id succeeded")
	}
	if _, err := handleProjectSnapshot(ctx, env, projectSnapshotInput{}); err == nil {
		t.Fatal("schema project snapshot succeeded")
	}
	if _, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{}); err == nil {
		t.Fatal("schema archive worklink succeeded")
	}
	if _, err := handleClaim(ctx, env, claimInput{}); err == nil {
		t.Fatal("schema claim succeeded")
	}
	if _, err := handleSupersedeWorklink(ctx, env, supersedeWorklinkInput{}); err == nil {
		t.Fatal("schema supersede worklink succeeded")
	}
}

func TestWorkIdentitySchemaMutationFailureBoundaries(t *testing.T) {
	forceSchemaQueryPath(t)
	ctx := context.Background()
	boom := errors.New("forced failure")
	base := schemaCoverageEnv(&coverageAdapterFake{})

	if _, err := handleAddWorklink(ctx, base, addWorklinkInput{}); err == nil {
		t.Fatal("incomplete worklink succeeded")
	}
	brokenKinds := schemaCoverageEnv(&coverageAdapterFake{})
	brokenKinds.Kinds = coverageKindsFake{err: boom}
	if _, err := handleAddWorklink(ctx, brokenKinds, addWorklinkInput{TaskID: "t", ArtifactType: "commit", ArtifactRef: "sha"}); !errors.Is(err, boom) {
		t.Fatalf("kind error = %v", err)
	}
	if _, err := handleSetStatus(ctx, base, setStatusInput{}); err == nil {
		t.Fatal("incomplete set status succeeded")
	}
	if _, err := handleReown(ctx, base, reownInput{}); err == nil {
		t.Fatal("incomplete reown succeeded")
	}

	readFailure := schemaCoverageEnv(&coverageAdapterFake{readErr: boom})
	if _, err := mergeTaskPayload(ctx, readFailure, "", "task", func(map[string]interface{}) {}); !errors.Is(err, boom) {
		t.Fatalf("merge read error = %v", err)
	}
	wrongKind := schemaCoverageEnv(&coverageAdapterFake{readNode: Node{NodeID: "artifact", NodeKind: "artifact"}})
	if _, err := mergeTaskPayload(ctx, wrongKind, "", "artifact", func(map[string]interface{}) {}); err == nil {
		t.Fatal("artifact payload merged through task mutation")
	}
	if _, err := schemaV1HandleReown(ctx, wrongKind, reownInput{TaskID: "artifact", Owner: "next", SessionID: "session", Repo: "repo"}, nil); err == nil {
		t.Fatal("artifact reowned through task mutation")
	}
	readFailure.Config.DefaultProjectID = ""
	if _, err := mergeTaskPayload(ctx, readFailure, "", "task", func(map[string]interface{}) {}); err == nil {
		t.Fatal("merge without project succeeded")
	}

	routerFailure := &Env{Substrates: coverageRouter{err: boom}}
	if _, err := soleTaskGraphAdapter(ctx, routerFailure); !errors.Is(err, boom) {
		t.Fatalf("sole adapter router error = %v", err)
	}
	if _, err := soleTaskGraphAdapter(ctx, &Env{Substrates: coverageRouter{}}); err == nil {
		t.Fatal("sole adapter accepted empty registry")
	}
}

func TestWorkIdentityDedupFailureAndSimilarityBoundaries(t *testing.T) {
	ctx := context.Background()
	policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 4}
	newEnv := func(adapter *coverageAdapterFake) *Env {
		return &Env{
			Config: SpecGraphConfig{DefaultOrganizationID: "org", Dedup: policy},
			Kinds:  coverageKindsFake{}, Substrates: coverageRouter{all: []SubstrateAdapter{adapter}},
			Embed: &vertexai.EmbedClient{},
		}
	}
	sub := ArtifactSubmission{ArtifactType: "task", NodeKind: "task", OrganizationID: "org", Contract: map[string]interface{}{"canonical_name": "candidate"}, Payload: map[string]interface{}{"title": "candidate"}}
	previous := embedForDedup
	embedForDedup = func(context.Context, *vertexai.EmbedClient, string) ([]float32, error) { return []float32{1, 2}, nil }
	t.Cleanup(func() { embedForDedup = previous })

	if _, err := runDedupGate(ctx, &Env{Config: SpecGraphConfig{Dedup: policy}, Kinds: coverageKindsFake{err: errors.New("kind")}, Substrates: coverageRouter{}}, sub, nil); err == nil {
		t.Fatal("dedup adapter resolution failure succeeded")
	}
	badJSON := sub
	badJSON.Payload = map[string]interface{}{"bad": make(chan int)}
	if _, err := runDedupGate(ctx, newEnv(&coverageAdapterFake{}), badJSON, nil); err == nil {
		t.Fatal("dedup definition hash accepted unsupported JSON")
	}
	for _, tc := range []struct {
		name string
		a    *coverageAdapterFake
		want string
	}{
		{"hash lookup", &coverageAdapterFake{byHashErr: errors.New("hash")}, "layer 1 lookup"},
		{"name lookup", &coverageAdapterFake{byNameErr: errors.New("name")}, "layer 2 lookup"},
		{"similar lookup", &coverageAdapterFake{similarErr: errors.New("similar")}, "layer 3 similarity lookup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runDedupGate(ctx, newEnv(tc.a), sub, nil)
			requireCoverageError(t, err, tc.want)
		})
	}
	embedForDedup = func(context.Context, *vertexai.EmbedClient, string) ([]float32, error) {
		return nil, errors.New("embed")
	}
	if _, err := runDedupGate(ctx, newEnv(&coverageAdapterFake{}), sub, nil); err == nil {
		t.Fatal("dedup embed failure succeeded")
	}
	embedForDedup = func(context.Context, *vertexai.EmbedClient, string) ([]float32, error) { return []float32{1, 2}, nil }
	hits := []SimilarNode{{Node: Node{NodeID: "near", NodeKind: "task", CurrentVersion: NodeVersion{Contract: map[string]interface{}{"canonical_name": "nearby"}}}, Distance: 0.05}}
	if _, err := runDedupGate(ctx, newEnv(&coverageAdapterFake{similar: hits}), sub, nil); !errors.Is(err, ErrPossibleDuplicate) {
		t.Fatalf("similarity error = %v", err)
	}
	if got, err := runDedupGate(ctx, newEnv(&coverageAdapterFake{similar: hits}), sub, []string{"near"}); err != nil || got != nil {
		t.Fatalf("acknowledged similarity = %+v, %v", got, err)
	}
	if !errorsAreDedupGate(PossibleDuplicateDetail{}) || !errorsAreDedupGate(DuplicateCanonicalNameDetail{}) || errorsAreDedupGate(errors.New("other")) {
		t.Fatal("dedup sentinel classifier disagrees with wrapped errors")
	}
	if got := filterUnacknowledged(nil, []string{"near"}); got != nil {
		t.Fatalf("empty similarity filter = %#v", got)
	}
}

func TestWorkIdentityErrorDetailsAndPureNativeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		err    error
		unwrap error
		parts  []string
	}{
		{DuplicateCanonicalNameDetail{NodeKind: "task", CanonicalName: "name", ExistingNodeID: "id", ExistingVersion: 2}, ErrDuplicateCanonicalName, []string{"task", "name", "id", "version 2"}},
		{SchemaPendingDetail{Step: "claim", Needs: "tuple", OwnerSpec: "owner"}, ErrSchemaPending, []string{"claim", "tuple", "owner"}},
		{SubstrateNotProvisionedDetail{Substrate: "native", NeededDDL: "Tasks"}, ErrSubstrateNotProvisioned, []string{"native", "Tasks"}},
	} {
		msg := tc.err.Error()
		if !errors.Is(tc.err, tc.unwrap) {
			t.Errorf("%T does not unwrap to %v", tc.err, tc.unwrap)
		}
		for _, part := range tc.parts {
			if !strings.Contains(msg, part) {
				t.Errorf("%T message %q missing %q", tc.err, msg, part)
			}
		}
	}

	ctx := context.Background()
	adapter := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ProjectColumn: "project_id", TaskTable: "Tasks", WorklinkTable: "TaskLinks", DepTable: "TaskDeps"}}
	isGap := true
	sql, params := nativeTasksQueryScoped(adapter, "arbor", tasksInput{Pillar: "P", Statuses: []string{"blocked"}, Owner: "owner", IsGap: &isGap, GapLabel: "G", Level: "task"}, 7, []string{"a"})
	for _, part := range []string{"pillar = @pillar", "status IN UNNEST", "owner = @owner", "is_gap = @is_gap", "gap_label = @gap_label", "layer = @level", "LIMIT 7"} {
		if !strings.Contains(sql, part) {
			t.Errorf("task query missing %q: %s", part, sql)
		}
	}
	if len(params) != 8 {
		t.Errorf("task query params = %#v", params)
	}
	if got := snapshotTaskScopePredicate("id", []string{}); got != " AND FALSE" {
		t.Errorf("empty scope predicate = %q", got)
	}
	if mode, err := validateSnapshotScope(projectSnapshotInput{Scope: "unknown"}); err == nil || mode != "" {
		t.Fatalf("unknown scope = %q, %v", mode, err)
	}
	if _, err := validateSnapshotScope(projectSnapshotInput{Scope: snapshotScopeProject, RootTaskID: "root"}); err == nil {
		t.Fatal("project scope accepted root id")
	}
	if _, err := validateSnapshotScope(projectSnapshotInput{Scope: snapshotScopeExecutionSubtree}); err == nil {
		t.Fatal("scoped snapshot accepted missing root")
	}
	if got := snapshotUniqueIDs([]string{"", "a", "a", "b"}); len(got) != 2 {
		t.Fatalf("unique ids = %#v", got)
	}
	if got := snapshotTotal(QueryResult{}); got != 0 {
		t.Fatalf("empty snapshot total = %d", got)
	}
	if _, err := nativeListNodes(ctx, adapter, ListFilter{}); err == nil {
		t.Fatal("native list accepted empty project")
	}
	if _, err := nativeSearchNodes(ctx, adapter, "arbor", "", "", 5); err == nil {
		t.Fatal("native search accepted empty query")
	}
	if got := nativeDependencyID("arbor", "source", "target"); !strings.HasPrefix(got, "DEP-") {
		t.Fatalf("dependency id = %q", got)
	}
	if got := nativeNullableBoolField(map[string]interface{}{"x": 7}, "x"); got != 7 {
		t.Fatalf("unexpected nullable bool representation = %#v", got)
	}
	if boolFromMap(nil, "x") || boolFromMap(map[string]interface{}{"x": 7}, "x") {
		t.Fatal("non-bool map value decoded as true")
	}
	if got := nullInt64FromMap(nil, "x"); got == nil {
		t.Fatal("nil integer map did not produce typed null")
	}
	if got := timeFromParam("not-time"); !got.IsZero() {
		t.Fatalf("non-time param = %v", got)
	}
	if _, err := projectIDOrDefault(nil, ""); err == nil {
		t.Fatal("empty project id without default succeeded")
	}
	if _, err := resolveNativeProjectID(nil, nil, ""); err == nil {
		t.Fatal("native project resolution without project succeeded")
	}
}

func TestWorkIdentitySnapshotMembershipFailClosedBoundaries(t *testing.T) {
	ctx := context.Background()
	adapter := snapshotTestAdapter()
	boom := errors.New("snapshot query failed")
	rootRow := rowsResult(map[string]interface{}{"id": "root"})
	childRow := rowsResult(map[string]interface{}{"id": "child"})

	t.Run("root query failure", func(t *testing.T) {
		_, err := discoverSnapshotMembership(ctx, adapter, func(context.Context, string, map[string]interface{}) (QueryResult, error) {
			return QueryResult{}, boom
		}, "arbor", snapshotScopeExecutionSubtree, "root", 10)
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want root query failure", err)
		}
	})

	t.Run("missing root", func(t *testing.T) {
		got, err := discoverSnapshotMembership(ctx, adapter, func(context.Context, string, map[string]interface{}) (QueryResult, error) {
			return QueryResult{}, nil
		}, "arbor", snapshotScopeExecutionSubtree, "root", 10)
		if err != nil || got.Complete || len(got.MissingTaskIDs) != 1 || got.MissingTaskIDs[0] != "root" {
			t.Fatalf("membership = %+v, %v", got, err)
		}
	})

	t.Run("execution cap before child read", func(t *testing.T) {
		got, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			if strings.Contains(sql, "root_task_id") {
				return rootRow, nil
			}
			if strings.Contains(sql, "SELECT id FROM") {
				return rootRow, nil
			}
			return QueryResult{}, nil
		}, "arbor", snapshotScopeExecutionSubtree, "root", 1)
		if err != nil || !got.Capped || got.Complete {
			t.Fatalf("membership = %+v, %v", got, err)
		}
	})

	t.Run("execution child query failure", func(t *testing.T) {
		_, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			if strings.Contains(sql, "root_task_id") {
				return rootRow, nil
			}
			return QueryResult{}, boom
		}, "arbor", snapshotScopeExecutionSubtree, "root", 10)
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want child query failure", err)
		}
	})

	t.Run("execution child result cap", func(t *testing.T) {
		got, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			switch {
			case strings.Contains(sql, "root_task_id"):
				return rootRow, nil
			case strings.Contains(sql, "parent_id IN UNNEST"):
				return rowsResult(map[string]interface{}{"id": "child"}, map[string]interface{}{"id": "extra"}), nil
			case strings.Contains(sql, "SELECT id FROM"):
				return rowsResult(map[string]interface{}{"id": "root"}, map[string]interface{}{"id": "child"}), nil
			default:
				return QueryResult{}, nil
			}
		}, "arbor", snapshotScopeExecutionSubtree, "root", 2)
		if err != nil || !got.Capped || got.Complete || len(got.TaskIDs) != 2 {
			t.Fatalf("membership = %+v, %v", got, err)
		}
	})

	t.Run("acceptance gate query failure", func(t *testing.T) {
		_, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			if strings.Contains(sql, "root_task_id") {
				return rootRow, nil
			}
			return QueryResult{}, boom
		}, "arbor", snapshotScopeAcceptanceDependencyClosure, "root", 10)
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want gate query failure", err)
		}
	})

	t.Run("acceptance gate result cap", func(t *testing.T) {
		got, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			switch {
			case strings.Contains(sql, "root_task_id"):
				return rootRow, nil
			case strings.Contains(sql, "parent_id IN UNNEST"):
				return rowsResult(map[string]interface{}{"id": "gate"}, map[string]interface{}{"id": "extra"}), nil
			case strings.Contains(sql, "SELECT id FROM"):
				return rowsResult(map[string]interface{}{"id": "root"}, map[string]interface{}{"id": "gate"}), nil
			default:
				return QueryResult{}, nil
			}
		}, "arbor", snapshotScopeAcceptanceDependencyClosure, "root", 2)
		if err != nil || !got.Capped || got.Complete || len(got.TaskIDs) != 2 {
			t.Fatalf("membership = %+v, %v", got, err)
		}
	})

	t.Run("acceptance dependency query failure", func(t *testing.T) {
		_, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			switch {
			case strings.Contains(sql, "root_task_id"):
				return rootRow, nil
			case strings.Contains(sql, "parent_id IN UNNEST"):
				return childRow, nil
			default:
				return QueryResult{}, boom
			}
		}, "arbor", snapshotScopeAcceptanceDependencyClosure, "root", 10)
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want dependency query failure", err)
		}
	})

	t.Run("acceptance dependency result cap", func(t *testing.T) {
		got, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			switch {
			case strings.Contains(sql, "root_task_id"):
				return rootRow, nil
			case strings.Contains(sql, "parent_id IN UNNEST"):
				return childRow, nil
			case strings.Contains(sql, "scope_frontier"):
				return rowsResult(
					map[string]interface{}{"depends_on_task_id": "dep-1"},
					map[string]interface{}{"depends_on_task_id": "dep-2"},
				), nil
			case strings.Contains(sql, "SELECT id FROM"):
				return rowsResult(map[string]interface{}{"id": "root"}, map[string]interface{}{"id": "child"}, map[string]interface{}{"id": "dep-1"}), nil
			default:
				return QueryResult{}, nil
			}
		}, "arbor", snapshotScopeAcceptanceDependencyClosure, "root", 3)
		if err != nil || !got.Capped || got.Complete || len(got.TaskIDs) != 3 {
			t.Fatalf("membership = %+v, %v", got, err)
		}
	})

	t.Run("existence query failure", func(t *testing.T) {
		_, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			switch {
			case strings.Contains(sql, "root_task_id"):
				return rootRow, nil
			case strings.Contains(sql, "parent_id IN UNNEST"):
				return QueryResult{}, nil
			default:
				return QueryResult{}, boom
			}
		}, "arbor", snapshotScopeExecutionSubtree, "root", 10)
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want existence query failure", err)
		}
	})

	t.Run("missing discovered task", func(t *testing.T) {
		childQueries := 0
		got, err := discoverSnapshotMembership(ctx, adapter, func(_ context.Context, sql string, _ map[string]interface{}) (QueryResult, error) {
			switch {
			case strings.Contains(sql, "root_task_id"):
				return rootRow, nil
			case strings.Contains(sql, "parent_id IN UNNEST"):
				childQueries++
				if childQueries > 1 {
					return QueryResult{}, nil
				}
				return childRow, nil
			case strings.Contains(sql, "SELECT id FROM"):
				return rootRow, nil
			default:
				return QueryResult{}, nil
			}
		}, "arbor", snapshotScopeExecutionSubtree, "root", 10)
		if err != nil || got.Complete || len(got.MissingTaskIDs) != 1 || got.MissingTaskIDs[0] != "child" {
			t.Fatalf("membership = %+v, %v", got, err)
		}
	})

	t.Run("snapshot reader and statement failures", func(t *testing.T) {
		reader := &fakeSnapshotReader{err: boom}
		if _, err := nativeHandleProjectSnapshot(ctx, snapshotTestEnv(), adapter, reader, projectSnapshotInput{}); !errors.Is(err, boom) {
			t.Fatalf("reader error = %v", err)
		}
		failing := &scriptedSnapshotReader{query: func(string, map[string]interface{}) (QueryResult, error) {
			return QueryResult{}, boom
		}}
		if _, err := nativeHandleProjectSnapshot(ctx, snapshotTestEnv(), adapter, failing, projectSnapshotInput{}); !errors.Is(err, boom) {
			t.Fatalf("statement error = %v", err)
		}
	})
}
