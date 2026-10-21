package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	dbadminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/spannertest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const nativeCoverageDatabase = "projects/coverage-project/instances/coverage-instance/databases/coverage-db"

var nativeCoverageSchema = []string{
	`CREATE TABLE Tasks (
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
		done_at TIMESTAMP,
	) PRIMARY KEY (project_id, id)`,
	`CREATE TABLE TaskLinks (
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
	) PRIMARY KEY (project_id, link_id)`,
	`CREATE TABLE TaskDeps (
		project_id STRING(64) NOT NULL,
		dep_id STRING(64) NOT NULL,
		task_id STRING(64) NOT NULL,
		depends_on_task_id STRING(64) NOT NULL,
		kind STRING(20),
		note STRING(MAX),
		created_at TIMESTAMP,
	) PRIMARY KEY (project_id, dep_id)`,
}

func newNativeCoverageEnv(t *testing.T) (*Env, *spanner.Client) {
	t.Helper()
	ctx := context.Background()
	srv, err := spannertest.NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start in-memory Spanner: %v", err)
	}
	srv.SetLogger(func(string, ...interface{}) {})
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Close()
		t.Fatalf("dial in-memory Spanner: %v", err)
	}
	client, err := spanner.NewClient(ctx, nativeCoverageDatabase, option.WithGRPCConn(conn))
	if err != nil {
		conn.Close()
		srv.Close()
		t.Fatalf("create Spanner client: %v", err)
	}
	admin, err := dbadmin.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		client.Close()
		conn.Close()
		srv.Close()
		t.Fatalf("create Spanner admin client: %v", err)
	}
	op, err := admin.UpdateDatabaseDdl(ctx, &dbadminpb.UpdateDatabaseDdlRequest{Database: nativeCoverageDatabase, Statements: nativeCoverageSchema})
	if err != nil {
		admin.Close()
		client.Close()
		conn.Close()
		srv.Close()
		t.Fatalf("apply native coverage schema: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		admin.Close()
		client.Close()
		conn.Close()
		srv.Close()
		t.Fatalf("wait for native coverage schema: %v", err)
	}
	t.Cleanup(func() {
		admin.Close()
		client.Close()
		conn.Close()
		srv.Close()
	})

	configPath, err := filepath.Abs("task-graph.post-cutover.config.json")
	if err != nil {
		t.Fatalf("resolve config path: %v", err)
	}
	t.Setenv("TASK_GRAPH_CONFIG", configPath)
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load post-cutover config: %v", err)
	}
	cfg.Substrates[0].SpannerDatabase = nativeCoverageDatabase
	taskStatuses, err := newTaskStatusRegistry(cfg.TaskStatuses)
	if err != nil {
		t.Fatalf("build task status registry: %v", err)
	}
	dependencyKinds, err := newDependencyKindRegistry(cfg.DependencyKinds)
	if err != nil {
		t.Fatalf("build dependency kind registry: %v", err)
	}
	artifactKinds, err := newArtifactKindRegistry(cfg.ArtifactKinds)
	if err != nil {
		t.Fatalf("build artifact kind registry: %v", err)
	}
	router := newRouter(cfg.GoogleCredentials, true, cfg.Substrates)
	all, err := router.All(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("resolve coverage substrate: adapters=%d err=%v", len(all), err)
	}
	all[0].(*SpannerSubstrateAdapter).client = client
	fixedNow := time.Date(2026, 9, 12, 12, 0, 0, 123, time.UTC)
	return &Env{
		Config:          cfg,
		Kinds:           newKindRegistry(cfg.NodeKinds),
		Edges:           newEdgeRegistry(cfg.EdgeLabels),
		Substrates:      router,
		Now:             func() time.Time { return fixedNow },
		TaskStatuses:    taskStatuses,
		DependencyKinds: dependencyKinds,
		ArtifactKinds:   artifactKinds,
	}, client
}

func coverageTaskInput(id string) createInput {
	return createInput{
		ProjectID: "arbor",
		NodeKind:  "task",
		StableID:  id,
		Contract: map[string]interface{}{
			"canonical_name": id,
			"task_ref":       id,
			"level":          "task",
		},
		Payload: map[string]interface{}{
			"title": id, "pillar": "TEST", "status": "not_started", "owner": "UNOWNED", "is_gap": false,
		},
	}
}

func requireCoverageError(t *testing.T, err error, contains string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), contains) {
		t.Fatalf("error = %v, want substring %q", err, contains)
	}
}

func nativeCoverageAdapter(t *testing.T, env *Env) *SpannerSubstrateAdapter {
	t.Helper()
	all, err := env.Substrates.All(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("resolve native coverage adapter: adapters=%d err=%v", len(all), err)
	}
	adapter, ok := all[0].(*SpannerSubstrateAdapter)
	if !ok {
		t.Fatalf("coverage adapter type = %T, want *SpannerSubstrateAdapter", all[0])
	}
	return adapter
}

func TestWorkIdentityNativeClaimAndHandoverFailClosed(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	ctx := context.Background()

	for _, id := range []string{"claimable", "occupied", "handover", "other"} {
		if _, err := nativeHandleCreate(ctx, env, coverageTaskInput(id)); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	badClaims := []struct {
		name string
		in   claimInput
		want string
	}{
		{"missing identity", claimInput{}, "required"},
		{"unowned claimant", claimInput{TaskID: "claimable", Owner: "UNOWNED", SessionID: "s", Repo: "task-graph"}, "required"},
		{"binding repo mismatch", claimInput{TaskID: "claimable", Owner: "worker", SessionID: "s", Repo: "task-graph", Binding: &ClaimBinding{Repo: "other"}}, "must match"},
		{"binding session mismatch", claimInput{TaskID: "claimable", Owner: "worker", SessionID: "s", Repo: "task-graph", Binding: &ClaimBinding{SessionID: "other"}}, "must match"},
		{"missing task", claimInput{TaskID: "absent", Owner: "worker", SessionID: "s", Repo: "task-graph"}, "not found"},
	}
	for _, tc := range badClaims {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeHandleClaim(ctx, env, tc.in)
			requireCoverageError(t, err, tc.want)
		})
	}

	claim := claimInput{
		ProjectID: "arbor", TaskID: "claimable", Owner: "worker-a", SessionID: "session-a", Repo: "task-graph",
		Binding: &ClaimBinding{Repo: "task-graph", SessionID: "session-a", Branch: "branch-a", WorktreePath: `D:\workspace\worktrees\a`, Host: "workstation-c", Account: "developer", Harness: "codex", Thread: "thread-a"},
	}
	claimed, err := nativeHandleClaim(ctx, env, claim)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if claimed.ClaimReceipt == nil || claimed.ClaimReceipt.RowsAffected != 1 || claimed.ClaimReceipt.Branch != "branch-a" {
		t.Fatalf("claim receipt = %+v", claimed.ClaimReceipt)
	}
	claim.Binding = nil
	retried, err := nativeHandleClaim(ctx, env, claim)
	if err != nil || retried.ClaimReceipt == nil || retried.ClaimReceipt.RowsAffected != 0 {
		t.Fatalf("idempotent claim = %+v, %v", retried.ClaimReceipt, err)
	}
	claim.Binding = &ClaimBinding{Branch: "replacement"}
	_, err = nativeHandleClaim(ctx, env, claim)
	requireCoverageError(t, err, "cannot re-attest")
	claim.Binding = nil
	claim.Owner = "worker-b"
	claim.SessionID = "session-b"
	_, err = nativeHandleClaim(ctx, env, claim)
	if !errors.Is(err, ErrTaskClaimCollision) {
		t.Fatalf("foreign claim error = %v, want ErrTaskClaimCollision", err)
	}

	if _, err := nativeHandleClaim(ctx, env, claimInput{TaskID: "occupied", Owner: "owner", SessionID: "session", Repo: "task-graph"}); err != nil {
		t.Fatalf("seed occupied task: %v", err)
	}
	badReowns := []struct {
		name string
		in   reownInput
		want string
	}{
		{"missing identity", reownInput{}, "required"},
		{"unowned keeps binding", reownInput{TaskID: "occupied", Owner: "UNOWNED", SessionID: "session"}, "must be empty"},
		{"owned missing binding", reownInput{TaskID: "occupied", Owner: "next"}, "required for owned"},
		{"missing task", reownInput{TaskID: "absent", Owner: "next", SessionID: "next-session", Repo: "task-graph"}, "not found"},
		{"stale owner", reownInput{TaskID: "occupied", Owner: "next", SessionID: "next-session", Repo: "task-graph", ExpectedOwner: "wrong", ExpectedSessionID: "session", ExpectedRepo: "task-graph"}, "claim collision"},
		{"stale session", reownInput{TaskID: "occupied", Owner: "next", SessionID: "next-session", Repo: "task-graph", ExpectedOwner: "owner", ExpectedSessionID: "wrong", ExpectedRepo: "task-graph"}, "claim collision"},
		{"stale repo", reownInput{TaskID: "occupied", Owner: "next", SessionID: "next-session", Repo: "task-graph", ExpectedOwner: "owner", ExpectedSessionID: "session", ExpectedRepo: "wrong"}, "claim collision"},
	}
	for _, tc := range badReowns {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeHandleReown(ctx, env, tc.in)
			requireCoverageError(t, err, tc.want)
		})
	}

	handed, err := nativeHandleReown(ctx, env, reownInput{TaskID: "occupied", Owner: "next", SessionID: "next-session", Repo: "task-graph", ExpectedOwner: "owner", ExpectedSessionID: "session", ExpectedRepo: "task-graph"})
	if err != nil || handed.NativeMutationReceipt == nil || !strings.Contains(handed.NativeMutationReceipt.Evidence, "expected_owner=owner") {
		t.Fatalf("handover result = %+v, %v", handed, err)
	}
	if _, err := nativeHandleReown(ctx, env, reownInput{TaskID: "occupied", Owner: "UNOWNED", ExpectedOwner: "next", ExpectedSessionID: "next-session", ExpectedRepo: "task-graph"}); err != nil {
		t.Fatalf("release task: %v", err)
	}
	for _, tc := range []struct {
		name string
		in   reownInput
	}{
		{name: "omitted expected tuple", in: reownInput{TaskID: "occupied", Owner: "late", SessionID: "late-session", Repo: "late-repo"}},
		{name: "stale owner only", in: reownInput{TaskID: "occupied", Owner: "late", SessionID: "late-session", Repo: "late-repo", ExpectedOwner: "next"}},
		{name: "stale session only", in: reownInput{TaskID: "occupied", Owner: "late", SessionID: "late-session", Repo: "late-repo", ExpectedSessionID: "next-session"}},
		{name: "stale repo only", in: reownInput{TaskID: "occupied", Owner: "late", SessionID: "late-session", Repo: "late-repo", ExpectedRepo: "task-graph"}},
		{name: "stale owned tuple", in: reownInput{TaskID: "occupied", Owner: "late", SessionID: "late-session", Repo: "late-repo", ExpectedOwner: "next", ExpectedSessionID: "next-session", ExpectedRepo: "task-graph"}},
	} {
		t.Run("released "+tc.name, func(t *testing.T) {
			if _, err := nativeHandleReown(ctx, env, tc.in); !errors.Is(err, ErrTaskClaimCollision) {
				t.Fatalf("released reown error = %v, want ErrTaskClaimCollision", err)
			}
		})
	}
	if _, err := nativeHandleClaim(ctx, env, claimInput{TaskID: "occupied", Owner: "late", SessionID: "late-session", Repo: "late-repo"}); err != nil {
		t.Fatalf("claim after guarded release: %v", err)
	}
}

func TestWorkIdentityNativeMutationBoundaries(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	ctx := context.Background()
	for _, id := range []string{"parent", "child", "task-a", "task-b"} {
		if _, err := nativeHandleCreate(ctx, env, coverageTaskInput(id)); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	createCases := []struct {
		name string
		in   createInput
		want string
	}{
		{"no stable identity", createInput{ProjectID: "arbor", NodeKind: "task", Contract: map[string]interface{}{}, Payload: map[string]interface{}{"status": "not_started"}}, "stable_id"},
		{"parent disagreement", createInput{ProjectID: "arbor", NodeKind: "task", StableID: "x", ParentRef: "a", Payload: map[string]interface{}{"parent_id": "b"}}, "does not match"},
		{"self parent", createInput{ProjectID: "arbor", NodeKind: "task", StableID: "x", ParentRef: "x", Payload: map[string]interface{}{}}, "own parent"},
		{"empty status", createInput{ProjectID: "arbor", NodeKind: "task", StableID: "x", Payload: map[string]interface{}{}}, "status required"},
		{"unknown status", createInput{ProjectID: "arbor", NodeKind: "task", StableID: "x", Payload: map[string]interface{}{"status": "novel"}}, "unknown task status"},
		{"missing parent", func() createInput { in := coverageTaskInput("orphan"); in.ParentRef = "absent"; return in }(), "not found"},
		{"unknown kind", createInput{ProjectID: "arbor", NodeKind: "mystery"}, "supports task/artifact"},
	}
	for _, tc := range createCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeHandleCreate(ctx, env, tc.in)
			requireCoverageError(t, err, tc.want)
		})
	}

	if _, err := nativeHandleCreate(ctx, env, func() createInput { in := coverageTaskInput("with-parent"); in.ParentRef = "parent"; return in }()); err != nil {
		t.Fatalf("create child with parent: %v", err)
	}
	_, err := nativeHandleCreate(ctx, env, coverageTaskInput("parent"))
	requireCoverageError(t, err, "insert task-plan task")

	badLinks := []struct {
		name string
		in   addWorklinkInput
		want string
	}{
		{"missing fields", addWorklinkInput{}, "required"},
		{"unknown artifact", addWorklinkInput{TaskID: "task-a", ArtifactType: "novel", ArtifactRef: "ref"}, "unknown artifact kind"},
		{"missing task", addWorklinkInput{TaskID: "absent", ArtifactID: "link-missing-task", ArtifactType: "commit", ArtifactRef: "ref"}, "not found"},
	}
	for _, tc := range badLinks {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeHandleAddWorklink(ctx, env, tc.in)
			requireCoverageError(t, err, tc.want)
		})
	}
	worklinkInput := addWorklinkInput{ProjectID: "arbor", TaskID: "task-a", ArtifactID: "link-a", ArtifactType: "commit", ArtifactRef: "sha", Thread: "thread", SessionID: "session", Repo: "task-graph", Branch: "branch", CommitSHA: "deadbeef", PRNumber: 7, PRState: "open", WorktreePath: `D:\worktree`, URL: "https://example.invalid/7"}
	if _, err := nativeHandleAddWorklink(ctx, env, worklinkInput); err != nil {
		t.Fatalf("add worklink: %v", err)
	}
	_, err = nativeHandleAddWorklink(ctx, env, worklinkInput)
	requireCoverageError(t, err, "insert task-plan worklink")

	archiveCases := []struct {
		name string
		in   archiveWorklinkInput
		want string
	}{
		{"missing fields", archiveWorklinkInput{}, "required"},
		{"missing task", archiveWorklinkInput{TaskID: "absent", LinkID: "link-a"}, "not found"},
	}
	for _, tc := range archiveCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeHandleArchiveWorklink(ctx, env, tc.in)
			requireCoverageError(t, err, tc.want)
		})
	}
	updateCases := []struct {
		name string
		id   string
		m    map[string]interface{}
		want string
	}{
		{"no fields", "task-a", nil, "no task fields"},
		{"unsupported fields", "task-a", map[string]interface{}{"fleet_updated_at": "x"}, "no supported"},
		{"empty status", "task-a", map[string]interface{}{"status": ""}, "non-empty string"},
		{"wrong status type", "task-a", map[string]interface{}{"status": 7}, "non-empty string"},
		{"unknown status", "task-a", map[string]interface{}{"status": "novel"}, "unknown task status"},
		{"missing task", "absent", map[string]interface{}{"title": "x"}, "not found"},
	}
	for _, tc := range updateCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nativeUpdateTaskFields(ctx, env, "arbor", tc.id, tc.m)
			requireCoverageError(t, err, tc.want)
		})
	}
	if _, err := nativeHandleSetStatus(ctx, env, setStatusInput{TaskID: "task-a", Status: "in_progress"}); err != nil {
		t.Fatalf("set status: %v", err)
	}
	if _, err := nativeHandleSetStatus(ctx, env, setStatusInput{}); err == nil {
		t.Fatal("missing status input succeeded")
	}
	if _, err := nativeLinkParent(ctx, env, "arbor", "parent", "child"); err != nil {
		t.Fatalf("link parent: %v", err)
	}
	if _, err := nativeLinkParent(ctx, env, "arbor", "absent", "child"); err == nil {
		t.Fatal("missing parent link succeeded")
	}
	if _, err := nativeLinkWorklinkToTask(ctx, env, "arbor", "link-a", "task-b"); err != nil {
		t.Fatalf("relink worklink: %v", err)
	}
	if _, err := nativeLinkWorklinkToTask(ctx, env, "arbor", "absent", "task-b"); err == nil {
		t.Fatal("missing worklink link succeeded")
	}

	for _, tc := range []linkInput{
		{},
		{ProjectID: "arbor", SourceID: "a", TargetID: "b", EdgeLabel: "mystery"},
		{ProjectID: "arbor", SourceID: "a", TargetID: "a", EdgeLabel: "gates"},
	} {
		if _, err := nativeHandleLink(ctx, env, tc); err == nil {
			t.Fatalf("invalid link succeeded: %+v", tc)
		}
	}
	dep, err := nativeHandleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: "task-a", TargetID: "task-b", EdgeLabel: "gates"})
	if err != nil || dep.NativeMutationReceipt == nil {
		t.Fatalf("create dependency = %+v, %v", dep, err)
	}
}

func TestWorkIdentityNativeReadAndTraversalBoundaries(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	ctx := context.Background()
	adapter := nativeCoverageAdapter(t, env)
	if _, err := nativeNeighbors(ctx, adapter, "child", NeighborOpts{}); err == nil {
		t.Fatal("neighbors without project succeeded")
	}
	for _, tc := range []struct{ project, start, direction string }{
		{"", "root", "forward"},
		{"arbor", "", "forward"},
	} {
		if _, err := nativeTraverseClosure(ctx, adapter, tc.project, tc.start, nil, 4, 20, tc.direction); err == nil {
			t.Fatalf("invalid traversal succeeded: %+v", tc)
		}
	}
}

type coverageRouter struct {
	all []SubstrateAdapter
	err error
}

func (r coverageRouter) Adapter(context.Context, NodeKindDescriptor) (SubstrateAdapter, error) {
	if r.err != nil {
		return nil, r.err
	}
	if len(r.all) == 0 {
		return nil, fmt.Errorf("no adapter")
	}
	return r.all[0], nil
}
func (r coverageRouter) All(context.Context) ([]SubstrateAdapter, error) { return r.all, r.err }
func (r coverageRouter) ByID(string) (SubstrateAdapter, error) {
	if r.err != nil || len(r.all) == 0 {
		return nil, r.err
	}
	return r.all[0], nil
}
func (coverageRouter) Close() error { return nil }

func TestWorkIdentityNativeAdapterFailures(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("router unavailable")
	env := &Env{Config: SpecGraphConfig{DefaultProjectID: "arbor"}, Substrates: coverageRouter{err: boom}, Now: time.Now}
	checks := []func() error{
		func() error { _, err := nativeHandleTasks(ctx, env, tasksInput{}); return err },
		func() error { _, err := nativeHandleTaskCounts(ctx, env, ""); return err },
		func() error { _, err := nativeHandleWorklinks(ctx, env, worklinksInput{TaskID: "x"}); return err },
		func() error {
			_, err := nativeHandleArchiveWorklink(ctx, env, archiveWorklinkInput{TaskID: "x", LinkID: "y"})
			return err
		},
		func() error {
			_, err := nativeHandleReown(ctx, env, reownInput{TaskID: "x", Owner: "UNOWNED"})
			return err
		},
		func() error {
			_, err := nativeHandleClaim(ctx, env, claimInput{TaskID: "x", Owner: "o", SessionID: "s", Repo: "r"})
			return err
		},
		func() error { _, err := nativeHandleCreate(ctx, env, coverageTaskInput("x")); return err },
		func() error { _, err := nativeHandleUpdate(ctx, env, updateInput{NodeID: "x"}); return err },
		func() error {
			_, err := nativeHandleLink(ctx, env, linkInput{SourceID: "x", TargetID: "y", EdgeLabel: "parent_of"})
			return err
		},
		func() error {
			_, err := nativeUpdateTaskFields(ctx, env, "", "x", map[string]interface{}{"title": "x"})
			return err
		},
		func() error {
			_, err := nativeUpdateWorklinkFields(ctx, env, "", "x", nil, map[string]interface{}{"note": "x"})
			return err
		},
		func() error { _, err := nativeHandleUnlink(ctx, env, "x"); return err },
	}
	for i, check := range checks {
		if err := check(); !errors.Is(err, boom) {
			t.Errorf("check %d error = %v, want router failure", i, err)
		}
	}

	env.Substrates = coverageRouter{}
	if _, err := soleNativeFleetPlanAdapter(ctx, env); err == nil {
		t.Fatal("empty adapter set succeeded")
	}
	env.Substrates = coverageRouter{all: []SubstrateAdapter{&fakeAdapter{}}}
	if _, err := soleNativeFleetPlanAdapter(ctx, env); err == nil {
		t.Fatal("non-Spanner native adapter succeeded")
	}
}

// ---------------- task_graph_supersede_worklink ----------------

const (
	supersedeTask        = "succ-task"
	supersedePredecessor = "BND-pred"
	supersedeSuccessorID = "BND-succ"
)

type supersedeFixture struct {
	env    *Env
	client *spanner.Client
	in     supersedeWorklinkInput
}

// newSupersedeFixture seeds one owned task with exactly one active canonical
// worktree binding: the valid predecessor a post-merge successor replaces.
func newSupersedeFixture(t *testing.T) supersedeFixture {
	t.Helper()
	env, client := newNativeCoverageEnv(t)
	ctx := context.Background()
	for _, id := range []string{supersedeTask, "other-task"} {
		if _, err := nativeHandleCreate(ctx, env, coverageTaskInput(id)); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	if _, err := nativeHandleClaim(ctx, env, claimInput{ProjectID: "arbor", TaskID: supersedeTask, Owner: "owner-a", SessionID: "sess-a", Repo: "example-infra"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := nativeHandleAddWorklink(ctx, env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: supersedePredecessor, ArtifactType: "worktree", ArtifactRef: "example-infra:branch-old:/wt/old", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "branch-old", WorktreePath: "/wt/old"}); err != nil {
		t.Fatalf("seed predecessor: %v", err)
	}
	return supersedeFixture{env: env, client: client, in: supersedeWorklinkInput{
		ProjectID: "arbor", TaskID: supersedeTask, ExpectedOwner: "owner-a", ExpectedSessionID: "sess-a", PredecessorLinkID: supersedePredecessor,
		ExpectedPredecessor: supersedePredecessorIdentity{ArtifactType: "worktree", ArtifactStatus: "recorded", Repo: "example-infra", Branch: "branch-old", WorktreePath: "/wt/old", Thread: "thread-a", SessionID: "sess-a"},
		Successor:           supersedeSuccessor{LinkID: supersedeSuccessorID, ArtifactType: "worktree", ArtifactRef: "example-infra:branch-new:/wt/new", Repo: "example-infra", Branch: "branch-new", WorktreePath: "/wt/new", Thread: "thread-a", SessionID: "sess-a"},
	}}
}

type storedLink struct {
	Found                                                                    bool
	LinkID, TaskID, ArtifactType, ArtifactRef, Thread, Note, SessionID, Repo string
	Branch, WorktreePath, Status                                             string
}

func readStoredLink(t *testing.T, client *spanner.Client, linkID string) storedLink {
	t.Helper()
	iter := client.Single().Query(context.Background(), spanner.Statement{
		SQL: `SELECT link_id, task_id, artifact_type, artifact_ref, thread, note, session_id, repo, branch, worktree_path, artifact_status
			FROM TaskLinks WHERE project_id = 'arbor' AND link_id = @id`,
		Params: map[string]interface{}{"id": linkID},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		return storedLink{}
	}
	var thread, note, session, repo, branch, worktree, status spanner.NullString
	out := storedLink{Found: true}
	if err := row.Columns(&out.LinkID, &out.TaskID, &out.ArtifactType, &out.ArtifactRef, &thread, &note, &session, &repo, &branch, &worktree, &status); err != nil {
		t.Fatalf("decode %s: %v", linkID, err)
	}
	out.Thread, out.Note, out.SessionID, out.Repo, out.Branch, out.WorktreePath = nullStringValue(thread), nullStringValue(note), nullStringValue(session), nullStringValue(repo), nullStringValue(branch), nullStringValue(worktree)
	out.Status = nullStringValue(status)
	if out.Status == "" {
		out.Status = "recorded"
	}
	return out
}

func setStoredTaskField(t *testing.T, client *spanner.Client, taskID, column, value string) {
	t.Helper()
	_, err := client.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{SQL: `UPDATE Tasks SET ` + column + ` = @value WHERE project_id = 'arbor' AND id = @id`, Params: map[string]interface{}{"value": value, "id": taskID}})
		return err
	})
	if err != nil {
		t.Fatalf("set task %s %s: %v", taskID, column, err)
	}
}

func setStoredStatus(t *testing.T, client *spanner.Client, linkID, status string) {
	t.Helper()
	_, err := client.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{SQL: `UPDATE TaskLinks SET artifact_status = @status WHERE project_id = 'arbor' AND link_id = @id`, Params: map[string]interface{}{"status": status, "id": linkID}})
		return err
	})
	if err != nil {
		t.Fatalf("set status %s: %v", linkID, err)
	}
}

// assertNoTransition proves a refused request left the predecessor active and
// installed no successor, so a failed transition never loses the binding.
func assertNoTransition(t *testing.T, f supersedeFixture, label string) {
	t.Helper()
	pred := readStoredLink(t, f.client, supersedePredecessor)
	if !pred.Found || pred.Status != "recorded" || pred.Branch != "branch-old" || pred.WorktreePath != "/wt/old" {
		t.Fatalf("%s: predecessor changed: %+v", label, pred)
	}
	if succ := readStoredLink(t, f.client, supersedeSuccessorID); succ.Found && succ.Note != "unrelated" {
		t.Fatalf("%s: successor was installed: %+v", label, succ)
	}
}

func TestSupersedeWorklinkCommitsPredecessorRetirementAndSuccessorTogether(t *testing.T) {
	f := newSupersedeFixture(t)
	ctx := context.Background()
	out, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in)
	if err != nil {
		t.Fatalf("supersede: %v", err)
	}
	native, receipt := out.NativeMutationReceipt, out.SupersedeReceipt
	if native == nil || native.Operation != "supersede_worklink" || native.ReceiptType != "native_mutation_receipt" || native.ProjectID != "arbor" || native.ArtifactID != supersedeSuccessorID || native.RowsAffected != 2 {
		t.Fatalf("native receipt = %+v", native)
	}
	if receipt == nil || receipt.ReceiptKind != "worklink_supersede" || receipt.Issuer != "task-graph" || receipt.ProjectID != "arbor" || receipt.TaskID != supersedeTask ||
		receipt.PredecessorLinkID != supersedePredecessor || receipt.SuccessorLinkID != supersedeSuccessorID || receipt.PredecessorStatus != "superseded" || receipt.Idempotent || receipt.RowsAffected != 2 {
		t.Fatalf("supersede receipt = %+v", receipt)
	}

	pred := readStoredLink(t, f.client, supersedePredecessor)
	if pred.Status != "superseded" || pred.TaskID != supersedeTask || pred.ArtifactType != "worktree" || pred.Repo != "example-infra" || pred.Branch != "branch-old" || pred.WorktreePath != "/wt/old" || pred.Thread != "thread-a" || pred.SessionID != "sess-a" || pred.ArtifactRef != "example-infra:branch-old:/wt/old" {
		t.Fatalf("predecessor must be retained as history with every coordinate intact: %+v", pred)
	}
	succ := readStoredLink(t, f.client, supersedeSuccessorID)
	want := storedLink{Found: true, LinkID: supersedeSuccessorID, TaskID: supersedeTask, ArtifactType: "worktree", ArtifactRef: "example-infra:branch-new:/wt/new", Thread: "thread-a", Note: "supersedes=" + supersedePredecessor, SessionID: "sess-a", Repo: "example-infra", Branch: "branch-new", WorktreePath: "/wt/new", Status: "recorded"}
	if succ != want {
		t.Fatalf("successor = %+v, want %+v", succ, want)
	}

}

func TestSupersedeWorklinkExactRetryIsIdempotentAndAnyDifferenceFailsClosed(t *testing.T) {
	f := newSupersedeFixture(t)
	ctx := context.Background()
	if _, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil {
		t.Fatalf("first supersede: %v", err)
	}
	before := readStoredLink(t, f.client, supersedeSuccessorID)
	retry, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in)
	if err != nil || retry.NativeMutationReceipt == nil || retry.NativeMutationReceipt.RowsAffected != 0 || retry.SupersedeReceipt == nil || !retry.SupersedeReceipt.Idempotent || retry.SupersedeReceipt.RowsAffected != 0 ||
		retry.SupersedeReceipt.PredecessorLinkID != supersedePredecessor || retry.SupersedeReceipt.SuccessorLinkID != supersedeSuccessorID {
		t.Fatalf("identical retry = %+v, %v", retry, err)
	}
	if after := readStoredLink(t, f.client, supersedeSuccessorID); after != before {
		t.Fatalf("retry mutated the successor: %+v -> %+v", before, after)
	}

	mutations := map[string]func(*supersedeWorklinkInput){
		"successor branch":     func(in *supersedeWorklinkInput) { in.Successor.Branch = "branch-other" },
		"successor worktree":   func(in *supersedeWorklinkInput) { in.Successor.WorktreePath = "/wt/other" },
		"successor ref":        func(in *supersedeWorklinkInput) { in.Successor.ArtifactRef = "other-ref" },
		"predecessor branch":   func(in *supersedeWorklinkInput) { in.ExpectedPredecessor.Branch = "branch-other" },
		"predecessor worktree": func(in *supersedeWorklinkInput) { in.ExpectedPredecessor.WorktreePath = "/wt/other" },
		"predecessor thread": func(in *supersedeWorklinkInput) {
			in.ExpectedPredecessor.Thread = "thread-b"
			in.Successor.Thread = "thread-b"
		},
		"predecessor session": func(in *supersedeWorklinkInput) {
			in.ExpectedPredecessor.SessionID = "sess-b"
			in.Successor.SessionID = "sess-b"
		},
		"owner":                  func(in *supersedeWorklinkInput) { in.ExpectedOwner = "owner-b" },
		"task session":           func(in *supersedeWorklinkInput) { in.ExpectedSessionID = "sess-b" },
		"different successor id": func(in *supersedeWorklinkInput) { in.Successor.LinkID = "BND-other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			in := f.in
			mutate(&in)
			if out, err := nativeHandleSupersedeWorklink(ctx, f.env, in); err == nil {
				t.Fatalf("differing retry succeeded: %+v", out)
			}
			if after := readStoredLink(t, f.client, supersedeSuccessorID); after != before {
				t.Fatalf("failed retry mutated the successor: %+v -> %+v", before, after)
			}
			if other := readStoredLink(t, f.client, "BND-other"); other.Found {
				t.Fatalf("a different successor was installed: %+v", other)
			}
		})
	}
	different := f.in
	different.Successor.LinkID = "BND-other"
	different.Successor.Branch = "branch-other"
	different.Successor.WorktreePath = "/wt/other"
	different.Successor.ArtifactRef = "other-ref"
	_, err = nativeHandleSupersedeWorklink(ctx, f.env, different)
	var conflict WorklinkSupersedeConflict
	if !errors.Is(err, ErrWorklinkSupersedeConflict) || !errors.As(err, &conflict) || conflict.WinnerLinkID != supersedeSuccessorID || conflict.PredecessorLinkID != supersedePredecessor {
		t.Fatalf("a different successor after the transition = %v, want a conflict naming winner %s", err, supersedeSuccessorID)
	}
}

// TestSupersedeWorklinkLostRaceReportsTheCommittedWinner interleaves a competing
// transition between this transaction's read and its retirement write. The
// status compare-and-set refuses the stale write and the fresh pass names the
// committed winner, so two successors can never both be installed.
func TestSupersedeWorklinkLostRaceReportsTheCommittedWinner(t *testing.T) {
	f := newSupersedeFixture(t)
	ctx := context.Background()
	interleaved := false
	nativeSupersedeFault = func(stage string) error {
		if stage != "before_retire" || interleaved {
			return nil
		}
		interleaved = true
		winner := f.in
		winner.Successor.LinkID = "BND-winner"
		winner.Successor.Branch = "branch-winner"
		winner.Successor.WorktreePath = "/wt/winner"
		winner.Successor.ArtifactRef = "winner-ref"
		nativeSupersedeFault = nil
		defer func() { nativeSupersedeFault = nil }()
		if _, err := nativeHandleSupersedeWorklink(ctx, f.env, winner); err != nil {
			t.Errorf("competing transition: %v", err)
		}
		return nil
	}
	t.Cleanup(func() { nativeSupersedeFault = nil })
	_, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in)
	var conflict WorklinkSupersedeConflict
	if !interleaved || !errors.Is(err, ErrWorklinkSupersedeConflict) || !errors.As(err, &conflict) || conflict.WinnerLinkID != "BND-winner" {
		t.Fatalf("interleaved=%v error = %v, want a conflict naming BND-winner", interleaved, err)
	}
	if succ := readStoredLink(t, f.client, supersedeSuccessorID); succ.Found {
		t.Fatalf("the losing successor was installed: %+v", succ)
	}
	if winner := readStoredLink(t, f.client, "BND-winner"); !winner.Found || winner.Note != "supersedes="+supersedePredecessor {
		t.Fatalf("winner = %+v", winner)
	}
	if pred := readStoredLink(t, f.client, supersedePredecessor); pred.Status != "superseded" || pred.Branch != "branch-old" {
		t.Fatalf("predecessor = %+v", pred)
	}
}

func TestSupersedeWorklinkHostileRequestsFailClosedWithoutLosingTheBinding(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		setup func(t *testing.T, f supersedeFixture)
		edit  func(in *supersedeWorklinkInput)
		want  string
	}{
		{name: "wrong project", edit: func(in *supersedeWorklinkInput) { in.ProjectID = "moonshot" }, want: "not found"},
		{name: "missing task", edit: func(in *supersedeWorklinkInput) { in.TaskID = "absent" }, want: "not found"},
		{
			name: "predecessor belongs to another task",
			setup: func(t *testing.T, f supersedeFixture) {
				if _, err := nativeHandleClaim(ctx, f.env, claimInput{ProjectID: "arbor", TaskID: "other-task", Owner: "owner-a", SessionID: "sess-a", Repo: "example-infra"}); err != nil {
					t.Fatalf("claim other task: %v", err)
				}
			},
			edit: func(in *supersedeWorklinkInput) { in.TaskID = "other-task" },
			want: "belongs to task",
		},
		{name: "task owner changed after the guard read it", setup: func(t *testing.T, f supersedeFixture) {
			setStoredTaskField(t, f.client, supersedeTask, "owner", "owner-z")
		}, want: "owner"},
		{name: "task session changed after the guard read it", setup: func(t *testing.T, f supersedeFixture) {
			setStoredTaskField(t, f.client, supersedeTask, "session_id", "sess-z")
		}, want: "session"},
		{name: "wrong owner", edit: func(in *supersedeWorklinkInput) { in.ExpectedOwner = "owner-b" }, want: "owner"},
		{name: "wrong task session", edit: func(in *supersedeWorklinkInput) { in.ExpectedSessionID = "sess-b" }, want: "session"},
		{name: "missing predecessor", edit: func(in *supersedeWorklinkInput) { in.PredecessorLinkID = "BND-absent" }, want: "not found"},
		{name: "predecessor type", edit: func(in *supersedeWorklinkInput) { in.ExpectedPredecessor.ArtifactType = "branch" }, want: "worktree"},
		{name: "predecessor status", edit: func(in *supersedeWorklinkInput) { in.ExpectedPredecessor.ArtifactStatus = "archived" }, want: "recorded"},
		{name: "predecessor repo", edit: func(in *supersedeWorklinkInput) {
			in.ExpectedPredecessor.Repo = "task-graph"
			in.Successor.Repo = "task-graph"
		}, want: "predecessor"},
		{name: "predecessor branch", edit: func(in *supersedeWorklinkInput) { in.ExpectedPredecessor.Branch = "branch-x" }, want: "predecessor"},
		{name: "predecessor worktree", edit: func(in *supersedeWorklinkInput) { in.ExpectedPredecessor.WorktreePath = "/wt/x" }, want: "predecessor"},
		{name: "predecessor thread", edit: func(in *supersedeWorklinkInput) {
			in.ExpectedPredecessor.Thread = "thread-x"
			in.Successor.Thread = "thread-x"
		}, want: "predecessor"},
		{name: "predecessor session", edit: func(in *supersedeWorklinkInput) {
			in.ExpectedPredecessor.SessionID = "sess-x"
			in.Successor.SessionID = "sess-x"
		}, want: "predecessor"},
		{name: "successor repo differs", edit: func(in *supersedeWorklinkInput) { in.Successor.Repo = "task-graph" }, want: "repository"},
		{name: "successor thread differs", edit: func(in *supersedeWorklinkInput) { in.Successor.Thread = "thread-x" }, want: "thread"},
		{name: "successor session differs", edit: func(in *supersedeWorklinkInput) { in.Successor.SessionID = "sess-x" }, want: "session"},
		{name: "successor type", edit: func(in *supersedeWorklinkInput) { in.Successor.ArtifactType = "binding" }, want: "worktree"},
		{name: "successor is predecessor id", edit: func(in *supersedeWorklinkInput) { in.Successor.LinkID = supersedePredecessor }, want: "differ"},
		{name: "successor keeps predecessor coordinates", edit: func(in *supersedeWorklinkInput) {
			in.Successor.Branch = "branch-old"
			in.Successor.WorktreePath = "/wt/old"
		}, want: "differ"},
		{name: "missing field", edit: func(in *supersedeWorklinkInput) { in.Successor.WorktreePath = "" }, want: "required"},
		{
			name:  "predecessor already archived",
			setup: func(t *testing.T, f supersedeFixture) { setStoredStatus(t, f.client, supersedePredecessor, "archived") },
			want:  "not active",
		},
		{
			name: "second active binding for the task and repository",
			setup: func(t *testing.T, f supersedeFixture) {
				if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: "BND-dup", ArtifactType: "worktree", ArtifactRef: "dup", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "branch-dup", WorktreePath: "/wt/dup"}); err != nil {
					t.Fatalf("seed duplicate: %v", err)
				}
			},
			want: "active binding",
		},
		{
			name: "noncanonical binding type is also an active binding",
			setup: func(t *testing.T, f supersedeFixture) {
				if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: "BND-alias", ArtifactType: "binding", ArtifactRef: "alias", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "branch-alias"}); err != nil {
					t.Fatalf("seed alias: %v", err)
				}
			},
			want: "active binding",
		},
		{
			name: "successor id collides with a row of another task",
			setup: func(t *testing.T, f supersedeFixture) {
				if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: "other-task", ArtifactID: supersedeSuccessorID, ArtifactType: "commit", ArtifactRef: "sha", Note: "unrelated"}); err != nil {
					t.Fatalf("seed foreign row: %v", err)
				}
			},
			want: "collides",
		},
		{
			name: "successor id collides with different content",
			setup: func(t *testing.T, f supersedeFixture) {
				if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: supersedeSuccessorID, ArtifactType: "commit", ArtifactRef: "sha", Note: "unrelated"}); err != nil {
					t.Fatalf("seed collision: %v", err)
				}
			},
			want: "collides",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSupersedeFixture(t)
			if tc.setup != nil {
				tc.setup(t, f)
			}
			in := f.in
			if tc.edit != nil {
				tc.edit(&in)
			}
			out, err := nativeHandleSupersedeWorklink(ctx, f.env, in)
			if err == nil {
				t.Fatalf("hostile request succeeded: %+v", out)
			}
			requireCoverageError(t, err, tc.want)
			if tc.name == "predecessor already archived" {
				if pred := readStoredLink(t, f.client, supersedePredecessor); pred.Status != "archived" {
					t.Fatalf("archived predecessor changed: %+v", pred)
				}
				return
			}
			assertNoTransition(t, f, tc.name)
		})
	}
}

func TestSupersedeWorklinkInputBoundaries(t *testing.T) {
	f := newSupersedeFixture(t)
	for name, mutate := range map[string]func(*supersedeWorklinkInput){
		"task":             func(in *supersedeWorklinkInput) { in.TaskID = " " },
		"owner":            func(in *supersedeWorklinkInput) { in.ExpectedOwner = "" },
		"unowned owner":    func(in *supersedeWorklinkInput) { in.ExpectedOwner = "UNOWNED" },
		"task session":     func(in *supersedeWorklinkInput) { in.ExpectedSessionID = "" },
		"predecessor id":   func(in *supersedeWorklinkInput) { in.PredecessorLinkID = "" },
		"successor id":     func(in *supersedeWorklinkInput) { in.Successor.LinkID = "" },
		"successor ref":    func(in *supersedeWorklinkInput) { in.Successor.ArtifactRef = "" },
		"successor thread": func(in *supersedeWorklinkInput) { in.Successor.Thread = "" },
	} {
		t.Run(name, func(t *testing.T) {
			in := f.in
			mutate(&in)
			if err := validateSupersedeWorklinkInput(in); err == nil {
				t.Fatalf("invalid input accepted: %+v", in)
			}
		})
	}
	if err := validateSupersedeWorklinkInput(f.in); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
}

// The emulator-backed TestIntegrationSupersedeWorklinkReadSurfaces executes the
// read queries; the in-memory Spanner cannot run CAST(created_at AS STRING), so
// this pins the shared predicate and the node projection without a store.
func TestSupersededWorklinksAreHistoryNotActiveBindings(t *testing.T) {
	for _, status := range []string{"archived", "superseded"} {
		if !strings.Contains(nativeActiveWorklinkPredicate, "'"+status+"'") {
			t.Fatalf("active worklink predicate %q must exclude %s", nativeActiveWorklinkPredicate, status)
		}
	}
	if !strings.Contains(nativeActiveWorklinkPredicate, "artifact_status IS NULL") {
		t.Fatalf("active worklink predicate %q must keep unstatused rows", nativeActiveWorklinkPredicate)
	}
	env, _ := newNativeCoverageEnv(t)
	adapter := nativeCoverageAdapter(t, env)
	pageSQL, _ := nativeSnapshotWorklinksPageQueryScoped(adapter, "arbor", 10, nil, nil)
	totalSQL, _ := nativeSnapshotTotalsQueryScoped(adapter, "arbor", adapter.Descriptor.WorklinkTable, nativeActiveWorklinkPredicate, nil)
	for name, sql := range map[string]string{"snapshot page": pageSQL, "snapshot total": totalSQL} {
		if !strings.Contains(sql, nativeActiveWorklinkPredicate) {
			t.Fatalf("%s query does not use the shared active predicate: %s", name, sql)
		}
	}
	for status, want := range map[string]string{"superseded": "superseded", "archived": "archived", "recorded": "active", "": "active"} {
		node := nativeArtifactNodeFromRow(map[string]interface{}{"link_id": "L", "artifact_status": status})
		if node.Status != want || node.CurrentVersion.Status != want {
			t.Fatalf("artifact_status %q projects node status %q/%q, want %q", status, node.Status, node.CurrentVersion.Status, want)
		}
	}
	for _, kind := range []string{"worktree", "binding", "branch"} {
		if !nativeBindingArtifactTypes[kind] {
			t.Fatalf("%s must be a binding artifact type", kind)
		}
	}
	for _, status := range []string{"archived", "superseded", "released", "closed"} {
		if nativeWorklinkIsActiveBinding(nativeWorklinkState{ArtifactType: "worktree", Status: status}) {
			t.Fatalf("status %s must not be an active binding", status)
		}
	}
	if !nativeWorklinkIsActiveBinding(nativeWorklinkState{ArtifactType: "Worktree", Status: "recorded"}) || nativeWorklinkIsActiveBinding(nativeWorklinkState{ArtifactType: "commit", Status: "recorded"}) {
		t.Fatal("binding classification must be case-insensitive on type and limited to binding types")
	}
}

func TestSupersedeWorklinkRetryFailsClosedWhenAnotherBindingBecameActive(t *testing.T) {
	f := newSupersedeFixture(t)
	ctx := context.Background()
	if _, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil {
		t.Fatalf("first supersede: %v", err)
	}
	if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: "BND-extra", ArtifactType: "worktree", ArtifactRef: "extra", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "branch-extra", WorktreePath: "/wt/extra"}); err != nil {
		t.Fatalf("seed second active binding: %v", err)
	}
	// A retry is only an idempotent no-op while the successor is still the single
	// active binding; otherwise it must not vouch for the transition.
	if out, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err == nil {
		t.Fatalf("retry beside a second active binding succeeded: %+v", out)
	}
}

// ---------------- generic update cannot mint or reopen the superseded lifecycle ----------------
//
// task_graph_update is a generic field update. superseded is reachable only
// through task_graph_supersede_worklink, whose transaction fences the owner and
// session, compares the predecessor and installs the successor. The unit tests
// call the production update function against the in-memory store; the
// emulator suite drives the public handler.

func TestGenericUpdateCannotMintSupersededLifecycle(t *testing.T) {
	ctx := context.Background()
	for _, status := range []string{"superseded", "Superseded", " SUPERSEDED "} {
		t.Run(status, func(t *testing.T) {
			f := newSupersedeFixture(t)
			before := readStoredLink(t, f.client, supersedePredecessor)
			out, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedePredecessor, nil, map[string]interface{}{"artifact_status": status, "note": "changed alongside"})
			if err == nil {
				t.Fatalf("generic update into %q succeeded: %+v", status, out)
			}
			requireCoverageError(t, err, "task_graph_supersede_worklink")
			if after := readStoredLink(t, f.client, supersedePredecessor); after != before {
				t.Fatalf("a refused update wrote: %+v -> %+v", before, after)
			}
			// The typed transition is still the way to retire it.
			if _, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil {
				t.Fatalf("typed supersede after the refusal: %v", err)
			}
		})
	}
	// The pure builder never emits the lifecycle change either.
	assignments, _ := nativeBuildWorklinkUpdate(map[string]interface{}{"artifact_status": "superseded"})
	for _, assignment := range assignments {
		if strings.HasPrefix(assignment, "artifact_status =") {
			t.Fatalf("builder emitted %q for artifact_status=superseded", assignment)
		}
	}
	if assignments, _ := nativeBuildWorklinkUpdate(map[string]interface{}{"artifact_status": "archived"}); len(assignments) != 1 {
		t.Fatalf("archived stays UPDATE-able, got %v", assignments)
	}
}

func TestGenericUpdateCannotReviveOrRewriteSupersededHistory(t *testing.T) {
	f := newSupersedeFixture(t)
	ctx := context.Background()
	if _, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil {
		t.Fatalf("supersede: %v", err)
	}
	pred := readStoredLink(t, f.client, supersedePredecessor)
	succ := readStoredLink(t, f.client, supersedeSuccessorID)
	for name, payload := range map[string]map[string]interface{}{
		"revive as recorded":     {"artifact_status": "recorded"},
		"revive with other data": {"artifact_status": "recorded", "note": "revived"},
		"archive the history":    {"artifact_status": "archived"},
		"rewrite a coordinate":   {"branch": "branch-rewritten"},
		"rewrite the note":       {"note": "rewritten"},
		"relink to another task": {"task_id": "other-task"},
		"re-mark superseded":     {"artifact_status": "superseded"},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedePredecessor, nil, payload)
			if err == nil {
				t.Fatalf("generic update of superseded history succeeded: %+v", out)
			}
			// An explicit refusal, never a silent no-op or a misleading not-found.
			if !strings.Contains(err.Error(), "task_graph_supersede_worklink") {
				t.Fatalf("refusal must name the typed transition: %v", err)
			}
			if after := readStoredLink(t, f.client, supersedePredecessor); after != pred {
				t.Fatalf("a refused update rewrote the history: %+v -> %+v", pred, after)
			}
		})
	}
	// The retirement was never reversible, so the successor still stands alone
	// and the exact typed retry still converges.
	if after := readStoredLink(t, f.client, supersedeSuccessorID); after != succ {
		t.Fatalf("successor changed: %+v -> %+v", succ, after)
	}
	if out, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil || out.SupersedeReceipt.RowsAffected != 0 || !out.SupersedeReceipt.Idempotent {
		t.Fatalf("exact retry after the refusals = %+v, %v", out, err)
	}
}

func TestGenericUpdateRefusesARowWhoseStoredStatusIsSupersededInAnySpelling(t *testing.T) {
	f := newSupersedeFixture(t)
	for _, spelling := range []string{"Superseded", " SUPERSEDED "} {
		setStoredStatus(t, f.client, supersedePredecessor, spelling)
		before := readStoredLink(t, f.client, supersedePredecessor)
		out, err := nativeUpdateWorklinkFields(context.Background(), f.env, "arbor", supersedePredecessor, nil, map[string]interface{}{"note": "x"})
		if err == nil || !strings.Contains(err.Error(), "superseded history") {
			t.Fatalf("stored %q: update = %+v, %v; want a superseded-history refusal", spelling, out, err)
		}
		if after := readStoredLink(t, f.client, supersedePredecessor); after != before {
			t.Fatalf("stored %q: refused update wrote: %+v -> %+v", spelling, before, after)
		}
	}
}

func TestGenericUpdateKeepsOrdinaryMetadataAndArchiveBehavior(t *testing.T) {
	f := newSupersedeFixture(t)
	ctx := context.Background()
	if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: "L-evidence", ArtifactType: "commit", ArtifactRef: "sha"}); err != nil {
		t.Fatalf("seed evidence: %v", err)
	}
	out, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", "L-evidence", nil, map[string]interface{}{"note": "edited", "url": "https://example.invalid/x", "pr_state": "merged"})
	if err != nil || out.NativeMutationReceipt == nil || out.NativeMutationReceipt.Operation != "update_worklink" || out.NativeMutationReceipt.RowsAffected != 1 {
		t.Fatalf("ordinary metadata update = %+v, %v", out, err)
	}
	if got := readStoredLink(t, f.client, "L-evidence"); got.Note != "edited" || got.Status != "recorded" {
		t.Fatalf("evidence row = %+v", got)
	}
	// The pre-existing lifecycle move to archived on an active row is untouched.
	if _, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", "L-evidence", nil, map[string]interface{}{"artifact_status": "archived"}); err != nil {
		t.Fatalf("archived through generic update: %v", err)
	}
	if got := readStoredLink(t, f.client, "L-evidence"); got.Status != "archived" {
		t.Fatalf("archived status = %+v", got)
	}
	// An unknown link is still reported as not found, and an ordinary task update still works.
	requireCoverageError(t, func() error {
		_, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", "L-absent", nil, map[string]interface{}{"note": "x"})
		return err
	}(), "not found")
	if _, err := nativeUpdateTaskFields(ctx, f.env, "arbor", supersedeTask, map[string]interface{}{"title": "retitled"}); err != nil {
		t.Fatalf("ordinary task update: %v", err)
	}
}

// ---------------- the sole valid binding is retired only by typed succession ----------------
//
// A valid active binding is an active worktree, binding or branch row that
// carries branch, worktree, thread and session and is the ONLY such row of its
// task and repository: exactly the row task_graph_supersede_worklink replaces.
// Neither the generic update nor the erroneous-row archive may retire, rewrite
// or re-home it, and a superseded predecessor and the successor's linkage note
// are the retry and read-back evidence, so they are immutable too.

func requireRefusedUnchanged(t *testing.T, f supersedeFixture, label, linkID string, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: succeeded, want a refusal containing %q", label, want)
	}
	requireCoverageError(t, err, want)
}

func TestGenericUpdateCannotRetireTheSoleValidBinding(t *testing.T) {
	ctx := context.Background()
	for _, status := range []string{"archived", "Archived", " released ", "closed", "CLOSED", "superseded"} {
		t.Run(status, func(t *testing.T) {
			f := newSupersedeFixture(t)
			before := readStoredLink(t, f.client, supersedePredecessor)
			_, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedePredecessor, nil, map[string]interface{}{"artifact_status": status, "note": "changed alongside"})
			requireRefusedUnchanged(t, f, status, supersedePredecessor, err, "task_graph_supersede_worklink")
			if after := readStoredLink(t, f.client, supersedePredecessor); after != before {
				t.Fatalf("a refused update wrote: %+v -> %+v", before, after)
			}
			// Typed succession is still the only way through.
			if out, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil || out.SupersedeReceipt.RowsAffected != 2 {
				t.Fatalf("typed supersede after the refusal = %+v, %v", out, err)
			}
		})
	}
}

func TestGenericUpdateCannotRewriteTheIdentityOfTheSoleValidBinding(t *testing.T) {
	ctx := context.Background()
	rewrites := map[string]map[string]interface{}{
		"retype away from a binding type": {"artifact_type": "commit"},
		"relink to another task":          {"task_id": "other-task"},
		"change the repository":           {"repo": "task-graph"},
		"change the branch":               {"branch": "branch-elsewhere"},
		"change the worktree":             {"worktree_path": "/wt/elsewhere"},
		"change the thread":               {"thread": "thread-elsewhere"},
		"change the session":              {"session_id": "sess-elsewhere"},
		"several at once with metadata":   {"branch": "branch-elsewhere", "worktree_path": "/wt/elsewhere", "note": "n"},
	}
	for name, payload := range rewrites {
		t.Run(name, func(t *testing.T) {
			f := newSupersedeFixture(t)
			before := readStoredLink(t, f.client, supersedePredecessor)
			_, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedePredecessor, nil, payload)
			requireRefusedUnchanged(t, f, name, supersedePredecessor, err, "task_graph_supersede_worklink")
			if after := readStoredLink(t, f.client, supersedePredecessor); after != before {
				t.Fatalf("a refused rewrite wrote: %+v -> %+v", before, after)
			}
		})
	}
	// The public MCP entry always carries a contract, which overrides the payload's
	// artifact_type: a retype arrives as contract.artifact_type_kind.
	viaContract := newSupersedeFixture(t)
	beforeContract := readStoredLink(t, viaContract.client, supersedePredecessor)
	_, err := nativeUpdateWorklinkFields(ctx, viaContract.env, "arbor", supersedePredecessor, map[string]interface{}{"artifact_type_kind": "commit", "artifact_ref": "x"}, map[string]interface{}{})
	requireCoverageError(t, err, "task_graph_supersede_worklink")
	if after := readStoredLink(t, viaContract.client, supersedePredecessor); after != beforeContract {
		t.Fatalf("a refused contract retype wrote: %+v -> %+v", beforeContract, after)
	}
	if _, err := nativeUpdateWorklinkFields(ctx, viaContract.env, "arbor", supersedePredecessor, map[string]interface{}{"artifact_type_kind": "worktree", "artifact_ref": "re-described"}, map[string]interface{}{"note": "n"}); err != nil {
		t.Fatalf("contract echo of the same type with metadata: %v", err)
	}
	// The get to update round trip echoes every identity field unchanged, and
	// non-identity metadata stays editable, so both are permitted.
	f := newSupersedeFixture(t)
	echo := map[string]interface{}{"task_id": supersedeTask, "artifact_type": "worktree", "repo": "example-infra", "branch": "branch-old", "worktree_path": "/wt/old", "thread": "thread-a", "session_id": "sess-a", "artifact_status": "recorded", "note": "edited", "pr_state": "merged", "url": "https://example.invalid/x"}
	if out, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedePredecessor, nil, echo); err != nil || out.NativeMutationReceipt.RowsAffected != 1 {
		t.Fatalf("round-trip echo with metadata = %+v, %v", out, err)
	}
	if got := readStoredLink(t, f.client, supersedePredecessor); got.Note != "edited" || got.Status != "recorded" || got.Branch != "branch-old" {
		t.Fatalf("after echo update: %+v", got)
	}
}

func TestGenericUpdateCannotForgeOrRewriteSuccessorLinkage(t *testing.T) {
	ctx := context.Background()
	f := newSupersedeFixture(t)
	if _, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil {
		t.Fatalf("supersede: %v", err)
	}
	if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: "L-evidence", ArtifactType: "commit", ArtifactRef: "sha", Note: "plain"}); err != nil {
		t.Fatal(err)
	}
	succ := readStoredLink(t, f.client, supersedeSuccessorID)
	evidence := readStoredLink(t, f.client, "L-evidence")
	// The successor's note is the retry and read-back linkage.
	_, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedeSuccessorID, nil, map[string]interface{}{"note": "rewritten"})
	requireCoverageError(t, err, "task_graph_supersede_worklink")
	// No other row may claim to be a successor.
	_, err = nativeUpdateWorklinkFields(ctx, f.env, "arbor", "L-evidence", nil, map[string]interface{}{"note": "supersedes=" + supersedePredecessor})
	requireCoverageError(t, err, "task_graph_supersede_worklink")
	if after := readStoredLink(t, f.client, supersedeSuccessorID); after != succ {
		t.Fatalf("successor changed: %+v -> %+v", succ, after)
	}
	if after := readStoredLink(t, f.client, "L-evidence"); after != evidence {
		t.Fatalf("evidence row changed: %+v -> %+v", evidence, after)
	}
	// Ordinary notes on ordinary rows still work, and so does the exact echo of the successor's own note.
	if _, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", "L-evidence", nil, map[string]interface{}{"note": "plain edited"}); err != nil {
		t.Fatalf("ordinary note update: %v", err)
	}
	if _, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedeSuccessorID, nil, map[string]interface{}{"note": "supersedes=" + supersedePredecessor, "pr_state": "open"}); err != nil {
		t.Fatalf("exact echo of the successor note with metadata: %v", err)
	}
}

func TestGenericUpdateStillRetiresRowsThatAreNotTheSoleValidBinding(t *testing.T) {
	ctx := context.Background()
	f := newSupersedeFixture(t)
	add := func(in addWorklinkInput) {
		in.ProjectID, in.TaskID = "arbor", supersedeTask
		if _, err := nativeHandleAddWorklink(ctx, f.env, in); err != nil {
			t.Fatalf("seed %s: %v", in.ArtifactID, err)
		}
	}
	// A second full binding for the same task and repository makes the pair an
	// erroneous duplicate, so either may be cleaned up. Rows that are not
	// canonical bindings (no thread or session, or not a binding type) never were valid.
	add(addWorklinkInput{ArtifactID: "BND-dup", ArtifactType: "worktree", ArtifactRef: "dup", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "branch-dup", WorktreePath: "/wt/dup"})
	add(addWorklinkInput{ArtifactID: "BND-legacy", ArtifactType: "binding", ArtifactRef: "legacy", Repo: "example-infra", Branch: "branch-legacy"})
	add(addWorklinkInput{ArtifactID: "L-evidence", ArtifactType: "commit", ArtifactRef: "sha"})
	for id, status := range map[string]string{"BND-dup": "archived", "BND-legacy": "released", "L-evidence": "closed"} {
		if _, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", id, nil, map[string]interface{}{"artifact_status": status}); err != nil {
			t.Fatalf("cleanup of %s to %s: %v", id, status, err)
		}
		if got := readStoredLink(t, f.client, id); got.Status != status {
			t.Fatalf("%s status = %q, want %q", id, got.Status, status)
		}
	}
	// With the duplicate gone the original is the sole valid binding again.
	_, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedePredecessor, nil, map[string]interface{}{"artifact_status": "archived"})
	requireCoverageError(t, err, "task_graph_supersede_worklink")
}

func TestTypedArchiveRefusesTheSoleValidBindingAndSupersededHistoryButKeepsErroneousCleanup(t *testing.T) {
	ctx := context.Background()
	f := newSupersedeFixture(t)
	archive := func(id string) (MutationWriteResult, error) {
		return nativeHandleArchiveWorklink(ctx, f.env, archiveWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, LinkID: id})
	}
	before := readStoredLink(t, f.client, supersedePredecessor)
	_, err := archive(supersedePredecessor)
	requireCoverageError(t, err, "task_graph_supersede_worklink")
	if after := readStoredLink(t, f.client, supersedePredecessor); after != before {
		t.Fatalf("a refused archive wrote: %+v -> %+v", before, after)
	}
	if _, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil {
		t.Fatalf("supersede: %v", err)
	}
	// The predecessor is retry and read-back evidence and the successor is now the sole valid binding.
	for _, id := range []string{supersedePredecessor, supersedeSuccessorID} {
		pre := readStoredLink(t, f.client, id)
		_, err := archive(id)
		requireCoverageError(t, err, "task_graph_supersede_worklink")
		if post := readStoredLink(t, f.client, id); post != pre {
			t.Fatalf("a refused archive of %s wrote: %+v -> %+v", id, pre, post)
		}
	}
	if out, err := nativeHandleSupersedeWorklink(ctx, f.env, f.in); err != nil || !out.SupersedeReceipt.Idempotent {
		t.Fatalf("exact retry after the refused archives = %+v, %v", out, err)
	}
	// Genuinely erroneous rows: evidence, duplicates and legacy rows archive as before, idempotently.
	for _, in := range []addWorklinkInput{
		{ArtifactID: "L-evidence", ArtifactType: "commit", ArtifactRef: "sha"},
		{ArtifactID: "BND-legacy", ArtifactType: "binding", ArtifactRef: "legacy", Repo: "example-infra", Branch: "b"},
	} {
		in.ProjectID, in.TaskID = "arbor", supersedeTask
		if _, err := nativeHandleAddWorklink(ctx, f.env, in); err != nil {
			t.Fatal(err)
		}
		out, err := archive(in.ArtifactID)
		if err != nil || out.NativeMutationReceipt.RowsAffected != 1 {
			t.Fatalf("erroneous archive of %s = %+v, %v", in.ArtifactID, out, err)
		}
		if again, err := archive(in.ArtifactID); err != nil || again.NativeMutationReceipt.RowsAffected != 0 {
			t.Fatalf("re-archive of %s = %+v, %v", in.ArtifactID, again, err)
		}
	}
	// A duplicate valid binding beside the successor is an erroneous row too.
	if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: "BND-dup", ArtifactType: "worktree", ArtifactRef: "dup", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "branch-dup", WorktreePath: "/wt/dup"}); err != nil {
		t.Fatal(err)
	}
	if out, err := archive("BND-dup"); err != nil || out.NativeMutationReceipt.RowsAffected != 1 {
		t.Fatalf("archive of a duplicate binding = %+v, %v", out, err)
	}
	requireCoverageError(t, func() error { _, err := archive("L-absent"); return err }(), "not found")
}

func TestGenericUpdateCannotMintSupersededOnAnyRow(t *testing.T) {
	ctx := context.Background()
	f := newSupersedeFixture(t)
	if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, ArtifactID: "L-evidence", ArtifactType: "commit", ArtifactRef: "sha"}); err != nil {
		t.Fatal(err)
	}
	before := readStoredLink(t, f.client, "L-evidence")
	for _, status := range []string{"superseded", "Superseded", " SUPERSEDED "} {
		_, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", "L-evidence", nil, map[string]interface{}{"artifact_status": status})
		requireCoverageError(t, err, "task_graph_supersede_worklink")
		if after := readStoredLink(t, f.client, "L-evidence"); after != before {
			t.Fatalf("%q: a refused update wrote: %+v -> %+v", status, before, after)
		}
	}
}

func TestLegacyBindingRowWithoutThreadAndSessionIsErroneousCleanup(t *testing.T) {
	ctx := context.Background()
	f := newSupersedeFixture(t)
	// other-task has no valid binding: its only binding-type row predates thread-bound registration.
	if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: "other-task", ArtifactID: "BND-legacy-only", ArtifactType: "binding", ArtifactRef: "legacy", Repo: "example-infra", Branch: "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", "BND-legacy-only", nil, map[string]interface{}{"artifact_status": "released"}); err != nil {
		t.Fatalf("retiring a legacy binding row: %v", err)
	}
	if _, err := nativeHandleAddWorklink(ctx, f.env, addWorklinkInput{ProjectID: "arbor", TaskID: "other-task", ArtifactID: "BND-legacy-two", ArtifactType: "worktree", ArtifactRef: "legacy", Repo: "example-infra", Branch: "b"}); err != nil {
		t.Fatal(err)
	}
	if out, err := nativeHandleArchiveWorklink(ctx, f.env, archiveWorklinkInput{ProjectID: "arbor", TaskID: "other-task", LinkID: "BND-legacy-two"}); err != nil || out.NativeMutationReceipt.RowsAffected != 1 {
		t.Fatalf("archiving a legacy worktree row = %+v, %v", out, err)
	}
}

func TestSoleValidBindingProtectionIgnoresInactiveSpellingsAndOtherRepositories(t *testing.T) {
	ctx := context.Background()
	f := newSupersedeFixture(t)
	add := func(in addWorklinkInput) {
		in.ProjectID, in.TaskID = "arbor", supersedeTask
		if _, err := nativeHandleAddWorklink(ctx, f.env, in); err != nil {
			t.Fatalf("seed %s: %v", in.ArtifactID, err)
		}
	}
	// Another repository's binding and a duplicate whose status only LOOKS active-adjacent do not make the sole binding a duplicate.
	add(addWorklinkInput{ArtifactID: "BND-other-repo", ArtifactType: "worktree", ArtifactRef: "x", Thread: "thread-a", SessionID: "sess-a", Repo: "task-graph", Branch: "b", WorktreePath: "/wt/other-repo"})
	add(addWorklinkInput{ArtifactID: "BND-retired-dup", ArtifactType: "worktree", ArtifactRef: "x", Thread: "thread-a", SessionID: "sess-a", Repo: "example-infra", Branch: "bd", WorktreePath: "/wt/retired-dup"})
	setStoredStatus(t, f.client, "BND-retired-dup", " ARCHIVED ")
	before := readStoredLink(t, f.client, supersedePredecessor)
	for _, status := range []string{"archived", "closed"} {
		_, err := nativeUpdateWorklinkFields(ctx, f.env, "arbor", supersedePredecessor, nil, map[string]interface{}{"artifact_status": status})
		requireCoverageError(t, err, "task_graph_supersede_worklink")
	}
	_, err := nativeHandleArchiveWorklink(ctx, f.env, archiveWorklinkInput{ProjectID: "arbor", TaskID: supersedeTask, LinkID: supersedePredecessor})
	requireCoverageError(t, err, "task_graph_supersede_worklink")
	if after := readStoredLink(t, f.client, supersedePredecessor); after != before {
		t.Fatalf("a refused mutation wrote: %+v -> %+v", before, after)
	}
	// The other repository's binding is itself the sole valid binding of that repository.
	_, err = nativeUpdateWorklinkFields(ctx, f.env, "arbor", "BND-other-repo", nil, map[string]interface{}{"artifact_status": "archived"})
	requireCoverageError(t, err, "task_graph_supersede_worklink")
}
