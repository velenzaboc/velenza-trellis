package main

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"google.golang.org/api/iterator"
)

type writerHandoverFixture struct {
	env    *Env
	client *spanner.Client
	in     reownInput
}

func newWriterHandoverFixture(t *testing.T, taskID string) writerHandoverFixture {
	t.Helper()
	env, client := newNativeCoverageEnv(t)
	ctx := context.Background()
	if _, err := nativeHandleCreate(ctx, env, coverageTaskInput(taskID)); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := nativeHandleClaim(ctx, env, claimInput{ProjectID: "arbor", TaskID: taskID, Owner: "claude-owner", SessionID: "claude-session", Repo: "example-infra"}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := nativeHandleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID: "arbor", TaskID: taskID, ArtifactID: "BND-existing", ArtifactType: "worktree",
		ArtifactRef: "example-infra:task/catalog:/wt/catalog", Thread: "claude-owner", SessionID: "claude-session",
		Repo: "example-infra", Branch: "task/catalog", WorktreePath: "/wt/catalog", Note: "keep-note",
		CommitSHA: "0123456789abcdef", PRNumber: 72, PRState: "open", URL: "https://example.invalid/pr/72",
	}); err != nil {
		t.Fatalf("seed binding: %v", err)
	}
	return writerHandoverFixture{env: env, client: client, in: reownInput{
		ProjectID: "arbor", TaskID: taskID,
		Owner: "codex-root", SessionID: "codex-session", Repo: "example-infra",
		ExpectedOwner: "claude-owner", ExpectedSessionID: "claude-session", ExpectedRepo: "example-infra",
		WriterHandover: &writerHandoverInput{
			LinkID: "BND-existing", ArtifactType: "worktree", ArtifactStatus: "recorded",
			Repo: "example-infra", Branch: "task/catalog", WorktreePath: "/wt/catalog",
			Thread: "claude-owner", SessionID: "claude-session", TargetThread: "01a0f327-e9d2-7150-9f9d-8556fcf216b0",
		},
	}}
}

func readStoredTaskTuple(t *testing.T, client *spanner.Client, taskID string) (string, string, string) {
	t.Helper()
	iter := client.Single().Query(context.Background(), spanner.Statement{
		SQL:    `SELECT owner, session_id, repo FROM Tasks WHERE project_id = 'arbor' AND id = @id`,
		Params: map[string]interface{}{"id": taskID},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		t.Fatalf("read task tuple: %v", err)
	}
	var owner string
	var session, repo spanner.NullString
	if err := row.Columns(&owner, &session, &repo); err != nil {
		t.Fatalf("decode task tuple: %v", err)
	}
	return owner, nullStringValue(session), nullStringValue(repo)
}

func countWriterHandoverHistory(t *testing.T, client *spanner.Client, taskID string) int {
	t.Helper()
	iter := client.Single().Read(context.Background(), "TaskLinks", spanner.AllKeys(), []string{"task_id", "artifact_type", "note"})
	defer iter.Stop()
	count := 0
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return count
		}
		if err != nil {
			t.Fatalf("read writer history: %v", err)
		}
		var storedTaskID, artifactType string
		var note spanner.NullString
		if err := row.Columns(&storedTaskID, &artifactType, &note); err != nil {
			t.Fatal(err)
		}
		if storedTaskID == taskID && artifactType == writerHandoverHistoryType && namesWriterHandoverHistory(nullStringValue(note)) {
			count++
		}
	}
}

// TestNativeReownWriterHandoverRepairsAnAlreadySplitTask is the regression
// reproducer for a resumed task whose Tasks tuple was re-owned before its
// protected worktree binding. The typed reown must repair the existing writer
// in the same transaction without changing the binding id, branch or path.
func TestNativeReownWriterHandoverRepairsAnAlreadySplitTask(t *testing.T) {
	const taskID = "writer-split"
	f := newWriterHandoverFixture(t, taskID)

	// Reproduce the observed partial handover: Tasks is already root-owned,
	// while the sole protected TaskLinks writer still carries Claude.
	setStoredTaskField(t, f.client, taskID, "owner", "codex-root")
	setStoredTaskField(t, f.client, taskID, "session_id", "codex-session")
	f.in.ExpectedOwner = "codex-root"
	f.in.ExpectedSessionID = "codex-session"

	out, err := nativeHandleReown(context.Background(), f.env, f.in)
	if err != nil {
		t.Fatalf("repair split handover: %v", err)
	}
	if out.NativeMutationReceipt == nil || out.NativeMutationReceipt.RowsAffected != 2 {
		t.Fatalf("native receipt = %+v, want history+writer rows", out.NativeMutationReceipt)
	}
	if out.WriterHandoverReceipt == nil || out.WriterHandoverReceipt.Idempotent || out.WriterHandoverReceipt.HistoryLinkID == "" {
		t.Fatalf("writer handover receipt = %+v", out.WriterHandoverReceipt)
	}

	active := readStoredLink(t, f.client, "BND-existing")
	if active.Thread != "01a0f327-e9d2-7150-9f9d-8556fcf216b0" || active.SessionID != "codex-session" || active.Repo != "example-infra" || active.Branch != "task/catalog" || active.WorktreePath != "/wt/catalog" || active.Note != "keep-note" || active.Status != "recorded" {
		t.Fatalf("existing binding was not coherently repaired in place: %+v", active)
	}
	history := readStoredLink(t, f.client, out.WriterHandoverReceipt.HistoryLinkID)
	if !history.Found || history.ArtifactType != "finding" || history.Thread != "claude-owner" || history.SessionID != "claude-session" || history.Status != "recorded" || history.Branch != "task/catalog" || history.WorktreePath != "/wt/catalog" {
		t.Fatalf("prior writer history = %+v", history)
	}
}

func TestNativeReownWriterHandoverNormalCommitAndExactRetry(t *testing.T) {
	f := newWriterHandoverFixture(t, "writer-normal")
	ctx := context.Background()
	out, err := nativeHandleReown(ctx, f.env, f.in)
	if err != nil {
		t.Fatalf("normal handover: %v", err)
	}
	if out.NativeMutationReceipt == nil || out.NativeMutationReceipt.RowsAffected != 3 || out.WriterHandoverReceipt == nil || out.WriterHandoverReceipt.Idempotent {
		t.Fatalf("first receipt = %+v", out)
	}
	owner, session, repo := readStoredTaskTuple(t, f.client, f.in.TaskID)
	if owner != f.in.Owner || session != f.in.SessionID || repo != f.in.Repo {
		t.Fatalf("task tuple = (%q,%q,%q)", owner, session, repo)
	}
	active := readStoredLink(t, f.client, f.in.WriterHandover.LinkID)
	if active.Thread != f.in.WriterHandover.TargetThread || active.SessionID != f.in.SessionID || active.Note != "keep-note" || active.Branch != f.in.WriterHandover.Branch || active.WorktreePath != f.in.WriterHandover.WorktreePath {
		t.Fatalf("active binding = %+v", active)
	}
	retried, err := nativeHandleReown(ctx, f.env, f.in)
	if err != nil || retried.NativeMutationReceipt == nil || retried.NativeMutationReceipt.RowsAffected != 0 || retried.WriterHandoverReceipt == nil || !retried.WriterHandoverReceipt.Idempotent || retried.WriterHandoverReceipt.HistoryLinkID != out.WriterHandoverReceipt.HistoryLinkID {
		t.Fatalf("exact retry = %+v, %v", retried, err)
	}
	if got := countWriterHandoverHistory(t, f.client, f.in.TaskID); got != 1 {
		t.Fatalf("history rows = %d, want 1", got)
	}
}

func TestNativeReownRefusesTaskOnlyChangeBesideProtectedBinding(t *testing.T) {
	f := newWriterHandoverFixture(t, "writer-required")
	in := f.in
	in.WriterHandover = nil
	if out, err := nativeHandleReown(context.Background(), f.env, in); err == nil || !strings.Contains(err.Error(), "writer_handover") {
		t.Fatalf("task-only handover = %+v, %v", out, err)
	}
	owner, session, repo := readStoredTaskTuple(t, f.client, in.TaskID)
	if owner != in.ExpectedOwner || session != in.ExpectedSessionID || repo != in.ExpectedRepo {
		t.Fatalf("refused handover changed task = (%q,%q,%q)", owner, session, repo)
	}
	if got := readStoredLink(t, f.client, in.WriterHandoverLinkID()); got.Thread != "claude-owner" || got.SessionID != "claude-session" {
		t.Fatalf("refused handover changed binding: %+v", got)
	}
}

func (in reownInput) WriterHandoverLinkID() string {
	if in.WriterHandover != nil {
		return in.WriterHandover.LinkID
	}
	return "BND-existing"
}

func TestNativeReownWriterHandoverHostileTuplesFailClosed(t *testing.T) {
	cases := map[string]func(*reownInput){
		"wrong current owner": func(in *reownInput) { in.ExpectedOwner = "wrong" },
		"wrong link":          func(in *reownInput) { in.WriterHandover.LinkID = "BND-absent" },
		"wrong type":          func(in *reownInput) { in.WriterHandover.ArtifactType = "binding" },
		"wrong status":        func(in *reownInput) { in.WriterHandover.ArtifactStatus = "archived" },
		"wrong repo":          func(in *reownInput) { in.WriterHandover.Repo = "task-graph" },
		"wrong branch":        func(in *reownInput) { in.WriterHandover.Branch = "wrong" },
		"wrong path":          func(in *reownInput) { in.WriterHandover.WorktreePath = "/wrong" },
		"wrong old thread":    func(in *reownInput) { in.WriterHandover.Thread = "wrong" },
		"wrong old session":   func(in *reownInput) { in.WriterHandover.SessionID = "wrong" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newWriterHandoverFixture(t, "hostile-"+strings.ReplaceAll(name, " ", "-"))
			in := f.in
			copyWriter := *in.WriterHandover
			in.WriterHandover = &copyWriter
			mutate(&in)
			if out, err := nativeHandleReown(context.Background(), f.env, in); err == nil {
				t.Fatalf("hostile handover succeeded: %+v", out)
			}
			owner, session, repo := readStoredTaskTuple(t, f.client, f.in.TaskID)
			if owner != "claude-owner" || session != "claude-session" || repo != "example-infra" || countWriterHandoverHistory(t, f.client, f.in.TaskID) != 0 {
				t.Fatalf("refusal changed task/history = (%q,%q,%q)", owner, session, repo)
			}
			if link := readStoredLink(t, f.client, "BND-existing"); link.Thread != "claude-owner" || link.SessionID != "claude-session" {
				t.Fatalf("refusal changed binding: %+v", link)
			}
		})
	}
}

func TestNativeReownWriterHandoverRefusesAmbiguousBindings(t *testing.T) {
	f := newWriterHandoverFixture(t, "writer-ambiguous")
	if _, err := nativeHandleAddWorklink(context.Background(), f.env, addWorklinkInput{
		ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "BND-duplicate", ArtifactType: "worktree", ArtifactRef: "duplicate",
		Thread: "claude-owner", SessionID: "claude-session", Repo: "example-infra", Branch: "duplicate", WorktreePath: "/wt/duplicate",
	}); err != nil {
		t.Fatal(err)
	}
	if out, err := nativeHandleReown(context.Background(), f.env, f.in); err == nil || !strings.Contains(err.Error(), "exactly") {
		t.Fatalf("ambiguous handover = %+v, %v", out, err)
	}
	if countWriterHandoverHistory(t, f.client, f.in.TaskID) != 0 {
		t.Fatal("ambiguous handover wrote history")
	}
}

func TestNativeReownWriterHandoverRefusesIdentityBearingIncompleteCompetitor(t *testing.T) {
	f := newWriterHandoverFixture(t, "writer-incomplete-competitor")
	if _, err := handleAddWorklink(context.Background(), f.env, addWorklinkInput{
		ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "BND-incomplete", ArtifactType: "worktree",
		ArtifactRef: "example-infra:incomplete", Repo: "example-infra", Thread: "other-thread", SessionID: "other-session",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeHandleReown(context.Background(), f.env, f.in); err == nil || !strings.Contains(err.Error(), "2 active writer-identity binding worklinks") {
		t.Fatalf("incomplete competing writer error = %v", err)
	}
	owner, session, repo := readStoredTaskTuple(t, f.client, f.in.TaskID)
	if owner != "claude-owner" || session != "claude-session" || repo != "example-infra" || countWriterHandoverHistory(t, f.client, f.in.TaskID) != 0 {
		t.Fatalf("refusal changed task/history: owner=%q session=%q repo=%q", owner, session, repo)
	}
}

func TestNativeReownWriterHandoverRefusesRepositoryAliasCompetitor(t *testing.T) {
	f := newWriterHandoverFixture(t, "writer-repo-alias-competitor")
	if _, err := handleAddWorklink(context.Background(), f.env, addWorklinkInput{
		ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "BND-repo-alias", ArtifactType: "worktree",
		ArtifactRef: "https://github.com/velenzaboc/example-infra.git:other:/wt/other",
		Repo:        "https://github.com/velenzaboc/example-infra.git", Branch: "other", WorktreePath: "/wt/other",
		Thread: "other-thread", SessionID: "other-session",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeHandleReown(context.Background(), f.env, f.in); err == nil || !strings.Contains(err.Error(), "2 active writer-identity binding worklinks") {
		t.Fatalf("repository-alias competing writer error = %v", err)
	}
	owner, session, repo := readStoredTaskTuple(t, f.client, f.in.TaskID)
	if owner != "claude-owner" || session != "claude-session" || repo != "example-infra" || countWriterHandoverHistory(t, f.client, f.in.TaskID) != 0 {
		t.Fatalf("refusal changed task/history: owner=%q session=%q repo=%q", owner, session, repo)
	}
}

func TestNativeNormalizeRepositoryMatchesWorkIdentityConsumer(t *testing.T) {
	for input, want := range map[string]string{
		"example-infra": "example-infra",
		" https://github.com/velenzaboc/example-infra.git/ ": "example-infra",
		"git@github.com:velenzaboc/example-infra.git":        "example-infra",
		`C:\workspace\example-infra`:                          "example-infra",
		"VELENZABOC:EXAMPLE-INFRA.GIT":                       "example-infra",
		"   ":                                             "",
	} {
		if got := nativeNormalizeRepository(input); got != want {
			t.Errorf("nativeNormalizeRepository(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestWriterHandoverHistoryCannotBeForgedOrMutatedThroughGenericRoutes(t *testing.T) {
	f := newWriterHandoverFixture(t, "writer-evidence")
	out, err := nativeHandleReown(context.Background(), f.env, f.in)
	if err != nil {
		t.Fatal(err)
	}
	historyID := out.WriterHandoverReceipt.HistoryLinkID
	for name, run := range map[string]func() error{
		"add worklink": func() error {
			_, err := handleAddWorklink(context.Background(), f.env, addWorklinkInput{ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "FORGED-add", ArtifactType: "finding", ArtifactRef: "forged", Note: writerHandoverHistoryNote + `{}`})
			return err
		},
		"generic create": func() error {
			_, err := handleCreate(context.Background(), f.env, createInput{ProjectID: "arbor", NodeKind: "artifact", StableID: "FORGED-create", Contract: map[string]interface{}{"artifact_type_kind": "finding", "artifact_ref": "forged"}, Payload: map[string]interface{}{"task_id": f.in.TaskID, "note": writerHandoverHistoryNote + `{}`}})
			return err
		},
		"generic import": func() error {
			_, err := handleImport(context.Background(), f.env, importInput{Submissions: []ArtifactSubmission{{
				NodeKind: "artifact", StableID: "FORGED-import",
				Contract: map[string]interface{}{"artifact_type_kind": "finding", "artifact_ref": "forged"},
				Payload:  map[string]interface{}{"task_id": f.in.TaskID, "note": writerHandoverHistoryNote + `{}`},
			}}})
			return err
		},
		"generic update existing history": func() error {
			_, err := handleUpdate(context.Background(), f.env, updateInput{ProjectID: "arbor", NodeID: historyID, Payload: map[string]interface{}{"pr_state": "closed"}})
			return err
		},
		"generic update forge prefix": func() error {
			_, err := handleUpdate(context.Background(), f.env, updateInput{ProjectID: "arbor", NodeID: "BND-existing", Payload: map[string]interface{}{"note": writerHandoverHistoryNote + `{}`}})
			return err
		},
		"typed archive": func() error {
			_, err := handleArchiveWorklink(context.Background(), f.env, archiveWorklinkInput{ProjectID: "arbor", TaskID: f.in.TaskID, LinkID: historyID})
			return err
		},
		"generic link": func() error {
			_, err := handleLink(context.Background(), f.env, linkInput{ProjectID: "arbor", SourceID: historyID, TargetID: f.in.TaskID, EdgeLabel: "advances"})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || !strings.Contains(err.Error(), writerHandoverHistoryNote) {
				t.Fatalf("route error = %v, want reserved-prefix refusal", err)
			}
		})
	}
	if got := countWriterHandoverHistory(t, f.client, f.in.TaskID); got != 1 {
		t.Fatalf("protected history rows = %d, want 1", got)
	}
}

func TestWriterHandoverInputAndModeBoundaries(t *testing.T) {
	f := newWriterHandoverFixture(t, "writer-input")
	for name, mutate := range map[string]func(*reownInput){
		"release":        func(in *reownInput) { in.Owner, in.SessionID, in.Repo = "UNOWNED", "", "" },
		"missing link":   func(in *reownInput) { in.WriterHandover.LinkID = "" },
		"missing target": func(in *reownInput) { in.WriterHandover.TargetThread = "" },
	} {
		t.Run(name, func(t *testing.T) {
			in := f.in
			w := *in.WriterHandover
			in.WriterHandover = &w
			mutate(&in)
			if err := validateReownInput(in); err == nil {
				t.Fatalf("invalid input accepted: %+v", in)
			}
		})
	}
	env := &Env{Substrates: newRouter(gcpauth.Selector{}, false, []SubstrateDescriptor{{ID: "schema-v1", SpannerDatabase: "projects/p/instances/i/databases/d"}})}
	defer env.Substrates.Close()
	if out, err := handleReown(context.Background(), env, f.in); err == nil || !strings.Contains(err.Error(), "requires fleet_plan_native") {
		t.Fatalf("schema-v1 writer_handover = %+v, %v", out, err)
	}
}
