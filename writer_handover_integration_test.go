//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

type writerHandoverIntegrationFixture struct {
	env    *Env
	client *spanner.Client
	in     reownInput
}

type writerHandoverTaskState struct {
	Owner     string
	SessionID string
	Repo      string
	UpdatedAt time.Time
}

type writerHandoverLinkState struct {
	LinkID         string
	TaskID         string
	ArtifactType   string
	ArtifactRef    string
	Thread         string
	Note           string
	CreatedAt      time.Time
	SessionID      string
	Repo           string
	Branch         string
	CommitSHA      string
	PRNumber       int64
	PRNumberValid  bool
	PRState        string
	WorktreePath   string
	URL            string
	ArtifactStatus string
}

type writerHandoverStoreState struct {
	Task  writerHandoverTaskState
	Links []writerHandoverLinkState
}

func newWriterHandoverIntegrationFixture(t *testing.T, env *Env, taskID string) writerHandoverIntegrationFixture {
	t.Helper()
	ctx := context.Background()
	createNativeTask(t, ctx, env, "arbor", taskID, taskID, "not_started")
	if _, err := handleClaim(ctx, env, claimInput{
		ProjectID: "arbor", TaskID: taskID, Owner: "old-owner", SessionID: "old-session", Repo: "example-infra",
	}); err != nil {
		t.Fatalf("claim %s: %v", taskID, err)
	}
	if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID: "arbor", TaskID: taskID, ArtifactID: "BND-" + taskID, ArtifactType: "worktree",
		ArtifactRef: "example-infra:task/catalog:/wt/catalog", Thread: "old-thread", SessionID: "old-session",
		Repo: "example-infra", Branch: "task/catalog", WorktreePath: "/wt/catalog", Note: "preserve-note",
		CommitSHA: "0123456789abcdef", PRNumber: 72, PRState: "open", URL: "https://example.invalid/pr/72",
	}); err != nil {
		t.Fatalf("seed writer %s: %v", taskID, err)
	}
	adapter, err := soleNativeFleetPlanAdapter(ctx, env)
	if err != nil {
		t.Fatalf("native adapter: %v", err)
	}
	client, err := adapter.getClient(ctx)
	if err != nil {
		t.Fatalf("native client: %v", err)
	}
	return writerHandoverIntegrationFixture{env: env, client: client, in: reownInput{
		ProjectID: "arbor", TaskID: taskID,
		Owner: "new-owner", SessionID: "new-session", Repo: "example-infra",
		ExpectedOwner: "old-owner", ExpectedSessionID: "old-session", ExpectedRepo: "example-infra",
		WriterHandover: &writerHandoverInput{
			LinkID: "BND-" + taskID, ArtifactType: "worktree", ArtifactStatus: "recorded",
			Repo: "example-infra", Branch: "task/catalog", WorktreePath: "/wt/catalog",
			Thread: "old-thread", SessionID: "old-session", TargetThread: "new-thread",
		},
	}}
}

func cloneWriterHandoverInput(in reownInput) reownInput {
	out := in
	if in.WriterHandover != nil {
		writer := *in.WriterHandover
		out.WriterHandover = &writer
	}
	return out
}

func readWriterHandoverStoreState(t *testing.T, ctx context.Context, client *spanner.Client, taskID string) writerHandoverStoreState {
	t.Helper()
	ro := client.ReadOnlyTransaction()
	defer ro.Close()

	row, err := ro.ReadRow(ctx, "Tasks", spanner.Key{"arbor", taskID}, []string{"owner", "session_id", "repo", "updated_at"})
	if err != nil {
		t.Fatalf("read task %s: %v", taskID, err)
	}
	var owner string
	var session, repo spanner.NullString
	var updatedAt time.Time
	if err := row.Columns(&owner, &session, &repo, &updatedAt); err != nil {
		t.Fatalf("decode task %s: %v", taskID, err)
	}
	out := writerHandoverStoreState{Task: writerHandoverTaskState{
		Owner: owner, SessionID: nullStringValue(session), Repo: nullStringValue(repo), UpdatedAt: updatedAt,
	}}
	iter := ro.Query(ctx, spanner.Statement{
		SQL: `SELECT link_id, task_id, artifact_type, artifact_ref, thread, note, created_at, session_id,
			repo, branch, commit_sha, pr_number, pr_state, worktree_path, url, artifact_status
			FROM TaskLinks WHERE project_id = 'arbor' AND task_id = @task_id ORDER BY link_id`,
		Params: map[string]interface{}{"task_id": taskID},
	})
	defer iter.Stop()
	if err := iter.Do(func(row *spanner.Row) error {
		var link writerHandoverLinkState
		var thread, note, sessionID, linkRepo, branch, commitSHA, prState, worktreePath, url, status spanner.NullString
		var prNumber spanner.NullInt64
		if err := row.Columns(
			&link.LinkID, &link.TaskID, &link.ArtifactType, &link.ArtifactRef, &thread, &note, &link.CreatedAt,
			&sessionID, &linkRepo, &branch, &commitSHA, &prNumber, &prState, &worktreePath, &url, &status,
		); err != nil {
			return err
		}
		link.Thread, link.Note = nullStringValue(thread), nullStringValue(note)
		link.SessionID, link.Repo, link.Branch = nullStringValue(sessionID), nullStringValue(linkRepo), nullStringValue(branch)
		link.CommitSHA, link.PRState = nullStringValue(commitSHA), nullStringValue(prState)
		link.WorktreePath, link.URL, link.ArtifactStatus = nullStringValue(worktreePath), nullStringValue(url), nullStringValue(status)
		link.PRNumber, link.PRNumberValid = prNumber.Int64, prNumber.Valid
		out.Links = append(out.Links, link)
		return nil
	}); err != nil {
		t.Fatalf("read links for %s: %v", taskID, err)
	}
	return out
}

func writerHandoverHistoryRows(state writerHandoverStoreState) []writerHandoverLinkState {
	var out []writerHandoverLinkState
	for _, link := range state.Links {
		if link.ArtifactType == writerHandoverHistoryType && namesWriterHandoverHistory(link.Note) {
			out = append(out, link)
		}
	}
	return out
}

func requireWriterHandoverStateEqual(t *testing.T, got, want writerHandoverStoreState) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("store changed\n got: %#v\nwant: %#v", got, want)
	}
}

func requireCoherentWriterTuple(t *testing.T, state writerHandoverStoreState, bindingID string, old, target bool) {
	t.Helper()
	var binding *writerHandoverLinkState
	for i := range state.Links {
		if state.Links[i].LinkID == bindingID {
			binding = &state.Links[i]
			break
		}
	}
	if binding == nil {
		t.Fatalf("binding %s missing from state: %#v", bindingID, state)
	}
	oldState := state.Task.Owner == "old-owner" && state.Task.SessionID == "old-session" &&
		binding.Thread == "old-thread" && binding.SessionID == "old-session"
	targetState := state.Task.Owner == "new-owner" && state.Task.SessionID == "new-session" &&
		binding.Thread == "new-thread" && binding.SessionID == "new-session"
	if old && oldState || target && targetState {
		return
	}
	t.Fatalf("mixed task/writer tuple: task=%+v binding=%+v", state.Task, *binding)
}

func setWriterHandoverTaskTuple(t *testing.T, ctx context.Context, client *spanner.Client, taskID, owner, sessionID, repo string) {
	t.Helper()
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		n, err := tx.Update(ctx, spanner.Statement{
			SQL: `UPDATE Tasks SET owner = @owner, session_id = @session_id, repo = @repo, updated_at = @updated_at
				WHERE project_id = 'arbor' AND id = @task_id`,
			Params: map[string]interface{}{
				"task_id": taskID, "owner": owner, "session_id": sessionID, "repo": repo, "updated_at": time.Now().UTC(),
			},
		})
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("updated %d task rows, want 1", n)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("set split task tuple: %v", err)
	}
}

func requireCommittedWriterHandover(t *testing.T, f writerHandoverIntegrationFixture, out MutationWriteResult, wantRows int64) writerHandoverStoreState {
	t.Helper()
	if out.NativeMutationReceipt == nil || out.NativeMutationReceipt.RowsAffected != wantRows || out.WriterHandoverReceipt == nil || out.WriterHandoverReceipt.Idempotent {
		t.Fatalf("handover receipt = %+v, want %d committed rows", out, wantRows)
	}
	state := readWriterHandoverStoreState(t, context.Background(), f.client, f.in.TaskID)
	requireCoherentWriterTuple(t, state, f.in.WriterHandover.LinkID, false, true)
	historyRows := writerHandoverHistoryRows(state)
	if len(historyRows) != 1 || historyRows[0].LinkID != out.WriterHandoverReceipt.HistoryLinkID {
		t.Fatalf("handover history = %+v, receipt = %+v", historyRows, out.WriterHandoverReceipt)
	}
	var history writerHandoverHistory
	if err := json.Unmarshal([]byte(strings.TrimPrefix(historyRows[0].Note, writerHandoverHistoryNote)), &history); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if history.BindingLinkID != f.in.WriterHandover.LinkID || history.FromThread != "old-thread" || history.FromSessionID != "old-session" ||
		history.ToThread != "new-thread" || history.ToSessionID != "new-session" || history.PriorNote != "preserve-note" {
		t.Fatalf("history did not preserve exact transition: %+v", history)
	}
	return state
}

func TestIntegrationWriterHandoverCommitSplitRepairAndExactRetry(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()

	t.Run("normal and response-loss retry", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-NORMAL")
		before := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		out, err := handleReown(ctx, env, f.in)
		if err != nil {
			t.Fatalf("normal handover: %v", err)
		}
		after := requireCommittedWriterHandover(t, f, out, 3)
		if len(before.Links) != 1 || len(after.Links) != 2 {
			t.Fatalf("link cardinality before=%d after=%d", len(before.Links), len(after.Links))
		}
		var active writerHandoverLinkState
		for _, link := range after.Links {
			if link.LinkID == f.in.WriterHandover.LinkID {
				active = link
			}
		}
		if active.Branch != "task/catalog" || active.WorktreePath != "/wt/catalog" || active.Note != "preserve-note" ||
			active.CommitSHA != "0123456789abcdef" || !active.PRNumberValid || active.PRNumber != 72 ||
			active.PRState != "open" || active.URL != "https://example.invalid/pr/72" {
			t.Fatalf("writer metadata changed: %+v", active)
		}
		retry, err := handleReown(ctx, env, f.in)
		if err != nil || retry.NativeMutationReceipt == nil || retry.NativeMutationReceipt.RowsAffected != 0 ||
			retry.WriterHandoverReceipt == nil || !retry.WriterHandoverReceipt.Idempotent ||
			retry.WriterHandoverReceipt.HistoryLinkID != out.WriterHandoverReceipt.HistoryLinkID {
			t.Fatalf("exact original-tuple retry = %+v, %v", retry, err)
		}
		requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID), after)
	})

	t.Run("already reowned split repair", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-SPLIT")
		setWriterHandoverTaskTuple(t, ctx, f.client, f.in.TaskID, f.in.Owner, f.in.SessionID, f.in.Repo)
		f.in.ExpectedOwner, f.in.ExpectedSessionID, f.in.ExpectedRepo = f.in.Owner, f.in.SessionID, f.in.Repo
		out, err := handleReown(ctx, env, f.in)
		if err != nil {
			t.Fatalf("repair split handover: %v", err)
		}
		requireCommittedWriterHandover(t, f, out, 2)
	})
}

func TestIntegrationWriterHandoverRefusalsLeaveFullStateUnchanged(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()

	cases := map[string]func(*reownInput){
		"wrong owner":              func(in *reownInput) { in.ExpectedOwner = "wrong-owner" },
		"wrong session":            func(in *reownInput) { in.ExpectedSessionID = "wrong-session" },
		"wrong expected repo":      func(in *reownInput) { in.ExpectedRepo = "wrong-repo" },
		"wrong project":            func(in *reownInput) { in.ProjectID = "moonshot" },
		"wrong link":               func(in *reownInput) { in.WriterHandover.LinkID = "BND-absent" },
		"wrong writer repo":        func(in *reownInput) { in.Repo, in.WriterHandover.Repo = "wrong-repo", "wrong-repo" },
		"wrong branch":             func(in *reownInput) { in.WriterHandover.Branch = "wrong-branch" },
		"wrong path":               func(in *reownInput) { in.WriterHandover.WorktreePath = "/wrong" },
		"wrong old thread":         func(in *reownInput) { in.WriterHandover.Thread = "wrong-thread" },
		"wrong old writer session": func(in *reownInput) { in.WriterHandover.SessionID = "wrong-session" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			taskID := "HND-REFUSE-" + strings.ToUpper(strings.ReplaceAll(name, " ", "-"))
			f := newWriterHandoverIntegrationFixture(t, env, taskID)
			before := readWriterHandoverStoreState(t, ctx, f.client, taskID)
			in := cloneWriterHandoverInput(f.in)
			mutate(&in)
			if out, err := handleReown(ctx, env, in); err == nil {
				t.Fatalf("hostile handover succeeded: %+v", out)
			}
			requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, taskID), before)
		})
	}
}

func TestIntegrationWriterHandoverRefusesAmbiguousAndMalformedBindings(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()

	t.Run("multiple valid bindings", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-MULTIPLE")
		if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
			ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "BND-HND-MULTIPLE-2", ArtifactType: "worktree",
			ArtifactRef: "duplicate", Thread: "old-thread", SessionID: "old-session", Repo: "example-infra",
			Branch: "duplicate", WorktreePath: "/wt/duplicate",
		}); err != nil {
			t.Fatal(err)
		}
		before := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		if out, err := handleReown(ctx, env, f.in); err == nil {
			t.Fatalf("ambiguous handover succeeded: %+v", out)
		}
		requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID), before)
	})

	t.Run("valid selected binding plus incomplete competing writer", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-INCOMPLETE-COMPETITOR")
		if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
			ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "BND-HND-INCOMPLETE-COMPETITOR-2", ArtifactType: "worktree",
			ArtifactRef: "incomplete", Thread: "old-thread", SessionID: "old-session", Repo: "example-infra",
			Note: "incomplete competing writer intentionally lacks branch and worktree path",
		}); err != nil {
			t.Fatal(err)
		}
		before := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		if out, err := handleReown(ctx, env, f.in); err == nil {
			t.Fatalf("handover ignored an active incomplete competing writer: %+v", out)
		}
		requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID), before)
	})

	t.Run("repository alias competing writer", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-REPO-ALIAS-COMPETITOR")
		if _, err := handleAddWorklink(ctx, env, addWorklinkInput{
			ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "BND-HND-REPO-ALIAS-COMPETITOR-2", ArtifactType: "worktree",
			ArtifactRef: "https://github.com/velenzaboc/example-infra.git:other:/wt/other",
			Repo:        "https://github.com/velenzaboc/example-infra.git", Branch: "other", WorktreePath: "/wt/other",
			Thread: "other-thread", SessionID: "other-session",
		}); err != nil {
			t.Fatal(err)
		}
		before := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		if out, err := handleReown(ctx, env, f.in); err == nil || !strings.Contains(err.Error(), "2 active writer-identity binding worklinks") {
			t.Fatalf("repository-alias competing writer handover = %+v, %v", out, err)
		}
		requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID), before)
	})

	t.Run("malformed active binding", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-MALFORMED")
		_, err := f.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, err := tx.Update(ctx, spanner.Statement{
				SQL:    `UPDATE TaskLinks SET session_id = NULL WHERE project_id = 'arbor' AND link_id = @link_id`,
				Params: map[string]interface{}{"link_id": f.in.WriterHandover.LinkID},
			})
			return err
		})
		if err != nil {
			t.Fatalf("malform fixture: %v", err)
		}
		before := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		if out, err := handleReown(ctx, env, f.in); err == nil {
			t.Fatalf("malformed handover succeeded: %+v", out)
		}
		requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID), before)
	})
}

func TestIntegrationWriterHandoverConcurrentCallersConvergeOrLeaveOneWinner(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()

	t.Run("identical retries converge", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-RACE-SAME")
		const callers = 6
		type result struct {
			out MutationWriteResult
			err error
		}
		results := make(chan result, callers)
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, err := handleReown(ctx, env, f.in)
				results <- result{out: out, err: err}
			}()
		}
		wg.Wait()
		close(results)
		committed, idempotent := 0, 0
		for result := range results {
			if result.err != nil || result.out.WriterHandoverReceipt == nil {
				t.Fatalf("identical caller failed: %+v, %v", result.out, result.err)
			}
			switch {
			case result.out.WriterHandoverReceipt.RowsAffected == 3 && !result.out.WriterHandoverReceipt.Idempotent:
				committed++
			case result.out.WriterHandoverReceipt.RowsAffected == 0 && result.out.WriterHandoverReceipt.Idempotent:
				idempotent++
			default:
				t.Fatalf("unexpected identical receipt: %+v", result.out.WriterHandoverReceipt)
			}
		}
		if committed != 1 || idempotent != callers-1 {
			t.Fatalf("committed=%d idempotent=%d, want 1/%d", committed, idempotent, callers-1)
		}
		state := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		requireCoherentWriterTuple(t, state, f.in.WriterHandover.LinkID, false, true)
		if got := len(writerHandoverHistoryRows(state)); got != 1 {
			t.Fatalf("history rows = %d, want 1", got)
		}
	})

	t.Run("distinct contenders leave one winner", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-RACE-DISTINCT")
		type result struct {
			in  reownInput
			out MutationWriteResult
			err error
		}
		results := make(chan result, 3)
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			in := cloneWriterHandoverInput(f.in)
			in.Owner = fmt.Sprintf("new-owner-%d", i)
			in.SessionID = fmt.Sprintf("new-session-%d", i)
			in.WriterHandover.TargetThread = fmt.Sprintf("new-thread-%d", i)
			wg.Add(1)
			go func(in reownInput) {
				defer wg.Done()
				out, err := handleReown(ctx, env, in)
				results <- result{in: in, out: out, err: err}
			}(in)
		}
		wg.Wait()
		close(results)
		var winner *result
		losers := 0
		for result := range results {
			if result.err == nil {
				copy := result
				winner = &copy
			} else {
				losers++
			}
		}
		if winner == nil || losers != 2 {
			t.Fatalf("winner=%+v losers=%d, want one winner", winner, losers)
		}
		state := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		if state.Task.Owner != winner.in.Owner || state.Task.SessionID != winner.in.SessionID {
			t.Fatalf("task does not match winner: task=%+v winner=%+v", state.Task, winner.in)
		}
		var binding writerHandoverLinkState
		for _, link := range state.Links {
			if link.LinkID == f.in.WriterHandover.LinkID {
				binding = link
			}
		}
		if binding.Thread != winner.in.WriterHandover.TargetThread || binding.SessionID != winner.in.SessionID || len(writerHandoverHistoryRows(state)) != 1 {
			t.Fatalf("winner left mixed state: task=%+v binding=%+v history=%+v", state.Task, binding, writerHandoverHistoryRows(state))
		}
	})
}

func TestIntegrationWriterHandoverRollsBackEveryWriteStageAndCanRetry(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()

	for _, stage := range []string{"after_history", "after_task", "after_writer"} {
		t.Run(stage, func(t *testing.T) {
			f := newWriterHandoverIntegrationFixture(t, env, "HND-ROLLBACK-"+strings.ToUpper(stage))
			before := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
			boom := errors.New("injected writer handover failure at " + stage)
			nativeWriterHandoverFault = func(got string) error {
				if got == stage {
					return boom
				}
				return nil
			}
			t.Cleanup(func() { nativeWriterHandoverFault = nil })
			if out, err := handleReown(ctx, env, f.in); !errors.Is(err, boom) {
				t.Fatalf("faulted handover = %+v, %v, want injected failure", out, err)
			}
			nativeWriterHandoverFault = nil
			afterFailure := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
			requireWriterHandoverStateEqual(t, afterFailure, before)
			if got := len(writerHandoverHistoryRows(afterFailure)); got != 0 {
				t.Fatalf("history rows after rollback = %d, want 0", got)
			}
			out, err := handleReown(ctx, env, f.in)
			if err != nil {
				t.Fatalf("retry after rollback: %v", err)
			}
			requireCommittedWriterHandover(t, f, out, 3)
		})
	}
}

func TestIntegrationWriterHandoverCoherentReaderNeverSeesATornTuple(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()
	f := newWriterHandoverIntegrationFixture(t, env, "HND-COHERENT-READ")

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	nativeWriterHandoverFault = func(stage string) error {
		if stage == "after_task" {
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	}
	t.Cleanup(func() { nativeWriterHandoverFault = nil })
	type result struct {
		out MutationWriteResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := handleReown(ctx, env, f.in)
		done <- result{out: out, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handover did not reach the after_task transaction stage")
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	during := readWriterHandoverStoreState(t, readCtx, f.client, f.in.TaskID)
	cancel()
	requireCoherentWriterTuple(t, during, f.in.WriterHandover.LinkID, true, true)
	close(release)
	got := <-done
	nativeWriterHandoverFault = nil
	if got.err != nil {
		t.Fatalf("handover after coherent read: %v", got.err)
	}
	requireCommittedWriterHandover(t, f, got.out, 3)
}

func TestIntegrationWriterHandoverKeepsPlainReownAndHistoryBoundaries(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := supersedeIntegrationEnv(t)
	defer env.Substrates.Close()

	t.Run("protected binding requires writer handover", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-PLAIN-REFUSE")
		before := readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID)
		in := f.in
		in.WriterHandover = nil
		if out, err := handleReown(ctx, env, in); err == nil || !strings.Contains(err.Error(), "writer_handover") {
			t.Fatalf("plain reown beside protected binding = %+v, %v", out, err)
		}
		requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID), before)
	})

	t.Run("no binding preserves legitimate plain reown", func(t *testing.T) {
		const taskID = "HND-PLAIN-ALLOWED"
		createNativeTask(t, ctx, env, "arbor", taskID, taskID, "not_started")
		if _, err := handleClaim(ctx, env, claimInput{ProjectID: "arbor", TaskID: taskID, Owner: "old-owner", SessionID: "old-session", Repo: "example-infra"}); err != nil {
			t.Fatal(err)
		}
		out, err := handleReown(ctx, env, reownInput{
			ProjectID: "arbor", TaskID: taskID, Owner: "new-owner", SessionID: "new-session", Repo: "example-infra",
			ExpectedOwner: "old-owner", ExpectedSessionID: "old-session", ExpectedRepo: "example-infra",
		})
		if err != nil || out.NativeMutationReceipt == nil || out.NativeMutationReceipt.RowsAffected != 1 || out.WriterHandoverReceipt != nil {
			t.Fatalf("plain no-binding reown = %+v, %v", out, err)
		}
		state := readWriterHandoverStoreState(t, ctx, fClient(t, env), taskID)
		if state.Task.Owner != "new-owner" || state.Task.SessionID != "new-session" || state.Task.Repo != "example-infra" || len(state.Links) != 0 {
			t.Fatalf("plain no-binding state = %+v", state)
		}
	})

	t.Run("history cannot be forged changed or archived", func(t *testing.T) {
		f := newWriterHandoverIntegrationFixture(t, env, "HND-HISTORY-GUARD")
		out, err := handleReown(ctx, env, f.in)
		if err != nil {
			t.Fatal(err)
		}
		after := requireCommittedWriterHandover(t, f, out, 3)
		historyID := out.WriterHandoverReceipt.HistoryLinkID
		for name, route := range map[string]func() error{
			"add worklink": func() error {
				_, err := handleAddWorklink(ctx, env, addWorklinkInput{
					ProjectID: "arbor", TaskID: f.in.TaskID, ArtifactID: "FORGED-ADD", ArtifactType: "finding",
					ArtifactRef: "forged", Note: writerHandoverHistoryNote + `{}`,
				})
				return err
			},
			"generic create": func() error {
				_, err := handleCreate(ctx, env, createInput{
					ProjectID: "arbor", NodeKind: "artifact", StableID: "FORGED-CREATE",
					Contract: map[string]interface{}{"artifact_type_kind": "finding", "artifact_ref": "forged"},
					Payload:  map[string]interface{}{"task_id": f.in.TaskID, "note": writerHandoverHistoryNote + `{}`},
				})
				return err
			},
			"generic update": func() error {
				_, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: historyID, Payload: map[string]interface{}{"pr_state": "closed"}})
				return err
			},
			"typed archive": func() error {
				_, err := handleArchiveWorklink(ctx, env, archiveWorklinkInput{ProjectID: "arbor", TaskID: f.in.TaskID, LinkID: historyID})
				return err
			},
		} {
			t.Run(name, func(t *testing.T) {
				if err := route(); err == nil || !strings.Contains(err.Error(), writerHandoverHistoryNote) {
					t.Fatalf("route error = %v, want reserved history refusal", err)
				}
				requireWriterHandoverStateEqual(t, readWriterHandoverStoreState(t, ctx, f.client, f.in.TaskID), after)
			})
		}
	})
}

func fClient(t *testing.T, env *Env) *spanner.Client {
	t.Helper()
	adapter, err := soleNativeFleetPlanAdapter(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	client, err := adapter.getClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return client
}
