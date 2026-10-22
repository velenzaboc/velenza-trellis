package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func auditReadTask(t *testing.T, env *Env, taskID string) Node {
	t.Helper()
	node, found, err := nativeReadTaskNode(context.Background(), nativeCoverageAdapter(t, env), "arbor", taskID)
	if err != nil {
		t.Fatalf("read task %s: %v", taskID, err)
	}
	if !found {
		t.Fatalf("task %s not found", taskID)
	}
	return node
}

func TestAuditGenericTaskUpdateCannotChangeOwnershipTuple(t *testing.T) {
	env, _ := newNativeCoverageEnv(t)
	ctx := context.Background()
	if _, err := nativeHandleCreate(ctx, env, coverageTaskInput("AUDIT-OWNER")); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := nativeHandleClaim(ctx, env, claimInput{
		ProjectID: "arbor", TaskID: "AUDIT-OWNER", Owner: "owner-a", SessionID: "session-a", Repo: "task-graph",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	if _, err := handleUpdate(ctx, env, updateInput{
		ProjectID: "arbor", NodeID: "AUDIT-OWNER", Payload: map[string]interface{}{"title": "ordinary metadata"},
	}); err != nil {
		t.Fatalf("ordinary metadata update: %v", err)
	}
	if _, err := handleUpdate(ctx, env, updateInput{
		ProjectID: "arbor", NodeID: "AUDIT-OWNER", Payload: map[string]interface{}{
			"owner": "owner-a", "session_id": "session-a", "repo": "task-graph", "title": "roundtrip",
		},
	}); err != nil {
		t.Fatalf("unchanged ownership roundtrip: %v", err)
	}

	for _, tc := range []struct {
		name    string
		payload map[string]interface{}
	}{
		{name: "owner", payload: map[string]interface{}{"owner": "owner-b"}},
		{name: "session", payload: map[string]interface{}{"session_id": "session-b"}},
		{name: "repository", payload: map[string]interface{}{"repo": "other-repo"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if out, err := handleUpdate(ctx, env, updateInput{ProjectID: "arbor", NodeID: "AUDIT-OWNER", Payload: tc.payload}); err == nil {
				t.Fatalf("generic ownership mutation succeeded: %+v", out)
			}
			node := auditReadTask(t, env, "AUDIT-OWNER")
			if got := stringFromMap(node.CurrentVersion.Payload, "owner"); got != "owner-a" {
				t.Fatalf("owner changed after refused update: %q", got)
			}
			if got := stringFromMap(node.CurrentVersion.Payload, "session_id"); got != "session-a" {
				t.Fatalf("session changed after refused update: %q", got)
			}
			if got := stringFromMap(node.CurrentVersion.Payload, "repo"); got != "task-graph" {
				t.Fatalf("repository changed after refused update: %q", got)
			}
		})
	}

	if _, err := nativeHandleReown(ctx, env, reownInput{
		ProjectID: "arbor", TaskID: "AUDIT-OWNER", Owner: "owner-b", SessionID: "session-b", Repo: "task-graph",
		ExpectedOwner: "owner-a", ExpectedSessionID: "session-a", ExpectedRepo: "task-graph",
	}); err != nil {
		t.Fatalf("dedicated compare-and-set reown: %v", err)
	}
}

func auditCreateTasks(t *testing.T, env *Env, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := nativeHandleCreate(context.Background(), env, coverageTaskInput(id)); err != nil {
			t.Fatalf("create task %s: %v", id, err)
		}
	}
}

func TestAuditTaskParentMutationMaintainsTree(t *testing.T) {
	t.Run("valid parent link", func(t *testing.T) {
		env, _ := newNativeCoverageEnv(t)
		auditCreateTasks(t, env, "AUDIT-PARENT", "AUDIT-CHILD")
		if _, err := handleLink(context.Background(), env, linkInput{
			ProjectID: "arbor", SourceID: "AUDIT-PARENT", TargetID: "AUDIT-CHILD", EdgeLabel: "parent_of",
		}); err != nil {
			t.Fatalf("valid parent link: %v", err)
		}
		if got := stringFromMap(auditReadTask(t, env, "AUDIT-CHILD").CurrentVersion.Payload, "parent_id"); got != "AUDIT-PARENT" {
			t.Fatalf("child parent = %q, want AUDIT-PARENT", got)
		}
	})

	for _, tc := range []struct {
		name     string
		parentID string
	}{
		{name: "missing parent", parentID: "AUDIT-MISSING"},
		{name: "self parent", parentID: "AUDIT-CHILD"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := newNativeCoverageEnv(t)
			auditCreateTasks(t, env, "AUDIT-CHILD")
			if out, err := handleUpdate(context.Background(), env, updateInput{
				ProjectID: "arbor", NodeID: "AUDIT-CHILD", Payload: map[string]interface{}{"parent_id": tc.parentID},
			}); err == nil {
				t.Fatalf("invalid generic parent update succeeded: %+v", out)
			}
			if got := stringFromMap(auditReadTask(t, env, "AUDIT-CHILD").CurrentVersion.Payload, "parent_id"); got != "" {
				t.Fatalf("refused parent update wrote %q", got)
			}
		})
	}

	t.Run("multi hop cycle", func(t *testing.T) {
		env, _ := newNativeCoverageEnv(t)
		auditCreateTasks(t, env, "AUDIT-A", "AUDIT-B", "AUDIT-C")
		for _, edge := range [][2]string{{"AUDIT-A", "AUDIT-B"}, {"AUDIT-B", "AUDIT-C"}} {
			if _, err := handleLink(context.Background(), env, linkInput{
				ProjectID: "arbor", SourceID: edge[0], TargetID: edge[1], EdgeLabel: "parent_of",
			}); err != nil {
				t.Fatalf("seed valid parent link %s -> %s: %v", edge[0], edge[1], err)
			}
		}
		if out, err := handleUpdate(context.Background(), env, updateInput{
			ProjectID: "arbor", NodeID: "AUDIT-A", Payload: map[string]interface{}{"parent_id": "AUDIT-C"},
		}); err == nil {
			t.Fatalf("cycle-closing update succeeded: %+v", out)
		}
		if got := stringFromMap(auditReadTask(t, env, "AUDIT-A").CurrentVersion.Payload, "parent_id"); got != "" {
			t.Fatalf("cycle-closing update wrote parent %q", got)
		}
	})

	t.Run("cross project endpoint", func(t *testing.T) {
		env, _ := newNativeCoverageEnv(t)
		auditCreateTasks(t, env, "AUDIT-LOCAL")
		foreign := coverageTaskInput("AUDIT-FOREIGN")
		foreign.ProjectID = "moonshot"
		if _, err := nativeHandleCreate(context.Background(), env, foreign); err != nil {
			t.Fatalf("create foreign project task: %v", err)
		}
		if out, err := handleLink(context.Background(), env, linkInput{
			ProjectID: "arbor", SourceID: "AUDIT-FOREIGN", TargetID: "AUDIT-LOCAL", EdgeLabel: "parent_of",
		}); err == nil {
			t.Fatalf("cross-project parent link succeeded: %+v", out)
		}
	})

	t.Run("concurrent opposite links", func(t *testing.T) {
		env, _ := newNativeCoverageEnv(t)
		auditCreateTasks(t, env, "AUDIT-X", "AUDIT-Y")
		start := make(chan struct{})
		errs := make(chan error, 2)
		link := func(parent, child string) {
			<-start
			_, err := handleLink(context.Background(), env, linkInput{
				ProjectID: "arbor", SourceID: parent, TargetID: child, EdgeLabel: "parent_of",
			})
			errs <- err
		}
		go link("AUDIT-X", "AUDIT-Y")
		go link("AUDIT-Y", "AUDIT-X")
		close(start)
		successes := 0
		for i := 0; i < 2; i++ {
			if err := <-errs; err == nil {
				successes++
			}
		}
		if successes != 1 {
			t.Fatalf("concurrent opposite links had %d successes, want exactly one", successes)
		}
		xParent := stringFromMap(auditReadTask(t, env, "AUDIT-X").CurrentVersion.Payload, "parent_id")
		yParent := stringFromMap(auditReadTask(t, env, "AUDIT-Y").CurrentVersion.Payload, "parent_id")
		if xParent == "AUDIT-Y" && yParent == "AUDIT-X" {
			t.Fatalf("concurrent opposite links committed a two-node cycle")
		}
	})
}

func auditRunnerInputFile(t *testing.T, in FGValidatorRunnerInput, includeDependencyKinds bool) string {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal runner input: %v", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode runner input document: %v", err)
	}
	if includeDependencyKinds {
		configRaw, err := os.ReadFile("task-graph.config.json")
		if err != nil {
			t.Fatalf("read task graph config: %v", err)
		}
		var configDocument map[string]json.RawMessage
		if err := json.Unmarshal(configRaw, &configDocument); err != nil {
			t.Fatalf("decode task graph config: %v", err)
		}
		dependencyKinds, ok := configDocument["dependency_kinds"]
		if !ok {
			t.Fatal("task graph config has no dependency_kinds")
		}
		document["dependency_kinds"] = dependencyKinds
	}
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatalf("marshal runner document: %v", err)
	}
	path := filepath.Join(t.TempDir(), "runner-input.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatalf("write runner input: %v", err)
	}
	return path
}

func TestAuditValidatorRunnerCanonicalizesRegisteredBlockingDependencyAliases(t *testing.T) {
	aliases := []string{
		"gates", "blocks", "blocks_until_done", "hard", "gated_by",
		"finding_gate", "validation_gate", "composition_gate", "requires",
	}
	for _, alias := range aliases {
		t.Run(alias, func(t *testing.T) {
			in := fgRunnerGoodInput(t)
			prerequisite := fgRunnerTask("AUDIT-PREREQUISITE", "MVP-ACTIVATE-00", "not_started")
			prerequisite.Level = "L5"
			dependent := fgRunnerTask("AUDIT-DEPENDENT", "MVP-ACTIVATE-00", "not_started")
			dependent.Level = "L5"
			dependent.Note = "leaf_prerequisites=AUDIT-PREREQUISITE"
			in.Snapshots.FleetGovernance.Tasks = append(in.Snapshots.FleetGovernance.Tasks, prerequisite, dependent)
			in.Snapshots.FleetGovernance.Dependencies = []DependencyRow{{
				ProjectID: fgRunnerProject, DependencyID: "AUDIT-DEPENDENCY", TaskID: dependent.TaskID,
				DependsOnTaskID: prerequisite.TaskID, Kind: alias, CreatedAt: "2026-09-30T22:00:00Z",
			}}
			in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, prerequisite, dependent)
			fgRunnerReseal(t, &in.Snapshots.FleetGovernance)
			fgRunnerReseal(t, &in.Snapshots.Board)

			var stdout strings.Builder
			if err := cmdFGValidatorRunner(context.Background(), []string{auditRunnerInputFile(t, in, true)}, &stdout); err != nil {
				t.Fatalf("runner rejected configured dependency alias %q: %v", alias, err)
			}
			var got FGValidatorRunnerResult
			if err := json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &got); err != nil {
				t.Fatalf("decode runner output: %v\n%s", err, stdout.String())
			}
			if !got.Passed || !got.Checks.DependencyIntegrity.Passed || got.Checks.DependencyIntegrity.Reciprocity.LogicalEdgeCount != 1 {
				t.Fatalf("alias %q was not judged as one blocking edge: %+v", alias, got.Checks.DependencyIntegrity)
			}
		})
	}
}

func TestAuditRetiredWorklinksCannotSatisfyTerminalProof(t *testing.T) {
	env, client := newNativeCoverageEnv(t)
	ctx := context.Background()
	auditCreateTasks(t, env, "AUDIT-ACCEPTANCE-ROOT")
	inactive := []string{"archived", "superseded", "released", "closed", " ARCHIVED ", "Superseded", " RELEASED ", "Closed"}
	expectedFindings := map[string]bool{}
	for i, status := range inactive {
		taskID := fmt.Sprintf("AUDIT-RETIRED-%02d", i)
		in := coverageTaskInput(taskID)
		in.ParentRef = "AUDIT-ACCEPTANCE-ROOT"
		in.Payload["status"] = "done"
		if _, err := nativeHandleCreate(ctx, env, in); err != nil {
			t.Fatalf("create done task %s: %v", taskID, err)
		}
		linkID := fmt.Sprintf("AUDIT-PROOF-%02d", i)
		if _, err := nativeHandleAddWorklink(ctx, env, addWorklinkInput{
			ProjectID: "arbor", TaskID: taskID, ArtifactID: linkID, ArtifactType: "validation", ArtifactRef: "proof:" + taskID,
		}); err != nil {
			t.Fatalf("create proof %s: %v", linkID, err)
		}
		setStoredStatus(t, client, linkID, status)
		expectedFindings[taskID] = true
	}

	active := coverageTaskInput("AUDIT-ACTIVE-PROOF")
	active.ParentRef = "AUDIT-ACCEPTANCE-ROOT"
	active.Payload["status"] = "done"
	if _, err := nativeHandleCreate(ctx, env, active); err != nil {
		t.Fatalf("create active-proof task: %v", err)
	}
	if _, err := nativeHandleAddWorklink(ctx, env, addWorklinkInput{
		ProjectID: "arbor", TaskID: "AUDIT-ACTIVE-PROOF", ArtifactID: "AUDIT-ACTIVE-LINK",
		ArtifactType: "validation", ArtifactRef: "proof:active",
	}); err != nil {
		t.Fatalf("create active proof: %v", err)
	}

	snapshot, err := handleProjectSnapshot(ctx, env, projectSnapshotInput{ProjectID: "arbor"})
	if err != nil {
		t.Fatalf("project snapshot: %v", err)
	}
	tasks, links, err := fgValidatorTerminalInput(snapshot)
	if err != nil {
		t.Fatalf("terminal input: %v", err)
	}
	result, err := RunFGTerminalProofCheck(ctx, env.TaskStatuses, env.ArtifactKinds, tasks, links)
	if err != nil {
		t.Fatalf("terminal proof check: %v", err)
	}
	if len(result.Findings) != len(expectedFindings) {
		t.Fatalf("terminal findings = %d, want %d; findings=%+v snapshot_links=%+v", len(result.Findings), len(expectedFindings), result.Findings, snapshot.Worklinks)
	}
	for _, finding := range result.Findings {
		if finding.Kind != FGFindingDoneWithoutProofKind || !expectedFindings[finding.TaskID] {
			t.Fatalf("unexpected terminal finding: %+v", finding)
		}
		delete(expectedFindings, finding.TaskID)
	}
	if len(expectedFindings) != 0 {
		t.Fatalf("retired proof tasks without findings: %v", expectedFindings)
	}
}

func TestAuditSnapshotContinuationSecretsAreDistinctAcrossRepresentations(t *testing.T) {
	keyMaterial := []byte("01234567890123456789012345678901")
	encoded := base64.StdEncoding.EncodeToString(keyMaterial)
	config := &SnapshotContinuationConfig{Mode: "hmac_sha256_base64_env", KeyEnv: "AUDIT_SNAPSHOT_KEY", TTLSeconds: 300}
	t.Setenv("AUDIT_TRANSPORT_SECRET", encoded)
	bearer, err := resolveTransportSecret(&TransportAuthConfig{Mode: "bearer_env", SecretEnv: "AUDIT_TRANSPORT_SECRET"})
	if err != nil {
		t.Fatalf("resolve bearer: %v", err)
	}
	for _, tc := range []struct {
		name    string
		encoded string
	}{
		{name: "same literal", encoded: encoded},
		{name: "equivalent line wrapped base64", encoded: encoded[:16] + "\n" + encoded[16:]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AUDIT_SNAPSHOT_KEY", tc.encoded)
			if key, ttl, err := resolveSnapshotContinuation(config, bearer); err == nil {
				t.Fatalf("reused credential accepted: key=%x ttl=%s", key, ttl)
			}
		})
	}

	distinct := []byte("abcdefghijklmnopqrstuvwxyzABCDEF")
	t.Setenv("AUDIT_SNAPSHOT_KEY", base64.StdEncoding.EncodeToString(distinct))
	key, ttl, err := resolveSnapshotContinuation(config, bearer)
	if err != nil {
		t.Fatalf("distinct continuation secret rejected: %v", err)
	}
	if string(key) != string(distinct) || ttl != 5*time.Minute {
		t.Fatalf("distinct continuation secret = %x, %s", key, ttl)
	}
}

func TestAuditValidatorCLIFailsProcessStatusAndPreservesJSON(t *testing.T) {
	in := fgRunnerGoodInput(t)
	in.Snapshots.FleetGovernance.Tasks[0].Level = "L4"
	in.Snapshots.FleetGovernance.Tasks[0].Note = "dependencies=AUDIT-MISSING-PREREQUISITE"
	fgRunnerReseal(t, &in.Snapshots.FleetGovernance)

	libraryResult, err := RunFGValidatorRunner(context.Background(), in)
	if err != nil {
		t.Fatalf("library validator returned execution error for semantic failure: %v", err)
	}
	if libraryResult.Passed {
		t.Fatalf("library validator passed known semantic failure")
	}

	path := auditRunnerInputFile(t, in, false)
	var stdout strings.Builder
	err = cmdFGValidatorRunner(context.Background(), []string{path}, &stdout)
	if err == nil {
		t.Fatal("validator command returned success for Passed:false result")
	}
	var commandResult FGValidatorRunnerResult
	if decodeErr := json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &commandResult); decodeErr != nil {
		t.Fatalf("validator command did not preserve JSON result: %v\n%s", decodeErr, stdout.String())
	}
	if commandResult.Passed {
		t.Fatalf("validator command JSON passed known semantic failure")
	}

	code, cliErr := runCLI(context.Background(), []string{"velenza-trellis", "fg-validator-runner", path})
	if code == 0 || cliErr == nil {
		t.Fatalf("validator CLI = code %d, err %v; want nonzero status and semantic failure", code, cliErr)
	}
}
