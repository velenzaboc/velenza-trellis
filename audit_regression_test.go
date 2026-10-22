package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
	newClaimedEnv := func(t *testing.T) *Env {
		t.Helper()
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
		return env
	}

	t.Run("legitimate updates and dedicated reown", func(t *testing.T) {
		env := newClaimedEnv(t)
		ctx := context.Background()
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
		if _, err := nativeHandleReown(ctx, env, reownInput{
			ProjectID: "arbor", TaskID: "AUDIT-OWNER", Owner: "owner-b", SessionID: "session-b", Repo: "task-graph",
			ExpectedOwner: "owner-a", ExpectedSessionID: "session-a", ExpectedRepo: "task-graph",
		}); err != nil {
			t.Fatalf("dedicated compare-and-set reown: %v", err)
		}
	})

	for _, tc := range []struct {
		name    string
		payload map[string]interface{}
	}{
		{name: "owner", payload: map[string]interface{}{"owner": "owner-b"}},
		{name: "session", payload: map[string]interface{}{"session_id": "session-b"}},
		{name: "repository", payload: map[string]interface{}{"repo": "other-repo"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newClaimedEnv(t)
			ctx := context.Background()
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
}

func auditCreateTasks(t *testing.T, env *Env, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := nativeHandleCreate(context.Background(), env, coverageTaskInput(id)); err != nil {
			t.Fatalf("create task %s: %v", id, err)
		}
	}
}

func auditRunnerInputFile(t *testing.T, in FGValidatorRunnerInput, dependencyKinds []map[string]interface{}) string {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal runner input: %v", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode runner input document: %v", err)
	}
	if dependencyKinds != nil {
		encoded, err := json.Marshal(dependencyKinds)
		if err != nil {
			t.Fatalf("marshal dependency registry: %v", err)
		}
		document["dependency_kinds"] = encoded
	} else {
		delete(document, "dependency_kinds")
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

func auditDependencyRegistry(t *testing.T) ([]map[string]interface{}, []string, string) {
	t.Helper()
	raw, err := os.ReadFile("task-graph.config.json")
	if err != nil {
		t.Fatalf("read task graph config: %v", err)
	}
	var document struct {
		DependencyKinds []map[string]interface{} `json:"dependency_kinds"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode task graph config: %v", err)
	}
	if len(document.DependencyKinds) == 0 {
		t.Fatal("task graph config has no dependency_kinds")
	}

	const syntheticBlockingAlias = "audit_gate"
	const syntheticNonblockingAlias = "audit_ref"
	var blockingAliases []string
	foundBlocking, foundNonblocking := false, false
	for _, descriptor := range document.DependencyKinds {
		name, _ := descriptor["name"].(string)
		absorbs, ok := descriptor["absorbs"].([]interface{})
		if !ok {
			t.Fatalf("dependency kind %q has non-array absorbs: %#v", name, descriptor["absorbs"])
		}
		switch name {
		case "gates":
			if blocking, _ := descriptor["blocking"].(bool); !blocking {
				t.Fatalf("configured gates descriptor is not blocking: %#v", descriptor)
			}
			absorbs = append(absorbs, syntheticBlockingAlias)
			descriptor["absorbs"] = absorbs
			for _, alias := range absorbs {
				value, ok := alias.(string)
				if !ok || value == "" {
					t.Fatalf("configured gates alias is invalid: %#v", alias)
				}
				blockingAliases = append(blockingAliases, value)
			}
			foundBlocking = true
		case "source_map":
			if blocking, _ := descriptor["blocking"].(bool); blocking {
				t.Fatalf("configured source_map descriptor is blocking: %#v", descriptor)
			}
			descriptor["absorbs"] = append(absorbs, syntheticNonblockingAlias)
			foundNonblocking = true
		}
	}
	if !foundBlocking || !foundNonblocking {
		t.Fatalf("dependency registry missing gates/source_map descriptors: %#v", document.DependencyKinds)
	}
	return document.DependencyKinds, blockingAliases, syntheticNonblockingAlias
}

func auditRunValidator(t *testing.T, in FGValidatorRunnerInput, dependencyKinds []map[string]interface{}) (FGValidatorRunnerResult, error) {
	t.Helper()
	var stdout strings.Builder
	err := cmdFGValidatorRunner(context.Background(), []string{auditRunnerInputFile(t, in, dependencyKinds)}, &stdout)
	var got FGValidatorRunnerResult
	if decodeErr := json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &got); decodeErr != nil {
		t.Fatalf("decode runner output: %v (command error: %v)\n%s", decodeErr, err, stdout.String())
	}
	return got, err
}

func auditAttachDependencyRegistry(t *testing.T, in FGValidatorRunnerInput, dependencyKinds []map[string]interface{}) FGValidatorRunnerInput {
	t.Helper()
	field := reflect.ValueOf(&in).Elem().FieldByName("DependencyKinds")
	if !field.IsValid() {
		return in
	}
	encoded, err := json.Marshal(dependencyKinds)
	if err != nil {
		t.Fatalf("marshal dependency registry: %v", err)
	}
	value := reflect.New(field.Type())
	if err := json.Unmarshal(encoded, value.Interface()); err != nil {
		t.Fatalf("decode dependency registry into runner input: %v", err)
	}
	field.Set(value.Elem())
	return in
}

func TestAuditValidatorRunnerRequiresDependencyKindRegistry(t *testing.T) {
	in := fgRunnerGoodInput(t)
	var stdout strings.Builder
	if err := cmdFGValidatorRunner(context.Background(), []string{auditRunnerInputFile(t, in, nil)}, &stdout); err == nil {
		t.Fatalf("validator accepted input without dependency_kinds registry: %s", stdout.String())
	}
}

func TestAuditValidatorRunnerCanonicalizesRegisteredBlockingDependencyAliases(t *testing.T) {
	registry, aliases, nonblockingAlias := auditDependencyRegistry(t)
	for _, alias := range aliases {
		t.Run(alias, func(t *testing.T) {
			in := fgRunnerGoodInput(t)
			prerequisite := fgRunnerTask("AUDIT-PREREQUISITE", "MVP-ACTIVATE-00", "not_started")
			prerequisite.Level = "L5"
			dependent := fgRunnerTask("AUDIT-DEPENDENT", "MVP-ACTIVATE-00", "not_started")
			dependent.Level = "L5"
			dependent.Note = "leaf_prerequisites=AUDIT-PREREQUISITE"
			nonblockingSource := fgRunnerTask("AUDIT-REF-SOURCE", "MVP-ACTIVATE-00", "not_started")
			nonblockingSource.Level = "L5"
			nonblockingTarget := fgRunnerTask("AUDIT-REF-TARGET", "MVP-ACTIVATE-00", "not_started")
			nonblockingTarget.Level = "L5"
			in.Snapshots.FleetGovernance.Tasks = append(in.Snapshots.FleetGovernance.Tasks, prerequisite, dependent, nonblockingSource, nonblockingTarget)
			in.Snapshots.FleetGovernance.Dependencies = []DependencyRow{
				{
					ProjectID: fgRunnerProject, DependencyID: "AUDIT-DEPENDENCY", TaskID: dependent.TaskID,
					DependsOnTaskID: prerequisite.TaskID, Kind: alias, CreatedAt: "2026-09-30T22:00:00Z",
				},
				{
					ProjectID: fgRunnerProject, DependencyID: "AUDIT-NONBLOCKING", TaskID: nonblockingTarget.TaskID,
					DependsOnTaskID: nonblockingSource.TaskID, Kind: nonblockingAlias, CreatedAt: "2026-09-30T22:00:00Z",
				},
			}
			in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, prerequisite, dependent, nonblockingSource, nonblockingTarget)
			fgRunnerReseal(t, &in.Snapshots.FleetGovernance)
			fgRunnerReseal(t, &in.Snapshots.Board)

			got, err := auditRunValidator(t, in, registry)
			if err != nil {
				t.Fatalf("runner rejected configured dependency alias %q: %v", alias, err)
			}
			if !got.Passed || !got.Checks.DependencyIntegrity.Passed || got.Checks.DependencyIntegrity.Reciprocity.LogicalEdgeCount != 1 {
				t.Fatalf("alias %q and nonblocking control were not registry-classified as exactly one blocking edge: %+v", alias, got.Checks.DependencyIntegrity)
			}
		})
	}
}

func TestAuditValidatorRunnerRejectsCycleCarriedByRegisteredAliases(t *testing.T) {
	registry, aliases, _ := auditDependencyRegistry(t)
	if len(aliases) < 2 {
		t.Fatalf("need at least two configured blocking aliases, got %v", aliases)
	}
	in := fgRunnerGoodInput(t)
	a := fgRunnerTask("AUDIT-CYCLE-A", "MVP-ACTIVATE-00", "not_started")
	a.Level = "L5"
	a.Note = "leaf_prerequisites=AUDIT-CYCLE-B"
	b := fgRunnerTask("AUDIT-CYCLE-B", "MVP-ACTIVATE-00", "not_started")
	b.Level = "L5"
	b.Note = "leaf_prerequisites=AUDIT-CYCLE-A"
	in.Snapshots.FleetGovernance.Tasks = append(in.Snapshots.FleetGovernance.Tasks, a, b)
	in.Snapshots.FleetGovernance.Dependencies = []DependencyRow{
		{ProjectID: fgRunnerProject, DependencyID: "AUDIT-CYCLE-AB", TaskID: b.TaskID, DependsOnTaskID: a.TaskID, Kind: aliases[0], CreatedAt: "2026-09-30T22:00:00Z"},
		{ProjectID: fgRunnerProject, DependencyID: "AUDIT-CYCLE-BA", TaskID: a.TaskID, DependsOnTaskID: b.TaskID, Kind: aliases[len(aliases)-1], CreatedAt: "2026-09-30T22:00:00Z"},
	}
	in.Snapshots.Board.Tasks = append(in.Snapshots.Board.Tasks, a, b)
	fgRunnerReseal(t, &in.Snapshots.FleetGovernance)
	fgRunnerReseal(t, &in.Snapshots.Board)

	got, _ := auditRunValidator(t, in, registry)
	dependency := got.Checks.DependencyIntegrity
	if got.Passed || dependency.Passed || dependency.Cycle.Passed {
		t.Fatalf("registered-alias cycle passed: overall=%t dependency=%+v", got.Passed, dependency)
	}
	if dependency.Reciprocity.LogicalEdgeCount != 2 {
		t.Fatalf("registered-alias cycle logical edges = %d, want 2", dependency.Reciprocity.LogicalEdgeCount)
	}
	if path := strings.Join(dependency.Cycle.Path, "->"); path != "AUDIT-CYCLE-A->AUDIT-CYCLE-B->AUDIT-CYCLE-A" {
		t.Fatalf("registered-alias cycle path = %q, want deterministic A->B->A", path)
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
	registry, _, _ := auditDependencyRegistry(t)
	in := fgRunnerGoodInput(t)
	in.Snapshots.FleetGovernance.Tasks[0].Level = "L4"
	in.Snapshots.FleetGovernance.Tasks[0].Note = "dependencies=AUDIT-MISSING-PREREQUISITE"
	fgRunnerReseal(t, &in.Snapshots.FleetGovernance)
	in = auditAttachDependencyRegistry(t, in, registry)

	libraryResult, err := RunFGValidatorRunner(context.Background(), in)
	if err != nil {
		t.Fatalf("library validator returned execution error for semantic failure: %v", err)
	}
	if libraryResult.Passed {
		t.Fatalf("library validator passed known semantic failure")
	}

	path := auditRunnerInputFile(t, in, registry)
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
