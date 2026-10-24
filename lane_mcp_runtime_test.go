package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLaneMCPRuntimeServesRegisteredTools(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := validIdentityBoundConfig()
	cfg.ListenAddress = address
	cfg.Embedding = nil
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "task-graph.json")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TASK_GRAPH_CONFIG", configPath)

	serverCtx, cancelServer := context.WithCancel(context.Background())
	serverErr := make(chan error, 1)
	serverStopped := false
	go func() { serverErr <- cmdMCP(serverCtx, nil) }()
	t.Cleanup(func() {
		if serverStopped {
			return
		}
		cancelServer()
		select {
		case <-serverErr:
		case <-time.After(2 * time.Second):
			t.Error("MCP server did not stop")
		}
	})

	endpoint := "http://" + address
	healthClient := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, requestErr := healthClient.Get(endpoint + "/healthz")
		if requestErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("MCP health endpoint did not become ready: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "lane-test", Version: "v1"}, nil)
	callCtx, cancelCalls := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCalls()
	session, err := client.Connect(callCtx, &mcp.StreamableClientTransport{Endpoint: endpoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	listed, err := session.ListTools(callCtx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Tools) != 28 {
		t.Fatalf("registered tools = %d, want 28", len(listed.Tools))
	}
	toolNames := map[string]bool{}
	for _, tool := range listed.Tools { toolNames[tool.Name] = true }
	for _, name := range []string{"task_graph_review_produce", "task_graph_review_get", "task_graph_tdd_produce", "task_graph_tdd_get"} {
		if !toolNames[name] { t.Fatalf("required typed tool %s is not registered", name) }
	}

	calls := []struct {
		name string
		args map[string]any
	}{
		{"task_graph_use", map[string]any{}},
		{"task_graph_create", map[string]any{"project_id": "other", "node_kind": "task", "contract": map[string]any{}, "payload": map[string]any{}}},
		{"task_graph_update", map[string]any{"project_id": "other", "node_id": "T", "contract": map[string]any{}, "payload": map[string]any{}}},
		{"task_graph_link", map[string]any{"project_id": "other", "source_id": "A", "target_id": "B", "edge_label": "gates"}},
		{"task_graph_unlink", map[string]any{"edge_id": ""}},
		{"task_graph_get", map[string]any{"project_id": "other", "node_id": "T"}},
		{"task_graph_list", map[string]any{"project_id": "other"}},
		{"task_graph_search", map[string]any{"project_id": "other", "query": "coverage"}},
		{"task_graph_neighbors", map[string]any{"project_id": "other", "node_id": "T"}},
		{"task_graph_closure", map[string]any{"project_id": "other", "node_id": "T", "closure_profile": "dep", "max_depth": 1, "result_cap": 1}},
		{"task_graph_impact", map[string]any{"project_id": "other", "node_id": "T", "max_depth": 1, "result_cap": 1}},
		{"task_graph_query", map[string]any{"sql": "DELETE FROM Tasks"}},
		{"task_graph_import", map[string]any{"submissions": []any{}}},
		{"task_graph_tasks", map[string]any{"project_id": "other"}},
		{"task_graph_project_snapshot", map[string]any{"project_id": "other"}},
		{"task_graph_task_counts", map[string]any{"project_id": "other"}},
		{"task_graph_worklinks", map[string]any{"project_id": "other", "task_id": "T"}},
		{"task_graph_add_worklink", map[string]any{"project_id": "other", "task_id": "T", "artifact_type": "finding", "artifact_ref": "ref"}},
		{"task_graph_archive_worklink", map[string]any{"project_id": "other", "task_id": "T", "link_id": "L"}},
		{"task_graph_supersede_worklink", map[string]any{"project_id": "other", "task_id": "T", "expected_owner": "owner", "expected_session_id": "session", "predecessor_link_id": "BND-old",
			"expected_predecessor": map[string]any{"artifact_type": "worktree", "artifact_status": "recorded", "repo": "example-infra", "branch": "old", "worktree_path": "/wt/old", "thread": "thread", "session_id": "session"},
			"successor":            map[string]any{"link_id": "BND-new", "artifact_type": "worktree", "artifact_ref": "ref", "repo": "example-infra", "branch": "new", "worktree_path": "/wt/new", "thread": "thread", "session_id": "session"}}},
		{"task_graph_set_status", map[string]any{"project_id": "other", "task_id": "T", "status": "in_progress"}},
		{"task_graph_reown", map[string]any{"project_id": "other", "task_id": "T", "owner": "UNOWNED"}},
		{"task_graph_claim", map[string]any{"project_id": "other", "task_id": "T", "owner": "owner", "session_id": "session", "repo": "task-graph"}},
	}
	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			result, callErr := session.CallTool(callCtx, &mcp.CallToolParams{Name: call.name, Arguments: call.args})
			if callErr != nil {
				t.Fatalf("transport/schema call failed before handler: %v", callErr)
			}
			if result == nil {
				t.Fatal("nil tool result")
			}
			if call.name == "task_graph_use" {
				if result.IsError {
					t.Fatalf("snapshot pin read failed: %#v", result)
				}
				return
			}
			if !result.IsError {
				t.Fatalf("fail-closed test input unexpectedly succeeded: %#v", result)
			}
		})
	}

	cancelServer()
	if err := <-serverErr; err != nil {
		t.Fatalf("cmdMCP returned error: %v", err)
	}
	serverStopped = true
}

func TestLaneMCPHelpersCoverSuccessAndFailureShapes(t *testing.T) {
	if secret, err := resolveTransportSecret(nil); err != nil || secret != "" {
		t.Fatalf("nil transport auth = %q, %v", secret, err)
	}
	t.Setenv("LANE_TRANSPORT_SECRET", "  value  ")
	secret, err := resolveTransportSecret(&TransportAuthConfig{Mode: "bearer_env", SecretEnv: "LANE_TRANSPORT_SECRET"})
	if err != nil || secret != "value" {
		t.Fatalf("resolved secret = %q, %v", secret, err)
	}

	if key, ttl, err := resolveSnapshotContinuation(nil, ""); err != nil || key != nil || ttl != 0 {
		t.Fatalf("nil snapshot continuation = %x, %s, %v", key, ttl, err)
	}
	keyMaterial := []byte("01234567890123456789012345678901")
	t.Setenv("LANE_SNAPSHOT_KEY", base64.StdEncoding.EncodeToString(keyMaterial))
	key, ttl, err := resolveSnapshotContinuation(&SnapshotContinuationConfig{
		Mode: "hmac_sha256_base64_env", KeyEnv: "LANE_SNAPSHOT_KEY", TTLSeconds: 300,
	}, "different bearer")
	if err != nil || string(key) != string(keyMaterial) || ttl != 5*time.Minute {
		t.Fatalf("resolved snapshot continuation = %x, %s, %v", key, ttl, err)
	}
	t.Setenv("LANE_SNAPSHOT_KEY", "not-base64")
	if _, _, err := resolveSnapshotContinuation(&SnapshotContinuationConfig{KeyEnv: "LANE_SNAPSHOT_KEY", TTLSeconds: 300}, ""); err == nil {
		t.Fatal("malformed snapshot continuation key was accepted")
	}
	t.Setenv("LANE_SNAPSHOT_KEY", base64.StdEncoding.EncodeToString([]byte("short")))
	if _, _, err := resolveSnapshotContinuation(&SnapshotContinuationConfig{KeyEnv: "LANE_SNAPSHOT_KEY", TTLSeconds: 300}, ""); err == nil {
		t.Fatal("short snapshot continuation key was accepted")
	}
	t.Setenv("LANE_SNAPSHOT_KEY", base64.StdEncoding.EncodeToString(keyMaterial))
	if _, _, err := resolveSnapshotContinuation(&SnapshotContinuationConfig{KeyEnv: "LANE_SNAPSHOT_KEY", TTLSeconds: 300}, string(keyMaterial)); err == nil {
		t.Fatal("transport bearer reuse was accepted")
	}

	called := false
	plain := requireBearer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}), "")
	recorder := &responseRecorder{header: make(http.Header)}
	plain.ServeHTTP(recorder, &http.Request{Header: make(http.Header)})
	if !called || recorder.code != http.StatusNoContent {
		t.Fatalf("empty-secret pass-through called=%v code=%d", called, recorder.code)
	}

	for _, authorization := range []string{"Basic value", "Bearer short", "Bearer value-extra"} {
		recorder = &responseRecorder{header: make(http.Header)}
		req := &http.Request{Header: make(http.Header)}
		req.Header.Set("Authorization", authorization)
		requireBearer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("unauthorized request reached downstream handler")
		}), "value").ServeHTTP(recorder, req)
		if recorder.code != http.StatusUnauthorized {
			t.Fatalf("authorization %q code=%d", authorization, recorder.code)
		}
	}

	success := textResult(map[string]any{"status": "ok"}, nil)
	if success.IsError || len(success.Content) != 1 {
		t.Fatalf("success result = %#v", success)
	}
	failure := textResult(nil, errors.New("boom"))
	if !failure.IsError || !strings.Contains(failure.Content[0].(*mcp.TextContent).Text, "boom") {
		t.Fatalf("error result = %#v", failure)
	}
	marshalFailure := textResult(map[string]any{"bad": func() {}}, nil)
	if !marshalFailure.IsError {
		t.Fatalf("marshal failure result = %#v", marshalFailure)
	}
}

type responseRecorder struct {
	header http.Header
	code   int
}

func (r *responseRecorder) Header() http.Header { return r.header }
func (r *responseRecorder) Write(body []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	return len(body), nil
}
func (r *responseRecorder) WriteHeader(code int) { r.code = code }

func TestLaneEmbeddingCanonicalization(t *testing.T) {
	sub := ArtifactSubmission{
		NodeKind: "task",
		StableID: "T-1",
		Contract: map[string]any{"canonical_name": "T-1"},
		Payload:  map[string]any{"status": "in_progress"},
	}
	got, err := canonicalEmbedText(sub)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{`"node_kind":"task"`, `"stable_id":"T-1"`, `"status":"in_progress"`} {
		if !strings.Contains(got, fragment) {
			t.Fatalf("canonical embedding text missing %s: %s", fragment, got)
		}
	}
	sub.Payload = map[string]any{"unsupported": func() {}}
	if _, err := canonicalEmbedText(sub); err == nil {
		t.Fatal("unsupported payload must fail canonicalization")
	}
}

func TestLaneAdminMechanicsAndEmulatorBoundary(t *testing.T) {
	statements := splitDDL("-- heading\nCREATE TABLE A (K INT64) PRIMARY KEY (K);\n\nCREATE INDEX IX ON A(K); -- tail\n;")
	if len(statements) != 2 || !strings.HasPrefix(statements[0], "CREATE TABLE") || !strings.HasPrefix(statements[1], "CREATE INDEX") {
		t.Fatalf("split DDL = %#v", statements)
	}
	if got := splitDDL(" -- only a comment\n ; "); len(got) != 0 {
		t.Fatalf("comment-only DDL = %#v", got)
	}

	t.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
	if _, err := newAdminClient(context.Background(), validIdentityBoundConfig().GoogleCredentials, false); err == nil || !strings.Contains(err.Error(), "admin operations") {
		t.Fatalf("disallowed emulator error = %v", err)
	}
	admin, err := newAdminClient(context.Background(), validIdentityBoundConfig().GoogleCredentials, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.CreateDatabase(context.Background(), "invalid"); err == nil {
		t.Fatal("invalid database resource must fail")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	resource := "projects/p/instances/i/databases/d"
	for name, invoke := range map[string]func() error{
		"create": func() error { return admin.CreateDatabase(canceled, resource) },
		"drop":   func() error { return admin.DropDatabase(canceled, resource) },
		"ddl": func() error {
			return admin.ApplyDDL(canceled, resource, []string{"CREATE TABLE T (K INT64) PRIMARY KEY (K)"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invoke(); err == nil {
				t.Fatal("canceled admin operation unexpectedly succeeded")
			}
		})
	}
	if err := admin.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLaneMCPConfigLoadError(t *testing.T) {
	t.Setenv("TASK_GRAPH_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	err := cmdMCP(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "load config") {
		t.Fatalf("cmdMCP load error = %v", err)
	}
	if got := fmt.Sprint(err); !strings.Contains(got, "missing.json") {
		t.Fatalf("cmdMCP error omitted config path: %s", got)
	}
}
