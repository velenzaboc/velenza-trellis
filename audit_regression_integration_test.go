//go:build integration

package main

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const auditSpannerCommitMethod = "/google.spanner.v1.Spanner/Commit"

type auditCommitBarrier struct {
	arrivals atomic.Int64
	aborts   atomic.Int64
	release  chan struct{}
	once     sync.Once
}

func newAuditCommitBarrier() *auditCommitBarrier {
	return &auditCommitBarrier{release: make(chan struct{})}
}

func (b *auditCommitBarrier) intercept(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if method == auditSpannerCommitMethod {
		arrival := b.arrivals.Add(1)
		if arrival <= 2 {
			if arrival == 2 {
				b.once.Do(func() { close(b.release) })
			}
			select {
			case <-b.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	err := invoker(ctx, method, req, reply, cc, opts...)
	if status.Code(err) == codes.Aborted {
		b.aborts.Add(1)
	}
	return err
}

func auditInstallOfficialEmulatorClient(t *testing.T, env *Env, interceptor grpc.UnaryClientInterceptor) {
	t.Helper()
	host := os.Getenv("SPANNER_EMULATOR_HOST")
	if host == "" {
		t.Fatal("SPANNER_EMULATOR_HOST must name the parent-managed official emulator; this test never skips")
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if interceptor != nil {
		opts = append(opts, grpc.WithUnaryInterceptor(interceptor))
	}
	conn, err := grpc.NewClient(host, opts...)
	if err != nil {
		t.Fatalf("dial official Spanner emulator %q: %v", host, err)
	}
	client, err := spanner.NewClient(context.Background(), emulatorResource, option.WithGRPCConn(conn))
	if err != nil {
		conn.Close()
		t.Fatalf("create official emulator client: %v", err)
	}
	adapter, err := soleNativeFleetPlanAdapter(context.Background(), env)
	if err != nil {
		client.Close()
		conn.Close()
		t.Fatalf("resolve native adapter: %v", err)
	}
	adapter.mu.Lock()
	adapter.client = client
	adapter.mu.Unlock()
	t.Cleanup(func() {
		client.Close()
		conn.Close()
	})
}

func TestAuditTaskParentMutationMaintainsTree(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	setupEmulatorDBWithSchema(ctx, t, nativeProjectSchema)
	env := nativeProjectEnv(t)
	auditInstallOfficialEmulatorClient(t, env, nil)
	t.Cleanup(func() { _ = env.Substrates.Close() })

	t.Run("valid direct parent link", func(t *testing.T) {
		auditCreateTasks(t, env, "AUDIT-VALID-PARENT", "AUDIT-VALID-CHILD")
		if _, err := handleLink(ctx, env, linkInput{
			ProjectID: "arbor", SourceID: "AUDIT-VALID-PARENT", TargetID: "AUDIT-VALID-CHILD", EdgeLabel: "parent_of",
		}); err != nil {
			t.Fatalf("valid parent link: %v", err)
		}
		if got := stringFromMap(auditReadTask(t, env, "AUDIT-VALID-CHILD").CurrentVersion.Payload, "parent_id"); got != "AUDIT-VALID-PARENT" {
			t.Fatalf("child parent = %q, want AUDIT-VALID-PARENT", got)
		}
	})

	for _, tc := range []struct {
		name     string
		childID  string
		parentID string
	}{
		{name: "generic missing parent", childID: "AUDIT-GENERIC-MISSING", parentID: "AUDIT-NOT-FOUND"},
		{name: "generic self parent", childID: "AUDIT-GENERIC-SELF", parentID: "AUDIT-GENERIC-SELF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auditCreateTasks(t, env, tc.childID)
			if out, err := handleUpdate(ctx, env, updateInput{
				ProjectID: "arbor", NodeID: tc.childID, Payload: map[string]interface{}{"parent_id": tc.parentID},
			}); err == nil {
				t.Fatalf("invalid generic parent update succeeded: %+v", out)
			}
			if got := stringFromMap(auditReadTask(t, env, tc.childID).CurrentVersion.Payload, "parent_id"); got != "" {
				t.Fatalf("refused generic parent update wrote %q", got)
			}
		})
	}

	t.Run("generic multi hop cycle", func(t *testing.T) {
		auditCreateTasks(t, env, "AUDIT-GENERIC-A", "AUDIT-GENERIC-B", "AUDIT-GENERIC-C")
		for _, edge := range [][2]string{{"AUDIT-GENERIC-A", "AUDIT-GENERIC-B"}, {"AUDIT-GENERIC-B", "AUDIT-GENERIC-C"}} {
			if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: edge[0], TargetID: edge[1], EdgeLabel: "parent_of"}); err != nil {
				t.Fatalf("seed valid parent link %s -> %s: %v", edge[0], edge[1], err)
			}
		}
		if out, err := handleUpdate(ctx, env, updateInput{
			ProjectID: "arbor", NodeID: "AUDIT-GENERIC-A", Payload: map[string]interface{}{"parent_id": "AUDIT-GENERIC-C"},
		}); err == nil {
			t.Fatalf("generic cycle-closing update succeeded: %+v", out)
		}
		if got := stringFromMap(auditReadTask(t, env, "AUDIT-GENERIC-A").CurrentVersion.Payload, "parent_id"); got != "" {
			t.Fatalf("generic cycle-closing update wrote parent %q", got)
		}
	})

	t.Run("direct missing parent", func(t *testing.T) {
		auditCreateTasks(t, env, "AUDIT-DIRECT-MISSING-CHILD")
		if out, err := handleLink(ctx, env, linkInput{
			ProjectID: "arbor", SourceID: "AUDIT-DIRECT-NOT-FOUND", TargetID: "AUDIT-DIRECT-MISSING-CHILD", EdgeLabel: "parent_of",
		}); err == nil {
			t.Fatalf("direct missing-parent link succeeded: %+v", out)
		}
		if got := stringFromMap(auditReadTask(t, env, "AUDIT-DIRECT-MISSING-CHILD").CurrentVersion.Payload, "parent_id"); got != "" {
			t.Fatalf("direct missing-parent refusal wrote %q", got)
		}
	})

	t.Run("direct self parent", func(t *testing.T) {
		auditCreateTasks(t, env, "AUDIT-DIRECT-SELF")
		if out, err := handleLink(ctx, env, linkInput{
			ProjectID: "arbor", SourceID: "AUDIT-DIRECT-SELF", TargetID: "AUDIT-DIRECT-SELF", EdgeLabel: "parent_of",
		}); err == nil {
			t.Fatalf("direct self-parent link succeeded: %+v", out)
		}
		if got := stringFromMap(auditReadTask(t, env, "AUDIT-DIRECT-SELF").CurrentVersion.Payload, "parent_id"); got != "" {
			t.Fatalf("direct self-parent refusal wrote %q", got)
		}
	})

	t.Run("direct multi hop cycle", func(t *testing.T) {
		auditCreateTasks(t, env, "AUDIT-DIRECT-A", "AUDIT-DIRECT-B", "AUDIT-DIRECT-C")
		for _, edge := range [][2]string{{"AUDIT-DIRECT-A", "AUDIT-DIRECT-B"}, {"AUDIT-DIRECT-B", "AUDIT-DIRECT-C"}} {
			if _, err := handleLink(ctx, env, linkInput{ProjectID: "arbor", SourceID: edge[0], TargetID: edge[1], EdgeLabel: "parent_of"}); err != nil {
				t.Fatalf("seed direct parent link %s -> %s: %v", edge[0], edge[1], err)
			}
		}
		if out, err := handleLink(ctx, env, linkInput{
			ProjectID: "arbor", SourceID: "AUDIT-DIRECT-C", TargetID: "AUDIT-DIRECT-A", EdgeLabel: "parent_of",
		}); err == nil {
			t.Fatalf("direct cycle-closing link succeeded: %+v", out)
		}
		if got := stringFromMap(auditReadTask(t, env, "AUDIT-DIRECT-A").CurrentVersion.Payload, "parent_id"); got != "" {
			t.Fatalf("direct cycle-closing refusal wrote parent %q", got)
		}
	})

	t.Run("cross project endpoint", func(t *testing.T) {
		auditCreateTasks(t, env, "AUDIT-CROSS-LOCAL")
		foreign := coverageTaskInput("AUDIT-CROSS-FOREIGN")
		foreign.ProjectID = "moonshot"
		if _, err := nativeHandleCreate(ctx, env, foreign); err != nil {
			t.Fatalf("create foreign project task: %v", err)
		}
		if out, err := handleLink(ctx, env, linkInput{
			ProjectID: "arbor", SourceID: "AUDIT-CROSS-FOREIGN", TargetID: "AUDIT-CROSS-LOCAL", EdgeLabel: "parent_of",
		}); err == nil {
			t.Fatalf("cross-project parent link succeeded: %+v", out)
		}
	})

	t.Run("concurrent opposite links share prestate and conflict", func(t *testing.T) {
		auditCreateTasks(t, env, "AUDIT-CONCURRENT-X", "AUDIT-CONCURRENT-Y")
		barrier := newAuditCommitBarrier()
		auditInstallOfficialEmulatorClient(t, env, barrier.intercept)
		operationCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		start := make(chan struct{})
		errs := make(chan error, 2)
		link := func(parent, child string) {
			<-start
			_, err := handleLink(operationCtx, env, linkInput{
				ProjectID: "arbor", SourceID: parent, TargetID: child, EdgeLabel: "parent_of",
			})
			errs <- err
		}
		go link("AUDIT-CONCURRENT-X", "AUDIT-CONCURRENT-Y")
		go link("AUDIT-CONCURRENT-Y", "AUDIT-CONCURRENT-X")
		close(start)
		successes := 0
		for i := 0; i < 2; i++ {
			if err := <-errs; err == nil {
				successes++
			}
		}
		if got := barrier.arrivals.Load(); got < 2 {
			t.Fatalf("commit barrier observed %d transactions, want both contenders from shared prestate", got)
		}
		if got := barrier.aborts.Load(); got < 1 {
			t.Fatalf("official emulator observed %d aborted commits, want conflict/retry evidence", got)
		}
		if successes != 1 {
			t.Fatalf("concurrent opposite links had %d successes, want exactly one", successes)
		}
		xParent := stringFromMap(auditReadTask(t, env, "AUDIT-CONCURRENT-X").CurrentVersion.Payload, "parent_id")
		yParent := stringFromMap(auditReadTask(t, env, "AUDIT-CONCURRENT-Y").CurrentVersion.Payload, "parent_id")
		if xParent == "AUDIT-CONCURRENT-Y" && yParent == "AUDIT-CONCURRENT-X" {
			t.Fatalf("concurrent opposite links committed a two-node cycle")
		}
	})
}
