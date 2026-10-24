package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type reviewStartChildObservation struct {
	Args       []string `json:"args"`
	Input      string   `json:"input"`
	Executable string   `json:"executable"`
	WorkingDir string   `json:"working_dir"`
}

func TestReviewProcessStartChild(t *testing.T) {
	marker := -1
	for index, arg := range os.Args {
		if arg == "review-start-child" {
			marker = index
			break
		}
	}
	if marker < 0 {
		return
	}
	if len(os.Args) < marker+2 {
		os.Exit(89)
	}
	mode := os.Args[marker+1]
	input := new(bytes.Buffer)
	if _, err := input.ReadFrom(os.Stdin); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(90)
	}
	switch mode {
	case "observe":
		executable, err := os.Executable()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(91)
		}
		workingDir, err := os.Getwd()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(92)
		}
		observation := reviewStartChildObservation{
			Args:       append([]string{}, os.Args[marker+2:]...),
			Input:      input.String(),
			Executable: executable,
			WorkingDir: workingDir,
		}
		if err := json.NewEncoder(os.Stdout).Encode(observation); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(93)
		}
		os.Exit(0)
	case "nonzero":
		if len(os.Args) != marker+3 {
			os.Exit(94)
		}
		file, err := os.OpenFile(os.Args[marker+2], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(95)
		}
		_, _ = file.WriteString("started\n")
		_ = file.Close()
		os.Exit(23)
	case "overflow":
		fmt.Fprint(os.Stdout, strings.Repeat("x", 4096))
	case "timeout":
		time.Sleep(time.Hour)
	default:
		os.Exit(96)
	}
}

func reviewStartFixture(t *testing.T, mode string, extraArgs ...string) (*ReviewProducerConfig, []byte) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	program, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-test.run=^TestReviewProcessStartChild$", "--", "review-start-child", mode}
	args = append(args, extraArgs...)
	return &ReviewProducerConfig{
		Generation:          "review-start-tests-e1",
		Executable:          executable,
		ExecutableSHA256:    reviewHashBytes(program),
		Args:                args,
		TimeoutMilliseconds: 5000,
		MaxOutputBytes:      64 * 1024,
	}, program
}

func reviewStartETXTBSY(cmd *exec.Cmd) error {
	return &os.PathError{Op: "fork/exec", Path: cmd.Path, Err: syscall.ETXTBSY}
}

type reviewControlledDeadlineContext struct {
	deadline time.Time
	done     chan struct{}
}

func (ctx *reviewControlledDeadlineContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func (ctx *reviewControlledDeadlineContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *reviewControlledDeadlineContext) Err() error {
	select {
	case <-ctx.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (ctx *reviewControlledDeadlineContext) Value(any) any {
	return nil
}

func (ctx *reviewControlledDeadlineContext) expire() {
	close(ctx.done)
}

// reviewRunProcessWithStart is the expected unexported per-call seam. The
// production reviewRunProcess wrapper supplies an inline cmd.Start call;
// tests can observe the actual attempt context and replace only the pre-start
// operation without replacing command setup, Wait, the original deadline,
// bounded output, or private executable cleanup.
func TestReviewRunProcessRetriesTypedPreStartETXTBSYAndPreservesBoundary(t *testing.T) {
	config, program := reviewStartFixture(t, "observe", "fixed", "")
	input := []byte(`{"kind":"review","value":"exact"}`)
	starts := 0
	start := func(_ context.Context, cmd *exec.Cmd) error {
		starts++
		if starts == 1 {
			return reviewStartETXTBSY(cmd)
		}
		return cmd.Start()
	}
	captured, err := reviewRunProcessWithStart(context.Background(), config, program, input, start)
	if err != nil {
		t.Fatalf("transient typed ETXTBSY did not recover: %v", err)
	}
	if starts != 2 {
		t.Fatalf("start calls = %d, want 2", starts)
	}
	if captured.Generation != config.Generation || captured.ExecutableSHA256 != config.ExecutableSHA256 || captured.ExitCode != 0 || captured.Stderr != "" {
		t.Fatalf("captured process identity/result changed: %#v", captured)
	}
	var observed reviewStartChildObservation
	if err := json.Unmarshal([]byte(captured.Stdout), &observed); err != nil {
		t.Fatalf("actual child output is not its observation: %v", err)
	}
	if !bytes.Equal([]byte(observed.Input), input) || !equalStrings(observed.Args, []string{"fixed", ""}) {
		t.Fatalf("actual child stdin/argv changed: %#v", observed)
	}
	if filepath.Dir(observed.Executable) != observed.WorkingDir || filepath.Base(observed.Executable) != "reviewer"+filepath.Ext(config.Executable) {
		t.Fatalf("actual child did not execute the private snapshot: %#v", observed)
	}
	if _, err := os.Stat(observed.WorkingDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private process directory remained after completion: %v", err)
	}
}

func TestReviewRunProcessBoundsETXTBSYAttemptsUnderOneDeadline(t *testing.T) {
	config, program := reviewStartFixture(t, "observe")
	starts := 0
	deadlines := []time.Time{}
	remaining := []time.Duration{}
	_, err := reviewRunProcessWithStart(context.Background(), config, program, nil,
		func(attempt context.Context, cmd *exec.Cmd) error {
			starts++
			deadline, ok := attempt.Deadline()
			if !ok {
				t.Fatal("start attempt has no configured deadline")
			}
			deadlines = append(deadlines, deadline)
			remaining = append(remaining, time.Until(deadline))
			return reviewStartETXTBSY(cmd)
		})
	if err == nil || !errors.Is(err, syscall.ETXTBSY) {
		t.Fatalf("ETXTBSY exhaustion = %v, want typed refusal", err)
	}
	if starts != 3 {
		t.Fatalf("exhaustion start calls = %d, want 3", starts)
	}
	if len(deadlines) != 3 || !deadlines[0].Equal(deadlines[1]) || !deadlines[1].Equal(deadlines[2]) {
		t.Fatalf("start attempts did not retain one absolute deadline: %v", deadlines)
	}
	if !(remaining[0] > remaining[1] && remaining[1] > remaining[2]) {
		t.Fatalf("retry attempts did not consume the original time budget: %v", remaining)
	}
}

func TestReviewRunProcessDoesNotRetryOtherStartFailures(t *testing.T) {
	config, program := reviewStartFixture(t, "observe")
	cases := []struct {
		name string
		err  error
	}{
		{"typed_nonretryable", &os.PathError{Op: "fork/exec", Path: config.Executable, Err: syscall.EACCES}},
		{"text_only_mimic", errors.New("fork/exec reviewer: text file busy")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			starts := 0
			_, err := reviewRunProcessWithStart(context.Background(), config, program, nil,
				func(context.Context, *exec.Cmd) error { starts++; return tc.err })
			if err == nil || starts != 1 {
				t.Fatalf("nonretryable start result = err:%v starts:%d", err, starts)
			}
		})
	}
}

func TestReviewRunProcessStopsBeforeStartAndDuringBackoff(t *testing.T) {
	config, program := reviewStartFixture(t, "observe")
	t.Run("cancelled_before_start", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		starts := 0
		_, err := reviewRunProcessWithStart(ctx, config, program, nil,
			func(context.Context, *exec.Cmd) error { starts++; return nil })
		if !errors.Is(err, context.Canceled) || starts != 0 {
			t.Fatalf("pre-cancel result = err:%v starts:%d", err, starts)
		}
	})
	t.Run("cancelled_during_backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		starts := 0
		first := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-first
			cancel()
		}()
		_, err := reviewRunProcessWithStart(ctx, config, program, nil,
			func(_ context.Context, cmd *exec.Cmd) error {
				starts++
				if starts == 1 {
					close(first)
				}
				return reviewStartETXTBSY(cmd)
			})
		<-done
		if !errors.Is(err, context.Canceled) || starts != 1 {
			t.Fatalf("backoff cancellation result = err:%v starts:%d", err, starts)
		}
	})
	t.Run("deadline_during_backoff", func(t *testing.T) {
		deadlineCtx := &reviewControlledDeadlineContext{
			deadline: time.Now().Add(time.Minute),
			done:     make(chan struct{}),
		}
		starts := 0
		_, err := reviewRunProcessWithStart(deadlineCtx, config, program, nil,
			func(attempt context.Context, cmd *exec.Cmd) error {
				starts++
				if starts != 1 {
					t.Fatalf("deadline allowed another start: %d", starts)
				}
				if _, ok := attempt.Deadline(); !ok {
					t.Fatal("first start attempt has no configured deadline")
				}
				deadlineCtx.expire()
				<-attempt.Done()
				if !errors.Is(attempt.Err(), context.DeadlineExceeded) {
					t.Fatalf("attempt deadline result = %v", attempt.Err())
				}
				return reviewStartETXTBSY(cmd)
			})
		if !errors.Is(err, context.DeadlineExceeded) || starts != 1 {
			t.Fatalf("backoff deadline result = err:%v starts:%d", err, starts)
		}
	})
}

func TestReviewRunProcessNeverRetriesAStartedChildFailure(t *testing.T) {
	t.Run("nonzero_exit", func(t *testing.T) {
		marker := filepath.Join(t.TempDir(), "starts")
		config, program := reviewStartFixture(t, "nonzero", marker)
		starts := 0
		_, err := reviewRunProcessWithStart(context.Background(), config, program, nil,
			func(_ context.Context, cmd *exec.Cmd) error { starts++; return cmd.Start() })
		if err == nil || starts != 1 {
			t.Fatalf("nonzero child result = err:%v starts:%d", err, starts)
		}
		content, readErr := os.ReadFile(marker)
		if readErr != nil || string(content) != "started\n" {
			t.Fatalf("nonzero child execution count is not exactly one: %q %v", content, readErr)
		}
	})
	t.Run("output_overflow", func(t *testing.T) {
		config, program := reviewStartFixture(t, "overflow")
		config.MaxOutputBytes = 32
		starts := 0
		_, err := reviewRunProcessWithStart(context.Background(), config, program, nil,
			func(_ context.Context, cmd *exec.Cmd) error { starts++; return cmd.Start() })
		if err == nil || starts != 1 {
			t.Fatalf("overflow child result = err:%v starts:%d", err, starts)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		config, program := reviewStartFixture(t, "timeout")
		config.TimeoutMilliseconds = 25
		starts := 0
		_, err := reviewRunProcessWithStart(context.Background(), config, program, nil,
			func(_ context.Context, cmd *exec.Cmd) error { starts++; return cmd.Start() })
		if err == nil || starts != 1 {
			t.Fatalf("timed out child result = err:%v starts:%d", err, starts)
		}
	})
}

func TestReviewRunProcessProductionEntryUsesFixedActualStart(t *testing.T) {
	config, program := reviewStartFixture(t, "observe", "production-entry")
	captured, err := reviewRunProcess(context.Background(), config, program, []byte("production-input"))
	if err != nil {
		t.Fatalf("production entry failed actual child start: %v", err)
	}
	var observed reviewStartChildObservation
	if err := json.Unmarshal([]byte(captured.Stdout), &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Input != "production-input" || !equalStrings(observed.Args, []string{"production-entry"}) {
		t.Fatalf("production entry changed configured stdin/argv: %#v", observed)
	}
}

func TestReviewRunProcessProductionWrapperDelegatesToRetryBoundary(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "review_source.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var wrapper *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "reviewRunProcess" {
			wrapper = function
			break
		}
	}
	if wrapper == nil || wrapper.Body == nil || len(wrapper.Body.List) != 1 {
		t.Fatal("reviewRunProcess must be one fixed delegation to the retry boundary")
	}
	result, ok := wrapper.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(result.Results) != 1 {
		t.Fatal("reviewRunProcess must return the retry-boundary result directly")
	}
	call, ok := result.Results[0].(*ast.CallExpr)
	if !ok || astIdentifier(call.Fun) != "reviewRunProcessWithStart" || len(call.Args) != 5 {
		t.Fatal("reviewRunProcess does not invoke the retry boundary exactly once")
	}
	for index, name := range []string{"ctx", "config", "program", "input"} {
		if astIdentifier(call.Args[index]) != name {
			t.Fatalf("retry-boundary argument %d = %q, want %q", index, astIdentifier(call.Args[index]), name)
		}
	}
	requireFixedReviewStart(t, call.Args[4])
}

func requireFixedReviewStart(t *testing.T, expression ast.Expr) {
	t.Helper()
	function, ok := expression.(*ast.FuncLit)
	if !ok || function.Type.Params == nil || len(function.Type.Params.List) != 2 || len(function.Body.List) != 1 {
		t.Fatal("production retry boundary must receive one inline fixed Start function")
	}
	contextParameter := function.Type.Params.List[0]
	commandParameter := function.Type.Params.List[1]
	if !astSelector(contextParameter.Type, "context", "Context") || len(commandParameter.Names) != 1 || commandParameter.Names[0].Name != "cmd" {
		t.Fatal("fixed Start function parameters must be context.Context and cmd *exec.Cmd")
	}
	star, ok := commandParameter.Type.(*ast.StarExpr)
	if !ok || !astSelector(star.X, "exec", "Cmd") {
		t.Fatal("fixed Start function command parameter must be *exec.Cmd")
	}
	result, ok := function.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(result.Results) != 1 {
		t.Fatal("fixed Start function must directly return cmd.Start")
	}
	call, ok := result.Results[0].(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		t.Fatal("fixed Start function must invoke cmd.Start without arguments")
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || astIdentifier(selector.X) != "cmd" || selector.Sel.Name != "Start" {
		t.Fatal("fixed Start function must directly return cmd.Start")
	}
}

func astIdentifier(expression ast.Expr) string {
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func astSelector(expression ast.Expr, packageName, member string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	return ok && astIdentifier(selector.X) == packageName && selector.Sel.Name == member
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
