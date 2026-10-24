package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func reviewNoBackoff(context.Context, time.Duration) error { return nil }

// reviewProcessStartHooks is the expected per-call test seam. Production calls
// omit it; tests inject only Start and context-aware backoff behavior while the
// real reviewRunProcess continues to own command construction, Wait, bounded
// output, its single deadline, and private executable cleanup.
func reviewStartHooks(start func(*exec.Cmd) error, backoff func(context.Context, time.Duration) error) reviewProcessStartHooks {
	return reviewProcessStartHooks{start: start, backoff: backoff}
}

func TestReviewRunProcessRetriesTypedPreStartETXTBSYAndPreservesBoundary(t *testing.T) {
	config, program := reviewStartFixture(t, "observe", "fixed", "")
	input := []byte(`{"kind":"review","value":"exact"}`)
	starts := 0
	backoffs := 0
	start := func(cmd *exec.Cmd) error {
		starts++
		if starts == 1 {
			return reviewStartETXTBSY(cmd)
		}
		return cmd.Start()
	}
	backoff := func(ctx context.Context, delay time.Duration) error {
		backoffs++
		if ctx.Err() != nil || delay <= 0 {
			t.Fatalf("retry backoff received invalid context/delay: err=%v delay=%v", ctx.Err(), delay)
		}
		return nil
	}
	captured, err := reviewRunProcess(context.Background(), config, program, input, reviewStartHooks(start, backoff))
	if err != nil {
		t.Fatalf("transient typed ETXTBSY did not recover: %v", err)
	}
	if starts != 2 || backoffs != 1 {
		t.Fatalf("start/backoff calls = %d/%d, want 2/1", starts, backoffs)
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
	_, err := reviewRunProcess(context.Background(), config, program, nil, reviewStartHooks(
		func(cmd *exec.Cmd) error {
			starts++
			return reviewStartETXTBSY(cmd)
		},
		func(ctx context.Context, delay time.Duration) error {
			deadline, ok := ctx.Deadline()
			if !ok || delay <= 0 {
				t.Fatalf("retry lacks the configured deadline/backoff: ok=%v delay=%v", ok, delay)
			}
			deadlines = append(deadlines, deadline)
			return nil
		}))
	if err == nil || !errors.Is(err, syscall.ETXTBSY) {
		t.Fatalf("ETXTBSY exhaustion = %v, want typed refusal", err)
	}
	if starts != 3 || len(deadlines) != 2 {
		t.Fatalf("exhaustion start/backoff calls = %d/%d, want 3/2", starts, len(deadlines))
	}
	if !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("retry reset the configured deadline: %v != %v", deadlines[0], deadlines[1])
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
			backoffs := 0
			_, err := reviewRunProcess(context.Background(), config, program, nil, reviewStartHooks(
				func(*exec.Cmd) error { starts++; return tc.err },
				func(context.Context, time.Duration) error { backoffs++; return nil }))
			if err == nil || starts != 1 || backoffs != 0 {
				t.Fatalf("nonretryable start result = err:%v starts:%d backoffs:%d", err, starts, backoffs)
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
		_, err := reviewRunProcess(ctx, config, program, nil, reviewStartHooks(
			func(*exec.Cmd) error { starts++; return nil }, reviewNoBackoff))
		if !errors.Is(err, context.Canceled) || starts != 0 {
			t.Fatalf("pre-cancel result = err:%v starts:%d", err, starts)
		}
	})
	t.Run("cancelled_during_backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		starts := 0
		_, err := reviewRunProcess(ctx, config, program, nil, reviewStartHooks(
			func(cmd *exec.Cmd) error { starts++; return reviewStartETXTBSY(cmd) },
			func(ctx context.Context, _ time.Duration) error {
				cancel()
				<-ctx.Done()
				return ctx.Err()
			}))
		if !errors.Is(err, context.Canceled) || starts != 1 {
			t.Fatalf("backoff cancellation result = err:%v starts:%d", err, starts)
		}
	})
	t.Run("deadline_during_backoff", func(t *testing.T) {
		deadlineCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		starts := 0
		_, err := reviewRunProcess(deadlineCtx, config, program, nil, reviewStartHooks(
			func(cmd *exec.Cmd) error { starts++; return reviewStartETXTBSY(cmd) },
			func(ctx context.Context, _ time.Duration) error { <-ctx.Done(); return ctx.Err() }))
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
		_, err := reviewRunProcess(context.Background(), config, program, nil, reviewStartHooks(
			func(cmd *exec.Cmd) error { starts++; return cmd.Start() }, reviewNoBackoff))
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
		_, err := reviewRunProcess(context.Background(), config, program, nil, reviewStartHooks(
			func(cmd *exec.Cmd) error { starts++; return cmd.Start() }, reviewNoBackoff))
		if err == nil || starts != 1 {
			t.Fatalf("overflow child result = err:%v starts:%d", err, starts)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		config, program := reviewStartFixture(t, "timeout")
		config.TimeoutMilliseconds = 25
		starts := 0
		_, err := reviewRunProcess(context.Background(), config, program, nil, reviewStartHooks(
			func(cmd *exec.Cmd) error { starts++; return cmd.Start() }, reviewNoBackoff))
		if err == nil || starts != 1 {
			t.Fatalf("timed out child result = err:%v starts:%d", err, starts)
		}
	})
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
