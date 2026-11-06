package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// The reviewer intentionally receives no coverage-control environment. Copying
// an instrumented parent test binary into that boundary would add a Go runtime
// warning to the child's stderr. Compile the same real child without coverage;
// the parent production review path remains instrumented and its environment
// contract is unchanged. All files belong to this test's temporary directory.
func reviewFixtureExecutable(t *testing.T) string {
	t.Helper()
	if testing.CoverMode() == "" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		return executable
	}
	output := filepath.Join(t.TempDir(), "review-fixture.test")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-c", "-cover=false", "-o", output, ".")
	// Test parents deliberately inject loader/token variables. Building the child
	// needs only the selected tools and this guarded run's private caches.
	for _, key := range []string{"PATH", "HOME", "USER", "USERPROFILE", "SYSTEMROOT", "WINDIR", "TMPDIR", "TMP", "TEMP", "GOCACHE", "GOMODCACHE"} {
		if value, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	cmd.Env = append(cmd.Env, "GOTOOLCHAIN=local")
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build uninstrumented reviewer fixture: %v\n%s", err, data)
	}
	return output
}
