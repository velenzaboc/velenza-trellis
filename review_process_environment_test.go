package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var reviewProcessOSCoordinates = []string{
	"PATH", "HOME", "USER", "USERPROFILE", "SYSTEMROOT", "WINDIR",
	"TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL",
}

var reviewProcessSyntheticParentVariables = map[string]string{
	"OPENAI_API_KEY":          "synthetic-openai-key",
	"CLAUDE_CODE_OAUTH_TOKEN": "synthetic-oauth-token",
	"LD_PRELOAD":              "synthetic-loader-control",
	"NODE_OPTIONS":            "synthetic-node-control",
	"CLAUDE_CONFIG_DIR":       "synthetic-claude-config",
	"GO_WANT_HELPER_PROCESS":  "synthetic-helper-control",
	"LOGNAME":                 "synthetic-logname",
	"XPC_SERVICE_NAME":        "synthetic-xpc-service",
}

type reviewProcessEnvironmentObservation struct {
	Keys        []string `json:"keys"`
	UserPresent bool     `json:"user_present"`
	User        string   `json:"user"`
	PathPresent bool     `json:"path_present"`
	Path        string   `json:"path"`
	Input       string   `json:"input"`
	WorkingDir  string   `json:"working_dir"`
	Executable  string   `json:"executable"`
	FixedArgs   []string `json:"fixed_args"`
}

func reviewProcessExpectedEnvironmentKeys() []string {
	keys := []string{}
	for _, key := range reviewProcessOSCoordinates {
		if _, ok := os.LookupEnv(key); ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func reviewProcessFixture(t *testing.T, input string) reviewProcessEnvironmentObservation {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	program, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-test.run=^TestReviewProcessEnvironmentChild$", "--", "review-osenv-child", "--tools", ""}
	config := &ReviewProducerConfig{
		Generation:          "review-osenv-tests-e1",
		Executable:          executable,
		ExecutableSHA256:    reviewHashBytes(program),
		Args:                args,
		TimeoutMilliseconds: 5000,
		MaxOutputBytes:      65536,
	}
	captured, err := reviewRunProcess(context.Background(), config, program, []byte(input))
	if err != nil {
		t.Fatalf("actual review subprocess failed: %v", err)
	}
	if captured.ExitCode != 0 || captured.Stderr != "" {
		t.Fatalf("actual review subprocess returned exit=%d stderr=%q", captured.ExitCode, captured.Stderr)
	}
	if captured.Generation != config.Generation || captured.ExecutableSHA256 != config.ExecutableSHA256 || !reflect.DeepEqual(captured.Args, args) {
		t.Fatal("captured process identity does not match the fixed service configuration")
	}
	var observed reviewProcessEnvironmentObservation
	if err := json.Unmarshal([]byte(captured.Stdout), &observed); err != nil {
		t.Fatalf("actual review subprocess did not return its bounded observation: %v", err)
	}
	if observed.Input != input || !reflect.DeepEqual(observed.FixedArgs, []string{"--tools", ""}) {
		t.Fatal("actual review subprocess did not receive fixed stdin and argv")
	}
	if !strings.HasPrefix(filepath.Base(observed.Executable), "reviewer") || filepath.Dir(observed.Executable) != observed.WorkingDir {
		t.Fatal("review subprocess did not execute the private copied program from its private working directory")
	}
	return observed
}

func setSyntheticReviewParentEnvironment(t *testing.T) {
	t.Helper()
	for key, value := range reviewProcessSyntheticParentVariables {
		t.Setenv(key, value)
	}
}

func unsetReviewEnvironmentForTest(t *testing.T, key string) {
	t.Helper()
	prior, existed := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv(key, prior)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

func assertSyntheticReviewParentEnvironmentStripped(t *testing.T, keys []string) {
	t.Helper()
	present := map[string]bool{}
	for _, key := range keys {
		present[key] = true
	}
	for key := range reviewProcessSyntheticParentVariables {
		if present[key] {
			t.Fatalf("review subprocess inherited synthetic credential/control key %q", key)
		}
	}
}

func TestReviewRunProcessPreservesUserAndOnlyOwnedOSCoordinates(t *testing.T) {
	user, userPresent := os.LookupEnv("USER")
	if !userPresent || user == "" {
		t.Fatal("positive control requires the service parent to expose its ordinary USER coordinate")
	}
	parentPath, pathPresent := os.LookupEnv("PATH")
	if !pathPresent || parentPath == "" {
		t.Fatal("positive control requires the service parent to expose PATH")
	}
	setSyntheticReviewParentEnvironment(t)
	expectedKeys := reviewProcessExpectedEnvironmentKeys()
	observed := reviewProcessFixture(t, "positive-user-coordinate")
	if !reflect.DeepEqual(observed.Keys, expectedKeys) {
		t.Fatalf("review subprocess environment keys=%v, want exact service OS coordinates=%v", observed.Keys, expectedKeys)
	}
	if !observed.UserPresent || observed.User != user {
		t.Fatal("review subprocess did not inherit the service parent's ordinary USER coordinate")
	}
	if !observed.PathPresent || observed.Path != parentPath {
		t.Fatal("review subprocess did not inherit the service parent's PATH coordinate")
	}
	assertSyntheticReviewParentEnvironmentStripped(t, observed.Keys)
}

func TestReviewRunProcessDoesNotManufactureAbsentUser(t *testing.T) {
	setSyntheticReviewParentEnvironment(t)
	unsetReviewEnvironmentForTest(t, "USER")
	expectedKeys := reviewProcessExpectedEnvironmentKeys()
	observed := reviewProcessFixture(t, "absent-user-coordinate")
	if !reflect.DeepEqual(observed.Keys, expectedKeys) {
		t.Fatalf("review subprocess environment keys=%v, want exact USER-absent OS coordinates=%v", observed.Keys, expectedKeys)
	}
	if observed.UserPresent || observed.User != "" {
		t.Fatal("review subprocess manufactured USER when the service parent had none")
	}
	assertSyntheticReviewParentEnvironmentStripped(t, observed.Keys)
}

// Executed only when reviewRunProcess launches this copied test executable with
// the fixed positional marker. No inherited environment variable selects it.
func TestReviewProcessEnvironmentChild(t *testing.T) {
	marker := -1
	for index, arg := range os.Args {
		if arg == "review-osenv-child" {
			marker = index
			break
		}
	}
	if marker < 0 {
		return
	}
	if len(os.Args) != marker+3 || os.Args[marker+1] != "--tools" || os.Args[marker+2] != "" {
		os.Exit(81)
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(82)
	}
	keys := []string{}
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			os.Exit(83)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	user, userPresent := os.LookupEnv("USER")
	pathValue, pathPresent := os.LookupEnv("PATH")
	workingDir, err := os.Getwd()
	if err != nil {
		os.Exit(84)
	}
	executable, err := os.Executable()
	if err != nil {
		os.Exit(85)
	}
	observed := reviewProcessEnvironmentObservation{
		Keys:        keys,
		UserPresent: userPresent,
		User:        user,
		PathPresent: pathPresent,
		Path:        pathValue,
		Input:       string(input),
		WorkingDir:  workingDir,
		Executable:  executable,
		FixedArgs:   append([]string{}, os.Args[marker+1:]...),
	}
	encoded, err := json.Marshal(observed)
	if err != nil {
		os.Exit(86)
	}
	_, _ = os.Stdout.Write(encoded)
	os.Exit(0)
}
