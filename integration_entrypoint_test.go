package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	integrationEntrypoint = "scripts/test-integration.sh"
	emulatorImage         = "gcr.io/cloud-spanner-emulator/emulator@sha256:c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79"
	ownedContainerID      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	goTestSelection       = "^(TestAuditBoundary|TestNativeCompatibility|TestTaggedPrimary)$"
)

type entrypointRun struct {
	exitCode   int
	output     string
	commands   string
	calls      map[string][][]string
	runnerTemp string
	goTestHost string
}

func TestIntegrationEntrypointDiscoversTaggedSourceMembershipAndRunsEverySelectedTest(t *testing.T) {
	run := runIntegrationEntrypoint(t, nil)
	if run.exitCode != 0 {
		t.Fatalf("entrypoint exit = %d, want 0; output:\n%s", run.exitCode, run.output)
	}
	requireCall(t, run, "go", []string{"list", "-f", "{{range .TestGoFiles}}{{println .}}{{end}}", "."})
	requireCall(t, run, "go", []string{"list", "-tags=integration", "-f", "{{range .TestGoFiles}}{{println .}}{{end}}", "."})
	requireCallPrefix(t, run, "go", []string{"run"})
	requireOnlyCall(t, run, "go-test", []string{"test", "-tags=integration", ".", "-run", goTestSelection, "-count=1"})
	if run.goTestHost != "localhost:9010" {
		t.Fatalf("SPANNER_EMULATOR_HOST = %q, want localhost:9010", run.goTestHost)
	}
	if strings.Contains(goTestSelection, "TestUnitOwnsEndpoint") || strings.Contains(goTestSelection, "TestWrongTagVariant") {
		t.Fatalf("selection admitted an untagged or unsatisfied-tag fixture: %s", goTestSelection)
	}
	requirePinnedCreate(t, run)
	requireReadinessProbes(t, run)
	requireOwnedCleanup(t, run)
}

func TestIntegrationEntrypointRefusesInvalidOrAmbiguousInventory(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{name: "empty", env: map[string]string{"FIXTURE_SOURCE_MODE": "empty"}},
		{name: "invalid signature", env: map[string]string{"FIXTURE_SOURCE_MODE": "invalid-signature"}},
		{name: "duplicate", env: map[string]string{"FIXTURE_SOURCE_MODE": "duplicate"}},
		{name: "helper failure", env: map[string]string{"FAKE_GO_RUN_FAIL": "1"}},
		{name: "missing runner temp", env: map[string]string{"OMIT_RUNNER_TEMP": "1"}},
		{name: "missing run identity", env: map[string]string{"OMIT_RUN_ID": "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := runIntegrationEntrypoint(t, tc.env)
			if run.exitCode == 0 {
				t.Fatalf("invalid inventory/input returned success; commands:\n%s", run.commands)
			}
			if len(run.calls["go-test"]) != 0 {
				t.Fatalf("invalid inventory/input reached go test: %#v", run.calls["go-test"])
			}
			if len(run.calls["docker-create"]) != 0 {
				t.Fatalf("invalid inventory/input allocated an emulator: %#v", run.calls["docker-create"])
			}
		})
	}
}

func TestIntegrationEntrypointRefusesOccupiedEndpointWithoutTouchingItsOwner(t *testing.T) {
	run := runIntegrationEntrypoint(t, map[string]string{"FAKE_ENDPOINT_OCCUPIED": "1"})
	if run.exitCode == 0 {
		t.Fatalf("occupied endpoint returned success; output:\n%s", run.output)
	}
	if len(run.calls["docker-create"]) != 0 || len(run.calls["docker-rm"]) != 0 {
		t.Fatalf("occupied endpoint caused Docker mutation: create=%#v remove=%#v", run.calls["docker-create"], run.calls["docker-rm"])
	}
	if len(run.calls["go-test"]) != 0 {
		t.Fatalf("occupied endpoint reached go test: %#v", run.calls["go-test"])
	}
}

func TestIntegrationEntrypointFailsClosedAcrossStartupTestAndCleanupFailures(t *testing.T) {
	cases := []struct {
		name            string
		env             map[string]string
		wantTest        bool
		wantExactRemove bool
	}{
		{name: "startup", env: map[string]string{"FAKE_READY": "0"}, wantExactRemove: true},
		{name: "go test", env: map[string]string{"FAKE_GO_TEST_EXIT": "23"}, wantTest: true, wantExactRemove: true},
		{name: "cleanup", env: map[string]string{"FAKE_DOCKER_REMOVE_FAIL": "1"}, wantTest: true, wantExactRemove: true},
		{name: "interrupted test process", env: map[string]string{"FAKE_GO_TEST_EXIT": "130"}, wantTest: true, wantExactRemove: true},
		{name: "container create", env: map[string]string{"FAKE_DOCKER_CREATE_FAIL": "1"}},
		{name: "malformed owned id", env: map[string]string{"FAKE_MALFORMED_CID": "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := runIntegrationEntrypoint(t, tc.env)
			if run.exitCode == 0 {
				t.Fatalf("failure returned success; output:\n%s", run.output)
			}
			if got := len(run.calls["go-test"]) != 0; got != tc.wantTest {
				t.Fatalf("go test presence = %v, want %v; calls=%#v", got, tc.wantTest, run.calls["go-test"])
			}
			if tc.wantExactRemove {
				requireOwnedCleanup(t, run)
			}
			if tc.env["FAKE_MALFORMED_CID"] == "1" && len(run.calls["docker-rm"]) != 0 {
				t.Fatalf("malformed CID reached remove: %#v", run.calls["docker-rm"])
			}
			if len(run.calls["docker-prune"]) != 0 {
				t.Fatalf("cleanup used prune: %#v", run.calls["docker-prune"])
			}
		})
	}
}

func TestIntegrationEntrypointBoundsReadinessAndCleansOwnedResources(t *testing.T) {
	run := runIntegrationEntrypoint(t, map[string]string{"FAKE_READY": "0"})
	if run.exitCode == 0 {
		t.Fatalf("readiness failure returned success; output:\n%s", run.output)
	}
	sleeps := len(run.calls["sleep"])
	if sleeps == 0 || sleeps > 30 {
		t.Fatalf("readiness sleep calls = %d, want 1..30", sleeps)
	}
	requireOwnedCleanup(t, run)
}

func TestIntegrationEntrypointCleansOwnedResourcesOnTermination(t *testing.T) {
	run := runIntegrationEntrypoint(t, map[string]string{"FAKE_GO_SIGNAL_PARENT": "1"})
	if run.exitCode != 143 {
		t.Fatalf("terminated entrypoint exit = %d, want 143; output:\n%s", run.exitCode, run.output)
	}
	requireOwnedCleanup(t, run)
}

func TestIntegrationEntrypointPreservesSubprocessStatusAndPromotesCleanupFailure(t *testing.T) {
	testFailure := runIntegrationEntrypoint(t, map[string]string{"FAKE_GO_TEST_EXIT": "37"})
	if testFailure.exitCode != 37 {
		t.Fatalf("test failure exit = %d, want 37; output:\n%s", testFailure.exitCode, testFailure.output)
	}
	cleanupFailure := runIntegrationEntrypoint(t, map[string]string{"FAKE_DOCKER_REMOVE_FAIL": "1"})
	if cleanupFailure.exitCode == 0 {
		t.Fatalf("cleanup failure returned success; output:\n%s", cleanupFailure.output)
	}
}

func TestIntegrationEntrypointConsumersUseOneBoundaryAndKeepUnitTier(t *testing.T) {
	makefile := readRepositoryFile(t, "Makefile")
	workflow := readRepositoryFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	if !strings.Contains(makefile, integrationEntrypoint) {
		t.Fatalf("Makefile test-integration does not invoke %s", integrationEntrypoint)
	}
	if !strings.Contains(workflow, integrationEntrypoint) {
		t.Fatalf("CI integration step does not invoke %s", integrationEntrypoint)
	}
	for _, unitCommand := range []string{"go vet ./...", "go test ./..."} {
		if !strings.Contains(makefile, unitCommand) {
			t.Fatalf("default unit tier lost %q", unitCommand)
		}
	}
	if strings.Contains(makefile, "docker rm -f task-graph-emulator") {
		t.Fatal("Makefile still owns an emulator through a shared container name")
	}
	if strings.Contains(workflow, "TestIntegrationReviewProducer|TestReviewNativeCompatibility") {
		t.Fatal("CI still carries a partial hard-coded integration selection")
	}
}

func runIntegrationEntrypoint(t *testing.T, overrides map[string]string) entrypointRun {
	t.Helper()
	repositoryRoot, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	scriptBytes, err := os.ReadFile(filepath.Join(repositoryRoot, integrationEntrypoint))
	if err != nil {
		t.Fatalf("read integration entrypoint: %v", err)
	}
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("resolve Go toolchain: %v", err)
	}

	sandbox := t.TempDir()
	scriptPath := filepath.Join(sandbox, integrationEntrypoint)
	if err := os.MkdirAll(filepath.Dir(scriptPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(scriptPath, scriptBytes, 0o755); err != nil {
		t.Fatal(err)
	}
	writeGoFixture(t, sandbox, overrides["FIXTURE_SOURCE_MODE"])

	binDir := filepath.Join(sandbox, "bin")
	stateDir := filepath.Join(sandbox, "state")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	commandLog := filepath.Join(sandbox, "commands.log")
	writeCommandDouble(t, binDir, "go", fakeGoCommand)
	writeCommandDouble(t, binDir, "docker", fakeDockerCommand)
	writeCommandDouble(t, binDir, "curl", fakeCurlCommand)
	writeCommandDouble(t, binDir, "timeout", fakeTimeoutCommand)
	writeCommandDouble(t, binDir, "sleep", fakeSleepCommand)

	runnerTemp := filepath.Join(sandbox, "runner-temp")
	if err := os.Mkdir(runnerTemp, 0o755); err != nil {
		t.Fatal(err)
	}
	env := append([]string{}, os.Environ()...)
	env = setEnvironment(env, "PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	env = setEnvironment(env, "REAL_GO", realGo)
	env = setEnvironment(env, "FAKE_COMMAND_LOG", commandLog)
	env = setEnvironment(env, "FAKE_STATE_DIR", stateDir)
	env = setEnvironment(env, "FAKE_READY", "1")
	env = setEnvironment(env, "RUNNER_TEMP", runnerTemp)
	env = setEnvironment(env, "GITHUB_RUN_ID", "12345")
	env = setEnvironment(env, "GITHUB_RUN_ATTEMPT", "1")
	for key, value := range overrides {
		env = setEnvironment(env, key, value)
	}
	if overrides["OMIT_RUNNER_TEMP"] == "1" {
		env = unsetEnvironment(env, "RUNNER_TEMP")
	}
	if overrides["OMIT_RUN_ID"] == "1" {
		env = unsetEnvironment(env, "GITHUB_RUN_ID")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", scriptPath)
	command.Dir = sandbox
	command.Env = env
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err = command.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("entrypoint exceeded the harness deadline; output:\n%s", output.String())
	}
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatalf("execute entrypoint: %v; output:\n%s", err, output.String())
		}
		exitCode = exitError.ExitCode()
	}
	commands, readErr := os.ReadFile(commandLog)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	host, readErr := os.ReadFile(filepath.Join(stateDir, "go-test-host"))
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	return entrypointRun{exitCode: exitCode, output: output.String(), commands: string(commands), calls: readRecordedCalls(t, stateDir), runnerTemp: runnerTemp, goTestHost: string(host)}
}

func writeGoFixture(t *testing.T, directory, mode string) {
	t.Helper()
	files := map[string]string{
		"go.mod":       "module example.com/integration-entrypoint-fixture\n\ngo 1.22\n",
		"main.go":      "package main\n",
		"unit_test.go": "package main\n\nimport \"testing\"\n\nfunc TestUnitOwnsEndpoint(t *testing.T) {}\n",
		"variant_integration_test.go": "//go:build integration && extra\n\npackage main\n\nimport \"testing\"\n\nfunc TestWrongTagVariant(t *testing.T) {}\n",
	}
	switch mode {
	case "empty":
		files["tagged_integration_test.go"] = "//go:build integration\n\npackage main\n"
	case "invalid-signature":
		files["tagged_integration_test.go"] = "//go:build integration\n\npackage main\n\nfunc TestInvalidSignature() {}\n"
	case "duplicate":
		files["tagged_integration_test.go"] = validTaggedFixture
		files["duplicate_integration_test.go"] = "//go:build integration\n\npackage main\n\nimport \"testing\"\n\nfunc TestTaggedPrimary(t *testing.T) {}\n"
	default:
		files["tagged_integration_test.go"] = validTaggedFixture
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

const validTaggedFixture = `//go:build integration

package main

import "testing"

func TestTaggedPrimary(t *testing.T) {}
func TestAuditBoundary(t *testing.T) {}
func TestNativeCompatibility(t *testing.T) {}
`

func writeCommandDouble(t *testing.T, directory, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, name), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

func setEnvironment(environment []string, key, value string) []string {
	environment = unsetEnvironment(environment, key)
	return append(environment, key+"="+value)
}

func unsetEnvironment(environment []string, key string) []string {
	prefix := key + "="
	filtered := environment[:0]
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func readRecordedCalls(t *testing.T, stateDir string) map[string][][]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(stateDir, "*.argv"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	calls := map[string][][]string{}
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		parts := bytes.Split(content, []byte{0})
		if len(parts) > 0 && len(parts[len(parts)-1]) == 0 {
			parts = parts[:len(parts)-1]
		}
		argv := make([]string, len(parts))
		for i, part := range parts {
			argv[i] = string(part)
		}
		name := strings.SplitN(filepath.Base(path), ".", 2)[0]
		calls[name] = append(calls[name], argv)
	}
	return calls
}

func readRepositoryFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func requireCall(t *testing.T, run entrypointRun, name string, want []string) {
	t.Helper()
	for _, got := range run.calls[name] {
		if slicesEqual(got, want) {
			return
		}
	}
	t.Fatalf("%s call %q not found in %#v", name, want, run.calls[name])
}

func requireCallPrefix(t *testing.T, run entrypointRun, name string, prefix []string) {
	t.Helper()
	for _, got := range run.calls[name] {
		if len(got) >= len(prefix) && slicesEqual(got[:len(prefix)], prefix) {
			return
		}
	}
	t.Fatalf("%s call prefix %q not found in %#v", name, prefix, run.calls[name])
}

func requireOnlyCall(t *testing.T, run entrypointRun, name string, want []string) {
	t.Helper()
	if len(run.calls[name]) != 1 || !slicesEqual(run.calls[name][0], want) {
		t.Fatalf("%s calls = %#v, want exactly %#v", name, run.calls[name], want)
	}
}

func slicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	return true
}

func requirePinnedCreate(t *testing.T, run entrypointRun) {
	t.Helper()
	if len(run.calls["docker-create"]) != 1 {
		t.Fatalf("docker create calls = %#v, want one", run.calls["docker-create"])
	}
	got := run.calls["docker-create"][0]
	if len(got) != 11 || !slicesEqual(got[:3], []string{"container", "create", "--cidfile"}) {
		t.Fatalf("docker create argv = %#v", got)
	}
	if !strings.HasPrefix(got[3], run.runnerTemp+string(os.PathSeparator)) || filepath.Base(got[3]) != "container.id" {
		t.Fatalf("cidfile is outside owned scratch: %q", got[3])
	}
	wantTail := []string{"--name", "harness-review-12345-1", "--publish", "127.0.0.1:9010:9010", "--publish", "127.0.0.1:9020:9020", emulatorImage}
	if !slicesEqual(got[4:], wantTail) {
		t.Fatalf("docker create tail = %#v, want %#v", got[4:], wantTail)
	}
}

func requireReadinessProbes(t *testing.T, run entrypointRun) {
	t.Helper()
	requireCall(t, run, "curl", []string{"--silent", "--show-error", "--max-time", "2", "--output", "/dev/null", "http://127.0.0.1:9020/"})
	requireCall(t, run, "timeout", []string{"2s", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9010"})
}

func requireOwnedCleanup(t *testing.T, run entrypointRun) {
	t.Helper()
	requireOnlyCall(t, run, "docker-rm", []string{"container", "rm", "--force", ownedContainerID})
	requireCall(t, run, "docker-ls", []string{"container", "ls", "--all", "--quiet", "--no-trunc", "--filter", "id=" + ownedContainerID})
}

const commandPrelude = `#!/usr/bin/env bash
set -u
name="${0##*/}"
count_file="$FAKE_STATE_DIR/$name.count"
count=0
if [[ -f "$count_file" ]]; then count="$(<"$count_file")"; fi
count=$((count+1))
printf '%s' "$count" >"$count_file"
printf -v sequence '%04d' "$count"
printf '%s\0' "$@" >"$FAKE_STATE_DIR/$name.$sequence.argv"
printf '%s' "$name" >>"$FAKE_COMMAND_LOG"
printf ' %q' "$@" >>"$FAKE_COMMAND_LOG"
printf '\n' >>"$FAKE_COMMAND_LOG"
`

var fakeGoCommand = commandPrelude + `
case "${1:-}" in
  list) exec "$REAL_GO" "$@" ;;
  run)
    [[ "${FAKE_GO_RUN_FAIL:-0}" == 1 ]] && exit 41
    exec "$REAL_GO" "$@"
    ;;
  test)
    cp "$FAKE_STATE_DIR/go.$sequence.argv" "$FAKE_STATE_DIR/go-test.0001.argv"
    printf '%s' "${SPANNER_EMULATOR_HOST:-}" >"$FAKE_STATE_DIR/go-test-host"
    if [[ "${FAKE_GO_SIGNAL_PARENT:-0}" == 1 ]]; then
      kill -TERM "$PPID"
      exit 0
    fi
    exit "${FAKE_GO_TEST_EXIT:-0}"
    ;;
  *) exit 91 ;;
esac
`

var fakeDockerCommand = commandPrelude + `
cid="$(printf 'a%.0s' {1..64})"
case "${1:-} ${2:-}" in
  "container create")
    cp "$FAKE_STATE_DIR/docker.$sequence.argv" "$FAKE_STATE_DIR/docker-create.0001.argv"
    [[ "${FAKE_DOCKER_CREATE_FAIL:-0}" == 1 ]] && exit 1
    for ((i=1;i<=$#;i++)); do
      if [[ "${!i}" == "--cidfile" ]]; then j=$((i+1)); cidfile="${!j}"; fi
    done
    if [[ "${FAKE_MALFORMED_CID:-0}" == 1 ]]; then printf 'not-an-owned-id\n' >"$cidfile"; else printf '%s\n' "$cid" >"$cidfile"; fi
    ;;
  "container start")
    cp "$FAKE_STATE_DIR/docker.$sequence.argv" "$FAKE_STATE_DIR/docker-start.0001.argv"
    touch "$FAKE_STATE_DIR/started"
    ;;
  "container ls")
    cp "$FAKE_STATE_DIR/docker.$sequence.argv" "$FAKE_STATE_DIR/docker-ls.$sequence.argv"
    [[ -e "$FAKE_STATE_DIR/removed" ]] || printf '%s\n' "$cid"
    ;;
  "container rm")
    cp "$FAKE_STATE_DIR/docker.$sequence.argv" "$FAKE_STATE_DIR/docker-rm.0001.argv"
    [[ "${FAKE_DOCKER_REMOVE_FAIL:-0}" == 1 ]] && exit 1
    [[ "$*" == *"$cid"* ]] || exit 92
    touch "$FAKE_STATE_DIR/removed"
    ;;
  "container logs") cp "$FAKE_STATE_DIR/docker.$sequence.argv" "$FAKE_STATE_DIR/docker-logs.0001.argv" ;;
  "container prune") cp "$FAKE_STATE_DIR/docker.$sequence.argv" "$FAKE_STATE_DIR/docker-prune.0001.argv" ;;
  *) exit 93 ;;
esac
`

var fakeCurlCommand = commandPrelude + `
if [[ ! -e "$FAKE_STATE_DIR/started" ]]; then
  [[ "${FAKE_ENDPOINT_OCCUPIED:-0}" == 1 ]]
else
  [[ "${FAKE_READY:-1}" == 1 ]]
fi
`

var fakeTimeoutCommand = commandPrelude + `
shift
if [[ "${1:-}" == "bash" ]]; then
  if [[ ! -e "$FAKE_STATE_DIR/started" ]]; then
    [[ "${FAKE_ENDPOINT_OCCUPIED:-0}" == 1 ]]
  else
    [[ "${FAKE_READY:-1}" == 1 ]]
  fi
else
  exec "$@"
fi
`

var fakeSleepCommand = commandPrelude + `exit 0
`
