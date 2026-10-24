package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const integrationEntrypoint = "scripts/test-integration.sh"

type entrypointRun struct {
	exitCode int
	output   string
	commands string
}

func TestIntegrationEntrypointDiscoversTaggedSourceMembershipAndRunsEverySelectedTest(t *testing.T) {
	run := runIntegrationEntrypoint(t, map[string]string{
		"FAKE_INTEGRATION_TESTS": "TestTaggedPrimary\nTestAuditBoundary\nTestNativeCompatibility",
	})
	if run.exitCode != 0 {
		t.Fatalf("entrypoint exit = %d, want 0; output:\n%s", run.exitCode, run.output)
	}
	requireCommand(t, run.commands, "go list", "-tags=integration")
	requireCommandWithout(t, run.commands, "go list", "-tags=integration")
	requireCommand(t, run.commands, "go run")
	testCommand := commandLine(t, run.commands, "go test")
	for _, name := range []string{"TestTaggedPrimary", "TestAuditBoundary", "TestNativeCompatibility"} {
		if !strings.Contains(testCommand, name) {
			t.Fatalf("go test command omitted discovered tagged test %q: %s", name, testCommand)
		}
	}
	if strings.Contains(testCommand, "TestUnitOwnsEndpoint") {
		t.Fatalf("go test command selected an untagged unit fixture: %s", testCommand)
	}
	for _, want := range []string{"-tags=integration", "-count=1", "."} {
		if !strings.Contains(testCommand, want) {
			t.Fatalf("go test command missing %q: %s", want, testCommand)
		}
	}
	if !strings.Contains(testCommand, "^(") || !strings.Contains(testCommand, ")$") {
		t.Fatalf("selection is not exact and anchored: %s", testCommand)
	}
	requireOwnedCleanup(t, run.commands)
}

func TestIntegrationEntrypointRefusesInvalidOrAmbiguousInventory(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{name: "empty", env: map[string]string{"FAKE_INTEGRATION_TESTS": ""}},
		{name: "malformed", env: map[string]string{"FAKE_INTEGRATION_TESTS": "not-a-go-test"}},
		{name: "duplicate", env: map[string]string{"FAKE_INTEGRATION_TESTS": "TestTaggedPrimary\nTestTaggedPrimary"}},
		{name: "missing runner temp", env: map[string]string{"OMIT_RUNNER_TEMP": "1"}},
		{name: "missing run identity", env: map[string]string{"OMIT_RUN_ID": "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := runIntegrationEntrypoint(t, tc.env)
			if run.exitCode == 0 {
				t.Fatalf("invalid inventory/input returned success; commands:\n%s", run.commands)
			}
			if strings.Contains(run.commands, "go test") {
				t.Fatalf("invalid inventory/input reached go test: %s", run.commands)
			}
			if strings.Contains(run.commands, "docker container create") {
				t.Fatalf("invalid inventory/input allocated an emulator: %s", run.commands)
			}
		})
	}
}

func TestIntegrationEntrypointRefusesOccupiedEndpointWithoutTouchingItsOwner(t *testing.T) {
	run := runIntegrationEntrypoint(t, map[string]string{"FAKE_ENDPOINT_OCCUPIED": "1"})
	if run.exitCode == 0 {
		t.Fatalf("occupied endpoint returned success; output:\n%s", run.output)
	}
	if strings.Contains(run.commands, "docker container create") || strings.Contains(run.commands, "docker container rm") {
		t.Fatalf("occupied endpoint caused Docker mutation: %s", run.commands)
	}
	if strings.Contains(run.commands, "go test") {
		t.Fatalf("occupied endpoint reached go test: %s", run.commands)
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
			if got := strings.Contains(run.commands, "go test"); got != tc.wantTest {
				t.Fatalf("go test presence = %v, want %v; commands:\n%s", got, tc.wantTest, run.commands)
			}
			if tc.wantExactRemove {
				requireOwnedCleanup(t, run.commands)
			}
			if strings.Contains(run.commands, "docker rm") || strings.Contains(run.commands, "docker container prune") {
				t.Fatalf("cleanup used a shared name or prune route: %s", run.commands)
			}
		})
	}
}

func TestIntegrationEntrypointCleansOwnedResourcesOnTermination(t *testing.T) {
	run := runIntegrationEntrypoint(t, map[string]string{"FAKE_GO_SIGNAL_PARENT": "1"})
	if run.exitCode != 143 {
		t.Fatalf("terminated entrypoint exit = %d, want 143; output:\n%s", run.exitCode, run.output)
	}
	requireOwnedCleanup(t, run.commands)
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

	sandbox := t.TempDir()
	scriptPath := filepath.Join(sandbox, integrationEntrypoint)
	if err := os.MkdirAll(filepath.Dir(scriptPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(scriptPath, scriptBytes, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"unit_test.go": "package main\nfunc TestUnitOwnsEndpoint() {}\n",
		"tagged_integration_test.go": "//go:build integration\n\npackage main\nfunc TestTaggedPrimary() {}\nfunc TestAuditBoundary() {}\nfunc TestNativeCompatibility() {}\n",
	} {
		if err := os.WriteFile(filepath.Join(sandbox, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	binDir := filepath.Join(sandbox, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	commandLog := filepath.Join(sandbox, "commands.log")
	stateDir := filepath.Join(sandbox, "state")
	if err := os.Mkdir(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
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
	env = setEnvironment(env, "FAKE_COMMAND_LOG", commandLog)
	env = setEnvironment(env, "FAKE_STATE_DIR", stateDir)
	env = setEnvironment(env, "FAKE_INTEGRATION_TESTS", "TestTaggedPrimary\nTestAuditBoundary\nTestNativeCompatibility")
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

	command := exec.Command("bash", scriptPath)
	command.Dir = sandbox
	command.Env = env
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err = command.Run()
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
	return entrypointRun{exitCode: exitCode, output: output.String(), commands: string(commands)}
}

func writeCommandDouble(t *testing.T, directory, name, body string) {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
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

func readRepositoryFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func requireCommand(t *testing.T, commands string, parts ...string) {
	t.Helper()
	for _, line := range strings.Split(commands, "\n") {
		matched := true
		for _, part := range parts {
			matched = matched && strings.Contains(line, part)
		}
		if matched {
			return
		}
	}
	t.Fatalf("command containing %q not found:\n%s", parts, commands)
}

func commandLine(t *testing.T, commands, prefix string) string {
	t.Helper()
	for _, line := range strings.Split(commands, "\n") {
		if strings.Contains(line, prefix) {
			return line
		}
	}
	t.Fatalf("%s command not found:\n%s", prefix, commands)
	return ""
}

func requireCommandWithout(t *testing.T, commands, required, forbidden string) {
	t.Helper()
	for _, line := range strings.Split(commands, "\n") {
		if strings.Contains(line, required) && !strings.Contains(line, forbidden) {
			return
		}
	}
	t.Fatalf("command containing %q without %q not found:\n%s", required, forbidden, commands)
}

func requireOwnedCleanup(t *testing.T, commands string) {
	t.Helper()
	line := commandLine(t, commands, "docker container rm")
	if !strings.Contains(line, strings.Repeat("a", 64)) || !strings.Contains(line, "--force") {
		t.Fatalf("cleanup did not remove the exact owned CID: %s", line)
	}
	requireCommand(t, commands, "docker container ls", "--filter", "id="+strings.Repeat("a", 64))
}

const commandPrelude = `#!/usr/bin/env bash
set -u
printf '%s' "${0##*/}" >>"$FAKE_COMMAND_LOG"
printf ' %q' "$@" >>"$FAKE_COMMAND_LOG"
printf '\n' >>"$FAKE_COMMAND_LOG"
`

var fakeGoCommand = commandPrelude + `
case "${1:-}" in
  list)
    if [[ " $* " == *" -tags=integration "* ]]; then
      printf '%s\n' unit_test.go tagged_integration_test.go
    else
      printf '%s\n' unit_test.go
    fi
    ;;
  run)
    printf '%b\n' "${FAKE_INTEGRATION_TESTS:-}"
    ;;
  test)
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
	[[ "${FAKE_DOCKER_CREATE_FAIL:-0}" == 1 ]] && exit 1
    for ((i=1;i<=$#;i++)); do
      if [[ "${!i}" == "--cidfile" ]]; then j=$((i+1)); cidfile="${!j}"; fi
    done
    if [[ "${FAKE_MALFORMED_CID:-0}" == 1 ]]; then printf 'not-an-owned-id\n' >"$cidfile"; else printf '%s\n' "$cid" >"$cidfile"; fi
    ;;
  "container start") touch "$FAKE_STATE_DIR/started" ;;
  "container ls") [[ -e "$FAKE_STATE_DIR/removed" ]] || printf '%s\n' "$cid" ;;
  "container rm")
    [[ "${FAKE_DOCKER_REMOVE_FAIL:-0}" == 1 ]] && exit 1
    [[ "$*" == *"$cid"* ]] || exit 92
    touch "$FAKE_STATE_DIR/removed"
    ;;
  "container logs") ;;
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
