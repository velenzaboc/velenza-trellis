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
	defaultSelection      = "^(TestAuditBoundary|TestNativeCompatibility|TestTaggedPrimary)$"
	alternateSelection    = "^(TestAlternateAlpha|TestAlternateAudit|TestAlternateBeta)$"
)

type recordedCall struct {
	name   string
	argv   []string
	stdout string
}

type entrypointRun struct {
	exitCode   int
	output     string
	commands   string
	calls      map[string][][]string
	events     []recordedCall
	runnerTemp string
	goTestHost string
}

func TestIntegrationEntrypointDiscoversTaggedSourceMembershipAndRunsEverySelectedTest(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		selection string
	}{
		{name: "one tagged file", selection: defaultSelection},
		{name: "different names across tagged files", env: map[string]string{"FIXTURE_SOURCE_MODE": "alternate-valid"}, selection: alternateSelection},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := runIntegrationEntrypoint(t, tc.env)
			if run.exitCode != 0 {
				t.Fatalf("entrypoint exit = %d, want 0; output:\n%s", run.exitCode, run.output)
			}
			requireCall(t, run, "go", []string{"list", "-f", "{{range .TestGoFiles}}{{println .}}{{end}}", "."})
			requireCall(t, run, "go", []string{"list", "-tags=integration", "-f", "{{range .TestGoFiles}}{{println .}}{{end}}", "."})
			requireASTHelperCall(t, run)
			requireOnlyCall(t, run, "go-test", []string{"test", "-tags=integration", ".", "-run", tc.selection, "-count=1"})
			if run.goTestHost != "localhost:9010" {
				t.Fatalf("SPANNER_EMULATOR_HOST = %q, want localhost:9010", run.goTestHost)
			}
			if strings.Contains(tc.selection, "TestUnitOwnsEndpoint") || strings.Contains(tc.selection, "TestWrongTagVariant") {
				t.Fatalf("selection admitted an untagged or unsatisfied-tag fixture: %s", tc.selection)
			}
			requirePinnedCreate(t, run, true)
			requireEndpointLifecycle(t, run)
			requireOwnedCleanup(t, run)
		})
	}
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
			if countDockerSubcommand(run, "create") != 0 {
				t.Fatalf("invalid inventory/input allocated an emulator: %#v", run.events)
			}
		})
	}
}

func TestIntegrationEntrypointRefusesEitherOccupiedEndpointWithoutTouchingItsOwner(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{name: "REST", env: map[string]string{"FAKE_REST_OCCUPIED": "1"}},
		{name: "gRPC", env: map[string]string{"FAKE_GRPC_OCCUPIED": "1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := runIntegrationEntrypoint(t, tc.env)
			if run.exitCode == 0 {
				t.Fatalf("occupied endpoint returned success; output:\n%s", run.output)
			}
			if countDockerSubcommand(run, "create") != 0 || countDockerSubcommand(run, "rm") != 0 {
				t.Fatalf("occupied endpoint caused Docker mutation: %#v", run.events)
			}
			if len(run.calls["go-test"]) != 0 {
				t.Fatalf("occupied endpoint reached go test: %#v", run.calls["go-test"])
			}
		})
	}
}

func TestIntegrationEntrypointRunsWithoutCIEnvironment(t *testing.T) {
	first := runIntegrationEntrypoint(t, map[string]string{"LOCAL_NO_CI": "1"})
	second := runIntegrationEntrypoint(t, map[string]string{"LOCAL_NO_CI": "1"})
	for _, run := range []entrypointRun{first, second} {
		if run.exitCode != 0 {
			t.Fatalf("local invocation exit = %d, want 0; output:\n%s", run.exitCode, run.output)
		}
		requirePinnedCreate(t, run, false)
		requireOwnedCleanup(t, run)
	}
	firstCreate := onlyDockerSubcommand(t, first, "create")
	secondCreate := onlyDockerSubcommand(t, second, "create")
	if firstCreate.argv[5] == secondCreate.argv[5] {
		t.Fatalf("two local invocations reused one container name: %q", firstCreate.argv[5])
	}
	if firstCreate.argv[3] == secondCreate.argv[3] {
		t.Fatalf("two local invocations reused one CID file: %q", firstCreate.argv[3])
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
		{name: "owner remains after cleanup", env: map[string]string{"FAKE_DOCKER_OWNER_REMAINS": "1"}, wantTest: true, wantExactRemove: true},
		{name: "cleanup verification", env: map[string]string{"FAKE_DOCKER_VERIFY_FAIL": "1"}, wantTest: true, wantExactRemove: true},
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
				requireExactOwnedRemove(t, run)
				if tc.env["FAKE_DOCKER_REMOVE_FAIL"] != "1" && tc.env["FAKE_DOCKER_VERIFY_FAIL"] != "1" && tc.env["FAKE_DOCKER_OWNER_REMAINS"] != "1" {
					requireOwnedCleanup(t, run)
				}
			}
			if tc.env["FAKE_MALFORMED_CID"] == "1" && countDockerSubcommand(run, "rm") != 0 {
				t.Fatalf("malformed CID reached remove: %#v", run.events)
			}
			if countDockerSubcommand(run, "prune") != 0 {
				t.Fatalf("cleanup used prune: %#v", run.events)
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
	requireOnlyExecutableBoundary(t, "Makefile test-integration", makeRecipes(makefile, "test-integration"), []string{"./" + integrationEntrypoint})
	requireOnlyExecutableBoundary(t, "Makefile test-unit", makeRecipes(makefile, "test-unit"), []string{"go vet ./...", "go test ./..."})
	requireOnlyExecutableBoundary(t, "CI integration step", workflowIntegrationRuns(workflow), []string{"./" + integrationEntrypoint})
}

func TestIntegrationEntrypointConsumerParsersExposeDuplicateExecutableBoundaries(t *testing.T) {
	makefile := "test-integration:\n\t@./scripts/test-integration.sh\n\ntest-integration: stale\n\t@go test -tags=integration ./...\n"
	if got := len(makeRecipes(makefile, "test-integration")); got != 2 {
		t.Fatalf("duplicate Makefile targets discovered = %d, want 2", got)
	}
	workflow := "steps:\n  - name: Integration tier\n    run: |\n      ./scripts/test-integration.sh\n  - name: Stale integration tier\n    run: |\n      go test -tags=integration ./...\n"
	if got := len(workflowIntegrationRuns(workflow)); got != 2 {
		t.Fatalf("duplicate CI integration run blocks discovered = %d, want 2", got)
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
	nativeTemp := filepath.Join(sandbox, "native-temp")
	if err := os.Mkdir(nativeTemp, 0o755); err != nil {
		t.Fatal(err)
	}
	env := append([]string{}, os.Environ()...)
	env = setEnvironment(env, "PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	env = setEnvironment(env, "REAL_GO", realGo)
	env = setEnvironment(env, "FAKE_COMMAND_LOG", commandLog)
	env = setEnvironment(env, "FAKE_STATE_DIR", stateDir)
	env = setEnvironment(env, "FAKE_READY", "1")
	env = setEnvironment(env, "TMPDIR", nativeTemp)
	env = setEnvironment(env, "RUNNER_TEMP", runnerTemp)
	env = setEnvironment(env, "GITHUB_RUN_ID", "12345")
	env = setEnvironment(env, "GITHUB_RUN_ATTEMPT", "1")
	for key, value := range overrides {
		env = setEnvironment(env, key, value)
	}
	if overrides["LOCAL_NO_CI"] == "1" {
		env = unsetEnvironment(env, "RUNNER_TEMP")
		env = unsetEnvironment(env, "GITHUB_RUN_ID")
		env = unsetEnvironment(env, "GITHUB_RUN_ATTEMPT")
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
	calls, events := readRecordedCalls(t, stateDir)
	ownedTemp := runnerTemp
	if overrides["LOCAL_NO_CI"] == "1" {
		ownedTemp = nativeTemp
	}
	return entrypointRun{exitCode: exitCode, output: output.String(), commands: string(commands), calls: calls, events: events, runnerTemp: ownedTemp, goTestHost: string(host)}
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
	case "alternate-valid":
		files["alpha_integration_test.go"] = "//go:build integration\n\npackage main\n\nimport \"testing\"\n\nfunc TestAlternateAlpha(t *testing.T) {}\n"
		files["beta_integration_test.go"] = "//go:build integration\n\npackage main\n\nimport \"testing\"\n\nfunc TestAlternateBeta(t *testing.T) {}\nfunc TestAlternateAudit(t *testing.T) {}\n"
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

func readRecordedCalls(t *testing.T, stateDir string) (map[string][][]string, []recordedCall) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(stateDir, "call.*.argv"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	calls := map[string][][]string{}
	events := make([]recordedCall, 0, len(paths))
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		parts := bytes.Split(content, []byte{0})
		if len(parts) > 0 && len(parts[len(parts)-1]) == 0 {
			parts = parts[:len(parts)-1]
		}
		if len(parts) == 0 {
			t.Fatalf("empty recorded call %s", path)
		}
		name := string(parts[0])
		argv := make([]string, len(parts)-1)
		for i, part := range parts[1:] {
			argv[i] = string(part)
		}
		calls[name] = append(calls[name], argv)
		if name == "go" && len(argv) > 0 && argv[0] == "test" {
			calls["go-test"] = append(calls["go-test"], argv)
		}
		stdout, readErr := os.ReadFile(strings.TrimSuffix(path, ".argv") + ".stdout")
		if readErr != nil && !os.IsNotExist(readErr) {
			t.Fatal(readErr)
		}
		events = append(events, recordedCall{name: name, argv: argv, stdout: string(stdout)})
	}
	return calls, events
}

func readRepositoryFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func executableLines(source string) []string {
	var lines []string
	for _, raw := range strings.Split(source, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, strings.TrimPrefix(line, "@"))
	}
	return lines
}

func makeRecipes(source, target string) []string {
	lines := strings.Split(source, "\n")
	header := target + ":"
	var recipes []string
	for i, line := range lines {
		if !strings.HasPrefix(line, header) {
			continue
		}
		var recipe []string
		for _, candidate := range lines[i+1:] {
			if strings.HasPrefix(candidate, "\t") {
				recipe = append(recipe, candidate)
				continue
			}
			if strings.TrimSpace(candidate) == "" || strings.HasPrefix(strings.TrimSpace(candidate), "#") {
				continue
			}
			break
		}
		recipes = append(recipes, strings.Join(recipe, "\n"))
	}
	return recipes
}

func workflowIntegrationRuns(source string) []string {
	lines := strings.Split(source, "\n")
	var runs []string
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "- name:") || !strings.Contains(strings.ToLower(trimmed), "integration") {
			continue
		}
		for j := i + 1; j < len(lines); j++ {
			if strings.HasPrefix(strings.TrimSpace(lines[j]), "- name:") {
				break
			}
			if strings.TrimSpace(lines[j]) != "run: |" {
				continue
			}
			indent := len(lines[j]) - len(strings.TrimLeft(lines[j], " "))
			var run []string
			for _, candidate := range lines[j+1:] {
				candidateIndent := len(candidate) - len(strings.TrimLeft(candidate, " "))
				if strings.TrimSpace(candidate) != "" && candidateIndent <= indent {
					break
				}
				run = append(run, candidate)
			}
			runs = append(runs, strings.Join(run, "\n"))
			break
		}
	}
	return runs
}

func requireOnlyExecutableBoundary(t *testing.T, name string, sources []string, want []string) {
	t.Helper()
	if len(sources) != 1 {
		t.Fatalf("%s executable boundaries = %d, want exactly 1", name, len(sources))
	}
	got := executableLines(sources[0])
	if !slicesEqual(got, want) {
		t.Fatalf("%s executable lines = %#v, want %#v", name, got, want)
	}
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

func requireASTHelperCall(t *testing.T, run entrypointRun) {
	t.Helper()
	for _, got := range run.calls["go"] {
		if len(got) < 4 || got[0] != "run" {
			continue
		}
		separator := -1
		for i, arg := range got {
			if arg == "--" {
				separator = i
				break
			}
		}
		if separator < 2 || separator == len(got)-1 {
			continue
		}
		for _, source := range got[separator+1:] {
			if !strings.HasSuffix(source, ".go") {
				t.Fatalf("AST helper input %q is not a Go source", source)
			}
		}
		return
	}
	t.Fatalf("go run call with explicit argument separator not found: %#v", run.calls["go"])
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
	}
	return true
}

func requirePinnedCreate(t *testing.T, run entrypointRun, wantCILabel bool) {
	t.Helper()
	got := onlyDockerSubcommand(t, run, "create").argv
	if len(got) != 11 || !slicesEqual(got[:3], []string{"container", "create", "--cidfile"}) {
		t.Fatalf("docker create argv = %#v", got)
	}
	if !strings.HasPrefix(got[3], run.runnerTemp+string(os.PathSeparator)) || filepath.Base(got[3]) != "container.id" {
		t.Fatalf("cidfile is outside owned scratch: %q", got[3])
	}
	wantSuffix := []string{"--publish", "127.0.0.1:9010:9010", "--publish", "127.0.0.1:9020:9020", emulatorImage}
	if len(got[4:]) != 7 || got[4] != "--name" || got[5] == "" || !slicesEqual(got[6:], wantSuffix) {
		t.Fatalf("docker create identity/mappings = %#v", got[4:])
	}
	if wantCILabel && got[5] != "harness-review-12345-1" {
		t.Fatalf("CI container name = %q, want harness-review-12345-1", got[5])
	}
}

func onlyDockerSubcommand(t *testing.T, run entrypointRun, subcommand string) recordedCall {
	t.Helper()
	var matches []recordedCall
	for _, event := range run.events {
		if event.name == "docker" && len(event.argv) >= 2 && event.argv[0] == "container" && event.argv[1] == subcommand {
			matches = append(matches, event)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("docker container %s calls = %#v, want one", subcommand, matches)
	}
	return matches[0]
}

func countDockerSubcommand(run entrypointRun, subcommand string) int {
	count := 0
	for _, event := range run.events {
		if event.name == "docker" && len(event.argv) >= 2 && event.argv[0] == "container" && event.argv[1] == subcommand {
			count++
		}
	}
	return count
}

func eventIndex(run entrypointRun, name string, argv []string, after int) int {
	for i := after + 1; i < len(run.events); i++ {
		if run.events[i].name == name && slicesEqual(run.events[i].argv, argv) {
			return i
		}
	}
	return -1
}

func requireEndpointLifecycle(t *testing.T, run entrypointRun) {
	t.Helper()
	rest := []string{"--silent", "--show-error", "--max-time", "2", "--output", "/dev/null", "http://127.0.0.1:9020/"}
	grpc := []string{"2s", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9010"}
	preflightREST := eventIndex(run, "curl", rest, -1)
	preflightGRPC := eventIndex(run, "timeout", grpc, -1)
	create := -1
	for i, event := range run.events {
		if event.name == "docker" && len(event.argv) >= 2 && slicesEqual(event.argv[:2], []string{"container", "create"}) {
			create = i
			break
		}
	}
	readyREST := eventIndex(run, "curl", rest, create)
	readyGRPC := eventIndex(run, "timeout", grpc, create)
	if preflightREST < 0 || preflightGRPC < 0 || create < 0 || preflightREST > create || preflightGRPC > create || readyREST < 0 || readyGRPC < 0 {
		t.Fatalf("endpoint lifecycle order is incomplete: %#v", run.events)
	}
}

func requireOwnedCleanup(t *testing.T, run entrypointRun) {
	t.Helper()
	rm := requireExactOwnedRemove(t, run)
	lsArgv := []string{"container", "ls", "--all", "--quiet", "--no-trunc", "--filter", "id=" + ownedContainerID}
	lsIndex := eventIndex(run, "docker", lsArgv, rm)
	if lsIndex < 0 {
		t.Fatalf("post-remove exact-CID verification missing: %#v", run.events)
	}
	if run.events[lsIndex].stdout != "" {
		t.Fatalf("post-remove exact-CID verification returned owner %q", run.events[lsIndex].stdout)
	}
}

func requireExactOwnedRemove(t *testing.T, run entrypointRun) int {
	t.Helper()
	rmArgv := []string{"container", "rm", "--force", ownedContainerID}
	index := eventIndex(run, "docker", rmArgv, -1)
	if index < 0 || countDockerSubcommand(run, "rm") != 1 {
		t.Fatalf("Docker remove calls are not exactly one owned CID removal: %#v", run.events)
	}
	return index
}

const commandPrelude = `#!/usr/bin/env bash
set -u
name="${0##*/}"
count_file="$FAKE_STATE_DIR/global.count"
count=0
if [[ -f "$count_file" ]]; then count="$(<"$count_file")"; fi
count=$((count+1))
printf '%s' "$count" >"$count_file"
printf -v sequence '%04d' "$count"
printf '%s\0' "$name" "$@" >"$FAKE_STATE_DIR/call.$sequence.argv"
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
    [[ "${FAKE_DOCKER_CREATE_FAIL:-0}" == 1 ]] && exit 1
    for ((i=1;i<=$#;i++)); do
      if [[ "${!i}" == "--cidfile" ]]; then j=$((i+1)); cidfile="${!j}"; fi
    done
    if [[ "${FAKE_MALFORMED_CID:-0}" == 1 ]]; then printf 'not-an-owned-id\n' >"$cidfile"; else printf '%s\n' "$cid" >"$cidfile"; fi
    ;;
  "container start")
    touch "$FAKE_STATE_DIR/started"
    ;;
  "container ls")
    [[ "${FAKE_DOCKER_VERIFY_FAIL:-0}" == 1 && -e "$FAKE_STATE_DIR/removed" ]] && exit 1
    if [[ ! -e "$FAKE_STATE_DIR/removed" || "${FAKE_DOCKER_OWNER_REMAINS:-0}" == 1 ]]; then
      printf '%s\n' "$cid" | tee "$FAKE_STATE_DIR/call.$sequence.stdout"
    else
      : >"$FAKE_STATE_DIR/call.$sequence.stdout"
    fi
    ;;
  "container rm")
    [[ "${FAKE_DOCKER_REMOVE_FAIL:-0}" == 1 ]] && exit 1
    [[ "$*" == *"$cid"* ]] || exit 92
    touch "$FAKE_STATE_DIR/removed"
    ;;
  "container logs") exit 0 ;;
  "container prune") exit 0 ;;
  *) exit 93 ;;
esac
`

var fakeCurlCommand = commandPrelude + `
canonical="http://127.0.0.1:9020/"
url="${!#}"
if [[ ! -e "$FAKE_STATE_DIR/started" ]]; then
  [[ "$url" == "$canonical" && "${FAKE_REST_OCCUPIED:-0}" == 1 ]]
else
  [[ "$url" == "$canonical" && "${FAKE_READY:-1}" == 1 ]]
fi
`

var fakeTimeoutCommand = commandPrelude + `
shift
if [[ "${1:-}" == "bash" ]]; then
  command="${3:-}"
  canonical="exec 3<>/dev/tcp/127.0.0.1/9010"
  if [[ ! -e "$FAKE_STATE_DIR/started" ]]; then
    [[ "$command" == "$canonical" && "${FAKE_GRPC_OCCUPIED:-0}" == 1 ]]
  else
    [[ "$command" == "$canonical" && "${FAKE_READY:-1}" == 1 ]]
  fi
else
  exec "$@"
fi
`

var fakeSleepCommand = commandPrelude + `exit 0
`
