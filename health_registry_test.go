package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

// health_registry_test.go covers task_graph_health's visibility of the three
// closed registries P3-GT-FP-REGISTRIES introduced (task_statuses,
// dependency_kinds, artifact_kinds).
//
// The defect these tests pin: handleHealth reported registry_sizes for only
// {substrates, node_kinds, edge_labels, projections}, so a server whose closed
// registries were unconfigured reported overall_ready=true while being
// structurally unable to accept a single fleet_plan_native write — every one
// refused at requireTaskStatusRegistry / Resolve. A readiness verb that cannot
// see a write-blocking condition is not reporting readiness.
//
// handleHealth had NO test of any kind before this file: the only "Health"
// token in the test tree was TestTransportHealthAndMCPRouting, which exercises
// the static GET /healthz route (mcp.go transportHandler) and never calls
// handleHealth. That route is deliberately untouched by this work — it is the
// infra liveness probe, and it must not start reporting a registry condition.

// ---------------- helpers ----------------

// emptyRouter is a SubstrateRouter over no adapters: All succeeds and returns
// nothing, so handleHealth's substrate probe is a no-op and cannot do I/O. This
// isolates the registry logic under test.
type emptyRouter struct{}

func (emptyRouter) Adapter(context.Context, NodeKindDescriptor) (SubstrateAdapter, error) {
	return nil, errors.New("no adapters")
}
func (emptyRouter) All(context.Context) ([]SubstrateAdapter, error) { return nil, nil }
func (emptyRouter) ByID(string) (SubstrateAdapter, error)           { return nil, errors.New("no adapters") }
func (emptyRouter) Close() error                                    { return nil }

// failingRouter drives handleHealth's early-return path (env.Substrates.All
// error), used to prove a substrate failure does not mask a registry blocker.
type failingRouter struct{ err error }

func (r failingRouter) Adapter(context.Context, NodeKindDescriptor) (SubstrateAdapter, error) {
	return nil, r.err
}
func (r failingRouter) All(context.Context) ([]SubstrateAdapter, error) { return nil, r.err }
func (r failingRouter) ByID(string) (SubstrateAdapter, error)           { return nil, r.err }
func (failingRouter) Close() error                                      { return nil }

func nativeSubstrateDescriptor() SubstrateDescriptor {
	return SubstrateDescriptor{
		ID:              "fleet",
		SpannerDatabase: "projects/p/instances/i/databases/task-plan",
		TaskStoreType:   fleetPlanNativeStoreType,
		TaskTable:       "Tasks",
		WorklinkTable:   "TaskLinks",
		DepTable:        "TaskDeps",
	}
}

func schemaV1SubstrateDescriptor() SubstrateDescriptor {
	return SubstrateDescriptor{
		ID:              "legacy",
		SpannerDatabase: "projects/p/instances/i/databases/task-graph",
		TaskStoreType:   "schema_v1",
		NodeTable:       "Nodes",
		EdgeTable:       "Edges",
	}
}

// populatedRegistries builds the three registries the way cmdMCP does, from
// descriptor slices — one member each is enough to be non-empty.
func populatedRegistries(t *testing.T) (TaskStatusRegistry, DependencyKindRegistry, ArtifactKindRegistry) {
	t.Helper()
	ts, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "not_started"}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "done"}),
	})
	if err != nil {
		t.Fatalf("newTaskStatusRegistry: %v", err)
	}
	dk, err := newDependencyKindRegistry([]DependencyKindDescriptor{
		NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep"}),
	})
	if err != nil {
		t.Fatalf("newDependencyKindRegistry: %v", err)
	}
	ak, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "pr"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "commit"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "spec"}),
	})
	if err != nil {
		t.Fatalf("newArtifactKindRegistry: %v", err)
	}
	return ts, dk, ak
}

// emptyRegistriesAsCmdMCPBuildsThem reproduces the PRODUCTION shape of an
// unconfigured registry. cmdMCP (mcp.go) calls newTaskStatusRegistry(
// cfg.TaskStatuses) unconditionally, and those constructors return a NON-NIL
// registry over an empty map for nil input — so on a config carrying no
// task_statuses/dependency_kinds/artifact_kinds the Env fields are non-nil and
// EMPTY, never nil. A readiness check written against nilness would report
// ready on exactly this configuration.
func emptyRegistriesAsCmdMCPBuildsThem(t *testing.T) (TaskStatusRegistry, DependencyKindRegistry, ArtifactKindRegistry) {
	t.Helper()
	ts, err := newTaskStatusRegistry(nil)
	if err != nil {
		t.Fatalf("newTaskStatusRegistry(nil): %v", err)
	}
	dk, err := newDependencyKindRegistry(nil)
	if err != nil {
		t.Fatalf("newDependencyKindRegistry(nil): %v", err)
	}
	ak, err := newArtifactKindRegistry(nil)
	if err != nil {
		t.Fatalf("newArtifactKindRegistry(nil): %v", err)
	}
	if ts == nil || dk == nil || ak == nil {
		t.Fatal("constructors returned a nil registry for nil input; the production unconfigured shape is EMPTY, not nil — if this ever fires, registryHealth's size-based check needs revisiting")
	}
	return ts, dk, ak
}

func healthOf(t *testing.T, env *Env) HealthResult {
	t.Helper()
	out, err := handleHealth(context.Background(), env, healthInput{}, &snapshotPin{})
	if err != nil {
		t.Fatalf("handleHealth returned error: %v", err)
	}
	return out
}

func assertSize(t *testing.T, out HealthResult, key string, want int) {
	t.Helper()
	got, ok := out.RegistrySizes[key]
	if !ok {
		t.Fatalf("registry_sizes is missing key %q; keys present: %v", key, sortedSizeKeys(out))
	}
	if got != want {
		t.Errorf("registry_sizes[%q] = %d, want %d", key, got, want)
	}
}

func sortedSizeKeys(out HealthResult) []string {
	keys := make([]string, 0, len(out.RegistrySizes))
	for k := range out.RegistrySizes {
		keys = append(keys, k)
	}
	return keys
}

func blockingMentioning(out HealthResult, token string) bool {
	for _, b := range out.BlockingIssues {
		if strings.Contains(b, token) {
			return true
		}
	}
	return false
}

// ---------------- tests ----------------

// TestHealthReportsClosedRegistrySizes is the visibility half: all three closed
// registries appear in registry_sizes alongside the original four, and a fully
// configured fleet_plan_native server still reports ready.
//
// RED before the fix: registry_sizes carried only the four generic keys, so
// each assertSize on task_statuses/dependency_kinds/artifact_kinds fails with
// "registry_sizes is missing key".
func TestHealthReportsClosedRegistrySizes(t *testing.T) {
	ts, dk, ak := populatedRegistries(t)
	env := &Env{
		Config: SpecGraphConfig{
			Mode:        ModeExperimentalBootstrap,
			Substrates:  []SubstrateDescriptor{nativeSubstrateDescriptor()},
			NodeKinds:   []NodeKindDescriptor{{}, {}},
			EdgeLabels:  []EdgeLabelDescriptor{{}, {}, {}},
			Projections: []ProjectionDescriptor{{Name: "p"}},
		},
		Substrates:      emptyRouter{},
		TaskStatuses:    ts,
		DependencyKinds: dk,
		ArtifactKinds:   ak,
	}

	out := healthOf(t, env)

	// The four that already existed must be unchanged.
	assertSize(t, out, "substrates", 1)
	assertSize(t, out, "node_kinds", 2)
	assertSize(t, out, "edge_labels", 3)
	assertSize(t, out, "projections", 1)
	// The three this task adds.
	assertSize(t, out, "task_statuses", 2)
	assertSize(t, out, "dependency_kinds", 1)
	assertSize(t, out, "artifact_kinds", 3)

	if !out.OverallReady {
		t.Errorf("overall_ready = false on a fully configured server; blocking_issues = %v", out.BlockingIssues)
	}
	if len(out.BlockingIssues) != 0 {
		t.Errorf("blocking_issues = %v, want none", out.BlockingIssues)
	}
}

// TestHealthNotReadyWhenClosedRegistryEmptyOnFleetPlanNative is the readiness
// half — the outage shape. A fleet_plan_native substrate with an unconfigured
// registry can accept no write, so it must not report ready.
//
// The "empty (production shape)" subtest is the load-bearing one: it is what
// cmdMCP actually produces for a registry-less config, and it would pass a
// nil-based check.
//
// RED before the fix: overall_ready = true in every subtest.
func TestHealthNotReadyWhenClosedRegistryEmptyOnFleetPlanNative(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T) (TaskStatusRegistry, DependencyKindRegistry, ArtifactKindRegistry)
	}{
		{"empty (production shape: cmdMCP builds non-nil registries from an absent config section)", emptyRegistriesAsCmdMCPBuildsThem},
		{"nil (hand-built Env; requireTaskStatusRegistry's guard)", func(*testing.T) (TaskStatusRegistry, DependencyKindRegistry, ArtifactKindRegistry) {
			return nil, nil, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, dk, ak := tc.build(t)
			env := &Env{
				Config: SpecGraphConfig{
					Mode:       ModeExperimentalBootstrap,
					Substrates: []SubstrateDescriptor{nativeSubstrateDescriptor()},
				},
				Substrates:      emptyRouter{},
				TaskStatuses:    ts,
				DependencyKinds: dk,
				ArtifactKinds:   ak,
			}

			out := healthOf(t, env)

			if out.OverallReady {
				t.Errorf("overall_ready = true on a fleet_plan_native server that can accept no write; registry_sizes = %v", out.RegistrySizes)
			}
			// Every unconfigured registry must be named, not just the first.
			for _, key := range []string{"task_statuses", "dependency_kinds", "artifact_kinds"} {
				assertSize(t, out, key, 0)
				if !blockingMentioning(out, key) {
					t.Errorf("blocking_issues does not name %q: %v", key, out.BlockingIssues)
				}
			}
		})
	}
}

// TestHealthStaysReadyWhenRegistriesEmptyOnSchemaV1 is the negative that keeps
// the rule CONDITIONAL. A blanket "empty registry means not ready" was already
// disproved one layer down at config validation: the three fields are
// `json:",omitempty"` and SpecGraphConfig.Validate imposes no non-empty rule, so
// a registry-less config is a legitimate shape and 12 TestConfigValidate
// subtests build one. The registries gate ONLY fleet_plan_native writes — every
// require*Registry call site is in fleet_plan_native.go — so a schema_v1
// deployment must stay ready without them.
//
// Without this test nothing stops a later "simplification" to an unconditional
// check, which would report every schema_v1 server not-ready.
func TestHealthStaysReadyWhenRegistriesEmptyOnSchemaV1(t *testing.T) {
	for _, storeType := range []string{"schema_v1", ""} {
		name := storeType
		if name == "" {
			name = "unset (defaults to schema_v1 per config.go Validate)"
		}
		t.Run(name, func(t *testing.T) {
			desc := schemaV1SubstrateDescriptor()
			desc.TaskStoreType = storeType
			env := &Env{
				Config: SpecGraphConfig{
					Mode:       ModeExperimentalBootstrap,
					Substrates: []SubstrateDescriptor{desc},
				},
				Substrates: emptyRouter{},
				// nil registries — a schema_v1 deployment never configures them.
			}

			out := healthOf(t, env)

			if !out.OverallReady {
				t.Errorf("overall_ready = false on a schema_v1 server, which never consults these registries; blocking_issues = %v", out.BlockingIssues)
			}
			if len(out.BlockingIssues) != 0 {
				t.Errorf("blocking_issues = %v, want none for schema_v1", out.BlockingIssues)
			}
			// Sizes are reported unconditionally even where they do not block.
			for _, key := range []string{"task_statuses", "dependency_kinds", "artifact_kinds"} {
				assertSize(t, out, key, 0)
			}
		})
	}
}

// TestHealthRegistryBlockerSurvivesSubstrateRouterError pins the ORDERING.
// handleHealth returns early when env.Substrates.All fails; if registry
// evaluation sat after the substrate probe it would be skipped on that path and
// a substrate outage would mask a registry blocker that refuses every write on
// its own. Both blockers must be reported.
//
// RED before the fix: blocking_issues carries only the router error.
func TestHealthRegistryBlockerSurvivesSubstrateRouterError(t *testing.T) {
	ts, dk, ak := emptyRegistriesAsCmdMCPBuildsThem(t)
	env := &Env{
		Config: SpecGraphConfig{
			Mode:       ModeExperimentalBootstrap,
			Substrates: []SubstrateDescriptor{nativeSubstrateDescriptor()},
		},
		Substrates:      failingRouter{err: errors.New("substrate router unreachable")},
		TaskStatuses:    ts,
		DependencyKinds: dk,
		ArtifactKinds:   ak,
	}

	out := healthOf(t, env)

	if out.OverallReady {
		t.Error("overall_ready = true despite both a router error and empty registries")
	}
	if !blockingMentioning(out, "substrate router unreachable") {
		t.Errorf("blocking_issues lost the substrate error: %v", out.BlockingIssues)
	}
	for _, key := range []string{"task_statuses", "dependency_kinds", "artifact_kinds"} {
		if !blockingMentioning(out, key) {
			t.Errorf("substrate failure masked the %q registry blocker: %v", key, out.BlockingIssues)
		}
	}
}

// TestHealthRegistrySizeAgreesWithBlockingVerdict pins WHERE the size comes
// from. The write path consults env.<Registry>, not Config, so an Env whose
// registry is nil refuses every write no matter how many members Config
// declares. Sourcing the reported size from Config would emit a
// self-contradicting response: task_statuses=2 next to a blocking issue saying
// the registry has no members. The number and the verdict must agree.
func TestHealthRegistrySizeAgreesWithBlockingVerdict(t *testing.T) {
	env := &Env{
		Config: SpecGraphConfig{
			Mode:       ModeExperimentalBootstrap,
			Substrates: []SubstrateDescriptor{nativeSubstrateDescriptor()},
			// Config declares members the Env registries do NOT carry.
			TaskStatuses: []TaskStatusDescriptor{
				NewTaskStatusDescriptor(map[string]interface{}{"name": "not_started"}),
				NewTaskStatusDescriptor(map[string]interface{}{"name": "done"}),
			},
			DependencyKinds: []DependencyKindDescriptor{
				NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep"}),
			},
			ArtifactKinds: []ArtifactKindDescriptor{
				NewArtifactKindDescriptor(map[string]interface{}{"name": "pr"}),
			},
		},
		Substrates: emptyRouter{},
		// Env registries nil: this Env refuses every write regardless of Config.
	}

	out := healthOf(t, env)

	for _, key := range []string{"task_statuses", "dependency_kinds", "artifact_kinds"} {
		assertSize(t, out, key, 0)
		if !blockingMentioning(out, key) {
			t.Errorf("blocking_issues does not name %q: %v", key, out.BlockingIssues)
		}
	}
	if out.OverallReady {
		t.Error("overall_ready = true on an Env whose registries refuse every write")
	}
}

// TestFleetPlanNativeConfiguredDiscriminates covers the predicate the
// conditionality rests on, including the mixed-substrate case: one
// fleet_plan_native substrate anywhere is enough to make the registries
// load-bearing.
func TestFleetPlanNativeConfiguredDiscriminates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		descs []SubstrateDescriptor
		want  bool
	}{
		{"no substrates", nil, false},
		{"schema_v1 only", []SubstrateDescriptor{schemaV1SubstrateDescriptor()}, false},
		{"fleet_plan_native only", []SubstrateDescriptor{nativeSubstrateDescriptor()}, true},
		{"mixed", []SubstrateDescriptor{schemaV1SubstrateDescriptor(), nativeSubstrateDescriptor()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{Config: SpecGraphConfig{Substrates: tc.descs}}
			if got := fleetPlanNativeConfigured(env); got != tc.want {
				t.Errorf("fleetPlanNativeConfigured = %v, want %v", got, tc.want)
			}
		})
	}
	if fleetPlanNativeConfigured(nil) {
		t.Error("fleetPlanNativeConfigured(nil) = true, want false")
	}
}

// TestCommittedConfigsCarryTheClosedRegistries guards the committed configs
// against the outage shape directly: both declare a fleet_plan_native
// substrate, so both must carry non-empty closed registries or the server
// they configure can accept no write.
//
// task-graph.cloud-run.config.json was removed from this list on 2026-09-06
// with the Cloud Run deployment it configured.
func TestCommittedConfigsCarryTheClosedRegistries(t *testing.T) {
	for _, path := range []string{
		"task-graph.config.json",
		"task-graph.post-cutover.config.json",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %q: %v", path, err)
			}
			var cfg SpecGraphConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				t.Fatalf("parse %q: %v", path, err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("validate %q: %v", path, err)
			}
			env := &Env{Config: cfg, Substrates: emptyRouter{}}
			// Asserted, never skipped: if a committed config stopped declaring a
			// fleet_plan_native substrate the readiness condition below would go
			// vacuous, and a vacuous guard is worse than none.
			if !fleetPlanNativeConfigured(env) {
				t.Fatalf("%q no longer declares a fleet_plan_native substrate; this guard has gone vacuous", path)
			}
			ts, err := newTaskStatusRegistry(cfg.TaskStatuses)
			if err != nil {
				t.Fatalf("newTaskStatusRegistry: %v", err)
			}
			dk, err := newDependencyKindRegistry(cfg.DependencyKinds)
			if err != nil {
				t.Fatalf("newDependencyKindRegistry: %v", err)
			}
			ak, err := newArtifactKindRegistry(cfg.ArtifactKinds)
			if err != nil {
				t.Fatalf("newArtifactKindRegistry: %v", err)
			}
			env.TaskStatuses, env.DependencyKinds, env.ArtifactKinds = ts, dk, ak

			out := healthOf(t, env)
			if !out.OverallReady {
				t.Errorf("committed config %q produces a server that can accept no write: %v", path, out.BlockingIssues)
			}
		})
	}
}
