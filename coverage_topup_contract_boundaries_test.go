package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type topupCommitAdapter struct {
	coverageAdapterFake
	plans   []AdmissionPlan
	records []AdmissionRecord
	err     error
}

func (a *topupCommitAdapter) CommitAdmission(_ context.Context, plans ...AdmissionPlan) ([]AdmissionRecord, error) {
	a.plans = append([]AdmissionPlan(nil), plans...)
	return a.records, a.err
}

type topupBrokenTaskStatuses struct{ err error }

func (r topupBrokenTaskStatuses) Resolve(context.Context, string) (TaskStatusDescriptor, error) {
	return TaskStatusDescriptor{}, r.err
}
func (topupBrokenTaskStatuses) Canonicalize(context.Context, string) (string, bool) {
	return "", false
}
func (r topupBrokenTaskStatuses) All(context.Context) ([]TaskStatusDescriptor, error) {
	return nil, r.err
}

type topupBrokenDependencyKinds struct{ err error }

func (r topupBrokenDependencyKinds) Resolve(context.Context, string) (DependencyKindDescriptor, error) {
	return DependencyKindDescriptor{}, r.err
}
func (topupBrokenDependencyKinds) Canonicalize(context.Context, string) (string, bool) {
	return "", false
}
func (r topupBrokenDependencyKinds) All(context.Context) ([]DependencyKindDescriptor, error) {
	return nil, r.err
}

type topupBrokenArtifactKinds struct{ err error }

func (r topupBrokenArtifactKinds) Resolve(context.Context, string) (ArtifactKindDescriptor, error) {
	return ArtifactKindDescriptor{}, r.err
}
func (topupBrokenArtifactKinds) Canonicalize(context.Context, string) (string, bool) {
	return "", false
}
func (r topupBrokenArtifactKinds) All(context.Context) ([]ArtifactKindDescriptor, error) {
	return nil, r.err
}

func requireTopupError(t *testing.T, err error, text string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), text) {
		t.Fatalf("error = %v, want text %q", err, text)
	}
}

func TestCoverageTopupDescriptorSerializationAndTypeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		marshal   func() ([]byte, error)
		rawLength func() int
		unmarshal func([]byte) error
	}{
		{
			name:      "task status",
			marshal:   func() ([]byte, error) { return json.Marshal(TaskStatusDescriptor{}) },
			rawLength: func() int { return len((TaskStatusDescriptor{}).Raw()) },
			unmarshal: func(b []byte) error { var d TaskStatusDescriptor; return json.Unmarshal(b, &d) },
		},
		{
			name:      "dependency kind",
			marshal:   func() ([]byte, error) { return json.Marshal(DependencyKindDescriptor{}) },
			rawLength: func() int { return len((DependencyKindDescriptor{}).Raw()) },
			unmarshal: func(b []byte) error { var d DependencyKindDescriptor; return json.Unmarshal(b, &d) },
		},
		{
			name:      "artifact kind",
			marshal:   func() ([]byte, error) { return json.Marshal(ArtifactKindDescriptor{}) },
			rawLength: func() int { return len((ArtifactKindDescriptor{}).Raw()) },
			unmarshal: func(b []byte) error { var d ArtifactKindDescriptor; return json.Unmarshal(b, &d) },
		},
		{
			name:      "node kind",
			marshal:   func() ([]byte, error) { return json.Marshal(NodeKindDescriptor{}) },
			rawLength: func() int { return len((NodeKindDescriptor{}).Raw()) },
			unmarshal: func(b []byte) error { var d NodeKindDescriptor; return json.Unmarshal(b, &d) },
		},
		{
			name:      "edge label",
			marshal:   func() ([]byte, error) { return json.Marshal(EdgeLabelDescriptor{}) },
			rawLength: func() int { return len((EdgeLabelDescriptor{}).Raw()) },
			unmarshal: func(b []byte) error { var d EdgeLabelDescriptor; return json.Unmarshal(b, &d) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := tc.marshal()
			if err != nil || string(encoded) != "{}" {
				t.Fatalf("zero descriptor marshal = %q, %v", encoded, err)
			}
			if tc.rawLength() != 0 {
				t.Fatal("zero descriptor exposed non-empty raw map")
			}
			if err := tc.unmarshal([]byte("{")); err == nil {
				t.Fatal("malformed descriptor JSON succeeded")
			}
		})
	}

	status := TaskStatusDescriptor{}
	if status.Name() != "" || status.Terminal() || status.LegalNext() != nil || status.Absorbs() != nil {
		t.Fatalf("zero task status accessors = %#v", status)
	}
	dependency := DependencyKindDescriptor{}
	if dependency.Name() != "" || dependency.EdgeFamily() != "" || dependency.Blocking() || dependency.LifecycleMarker() || dependency.LegalNext() != nil || dependency.Absorbs() != nil {
		t.Fatalf("zero dependency accessors = %#v", dependency)
	}
	artifact := ArtifactKindDescriptor{}
	if artifact.Name() != "" || artifact.Absorbs() != nil {
		t.Fatalf("zero artifact accessors = %#v", artifact)
	}
	node := NodeKindDescriptor{}
	if node.Name() != "" || node.SubstrateID() != "" || node.ArtifactType() != "" || node.VersionScope() != "" || node.Projections() != nil || node.PreSignPasses() != nil || node.PostSignPasses() != nil || node.DualControlRequired() {
		t.Fatalf("zero node accessors = %#v", node)
	}
	edge := EdgeLabelDescriptor{}
	if edge.Name() != "" || edge.SourceKinds() != nil || edge.TargetKinds() != nil || edge.ReciprocalLabel() != "" || edge.EdgeFamily() != "" || edge.DiagnosticOnly() || edge.VersionPinned() || edge.ForbidSelfLoops() {
		t.Fatalf("zero edge accessors = %#v", edge)
	}

	mixed := map[string]interface{}{"s": 7, "b": "no", "a": []interface{}{"keep", 2.0}}
	if descOptString(mixed, "s") != "" || descOptBool(mixed, "b") || !reflect.DeepEqual(descOptStrings(mixed, "a"), []string{"keep"}) || descOptStrings(mixed, "missing") != nil {
		t.Fatalf("shared descriptor coercion accepted wrong types")
	}
	if descOptString(nil, "x") != "" || descOptBool(nil, "x") || descOptStrings(nil, "x") != nil || descOptStrings(map[string]interface{}{"x": "bad"}, "x") != nil {
		t.Fatalf("nil or wrong-typed shared descriptor values did not fail closed")
	}
	if got := NewTaskStatusDescriptor(nil).Raw(); len(got) != 0 {
		t.Fatalf("nil task descriptor = %#v", got)
	}
	if got := NewDependencyKindDescriptor(nil).Raw(); len(got) != 0 {
		t.Fatalf("nil dependency descriptor = %#v", got)
	}
	if got := NewArtifactKindDescriptor(nil).Raw(); len(got) != 0 {
		t.Fatalf("nil artifact descriptor = %#v", got)
	}
	if got := NewNodeKindDescriptor(nil).Raw(); len(got) != 0 {
		t.Fatalf("nil node descriptor = %#v", got)
	}
	if got := NewEdgeLabelDescriptor(nil).Raw(); len(got) != 0 {
		t.Fatalf("nil edge descriptor = %#v", got)
	}
}

func TestCoverageTopupDescriptorValidationAndRegistryFailures(t *testing.T) {
	ctx := context.Background()
	for name, decode := range map[string]func([]byte) error{
		"task status":     func(b []byte) error { var d TaskStatusDescriptor; return d.UnmarshalJSON(b) },
		"dependency kind": func(b []byte) error { var d DependencyKindDescriptor; return d.UnmarshalJSON(b) },
		"artifact kind":   func(b []byte) error { var d ArtifactKindDescriptor; return d.UnmarshalJSON(b) },
		"node kind":       func(b []byte) error { var d NodeKindDescriptor; return d.UnmarshalJSON(b) },
		"edge label":      func(b []byte) error { var d EdgeLabelDescriptor; return d.UnmarshalJSON(b) },
	} {
		if err := decode([]byte("{")); err == nil {
			t.Fatalf("direct malformed %s decode succeeded", name)
		}
	}
	for _, raw := range []map[string]interface{}{
		{},
		{"name": "task"},
		{"name": "task", "substrate_id": "s"},
		{"name": "task", "substrate_id": "s", "artifact_type": "task"},
	} {
		if err := NewNodeKindDescriptor(raw).Validate(3); err == nil {
			t.Fatalf("invalid node descriptor succeeded: %#v", raw)
		}
	}
	for _, raw := range []map[string]interface{}{
		{},
		{"name": "gates"},
		{"name": "gates", "edge_family": "dep", "source_kinds": "task"},
		{"name": "gates", "edge_family": "dep", "target_kinds": "task"},
	} {
		if err := NewEdgeLabelDescriptor(raw).Validate(4); err == nil {
			t.Fatalf("invalid edge descriptor succeeded: %#v", raw)
		}
	}

	if _, err := newDependencyKindRegistry([]DependencyKindDescriptor{
		NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep"}),
		NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep"}),
	}); err == nil {
		t.Fatal("duplicate dependency member succeeded")
	}
	if _, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "commit"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "commit"}),
	}); err == nil {
		t.Fatal("duplicate artifact member succeeded")
	}
	if _, err := newTaskStatusRegistry([]TaskStatusDescriptor{
		NewTaskStatusDescriptor(map[string]interface{}{"name": "queued", "absorbs": []interface{}{"active"}}),
		NewTaskStatusDescriptor(map[string]interface{}{"name": "active"}),
	}); err == nil {
		t.Fatal("task status name colliding with an absorbed literal succeeded")
	}
	if _, err := newDependencyKindRegistry([]DependencyKindDescriptor{
		NewDependencyKindDescriptor(map[string]interface{}{"name": "gates", "edge_family": "dep", "absorbs": []interface{}{"source_map"}}),
		NewDependencyKindDescriptor(map[string]interface{}{"name": "source_map", "edge_family": "ref"}),
	}); err == nil {
		t.Fatal("dependency name colliding with an absorbed literal succeeded")
	}
	if _, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "commit", "absorbs": []interface{}{"pull_request"}}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "pull_request"}),
	}); err == nil {
		t.Fatal("artifact name colliding with an absorbed literal succeeded")
	}
	depReg, err := newDependencyKindRegistry([]DependencyKindDescriptor{
		NewDependencyKindDescriptor(map[string]interface{}{"name": "z", "edge_family": "dep"}),
		NewDependencyKindDescriptor(map[string]interface{}{"name": "a", "edge_family": "ref"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	deps, err := depReg.All(ctx)
	if err != nil || len(deps) != 2 || deps[0].Name() != "a" {
		t.Fatalf("sorted dependency registry = %#v, %v", deps, err)
	}
	if _, err := depReg.Resolve(ctx, "missing"); !errors.Is(err, ErrUnknownDependencyKind) {
		t.Fatalf("missing dependency = %v", err)
	}
	artifactReg, err := newArtifactKindRegistry([]ArtifactKindDescriptor{
		NewArtifactKindDescriptor(map[string]interface{}{"name": "z"}),
		NewArtifactKindDescriptor(map[string]interface{}{"name": "a"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifactReg.All(ctx)
	if err != nil || len(artifacts) != 2 || artifacts[0].Name() != "a" {
		t.Fatalf("sorted artifact registry = %#v, %v", artifacts, err)
	}
	if _, err := artifactReg.Resolve(ctx, "missing"); !errors.Is(err, ErrUnknownArtifactKind) {
		t.Fatalf("missing artifact = %v", err)
	}

	for _, err := range []error{
		requireTaskStatusRegistry(nil), requireTaskStatusRegistry(&Env{}),
		requireDependencyKindRegistry(nil), requireDependencyKindRegistry(&Env{}),
		requireArtifactKindRegistry(nil), requireArtifactKindRegistry(&Env{}),
	} {
		if err == nil {
			t.Fatal("missing registry succeeded")
		}
	}

	boom := errors.New("registry read failed")
	native := &Env{
		Config:       SpecGraphConfig{Substrates: []SubstrateDescriptor{{TaskStoreType: fleetPlanNativeStoreType}}},
		TaskStatuses: topupBrokenTaskStatuses{err: boom}, DependencyKinds: topupBrokenDependencyKinds{err: boom}, ArtifactKinds: topupBrokenArtifactKinds{err: boom},
	}
	sizes, blocking := registryHealth(ctx, native)
	if len(sizes) != 3 || len(blocking) != 3 {
		t.Fatalf("broken registry health = %#v, %#v", sizes, blocking)
	}
	emptyStatuses, _ := newTaskStatusRegistry(nil)
	emptyDeps, _ := newDependencyKindRegistry(nil)
	emptyArtifacts, _ := newArtifactKindRegistry(nil)
	native.TaskStatuses, native.DependencyKinds, native.ArtifactKinds = emptyStatuses, emptyDeps, emptyArtifacts
	_, blocking = registryHealth(ctx, native)
	if len(blocking) != 3 {
		t.Fatalf("empty native registries blocking = %#v", blocking)
	}
	if sizes, blocking := registryHealth(ctx, nil); len(sizes) != 3 || blocking != nil {
		t.Fatalf("nil non-native health = %#v, %#v", sizes, blocking)
	}
}

func TestCoverageTopupIdentityRejectionAndEncodingBoundaries(t *testing.T) {
	if _, err := NormalizeSectionPath("not-rooted"); err == nil {
		t.Fatal("invalid section path normalized")
	}
	for _, value := range []string{
		"", "0", "01", "12x",
	} {
		if _, err := parsePositiveInt(value); err == nil {
			t.Fatalf("invalid positive integer %q succeeded", value)
		}
	}
	if got, err := parsePositiveInt("42"); err != nil || got != 42 {
		t.Fatalf("positive integer = %d, %v", got, err)
	}

	uuid := "11111111-1111-1111-1111-111111111111"
	for _, value := range []string{
		"docobject:bad",
		"spec:bad@v1",
		"source:bad:name@v1",
		"source:" + uuid + ":bad%ZZ@v1",
		"source:" + uuid + ":name@bad%ZZ",
		"latest",
	} {
		if _, err := ParseVersionCoordinate(value); err == nil {
			t.Fatalf("invalid version coordinate %q succeeded", value)
		}
	}
	if got := (VersionCoordinate{Kind: "unknown"}).String(); got != "" {
		t.Fatalf("unknown version kind string = %q", got)
	}
	source := VersionCoordinate{Kind: VersionSourceNamed, SourceAuthorityID: uuid, StableName: "a b", SourceVersionToken: "v/1"}
	if got := source.String(); !strings.Contains(got, "a%20b@v%2F1") {
		t.Fatalf("source coordinate encoding = %q", got)
	}

	for _, value := range []string{
		"",
		"oodg://task/" + uuid + "/id/x?version=unversioned",
		"oodg://task/" + uuid + "/id/x#$.a",
		"oodg://Task/" + uuid + "/id/x?version=unversioned#$.a",
		"oodg://task/" + uuid + "/id/bad%ZZ?version=unversioned#$.a",
		"oodg://task/" + uuid + "/id/x?other=unversioned#$.a",
		"oodg://task/" + uuid + "/id/x?version=unversioned&other=x#$.a",
		"oodg://task/" + uuid + "/id/x?version=bad#$.a",
		"oodg://task/" + uuid + "/id/x?version=unversioned#bad%ZZ",
		"oodg://task/" + uuid + "/id/x?version=unversioned#not-rooted",
	} {
		if _, err := ParseGraphAddress(value); err == nil {
			t.Fatalf("invalid graph address %q succeeded", value)
		}
	}
	address := GraphAddress{ObjectClass: "task", SourceAuthorityID: uuid, SourceRefKind: "name", SourceRef: "a b", VersionCoordinate: "unversioned", SectionPath: "$.items[0]"}
	encoded := address.String()
	decoded, err := ParseGraphAddress(encoded)
	if err != nil || decoded.SourceRef != "a b" || decoded.SectionPath != "$.items[0]" {
		t.Fatalf("graph address round trip = %#v, %v (%q)", decoded, err, encoded)
	}
	if isFragmentSafe('%') || percentEncodeUpper("a b") != "a%20b" || percentEncodeFragmentUpper("$.a%") != "$.a%25" {
		t.Fatal("percent-encoding boundary changed")
	}
}

func TestCoverageTopupCeremonyFailureAndPlanBoundaries(t *testing.T) {
	ctx := context.Background()
	baseSub := ArtifactSubmission{ArtifactType: "task", OrganizationID: "org", NodeKind: "task", StableID: "task-1", Contract: map[string]interface{}{"canonical_name": "task-1"}, Payload: map[string]interface{}{"status": "active"}}
	if _, err := AdmitArtifact(ctx, nil, baseSub); err == nil {
		t.Fatal("nil ceremony env succeeded")
	}
	adapter := &topupCommitAdapter{}
	env := schemaCoverageEnv(adapter)
	if _, err := AdmitArtifact(ctx, env, ArtifactSubmission{}); err == nil {
		t.Fatal("shapeless ceremony submission succeeded")
	}
	noAdmitter := schemaCoverageEnv(adapter)
	noAdmitter.Config.ExperimentalAdmitter = ""
	if _, err := AdmitArtifact(ctx, noAdmitter, baseSub); err == nil {
		t.Fatal("ceremony without admitter succeeded")
	}
	badJSON := baseSub
	badJSON.Payload = map[string]interface{}{"bad": make(chan int)}
	if _, err := AdmitArtifact(ctx, env, badJSON); err == nil {
		t.Fatal("ceremony accepted non-JSON payload")
	}
	badKind := schemaCoverageEnv(adapter)
	badKind.Kinds = coverageKindsFake{err: errors.New("kind resolution failed")}
	if _, err := AdmitArtifact(ctx, badKind, baseSub); err == nil {
		t.Fatal("ceremony accepted unresolved node kind")
	}
	badRouter := schemaCoverageEnv(adapter)
	badRouter.Substrates = coverageRouter{err: errors.New("router failed")}
	if _, err := AdmitArtifact(ctx, badRouter, baseSub); err == nil {
		t.Fatal("ceremony accepted unresolved substrate")
	}
	if _, err := AdmitArtifact(ctx, schemaCoverageEnv(adapter), ArtifactSubmission{ArtifactType: "edge", OrganizationID: "org", EdgeLabel: "gates", StableID: "edge-1"}); err == nil {
		t.Fatal("edge ceremony with no adapter succeeded")
	}
	commitFailure := &topupCommitAdapter{err: errors.New("commit failed")}
	if _, err := AdmitArtifact(ctx, schemaCoverageEnv(commitFailure), baseSub); err == nil {
		t.Fatal("commit failure was ignored")
	}
	if _, err := AdmitArtifact(ctx, env, baseSub); err == nil {
		t.Fatal("empty commit result succeeded")
	}
	committed := AdmissionRecord{ArtifactID: "committed", ArtifactVersion: 1}
	success := &topupCommitAdapter{records: []AdmissionRecord{committed}}
	got, err := AdmitArtifact(ctx, schemaCoverageEnv(success), baseSub)
	if err != nil || got.ArtifactID != "committed" || len(success.plans) != 1 || success.plans[0].Node == nil {
		t.Fatalf("successful ceremony = %#v, plans=%#v, err=%v", got, success.plans, err)
	}
	priorSuccess := &topupCommitAdapter{records: []AdmissionRecord{{ArtifactID: "task-prior", ArtifactVersion: 8}}}
	priorSubmission := baseSub
	priorSubmission.PriorVersion = &NodeVersion{NodeID: "task-1", Version: 7}
	got, err = AdmitArtifact(ctx, schemaCoverageEnv(priorSuccess), priorSubmission)
	if err != nil || got.ArtifactID != "task-prior" || len(priorSuccess.plans) != 1 || priorSuccess.plans[0].Record.ArtifactVersion != 8 {
		t.Fatalf("prior-version ceremony = %#v, plans=%#v, err=%v", got, priorSuccess.plans, err)
	}
	edgeSuccess := &topupCommitAdapter{records: []AdmissionRecord{{ArtifactID: "edge-final", ArtifactVersion: 1}}}
	edgeSubmission := ArtifactSubmission{ArtifactType: "edge", OrganizationID: "org", EdgeLabel: "gates", StableID: "edge-1", EdgeSourceID: "a", EdgeTargetID: "b", IsRetirement: true}
	got, err = AdmitArtifact(ctx, schemaCoverageEnv(edgeSuccess), edgeSubmission)
	if err != nil || got.ArtifactID != "edge-final" || len(edgeSuccess.plans) != 1 || edgeSuccess.plans[0].Edge == nil || edgeSuccess.plans[0].Edge.Status != "retired" {
		t.Fatalf("retired edge ceremony = %#v, plans=%#v, err=%v", got, edgeSuccess.plans, err)
	}

	if _, err := PrepareAdmissionPlan(ctx, nil, baseSub); err == nil {
		t.Fatal("nil plan env succeeded")
	}
	if _, err := PrepareAdmissionPlan(ctx, env, ArtifactSubmission{}); err == nil {
		t.Fatal("shapeless plan succeeded")
	}
	if _, err := PrepareAdmissionPlan(ctx, noAdmitter, baseSub); err == nil {
		t.Fatal("plan without admitter succeeded")
	}
	if _, err := PrepareAdmissionPlan(ctx, env, badJSON); err == nil {
		t.Fatal("plan accepted non-JSON payload")
	}
	prior := &NodeVersion{NodeID: "task-1", Version: 7}
	retiredNode, err := PrepareAdmissionPlan(ctx, env, ArtifactSubmission{ArtifactType: "task", OrganizationID: "org", NodeKind: "task", PriorVersion: prior, IsRetirement: true})
	if err != nil || retiredNode.Node == nil || retiredNode.Node.Status != "retired" || retiredNode.Record.ArtifactVersion != 8 || retiredNode.Record.ArtifactID == "" {
		t.Fatalf("retired node plan = %#v, %v", retiredNode, err)
	}
	retiredEdge, err := PrepareAdmissionPlan(ctx, env, ArtifactSubmission{ArtifactType: "edge", OrganizationID: "org", EdgeLabel: "gates", StableID: "edge-1", EdgeSourceID: "a", EdgeTargetID: "b", IsRetirement: true})
	if err != nil || retiredEdge.Edge == nil || retiredEdge.Edge.Status != "retired" || !retiredEdge.IsEdge {
		t.Fatalf("retired edge plan = %#v, %v", retiredEdge, err)
	}
	realPrincipal := schemaCoverageEnv(adapter)
	realPrincipal.Config.ExperimentalAdmitter = ""
	nonGenesis, err := PrepareAdmissionPlan(ctx, realPrincipal, ArtifactSubmission{ArtifactType: "task", OrganizationID: "org", NodeKind: "task", Submitter: "principal"})
	if err != nil || !nonGenesis.Record.IsNonGenesis {
		t.Fatalf("principal plan genesis classification = %#v, %v", nonGenesis.Record, err)
	}
	realPrincipal.Config.GenesisAdmitter = "GENESIS"
	genesis, err := PrepareAdmissionPlan(ctx, realPrincipal, ArtifactSubmission{ArtifactType: "task", OrganizationID: "org", NodeKind: "task", Submitter: "GENESIS"})
	if err != nil || genesis.Record.IsNonGenesis {
		t.Fatalf("genesis plan classification = %#v, %v", genesis.Record, err)
	}

	production := &Env{Config: SpecGraphConfig{Mode: ModeProduction}}
	if err := stepSchemaValidate(ctx, production, baseSub); !errors.Is(err, ErrSchemaPending) {
		t.Fatalf("production schema step = %v", err)
	}
	if _, err := stepRunPasses(ctx, production, baseSub, "pre_sign"); !errors.Is(err, ErrSchemaPending) {
		t.Fatalf("production pass step = %v", err)
	}
	if _, err := stepSign(ctx, production, baseSub, &AdmissionRecord{}); !errors.Is(err, ErrSchemaPending) {
		t.Fatalf("production sign step = %v", err)
	}
	if _, err := canonicalJSON(make(chan int)); err == nil {
		t.Fatal("canonical JSON accepted channel")
	}
	if _, err := marshalCanonical(map[string]interface{}{"bad": make(chan int)}); err == nil {
		t.Fatal("canonical map accepted channel")
	}
	if _, err := marshalCanonical([]interface{}{make(chan int)}); err == nil {
		t.Fatal("canonical list accepted channel")
	}
}

func TestCoverageTopupSubstrateFailClosedBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
	empty := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ID: "empty"}}
	if _, _, _, err := empty.requireWriteTables(); err == nil {
		t.Fatal("missing write tables succeeded")
	}
	readOnly := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ID: "read-only", ReadOnly: true}}
	if _, _, _, err := readOnly.requireWriteTables(); err == nil {
		t.Fatal("read-only write tables succeeded")
	}
	if err := empty.CheckSchema(ctx); err == nil {
		t.Fatal("schema check without tables succeeded")
	}
	if err := empty.VerifyExperimentalSchema(ctx); err == nil {
		t.Fatal("experimental fingerprint without tables succeeded")
	}
	if _, err := empty.CommitAdmission(ctx); err == nil {
		t.Fatal("empty admission batch succeeded")
	}
	if _, err := readOnly.CommitAdmission(ctx, AdmissionPlan{}); err == nil {
		t.Fatal("read-only admission succeeded")
	}
	if _, err := empty.ReadNodeInProject(ctx, "", "node", "", ""); err == nil {
		t.Fatal("node read without table succeeded")
	}
	if _, err := empty.ReadEdge(ctx, "edge", ""); err == nil {
		t.Fatal("edge read without table succeeded")
	}
	if _, err := empty.ListNodes(ctx, ListFilter{}); err == nil {
		t.Fatal("node list without table succeeded")
	}
	if _, err := empty.Neighbors(ctx, "node", NeighborOpts{}); err == nil {
		t.Fatal("neighbors without table succeeded")
	}
	if _, err := empty.History(ctx, "node"); err == nil {
		t.Fatal("history without table succeeded")
	}
	if _, err := empty.SearchByEmbedding(ctx, nil, "", 1, "org"); err == nil {
		t.Fatal("embedding search without table succeeded")
	}
	if _, _, err := empty.FindByDefinitionHash(ctx, "org", "hash"); err == nil {
		t.Fatal("definition lookup without table succeeded")
	}
	if _, _, err := empty.FindByCanonicalName(ctx, "org", "task", "name"); err == nil {
		t.Fatal("canonical-name lookup without table succeeded")
	}
	if _, err := empty.FindSimilarByEmbedding(ctx, "org", nil, 0.1, 1); err == nil {
		t.Fatal("similarity lookup without table succeeded")
	}
	if _, err := empty.TraverseClosure(ctx, "", "node", nil, 1, 1, "forward"); err == nil {
		t.Fatal("closure without table succeeded")
	}

	withNodeTable := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ID: "schema", NodeTable: "Nodes"}}
	if _, _, err := withNodeTable.FindByDefinitionHash(ctx, "org", ""); err == nil {
		t.Fatal("empty definition hash succeeded")
	}
	if _, found, err := withNodeTable.FindByCanonicalName(ctx, "org", "task", ""); err != nil || found {
		t.Fatalf("empty canonical name = found %v, err %v", found, err)
	}
	if _, err := withNodeTable.FindSimilarByEmbedding(ctx, "org", nil, 0.1, 0); err == nil {
		t.Fatal("non-positive similarity limit succeeded")
	}
	if _, err := withNodeTable.getClient(ctx); err == nil {
		t.Fatal("ambient emulator was accepted without explicit permission")
	}
	if _, err := withNodeTable.ExecuteReadOnlyQuery(ctx, "SELECT 1", nil); err == nil {
		t.Fatal("query ignored client initialization failure")
	}
	if _, err := withNodeTable.ReadOnlySnapshot(ctx, func(SnapshotQuerier) error { return nil }); err == nil {
		t.Fatal("snapshot ignored client initialization failure")
	}
	native := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{ID: "native", TaskStoreType: fleetPlanNativeStoreType}}
	if _, err := native.ReadNodeInProject(ctx, "arbor", "task", "2", ""); err == nil {
		t.Fatal("native versioned read succeeded")
	}
	if _, err := native.ReadEdge(ctx, "edge", ""); err == nil {
		t.Fatal("native direct edge read succeeded")
	}
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
	if got := nullableString(""); reflect.TypeOf(got).String() != "spanner.NullString" {
		t.Fatalf("empty nullable string = %T", got)
	}
	if got := nullableInt64(0); reflect.TypeOf(got).String() != "spanner.NullInt64" {
		t.Fatalf("zero nullable int = %T", got)
	}
	if got := nullableString("x"); got != "x" {
		t.Fatalf("non-empty nullable string = %#v", got)
	}
	if got := nullableInt64(9); got != int64(9) {
		t.Fatalf("non-zero nullable int = %#v", got)
	}
}

func TestCoverageTopupConfiguredSubstratePropagatesClientInitializationFailure(t *testing.T) {
	ctx := context.Background()
	t.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
	configured := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{
		ID: "configured", NodeTable: "Nodes", EdgeTable: "Edges", AdmissionLogTable: "ArtifactAdmissionLog",
	}}

	checks := []struct {
		name string
		run  func() error
	}{
		{"schema", func() error { return configured.CheckSchema(ctx) }},
		{"fingerprint", func() error { return configured.VerifyExperimentalSchema(ctx) }},
		{"node read", func() error { _, err := configured.ReadNodeInProject(ctx, "org", "node", "", ""); return err }},
		{"edge read", func() error { _, err := configured.ReadEdge(ctx, "edge", ""); return err }},
		{"node list", func() error { _, err := configured.ListNodes(ctx, ListFilter{OrganizationID: "org"}); return err }},
		{"neighbors", func() error { _, err := configured.Neighbors(ctx, "node", NeighborOpts{}); return err }},
		{"history", func() error { _, err := configured.History(ctx, "node"); return err }},
		{"embedding search", func() error {
			_, err := configured.SearchByEmbedding(ctx, []float32{0.1}, "task", 1, "org")
			return err
		}},
		{"definition lookup", func() error { _, _, err := configured.FindByDefinitionHash(ctx, "org", "hash"); return err }},
		{"canonical lookup", func() error { _, _, err := configured.FindByCanonicalName(ctx, "org", "task", "name"); return err }},
		{"similarity lookup", func() error {
			_, err := configured.FindSimilarByEmbedding(ctx, "org", []float32{0.1}, 0.5, 1)
			return err
		}},
		{"closure", func() error {
			_, err := configured.TraverseClosure(ctx, "org", "node", []string{"gates"}, 1, 10, "forward")
			return err
		}},
		{"commit", func() error { _, err := configured.CommitAdmission(ctx, AdmissionPlan{}); return err }},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); err == nil || !strings.Contains(err.Error(), "SPANNER_EMULATOR_HOST") {
				t.Fatalf("configured substrate did not propagate explicit-emulator denial: %v", err)
			}
		})
	}
}
