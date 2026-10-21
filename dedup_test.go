package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeAdapter implements just enough of SubstrateAdapter for runDedupGate's
// three lookups. Each lookup either returns the canned hit or simulates the
// "not found" branch. Other interface methods are unimplemented — tests that
// exercise dedup never reach them.
type fakeAdapter struct {
	byHash    map[string]Node // organization_id|definition_hash → existing node
	byName    map[string]Node // organization_id|node_kind|canonical_name → existing node
	similar   []SimilarNode   // any FindSimilarByEmbedding call returns this set
	similarOK bool            // when false, simulate "no embedder hits"
	lastQuery []float32       // for assertions
}

func (f *fakeAdapter) ID() string                                     { return "fake" }
func (f *fakeAdapter) Description() string                            { return "fake" }
func (f *fakeAdapter) Database() string                               { return "fake" }
func (f *fakeAdapter) Close() error                                   { return nil }
func (f *fakeAdapter) CheckSchema(context.Context) error              { return nil }
func (f *fakeAdapter) VerifyExperimentalSchema(context.Context) error { return nil }
func (f *fakeAdapter) CommitAdmission(context.Context, ...AdmissionPlan) ([]AdmissionRecord, error) {
	panic("CommitAdmission unused in dedup tests")
}
func (f *fakeAdapter) ReadNode(context.Context, string, string, string) (Node, error) {
	return Node{}, errors.New("ReadNode unused")
}
func (f *fakeAdapter) ReadNodeInProject(context.Context, string, string, string, string) (Node, error) {
	return Node{}, errors.New("ReadNodeInProject unused")
}
func (f *fakeAdapter) ReadEdge(context.Context, string, string) (Edge, error) {
	return Edge{}, errors.New("ReadEdge unused")
}
func (f *fakeAdapter) ListNodes(context.Context, ListFilter) ([]Node, error) {
	return nil, nil
}
func (f *fakeAdapter) Neighbors(context.Context, string, NeighborOpts) ([]Edge, error) {
	return nil, nil
}
func (f *fakeAdapter) History(context.Context, string) ([]NodeVersion, error) { return nil, nil }
func (f *fakeAdapter) ExecuteReadOnlyQuery(context.Context, string, map[string]interface{}) (QueryResult, error) {
	return QueryResult{}, nil
}
func (f *fakeAdapter) SearchByEmbedding(context.Context, []float32, string, int, string) ([]Node, error) {
	return nil, nil
}
func (f *fakeAdapter) TraverseClosure(context.Context, string, string, []string, int, int, string) ([]Node, error) {
	return nil, nil
}

func (f *fakeAdapter) FindByDefinitionHash(_ context.Context, orgID, hash string) (Node, bool, error) {
	n, ok := f.byHash[orgID+"|"+hash]
	return n, ok, nil
}
func (f *fakeAdapter) FindByCanonicalName(_ context.Context, orgID, kind, name string) (Node, bool, error) {
	n, ok := f.byName[orgID+"|"+kind+"|"+name]
	return n, ok, nil
}
func (f *fakeAdapter) FindSimilarByEmbedding(_ context.Context, _ string, q []float32, _ float64, _ int) ([]SimilarNode, error) {
	f.lastQuery = q
	if !f.similarOK {
		return nil, nil
	}
	return f.similar, nil
}

// fakeRouter wraps a single fakeAdapter.
type fakeRouter struct{ a *fakeAdapter }

func (r *fakeRouter) Adapter(context.Context, NodeKindDescriptor) (SubstrateAdapter, error) {
	return r.a, nil
}
func (r *fakeRouter) All(context.Context) ([]SubstrateAdapter, error) {
	return []SubstrateAdapter{r.a}, nil
}
func (r *fakeRouter) ByID(string) (SubstrateAdapter, error) { return r.a, nil }
func (r *fakeRouter) Close() error                          { return nil }

// fakeKinds resolves any name to a synthetic descriptor with the matching
// artifact_type — enough for resolveAdapterForSubmission to succeed.
type fakeKinds struct{}

func (fakeKinds) Resolve(_ context.Context, name string) (NodeKindDescriptor, error) {
	return NewNodeKindDescriptor(map[string]interface{}{
		"name":          name,
		"artifact_type": name,
		"substrate_id":  "fake",
	}), nil
}
func (fakeKinds) All(context.Context) ([]NodeKindDescriptor, error) { return nil, nil }

// newDedupEnv constructs a minimal Env wired with the fake substrate router
// and the supplied DedupPolicyConfig. Embed remains nil unless overridden by
// the test; canonicalEmbedText / embed paths get exercised only when explicitly
// installed.
func newDedupEnv(t *testing.T, fa *fakeAdapter, policy *DedupPolicyConfig) *Env {
	t.Helper()
	return &Env{
		Config: SpecGraphConfig{
			Mode:                     ModeExperimentalBootstrap,
			ExperimentalAdmitter:     "TEST",
			DefaultOrganizationID:    "org-test",
			ExperimentalQueryEnabled: true,
			Dedup:                    policy,
		},
		Kinds:      fakeKinds{},
		Substrates: &fakeRouter{a: fa},
		Now:        func() time.Time { return time.Unix(0, 0).UTC() },
	}
}

func proposedSection(canonicalName string) ArtifactSubmission {
	return ArtifactSubmission{
		ArtifactType:   "section",
		NodeKind:       "section",
		OrganizationID: "org-test",
		Contract:       map[string]interface{}{"canonical_name": canonicalName},
		Payload:        map[string]interface{}{"title": "p"},
	}
}

func TestDedupGate_LayerCarveouts(t *testing.T) {
	t.Run("policy_nil_means_skipped", func(t *testing.T) {
		fa := &fakeAdapter{}
		env := newDedupEnv(t, fa, nil) // no policy
		got, err := runDedupGate(context.Background(), env, proposedSection("x"), nil)
		if err != nil || got != nil {
			t.Errorf("expected proceed (nil, nil); got (%v, %v)", got, err)
		}
	})

	t.Run("re_admission_skips_gate", func(t *testing.T) {
		fa := &fakeAdapter{}
		policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 5}
		env := newDedupEnv(t, fa, policy)
		sub := proposedSection("any")
		sub.PriorVersion = &NodeVersion{NodeID: "p", Version: 1}
		got, err := runDedupGate(context.Background(), env, sub, nil)
		if err != nil || got != nil {
			t.Errorf("expected proceed for re-admission; got (%v, %v)", got, err)
		}
	})

	t.Run("edge_admission_skips_gate", func(t *testing.T) {
		fa := &fakeAdapter{}
		policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 5}
		env := newDedupEnv(t, fa, policy)
		sub := ArtifactSubmission{ArtifactType: "edge", OrganizationID: "org-test", EdgeLabel: "depends_on"}
		got, err := runDedupGate(context.Background(), env, sub, nil)
		if err != nil || got != nil {
			t.Errorf("expected proceed for edge; got (%v, %v)", got, err)
		}
	})

	t.Run("skip_kinds_carveout", func(t *testing.T) {
		fa := &fakeAdapter{}
		policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 5, SkipKinds: []string{"snapshot"}}
		env := newDedupEnv(t, fa, policy)
		sub := ArtifactSubmission{ArtifactType: "snapshot", NodeKind: "snapshot", OrganizationID: "org-test"}
		got, err := runDedupGate(context.Background(), env, sub, nil)
		if err != nil || got != nil {
			t.Errorf("expected proceed for skipped kind; got (%v, %v)", got, err)
		}
	})
}

func TestDedupGate_Layer1_IdempotentReturn(t *testing.T) {
	sub := proposedSection("foo")
	hash, err := definitionHashOf(sub)
	if err != nil {
		t.Fatalf("hash compute: %v", err)
	}
	existing := Node{
		NodeID:         "existing-1",
		NodeKind:       "section",
		OrganizationID: "org-test",
		CurrentVersion: NodeVersion{
			NodeID: "existing-1", Version: 3, DefinitionHash: hash,
			ManifestHash: "m", SignatureChain: []string{"s"},
			AdmittedAt: time.Unix(123, 0).UTC(), AdmittedBy: "TEST",
		},
	}
	fa := &fakeAdapter{byHash: map[string]Node{"org-test|" + hash: existing}}
	policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 5}
	env := newDedupEnv(t, fa, policy)

	got, err := runDedupGate(context.Background(), env, sub, nil)
	if err != nil {
		t.Fatalf("expected nil err on idempotent return; got %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil AdmissionRecord; got nil")
	}
	if got.ArtifactID != "existing-1" || got.ArtifactVersion != 3 || got.DefinitionHash != hash {
		t.Errorf("idempotent record wrong: %+v", *got)
	}
}

func TestDedupGate_Layer2_CanonicalNameCollision(t *testing.T) {
	sub := proposedSection("colliding_name")
	existing := Node{
		NodeID:         "existing-2",
		NodeKind:       "section",
		OrganizationID: "org-test",
		CurrentVersion: NodeVersion{NodeID: "existing-2", Version: 1},
	}
	fa := &fakeAdapter{byName: map[string]Node{"org-test|section|colliding_name": existing}}
	policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 5}
	env := newDedupEnv(t, fa, policy)

	got, err := runDedupGate(context.Background(), env, sub, nil)
	if got != nil {
		t.Errorf("expected nil idempotent; got %+v", got)
	}
	if !errors.Is(err, ErrDuplicateCanonicalName) {
		t.Fatalf("expected ErrDuplicateCanonicalName; got %v", err)
	}
	var detail DuplicateCanonicalNameDetail
	if !errors.As(err, &detail) {
		t.Fatalf("expected DuplicateCanonicalNameDetail; got %T", err)
	}
	if detail.ExistingNodeID != "existing-2" || detail.CanonicalName != "colliding_name" {
		t.Errorf("detail wrong: %+v", detail)
	}
}

func TestDedupGate_Layer2_NoCanonicalNameFieldSkipsLayer2(t *testing.T) {
	// A node without canonical_name in its contract simply skips Layer 2.
	sub := ArtifactSubmission{
		ArtifactType:   "section",
		NodeKind:       "section",
		OrganizationID: "org-test",
		Contract:       map[string]interface{}{"shape": "x"}, // no canonical_name
		Payload:        map[string]interface{}{"title": "p"},
	}
	fa := &fakeAdapter{} // both maps empty
	policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 5}
	env := newDedupEnv(t, fa, policy)

	got, err := runDedupGate(context.Background(), env, sub, nil)
	if err != nil || got != nil {
		t.Errorf("expected proceed when canonical_name absent; got (%v, %v)", got, err)
	}
}

func TestDedupGate_Layer3_SkippedWithoutEmbedder(t *testing.T) {
	sub := proposedSection("new")
	fa := &fakeAdapter{
		similar:   []SimilarNode{{Node: Node{NodeID: "neighbor"}, Distance: 0.05}},
		similarOK: true,
	}
	policy := &DedupPolicyConfig{EmbeddingSimilarityThreshold: 0.1, CandidateLimit: 5}
	env := newDedupEnv(t, fa, policy) // env.Embed remains nil

	got, err := runDedupGate(context.Background(), env, sub, nil)
	if err != nil || got != nil {
		t.Errorf("expected proceed when embedder absent; got (%v, %v)", got, err)
	}
	if fa.lastQuery != nil {
		t.Errorf("Layer 3 ran without embedder; saw lastQuery=%v", fa.lastQuery)
	}
}

func TestDedupGate_FilterUnacknowledged(t *testing.T) {
	hits := []SimilarNode{
		{Node: Node{NodeID: "a"}, Distance: 0.01},
		{Node: Node{NodeID: "b"}, Distance: 0.02},
		{Node: Node{NodeID: "c"}, Distance: 0.03},
	}

	t.Run("none_acknowledged_returns_all", func(t *testing.T) {
		got := filterUnacknowledged(hits, nil)
		if len(got) != 3 {
			t.Errorf("expected 3; got %d", len(got))
		}
	})

	t.Run("some_acknowledged_returns_remainder", func(t *testing.T) {
		got := filterUnacknowledged(hits, []string{"a", "c"})
		if len(got) != 1 || got[0].Node.NodeID != "b" {
			t.Errorf("expected [b]; got %v", got)
		}
	})

	t.Run("all_acknowledged_returns_empty", func(t *testing.T) {
		got := filterUnacknowledged(hits, []string{"a", "b", "c"})
		if len(got) != 0 {
			t.Errorf("expected empty; got %v", got)
		}
	})

	t.Run("unknown_acknowledgment_no_effect", func(t *testing.T) {
		got := filterUnacknowledged(hits, []string{"z"})
		if len(got) != 3 {
			t.Errorf("expected 3; got %d", len(got))
		}
	})
}

func TestPossibleDuplicateDetail_ErrorMessage(t *testing.T) {
	d := PossibleDuplicateDetail{
		Threshold: 0.08,
		Candidates: []PossibleDuplicateCandidate{
			{NodeID: "a", NodeKind: "section", CanonicalName: "x", CosineDistance: 0.05},
			{NodeID: "b", NodeKind: "section", CanonicalName: "y", CosineDistance: 0.07},
		},
	}
	msg := d.Error()
	for _, want := range []string{"2 candidate", "0.0800", "[a b]"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q: %s", want, msg)
		}
	}
	if !errors.Is(d, ErrPossibleDuplicate) {
		t.Errorf("PossibleDuplicateDetail should wrap ErrPossibleDuplicate")
	}
}

func TestExtractCanonicalName(t *testing.T) {
	cases := []struct {
		name   string
		in     map[string]interface{}
		want   string
		wantOK bool
	}{
		{"present", map[string]interface{}{"canonical_name": "foo"}, "foo", true},
		{"missing", map[string]interface{}{"other": "x"}, "", false},
		{"empty_string", map[string]interface{}{"canonical_name": ""}, "", false},
		{"non_string", map[string]interface{}{"canonical_name": 42}, "", false},
		{"nil_contract", nil, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractCanonicalName(tc.in)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("got (%q, %v); want (%q, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
