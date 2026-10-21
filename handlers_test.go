package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

// TestTopologicalOrder verifies that spec_graph_import orders submissions so
// that any submission whose payload references another submission's stable_id
// appears AFTER the referent in the result.
func TestTopologicalOrder(t *testing.T) {
	subs := []ArtifactSubmission{
		{StableID: "c", Payload: map[string]interface{}{"links_to": map[string]interface{}{"$ref": "b"}}},
		{StableID: "b", Payload: map[string]interface{}{"links_to": map[string]interface{}{"$ref": "a"}}},
		{StableID: "a", Payload: map[string]interface{}{"value": 1}},
	}
	ordered, err := topologicallyOrder(subs)
	if err != nil {
		t.Fatalf("topologicallyOrder returned %v", err)
	}
	if len(ordered) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(ordered))
	}
	positions := map[string]int{}
	for i, s := range ordered {
		positions[s.StableID] = i
	}
	if positions["a"] > positions["b"] {
		t.Errorf("a (pos %d) must come before b (pos %d)", positions["a"], positions["b"])
	}
	if positions["b"] > positions["c"] {
		t.Errorf("b (pos %d) must come before c (pos %d)", positions["b"], positions["c"])
	}
}

func TestTopologicalOrderRejectsCycle(t *testing.T) {
	subs := []ArtifactSubmission{
		{StableID: "x", Payload: map[string]interface{}{"r": map[string]interface{}{"$ref": "y"}}},
		{StableID: "y", Payload: map[string]interface{}{"r": map[string]interface{}{"$ref": "x"}}},
	}
	_, err := topologicallyOrder(subs)
	if !errors.Is(err, ErrImportDependencyUnresolved) {
		t.Fatalf("expected ErrImportDependencyUnresolved, got %v", err)
	}
}

func TestTopologicalOrderRejectsDuplicateIDs(t *testing.T) {
	subs := []ArtifactSubmission{
		{StableID: "a"},
		{StableID: "a"},
	}
	_, err := topologicallyOrder(subs)
	if !errors.Is(err, ErrImportDependencyUnresolved) {
		t.Fatalf("expected ErrImportDependencyUnresolved on duplicate, got %v", err)
	}
}

// TestConfigValidate covers every cross-field rule.
func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     SpecGraphConfig
		wantErr bool
		errSub  string
	}{
		{name: "unset_mode_rejected",
			cfg:     SpecGraphConfig{},
			wantErr: true,
			errSub:  "mode is required"},

		{name: "invalid_mode_rejected",
			cfg:     SpecGraphConfig{Mode: "weird"},
			wantErr: true,
			errSub:  "invalid mode"},

		{name: "bootstrap_complete_ok",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "MY_ADMITTER_NAME",
				DefaultOrganizationID: "tenant-x",
			},
			wantErr: false},

		{name: "bootstrap_arbitrary_admitter_accepted",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "ANYTHING_THE_DEV_WANTS",
				DefaultOrganizationID: "tenant-x",
			},
			wantErr: false},

		{name: "bootstrap_requires_admitter",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				DefaultOrganizationID: "tenant-x",
			},
			wantErr: true,
			errSub:  "experimental_admitter required"},

		{name: "bootstrap_requires_org_id",
			cfg: SpecGraphConfig{
				Mode:                 ModeExperimentalBootstrap,
				ExperimentalAdmitter: "ANY",
			},
			wantErr: true,
			errSub:  "default_organization_id required"},

		{name: "production_clean_ok",
			cfg:     SpecGraphConfig{Mode: ModeProduction, GenesisAdmitter: "GENESIS"},
			wantErr: false},

		{name: "production_requires_genesis_admitter",
			cfg:     SpecGraphConfig{Mode: ModeProduction},
			wantErr: true,
			errSub:  "genesis_admitter required"},

		{name: "production_rejects_experimental_admitter",
			cfg:     SpecGraphConfig{Mode: ModeProduction, GenesisAdmitter: "GENESIS", ExperimentalAdmitter: "X"},
			wantErr: true,
			errSub:  "experimental_admitter must be empty"},

		{name: "production_rejects_default_org_id",
			cfg:     SpecGraphConfig{Mode: ModeProduction, GenesisAdmitter: "GENESIS", DefaultOrganizationID: "X"},
			wantErr: true,
			errSub:  "default_organization_id must be empty"},

		{name: "production_rejects_query_enabled",
			cfg:     SpecGraphConfig{Mode: ModeProduction, GenesisAdmitter: "GENESIS", ExperimentalQueryEnabled: true},
			wantErr: true,
			errSub:  "experimental_query_enabled must be false"},

		{name: "substrate_requires_id",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				Substrates:            []SubstrateDescriptor{{SpannerDatabase: "x"}},
			},
			wantErr: true,
			errSub:  "id required"},

		{name: "substrate_requires_spanner_database",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				Substrates:            []SubstrateDescriptor{{ID: "s1"}},
			},
			wantErr: true,
			errSub:  "spanner_database required"},

		{name: "legacy_substrate_credentials_rejected",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				Substrates: []SubstrateDescriptor{{
					ID: "s1", SpannerDatabase: "projects/p/instances/i/databases/d", CredentialsFile: "/tmp/stale.json", ReadOnly: false,
				}},
			},
			wantErr: true,
			errSub:  "legacy per-client credential selector"},

		{name: "writable_substrate_requires_tables",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				Substrates: []SubstrateDescriptor{{
					ID: "s1", SpannerDatabase: "projects/p/instances/i/databases/d", CredentialsFile: "/k.json", ReadOnly: false,
				}},
			},
			wantErr: true,
			errSub:  "node_table required"},

		{name: "read_only_substrate_no_tables_needed",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				Substrates: []SubstrateDescriptor{{
					ID: "s1", SpannerDatabase: "projects/p/instances/i/databases/d", CredentialsFile: "/k.json", ReadOnly: true,
				}},
			},
			wantErr: false},

		{name: "fleet_plan_native_requires_default_project",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				Substrates: []SubstrateDescriptor{{
					ID: "fleet", SpannerDatabase: "projects/p/instances/i/databases/task-plan", CredentialsFile: "/k.json",
					TaskStoreType: fleetPlanNativeStoreType, TaskTable: "Tasks", WorklinkTable: "TaskLinks", DepTable: "TaskDeps",
				}},
			},
			wantErr: true,
			errSub:  "default_project_id required"},

		{name: "fleet_plan_native_pre_cutover_config_ok",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				DefaultProjectID:      "arbor",
				Substrates: []SubstrateDescriptor{{
					ID: "fleet", SpannerDatabase: "projects/p/instances/i/databases/task-plan", CredentialsFile: "/k.json",
					TaskStoreType: fleetPlanNativeStoreType, TaskTable: "Tasks", WorklinkTable: "TaskLinks", DepTable: "TaskDeps",
				}},
			},
			wantErr: false},

		{name: "fleet_plan_native_post_cutover_config_ok",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				DefaultProjectID:      "arbor",
				Substrates: []SubstrateDescriptor{{
					ID: "fleet", SpannerDatabase: "projects/p/instances/i/databases/task-plan", CredentialsFile: "/k.json",
					TaskStoreType: fleetPlanNativeStoreType, ProjectColumn: "project_id", TaskTable: "Tasks", WorklinkTable: "TaskLinks", DepTable: "TaskDeps",
				}},
			},
			wantErr: false},

		{name: "listen_address_required",
			cfg: SpecGraphConfig{
				Mode:                  ModeExperimentalBootstrap,
				ExperimentalAdmitter:  "A",
				DefaultOrganizationID: "T",
				// ListenAddress deliberately empty
			},
			wantErr: true,
			errSub:  "listen_address is required"},
	}
	// Backfill ListenAddress on every case whose Mode is set, so the existing
	// cases keep testing the same downstream rules. The two mode-error cases
	// (unset_mode_rejected, invalid_mode_rejected) and the new
	// listen_address_required case deliberately omit ListenAddress and are
	// skipped by this fixup.
	for i := range cases {
		c := &cases[i]
		if c.cfg.Mode == ModeExperimentalBootstrap || c.cfg.Mode == ModeProduction {
			c.cfg.GoogleCredentials = gcpauth.Selector{
				CredentialsSource:   gcpauth.CredentialsSourceApplicationDefault,
				CredentialsProfile:  gcpauth.CredentialsProfileImpersonatedServiceAccount,
				CredentialsIdentity: "task-graph-operator@example-gcp-project.iam.gserviceaccount.com",
			}
		}
		if c.name != "legacy_substrate_credentials_rejected" {
			for j := range c.cfg.Substrates {
				c.cfg.Substrates[j].CredentialsFile = ""
				c.cfg.Substrates[j].CredentialsSource = ""
			}
		}
		switch c.name {
		case "unset_mode_rejected", "invalid_mode_rejected", "listen_address_required":
			// leave ListenAddress empty — these cases test failure modes that
			// fire before the listen_address check would.
		default:
			c.cfg.ListenAddress = "127.0.0.1:8765"
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.cfg.Validate()
			if c.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if err != nil && c.errSub != "" && !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error %q does not contain %q", err.Error(), c.errSub)
			}
		})
	}
}

func TestResolveNativeProjectIDPreCutoverFailClosed(t *testing.T) {
	env := &Env{Config: SpecGraphConfig{DefaultProjectID: "arbor"}}
	preCutover := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{
		ID: "fleet", TaskStoreType: fleetPlanNativeStoreType,
	}}
	postCutover := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{
		ID: "fleet", TaskStoreType: fleetPlanNativeStoreType, ProjectColumn: "project_id",
	}}

	got, err := resolveNativeProjectID(env, preCutover, "")
	if err != nil || got != "arbor" {
		t.Fatalf("pre-cutover omitted project = (%q, %v), want arbor nil", got, err)
	}
	got, err = resolveNativeProjectID(env, preCutover, "arbor")
	if err != nil || got != "arbor" {
		t.Fatalf("pre-cutover default project = (%q, %v), want arbor nil", got, err)
	}
	if _, err := resolveNativeProjectID(env, preCutover, "moonshot"); err == nil || !strings.Contains(err.Error(), "requires task-plan project namespace cutover") {
		t.Fatalf("pre-cutover non-default project should fail closed, got %v", err)
	}
	got, err = resolveNativeProjectID(env, postCutover, "moonshot")
	if err != nil || got != "moonshot" {
		t.Fatalf("post-cutover non-default project = (%q, %v), want moonshot nil", got, err)
	}
}

func TestIsReadOnlySQL(t *testing.T) {
	cases := map[string]bool{
		"SELECT * FROM Nodes":                  true,
		"  select 1":                           true,
		"WITH a AS (SELECT 1) SELECT * FROM a": true,
		"GRAPH CorpusGraph MATCH (n) RETURN n": true,
		"MATCH (n) RETURN n":                   true,
		"INSERT INTO Nodes VALUES (1)":         false,
		"UPDATE Nodes SET x = 1":               false,
		"DELETE FROM Nodes":                    false,
		"DROP TABLE Nodes":                     false,
	}
	for sql, want := range cases {
		t.Run(sql, func(t *testing.T) {
			got := isReadOnlySQL(sql)
			if got != want {
				t.Errorf("isReadOnlySQL(%q) = %v, want %v", sql, got, want)
			}
		})
	}
}

func TestMutationWriteResultDistinguishesNativeReceiptFromAdmissionRecord(t *testing.T) {
	at := time.Unix(123, 0).UTC()
	env := &Env{Config: SpecGraphConfig{ExperimentalAdmitter: "task-plan-native"}}
	receipt := nativeMutationReceipt(env, "arbor", "update_task", "task", "CR-1", "Tasks", 1, at)
	nativeResult := nativeMutationWriteResult(receipt)
	if nativeResult.AdmissionRecord != nil {
		t.Fatalf("native result carried admission_record: %+v", nativeResult.AdmissionRecord)
	}
	if nativeResult.NativeMutationReceipt == nil {
		t.Fatal("native result did not carry native_mutation_receipt")
	}
	if nativeResult.NativeMutationReceipt.ReceiptType != "native_mutation_receipt" {
		t.Errorf("ReceiptType = %q, want native_mutation_receipt", nativeResult.NativeMutationReceipt.ReceiptType)
	}

	nativeJSON, err := json.Marshal(nativeResult)
	if err != nil {
		t.Fatal(err)
	}
	nativeText := string(nativeJSON)
	mustContain(t, nativeText, "native_mutation_receipt")
	mustNotContain(t, nativeText, "admission_record")
	mustNotContain(t, nativeText, "definition_hash")
	mustNotContain(t, nativeText, "manifest_hash")
	mustNotContain(t, nativeText, "signature_chain")

	admissionResult := admissionWriteResult(AdmissionRecord{
		ArtifactType:   "task",
		OrganizationID: "test-org",
		ArtifactID:     "CR-1",
		DefinitionHash: "real-definition-hash",
		ManifestHash:   "real-manifest-hash",
		SignatureChain: []string{"real-signature"},
	})
	if admissionResult.AdmissionRecord == nil {
		t.Fatal("admission result did not carry admission_record")
	}
	if admissionResult.NativeMutationReceipt != nil {
		t.Fatalf("admission result carried native_mutation_receipt: %+v", admissionResult.NativeMutationReceipt)
	}
	admissionJSON, err := json.Marshal(admissionResult)
	if err != nil {
		t.Fatal(err)
	}
	admissionText := string(admissionJSON)
	mustContain(t, admissionText, "admission_record")
	mustContain(t, admissionText, "definition_hash")
	mustNotContain(t, admissionText, "native_mutation_receipt")
}

func TestValidateNativeWorklinkArchiveTarget(t *testing.T) {
	tests := []struct {
		name            string
		taskID          string
		linkID          string
		ownedTaskID     string
		artifactStatus  string
		taskExists      bool
		linkExists      bool
		wantIdempotent  bool
		wantErrContains string
	}{
		{name: "active_owned_link", taskID: "TASK-1", linkID: "LNK-1", ownedTaskID: "TASK-1", artifactStatus: "recorded", taskExists: true, linkExists: true},
		{name: "already_archived_is_idempotent", taskID: "TASK-1", linkID: "LNK-1", ownedTaskID: "TASK-1", artifactStatus: "archived", taskExists: true, linkExists: true, wantIdempotent: true},
		{name: "task_must_exist", taskID: "MISSING", linkID: "LNK-1", taskExists: false, linkExists: true, ownedTaskID: "MISSING", wantErrContains: `task "MISSING" not found`},
		{name: "link_must_exist", taskID: "TASK-1", linkID: "LNK-MISSING", taskExists: true, linkExists: false, wantErrContains: `worklink "LNK-MISSING" not found`},
		{name: "link_must_belong_to_task", taskID: "TASK-1", linkID: "LNK-1", ownedTaskID: "TASK-2", taskExists: true, linkExists: true, wantErrContains: `belongs to task "TASK-2"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateNativeWorklinkArchiveTarget(tc.taskID, tc.linkID, tc.ownedTaskID, tc.artifactStatus, tc.taskExists, tc.linkExists)
			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.wantIdempotent {
				t.Fatalf("idempotent = %v, want %v", got, tc.wantIdempotent)
			}
		})
	}
}

func TestNativeHandleArchiveWorklinkRejectsNonNativeStore(t *testing.T) {
	env := &Env{Substrates: newRouter(gcpauth.Selector{}, false, []SubstrateDescriptor{{
		ID: "schema-v1", SpannerDatabase: "projects/p/instances/i/databases/d",
	}})}
	defer env.Substrates.Close()
	_, err := nativeHandleArchiveWorklink(context.Background(), env, archiveWorklinkInput{TaskID: "TASK-1", LinkID: "LNK-1"})
	if err == nil || !strings.Contains(err.Error(), "requires fleet_plan_native store") {
		t.Fatalf("non-native direct call error = %v, want fleet_plan_native rejection", err)
	}
}

func TestNativeHandleSupersedeWorklinkRejectsNonNativeStore(t *testing.T) {
	kinds, err := newArtifactKindRegistry([]ArtifactKindDescriptor{NewArtifactKindDescriptor(map[string]interface{}{"name": "worktree"})})
	if err != nil {
		t.Fatal(err)
	}
	env := &Env{ArtifactKinds: kinds, Substrates: newRouter(gcpauth.Selector{}, false, []SubstrateDescriptor{{
		ID: "schema-v1", SpannerDatabase: "projects/p/instances/i/databases/d",
	}})}
	defer env.Substrates.Close()
	in := supersedeWorklinkInput{TaskID: "TASK-1", ExpectedOwner: "owner", ExpectedSessionID: "session", PredecessorLinkID: "BND-old",
		ExpectedPredecessor: supersedePredecessorIdentity{ArtifactType: "worktree", ArtifactStatus: "recorded", Repo: "r", Branch: "old", WorktreePath: "/wt/old", Thread: "t", SessionID: "session"},
		Successor:           supersedeSuccessor{LinkID: "BND-new", ArtifactType: "worktree", ArtifactRef: "ref", Repo: "r", Branch: "new", WorktreePath: "/wt/new", Thread: "t", SessionID: "session"}}
	_, err = nativeHandleSupersedeWorklink(context.Background(), env, in)
	if err == nil || !strings.Contains(err.Error(), "requires fleet_plan_native store") {
		t.Fatalf("non-native direct call error = %v, want fleet_plan_native rejection", err)
	}
}

// TestAllowlistCheck covers the config-driven init/reset gate. The allowlist
// patterns live in config; the binary holds no hardcoded patterns.
func TestAllowlistCheck(t *testing.T) {
	// Empty allowlist refuses everything.
	if err := allowlistCheck(SpecGraphConfig{}, "anything"); !errors.Is(err, ErrForbiddenDatabaseTarget) {
		t.Errorf("empty allowlist should refuse all, got %v", err)
	}

	// Matching pattern accepts.
	cfg := SpecGraphConfig{InitDatabaseAllowlist: []string{"corpus-graph-*"}}
	if err := allowlistCheck(cfg, "corpus-graph-iter1"); err != nil {
		t.Errorf("corpus-graph-iter1 should pass under corpus-graph-*, got %v", err)
	}

	// Non-matching pattern refuses. The production-tier database names
	// the user explicitly named as catastrophic-if-touched all refused under
	// the corpus-graph-* allowlist.
	for _, prodDB := range []string{"example-staging", "legacy-graph"} {
		if err := allowlistCheck(cfg, prodDB); !errors.Is(err, ErrForbiddenDatabaseTarget) {
			t.Errorf("%q should be refused under corpus-graph-*, got %v", prodDB, err)
		}
	}

	// User can pick any patterns they want — including allowing prod-* if
	// that's the developer's choice. The binary doesn't second-guess config.
	cfg2 := SpecGraphConfig{InitDatabaseAllowlist: []string{"test-*", "experimental-*"}}
	if err := allowlistCheck(cfg2, "test-foo"); err != nil {
		t.Errorf("test-foo should pass under test-*, got %v", err)
	}
	if err := allowlistCheck(cfg2, "corpus-graph-iter1"); !errors.Is(err, ErrForbiddenDatabaseTarget) {
		t.Errorf("corpus-graph-iter1 should be refused under test-*|experimental-*, got %v", err)
	}
}

// TestSchemaFingerprintLogic verifies the set-difference logic behind
// VerifyExperimentalSchema without touching Spanner — exercises the
// exclusivity rule for the catastrophic-prevention gate.
func TestSchemaFingerprintLogic(t *testing.T) {
	classify := func(expected, found []string) (missing, extra []string, identical bool) {
		exp := map[string]bool{}
		for _, e := range expected {
			exp[e] = true
		}
		fnd := map[string]bool{}
		for _, f := range found {
			fnd[f] = true
		}
		for e := range exp {
			if !fnd[e] {
				missing = append(missing, e)
			}
		}
		for f := range fnd {
			if !exp[f] {
				extra = append(extra, f)
			}
		}
		identical = len(missing) == 0 && len(extra) == 0
		return
	}

	expected := []string{"Nodes", "Edges", "ArtifactAdmissionLog"}

	cases := []struct {
		name            string
		found           []string
		wantIdentical   bool
		wantMissing     []string
		wantExtraExists bool
	}{
		{name: "exact_match_spec_graph_db",
			found: []string{"Nodes", "Edges", "ArtifactAdmissionLog"}, wantIdentical: true},

		{name: "empty_db_zero_tables",
			found: []string{}, wantIdentical: false, wantMissing: []string{"ArtifactAdmissionLog", "Edges", "Nodes"}},

		{name: "legacy_graphy_shape_extras_refuse",
			found:           []string{"ArtifactAdmissionLog", "ASG_Nodes", "DocObjects", "AgentRuns", "ArcLedger"},
			wantIdentical:   false,
			wantMissing:     []string{"Edges", "Nodes"},
			wantExtraExists: true},

		{name: "example_staging_shape_subset_plus_extras",
			found:           []string{"Nodes", "Circuits", "OODGraphObjects", "Knowledge"},
			wantIdentical:   false,
			wantMissing:     []string{"ArtifactAdmissionLog", "Edges"},
			wantExtraExists: true},

		{name: "extra_only_refuse",
			found:           []string{"Nodes", "Edges", "ArtifactAdmissionLog", "Memories"},
			wantIdentical:   false,
			wantExtraExists: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			missing, extra, identical := classify(expected, c.found)
			if identical != c.wantIdentical {
				t.Errorf("identical = %v, want %v (missing=%v extra=%v)", identical, c.wantIdentical, missing, extra)
			}
			if c.wantExtraExists && len(extra) == 0 {
				t.Errorf("expected extras but got none (missing=%v)", missing)
			}
			if c.wantMissing != nil {
				gotMissing := map[string]bool{}
				for _, m := range missing {
					gotMissing[m] = true
				}
				for _, m := range c.wantMissing {
					if !gotMissing[m] {
						t.Errorf("expected %q in missing set, got missing=%v", m, missing)
					}
				}
			}
		})
	}
}

func TestParseSpannerResource(t *testing.T) {
	p, i, d, err := parseSpannerResource("projects/example-gcp-project/instances/example-spanner-instance/databases/corpus-graph-iter1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p != "example-gcp-project" || i != "example-spanner-instance" || d != "corpus-graph-iter1" {
		t.Errorf("got (%q, %q, %q)", p, i, d)
	}
	if _, _, _, err := parseSpannerResource("not-a-resource"); err == nil {
		t.Error("expected error on malformed resource")
	}
}

func TestCanonicalJSON(t *testing.T) {
	a := map[string]interface{}{"b": 2, "a": 1, "c": map[string]interface{}{"y": "v", "x": "u"}}
	out1, err := canonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	out2, err := canonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	if string(out1) != string(out2) {
		t.Errorf("canonicalJSON not deterministic: %q vs %q", out1, out2)
	}
	want := `{"a":1,"b":2,"c":{"x":"u","y":"v"}}`
	if string(out1) != want {
		t.Errorf("canonicalJSON sort order wrong: got %q, want %q", out1, want)
	}
}

// TestValidateEdgeEndpoints exercises the edge-admission gate that handleLink
// runs against the resolved primary (and, for reciprocal labels, mirror)
// direction before the ceremony is invoked. Three structural defects were
// previously possible because the gate didn't exist:
//
//  1. source_kinds violation (e.g. an `audits` edge whose source is not an
//     audit_record);
//  2. target_kinds violation (e.g. a `raises` edge whose target is not a
//     sentinel);
//  3. self-loop on a label that declares forbid_self_loops=true (e.g. `audits`
//     per 036's audit-record fixed-point assert).
//
// Each subtest constructs an EdgeLabelDescriptor and two Nodes inline so the
// test is hermetic — no Spanner, no config file.
func TestValidateEdgeEndpoints(t *testing.T) {
	auditsDesc := NewEdgeLabelDescriptor(map[string]interface{}{
		"name":              "audits",
		"source_kinds":      []interface{}{"audit_record"},
		"target_kinds":      []interface{}{"axiom", "section", "audit_record"},
		"edge_family":       "audit",
		"forbid_self_loops": true,
	})
	raisesDesc := NewEdgeLabelDescriptor(map[string]interface{}{
		"name":         "raises",
		"source_kinds": []interface{}{"axiom", "section", "decision", "circuit"},
		"target_kinds": []interface{}{"sentinel"},
		"edge_family":  "diag",
	})
	dependsOnDesc := NewEdgeLabelDescriptor(map[string]interface{}{
		"name":         "depends_on",
		"source_kinds": []interface{}{"axiom", "section"},
		"target_kinds": []interface{}{"axiom", "section"},
		"edge_family":  "dep",
		// forbid_self_loops not set → self-loops permitted for this label.
	})
	axiom := func(id string) Node { return Node{NodeID: id, NodeKind: "axiom"} }
	auditRecord := func(id string) Node { return Node{NodeID: id, NodeKind: "audit_record"} }
	section := func(id string) Node { return Node{NodeID: id, NodeKind: "section"} }
	sentinel := func(id string) Node { return Node{NodeID: id, NodeKind: "sentinel"} }

	t.Run("valid_primary_admits", func(t *testing.T) {
		if err := validateEdgeEndpoints(auditsDesc, auditRecord("ar-1"), axiom("ax-1")); err != nil {
			t.Errorf("expected nil; got %v", err)
		}
	})

	t.Run("bad_source_kind_audits_from_axiom_refused", func(t *testing.T) {
		// Defect-1 reproduction: previously admitted ID 4ee317e3...
		err := validateEdgeEndpoints(auditsDesc, axiom("ax-1"), axiom("ax-2"))
		if !errors.Is(err, ErrEdgeKindMismatch) {
			t.Fatalf("expected ErrEdgeKindMismatch; got %v", err)
		}
		if !strings.Contains(err.Error(), "source_id") {
			t.Errorf("error should name source side; got %q", err.Error())
		}
	})

	t.Run("bad_target_kind_raises_to_axiom_refused", func(t *testing.T) {
		// Defect-2 reproduction: previously admitted ID 64f076f5...
		err := validateEdgeEndpoints(raisesDesc, axiom("ax-1"), axiom("ax-2"))
		if !errors.Is(err, ErrEdgeKindMismatch) {
			t.Fatalf("expected ErrEdgeKindMismatch; got %v", err)
		}
		if !strings.Contains(err.Error(), "target_id") {
			t.Errorf("error should name target side; got %q", err.Error())
		}
	})

	t.Run("valid_raises_admits", func(t *testing.T) {
		if err := validateEdgeEndpoints(raisesDesc, section("sec-1"), sentinel("sen-1")); err != nil {
			t.Errorf("expected nil; got %v", err)
		}
	})

	t.Run("self_loop_audits_refused", func(t *testing.T) {
		// Defect-3 reproduction: same-id source+target on a label whose
		// config sets forbid_self_loops=true.
		err := validateEdgeEndpoints(auditsDesc, auditRecord("ar-1"), auditRecord("ar-1"))
		if !errors.Is(err, ErrEdgeSelfLoopForbidden) {
			t.Fatalf("expected ErrEdgeSelfLoopForbidden; got %v", err)
		}
	})

	t.Run("self_loop_depends_on_allowed_when_unset", func(t *testing.T) {
		// depends_on doesn't declare forbid_self_loops in this test desc;
		// the gate must NOT refuse self-loops for labels that didn't opt in.
		// (Per-label policy lives in config; binary carries no global default.)
		if err := validateEdgeEndpoints(dependsOnDesc, axiom("ax-1"), axiom("ax-1")); err != nil {
			t.Errorf("expected nil for unset forbid_self_loops; got %v", err)
		}
	})

	t.Run("empty_source_kinds_means_any", func(t *testing.T) {
		// A label with no declared SourceKinds restriction admits any source kind.
		desc := NewEdgeLabelDescriptor(map[string]interface{}{
			"name":         "open_src",
			"target_kinds": []interface{}{"axiom"},
			"edge_family":  "ref",
		})
		if err := validateEdgeEndpoints(desc, section("sec-1"), axiom("ax-1")); err != nil {
			t.Errorf("expected nil for unset SourceKinds; got %v", err)
		}
	})

	t.Run("empty_target_kinds_means_any", func(t *testing.T) {
		desc := NewEdgeLabelDescriptor(map[string]interface{}{
			"name":         "open_tgt",
			"source_kinds": []interface{}{"axiom"},
			"edge_family":  "ref",
		})
		if err := validateEdgeEndpoints(desc, axiom("ax-1"), sentinel("sen-1")); err != nil {
			t.Errorf("expected nil for unset TargetKinds; got %v", err)
		}
	})
}

// TestCanonicalNameFromFilename verifies the seed_node filename →
// canonical name derivation: strip leading "NNN_" admission-order
// prefix and ".json" suffix. Gates every seed_node admission's identity.
func TestCanonicalNameFromFilename(t *testing.T) {
	cases := []struct{ in, want string }{
		{"001_origin.json", "origin"},
		{"002_node_identity_axiom.json", "node_identity_axiom"},
		{"039_some_long_canonical_name.json", "some_long_canonical_name"},
		{"100_x.json", "x"},
		{"no_prefix.json", "no_prefix"},
		{"a.json", "a"},
		{"_underscore_first.json", "_underscore_first"},
		{"123.json", "123"},
		{"file_with_underscores_only.json", "file_with_underscores_only"},
	}
	for _, c := range cases {
		got := canonicalNameFromFilename(c.in)
		if got != c.want {
			t.Errorf("canonicalNameFromFilename(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestLoadSubmissionsFromPathSkipsUnderscoreFiles verifies that the
// directory scan in loadSubmissionsFromPath ignores leading-underscore JSON
// filenames. _edges.json and _findings.json are authoring/validator
// artifacts that share the directory with submission files; admitting them
// as submissions corrupts the import (they have no node_kind/edge_label and
// trip stepSchemaValidate). AUTHORING_GUIDE.md §11 promises this skip.
func TestLoadSubmissionsFromPathSkipsUnderscoreFiles(t *testing.T) {
	dir := t.TempDir()
	valid := `{"artifact_type":"axiom","organization_id":"test-org","node_kind":"axiom","contract":{"asserts":"u1"},"payload":{"title":"u1"}}`
	if err := os.WriteFile(filepath.Join(dir, "001_node.json"), []byte(valid), 0o644); err != nil {
		t.Fatal(err)
	}
	// Files the scan must skip. Their content is deliberately non-submission
	// shape so a regression that picks them up surfaces as a parse error
	// rather than a silent malformed admission.
	if err := os.WriteFile(filepath.Join(dir, "_edges.json"), []byte(`{"edges":[{"source_canonical_name":"x"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "_findings.json"), []byte(`{"verdict":"pass"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	subs, err := loadSubmissionsFromPath(dir)
	if err != nil {
		t.Fatalf("loadSubmissionsFromPath returned err — scan likely picked up an underscore file: %v", err)
	}
	if len(subs) != 1 {
		t.Errorf("expected 1 submission (underscore files skipped), got %d (subs=%+v)", len(subs), subs)
	}
}

// TestResolveSubmissionCanonicalName covers the OOD_GRAPH_PROTOCOL_v1 §3
// identity-tuple canonical_name resolution rule: stable_id wins, then
// contract.canonical_name, then contract.canonical_err_name (sentinels),
// then error. Regression guard for the collapsed-identity bug where
// omitting stable_id silently fell back to the kind name, producing the
// same UUIDv5 for every same-kind submission.
func TestResolveSubmissionCanonicalName(t *testing.T) {
	cases := []struct {
		name    string
		sub     ArtifactSubmission
		want    string
		wantErr bool
	}{
		{
			name: "stable_id_wins",
			sub: ArtifactSubmission{
				StableID: "explicit_name",
				Contract: map[string]interface{}{"canonical_name": "other_name"},
			},
			want: "explicit_name",
		},
		{
			name: "contract_canonical_name_when_stable_id_empty",
			sub: ArtifactSubmission{
				Contract: map[string]interface{}{"canonical_name": "from_contract"},
			},
			want: "from_contract",
		},
		{
			name: "contract_canonical_err_name_for_sentinels",
			sub: ArtifactSubmission{
				Contract: map[string]interface{}{"canonical_err_name": "ErrSomeFailure"},
			},
			want: "ErrSomeFailure",
		},
		{
			name: "canonical_name_preferred_over_err_name_when_both_present",
			sub: ArtifactSubmission{
				Contract: map[string]interface{}{
					"canonical_name":     "preferred",
					"canonical_err_name": "ErrFallback",
				},
			},
			want: "preferred",
		},
		{
			name:    "error_when_nothing_resolves",
			sub:     ArtifactSubmission{NodeKind: "section"},
			wantErr: true,
		},
		{
			name: "error_when_contract_present_but_empty",
			sub: ArtifactSubmission{
				NodeKind: "section",
				Contract: map[string]interface{}{},
			},
			wantErr: true,
		},
		{
			name: "error_when_contract_canonical_name_is_empty_string",
			sub: ArtifactSubmission{
				Contract: map[string]interface{}{"canonical_name": ""},
			},
			wantErr: true,
		},
		{
			name: "error_when_contract_canonical_name_is_wrong_type",
			sub: ArtifactSubmission{
				Contract: map[string]interface{}{"canonical_name": 42},
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveSubmissionCanonicalName(tc.sub)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil (resolved to %q)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNativeSearchTerms(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"lowercases_and_splits", "BlobKeys Referent DEK", []string{"blobkeys", "referent", "dek"}},
		{"dedupes_first_seen", "dek DEK residence dek", []string{"dek", "residence"}},
		{"whitespace_only_is_empty", "   \t  ", nil},
		{"caps_at_eight", "a b c d e f g h i j", []string{"a", "b", "c", "d", "e", "f", "g", "h"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nativeSearchTerms(tc.query)
			if len(got) != len(tc.want) {
				t.Fatalf("terms = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("value missing expected substring %q: %s", needle, haystack)
	}
}

func mustNotContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Errorf("value unexpectedly contains substring %q: %s", needle, haystack)
	}
}

func TestNativeSearchHaystackAndScoreExpr(t *testing.T) {
	hay := nativeSearchHaystackExpr([]string{"id", "title"})
	wantHay := `LOWER(COALESCE(id, '') || ' ' || COALESCE(title, ''))`
	if hay != wantHay {
		t.Fatalf("haystack = %q, want %q", hay, wantHay)
	}
	score := nativeSearchScoreExpr(hay, 2)
	wantScore := `IF(STRPOS(` + wantHay + `, @t0) > 0, 1, 0) + IF(STRPOS(` + wantHay + `, @t1) > 0, 1, 0)`
	if score != wantScore {
		t.Fatalf("score = %q, want %q", score, wantScore)
	}
}

func TestNativeWorklinkSearchHaystackIncludesSessionIdentity(t *testing.T) {
	hay := nativeWorklinkSearchHaystackExpr()
	for _, column := range []string{"artifact_ref", "thread", "session_id"} {
		if !strings.Contains(hay, `COALESCE(`+column+`, '')`) {
			t.Fatalf("worklink search haystack omits exact identity carrier %q: %s", column, hay)
		}
	}
}

// TestHandleSearchNativeBypassesEmbeddingGate proves the fleet_plan_native
// dispatch happens BEFORE the embedding-config gate: the historical defect was
// task_graph_search demanding node_table/embeddings the native store cannot
// have. With a native substrate registered and Embed nil, the handler must not
// fail with either of those two configuration errors (the fake database means
// the Spanner query itself errors later, which is fine — the dispatch is what
// is under test).
func TestHandleSearchNativeBypassesEmbeddingGate(t *testing.T) {
	env := &Env{
		Config: SpecGraphConfig{DefaultProjectID: "arbor"},
		Substrates: newRouter(gcpauth.Selector{}, false, []SubstrateDescriptor{{
			ID:              "fleet",
			SpannerDatabase: "projects/p/instances/i/databases/d",
			TaskStoreType:   fleetPlanNativeStoreType,
			TaskTable:       "Tasks",
			WorklinkTable:   "TaskLinks",
			DepTable:        "TaskDeps",
		}}),
	}
	defer env.Substrates.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := handleSearch(ctx, env, searchInput{Query: "blobkeys residence"}, &snapshotPin{})
	if err == nil {
		t.Fatal("expected an error from the fake database, got nil")
	}
	for _, forbidden := range []string{"embedding not configured", "node_table not configured"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("native search hit the %q gate the dispatch must bypass: %v", forbidden, err)
		}
	}
}

func TestHandleSearchRejectsEmptyAndUnsearchableQuery(t *testing.T) {
	env := &Env{Config: SpecGraphConfig{DefaultProjectID: "arbor"}}
	if _, err := handleSearch(context.Background(), env, searchInput{}, &snapshotPin{}); err == nil || !strings.Contains(err.Error(), "query required") {
		t.Fatalf("empty query error = %v, want query required", err)
	}
	native := &SpannerSubstrateAdapter{Descriptor: SubstrateDescriptor{
		ID: "fleet", TaskStoreType: fleetPlanNativeStoreType,
		TaskTable: "Tasks", WorklinkTable: "TaskLinks", DepTable: "TaskDeps",
	}}
	if _, err := nativeSearchNodes(context.Background(), native, "arbor", "  \t ", "", 15); err == nil || !strings.Contains(err.Error(), "no searchable terms") {
		t.Fatalf("unsearchable query error = %v, want no searchable terms", err)
	}
}
