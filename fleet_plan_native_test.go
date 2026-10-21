package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

func TestTaskClaimCollisionDetail(t *testing.T) {
	err := TaskClaimCollisionDetail{TaskID: "T-1", Owner: "worker-a", SessionID: "sess-a", Repo: "task-graph"}
	if !errors.Is(err, ErrTaskClaimCollision) {
		t.Fatalf("collision does not unwrap ErrTaskClaimCollision: %v", err)
	}
	for _, want := range []string{"T-1", "worker-a", "sess-a", "task-graph"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("collision error %q missing %q", err, want)
		}
	}
}

func TestClaimReceiptEnvelopeShape(t *testing.T) {
	r := ClaimReceipt{ReceiptKind: "claim", Issuer: "task-graph", ProjectID: "arbor", TaskID: "T", Owner: "worker", SessionID: "sess", Repo: "task-graph"}
	if r.ReceiptKind != "claim" || r.Issuer != "task-graph" || r.CryptographicSignature != "" {
		t.Fatalf("unexpected claim receipt envelope: %#v", r)
	}
}

func TestIdempotentClaimCannotReattestEphemeralBinding(t *testing.T) {
	if err := validateClaimReceiptBinding("claim", &ClaimBinding{Branch: "branch-a", WorktreePath: "C:/worktree-a"}); err != nil {
		t.Fatalf("first claim binding rejected: %v", err)
	}
	if err := validateClaimReceiptBinding("idempotent", nil); err != nil {
		t.Fatalf("binding-free idempotent retry rejected: %v", err)
	}
	if err := validateClaimReceiptBinding("idempotent", &ClaimBinding{Repo: "task-graph", SessionID: "session-a"}); err != nil {
		t.Fatalf("durable tuple repeated inside binding rejected: %v", err)
	}
	for field, binding := range map[string]*ClaimBinding{
		"branch":        {Branch: "branch-b"},
		"worktree_path": {WorktreePath: "C:/worktree-b"},
		"host":          {Host: "host-b"},
		"account":       {Account: "account-b"},
		"harness":       {Harness: "harness-b"},
		"thread":        {Thread: "thread-b"},
	} {
		if err := validateClaimReceiptBinding("idempotent", binding); err == nil {
			t.Fatalf("idempotent retry re-attested caller-supplied %s", field)
		}
	}
}

func TestTaskClaimDecisionFirstWriterWins(t *testing.T) {
	for _, tc := range []struct {
		name, currentOwner, currentSession, want string
	}{
		{"empty", "", "", "claim"},
		{"unowned", "UNOWNED", "old-session", "claim"},
		{"same-owner-session", "worker-a", "sess-a", "idempotent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := taskClaimDecision("T-1", tc.currentOwner, tc.currentSession, "repo", "worker-a", "sess-a", "repo")
			if err != nil || got != tc.want {
				t.Fatalf("decision = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
	if _, err := taskClaimDecision("T-1", "worker-b", "sess-b", "repo-b", "worker-a", "sess-a", "repo"); !errors.Is(err, ErrTaskClaimCollision) {
		t.Fatalf("occupied task error = %v, want ErrTaskClaimCollision", err)
	}
}

func TestValidateClaimInputRequiresRepository(t *testing.T) {
	in := claimInput{TaskID: "T-1", Owner: "worker", SessionID: "session"}
	if err := validateClaimInput(in); err == nil || !strings.Contains(err.Error(), "repo required") {
		t.Fatalf("missing repo validation = %v, want repo required", err)
	}
	in.Repo = "task-graph"
	if err := validateClaimInput(in); err != nil {
		t.Fatalf("complete claim input rejected: %v", err)
	}
}

func TestTaskReownHandoverDecision(t *testing.T) {
	if _, err := taskClaimDecision("T-1", "owner-a", "sess-a", "repo-a", "owner-b", "sess-b", "repo-b"); !errors.Is(err, ErrTaskClaimCollision) {
		t.Fatalf("mismatched handover = %v, want collision", err)
	}
	if decision, err := taskClaimDecision("T-1", "owner-a", "sess-a", "repo-a", "owner-a", "sess-a", "repo-a"); err != nil || decision != "idempotent" {
		t.Fatalf("matching handover = %q, %v; want idempotent", decision, err)
	}
	if decision, err := taskClaimDecision("T-1", "UNOWNED", "", "", "owner-b", "sess-b", "repo-b"); err != nil || decision != "claim" {
		t.Fatalf("UNOWNED recovery decision = %q, %v; want claim", decision, err)
	}
}

func TestReownExpectedTupleIncludesRepo(t *testing.T) {
	in := reownInput{ExpectedOwner: "owner", ExpectedSessionID: "session", ExpectedRepo: "repo"}
	if !reownExpectedTupleMatches(in, "owner", "session", "repo") {
		t.Fatal("matching owner/session/repo tuple rejected")
	}
	in.ExpectedRepo = "other-repo"
	if reownExpectedTupleMatches(in, "owner", "session", "repo") {
		t.Fatal("repo mismatch accepted by schema-v1 handover guard")
	}
}

func TestSchemaV1ReownReleaseRequiresExpectedOwnedTuple(t *testing.T) {
	release := reownInput{TaskID: "T-1", Owner: "UNOWNED"}
	if err := validateReownExpectedTuple(release, "owner-a", "session-a", "repo-a"); err == nil {
		t.Fatal("owned schema-v1 task released without expected owner/session/repository tuple")
	}
	release.ExpectedOwner = "owner-a"
	release.ExpectedSessionID = "session-a"
	release.ExpectedRepo = "repo-a"
	if err := validateReownExpectedTuple(release, "owner-a", "session-a", "repo-a"); err != nil {
		t.Fatalf("exact expected tuple rejected for schema-v1 release: %v", err)
	}
	release.ExpectedRepo = "other-repo"
	if err := validateReownExpectedTuple(release, "owner-a", "session-a", "repo-a"); !errors.Is(err, ErrTaskClaimCollision) {
		t.Fatalf("mismatched expected tuple error = %v, want ErrTaskClaimCollision", err)
	}
	if err := validateReownExpectedTuple(reownInput{TaskID: "T-1", Owner: "UNOWNED"}, "UNOWNED", "", ""); err != nil {
		t.Fatalf("idempotent release of unowned schema-v1 task rejected: %v", err)
	}
}

func TestReownFromUnownedRequiresClaim(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   reownInput
	}{
		{name: "omitted expected tuple", in: reownInput{TaskID: "T-1", Owner: "owner-b", SessionID: "session-b", Repo: "repo-b"}},
		{name: "stale owner only", in: reownInput{TaskID: "T-1", Owner: "owner-b", SessionID: "session-b", Repo: "repo-b", ExpectedOwner: "owner-a"}},
		{name: "stale session only", in: reownInput{TaskID: "T-1", Owner: "owner-b", SessionID: "session-b", Repo: "repo-b", ExpectedSessionID: "session-a"}},
		{name: "stale repo only", in: reownInput{TaskID: "T-1", Owner: "owner-b", SessionID: "session-b", Repo: "repo-b", ExpectedRepo: "repo-a"}},
		{name: "stale owned tuple", in: reownInput{TaskID: "T-1", Owner: "owner-b", SessionID: "session-b", Repo: "repo-b", ExpectedOwner: "owner-a", ExpectedSessionID: "session-a", ExpectedRepo: "repo-a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateReownExpectedTuple(tc.in, "UNOWNED", "", ""); !errors.Is(err, ErrTaskClaimCollision) {
				t.Fatalf("unowned reown error = %v, want ErrTaskClaimCollision", err)
			}
		})
	}
}

func TestValidateReownInputRequiresOwnedBinding(t *testing.T) {
	for _, in := range []reownInput{
		{},
		{TaskID: "T-1", Owner: "owner-a"},
		{TaskID: "T-1", Owner: "owner-a", SessionID: "session-a"},
	} {
		if err := validateReownInput(in); err == nil {
			t.Fatalf("incomplete owned handover accepted: %#v", in)
		}
	}
	if err := validateReownInput(reownInput{TaskID: "T-1", Owner: "owner-a", SessionID: "session-a", Repo: "task-graph"}); err != nil {
		t.Fatalf("complete owned handover rejected: %v", err)
	}
	if err := validateReownInput(reownInput{TaskID: "T-1", Owner: "UNOWNED"}); err != nil {
		t.Fatalf("UNOWNED handover rejected: %v", err)
	}
	if err := validateReownInput(reownInput{TaskID: "T-1", Owner: "UNOWNED", SessionID: "stale-session", Repo: "task-graph"}); err == nil {
		t.Fatal("UNOWNED handover retained an owner-bound session/repository tuple")
	}
}

// nativeParamString unwraps a Spanner param value produced by either the
// create or the update builder into a comparable Go string, tolerating the
// bare-string form (pillar/status/layer on create) and the
// spanner.NullString-wrapped form (nativeParam on update; title/owner/
// gap_label/note/session_id/repo/parent_id on create).
func nativeParamString(v interface{}) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case spanner.NullString:
		return t.StringVal, t.Valid
	default:
		return "", false
	}
}

// nativeParamBool unwraps a Spanner param value into a comparable Go bool,
// tolerating the bare-bool form (is_gap on both paths) and the
// spanner.NullBool-wrapped form (is_critical_path on create via
// nullBoolFromMap).
func nativeParamBool(v interface{}) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case spanner.NullBool:
		return t.Bool, t.Valid
	default:
		return false, false
	}
}

// TestNativeTaskCreateUpdateFieldParity is the regression guard for
// P3-GT-FP-CREATE-DROPS-FIELDS: task_graph_create's task-plan-native INSERT
// silently dropped is_critical_path — and, once the two write paths were
// enumerated field-by-field rather than eyeballed, session_id, repo, and the
// payload.level alias for layer too — while task_graph_update persisted the
// same fields correctly (re-sending the identical value through
// nativeUpdateTaskFields worked).
//
// This test does not assert anything about generated SQL text. It asserts
// the PARITY PROPERTY the ticket actually cares about: for every field
// nativeTaskFieldColumns declares write-able on the Tasks table, a payload
// carrying that field alone must produce a Spanner param for the field's
// target column on BOTH the create builder (nativeBuildTaskInsert) and the
// update builder (nativeBuildTaskUpdate), carrying the value the caller
// supplied. A field added to nativeTaskFieldColumns (i.e. accepted by
// UPDATE) and forgotten in nativeBuildTaskInsert fails this test
// immediately — no Spanner emulator required, since both builders are pure
// functions with no I/O.
func TestNativeTaskCreateUpdateFieldParity(t *testing.T) {
	now := time.Now().UTC()

	for field, column := range nativeTaskFieldColumns {
		t.Run(field, func(t *testing.T) {
			isBool := column == "is_gap" || column == "is_critical_path"

			// parent_id is threaded into nativeBuildTaskInsert as a
			// separately-resolved argument (nativeHandleCreate validates
			// parent_ref vs payload.parent_id and existence BEFORE calling
			// the builder) rather than read out of payload directly — so it
			// is probed differently on the create side. nativeBuildTaskUpdate
			// has no such split; it reads parent_id straight out of the
			// fields map like everything else.
			payload := map[string]interface{}{}
			parentID := ""
			if field == "parent_id" {
				parentID = "probe-" + field
			} else if isBool {
				payload[field] = true
			} else {
				payload[field] = "probe-" + field
			}

			_, createParams := nativeBuildTaskInsert("TASK-ID", parentID, nil, payload, now)
			createVal, createPresent := createParams[column]
			if !createPresent {
				t.Fatalf("create builder: payload field %q (column %q) produced no param at all — silently dropped", field, column)
			}
			if isBool {
				got, valid := nativeParamBool(createVal)
				if !valid || got != true {
					t.Fatalf("create builder: payload field %q (column %q) = %#v, want true", field, column, createVal)
				}
			} else {
				got, valid := nativeParamString(createVal)
				want := "probe-" + field
				if !valid || got != want {
					t.Fatalf("create builder: payload field %q (column %q) = %#v, want %q", field, column, createVal, want)
				}
			}

			// UPDATE: parent_id round-trips through the fields map exactly
			// like every other field, so no special-casing needed here.
			updateFields := map[string]interface{}{field: payload[field]}
			if field == "parent_id" {
				updateFields = map[string]interface{}{field: "probe-" + field}
			}
			_, updateParams := nativeBuildTaskUpdate(updateFields)
			updateVal, updatePresent := updateParams[column]
			if !updatePresent {
				t.Fatalf("update builder: payload field %q (column %q) produced no param at all — silently dropped", field, column)
			}
			if isBool {
				got, valid := nativeParamBool(updateVal)
				if !valid || got != true {
					t.Fatalf("update builder: payload field %q (column %q) = %#v, want true", field, column, updateVal)
				}
			} else {
				got, valid := nativeParamString(updateVal)
				want := "probe-" + field
				if !valid || got != want {
					t.Fatalf("update builder: payload field %q (column %q) = %#v, want %q", field, column, updateVal, want)
				}
			}
		})
	}
}

// TestNativeBuildTaskInsertCriticalPathAbsentIsNull pins the NULL-vs-false
// decision recorded in gt-w1-fp-create-fields.md: is_critical_path is a
// nullable Tasks column (unlike is_gap, which is NOT NULL and defaults
// false), and nativeUpdateTaskFields already treats an absent key as
// "leave the column alone" rather than "set it false". A create call that
// never mentions is_critical_path must therefore write SQL NULL, not false,
// so a freshly created task is indistinguishable from one nobody has
// classified yet — exactly the state the orchestrator's own diagnostic
// query (IFNULL(CAST(is_critical_path AS STRING),'NULL')) treats as a
// distinct, meaningful value.
func TestNativeBuildTaskInsertCriticalPathAbsentIsNull(t *testing.T) {
	_, params := nativeBuildTaskInsert("TASK-ID", "", nil, map[string]interface{}{}, time.Now().UTC())
	got, ok := params["is_critical_path"].(spanner.NullBool)
	if !ok {
		t.Fatalf("is_critical_path param = %#v (%T), want spanner.NullBool", params["is_critical_path"], params["is_critical_path"])
	}
	if got.Valid {
		t.Fatalf("is_critical_path absent from payload produced Valid=true (%v) — want SQL NULL (Valid=false)", got.Bool)
	}
}

// TestNativeTaskNodeFromRowPreservesNullableCriticalPath pins the read half
// of the task-plan get→update round trip. SQL NULL is an unclassified value,
// not false, so task_graph_get must expose it as JSON null while preserving
// concrete true and false values.
func TestNativeTaskNodeFromRowPreservesNullableCriticalPath(t *testing.T) {
	tests := []struct {
		name string
		raw  interface{}
		want interface{}
	}{
		{name: "null", raw: nil, want: nil},
		{name: "false", raw: false, want: false},
		{name: "true", raw: true, want: true},
		{name: "string-false", raw: "false", want: false},
		{name: "string-true", raw: "true", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := map[string]interface{}{
				"id":               "TASK-ID",
				"project_id":       "arbor",
				"created_at":       "2026-08-27T00:00:00Z",
				"updated_at":       "2026-08-27T00:00:00Z",
				"is_critical_path": tc.raw,
			}
			payload := nativeTaskNodeFromRow(row).CurrentVersion.Payload
			got, present := payload["is_critical_path"]
			if !present {
				t.Fatal("is_critical_path missing from task payload")
			}
			if got != tc.want {
				t.Fatalf("is_critical_path = %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

// TestNativeBuildTaskUpdateCriticalPathValueSemantics pins the write half of
// the same round trip. An explicit JSON null must bind a typed BOOL NULL;
// true and false must remain concrete BOOL values.
func TestNativeBuildTaskUpdateCriticalPathValueSemantics(t *testing.T) {
	_, nullParams := nativeBuildTaskUpdate(map[string]interface{}{"is_critical_path": nil})
	nullValue, ok := nullParams["is_critical_path"].(spanner.NullBool)
	if !ok {
		t.Fatalf("explicit null param = %#v (%T), want spanner.NullBool", nullParams["is_critical_path"], nullParams["is_critical_path"])
	}
	if nullValue.Valid {
		t.Fatalf("explicit null produced Valid=true (%v), want SQL NULL", nullValue.Bool)
	}

	for _, want := range []bool{false, true} {
		_, params := nativeBuildTaskUpdate(map[string]interface{}{"is_critical_path": want})
		got, valid := nativeParamBool(params["is_critical_path"])
		if !valid || got != want {
			t.Fatalf("is_critical_path %v update param = %#v, want concrete BOOL %v", want, params["is_critical_path"], want)
		}
	}
}

// TestNativeBuildTaskInsertHonorsPayloadLevelAlias pins the fourth member of
// the asymmetry set found while enumerating (not eyeballing) the field
// diff: nativeUpdateTaskFields has long accepted "level" as a caller-facing
// alias for "layer" (nativeTaskFieldColumns maps both to the layer column);
// CREATE only ever consulted contract.level and payload.layer, silently
// ignoring payload.level.
func TestNativeBuildTaskInsertHonorsPayloadLevelAlias(t *testing.T) {
	_, params := nativeBuildTaskInsert("TASK-ID", "", nil, map[string]interface{}{"level": "L2"}, time.Now().UTC())
	got, _ := nativeParamString(params["layer"])
	if got != "L2" {
		t.Fatalf("layer param with only payload.level set = %q, want %q", got, "L2")
	}
}

// nativeParamInt64 unwraps a Spanner param value for an INT64 column into a
// comparable Go int64, tolerating the bare-int64 form (int64FromAny on a
// supplied value) and the spanner.NullInt64-wrapped form (nullInt64FromMap on
// an absent one).
func nativeParamInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case spanner.NullInt64:
		return t.Int64, t.Valid
	default:
		return 0, false
	}
}

// worklinkProbeValue returns the probe a field of the given target column
// should be exercised with: an int for the INT64 pr_number column, a
// distinctive string for everything else.
func worklinkProbeValue(field, column string) interface{} {
	if column == "pr_number" {
		return 4242
	}
	return "probe-" + field
}

// worklinkProbeMatches asserts a produced Spanner param carries the probe,
// unwrapping whichever nullable form the builder used.
func worklinkProbeMatches(t *testing.T, side, field, column string, got, want interface{}) {
	t.Helper()
	if column == "pr_number" {
		n, valid := nativeParamInt64(got)
		if !valid || n != int64(want.(int)) {
			t.Fatalf("%s builder: field %q (column %q) = %#v, want %d", side, field, column, got, want)
		}
		return
	}
	s, valid := nativeParamString(got)
	if !valid || s != want.(string) {
		t.Fatalf("%s builder: field %q (column %q) = %#v, want %q", side, field, column, got, want)
	}
}

// TestNativeWorklinkCreateUpdateFieldParity is the regression guard for
// P3-GT-FP-WORKLINK-FIELD-PARITY: task_graph_add_worklink's task-plan-native
// INSERT named its own literal eight-column list while
// nativeUpdateWorklinkFields accepted thirteen allow-listed fields PLUS a
// pr_number special case — so session_id, repo, branch, commit_sha,
// pr_number, pr_state, worktree_path and url were all persisted by UPDATE and
// silently dropped by CREATE. Same defect class as
// P3-GT-FP-CREATE-DROPS-FIELDS on the task lane, and wider: eight fields, not
// four.
//
// Like its task-lane sibling this asserts no SQL text. It asserts the PARITY
// PROPERTY: for every field nativeWorklinkFieldColumns declares write-able on
// TaskLinks, a caller supplying that field alone must produce a Spanner param
// for the field's target column on BOTH nativeBuildWorklinkInsert and
// nativeBuildWorklinkUpdate, carrying the supplied value. A field added to
// one builder and forgotten in the other fails here immediately — both are
// pure functions, so no Spanner emulator is required.
func TestNativeWorklinkCreateUpdateFieldParity(t *testing.T) {
	now := time.Now().UTC()

	for field, column := range nativeWorklinkFieldColumns {
		t.Run(field, func(t *testing.T) {
			want := worklinkProbeValue(field, column)

			// link_id, task_id, artifact_type and artifact_ref are threaded
			// into nativeBuildWorklinkInsert as separately-resolved arguments
			// (nativeHandleAddWorklink validates task existence in-transaction
			// and resolves artifact_type against the closed registry BEFORE
			// calling the builder) rather than read out of the fields map — so
			// the three that are also UPDATE-able are probed through their
			// argument on the create side. nativeBuildWorklinkUpdate has no
			// such split; it reads all four straight out of the fields map.
			fields := map[string]interface{}{}
			taskID, artifactType, artifactRef := "", "", ""
			switch field {
			case "task_id":
				taskID = want.(string)
			case "artifact_type":
				artifactType = want.(string)
			case "artifact_ref":
				artifactRef = want.(string)
			default:
				fields[field] = want
			}

			_, createParams := nativeBuildWorklinkInsert("LNK-ID", taskID, artifactType, artifactRef, fields, now)
			createVal, createPresent := createParams[column]
			if !createPresent {
				t.Fatalf("create builder: field %q (column %q) produced no param at all — silently dropped", field, column)
			}
			worklinkProbeMatches(t, "create", field, column, createVal, want)

			_, updateParams := nativeBuildWorklinkUpdate(map[string]interface{}{field: want})
			updateVal, updatePresent := updateParams[column]
			if !updatePresent {
				t.Fatalf("update builder: field %q (column %q) produced no param at all — silently dropped", field, column)
			}
			worklinkProbeMatches(t, "update", field, column, updateVal, want)
		})
	}
}

// TestNativeAddWorklinkInputReachesInsertParams closes the loop the parity
// test above leaves open: the parity test probes the BUILDER, but the caller
// hands a typed addWorklinkInput, and a field can be declared write-able,
// accepted by the builder, and still dropped because the wire struct never
// carried it. For every field in nativeWorklinkFieldColumns, this drives the
// real projection nativeHandleAddWorklink uses (nativeWorklinkInsertFields)
// and asserts the value survives into the insert params.
func TestNativeAddWorklinkInputReachesInsertParams(t *testing.T) {
	now := time.Now().UTC()
	in := addWorklinkInput{
		TaskID:       "probe-task_id",
		ArtifactType: "probe-artifact_type",
		ArtifactRef:  "probe-artifact_ref",
		Thread:       "probe-thread",
		Note:         "probe-note",
		SessionID:    "probe-session_id",
		Repo:         "probe-repo",
		Branch:       "probe-branch",
		CommitSHA:    "probe-commit_sha",
		PRNumber:     4242,
		PRState:      "probe-pr_state",
		WorktreePath: "probe-worktree_path",
		URL:          "probe-url",
	}
	_, params := nativeBuildWorklinkInsert("LNK-ID", in.TaskID, in.ArtifactType, in.ArtifactRef, nativeWorklinkInsertFields(in), now)

	for field, column := range nativeWorklinkFieldColumns {
		got, present := params[column]
		if !present {
			t.Fatalf("addWorklinkInput field %q (column %q) produced no insert param — the wire struct or the projection drops it", field, column)
		}
		worklinkProbeMatches(t, "add_worklink input", field, column, got, worklinkProbeValue(field, column))
	}
}

// TestNativeArtifactCreatePayloadReachesInsertParams covers the SECOND create
// entrypoint into TaskLinks: task_graph_create(node_kind="artifact") maps an
// open-ended payload map into addWorklinkInput. It previously read only
// task_id, thread and note, discarding every provenance key a caller put in
// the payload — the same silent drop as the typed lane, reachable without any
// schema change at all.
func TestNativeArtifactCreatePayloadReachesInsertParams(t *testing.T) {
	now := time.Now().UTC()
	contract := map[string]interface{}{
		"canonical_name":     "LNK-PROBE",
		"artifact_type_kind": "probe-artifact_type",
		"artifact_ref":       "probe-artifact_ref",
	}
	payload := map[string]interface{}{
		"task_id":       "probe-task_id",
		"thread":        "probe-thread",
		"note":          "probe-note",
		"session_id":    "probe-session_id",
		"repo":          "probe-repo",
		"branch":        "probe-branch",
		"commit_sha":    "probe-commit_sha",
		"pr_number":     float64(4242), // the shape a JSON decode delivers
		"pr_state":      "probe-pr_state",
		"worktree_path": "probe-worktree_path",
		"url":           "probe-url",
	}
	in := nativeArtifactCreateInput("arbor", "", contract, payload)
	_, params := nativeBuildWorklinkInsert(in.ArtifactID, in.TaskID, in.ArtifactType, in.ArtifactRef, nativeWorklinkInsertFields(in), now)

	for field, column := range nativeWorklinkFieldColumns {
		got, present := params[column]
		if !present {
			t.Fatalf("create(node_kind=artifact) payload field %q (column %q) produced no insert param — silently dropped", field, column)
		}
		worklinkProbeMatches(t, "create-artifact payload", field, column, got, worklinkProbeValue(field, column))
	}
}

// TestNativeBuildWorklinkInsertPRNumberAbsentIsNull is the worklink-lane
// analogue of TestNativeBuildTaskInsertCriticalPathAbsentIsNull: pr_number is
// a nullable INT64 column, and nativeBuildWorklinkUpdate already treats an
// absent key as "leave the column alone" rather than "set it 0". A create
// that never mentions a PR must therefore write SQL NULL, so a link with no
// PR attached is distinguishable from one someone recorded as PR #0.
func TestNativeBuildWorklinkInsertPRNumberAbsentIsNull(t *testing.T) {
	_, params := nativeBuildWorklinkInsert("LNK-ID", "TASK-1", "pr", "ref", map[string]interface{}{}, time.Now().UTC())
	got, ok := params["pr_number"].(spanner.NullInt64)
	if !ok {
		t.Fatalf("pr_number param = %#v (%T), want spanner.NullInt64", params["pr_number"], params["pr_number"])
	}
	if got.Valid {
		t.Fatalf("pr_number absent from fields produced Valid=true (%d) — want SQL NULL (Valid=false)", got.Int64)
	}
}

// TestNativeBuildWorklinkInsertNeverAcceptsArtifactStatus pins the carve-out
// documented on nativeWorklinkFieldColumns: artifact_status is lifecycle, not
// content. CREATE always stamps 'recorded' and must ignore a caller-supplied
// value, or task_graph_add_worklink becomes a way to mint an already-archived
// link that task_graph_worklinks will never return.
func TestNativeBuildWorklinkInsertNeverAcceptsArtifactStatus(t *testing.T) {
	_, params := nativeBuildWorklinkInsert("LNK-ID", "TASK-1", "pr", "ref",
		map[string]interface{}{"artifact_status": "archived"}, time.Now().UTC())
	got, _ := nativeParamString(params["artifact_status"])
	if got != "recorded" {
		t.Fatalf("artifact_status on create with a caller-supplied 'archived' = %q, want %q", got, "recorded")
	}
	if _, present := nativeWorklinkFieldColumns["artifact_status"]; present {
		t.Fatal("artifact_status is in nativeWorklinkFieldColumns — the parity surface would then require CREATE to honor a caller-supplied lifecycle value")
	}
}

// TestNativeBuildWorklinkUpdateValueSemantics pins the two value-shaped rules
// the shared-map refactor had to preserve verbatim from the pre-refactor
// nativeUpdateWorklinkFields. Neither is new policy; both are easy to lose
// silently when folding a special case into a table-driven loop, and a green
// suite would not have noticed.
func TestNativeBuildWorklinkUpdateValueSemantics(t *testing.T) {
	// (1) an empty or nil STRING-shaped value is skipped, never written.
	assignments, params := nativeBuildWorklinkUpdate(map[string]interface{}{"repo": "", "branch": nil})
	if len(assignments) != 0 {
		t.Fatalf("empty/nil string fields produced assignments %v (params %v), want none", assignments, params)
	}

	// (2) pr_number is applied whenever its KEY is present, regardless of
	// value — the string-shaped guard above must not swallow a clear on an
	// INT64 column. nil renders as SQL NULL.
	assignments, params = nativeBuildWorklinkUpdate(map[string]interface{}{"pr_number": nil})
	if len(assignments) != 1 {
		t.Fatalf("pr_number:nil produced assignments %v, want exactly one", assignments)
	}
	got, ok := params["pr_number"].(spanner.NullInt64)
	if !ok || got.Valid {
		t.Fatalf("pr_number:nil param = %#v, want spanner.NullInt64{Valid:false}", params["pr_number"])
	}

	// (3) artifact_status stays UPDATE-able even though it is outside the
	// shared parity map.
	assignments, params = nativeBuildWorklinkUpdate(map[string]interface{}{"artifact_status": "archived"})
	if len(assignments) != 1 {
		t.Fatalf("artifact_status produced assignments %v, want exactly one", assignments)
	}
	if s, _ := nativeParamString(params["artifact_status"]); s != "archived" {
		t.Fatalf("artifact_status param = %#v, want %q", params["artifact_status"], "archived")
	}
}

// TestNativeBuildWorklinkInsertColumnsAndParamsAgree guards the hand-written
// column list against a typo: every declared column must have a param and
// every param must be a declared column, or the INSERT fails at Spanner with
// an unbound-parameter error that only an emulator run would surface.
func TestNativeBuildWorklinkInsertColumnsAndParamsAgree(t *testing.T) {
	columns, params := nativeBuildWorklinkInsert("LNK-ID", "TASK-1", "pr", "ref", map[string]interface{}{}, time.Now().UTC())
	if len(columns) != len(params) {
		t.Fatalf("columns=%d params=%d — %v vs %v", len(columns), len(params), columns, params)
	}
	declared := map[string]bool{}
	for _, col := range columns {
		if declared[col] {
			t.Fatalf("column %q declared twice", col)
		}
		declared[col] = true
		if _, ok := params[col]; !ok {
			t.Fatalf("column %q has no param", col)
		}
	}
	for col := range params {
		if !declared[col] {
			t.Fatalf("param %q is not a declared column", col)
		}
	}
}
