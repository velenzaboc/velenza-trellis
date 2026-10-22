package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// These receipts are the EXISTING circuit workflow's untrusted bookkeeping
// projection, not server review origin, oracle execution or runtime admission.
// No fixture inserts an IndependentReviewEvidence row. Software-origin positive
// controls continue to require the real configured child through actual MCP.
type reviewCircuitFixture struct {
	f *reviewProducerFixture
	leaf,delivery,binding,worktree string
	scope,receipt map[string]any
	files,candidates []any
}

func rpCircuitFixture(t *testing.T,f *reviewProducerFixture,legacy bool) *reviewCircuitFixture {
	t.Helper()
	c:=&reviewCircuitFixture{f:f,leaf:"CIRCUIT-LEAF",delivery:"CIRCUIT-DELIVERY",binding:"CIRCUIT-BINDING",worktree:"/recorded/remote/circuit-workspace"}
	c.scope=map[string]any{"project_id":"arbor","task_id":c.delivery,"epoch":"circuit-e1","context_sha256":strings.Repeat("e",64)}
	dir:=t.TempDir()
	validation:=[]byte(`{"positive":"candidate shape allowed","refusal":"malformed candidate refused","exception":"hash-only input without a software release"}`)
	candidate:=[]byte(`{"schema_version":"unadmitted_test_candidate/1","name":"fixture-only"}`)
	rpWrite(t,filepath.Join(dir,"validation.cases.json"),validation)
	rpWrite(t,filepath.Join(dir,"candidate.json"),candidate)
	c.files=[]any{map[string]any{"path":"validation.cases.json","sha256":rpDigest(validation)}}
	c.candidates=[]any{map[string]any{"path":"candidate.json","sha256":rpDigest(candidate)}}
	owners:=[]any{}
	for _,queue:=range []string{"author-validation","review-validation","author-circuit","validate-circuit","admit-circuit"} {owners=append(owners,map[string]any{"queue":queue,"kind":"subagent","ref":"circuit-fixture-"+queue})}
	plan:=map[string]any{"schema_version":1,"project_id":"arbor","task_id":c.delivery,"topic":"circuit hash-only preservation","kind":"circuit","mode":"subagents","rationale":"existing runtime bookkeeping boundary","epoch":"circuit-e1","context_sha256":strings.Repeat("e",64),"owners":owners,"admission":map[string]any{"ceremony":"node.save","post_admission_check":"owner runtime readback and representative run"}}
	f.createTask(c.delivery,"",map[string]any{"project_queues":plan})
	f.createTask(c.leaf,c.delivery,map[string]any{"ordinary":"keep circuit document","project_queue":c.scope})
	f.allow("task_graph_claim",map[string]any{"project_id":"arbor","task_id":c.leaf,"owner":"circuit-worker","session_id":"shared-session","repo":"example-infra"})
	f.allow("task_graph_add_worklink",map[string]any{"project_id":"arbor","task_id":c.leaf,"artifact_id":c.binding,"artifact_type":"worktree","artifact_ref":"circuit candidate workspace","repo":"example-infra","branch":"candidate-work","worktree_path":c.worktree,"thread":"circuit-thread","session_id":"shared-session"})
	tests:=map[string]any{"author":"validation-author","files":c.files,"requirements":[]any{"circuit-hash-boundary"}}
	slot:=map[string]any{"active":true,"project_id":"arbor","task_id":c.leaf,"queue":"review-validation","repo":"example-infra","worktree":c.worktree,"project_queue":c.scope,"tests":tests}
	note:=f.taskNote()
	if legacy {
		// No protected software origin may be discarded merely to choose the
		// singleton fixture. Callers use this only before software production.
		note["queue_assignment"]=slot;delete(note,"queue_assignments")
	} else {note["queue_assignments"]=append(note["queue_assignments"].([]any),slot)}
	f.updateNote(note)
	c.receipt=rpCircuitValidation(t,"arbor",c.leaf,strings.Repeat("e",64),tests)
	return c
}

func rpCircuitValidation(t *testing.T,project,task,contextHash string,tests map[string]any) map[string]any {
	t.Helper()
	coverage:=[]any{}
	for _,requirement:=range tests["requirements"].([]any) {for _,scenario:=range []string{"positive","refusal","exception"} {coverage=append(coverage,map[string]any{"requirement":requirement,"path":tests["files"].([]any)[0].(map[string]any)["path"],"case":"hash-only circuit validation","assertion":"registered candidate hashes remain the runtime handoff","boundary":"native circuit bookkeeping; owner oracle/admission remains separate","scenario":scenario})}}
	return map[string]any{"schema_version":"velenza_validation_acceptance/1","project_id":project,"task_id":task,"context_sha256":contextHash,"author":tests["author"],"reviewer":"independent-circuit-case-reviewer","blocking":0,"files":tests["files"],"requirements":tests["requirements"],"coverage":coverage,"evidence":[]any{"existing circuit review bookkeeping; no software origin or runtime PASS asserted"},"production_boundary":true,"tautologies":false,"fixture_supplied_authority":false}
}

func (c *reviewCircuitFixture) slot(note map[string]any) map[string]any {
	if singleton,ok:=note["queue_assignment"].(map[string]any);ok {if singleton["task_id"]==c.leaf {return singleton}}
	if slots,ok:=note["queue_assignments"].([]any);ok {for _,raw:=range slots {slot:=raw.(map[string]any);if slot["task_id"]==c.leaf {return slot}}}
	c.f.t.Fatal("registered circuit slot missing")
	return nil
}

func (c *reviewCircuitFixture) publishReview() {
	leaf:=c.f.noteFor(c.leaf);leaf["validation_acceptance"]=rpClone(c.f.t,c.receipt)
	c.f.updateNoteFor(c.leaf,leaf)
}

func (c *reviewCircuitFixture) approvalHash() string {
	var raw map[string]json.RawMessage
	if err:=json.Unmarshal([]byte(c.f.noteTextFor(c.leaf)),&raw);err!=nil {c.f.t.Fatal(err)}
	var compact bytes.Buffer
	if err:=json.Compact(&compact,raw["validation_acceptance"]);err!=nil {c.f.t.Fatal(err)}
	return rpDigest(compact.Bytes())
}

func (c *reviewCircuitFixture) publishAuthoring() {
	note:=c.f.taskNote();slot:=c.slot(note);slot["queue"]="author-circuit"
	slot["tests"].(map[string]any)["acceptance"]=map[string]any{"task_id":c.leaf,"sha256":c.approvalHash()}
	c.f.updateNote(note)
}

func (c *reviewCircuitFixture) publishAll() {c.publishReview();c.publishAuthoring()}

func TestReviewBoundaryCircuitHashHandoffAndRuntimeBookkeeping(t *testing.T) {
	for _,legacy:=range []bool{false,true} {t.Run(map[bool]string{false:"array-with-disjoint-software",true:"legacy-singleton"}[legacy],func(t *testing.T){
		f:=newReviewProducerFixture(t)
		if !legacy {f.allow("task_graph_review_produce",f.request)}
		beforeRows:=f.rows()
		var software map[string]any
		if !legacy {software=rpClone(t,f.slot(f.taskNote()))}
		c:=rpCircuitFixture(t,f,legacy)
		if _,exists:=c.slot(f.taskNote())["tests"].(map[string]any)["revision"];exists {t.Fatal("circuit fixture invented a Git test revision")}
		c.publishAll()
		leaf,controller:=f.noteFor(c.leaf),f.taskNote();slot:=c.slot(controller)
		approval:=leaf["validation_acceptance"].(map[string]any)
		if !reflect.DeepEqual(approval,rpClone(t,c.receipt)) || slot["queue"]!="author-circuit" || !reflect.DeepEqual(slot["tests"].(map[string]any)["files"],c.files) || slot["tests"].(map[string]any)["acceptance"].(map[string]any)["sha256"]!=c.approvalHash() {t.Fatal("hash-only circuit review/authoring handoff was not persisted exactly")}
		if _,exists:=approval["review_ref"];exists {t.Fatal("circuit metadata was mislabeled software origin")}
		if !legacy && !reflect.DeepEqual(software,f.slot(controller)) {t.Fatal("circuit publication changed the disjoint software slot")}
		// Candidate acceptance is the existing separate workflow receipt. It is
		// not an executed oracle, signature, node.save or post-admission proof.
		leaf["candidate_acceptance"]=map[string]any{"project_id":"arbor","task_id":c.leaf,"context_sha256":c.scope["context_sha256"],"blocking":0,"candidate_files":c.candidates,"reviewer":"independent-candidate-reviewer"}
		leaf["ordinary"]="updated circuit document about review";f.updateNoteFor(c.leaf,leaf)
		controller=f.taskNote();slot=c.slot(controller);slot["queue"]="admit-circuit";slot["candidate_files"]=c.candidates;slot["commands"]=[]any{"owner-validator candidate.json"};slot["validation_evidence"]=map[string]any{"task_id":c.leaf,"sha256":rpDigest(rpJSON(t,leaf["candidate_acceptance"]))};f.updateNote(controller)
		if !reflect.DeepEqual(f.noteFor(c.leaf)["candidate_acceptance"],rpClone(t,leaf["candidate_acceptance"].(map[string]any))) || c.slot(f.taskNote())["queue"]!="admit-circuit" || !reflect.DeepEqual(beforeRows,f.rows()) {t.Fatal("runtime candidate bookkeeping changed software origin or lost candidate handoff")}
	})}
}

func TestReviewBoundaryCircuitNeedsNoSoftwareProducerCapability(t *testing.T) {
	f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
	c:=rpCircuitFixture(t,f,false);before:=f.rows()
	delete(f.cfg,"review_producer");delete(f.cfg["substrates"].([]any)[0].(map[string]any),"review_evidence_table");f.start()
	c.publishAll()
	if !reflect.DeepEqual(before,f.rows()) {t.Fatal("circuit bookkeeping manufactured exclusive software origin")}
	f.deny("task_graph_review_get",f.request)
}

func TestReviewBoundaryCircuitDoorRequiresActualRegisteredContext(t *testing.T) {
	for _,change:=range []string{"missing-plan","software-plan","wrong-project-plan","stale-plan","missing-owner","duplicate-owner","wrong-admission","wrong-controller-session","wrong-slot-scope","caller-kind-only","missing-slot","duplicate-worktree-slot"} {t.Run(change,func(t *testing.T){
		f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
		c:=rpCircuitFixture(t,f,false);c.publishReview()
		// A renewed receipt is allowed in this exact review-validation stage
		// before context corruption, so no refusal can hide behind wrong stage.
		renewed:=rpClone(t,c.receipt);renewed["evidence"]=[]any{"second independent hash-only circuit review"};c.receipt=renewed;c.publishReview()
		// Change only pre-existing ordinary context, then ask to record a NEW
		// review. An unchanged protected echo alone cannot prove this refusal.
		if strings.Contains(change,"plan") || change=="missing-owner" || change=="duplicate-owner" || change=="wrong-admission" {
			note:=f.noteFor(c.delivery);plan:=note["project_queues"].(map[string]any)
			switch change {case "missing-plan":delete(note,"project_queues");case "software-plan":plan["kind"]="software";case "wrong-project-plan":plan["project_id"]="other-project";case "stale-plan":plan["epoch"]="stale";case "missing-owner":plan["owners"]=plan["owners"].([]any)[:4];case "duplicate-owner":plan["owners"].([]any)[1].(map[string]any)["ref"]=plan["owners"].([]any)[0].(map[string]any)["ref"];case "wrong-admission":plan["admission"].(map[string]any)["ceremony"]="software-merge"}
			f.updateNoteFor(c.delivery,note)
		} else if change=="wrong-controller-session" {f.allow("task_graph_update",map[string]any{"project_id":"arbor","node_id":f.executionID,"payload":map[string]any{"session_id":"other-session"}})} else {
			note:=f.taskNote();slot:=c.slot(note)
			switch change {case "wrong-slot-scope":slot["project_queue"].(map[string]any)["project_id"]="other-project";case "caller-kind-only":slot["kind"]="circuit";slot["project_queue"]=nil;case "missing-slot":note["queue_assignments"]=note["queue_assignments"].([]any)[:1];case "duplicate-worktree-slot":duplicate:=rpClone(t,slot);duplicate["task_id"]="caller-shadow";delete(duplicate["tests"].(map[string]any),"acceptance");note["queue_assignments"]=append(note["queue_assignments"].([]any),duplicate)}
			f.updateNote(note)
		}
		leaf:=f.noteFor(c.leaf);renewed=rpClone(t,c.receipt);renewed["evidence"]=[]any{"new review must resolve actual context"};leaf["validation_acceptance"]=renewed
		before:=f.taskObservation(c.leaf)
		f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":c.leaf,"payload":map[string]any{"note":string(rpJSON(t,leaf)),"title":"must not partially persist"}})
		if !reflect.DeepEqual(before,f.taskObservation(c.leaf)) || len(f.rows())!=1 {t.Fatal("invalid context changed leaf or exclusive origin")}
	})}
}

func TestReviewBoundaryCircuitReceiptAndReferenceBinding(t *testing.T) {
	for _,change:=range []string{"project","task","context","files","author","reviewer-is-author","blocking","flags","missing-coverage","software-ref"} {t.Run(change,func(t *testing.T){
		f:=newReviewProducerFixture(t);c:=rpCircuitFixture(t,f,false);c.publishReview()
		bad:=rpClone(t,c.receipt)
		switch change {case "project":bad["project_id"]="other-project";case "task":bad["task_id"]="other-leaf";case "context":bad["context_sha256"]=strings.Repeat("a",64);case "files":bad["files"].([]any)[0].(map[string]any)["sha256"]=strings.Repeat("a",64);case "author":bad["author"]="other-author";case "reviewer-is-author":bad["reviewer"]=bad["author"];case "blocking":bad["blocking"]=1;case "flags":bad["fixture_supplied_authority"]=true;case "missing-coverage":bad["coverage"]=[]any{};case "software-ref":bad["review_ref"]=rpClone(t,f.request)}
		leaf:=f.noteFor(c.leaf);leaf["validation_acceptance"]=bad
		f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":c.leaf,"payload":map[string]any{"note":string(rpJSON(t,leaf))}})
		if !reflect.DeepEqual(f.noteFor(c.leaf)["validation_acceptance"],rpClone(t,c.receipt)) {t.Fatal("invalid circuit receipt replaced accepted hash metadata")}
	})}
	for _,change:=range []string{"wrong-task","wrong-hash","software-reference","stale-files"} {t.Run("reference-"+change,func(t *testing.T){
		f:=newReviewProducerFixture(t);c:=rpCircuitFixture(t,f,false);c.publishReview()
		note:=f.taskNote();slot:=c.slot(note);slot["queue"]="author-circuit";acceptance:=map[string]any{"task_id":c.leaf,"sha256":c.approvalHash()};slot["tests"].(map[string]any)["acceptance"]=acceptance
		switch change {case "wrong-task":acceptance["task_id"]="other-leaf";case "wrong-hash":acceptance["sha256"]=strings.Repeat("a",64);case "software-reference":acceptance["review_ref"]=rpClone(t,f.request);case "stale-files":slot["tests"].(map[string]any)["files"].([]any)[0].(map[string]any)["sha256"]=strings.Repeat("a",64)}
		before:=f.taskObservation(f.executionID);f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":f.executionID,"payload":map[string]any{"note":string(rpJSON(t,note))}})
		if !reflect.DeepEqual(before,f.taskObservation(f.executionID)) || len(f.rows())!=0 {t.Fatal("cross-bound reference changed controller or minted software origin")}
	})}
}

func TestReviewBoundaryCircuitMetadataNeverSuppliesSoftwareOrigin(t *testing.T) {
	f:=newReviewProducerFixture(t);c:=rpCircuitFixture(t,f,false);c.publishAll()
	// There is a coherent real SOFTWARE plan/slot but no origin yet. Caller
	// labels cannot turn its old shared receipt shape into software authority.
	leaf:=f.leafNote();leaf["kind"]="circuit"
	softwareTests:=f.slot(f.taskNote())["tests"].(map[string]any)
	leaf["validation_acceptance"]=rpCircuitValidation(t,"arbor","REVIEW-TASK",strings.Repeat("c",64),softwareTests)
	f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK","payload":map[string]any{"note":string(rpJSON(t,leaf))}})
	software:=f.allow("task_graph_review_produce",f.request)
	before:=f.rows()
	// Valid circuit bookkeeping has no exclusive row; a circuit/legacy label
	// and a software kind request cannot turn that metadata into origin.
	request:=rpClone(t,f.request);request["task_id"]=c.leaf;request["binding_link_id"]=c.binding;request["epoch"]=c.scope["epoch"];request["context_sha256"]=c.scope["context_sha256"];request["review_id"]="circuit-legacy-review"
	f.deny("task_graph_review_get",request);f.deny("task_graph_review_produce",request)
	leaf=f.noteFor(c.leaf);leaf["validation_acceptance"].(map[string]any)["review_ref"]=software["review_ref"]
	f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":c.leaf,"payload":map[string]any{"note":string(rpJSON(t,leaf))}})
	controller:=f.taskNote();c.slot(controller)["tests"].(map[string]any)["acceptance"]=rpClone(t,f.slot(controller)["tests"].(map[string]any)["acceptance"].(map[string]any))
	f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":f.executionID,"payload":map[string]any{"note":string(rpJSON(t,controller))}})
	if !reflect.DeepEqual(before,f.rows()) {t.Fatal("circuit metadata changed genuine exclusive origin")}
}

func TestReviewBoundarySoftwarePublicationCannotBeRelabelledCircuit(t *testing.T) {
	f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
	beforeRows,beforeLeaf:=f.rows(),f.leafNote()
	c:=rpCircuitFixture(t,f,false)
	// Ordinary registered scope can change; it must not erase the immutable
	// before-value's real software reference when a circuit write is proposed.
	note:=f.taskNote();software:=f.slot(note);software["queue"]="review-validation";software["project_queue"]=rpClone(t,c.scope);delete(software["tests"].(map[string]any),"revision");f.updateNote(note)
	leaf:=f.leafNote();leaf["project_queue"]=rpClone(t,c.scope);f.updateNoteFor("REVIEW-TASK",leaf)
	receipt:=rpCircuitValidation(t,"arbor","REVIEW-TASK",strings.Repeat("e",64),software["tests"].(map[string]any))
	leaf["validation_acceptance"]=receipt
	f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK","payload":map[string]any{"note":string(rpJSON(t,leaf))}})
	note=f.taskNote();f.slot(note)["queue"]="author-circuit";f.slot(note)["tests"].(map[string]any)["acceptance"]=map[string]any{"task_id":"REVIEW-TASK","sha256":rpDigest(rpJSON(t,receipt))}
	f.denyOrigin("task_graph_update",map[string]any{"project_id":"arbor","node_id":f.executionID,"payload":map[string]any{"note":string(rpJSON(t,note))}})
	if !reflect.DeepEqual(beforeRows,f.rows()) || !reflect.DeepEqual(beforeLeaf["validation_acceptance"],f.leafNote()["validation_acceptance"]) {t.Fatal("relabel erased immutable software origin/reference")}
}

func TestReviewBoundaryCircuitLabelCannotBootstrapCreateOrImport(t *testing.T) {
	f:=newReviewProducerFixture(t);c:=rpCircuitFixture(t,f,false);c.publishAll()
	note:=map[string]any{"kind":"circuit","project_queue":c.scope,"validation_acceptance":rpClone(t,c.receipt)}
	note["validation_acceptance"].(map[string]any)["task_id"]="UNREGISTERED-CIRCUIT"
	contract:=map[string]any{"canonical_name":"UNREGISTERED-CIRCUIT","task_ref":"UNREGISTERED-CIRCUIT","level":"task"}
	payload:=map[string]any{"title":"unregistered circuit","pillar":"TEST","status":"not_started","owner":"UNOWNED","is_gap":false,"note":string(rpJSON(t,note))}
	f.denyOrigin("task_graph_create",map[string]any{"project_id":"arbor","node_kind":"task","stable_id":"UNREGISTERED-CIRCUIT","contract":contract,"payload":payload})
	f.denyOrigin("task_graph_import",map[string]any{"submissions":[]any{map[string]any{"node_kind":"task","stable_id":"UNREGISTERED-CIRCUIT","contract":contract,"payload":payload}}})
	if len(f.rows())!=0 {t.Fatal("generic bootstrap minted exclusive origin")}
}

// Both namespaces contain a real ordinary Task with the same ID. A wrong-project
// request must reach the origin fence, rather than pass by a not-found refusal.
func TestReviewBoundaryCircuitWrongProjectControllerRefuses(t *testing.T) {
	f:=newReviewProducerFixture(t);c:=rpCircuitFixture(t,f,false);c.publishReview()
	f.createTaskIn("other-project",c.leaf,"",map[string]any{"ordinary":"distinct project row","project_queue":rpClone(t,c.scope)})
	leaf:=f.noteFor(c.leaf);receipt:=rpClone(t,c.receipt);receipt["reviewer"]="another-independent-reviewer";leaf["validation_acceptance"]=receipt
	f.denyOrigin("task_graph_update",map[string]any{"project_id":"other-project","node_id":c.leaf,"payload":map[string]any{"note":string(rpJSON(t,leaf))}})
	if !reflect.DeepEqual(f.noteFor(c.leaf)["validation_acceptance"],rpClone(t,c.receipt)) {t.Fatal("wrong namespace changed real circuit receipt")}
}

func TestReviewBoundaryCircuitChangedHashesRequireRenewedReview(t *testing.T) {
	f:=newReviewProducerFixture(t);c:=rpCircuitFixture(t,f,false);c.publishAll()
	oldHash:=c.approvalHash()
	validation:=[]byte(`{"positive":"renewed candidate validation","refusal":"changed hash requires renewed review","exception":"still no Git revision"}`)
	rpWrite(t,filepath.Join(t.TempDir(),"validation.cases.json"),validation)
	c.files=[]any{map[string]any{"path":"validation.cases.json","sha256":rpDigest(validation)}}
	note:=f.taskNote();slot:=c.slot(note);slot["queue"]="review-validation";tests:=slot["tests"].(map[string]any);tests["files"]=c.files
	// Echoing old metadata during ordinary stage/input changes is allowed. The
	// renewed authoring handoff below must resolve the new receipt and file hash.
	f.updateNote(note)
	c.receipt=rpCircuitValidation(t,"arbor",c.leaf,strings.Repeat("e",64),tests)
	c.publishAll()
	if c.approvalHash()==oldHash || c.slot(f.taskNote())["tests"].(map[string]any)["acceptance"].(map[string]any)["sha256"]!=c.approvalHash() || len(f.rows())!=0 {t.Fatal("renewed hash-only review did not replace only circuit workflow metadata")}
}
