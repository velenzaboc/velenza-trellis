//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/spanner"
)

// This private fixture reuses the real MCP launcher, controlled executable
// reviewer, Git helpers and exclusive-table DDL. Existing frozen fixtures assume
// a physical project column and an arbor controller; they cannot construct the
// pre-cutover path. No helper here inserts origin or acceptance directly.
type rnFixture struct { f *reviewProducerFixture; project string }

func rnNewFixture(t *testing.T, project string, keyed bool) *rnFixture {
	t.Helper()
	requireEmulator(t)
	ledger:=nativePreCutoverSchema
	if keyed { ledger=nativeProjectSchema }
	schema:=append(append([]string{},ledger...),reviewProducerTestDDL)
	setupEmulatorDBWithSchema(context.Background(),t,schema)
	client,err:=spanner.NewClient(context.Background(),emulatorResource)
	if err!=nil { t.Fatal(err) }
	t.Cleanup(client.Close)
	dir:=t.TempDir();repo:=filepath.Join(dir,"source")
	if err:=os.Mkdir(repo,0700);err!=nil { t.Fatal(err) }
	rpGit(t,repo,"init","-b","task/review")
	rpGit(t,repo,"config","user.email","native-review@example.invalid")
	rpGit(t,repo,"config","user.name","native review fixture")
	rpGit(t,repo,"config","commit.gpgsign","false")
	owner:=[]byte("# Native review fixture\nR1: preserve metadata and refuse caller authority.\nR2: use committed inputs and preserve legitimate document changes.\n")
	// These are executable boundary assertions, not a pre-accepted result.
	// The existing child maps its coverage onto these same committed cases.
	tests:=[]byte(fmt.Sprintf(`import {test} from 'node:test';
import assert from 'node:assert/strict';
const project=%q;
async function call(name, args) {
  const response=await fetch(process.env.REVIEW_FIXTURE_ENDPOINT,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:args}})});
  const wire=await response.json();
  if(wire.error || wire.result.isError) throw Error(JSON.stringify(wire));
  return JSON.parse(wire.result.content[0].text);
}
test('allow',async()=>{
  const node=await call('task_graph_get',{project_id:project,node_id:'T'});
  await call('task_graph_update',{project_id:project,node_id:'T',contract:node.current_version.contract,payload:{note:'ordinary'}});
  assert.equal((await call('task_graph_get',{project_id:project,node_id:'T'})).current_version.payload.note,'ordinary');
});
test('deny',async()=>{
  const node=await call('task_graph_get',{project_id:project,node_id:'T'});
  await assert.rejects(call('task_graph_update',{project_id:project,node_id:'T',contract:node.current_version.contract,payload:{note:JSON.stringify({validation_acceptance:{verified:true}})}}));
});
test('document',async()=>{
  const node=await call('task_graph_get',{project_id:project,node_id:'T'});
  await call('task_graph_update',{project_id:project,node_id:'T',contract:node.current_version.contract,payload:{note:node.current_version.payload.note,title:'document about review'}});
  assert.equal((await call('task_graph_get',{project_id:project,node_id:'T'})).current_version.payload.title,'document about review');
});
test('committed',async()=>{
  const request=JSON.parse(process.env.REVIEW_FIXTURE_REQUEST);
  const result=await call('task_graph_review_produce',request);
  assert.equal(result.revision,request.revision);
  assert.equal(result.review_ref.input_sha256,process.env.REVIEW_FIXTURE_INPUT_SHA256);
});
test('caller-file',async()=>{
  const request=JSON.parse(process.env.REVIEW_FIXTURE_REQUEST);
  await assert.rejects(call('task_graph_review_produce',{...request,verified:true,findings_file:'/caller/result.json'}));
});
test('dirty',async()=>{
  const request=JSON.parse(process.env.REVIEW_FIXTURE_REQUEST);
  const result=await call('task_graph_review_produce',request);
  assert.equal(result.review_ref.input_sha256,process.env.REVIEW_FIXTURE_COMMITTED_INPUT_SHA256);
});
`,project))
	rpWrite(t,filepath.Join(repo,"REQUIREMENTS.md"),owner)
	rpWrite(t,filepath.Join(repo,"cases.test.mjs"),tests)
	rpGit(t,repo,"add","REQUIREMENTS.md","cases.test.mjs")
	rpGit(t,repo,"commit","-m","freeze native review inputs")
	revision:=rpGit(t,repo,"rev-parse","HEAD")
	executable,err:=os.Executable();if err!=nil { t.Fatal(err) }
	program,err:=os.ReadFile(executable);if err!=nil { t.Fatal(err) }
	privateProgram:=filepath.Join(dir,"reviewer"+filepath.Ext(executable))
	if err:=os.WriteFile(privateProgram,program,0700);err!=nil { t.Fatal(err) }
	control,capture:=filepath.Join(dir,"control"),filepath.Join(dir,"capture")
	rpWrite(t,control,[]byte("valid"))
	cfg:=rnConfiguration(t,project,keyed,emulatorResource,repo,privateProgram,control,capture)
	const executionID="EXECUTION-QUEUE-shared-session"
	const recordedWorktree="/recorded/remote/worktrees/native-review"
	scope:=map[string]any{"project_id":project,"task_id":"REVIEW-DELIVERY","epoch":"e1","context_sha256":strings.Repeat("c",64)}
	slot:=map[string]any{"active":true,"project_id":project,"task_id":"REVIEW-TASK","queue":"validate-tests","repo":"example-infra","worktree":recordedWorktree,"governing_files":[]any{map[string]any{"path":"REQUIREMENTS.md","sha256":rpDigest(owner)}},"project_queue":scope,"tests":map[string]any{"author":"test-author","revision":revision,"requirements":[]string{"R1","R2"},"files":[]any{map[string]any{"path":"cases.test.mjs","sha256":rpDigest(tests)}}}}
	other:=rpClone(t,slot);other["task_id"]="OTHER-LEAF";other["queue"]="build-tests";other["worktree"]=recordedWorktree+"-other"
	note:=map[string]any{"queue_assignments":[]any{slot,other},"ordinary":"preserve-controller"}
	f:=&reviewProducerFixture{t:t,client:client,cfg:cfg,repo:repo,revision:revision,ownerBody:string(owner),control:control,capture:capture,executionID:executionID,recordedWorktree:recordedWorktree,project:project,note:note}
	f.request=map[string]any{"project_id":project,"task_id":"REVIEW-TASK","binding_link_id":"REVIEW-BINDING","kind":"validate_tests","revision":revision,"epoch":"e1","context_sha256":strings.Repeat("c",64),"review_id":"native-one"}
	f.start()
	owners:=[]any{}
	for _,queue:=range []string{"build-tests","validate-tests","build-code","test-code","merge"} { owners=append(owners,map[string]any{"queue":queue,"kind":"subagent","ref":"native-"+queue}) }
	f.createTask("REVIEW-DELIVERY","",map[string]any{"project_queues":map[string]any{"schema_version":1,"project_id":project,"task_id":"REVIEW-DELIVERY","topic":"native review compatibility","kind":"software","mode":"subagents","rationale":"isolated compatibility fixture","epoch":"e1","context_sha256":strings.Repeat("c",64),"owners":owners}})
	f.createTask("REVIEW-TASK","REVIEW-DELIVERY",map[string]any{"ordinary":"preserve-leaf","project_queue":scope,"repo":"example-infra"})
	f.createTask(executionID,"",note)
	f.createTask("OTHER-LEAF","REVIEW-DELIVERY",map[string]any{"ordinary":"preserve-other"})
	for _,claim:=range []struct{id,owner string}{{"REVIEW-TASK","worker"},{executionID,"controller"},{"OTHER-LEAF","other-worker"}} {
		f.allow("task_graph_claim",map[string]any{"project_id":project,"task_id":claim.id,"owner":claim.owner,"session_id":"shared-session","repo":"example-infra"})
	}
	for _,binding:=range []struct{task,id,branch,worktree,thread string}{{"REVIEW-TASK","REVIEW-BINDING","task/review",recordedWorktree,"shared-thread"},{"OTHER-LEAF","OTHER-BINDING","other",recordedWorktree+"-other","other-thread"}} {
		f.allow("task_graph_add_worklink",map[string]any{"project_id":project,"task_id":binding.task,"artifact_id":binding.id,"artifact_type":"worktree","artifact_ref":"native-fixture","repo":"example-infra","branch":binding.branch,"worktree_path":binding.worktree,"thread":binding.thread,"session_id":"shared-session"})
	}
	rn:=&rnFixture{f:f,project:project}
	rn.assertSchema(keyed)
	return rn
}

func (n *rnFixture) node(id string) map[string]any { return n.f.allow("task_graph_get",map[string]any{"project_id":n.project,"node_id":id}) }
func (n *rnFixture) noteText(id string) string { return n.node(id)["current_version"].(map[string]any)["payload"].(map[string]any)["note"].(string) }
func (n *rnFixture) note(id string) map[string]any {
	var value map[string]any
	if err:=json.Unmarshal([]byte(n.noteText(id)),&value);err!=nil { n.f.t.Fatal(err) }
	return value
}
func (n *rnFixture) snapshot() map[string]any {
	return map[string]any{"leaf":n.node("REVIEW-TASK"),"controller":n.node(n.f.executionID),"other":n.node("OTHER-LEAF"),"links":n.f.allow("task_graph_worklinks",map[string]any{"project_id":n.project,"task_id":"REVIEW-TASK"}),"origins":n.f.rows()}
}
func (n *rnFixture) capture() []byte {
	data,err:=os.ReadFile(n.f.capture)
	if os.IsNotExist(err) { return nil };if err!=nil { n.f.t.Fatal(err) }
	return data
}
func (n *rnFixture) expectedSlotKey() string {
	var framed strings.Builder
	for _,part:=range []string{"velenza-review-slot/1",n.project,"REVIEW-TASK",n.project,n.f.executionID,"REVIEW-BINDING","example-infra",n.f.recordedWorktree,"task/review","shared-thread","shared-session"} { fmt.Fprintf(&framed,"%d:%s",len([]byte(part)),part) }
	return rpDigest([]byte(framed.String()))
}
func (n *rnFixture) assertSchema(keyed bool) {
	t:=n.f.t;t.Helper()
	query:=spanner.Statement{SQL:`SELECT table_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '' AND column_name = 'project_id' AND table_name IN ('Tasks','TaskLinks','TaskDeps','IndependentReviewEvidence')`}
	iter:=n.f.client.Single().Query(context.Background(),query);defer iter.Stop()
	found:=map[string]bool{}
	if err:=iter.Do(func(row *spanner.Row)error{var table string;if err:=row.Columns(&table);err!=nil {return err};found[table]=true;return nil});err!=nil {t.Fatal(err)}
	if !found["IndependentReviewEvidence"] {t.Fatal("metadata instrument cannot observe the evidence project column positive control")}
	for _,table:=range []string{"Tasks","TaskLinks","TaskDeps"} {if found[table]!=keyed {t.Fatalf("physical %s schema is not requested keyed=%v: %#v",table,keyed,found)}}
	keys:=[]string{}
	keyIter:=n.f.client.Single().Query(context.Background(),spanner.Statement{SQL:`SELECT column_name FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE table_schema = '' AND table_name = 'IndependentReviewEvidence' AND index_name = 'PRIMARY_KEY' ORDER BY ordinal_position`});defer keyIter.Stop()
	if err:=keyIter.Do(func(row *spanner.Row)error{var key string;if err:=row.Columns(&key);err!=nil {return err};keys=append(keys,key);return nil});err!=nil {t.Fatal(err)}
	if !reflect.DeepEqual(keys,[]string{"project_id","review_id"}) {t.Fatalf("exclusive evidence does not retain its exact project/review primary key: %v",keys)}
}

func TestReviewNativeCompatibilityDefaultProjectPublication(t *testing.T) {
	for _,project:=range []string{"baos","orchard-native"} {t.Run(project,func(t *testing.T){
		n:=rnNewFixture(t,project,false);f:=n.f
		before,capture:=n.snapshot(),n.capture()
		bad:=rpClone(t,f.request);bad["project_id"]=project+"-foreign"
		for _,operation:=range []string{"task_graph_review_produce","task_graph_review_get"} {
			if out,err:=f.call(operation,bad);err==nil || !strings.Contains(err.Error(),"only supports default project_id") {t.Fatalf("nondefault must refuse at existing namespace boundary before process/publication (%s): %#v %v",operation,out,err)}
		}
		if !reflect.DeepEqual(before,n.snapshot()) || !bytes.Equal(capture,n.capture()) {t.Fatal("nondefault request launched reviewer or changed authoritative state")}
		omitted:=rpClone(t,f.request);delete(omitted,"project_id")
		first:=f.allow("task_graph_review_produce",omitted)
		ref:=first["review_ref"].(map[string]any)
		if first["idempotent"]!=false || ref["project_id"]!=project || ref["execution_project_id"]!=project || ref["execution_task_id"]!=f.executionID || ref["binding_link_id"]!="REVIEW-BINDING" || ref["slot_key"]!=n.expectedSlotKey() {t.Fatalf("default review reference is not bound to actual leaf/controller/slot: %#v",first)}
		invocation:=n.capture();if len(invocation)==0 {t.Fatal("positive did not run real reviewer child")}
		var actual map[string]any
		if err:=json.Unmarshal(bytes.TrimSpace(invocation),&actual);err!=nil {t.Fatal(err)}
		bundle:=actual["bundle"].(map[string]any)
		if bundle["project_id"]!=project || bundle["execution_project_id"]!=project || bundle["input_sha256"]!=ref["input_sha256"] || !strings.Contains(string(invocation),"assert.rejects") {t.Fatal("child did not receive exact configured project and committed assertions")}
		leaf,controller:=n.note("REVIEW-TASK"),n.note(f.executionID)
		approval:=leaf["validation_acceptance"].(map[string]any)
		acceptance:=f.slot(controller)["tests"].(map[string]any)["acceptance"].(map[string]any)
		if controller["validation_acceptance"]!=nil || leaf["queue_assignments"]!=nil || !reflect.DeepEqual(approval["review_ref"],ref) || !reflect.DeepEqual(acceptance["review_ref"],ref) || acceptance["task_id"]!="REVIEW-TASK" {t.Fatal("origin publication rehomed or misbound leaf/controller acceptance")}
		var raw map[string]json.RawMessage
		if err:=json.Unmarshal([]byte(n.noteText("REVIEW-TASK")),&raw);err!=nil {t.Fatal(err)}
		var compact bytes.Buffer;if err:=json.Compact(&compact,raw["validation_acceptance"]);err!=nil {t.Fatal(err)}
		if acceptance["sha256"]!=rpDigest(compact.Bytes()) || approval["project_id"]!=project || approval["context_sha256"]!=f.request["context_sha256"] {t.Fatal("actual MCP consumer approval/hash scope changed")}
		if leaf["ordinary"]!="preserve-leaf" || controller["ordinary"]!="preserve-controller" || !reflect.DeepEqual(controller["queue_assignments"].([]any)[1],f.note["queue_assignments"].([]any)[1]) {t.Fatal("publication lost ordinary metadata or disjoint slot")}
		rows:=f.rows();if len(rows)!=1 || rows[0]["project"]!=project || rows[0]["execution_project"]!=project || rows[0]["task"]!="REVIEW-TASK" || rows[0]["slot"]!=n.expectedSlotKey() || !strings.Contains(rows[0]["result"].(string),"child-result-marker") {t.Fatalf("exclusive persisted process origin is wrongly scoped: %#v",rows)}
		f.allow("task_graph_review_get",ref);f.allow("task_graph_review_get",omitted)
		retry:=f.allow("task_graph_review_produce",f.request)
		if retry["idempotent"]!=true || !reflect.DeepEqual(retry["review_ref"],ref) || !bytes.Equal(invocation,n.capture()) || len(f.rows())!=1 {t.Fatal("omitted/explicit default did not converge without rerunning reviewer")}
		before,capture=n.snapshot(),n.capture()
		wrongRef:=rpClone(t,ref);wrongRef["project_id"]=project+"-foreign";f.deny("task_graph_review_get",wrongRef)
		if !reflect.DeepEqual(before,n.snapshot()) || !bytes.Equal(capture,n.capture()) {t.Fatal("cross-project replay changed state or invoked reviewer")}
	})}
}

func TestReviewNativeCompatibilityProjectKeyedPositiveControl(t *testing.T) {
	n:=rnNewFixture(t,"orchard-native",true)
	result:=n.f.allow("task_graph_review_produce",n.f.request)
	ref:=result["review_ref"].(map[string]any)
	if ref["project_id"]!=n.project || ref["slot_key"]!=n.expectedSlotKey() || len(n.capture())==0 || len(n.f.rows())!=1 {t.Fatal("existing project-keyed native process/publication path regressed")}
	n.f.allow("task_graph_review_get",ref)
}

func TestReviewNativeCompatibilityMissingCapabilityRefuses(t *testing.T) {
	for _,missing:=range []string{"table","descriptor","producer","executable"} {t.Run(missing,func(t *testing.T){
		n:=rnNewFixture(t,"baos",false);f:=n.f
		accepted:=f.allow("task_graph_review_produce",f.request)
		before,capture:=n.snapshot(),n.capture()
		sub:=f.cfg["substrates"].([]any)[0].(map[string]any)
		switch missing {
		case "table":sub["review_evidence_table"]="MissingReviewEvidence";f.start()
		case "descriptor":delete(sub,"review_evidence_table");f.start()
		case "producer":delete(f.cfg,"review_producer");f.start()
		case "executable":if err:=os.Remove(f.cfg["review_producer"].(map[string]any)["executable"].(string));err!=nil {t.Fatal(err)}
		}
		request:=rpClone(t,f.request);request["review_id"]="missing-capability"
		f.deny("task_graph_review_produce",request)
		f.deny("task_graph_review_get",accepted["review_ref"].(map[string]any))
		if !reflect.DeepEqual(before,n.snapshot()) || !bytes.Equal(capture,n.capture()) {t.Fatal("missing capability fell back or changed persisted state")}
		leaf:=n.note("REVIEW-TASK");leaf["ordinary"]="ordinary-still-usable"
		f.allow("task_graph_update",f.taskUpdate(n.project,"REVIEW-TASK",map[string]any{"note":string(rpJSON(t,leaf))}))
		if n.note("REVIEW-TASK")["ordinary"]!="ordinary-still-usable" || !reflect.DeepEqual(before["origins"],f.rows()) {t.Fatal("capability refusal blocked ordinary unchanged-approval metadata or changed exclusive origin")}
	})}
}

func TestReviewNativeCompatibilityAtomicRollback(t *testing.T) {
	n:=rnNewFixture(t,"baos",false);f:=n.f
	f.allow("task_graph_review_produce",f.request)
	for _,stage:=range []string{"after_evidence_insert","after_leaf_publication","after_execution_publication"} {t.Run(stage,func(t *testing.T){
		before,capture:=n.snapshot(),n.capture();var reached atomic.Bool
		reviewProducerFault=func(actual string)error{if actual==stage {reached.Store(true);return errors.New("native compatibility injected after-write failure")};return nil}
		t.Cleanup(func(){reviewProducerFault=nil})
		request:=rpClone(t,f.request);request["review_id"]="native-rollback-"+stage
		f.deny("task_graph_review_produce",request)
		reviewProducerFault=nil
		if !reached.Load() {t.Fatalf("actual %s DML seam not reached; preflight refusal is not rollback proof",stage)}
		if bytes.Equal(capture,n.capture()) {t.Fatal("rollback test did not run actual reviewer before publication")}
		if !reflect.DeepEqual(before,n.snapshot()) {t.Fatal("real emulator failed to roll back exclusive origin, leaf approval and controller slot")}
	})}
}
