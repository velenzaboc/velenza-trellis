package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	dbadminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/spannertest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This is candidate test DDL, not an instruction to mutate the live substrate.
// Both test tiers use the same dedicated table and the real native MCP server.
const reviewProducerTestDDL = `CREATE TABLE IndependentReviewEvidence (
	project_id STRING(64) NOT NULL,
	review_id STRING(64) NOT NULL,
	task_id STRING(64) NOT NULL,
	slot_id STRING(128) NOT NULL,
	kind STRING(32) NOT NULL,
	input_sha256 STRING(64) NOT NULL,
	binding_json STRING(MAX) NOT NULL,
	result_json STRING(MAX) NOT NULL,
	producer_generation STRING(128) NOT NULL,
	created_at TIMESTAMP NOT NULL,
) PRIMARY KEY (project_id, review_id)`

type reviewProducerFixture struct {
	t *testing.T
	client *spanner.Client
	session *mcp.ClientSession
	cfg map[string]any
	repo, revision, control, capture string
	note map[string]any
	request map[string]any
}

func rpJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil { t.Fatal(err) }
	return b
}

func rpDigest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func rpWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil { t.Fatal(err) }
}

func rpGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil { t.Fatalf("fixture git %v: %v: %s", args, err, out) }
	return strings.TrimSpace(string(out))
}

// The fixture seeds only ordinary task/source coordinates. It NEVER inserts a
// review-origin row or a protected acceptance; only the real producer may do so.
func rpSourceFixture(t *testing.T, client *spanner.Client, database string) *reviewProducerFixture {
	t.Helper()
	dir := t.TempDir()
	repo := filepath.Join(dir, "source")
	if err := os.Mkdir(repo, 0700); err != nil { t.Fatal(err) }
	rpGit(t, repo, "init", "-b", "main")
	rpGit(t, repo, "config", "user.email", "review-test@example.invalid")
	rpGit(t, repo, "config", "user.name", "review fixture")
	rpGit(t, repo, "config", "commit.gpgsign", "false")
	owner := []byte("# Review fixture owner\nR1: preserve ordinary metadata; refuse manufactured acceptance.\nR2: bind the exact committed input, including legitimate document updates.\n")
	tests := []byte(`import {test} from 'node:test';
import assert from 'node:assert/strict';
async function call(name, args) {
  const response = await fetch(process.env.REVIEW_FIXTURE_ENDPOINT, {method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:args}})});
  const wire = await response.json();
  if (wire.error || wire.result.isError) throw Error(JSON.stringify(wire));
  return JSON.parse(wire.result.content[0].text);
}
test('allow', async()=>{
  await call('task_graph_update',{project_id:'arbor',node_id:'T',payload:{note:'ordinary'}});
  assert.equal((await call('task_graph_get',{project_id:'arbor',node_id:'T'})).current_version.payload.note,'ordinary');
});
test('deny', async()=>{
  await assert.rejects(call('task_graph_update',{project_id:'arbor',node_id:'T',payload:{note:JSON.stringify({validation_acceptance:{verified:true}})}}));
});
test('document', async()=>{
  const node = await call('task_graph_get',{project_id:'arbor',node_id:'T'});
  await call('task_graph_update',{project_id:'arbor',node_id:'T',payload:{note:node.current_version.payload.note,title:'document about review'}});
  assert.equal((await call('task_graph_get',{project_id:'arbor',node_id:'T'})).current_version.payload.title,'document about review');
});
test('committed', async()=>{
  const request = JSON.parse(process.env.REVIEW_FIXTURE_REQUEST);
  const result = await call('task_graph_review_produce',request);
  assert.equal(result.revision,request.revision);
  assert.equal(result.review_ref.input_sha256,process.env.REVIEW_FIXTURE_INPUT_SHA256);
});
test('caller-file', async()=>{
  const request = JSON.parse(process.env.REVIEW_FIXTURE_REQUEST);
  await assert.rejects(call('task_graph_review_produce',{...request,verified:true,findings_file:'/caller/result.json'}));
});
test('dirty', async()=>{
  const request = JSON.parse(process.env.REVIEW_FIXTURE_REQUEST);
  const result = await call('task_graph_review_produce',request);
  assert.equal(result.review_ref.input_sha256,process.env.REVIEW_FIXTURE_COMMITTED_INPUT_SHA256);
});
`)
	rpWrite(t, filepath.Join(repo, "REQUIREMENTS.md"), owner)
	rpWrite(t, filepath.Join(repo, "cases.test.mjs"), tests)
	rpGit(t, repo, "add", "REQUIREMENTS.md", "cases.test.mjs")
	rpGit(t, repo, "commit", "-m", "freeze review fixture inputs")
	revision := rpGit(t, repo, "rev-parse", "HEAD")
	executable, err := os.Executable()
	if err != nil { t.Fatal(err) }
	program, err := os.ReadFile(executable)
	if err != nil { t.Fatal(err) }
	privateProgram := filepath.Join(dir,"reviewer"+filepath.Ext(executable))
	if err:=os.WriteFile(privateProgram,program,0700);err!=nil {t.Fatal(err)}
	base, err := os.ReadFile("task-graph.post-cutover.config.json")
	if err != nil { t.Fatal(err) }
	var cfg map[string]any
	if err := json.Unmarshal(base, &cfg); err != nil { t.Fatal(err) }
	cfg["embedding"] = nil
	cfg["allow_spanner_emulator"] = true
	substrates := cfg["substrates"].([]any)
	substrate := substrates[0].(map[string]any)
	substrate["spanner_database"] = database
	substrate["review_evidence_table"] = "IndependentReviewEvidence"
	control, capture := filepath.Join(dir, "process-control"), filepath.Join(dir, "process-capture")
	rpWrite(t, control, []byte("valid"))
	cfg["review_producer"] = map[string]any{
		"generation": "review-tests-e1", "executable": privateProgram,
		"executable_sha256": rpDigest(program),
		"args": []string{"-test.run=^TestReviewProducerChild$", "--", "review-process-child", control, capture, "--tools", ""},
		"timeout_milliseconds": 1000, "max_output_bytes": 65536,
		"repositories": []any{map[string]any{"repo": "example-infra", "path": repo, "requirements_path": "REQUIREMENTS.md"}},
	}
	slot := map[string]any{
		"slot_id": "slot-a", "project_id": "arbor", "task_id": "REVIEW-TASK",
		"queue": "build-code", "owner": "worker", "session_id": "shared-session",
		"repo": "example-infra", "worktree": repo,
		"project_queue": map[string]any{"project_id": "arbor", "task_id": "REVIEW-DELIVERY", "epoch": "e1", "context_sha256": strings.Repeat("c",64)},
		"tests": map[string]any{"author": "test-author", "revision": revision, "requirements": []string{"R1","R2"},
			"files": []any{map[string]any{"path":"cases.test.mjs", "sha256":rpDigest(tests)}, map[string]any{"path":"REQUIREMENTS.md", "sha256":rpDigest(owner)}}},
	}
	note := map[string]any{"queue_assignments": []any{slot}, "ordinary": "keep"}
	f := &reviewProducerFixture{t:t, client:client, cfg:cfg, repo:repo, revision:revision, control:control, capture:capture, note:note}
	f.request = map[string]any{"project_id":"arbor", "task_id":"REVIEW-TASK", "slot_id":"slot-a", "binding_link_id":"REVIEW-BINDING", "kind":"validate_tests", "revision":revision, "epoch":"e1", "context_sha256":strings.Repeat("c",64), "review_id":"review-one"}
	f.start()
	f.allow("task_graph_create", map[string]any{"project_id":"arbor", "node_kind":"task", "stable_id":"REVIEW-TASK", "contract":map[string]any{"canonical_name":"REVIEW-TASK","task_ref":"REVIEW-TASK","level":"task"}, "payload":map[string]any{"title":"review test", "pillar":"TEST", "status":"not_started", "owner":"UNOWNED", "is_gap":false, "note":string(rpJSON(t,note))}})
	f.allow("task_graph_claim", map[string]any{"project_id":"arbor","task_id":"REVIEW-TASK","owner":"worker","session_id":"shared-session","repo":"example-infra"})
	f.allow("task_graph_add_worklink", map[string]any{"project_id":"arbor","task_id":"REVIEW-TASK","artifact_id":"REVIEW-BINDING","artifact_type":"worktree","artifact_ref":"fixture", "repo":"example-infra","branch":"main","worktree_path":repo,"thread":"shared-thread","session_id":"shared-session"})
	return f
}

func newReviewProducerFixture(t *testing.T) *reviewProducerFixture {
	t.Helper()
	srv, err := spannertest.NewServer("127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	srv.SetLogger(func(string,...interface{}){})
	t.Cleanup(srv.Close)
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { t.Fatal(err) }
	t.Cleanup(func(){ _ = conn.Close() })
	client, err := spanner.NewClient(context.Background(), nativeCoverageDatabase, option.WithGRPCConn(conn))
	if err != nil { t.Fatal(err) }
	t.Cleanup(client.Close)
	admin, err := dbadmin.NewDatabaseAdminClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil { t.Fatal(err) }
	t.Cleanup(func(){ _ = admin.Close() })
	schema := append(append([]string{},nativeCoverageSchema...),reviewProducerTestDDL)
	op, err := admin.UpdateDatabaseDdl(context.Background(), &dbadminpb.UpdateDatabaseDdlRequest{Database:nativeCoverageDatabase,Statements:schema})
	if err != nil { t.Fatal(err) }
	if err := op.Wait(context.Background()); err != nil { t.Fatal(err) }
	t.Setenv("SPANNER_EMULATOR_HOST",srv.Addr)
	return rpSourceFixture(t,client,nativeCoverageDatabase)
}

func (f *reviewProducerFixture) start() {
	t := f.t
	listener, err := net.Listen("tcp","127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	address := listener.Addr().String()
	if err := listener.Close(); err != nil { t.Fatal(err) }
	f.cfg["listen_address"] = address
	configPath := filepath.Join(t.TempDir(),"review-server.json")
	rpWrite(t,configPath,rpJSON(t,f.cfg))
	t.Setenv("TASK_GRAPH_CONFIG",configPath)
	ctx,cancel := context.WithCancel(context.Background())
	stopped := make(chan error,1)
	go func(){ stopped <- cmdMCP(ctx,nil) }()
	t.Cleanup(func(){ cancel(); select { case err := <-stopped: if err != nil {t.Errorf("server stop: %v",err)}; case <-time.After(3*time.Second):t.Error("server did not stop") } })
	endpoint := "http://"+address
	deadline := time.Now().Add(3*time.Second)
	for {
		resp,err := (&http.Client{Timeout:100*time.Millisecond}).Get(endpoint+"/healthz")
		if err == nil { _ = resp.Body.Close(); if resp.StatusCode == 200 {break} }
		select { case err := <-stopped:t.Fatalf("server startup: %v",err); default: }
		if time.Now().After(deadline) {t.Fatal("server readiness timeout")}
		time.Sleep(10*time.Millisecond)
	}
	client := mcp.NewClient(&mcp.Implementation{Name:"review-contract-test",Version:"e1"},nil)
	session,err := client.Connect(context.Background(),&mcp.StreamableClientTransport{Endpoint:endpoint},nil)
	if err != nil {t.Fatal(err)}
	f.session = session
	t.Cleanup(func(){ _ = session.Close() })
}

func (f *reviewProducerFixture) call(name string,args map[string]any) (map[string]any,error) {
	ctx,cancel := context.WithTimeout(context.Background(),10*time.Second)
	defer cancel()
	result,err := f.session.CallTool(ctx,&mcp.CallToolParams{Name:name,Arguments:args})
	if err != nil {return nil,err}
	if result == nil || len(result.Content) != 1 {return nil,fmt.Errorf("invalid tool result: %#v",result)}
	text,ok := result.Content[0].(*mcp.TextContent)
	if !ok {return nil,fmt.Errorf("non-text result")}
	if result.IsError {return nil,fmt.Errorf("%s",text.Text)}
	var out map[string]any
	if err := json.Unmarshal([]byte(text.Text),&out); err != nil {return nil,err}
	return out,nil
}

func (f *reviewProducerFixture) allow(name string,args map[string]any) map[string]any {
	f.t.Helper()
	out,err := f.call(name,args)
	if err != nil {f.t.Fatalf("%s must succeed at real MCP handler: %v",name,err)}
	return out
}

func (f *reviewProducerFixture) deny(name string,args map[string]any) {
	f.t.Helper()
	if out,err := f.call(name,args); err == nil {f.t.Fatalf("%s manufactured authority: %#v",name,out)}
}

func rpClone(t *testing.T,in map[string]any) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rpJSON(t,in),&out); err != nil {t.Fatal(err)}
	return out
}

func (f *reviewProducerFixture) rows() []map[string]any {
	f.t.Helper()
	iter := f.client.Single().Read(context.Background(),"IndependentReviewEvidence",spanner.AllKeys(),[]string{"project_id","review_id","task_id","slot_id","kind","input_sha256","binding_json","result_json","producer_generation"})
	defer iter.Stop()
	var rows []map[string]any
	if err := iter.Do(func(row *spanner.Row)error{ values:=make([]string,9); pointers:=make([]any,9); for i:=range values {pointers[i]=&values[i]}; if err:=row.Columns(pointers...);err!=nil{return err}; rows=append(rows,map[string]any{"project":values[0],"id":values[1],"task":values[2],"slot":values[3],"kind":values[4],"input":values[5],"binding":values[6],"result":values[7],"producer":values[8]});return nil });err!=nil {f.t.Fatal(err)}
	return rows
}

func (f *reviewProducerFixture) taskNote() map[string]any {
	node:=f.allow("task_graph_get",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK"})
	payload:=node["current_version"].(map[string]any)["payload"].(map[string]any)
	var note map[string]any
	if err:=json.Unmarshal([]byte(payload["note"].(string)),&note);err!=nil {f.t.Fatal(err)}
	return note
}

func (f *reviewProducerFixture) updateNote(note map[string]any) {
	f.allow("task_graph_update",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK","payload":map[string]any{"note":string(rpJSON(f.t,note))}})
}

func TestReviewProducerRequiresRealProcessAndImmutableSource(t *testing.T) {
	f:=newReviewProducerFixture(t)
	// Dirty local content is not the authoritative source at the pinned commit.
	rpWrite(t,filepath.Join(f.repo,"cases.test.mjs"),[]byte("CALLER-CONTROLLED-DIRTY-BYTES"))
	out:=f.allow("task_graph_review_produce",f.request)
	if out["review_id"]!="review-one" || out["kind"]!="validate_tests" {t.Fatalf("wrong typed result: %#v",out)}
	captured,err:=os.ReadFile(f.capture)
	if err!=nil {t.Fatalf("real server child did not run: %v",err)}
	if strings.Contains(string(captured),"CALLER-CONTROLLED-DIRTY-BYTES") || !strings.Contains(string(captured),"assert.rejects") || !strings.Contains(string(captured),"R2") {t.Fatalf("process did not receive exact committed owner/tests: %s",captured)}
	rows:=f.rows()
	if len(rows)!=1 || rows[0]["project"]!="arbor" || rows[0]["task"]!="REVIEW-TASK" || rows[0]["slot"]!="slot-a" || rows[0]["producer"]!="review-tests-e1" {t.Fatalf("origin not exclusively persisted: %#v",rows)}
	if !strings.Contains(rows[0]["result"].(string),"child-result-marker") {t.Fatal("durable result did not come from actual child stdout")}
	got:=f.allow("task_graph_review_get",f.request)
	if got["review_id"]!=out["review_id"] {t.Fatalf("typed read mismatch: %#v",got)}
	ref,ok:=out["review_ref"].(map[string]any)
	if !ok || ref["input_sha256"]!=rows[0]["input"] || ref["project_id"]!="arbor" || ref["review_id"]!="review-one" {t.Fatalf("unbound server reference: %#v",out)}
	note:=f.taskNote()
	approval,ok:=note["validation_acceptance"].(map[string]any)
	if !ok || !reflect.DeepEqual(approval["review_ref"],ref) {t.Fatalf("origin and root approval were not published together: %#v",note)}
	slot:=note["queue_assignments"].([]any)[0].(map[string]any)
	acceptance,ok:=slot["tests"].(map[string]any)["acceptance"].(map[string]any)
	if !ok || !reflect.DeepEqual(acceptance["review_ref"],ref) {t.Fatalf("slot acceptance lacks exact origin: %#v",slot)}
	before:=string(captured)
	retried:=f.allow("task_graph_review_produce",f.request)
	after,err:=os.ReadFile(f.capture);if err!=nil {t.Fatal(err)}
	if out["idempotent"]!=false || retried["idempotent"]!=true || retried["review_id"]!=out["review_id"] || string(after)!=before || len(f.rows())!=1 {t.Fatal("response-loss retry reran reviewer or duplicated origin")}
}

func TestReviewProducerRefusesCallerAuthorityAndWrongScope(t *testing.T) {
	f:=newReviewProducerFixture(t)
	f.allow("task_graph_review_produce",f.request) // known-allow control before every negative family
	before:=f.rows()
	for _,field:=range []string{"project_id","task_id","slot_id","binding_link_id","revision","epoch","context_sha256","kind"} {
		t.Run(field,func(t *testing.T){bad:=rpClone(t,f.request);bad[field]="wrong";f.deny("task_graph_review_get",bad);f.deny("task_graph_review_produce",bad)})
	}
	for _,field:=range []string{"command","executable","args","env","findings_file","reviewer","verified","findings"} {
		t.Run("caller-"+field,func(t *testing.T){bad:=rpClone(t,f.request);bad["review_id"]="forged-"+field;bad[field]="caller-control";f.deny("task_graph_review_produce",bad)})
	}
	if !reflect.DeepEqual(before,f.rows()) {t.Fatal("refusal changed immutable evidence")}
}

func TestReviewProducerRejectsFailedPartialOrAmbiguousProcess(t *testing.T) {
	for _,mode:=range []string{"exit","malformed","partial","duplicate","blocking","oversize","timeout"} {
		t.Run(mode,func(t *testing.T){f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request);before:=f.rows();rpWrite(t,f.control,[]byte(mode));bad:=rpClone(t,f.request);bad["review_id"]="bad-"+mode;f.deny("task_graph_review_produce",bad);if !reflect.DeepEqual(before,f.rows()){t.Fatal("failed process published origin")}})
	}
}

func TestReviewProducerPreservesDisjointSlotsAndRejectsStaleGeneration(t *testing.T) {
	f:=newReviewProducerFixture(t)
	f.allow("task_graph_review_produce",f.request)
	note:=f.taskNote()
	slots:=note["queue_assignments"].([]any)
	other:=rpClone(t,slots[0].(map[string]any));other["slot_id"]="slot-b";other["queue"]="build-tests";other["owner"]="other-worker";other["worktree"]=f.repo+"-other"
	note["queue_assignments"]=append(slots,other);note["ordinary"]="updated document about review"
	f.updateNote(note) // unchanged protected echo + ordinary metadata is allowed
	got:=f.taskNote();if got["ordinary"]!="updated document about review" || len(got["queue_assignments"].([]any))!=2 {t.Fatal("unrelated metadata/slot was lost")}
	slots=got["queue_assignments"].([]any);slot:=slots[0].(map[string]any);slot["project_queue"].(map[string]any)["context_sha256"]=strings.Repeat("d",64)
	f.updateNote(got)
	f.deny("task_graph_review_get",f.request)
	bad:=rpClone(t,f.request);bad["review_id"]="stale-publication";f.deny("task_graph_review_produce",bad)
	if len(f.rows())!=1 {t.Fatal("stale generation published a new result")}
}

func TestReviewProducerGenericMutationCannotManufactureAcceptance(t *testing.T) {
	paths:=[][]string{{"validation_acceptance"},{"tests","acceptance"},{"acceptance"},{"tdd_red_receipt"},{"tests","tdd_audit_receipt"},{"tdd_green_receipt"},{"reopen_history"}}
	for _,legacy:=range []bool{false,true} {for _,path:=range paths {t.Run(fmt.Sprintf("legacy=%t/%s",legacy,strings.Join(path,".")),func(t *testing.T){
		f:=newReviewProducerFixture(t)
		// Ordinary bookkeeping/prose must remain usable without running a review.
		note:=rpClone(t,f.note);note["ordinary"]="review is a word, not acceptance";f.updateNote(note)
		if legacy {note["queue_assignment"]=note["queue_assignments"].([]any)[0];delete(note,"queue_assignments");f.updateNote(note)}
		forged:=rpClone(t,note)
		target:=forged
		if path[0]!="validation_acceptance" {if legacy {target=forged["queue_assignment"].(map[string]any)} else {target=forged["queue_assignments"].([]any)[0].(map[string]any)}}
		for _,part:=range path[:len(path)-1] {next,ok:=target[part].(map[string]any);if !ok {next=map[string]any{};target[part]=next};target=next}
		target[path[len(path)-1]]=map[string]any{"verified":true,"reviewer":"different-string","blocking":0,"task_id":"forged","sha256":strings.Repeat("a",64)}
		before:=f.allow("task_graph_get",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK"})
		f.deny("task_graph_update",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK","payload":map[string]any{"note":string(rpJSON(t,forged)),"title":"must roll back too"}})
		if !reflect.DeepEqual(before,f.allow("task_graph_get",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK"})){t.Fatal("forgery partially mutated task fields")}
		create:=map[string]any{"project_id":"arbor","node_kind":"task","stable_id":"FORGED-TASK","contract":map[string]any{"canonical_name":"FORGED-TASK","task_ref":"FORGED-TASK","level":"task"},"payload":map[string]any{"title":"forgery","pillar":"TEST","status":"not_started","owner":"UNOWNED","is_gap":false,"note":string(rpJSON(t,forged))}}
		f.deny("task_graph_create",create)
		f.deny("task_graph_import",map[string]any{"submissions":[]any{map[string]any{"node_kind":"task","stable_id":"FORGED-IMPORT","contract":create["contract"],"payload":create["payload"]}}})
		if len(f.rows())!=0 {t.Fatal("generic mutation minted origin")}
	})}}
}

func TestReviewProducerConcurrentExactRetryHasOneOrigin(t *testing.T) {
	f:=newReviewProducerFixture(t)
	// First prove the real producer boundary exists. Then use one fresh ID race.
	f.allow("task_graph_review_produce",f.request)
	in:=rpClone(t,f.request);in["review_id"]="concurrent"
	var wg sync.WaitGroup
	results:=make(chan error,2)
	for range 2 {wg.Add(1);go func(){defer wg.Done();_,err:=f.call("task_graph_review_produce",in);results<-err}()}
	wg.Wait();close(results);for err:=range results {if err!=nil {t.Fatalf("identical concurrent request did not converge: %v",err)}}
	if len(f.rows())!=2 {t.Fatal("concurrent retry duplicated or lost origin")}
}

func TestReviewProducerLegacySlotAndDeletionCannotEraseOrigin(t *testing.T) {
	for _,legacy:=range []bool{false,true} {
		t.Run(fmt.Sprintf("legacy=%t",legacy),func(t *testing.T){
			f:=newReviewProducerFixture(t)
			if legacy {note:=rpClone(t,f.note);note["queue_assignment"]=note["queue_assignments"].([]any)[0];delete(note,"queue_assignments");f.updateNote(note)}
			f.allow("task_graph_review_produce",f.request)
			for _,erase:=range []string{"root-approval","slot-approval","whole-slot"} {
				t.Run(erase,func(t *testing.T){before:=f.taskNote();note:=rpClone(t,before)
					var slot map[string]any;if legacy {slot=note["queue_assignment"].(map[string]any)} else {slot=note["queue_assignments"].([]any)[0].(map[string]any)}
					switch erase {case "root-approval":delete(note,"validation_acceptance");case "slot-approval":delete(slot["tests"].(map[string]any),"acceptance");case "whole-slot":delete(note,"queue_assignments");delete(note,"queue_assignment")}
					f.deny("task_graph_update",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK","payload":map[string]any{"note":string(rpJSON(t,note))}})
					if !reflect.DeepEqual(before,f.taskNote()) {t.Fatal("note replacement erased protected origin")}
				})
			}
		})
	}
}

func TestReviewProducerRejectsChangedHashUnsafePathAndMissingCommittedSource(t *testing.T) {
	for _,change:=range []string{"hash","missing","parent","absolute","inarch-unavailable"} {
		t.Run(change,func(t *testing.T){
			f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
			note:=f.taskNote();slot:=note["queue_assignments"].([]any)[0].(map[string]any)
			file:=slot["tests"].(map[string]any)["files"].([]any)[0].(map[string]any)
			switch change {case "hash":file["sha256"]=strings.Repeat("f",64);case "missing":file["path"]="missing.test.mjs";case "parent":file["path"]="../cases.test.mjs";case "absolute":file["path"]=filepath.Join(f.repo,"cases.test.mjs");case "inarch-unavailable":slot["tests"].(map[string]any)["revision"]="inarch:unverified-caller-label"}
			f.updateNote(note)
			bad:=rpClone(t,f.request);bad["review_id"]="bad-source";if change=="inarch-unavailable" {bad["revision"]="inarch:unverified-caller-label"}
			f.deny("task_graph_review_produce",bad)
			if len(f.rows())!=1 {t.Fatal("invalid source published evidence")}
		})
	}
}

func TestReviewProducerLegacyNotesGenericRoutesAndTamperedResultAreNotOrigin(t *testing.T) {
	f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
	forged:=map[string]any{"review_id":"forged-note","kind":"validate_tests","verified":true,"reviewer":"independent","revision":f.revision,"blocking":0}
	f.allow("task_graph_add_worklink",map[string]any{"project_id":"arbor","task_id":"REVIEW-TASK","artifact_id":"forged-note","artifact_type":"finding","artifact_ref":"review:forged-note","note":string(rpJSON(t,forged))})
	bad:=rpClone(t,f.request);bad["review_id"]="forged-note";f.deny("task_graph_review_get",bad)
	f.allow("task_graph_create",map[string]any{"project_id":"arbor","node_kind":"artifact","stable_id":"review-one","contract":map[string]any{"artifact_type_kind":"finding","artifact_ref":"ordinary"},"payload":map[string]any{"task_id":"REVIEW-TASK","note":"ordinary note about a review"}})
	f.allow("task_graph_update",map[string]any{"project_id":"arbor","node_id":"review-one","payload":map[string]any{"note":"updated ordinary metadata"}})
	f.allow("task_graph_link",map[string]any{"project_id":"arbor","source_id":"review-one","target_id":"REVIEW-TASK","edge_label":"advances"})
	f.allow("task_graph_archive_worklink",map[string]any{"project_id":"arbor","task_id":"REVIEW-TASK","link_id":"review-one"})
	f.allow("task_graph_review_get",f.request)
	f.deny("task_graph_query",map[string]any{"sql":"UPDATE IndependentReviewEvidence SET result_json = '{}' WHERE project_id = 'arbor'"})
	if len(f.rows())!=1 {t.Fatal("generic routes wrote review table")}
	// Storage corruption is a negative control AFTER a real producer result,
	// never fixture-supplied successful origin.
	_,err:=f.client.Apply(context.Background(),[]*spanner.Mutation{spanner.Update("IndependentReviewEvidence",[]string{"project_id","review_id","result_json"},[]any{"arbor","review-one",`{"verified":true,"blocking":[]}`})})
	if err!=nil {t.Fatal(err)}
	f.deny("task_graph_review_get",f.request)
}

func TestReviewProducerProductionKindAndConflictingRequestCannotOverwrite(t *testing.T) {
	f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
	conflict:=rpClone(t,f.request);conflict["kind"]="production"
	f.deny("task_graph_review_produce",conflict)
	conflict["review_id"]="production-one"
	out:=f.allow("task_graph_review_produce",conflict)
	if out["kind"]!="production" {t.Fatalf("kind substitution: %#v",out)}
	f.allow("task_graph_review_get",conflict)
	slot:=f.taskNote()["queue_assignments"].([]any)[0].(map[string]any)
	acceptance,ok:=slot["acceptance"].(map[string]any)
	if !ok || !reflect.DeepEqual(acceptance["review_ref"],out["review_ref"]) {t.Fatalf("production publication missing exact origin: %#v",slot)}
	if len(f.rows())!=2 {t.Fatal("production result lost or replaced earlier review")}
}

func TestReviewProducerValidShapedStoredTamperingRefuses(t *testing.T) {
	for _,column:=range []string{"binding_json","input_sha256","producer_generation","kind","slot_id","task_id","result_json"} {
		t.Run(column,func(t *testing.T){
			f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
			row:=f.rows()[0]
			value:="wrong-but-shaped"
			switch column {case "input_sha256":value=strings.Repeat("9",64);case "kind":value="production";case "binding_json":var binding map[string]any;if err:=json.Unmarshal([]byte(row["binding"].(string)),&binding);err!=nil {t.Fatal(err)};binding["context_sha256"]=strings.Repeat("9",64);value=string(rpJSON(t,binding));case "result_json":var result map[string]any;if err:=json.Unmarshal([]byte(row["result"].(string)),&result);err!=nil {t.Fatal(err)};result["input_sha256"]=strings.Repeat("9",64);value=string(rpJSON(t,result))}
			_,err:=f.client.Apply(context.Background(),[]*spanner.Mutation{spanner.Update("IndependentReviewEvidence",[]string{"project_id","review_id",column},[]any{"arbor","review-one",value})})
			if err!=nil {t.Fatal(err)}
			f.deny("task_graph_review_get",f.request)
		})
	}
}

func rpRequireAtomicRollback(t *testing.T,f *reviewProducerFixture) {
	t.Helper()
	f.allow("task_graph_review_produce",f.request)
	for _,stage:=range []string{"after_evidence_insert","after_publication"} {
		t.Run(stage,func(t *testing.T){
			beforeRows,beforeNote:=f.rows(),f.taskNote()
			reached:=false
			reviewProducerFault=func(actual string)error{if actual==stage {reached=true;return errors.New("injected review publication failure")};return nil}
			t.Cleanup(func(){reviewProducerFault=nil})
			in:=rpClone(t,f.request);in["review_id"]="rollback-"+stage
			f.deny("task_graph_review_produce",in)
			reviewProducerFault=nil
			if !reached {t.Fatalf("required %s seam was never reached; preflight refusal is not rollback proof",stage)}
			if !reflect.DeepEqual(beforeRows,f.rows()) || !reflect.DeepEqual(beforeNote,f.taskNote()) {t.Fatal("evidence/publication failed to roll back atomically")}
		})
	}
}

func TestReviewProducerAtomicPublicationRollback(t *testing.T) {
	rpRequireAtomicRollback(t,newReviewProducerFixture(t))
}

func TestReviewProducerMissingCapabilityAndChangedPinnedExecutableRefuse(t *testing.T) {
	for _,missing:=range []string{"producer","descriptor","table","executable","changed-executable"} {
		t.Run(missing,func(t *testing.T){
			f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
			before:=f.taskNote()
			config:=f.cfg["review_producer"].(map[string]any)
			substrate:=f.cfg["substrates"].([]any)[0].(map[string]any)
			switch missing {
			case "producer":delete(f.cfg,"review_producer");f.start()
			case "descriptor":delete(substrate,"review_evidence_table");f.start()
			case "table":substrate["review_evidence_table"]="MissingReviewTable";f.start()
			case "executable":if err:=os.Remove(config["executable"].(string));err!=nil {t.Fatal(err)}
			case "changed-executable":file,err:=os.OpenFile(config["executable"].(string),os.O_APPEND|os.O_WRONLY,0700);if err!=nil {t.Fatal(err)};_,err=file.WriteString("pin-changed");_ = file.Close();if err!=nil {t.Fatal(err)}
			}
			in:=rpClone(t,f.request);in["review_id"]="missing-capability"
			f.deny("task_graph_review_produce",in)
			if missing=="producer" || missing=="descriptor" || missing=="table" {f.deny("task_graph_review_get",f.request)}
			if !reflect.DeepEqual(before,f.taskNote()) || len(f.rows())!=1 {t.Fatal("missing capability changed publication")}
		})
	}
}

func rpWaitCaptures(t *testing.T,path string,want int) {
	t.Helper()
	deadline:=time.Now().Add(3*time.Second)
	for {data,err:=os.ReadFile(path);if err==nil && strings.Count(string(data),"\n")>=want {return};if time.Now().After(deadline){t.Fatalf("actual reviewer never captured input %d",want)};time.Sleep(10*time.Millisecond)}
}

func TestReviewProducerExternalProcessDoesNotHoldPublicationTransaction(t *testing.T) {
	f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
	f.cfg["review_producer"].(map[string]any)["timeout_milliseconds"]=5000;f.start()
	rpWrite(t,f.control,[]byte("hold"))
	in:=rpClone(t,f.request);in["review_id"]="metadata-race"
	done:=make(chan error,1);go func(){_,err:=f.call("task_graph_review_produce",in);done<-err}()
	rpWaitCaptures(t,f.capture,2)
	note:=f.taskNote();note["ordinary"]="concurrent metadata survives"
	slots:=note["queue_assignments"].([]any);other:=rpClone(t,slots[0].(map[string]any));other["slot_id"]="slot-b";other["worktree"]=f.repo+"-b";delete(other["tests"].(map[string]any),"acceptance");note["queue_assignments"]=append(slots,other)
	f.updateNote(note)
	rpWrite(t,f.control,[]byte("valid"))
	if err:=<-done;err!=nil {t.Fatalf("unrelated metadata rejected publication: %v",err)}
	got:=f.taskNote();if got["ordinary"]!="concurrent metadata survives" || len(got["queue_assignments"].([]any))!=2 {t.Fatal("publication overwrote concurrent metadata/disjoint slot")}
	data,err:=os.ReadFile(f.capture);if err!=nil {t.Fatal(err)};if strings.Count(string(data),"\n")!=2 {t.Fatal("transaction retry reran reviewer")}
	// A changed protected input during a later real process must instead refuse.
	rpWrite(t,f.control,[]byte("hold"));in["review_id"]="protected-race"
	go func(){_,err:=f.call("task_graph_review_produce",in);done<-err}()
	rpWaitCaptures(t,f.capture,3)
	got=f.taskNote();got["queue_assignments"].([]any)[0].(map[string]any)["project_queue"].(map[string]any)["context_sha256"]=strings.Repeat("e",64);f.updateNote(got)
	rpWrite(t,f.control,[]byte("valid"))
	if err:=<-done;err==nil {t.Fatal("changed protected generation was published")}
	if len(f.rows())!=2 {t.Fatal("stale in-flight review minted origin")}
}

// Executed ONLY when the production runner starts this test executable with
// fixed administrator argv. Parent tests never import its result as authority.
func TestReviewProducerChild(t *testing.T) {
	marker:=-1;for i,arg:=range os.Args {if arg=="review-process-child" {marker=i;break}}
	if marker<0 {return}
	if len(os.Args)!=marker+5 || os.Args[marker+3]!="--tools" || os.Args[marker+4]!="" {os.Exit(31)}
	control,capture:=os.Args[marker+1],os.Args[marker+2]
	input,err:=io.ReadAll(os.Stdin);if err!=nil {os.Exit(32)}
	var bundle map[string]any
	if err:=json.Unmarshal(input,&bundle);err!=nil || bundle["input_sha256"]==nil {os.Exit(33)}
	file,err:=os.OpenFile(capture,os.O_APPEND|os.O_CREATE|os.O_WRONLY,0600);if err!=nil {os.Exit(34)}
	captured,err:=json.Marshal(map[string]any{"pid":os.Getpid(),"argv":os.Args[marker+3:],"bundle":json.RawMessage(input)});if err!=nil {os.Exit(37)}
	_,_ = file.Write(append(captured,'\n'));_ = file.Close()
	modeBytes,err:=os.ReadFile(control);if err!=nil {os.Exit(35)}
	for string(modeBytes)=="hold" {time.Sleep(10*time.Millisecond);modeBytes,err=os.ReadFile(control);if err!=nil {os.Exit(35)}}
	switch string(modeBytes) {case "exit":fmt.Fprintln(os.Stderr,"controlled child failure");os.Exit(9);case "timeout":time.Sleep(10*time.Second);case "malformed":fmt.Print("not JSON");os.Exit(0);case "oversize":fmt.Print(strings.Repeat("x",70000));os.Exit(0)}
	coverage:=[]any{}
	for _,requirement:=range []string{"R1","R2"} {for _,scenario:=range []string{"positive","refusal","exception"} {
		cases:=map[string]string{"positive":"allow","refusal":"deny","exception":"document"}
		assertions:=map[string]string{"positive":"payload.note,'ordinary'","refusal":"validation_acceptance:{verified:true}","exception":"payload.title,'document about review'"}
		if requirement=="R2" {cases=map[string]string{"positive":"committed","refusal":"caller-file","exception":"dirty"};assertions=map[string]string{"positive":"assert.equal(result.revision,request.revision)","refusal":"findings_file:'/caller/result.json'","exception":"REVIEW_FIXTURE_COMMITTED_INPUT_SHA256"}}
		coverage=append(coverage,map[string]any{"requirement":requirement,"path":"cases.test.mjs","case":cases[scenario],"assertion":assertions[scenario],"boundary":"native MCP transaction","scenario":scenario})
	}}
	if string(modeBytes)=="partial" {coverage=coverage[:1]}
	blocking:=[]any{};if string(modeBytes)=="blocking" {blocking=append(blocking,map[string]any{"id":"R1","reason":"controlled blocking finding"})}
	output:=map[string]any{"schema_version":"velenza_independent_review/1","kind":bundle["kind"],"input_sha256":bundle["input_sha256"],"requirements":[]string{"R1","R2"},"coverage":coverage,"blocking":blocking,"production_boundary":true,"tautologies":false,"fixture_supplied_authority":false,"summary":"child-result-marker"}
	b,err:=json.Marshal(output);if err!=nil {os.Exit(36)}
	if string(modeBytes)=="duplicate" {b=[]byte(strings.Replace(string(b),`"kind":"validate_tests"`,`"kind":"production","kind":"validate_tests"`,1))}
	_,_ = os.Stdout.Write(b)
	os.Exit(0)
}
