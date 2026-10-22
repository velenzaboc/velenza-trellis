package main

import (
	"bytes"
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
	execution_project_id STRING(64) NOT NULL,
	execution_task_id STRING(128) NOT NULL,
	slot_key STRING(64) NOT NULL,
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
	repo, revision, head, productionBody, ownerBody, control, capture string
	executionID, recordedWorktree string
	project string
	// Record only contracts actually supplied to successful native Task creates,
	// keyed by exact project and ID; never synthesize a missing update identity.
	taskContracts map[[2]string]map[string]any
	// The in-memory SQL fake cannot infer timestamp CAST projections. Only
	// its observers read simple persisted columns; real-tier observers use MCP.
	fakeObserver bool
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
	return rpSourceFixtureForProject(t,client,database,"arbor")
}

func rpSourceFixtureForProject(t *testing.T, client *spanner.Client, database, project string) *reviewProducerFixture {
	t.Helper()
	dir := t.TempDir()
	repo := filepath.Join(dir, "source")
	if err := os.Mkdir(repo, 0700); err != nil { t.Fatal(err) }
	rpGit(t, repo, "init", "-b", "task/review")
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
  const node = await call('task_graph_get',{project_id:'arbor',node_id:'T'});
  await call('task_graph_update',{project_id:'arbor',node_id:'T',contract:node.current_version.contract,payload:{note:'ordinary'}});
  assert.equal((await call('task_graph_get',{project_id:'arbor',node_id:'T'})).current_version.payload.note,'ordinary');
});
test('deny', async()=>{
  const node = await call('task_graph_get',{project_id:'arbor',node_id:'T'});
  await assert.rejects(call('task_graph_update',{project_id:'arbor',node_id:'T',contract:node.current_version.contract,payload:{note:JSON.stringify({validation_acceptance:{verified:true}})}}));
});
test('document', async()=>{
  const node = await call('task_graph_get',{project_id:'arbor',node_id:'T'});
  await call('task_graph_update',{project_id:'arbor',node_id:'T',contract:node.current_version.contract,payload:{note:node.current_version.payload.note,title:'document about review'}});
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
	rpWrite(t, filepath.Join(repo, "operation.mjs"), []byte("export function requireReviewOrigin(value) { return Boolean(value.verified); }\n"))
	rpWrite(t, filepath.Join(repo, "REQUIREMENTS.md"), owner)
	rpWrite(t, filepath.Join(repo, "cases.test.mjs"), tests)
	rpGit(t, repo, "add", "REQUIREMENTS.md", "cases.test.mjs", "operation.mjs")
	rpGit(t, repo, "commit", "-m", "freeze review fixture inputs")
	revision := rpGit(t, repo, "rev-parse", "HEAD")
	productionBody := "export function requireReviewOrigin(value) {\n  if (!value.review_ref || value.review_ref.kind !== 'production') throw Error('origin required');\n  return value.review_ref;\n}\n"
	rpWrite(t, filepath.Join(repo, "operation.mjs"), []byte(productionBody))
	rpGit(t, repo, "add", "operation.mjs")
	rpGit(t, repo, "commit", "-m", "require actual production origin")
	head := rpGit(t, repo, "rev-parse", "HEAD")
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
		"generation": "review-tests-e2", "executable": privateProgram,
		"executable_sha256": rpDigest(program),
		"args": []string{"-test.run=^TestReviewProducerChild$", "--", "review-process-child", control, capture, "--tools", ""},
		"timeout_milliseconds": 1000, "max_output_bytes": 65536,
		"repositories": []any{map[string]any{"repo": "example-infra", "path": repo, "requirements_path": "REQUIREMENTS.md"}},
	}
	const executionID = "EXECUTION-QUEUE-shared-session"
	// The recorded physical path belongs to a different worker coordinate.
	// Source acquisition is the administrator's local Git checkout, not this path.
	const recordedWorktree = "/recorded/remote/worktrees/review-task"
	scope := map[string]any{"project_id": project, "task_id": "REVIEW-DELIVERY", "epoch": "e1", "context_sha256": strings.Repeat("c",64)}
	slot := map[string]any{
		"active": true, "project_id": project, "task_id": "REVIEW-TASK",
		"queue": "build-code", "repo": "example-infra", "worktree": recordedWorktree,
		"revision": head, "base_revision": revision,
		"production_files": []any{map[string]any{"path":"operation.mjs", "sha256":rpDigest([]byte(productionBody))}},
		"governing_files": []any{map[string]any{"path":"REQUIREMENTS.md", "sha256":rpDigest(owner)}},
		"project_queue": scope,
		"tests": map[string]any{"author": "test-author", "revision": revision, "requirements": []string{"R1","R2"},
			"files": []any{map[string]any{"path":"cases.test.mjs", "sha256":rpDigest(tests)}}},
	}
	note := map[string]any{"queue_assignments": []any{slot}, "ordinary": "keep-controller"}
	f := &reviewProducerFixture{t:t, client:client, cfg:cfg, repo:repo, revision:revision, head:head, productionBody:productionBody, ownerBody:string(owner), control:control, capture:capture, note:note, executionID:executionID, recordedWorktree:recordedWorktree, project:project}
	f.request = map[string]any{"project_id":project, "task_id":"REVIEW-TASK", "execution_task_id":executionID, "binding_link_id":"REVIEW-BINDING", "kind":"validate_tests", "revision":revision, "epoch":"e1", "context_sha256":strings.Repeat("c",64), "review_id":"review-one"}
	f.start()
	owners:=[]any{}
	for _,queue:=range []string{"build-tests","validate-tests","build-code","test-code","merge"} {owners=append(owners,map[string]any{"queue":queue,"kind":"subagent","ref":"fixture-"+queue})}
	f.createTask("REVIEW-DELIVERY", "", map[string]any{"project_queues":map[string]any{"schema_version":1,"project_id":project,"task_id":"REVIEW-DELIVERY","topic":"review fixture","kind":"software","mode":"subagents","rationale":"isolated contract fixture","epoch":"e1","context_sha256":strings.Repeat("c",64),"owners":owners}})
	f.createTask("REVIEW-TASK", "REVIEW-DELIVERY", map[string]any{"ordinary":"keep-leaf","repo":"example-infra","project_queue":scope})
	f.createTaskIn("arbor",executionID, "", note)
	f.allow("task_graph_claim", map[string]any{"project_id":project,"task_id":"REVIEW-TASK","owner":"worker","session_id":"shared-session","repo":"example-infra"})
	f.allow("task_graph_claim", map[string]any{"project_id":"arbor","task_id":executionID,"owner":"controller","session_id":"shared-session","repo":"example-infra"})
	f.allow("task_graph_add_worklink", map[string]any{"project_id":project,"task_id":"REVIEW-TASK","artifact_id":"REVIEW-BINDING","artifact_type":"worktree","artifact_ref":"fixture", "repo":"example-infra","branch":"task/review","worktree_path":recordedWorktree,"thread":"shared-thread","session_id":"shared-session"})
	return f
}

func (f *reviewProducerFixture) createTask(id,parent string,note map[string]any) {
	f.createTaskIn(f.project,id,parent,note)
}

func (f *reviewProducerFixture) createTaskIn(project,id,parent string,note map[string]any) {
	payload:=map[string]any{"title":id,"pillar":"TEST","status":"not_started","owner":"UNOWNED","is_gap":false,"note":string(rpJSON(f.t,note))}
	if parent!="" {payload["parent_id"]=parent}
	contract:=map[string]any{"canonical_name":id,"task_ref":id,"level":"task"}
	f.allow("task_graph_create",map[string]any{"project_id":project,"node_kind":"task","stable_id":id,"contract":contract,"payload":payload})
	if f.taskContracts==nil {f.taskContracts=map[[2]string]map[string]any{}}
	f.taskContracts[[2]string{project,id}]=rpClone(f.t,contract)
}

func (f *reviewProducerFixture) taskUpdate(project,id string,payload map[string]any) map[string]any {
	f.t.Helper()
	contract,ok:=f.taskContracts[[2]string{project,id}]
	if !ok {f.t.Fatalf("fixture update has no successful creation contract for project %q Task %q",project,id)}
	return map[string]any{"project_id":project,"node_id":id,"contract":rpClone(f.t,contract),"payload":payload}
}

func newReviewProducerFixture(t *testing.T) *reviewProducerFixture {
	return newReviewProducerFixtureForProject(t,"arbor")
}

func newReviewProducerFixtureForProject(t *testing.T,project string) *reviewProducerFixture {
	t.Helper()
	srv, err := spannertest.NewServer("127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	srv.SetLogger(func(string,...interface{}){})
	t.Cleanup(srv.Close)
	rpOwnEmulatorEndpoint(t,srv.Addr)
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
	t.Setenv("SPANNER_EMULATOR_HOST","localhost:9010")
	fixture:=rpSourceFixtureForProject(t,client,nativeCoverageDatabase,project)
	fixture.fakeObserver=true
	return fixture
}

// The REAL cmdMCP client uses the unchanged allowed localhost:9010 authority.
// Acquire every resolved loopback address exclusively and proxy to this test's
// private backend. Never connect to, kill or reuse a pre-existing port owner.
func rpOwnEmulatorEndpoint(t *testing.T,backend string) {
	t.Helper()
	ips,err:=net.LookupIP("localhost");if err!=nil {t.Fatalf("fixture environment: localhost resolution: %v",err)}
	seen:=map[string]bool{}
	listeners:=[]net.Listener{}
	t.Cleanup(func(){for _,listener:=range listeners {_ = listener.Close()}})
	for _,ip:=range ips {
		if !ip.IsLoopback() {t.Fatalf("fixture environment: localhost resolved outside loopback: %s",ip)}
		address:=net.JoinHostPort(ip.String(),"9010");if seen[address] {continue};seen[address]=true
		listener,err:=net.Listen("tcp",address)
		if err!=nil {for _,owned:=range listeners {_ = owned.Close()};t.Fatalf("fixture environment: cannot exclusively own %s: %v",address,err)}
		listeners=append(listeners,listener)
	}
	if len(listeners)==0 {t.Fatal("fixture environment: localhost resolved no listener addresses")}
	var mu sync.Mutex
	closing:=false
	connections:=map[net.Conn]bool{}
	var wg sync.WaitGroup
	t.Cleanup(func(){
		for _,listener:=range listeners {_ = listener.Close()}
		mu.Lock();closing=true;for conn:=range connections {_ = conn.Close()};mu.Unlock()
		wg.Wait()
	})
	for _,listener:=range listeners {wg.Add(1);go func(listener net.Listener){defer wg.Done();for {
		incoming,err:=listener.Accept();if err!=nil {return}
		outgoing,err:=net.DialTimeout("tcp",backend,time.Second);if err!=nil {_ = incoming.Close();t.Errorf("owned emulator proxy backend: %v",err);continue}
		mu.Lock();if closing {_ = incoming.Close();_ = outgoing.Close();mu.Unlock();return};connections[incoming]=true;connections[outgoing]=true;mu.Unlock()
		wg.Add(1);go func(){defer wg.Done();done:=make(chan struct{});go func(){_,_ = io.Copy(outgoing,incoming);_ = incoming.Close();_ = outgoing.Close();close(done)}();_,_ = io.Copy(incoming,outgoing);_ = incoming.Close();_ = outgoing.Close();<-done;mu.Lock();delete(connections,incoming);delete(connections,outgoing);mu.Unlock()}()
	}}(listener)}
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
	iter := f.client.Single().Read(context.Background(),"IndependentReviewEvidence",spanner.AllKeys(),[]string{"project_id","review_id","task_id","execution_project_id","execution_task_id","slot_key","kind","input_sha256","binding_json","result_json","producer_generation"})
	defer iter.Stop()
	var rows []map[string]any
	if err := iter.Do(func(row *spanner.Row)error{ values:=make([]string,11); pointers:=make([]any,11); for i:=range values {pointers[i]=&values[i]}; if err:=row.Columns(pointers...);err!=nil{return err}; rows=append(rows,map[string]any{"project":values[0],"id":values[1],"task":values[2],"execution_project":values[3],"execution_task":values[4],"slot":values[5],"kind":values[6],"input":values[7],"binding":values[8],"result":values[9],"producer":values[10]});return nil });err!=nil {f.t.Fatal(err)}
	return rows
}

func (f *reviewProducerFixture) noteFor(taskID string) map[string]any {
	var note map[string]any
	if err:=json.Unmarshal([]byte(f.noteTextFor(taskID)),&note);err!=nil {f.t.Fatal(err)}
	return note
}

func (f *reviewProducerFixture) noteTextFor(taskID string) string {
	project:=f.project;if taskID==f.executionID {project="arbor"}
	if f.fakeObserver {
		ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel()
		row,err:=f.client.Single().ReadRow(ctx,"Tasks",spanner.Key{project,taskID},[]string{"note"})
		if err!=nil {f.t.Fatal(err)}
		var note spanner.NullString
		if err:=row.Columns(&note);err!=nil {f.t.Fatal(err)}
		return nullStringValue(note)
	}
	node:=f.allow("task_graph_get",map[string]any{"project_id":project,"node_id":taskID})
	payload:=node["current_version"].(map[string]any)["payload"].(map[string]any)
	return payload["note"].(string)
}

func (f *reviewProducerFixture) taskObservation(taskID string) map[string]any {
	project:=f.project;if taskID==f.executionID {project="arbor"}
	if !f.fakeObserver {return map[string]any{"observer":"actual-mcp-task-get","node":f.allow("task_graph_get",map[string]any{"project_id":project,"node_id":taskID})}}
	ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel()
	// Observe the complete persisted row, without pretending it is a generic
	// MCP projection. All mutations and producer/process calls still use MCP.
	iter:=f.client.Single().Query(ctx,spanner.Statement{SQL:`SELECT project_id,id,title,pillar,layer,status,owner,parent_id,is_gap,note,created_at,updated_at,gap_label,session_id,repo,is_critical_path,started_at,done_at FROM Tasks WHERE project_id = @project_id AND id = @id`,Params:map[string]any{"project_id":project,"id":taskID}})
	defer iter.Stop()
	result,err:=collectQueryResult(iter)
	if err!=nil {f.t.Fatal(err)}
	if len(result.Rows)!=1 {f.t.Fatalf("persisted Task observer expected one row: %#v",result)}
	return map[string]any{"observer":"actual-persisted-task-row","row":result.Rows[0]}
}

func (f *reviewProducerFixture) taskNote() map[string]any { return f.noteFor(f.executionID) }
func (f *reviewProducerFixture) leafNote() map[string]any { return f.noteFor("REVIEW-TASK") }

func (f *reviewProducerFixture) updateNoteFor(taskID string,note map[string]any) {
	project:=f.project;if taskID==f.executionID {project="arbor"}
	f.allow("task_graph_update",f.taskUpdate(project,taskID,map[string]any{"note":string(rpJSON(f.t,note))}))
}

func (f *reviewProducerFixture) updateNote(note map[string]any) {
	f.updateNoteFor(f.executionID,note)
}

func (f *reviewProducerFixture) slot(note map[string]any) map[string]any {
	if singleton,ok:=note["queue_assignment"].(map[string]any);ok {return singleton}
	return note["queue_assignments"].([]any)[0].(map[string]any)
}

func (f *reviewProducerFixture) expectedSlotKey() string {
	// Exact existing slot/binding identity. No slot_id is invented or written.
	fields:=[]string{"velenza-review-slot/1",f.project,"REVIEW-TASK","arbor",f.executionID,"REVIEW-BINDING","example-infra",f.recordedWorktree,"task/review","shared-thread","shared-session"}
	var framed strings.Builder
	for _,field:=range fields {fmt.Fprintf(&framed,"%d:%s",len([]byte(field)),field)}
	return rpDigest([]byte(framed.String()))
}

func (f *reviewProducerFixture) productionRequest(id string) map[string]any {
	in:=rpClone(f.t,f.request);in["review_id"]=id;in["kind"]="production";in["revision"]=f.head;in["base_revision"]=f.revision
	return in
}

func (f *reviewProducerFixture) disjointSlot() map[string]any {
	f.createTask("OTHER-LEAF","REVIEW-DELIVERY",map[string]any{"ordinary":"other leaf"})
	f.allow("task_graph_claim",map[string]any{"project_id":"arbor","task_id":"OTHER-LEAF","owner":"other-worker","session_id":"shared-session","repo":"example-infra"})
	f.allow("task_graph_add_worklink",map[string]any{"project_id":"arbor","task_id":"OTHER-LEAF","artifact_id":"OTHER-BINDING","artifact_type":"worktree","artifact_ref":"other-fixture","repo":"example-infra","branch":"other","worktree_path":f.recordedWorktree+"-other","thread":"other-thread","session_id":"shared-session"})
	other:=rpClone(f.t,f.slot(f.taskNote()));other["task_id"]="OTHER-LEAF";other["queue"]="build-tests";other["worktree"]=f.recordedWorktree+"-other";delete(other["tests"].(map[string]any),"acceptance")
	return other
}

func (f *reviewProducerFixture) denyOrigin(name string,args map[string]any) {
	f.t.Helper()
	if out,err:=f.call(name,args);err==nil {f.t.Fatalf("generic %s manufactured protected origin: %#v",name,out)} else if !strings.Contains(err.Error(),"REVIEW_ORIGIN_REQUIRED") {f.t.Fatalf("%s refused before origin-write boundary: %v",name,err)}
}

func TestReviewProducerRequiresRealProcessAndImmutableSource(t *testing.T) {
	f:=newReviewProducerFixture(t)
	// Dirty local content is not the authoritative source at the pinned commit.
	rpWrite(t,filepath.Join(f.repo,"cases.test.mjs"),[]byte("CALLER-CONTROLLED-DIRTY-BYTES"))
	out:=f.allow("task_graph_review_produce",f.request)
	if out["review_id"]!="review-one" || out["kind"]!="validate_tests" {t.Fatalf("wrong typed result: %#v",out)}
	captured,err:=os.ReadFile(f.capture)
	if err!=nil {t.Fatalf("real server child did not run: %v",err)}
	if strings.Contains(string(captured),"CALLER-CONTROLLED-DIRTY-BYTES") || !strings.Contains(string(captured),"assert.rejects") || !strings.Contains(string(captured),"R2") || !strings.Contains(string(captured),f.recordedWorktree) {t.Fatalf("process did not receive exact committed owner/tests/recorded binding: %s",captured)}
	var actual map[string]any;if err:=json.Unmarshal([]byte(strings.TrimSpace(string(captured))),&actual);err!=nil {t.Fatal(err)}
	governing:=actual["bundle"].(map[string]any)["governing_files"].([]any)
	if len(governing)!=1 || governing[0].(map[string]any)["content"]!=f.ownerBody || governing[0].(map[string]any)["sha256"]!=rpDigest([]byte(f.ownerBody)) {t.Fatal("full committed governing owner bytes/hash did not reach reviewer")}
	rows:=f.rows()
	if len(rows)!=1 || rows[0]["project"]!="arbor" || rows[0]["task"]!="REVIEW-TASK" || rows[0]["execution_project"]!="arbor" || rows[0]["execution_task"]!=f.executionID || rows[0]["slot"]!=f.expectedSlotKey() || rows[0]["producer"]!="review-tests-e2" {t.Fatalf("origin not exclusively persisted against both actual rows: %#v",rows)}
	if !strings.Contains(rows[0]["result"].(string),"child-result-marker") {t.Fatal("durable result did not come from actual child stdout")}
	got:=f.allow("task_graph_review_get",f.request)
	if got["review_id"]!=out["review_id"] {t.Fatalf("typed read mismatch: %#v",got)}
	ref,ok:=out["review_ref"].(map[string]any)
	if !ok || ref["input_sha256"]!=rows[0]["input"] || ref["project_id"]!="arbor" || ref["review_id"]!="review-one" || ref["execution_task_id"]!=f.executionID || ref["slot_key"]!=f.expectedSlotKey() {t.Fatalf("unbound server reference: %#v",out)}
	f.allow("task_graph_review_get",ref) // exact wire object that B consumers resolve
	leaf,controller:=f.leafNote(),f.taskNote()
	approval,ok:=leaf["validation_acceptance"].(map[string]any)
	if !ok || !reflect.DeepEqual(approval["review_ref"],ref) {t.Fatalf("origin and LEAF approval were not published together: %#v",leaf)}
	if _,exists:=controller["validation_acceptance"];exists {t.Fatal("leaf approval was incorrectly rehomed onto execution controller")}
	if _,exists:=leaf["queue_assignments"];exists {t.Fatal("execution slots were incorrectly rehomed onto leaf")}
	slot:=f.slot(controller)
	acceptance,ok:=slot["tests"].(map[string]any)["acceptance"].(map[string]any)
	if !ok || acceptance["task_id"]!="REVIEW-TASK" || !reflect.DeepEqual(acceptance["review_ref"],ref) {t.Fatalf("execution slot acceptance lacks exact LEAF origin: %#v",slot)}
	if approval["schema_version"]!="velenza_validation_acceptance/1" || approval["project_id"]!="arbor" || approval["task_id"]!="REVIEW-TASK" || approval["context_sha256"]!=f.request["context_sha256"] || approval["author"]!="test-author" || approval["reviewer"]==nil || approval["reviewer"]=="" || approval["reviewer"]==approval["author"] || approval["blocking"]!=float64(0) || approval["production_boundary"]!=true || approval["tautologies"]!=false || approval["fixture_supplied_authority"]!=false {t.Fatalf("leaf approval does not reach existing validation consumer: %#v",approval)}
	if !reflect.DeepEqual(approval["requirements"],slot["tests"].(map[string]any)["requirements"]) || len(approval["evidence"].([]any))==0 {t.Fatal("legacy validation requirement/evidence projection is incomplete")}
	// These fixture receipt fields are ASCII and contain no HTML escaping;
	// compacting the exact stored object preserves JSON.stringify's key order.
	storedNote:=f.noteTextFor("REVIEW-TASK")
	var rawNote map[string]json.RawMessage;if err:=json.Unmarshal([]byte(storedNote),&rawNote);err!=nil {t.Fatal(err)}
	var compact bytes.Buffer;if err:=json.Compact(&compact,rawNote["validation_acceptance"]);err!=nil {t.Fatal(err)}
	if acceptance["sha256"]!=rpDigest(compact.Bytes()) {t.Fatal("controller does not bind the separately stored leaf approval hash")}
	if _,exists:=slot["slot_id"];exists {t.Fatal("producer invented slot_id state")}
	if !reflect.DeepEqual(approval["files"],slot["tests"].(map[string]any)["files"]) {t.Fatal("approval changed frozen executable file set")}
	files:=approval["files"].([]any)
	if len(files)!=1 || files[0].(map[string]any)["path"]!="cases.test.mjs" {t.Fatal("governing owner was mislabelled as executable test")}
	for _,file:=range files {covered:=false;for _,item:=range approval["coverage"].([]any) {if item.(map[string]any)["path"]==file.(map[string]any)["path"] {covered=true}};if !covered {t.Fatal("a frozen executable test file has no coverage")}}
	before:=string(captured)
	retried:=f.allow("task_graph_review_produce",f.request)
	after,err:=os.ReadFile(f.capture);if err!=nil {t.Fatal(err)}
	if out["idempotent"]!=false || retried["idempotent"]!=true || retried["review_id"]!=out["review_id"] || string(after)!=before || len(f.rows())!=1 {t.Fatal("response-loss retry reran reviewer or duplicated origin")}
}

func TestReviewProducerRefusesCallerAuthorityAndWrongScope(t *testing.T) {
	f:=newReviewProducerFixture(t)
	f.allow("task_graph_review_produce",f.request) // known-allow control before every negative family
	before:=f.rows()
	for _,field:=range []string{"project_id","task_id","execution_project_id","execution_task_id","slot_id","slot_key","binding_link_id","revision","epoch","context_sha256","kind"} {
		t.Run(field,func(t *testing.T){bad:=rpClone(t,f.request);bad[field]="wrong";f.deny("task_graph_review_get",bad);f.deny("task_graph_review_produce",bad)})
	}
	ref:=f.allow("task_graph_review_get",f.request)["review_ref"].(map[string]any)
	f.allow("task_graph_review_get",ref)
	for _,field:=range []string{"input_sha256","producer_generation"} {bad:=rpClone(t,ref);bad[field]=strings.Repeat("a",64);f.deny("task_graph_review_get",bad)}
	for _,field:=range []string{"command","executable","args","env","findings_file","reviewer","verified","findings"} {
		t.Run("caller-"+field,func(t *testing.T){bad:=rpClone(t,f.request);bad["review_id"]="forged-"+field;bad[field]="caller-control";f.deny("task_graph_review_produce",bad)})
	}
	if !reflect.DeepEqual(before,f.rows()) {t.Fatal("refusal changed immutable evidence")}
}

func TestReviewProducerUsesActualControllerAndDerivedSlotIdentity(t *testing.T) {
	f:=newReviewProducerFixture(t)
	in:=rpClone(t,f.request);delete(in,"execution_task_id")
	out:=f.allow("task_graph_review_produce",in)
	ref:=out["review_ref"].(map[string]any)
	if ref["execution_task_id"]!=f.executionID || ref["execution_project_id"]!="arbor" || ref["slot_key"]!=f.expectedSlotKey() {t.Fatalf("controller/slot was not derived from actual binding: %#v",ref)}
	leaf:=f.leafNote()
	if leaf["validation_acceptance"]==nil || f.taskNote()["validation_acceptance"]!=nil {t.Fatal("approval published to wrong Task")}
	f.createTask("EXECUTION-QUEUE-other-session","",rpClone(t,f.note))
	f.allow("task_graph_claim",map[string]any{"project_id":"arbor","task_id":"EXECUTION-QUEUE-other-session","owner":"other-controller","session_id":"other-session","repo":"example-infra"})
	f.createTask("OTHER-LEAF","REVIEW-DELIVERY",map[string]any{"ordinary":"other"})
	for _,change:=range []string{"controller","leaf","invented-slot-id","wrong-slot-key"} {
		bad:=rpClone(t,f.request)
		switch change {case "controller":bad["execution_task_id"]="EXECUTION-QUEUE-other-session";case "leaf":bad["task_id"]="OTHER-LEAF";case "invented-slot-id":bad["slot_id"]="caller-created-slot";case "wrong-slot-key":bad["slot_key"]=strings.Repeat("a",64)}
		f.deny("task_graph_review_get",bad);bad["review_id"]="wrong-"+change;f.deny("task_graph_review_produce",bad)
	}
	if len(f.rows())!=1 {t.Fatal("wrong controller/leaf/slot created origin")}
}

func TestReviewProducerBindsDefaultControllerAndNondefaultLeafProjects(t *testing.T) {
	f:=newReviewProducerFixtureForProject(t,"leaf-project")
	out:=f.allow("task_graph_review_produce",f.request)
	ref:=out["review_ref"].(map[string]any)
	if ref["project_id"]!="leaf-project" || ref["execution_project_id"]!="arbor" || ref["execution_task_id"]!=f.executionID || ref["slot_key"]!=f.expectedSlotKey() {t.Fatalf("project namespaces were conflated: %#v",ref)}
	if f.leafNote()["validation_acceptance"]==nil || f.slot(f.taskNote())["tests"].(map[string]any)["acceptance"]==nil {t.Fatal("actual leaf/controller projection not published")}
	f.allow("task_graph_review_get",f.request)
	f.createTaskIn("arbor","REVIEW-TASK","",map[string]any{"ordinary":"same ID in another project"})
	wrong:=rpClone(t,f.request);wrong["project_id"]="arbor"
	f.deny("task_graph_review_get",wrong);wrong["review_id"]="wrong-project";f.deny("task_graph_review_produce",wrong)
	if len(f.rows())!=1 {t.Fatal("wrong leaf namespace created origin")}
}

func TestReviewProducerRefusesControllerSessionOrAmbiguousRecordedSelection(t *testing.T) {
	for _,change:=range []string{"session","duplicate","worktree","repository"} {t.Run(change,func(t *testing.T){
		f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
		// Negative-only mutation models contradictory pre-existing state. It
		// supplies no new origin or successful authorization.
		columns:=[]string{"project_id","id"};values:=[]any{"arbor",f.executionID}
		if change=="session" {columns=append(columns,"session_id");values=append(values,"other-session")} else {
			note:=f.taskNote();slot:=f.slot(note)
			switch change {case "duplicate":note["queue_assignments"]=append(note["queue_assignments"].([]any),rpClone(t,slot));case "worktree":slot["worktree"]="/different/recorded/worktree";case "repository":slot["repo"]="other-repository"}
			columns=append(columns,"note");values=append(values,string(rpJSON(t,note)))
		}
		if _,err:=f.client.Apply(context.Background(),[]*spanner.Mutation{spanner.Update("Tasks",columns,values)});err!=nil {t.Fatal(err)}
		f.deny("task_graph_review_get",f.request);bad:=rpClone(t,f.request);bad["review_id"]="contradictory";f.deny("task_graph_review_produce",bad)
		if len(f.rows())!=1 {t.Fatal("ambiguous/stale recorded identity minted origin")}
	})}
}

func TestReviewProducerValidatesAnExistingOptionalSlotID(t *testing.T) {
	f:=newReviewProducerFixture(t)
	note:=f.taskNote();f.slot(note)["slot_id"]="already-recorded-slot";f.updateNote(note)
	in:=rpClone(t,f.request);in["slot_id"]="already-recorded-slot"
	f.allow("task_graph_review_produce",in);f.allow("task_graph_review_get",in)
	if f.slot(f.taskNote())["slot_id"]!="already-recorded-slot" {t.Fatal("producer rewrote existing slot identity")}
	in["slot_id"]="different-slot";f.deny("task_graph_review_get",in)
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
	other:=f.disjointSlot()
	note["queue_assignments"]=append(slots,other);note["ordinary"]="updated document about review"
	f.updateNote(note) // unchanged protected echo + ordinary metadata is allowed
	leaf:=f.leafNote();leaf["ordinary"]="updated leaf document about review";f.updateNoteFor("REVIEW-TASK",leaf)
	got:=f.taskNote();if got["ordinary"]!="updated document about review" || len(got["queue_assignments"].([]any))!=2 || f.leafNote()["ordinary"]!="updated leaf document about review" {t.Fatal("unrelated metadata/slot in either row was lost")}
	slots=got["queue_assignments"].([]any);slot:=slots[0].(map[string]any);slot["project_queue"].(map[string]any)["context_sha256"]=strings.Repeat("d",64)
	f.updateNote(got)
	f.deny("task_graph_review_get",f.request)
	bad:=rpClone(t,f.request);bad["review_id"]="stale-publication";f.deny("task_graph_review_produce",bad)
	if len(f.rows())!=1 {t.Fatal("stale generation published a new result")}
}

func TestReviewProducerGenericMutationCannotManufactureAcceptance(t *testing.T) {
	paths:=[][]string{{"validation_acceptance"},{"tests","acceptance"},{"acceptance"},{"tdd_red_receipt"},{"tests","tdd_audit_receipt"},{"tdd_green_receipt"},{"reopen_history"}}
	for _,legacy:=range []bool{false,true} {t.Run(fmt.Sprintf("legacy=%t",legacy),func(t *testing.T){
		f:=newReviewProducerFixture(t)
		note:=rpClone(t,f.note);note["ordinary"]="review is a word, not acceptance";f.updateNote(note)
		if legacy {note["queue_assignment"]=note["queue_assignments"].([]any)[0];delete(note,"queue_assignments");f.updateNote(note)}
		f.allow("task_graph_review_produce",f.request)
		f.allow("task_graph_review_produce",f.productionRequest("production-old"))
		oldLeaf,oldController:=f.leafNote(),f.taskNote()
		// Produce a genuine newer generation so replay cases contain a real,
		// valid-shaped historical origin, not a fixture-invented authority row.
		controller:=f.taskNote();scope:=f.slot(controller)["project_queue"].(map[string]any);scope["epoch"]="e2";scope["context_sha256"]=strings.Repeat("d",64);f.updateNote(controller)
		leaf:=f.leafNote();leaf["project_queue"]=rpClone(t,scope);f.updateNoteFor("REVIEW-TASK",leaf)
		delivery:=f.noteFor("REVIEW-DELIVERY");plan:=delivery["project_queues"].(map[string]any);plan["epoch"]="e2";plan["context_sha256"]=strings.Repeat("d",64);f.updateNoteFor("REVIEW-DELIVERY",delivery)
		f.request["epoch"]="e2";f.request["context_sha256"]=strings.Repeat("d",64);f.request["review_id"]="review-new"
		f.allow("task_graph_review_produce",f.request);f.allow("task_graph_review_produce",f.productionRequest("production-new"))
		f.updateNote(f.taskNote());f.updateNoteFor("REVIEW-TASK",f.leafNote()) // unchanged protected echo positives on BOTH rows
		beforeRows:=f.rows()
		for _,path:=range paths {for _,mode:=range []string{"caller-authored","cross-bound","replayed"} {t.Run(strings.Join(path,".")+"/"+mode,func(t *testing.T){
			id:=f.executionID;original:=f.taskNote();if path[0]=="validation_acceptance" {id="REVIEW-TASK";original=f.leafNote()}
			forged:=rpClone(t,original);target:=forged;if path[0]!="validation_acceptance" {target=f.slot(forged)}
			for _,part:=range path[:len(path)-1] {target=target[part].(map[string]any)}
			target[path[len(path)-1]]=f.forgedProtectedValue(path,mode,oldLeaf,oldController)
			before:=f.taskObservation(id)
			f.denyOrigin("task_graph_update",f.taskUpdate("arbor",id,map[string]any{"note":string(rpJSON(t,forged)),"title":"must roll back too"}))
			if !reflect.DeepEqual(before,f.taskObservation(id)){t.Fatal("forgery partially mutated Task")}
			createID:="FORGED-TASK";if id==f.executionID {createID="EXECUTION-QUEUE-forged-session"}
			if path[0]=="validation_acceptance" {approval:=forged["validation_acceptance"].(map[string]any);approval["task_id"]=createID;approval["review_ref"].(map[string]any)["task_id"]=createID}
			// Isolate this exact protected path in create/import too. Otherwise
			// a guard on an unrelated genuine approval could hide a missing
			// guard for the path this case claims to exercise.
			createNote:=rpClone(t,forged)
			if path[0]!="validation_acceptance" {
				delete(createNote,"validation_acceptance")
				selected:=f.slot(createNote)
				for _,otherPath:=range paths[1:] {
					if reflect.DeepEqual(path,otherPath) {continue}
					parent:=selected
					for _,part:=range otherPath[:len(otherPath)-1] {parent=parent[part].(map[string]any)}
					delete(parent,otherPath[len(otherPath)-1])
				}
			}
			contract:=map[string]any{"canonical_name":createID,"task_ref":createID,"level":"task"}
			payload:=map[string]any{"title":"forgery","pillar":"TEST","status":"not_started","owner":"UNOWNED","is_gap":false,"note":string(rpJSON(t,createNote))}
			f.denyOrigin("task_graph_create",map[string]any{"project_id":"arbor","node_kind":"task","stable_id":createID,"contract":contract,"payload":payload})
			f.denyOrigin("task_graph_import",map[string]any{"submissions":[]any{map[string]any{"node_kind":"task","stable_id":createID,"contract":contract,"payload":payload}}})
			if !reflect.DeepEqual(beforeRows,f.rows()) {t.Fatal("generic mutation changed exclusive origin")}
		})}}
	})}
}

func (f *reviewProducerFixture) forgedProtectedValue(path []string,mode string,oldLeaf,oldController map[string]any) any {
	t:=f.t
	name:=strings.Join(path,".")
	leaf,controller:=f.leafNote(),f.taskNote();if mode=="replayed" {leaf,controller=oldLeaf,oldController}
	var value map[string]any
	switch name {
	case "validation_acceptance":value=rpClone(t,leaf["validation_acceptance"].(map[string]any))
	case "tests.acceptance":value=rpClone(t,f.slot(controller)["tests"].(map[string]any)["acceptance"].(map[string]any))
	case "acceptance":value=rpClone(t,f.slot(controller)["acceptance"].(map[string]any));file:=filepath.Join(t.TempDir(),"caller-production-review.json");bytes:=rpJSON(t,value);rpWrite(t,file,bytes);value["evidence_path"]=file;value["evidence_sha256"]=rpDigest(bytes)
	default:
		// These are untrusted but correctly shaped historical TDD records.
		// A valid local receipt/file is deliberately NOT server execution origin.
		taskID,worktree,testRev,codeRev:="REVIEW-TASK",f.recordedWorktree,f.revision,f.head
		if mode=="cross-bound" {taskID="OTHER-LEAF";worktree+="-other"}
		if mode=="replayed" {testRev=strings.Repeat("a",40);codeRev=strings.Repeat("b",40)}
		core:=map[string]any{"schema_version":"velenza_tdd_red_receipt/1","project_id":"arbor","task_id":taskID,"test_rev":testRev,"test_revision":testRev,"exit_code":1,"output_sha256":strings.Repeat("c",64),"created_at":"2026-10-22T10:00:00Z"}
		if name=="reopen_history" {core=map[string]any{"schema_version":"velenza_tdd_reopen_receipt/1","task_id":taskID,"worktree":worktree,"previous_queue":"test-code","previous_test_rev":testRev,"reason":"fixture-only assertion defect","reopened_at":"2026-10-22T10:01:00Z"};core["receipt_sha256"]=rpDigest(rpJSON(t,core));return []any{core}}
		if name=="tests.tdd_audit_receipt" {core["schema_version"]="velenza_tdd_audit_receipt/1";core["blocking"]=0}
		if name=="tdd_green_receipt" {core["schema_version"]="velenza_tdd_green_receipt/1";core["code_rev"]=codeRev;core["exit_code"]=0}
		coreSHA:=rpDigest(rpJSON(t,core));core["receipt_sha256"]=coreSHA
		file:=filepath.Join(t.TempDir(),"caller-receipt.json");bytes:=rpJSON(t,core);rpWrite(t,file,bytes)
		value=map[string]any{"path":file,"sha256":rpDigest(bytes),"receipt_sha256":coreSHA}
		if name=="tdd_red_receipt" {value["test_rev"]=testRev};if name=="tdd_green_receipt" {value["code_rev"]=codeRev}
		return value
	}
	if mode=="replayed" {return value}
	ref:=value["review_ref"].(map[string]any)
	if mode=="cross-bound" {ref["task_id"]="OTHER-LEAF";if name=="validation_acceptance" || name=="tests.acceptance" {value["task_id"]="OTHER-LEAF"}} else {ref["review_id"]="caller-authored-review";ref["input_sha256"]=strings.Repeat("f",64)}
	return value
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
				t.Run(erase,func(t *testing.T){beforeLeaf,beforeController:=f.leafNote(),f.taskNote();note:=rpClone(t,beforeController);id:=f.executionID
					switch erase {case "root-approval":note=rpClone(t,beforeLeaf);id="REVIEW-TASK";delete(note,"validation_acceptance");case "slot-approval":delete(f.slot(note)["tests"].(map[string]any),"acceptance");case "whole-slot":delete(note,"queue_assignments");delete(note,"queue_assignment")}
					f.denyOrigin("task_graph_update",f.taskUpdate("arbor",id,map[string]any{"note":string(rpJSON(t,note))}))
					if !reflect.DeepEqual(beforeLeaf,f.leafNote()) || !reflect.DeepEqual(beforeController,f.taskNote()) {t.Fatal("note replacement erased protected origin in leaf/controller")}
				})
			}
		})
	}
}

func TestReviewProducerRejectsChangedHashUnsafePathAndMissingCommittedSource(t *testing.T) {
	for _,change:=range []string{"hash","missing","parent","absolute","owner-hash","missing-owner","inarch-unavailable"} {
		t.Run(change,func(t *testing.T){
			f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
			note:=f.taskNote();slot:=note["queue_assignments"].([]any)[0].(map[string]any)
			file:=slot["tests"].(map[string]any)["files"].([]any)[0].(map[string]any)
			switch change {case "hash":file["sha256"]=strings.Repeat("f",64);case "missing":file["path"]="missing.test.mjs";case "parent":file["path"]="../cases.test.mjs";case "absolute":file["path"]=filepath.Join(f.repo,"cases.test.mjs");case "owner-hash":slot["governing_files"].([]any)[0].(map[string]any)["sha256"]=strings.Repeat("f",64);case "missing-owner":slot["governing_files"]=[]any{};case "inarch-unavailable":slot["tests"].(map[string]any)["revision"]="inarch:unverified-caller-label"}
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
	artifactContract:=map[string]any{"artifact_type_kind":"finding","artifact_ref":"ordinary"}
	f.allow("task_graph_create",map[string]any{"project_id":"arbor","node_kind":"artifact","stable_id":"review-one","contract":artifactContract,"payload":map[string]any{"task_id":"REVIEW-TASK","note":"ordinary note about a review"}})
	f.allow("task_graph_update",map[string]any{"project_id":"arbor","node_id":"review-one","contract":rpClone(t,artifactContract),"payload":map[string]any{"note":"updated ordinary metadata"}})
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
	beforeTests:=rpClone(t,f.slot(f.taskNote())["tests"].(map[string]any))
	conflict:=f.productionRequest("review-one")
	f.deny("task_graph_review_produce",conflict)
	conflict["review_id"]="production-one"
	rpWrite(t,filepath.Join(f.repo,"operation.mjs"),[]byte("DIRTY-NOT-PRODUCTION-INPUT"))
	out:=f.allow("task_graph_review_produce",conflict)
	if out["kind"]!="production" || out["revision"]!=f.head || out["base_revision"]!=f.revision {t.Fatalf("production head/base substitution: %#v",out)}
	capture,err:=os.ReadFile(f.capture);if err!=nil {t.Fatal(err)}
	lines:=strings.Split(strings.TrimSpace(string(capture)),"\n");var actual map[string]any;if err:=json.Unmarshal([]byte(lines[len(lines)-1]),&actual);err!=nil {t.Fatal(err)}
	bundle:=actual["bundle"].(map[string]any)
	production,ok:=bundle["production"].(map[string]any)
	if !ok || production["revision"]!=f.head || production["base_revision"]!=f.revision {t.Fatalf("child did not receive actual production lineage: %#v",bundle)}
	diff,ok:=production["diff"].(string)
	if !ok || !strings.Contains(diff,"-export function requireReviewOrigin") || !strings.Contains(diff,"+  if (!value.review_ref") || strings.Contains(string(capture),"DIRTY-NOT-PRODUCTION-INPUT") {t.Fatalf("missing/dirty production diff: %#v",production)}
	code:=production["files"].([]any)
	if len(code)!=1 || code[0].(map[string]any)["path"]!="operation.mjs" || code[0].(map[string]any)["content"]!=f.productionBody || code[0].(map[string]any)["sha256"]!=rpDigest([]byte(f.productionBody)) {t.Fatal("complete committed production body/hash absent")}
	if len(bundle["test_files"].([]any))!=1 || len(bundle["governing_files"].([]any))!=1 {t.Fatal("production review omitted frozen tests or governing owner")}
	f.allow("task_graph_review_get",conflict)
	slot:=f.slot(f.taskNote())
	acceptance,ok:=slot["acceptance"].(map[string]any)
	if !ok || acceptance["revision"]!=f.head || !reflect.DeepEqual(acceptance["review_ref"],out["review_ref"]) {t.Fatalf("production publication missing exact final head: %#v",slot)}
	if !reflect.DeepEqual(beforeTests,slot["tests"]) {t.Fatal("production review rewrote frozen test handoff")}
	if len(f.rows())!=2 {t.Fatal("production result lost or replaced earlier review")}
}

func TestReviewProducerProductionRequiresFrozenHeadBaseAndCompleteScope(t *testing.T) {
	for _,change:=range []string{"missing-head","wrong-head","missing-base","wrong-base","missing-scope","incomplete-scope","body-hash","undeclared-file"} {t.Run(change,func(t *testing.T){
		f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.productionRequest("production-control"))
		note:=f.taskNote();slot:=f.slot(note);bad:=f.productionRequest("production-invalid")
		switch change {case "missing-head":delete(slot,"revision");case "wrong-head":slot["revision"]=f.revision;case "missing-base":delete(slot,"base_revision");case "wrong-base":slot["base_revision"]=f.head;case "missing-scope":delete(slot,"production_files");case "incomplete-scope":slot["production_files"]=[]any{};case "body-hash":slot["production_files"].([]any)[0].(map[string]any)["sha256"]=strings.Repeat("9",64);case "undeclared-file":rpWrite(t,filepath.Join(f.repo,"outside.mjs"),[]byte("export const outside = true;\n"));rpGit(t,f.repo,"add","outside.mjs");rpGit(t,f.repo,"commit","-m","undeclared production path");head:=rpGit(t,f.repo,"rev-parse","HEAD");slot["revision"]=head;bad["revision"]=head}
		f.updateNote(note);f.deny("task_graph_review_produce",bad)
		if len(f.rows())!=1 {t.Fatal("incomplete production input minted origin")}
	})}
}

func TestReviewProducerValidShapedStoredTamperingRefuses(t *testing.T) {
	for _,column:=range []string{"binding_json","input_sha256","producer_generation","kind","slot_key","task_id","execution_project_id","execution_task_id","result_json"} {
		t.Run(column,func(t *testing.T){
			f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.request)
			row:=f.rows()[0]
			value:="wrong-but-shaped"
			switch column {case "input_sha256","slot_key":value=strings.Repeat("9",64);case "kind":value="production";case "binding_json":var binding map[string]any;if err:=json.Unmarshal([]byte(row["binding"].(string)),&binding);err!=nil {t.Fatal(err)};binding["context_sha256"]=strings.Repeat("9",64);value=string(rpJSON(t,binding));case "result_json":var result map[string]any;if err:=json.Unmarshal([]byte(row["result"].(string)),&result);err!=nil {t.Fatal(err)};result["input_sha256"]=strings.Repeat("9",64);value=string(rpJSON(t,result))}
			_,err:=f.client.Apply(context.Background(),[]*spanner.Mutation{spanner.Update("IndependentReviewEvidence",[]string{"project_id","review_id",column},[]any{"arbor","review-one",value})})
			if err!=nil {t.Fatal(err)}
			f.deny("task_graph_review_get",f.request)
		})
	}
}

func rpRequireAtomicRollback(t *testing.T,f *reviewProducerFixture) {
	t.Helper()
	f.allow("task_graph_review_produce",f.request)
	for _,stage:=range []string{"after_evidence_insert","after_leaf_publication","after_execution_publication"} {
		t.Run(stage,func(t *testing.T){
			beforeRows,beforeLeaf,beforeController:=f.rows(),f.leafNote(),f.taskNote()
			reached:=false
			reviewProducerFault=func(actual string)error{if actual==stage {reached=true;return errors.New("injected review publication failure")};return nil}
			t.Cleanup(func(){reviewProducerFault=nil})
			in:=rpClone(t,f.request);in["review_id"]="rollback-"+stage
			f.deny("task_graph_review_produce",in)
			reviewProducerFault=nil
			if !reached {t.Fatalf("required %s seam was never reached; preflight refusal is not rollback proof",stage)}
			if !reflect.DeepEqual(beforeRows,f.rows()) || !reflect.DeepEqual(beforeLeaf,f.leafNote()) || !reflect.DeepEqual(beforeController,f.taskNote()) {t.Fatal("origin/leaf/controller failed to roll back atomically")}
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
			before,beforeLeaf:=f.taskNote(),f.leafNote()
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
			if !reflect.DeepEqual(before,f.taskNote()) || !reflect.DeepEqual(beforeLeaf,f.leafNote()) || len(f.rows())!=1 {t.Fatal("missing capability changed either publication row")}
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
	other:=f.disjointSlot()
	rpWrite(t,f.control,[]byte("hold"))
	in:=rpClone(t,f.request);in["review_id"]="metadata-race"
	done:=make(chan error,1);go func(){_,err:=f.call("task_graph_review_produce",in);done<-err}()
	rpWaitCaptures(t,f.capture,2)
	note:=f.taskNote();note["ordinary"]="concurrent metadata survives"
	slots:=note["queue_assignments"].([]any);note["queue_assignments"]=append(slots,other)
	f.updateNote(note)
	leaf:=f.leafNote();leaf["ordinary"]="concurrent leaf metadata survives";f.updateNoteFor("REVIEW-TASK",leaf)
	rpWrite(t,f.control,[]byte("valid"))
	if err:=<-done;err!=nil {t.Fatalf("unrelated metadata rejected publication: %v",err)}
	got:=f.taskNote();if got["ordinary"]!="concurrent metadata survives" || len(got["queue_assignments"].([]any))!=2 || f.leafNote()["ordinary"]!="concurrent leaf metadata survives" {t.Fatal("publication overwrote concurrent metadata/disjoint slot in either Task")}
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
	inputSHA:=bundle["input_sha256"]
	delete(bundle,"input_sha256")
	if inputSHA!=rpDigest(rpJSON(t,bundle)) {os.Exit(45)}
	bundle["input_sha256"]=inputSHA
	testFiles,ok:=bundle["test_files"].([]any)
	if !ok || len(testFiles)!=1 || testFiles[0].(map[string]any)["path"]!="cases.test.mjs" {os.Exit(38)}
	governing,ok:=bundle["governing_files"].([]any)
	if !ok || len(governing)!=1 {os.Exit(39)}
	owner:=governing[0].(map[string]any)
	ownerBody,ok:=owner["content"].(string)
	if !ok || owner["path"]!="REQUIREMENTS.md" || owner["sha256"]!=rpDigest([]byte(ownerBody)) || !strings.Contains(ownerBody,"R1:") || !strings.Contains(ownerBody,"R2:") {os.Exit(40)}
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
	if bundle["kind"]=="production" {
		production,ok:=bundle["production"].(map[string]any);if !ok {os.Exit(41)}
		diff,ok:=production["diff"].(string);if !ok || !strings.Contains(diff,"+  if (!value.review_ref") {os.Exit(42)}
		files,ok:=production["files"].([]any);if !ok || len(files)!=1 {os.Exit(43)}
		code:=files[0].(map[string]any);body,ok:=code["content"].(string)
		if !ok || code["path"]!="operation.mjs" || code["sha256"]!=rpDigest([]byte(body)) || !strings.Contains(body,"origin required") {os.Exit(44)}
		output["production_base_revision"]=production["base_revision"];output["production_revision"]=production["revision"]
		output["reviewed_files"]=[]any{map[string]any{"path":code["path"],"sha256":code["sha256"]}}
		if string(modeBytes)=="incomplete-production" {output["reviewed_files"]=[]any{}}
		if string(modeBytes)=="wrong-production-head" {output["production_revision"]=production["base_revision"]}
	}
	b,err:=json.Marshal(output);if err!=nil {os.Exit(36)}
	if string(modeBytes)=="duplicate" {b=[]byte(strings.Replace(string(b),`"kind":"validate_tests"`,`"kind":"production","kind":"validate_tests"`,1))}
	_,_ = os.Stdout.Write(b)
	os.Exit(0)
}

func TestReviewProducerRejectsIncompleteProductionReviewOutput(t *testing.T) {
	for _,mode:=range []string{"incomplete-production","wrong-production-head"} {t.Run(mode,func(t *testing.T){
		f:=newReviewProducerFixture(t);f.allow("task_graph_review_produce",f.productionRequest("production-control"));beforeRows,beforeLeaf,beforeController:=f.rows(),f.leafNote(),f.taskNote()
		rpWrite(t,f.control,[]byte(mode));f.deny("task_graph_review_produce",f.productionRequest("production-invalid"))
		if !reflect.DeepEqual(beforeRows,f.rows()) || !reflect.DeepEqual(beforeLeaf,f.leafNote()) || !reflect.DeepEqual(beforeController,f.taskNote()) {t.Fatal("incomplete production review published partial origin/acceptance")}
	})}
}
