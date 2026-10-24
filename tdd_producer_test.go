package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

// Candidate test DDL only. No fixture inserts trusted execution/lineage rows.
const tddProducerTestDDL = `CREATE TABLE TDDExecutionEvidence (
 project_id STRING(64) NOT NULL, evidence_id STRING(64) NOT NULL,
 task_id STRING(64) NOT NULL, execution_project_id STRING(64) NOT NULL,
 execution_task_id STRING(128) NOT NULL, slot_key STRING(64) NOT NULL,
 action STRING(32) NOT NULL, generation_id STRING(64) NOT NULL,
 parent_evidence_id STRING(64), input_sha256 STRING(64) NOT NULL,
 binding_json STRING(MAX) NOT NULL, result_json STRING(MAX) NOT NULL,
 producer_generation STRING(128) NOT NULL, created_at TIMESTAMP NOT NULL,
) PRIMARY KEY (project_id,evidence_id)`

type tddFixture struct {
	f *reviewProducerFixture
	base,tests,code,repaired,changed string
	testBody,repairedBody,codeBody string
	control,capture string
	slotTemplate map[string]any
	startRef,redRef,greenRef,reviewRef,productionRef map[string]any
}

func newTDDFixture(t *testing.T) *tddFixture {return tdSourceFixture(t,newReviewProducerFixtureForProject(t,"arbor",tddProducerTestDDL))}

func tdSourceFixture(t *testing.T,f *reviewProducerFixture) *tddFixture {
	t.Helper()
	d:=&tddFixture{f:f}
	owner:="# Execution fixture owner\nR1: Add returns the sum; compiled assertions must fail before implementation.\nR2: Negative and zero inputs preserve arithmetic; source and argv must be exact.\n"
	rpWrite(t,filepath.Join(f.repo,"REQUIREMENTS.md"),[]byte(owner))
	rpWrite(t,filepath.Join(f.repo,"go.mod"),[]byte("module example.test/tddfixture\n\ngo 1.26\n"))
	rpWrite(t,filepath.Join(f.repo,"go.sum"),[]byte{})
	if err:=os.Mkdir(filepath.Join(f.repo,"testdata"),0700);err!=nil {t.Fatal(err)}
	rpWrite(t,filepath.Join(f.repo,"testdata","message.txt"),[]byte("committed embedded source input\n"))
	rpWrite(t,filepath.Join(f.repo,"embedded.go"),[]byte("package tddfixture\nimport _ \"embed\"\n//go:embed testdata/message.txt\nvar fixtureText string\n"))
	rpWrite(t,filepath.Join(f.repo,"operation.go"),[]byte("package tddfixture\nfunc Add(a,b int) int { return 0 }\n"))
	rpGit(t,f.repo,"add","REQUIREMENTS.md","go.mod","go.sum","operation.go","embedded.go","testdata/message.txt");rpGit(t,f.repo,"commit","-m","committed preimplementation Go source");d.base=rpGit(t,f.repo,"rev-parse","HEAD")
	d.testBody="package tddfixture\nimport \"testing\"\nfunc TestAdd(t *testing.T) { if Add(2,3) != 5 { t.Fatal(\"sum assertion\") } }\nfunc TestNegative(t *testing.T) { if Add(-2,1) != -1 { t.Fatal(\"negative assertion\") } }\n"
	rpWrite(t,filepath.Join(f.repo,"cases_test.go"),[]byte(d.testBody));rpGit(t,f.repo,"add","cases_test.go");rpGit(t,f.repo,"commit","-m","frozen compiled assertion cases");d.tests=rpGit(t,f.repo,"rev-parse","HEAD")
	d.codeBody="package tddfixture\nfunc Add(a,b int) int { return a+b }\n"
	rpWrite(t,filepath.Join(f.repo,"operation.go"),[]byte(d.codeBody));rpGit(t,f.repo,"add","operation.go");rpGit(t,f.repo,"commit","-m","implement sum");d.code=rpGit(t,f.repo,"rev-parse","HEAD")
	d.repairedBody=d.testBody+"func TestZero(t *testing.T) { if Add(0,0) != 0 { t.Fatal(\"zero assertion\") } }\n"
	rpWrite(t,filepath.Join(f.repo,"cases_test.go"),[]byte(d.repairedBody));rpGit(t,f.repo,"add","cases_test.go");rpGit(t,f.repo,"commit","-m","repair only validation fixture coverage");d.repaired=rpGit(t,f.repo,"rev-parse","HEAD")
	rpWrite(t,filepath.Join(f.repo,"operation.go"),[]byte("package tddfixture\nfunc Add(a,b int) int { if a==0 && b==0 { return 0 }; return a+b }\n"));rpGit(t,f.repo,"add","operation.go");rpGit(t,f.repo,"commit","-m","actual production bytes changed too");d.changed=rpGit(t,f.repo,"rev-parse","HEAD")
	d.control,d.capture=filepath.Join(t.TempDir(),"executor-control"),filepath.Join(t.TempDir(),"executor-capture")
	rpWrite(t,d.control,[]byte("valid"))
	program:=f.cfg["review_producer"].(map[string]any)
	program["args"]=[]string{"-test.run=^TestTDDReviewChild$","--","tdd-review-child",f.control,f.capture,"--tools",""}
	f.cfg["tdd_executor"]=map[string]any{"generation":"tdd-executor-e1","executable":program["executable"],"executable_sha256":program["executable_sha256"],"args":[]string{"-test.run=^TestTDDExecutorChild$","--","tdd-executor-child",d.control,d.capture},"timeout_milliseconds":1000,"max_output_bytes":131072,"go_path":"/qualified/native/go","packages":[]string{"./..."}}
	f.cfg["substrates"].([]any)[0].(map[string]any)["tdd_evidence_table"]="TDDExecutionEvidence"
	f.start()
	d.slotTemplate=rpClone(t,f.slot(f.taskNote()))
	d.slotTemplate["queue"]="build-tests";d.slotTemplate["revision"]=d.code;d.slotTemplate["base_revision"]=d.tests
	d.slotTemplate["tests"]=map[string]any{"author":"test-author","revision":d.tests,"files":[]any{map[string]any{"path":"cases_test.go","sha256":rpDigest([]byte(d.testBody))}},"requirements":[]any{"R1","R2"}}
	d.slotTemplate["production_files"]=[]any{map[string]any{"path":"operation.go","sha256":rpDigest([]byte(d.codeBody))}}
	d.slotTemplate["governing_files"]=[]any{map[string]any{"path":"REQUIREMENTS.md","sha256":rpDigest([]byte(owner))}}
	// Start must not need or manufacture a slot. Preserve a disjoint ordinary
	// assignment; only existing typed bookkeeping registers this leaf later.
	note:=f.taskNote();other:=f.disjointSlot();note["queue_assignments"]=[]any{other};f.updateNote(note)
	f.revision,f.head,f.ownerBody=d.tests,d.code,owner
	f.request["revision"]=d.tests
	return d
}

func (d *tddFixture) request(action,id,revision string) map[string]any {
	in:=map[string]any{"project_id":d.f.project,"task_id":"REVIEW-TASK","binding_link_id":"REVIEW-BINDING","execution_task_id":d.f.executionID,"epoch":"e1","context_sha256":strings.Repeat("c",64),"action":action,"request_id":id,"revision":revision}
	if action!="start" {ref:=d.current();in["generation_id"]=ref["generation_id"];in["previous_ref"]=ref;in["expected_queue"]=d.slot(d.f.taskNote())["queue"]}
	if action=="reopen" {in["reason"]="repair exact committed validation inputs"}
	return in
}

func (d *tddFixture) current() map[string]any {return d.f.leafNote()["tdd_execution_ref"].(map[string]any)}

func (d *tddFixture) slot(note map[string]any) map[string]any {
	if one,ok:=note["queue_assignment"].(map[string]any);ok && one["task_id"]=="REVIEW-TASK" {return one}
	if slots,ok:=note["queue_assignments"].([]any);ok {for _,raw:=range slots {slot:=raw.(map[string]any);if slot["task_id"]=="REVIEW-TASK" {return slot}}}
	d.f.t.Fatal("actual TDD slot missing");return nil
}

func (d *tddFixture) start() map[string]any {
	out:=d.f.allow("task_graph_tdd_produce",d.request("start","tdd-start",d.base));d.startRef=out["tdd_ref"].(map[string]any);return out
}

func (d *tddFixture) register() {
	note:=d.f.taskNote();note["queue_assignments"]=append(note["queue_assignments"].([]any),rpClone(d.f.t,d.slotTemplate));d.f.updateNote(note)
}

func (d *tddFixture) red(id,revision string) map[string]any {
	out:=d.f.allow("task_graph_tdd_produce",d.request("red",id,revision));d.redRef=out["tdd_ref"].(map[string]any);return out
}

func (d *tddFixture) review(id string) map[string]any {
	in:=rpClone(d.f.t,d.f.request);in["revision"]=d.slot(d.f.taskNote())["tests"].(map[string]any)["revision"];in["review_id"]=id
	out:=d.f.allow("task_graph_review_produce",in);d.reviewRef=out["review_ref"].(map[string]any)
	note:=d.f.taskNote();d.slot(note)["queue"]="build-code";d.f.updateNote(note)
	return out
}

func (d *tddFixture) green(id,revision string) map[string]any {out:=d.f.allow("task_graph_tdd_produce",d.request("green",id,revision));d.greenRef=out["tdd_ref"].(map[string]any);return out}

func (d *tddFixture) productionReview() map[string]any {
	in:=rpClone(d.f.t,d.f.request);in["kind"]="production";in["revision"]=d.code;in["base_revision"]=d.tests;in["review_id"]="prior-production-review"
	out:=d.f.allow("task_graph_review_produce",in);d.productionRef=out["review_ref"].(map[string]any);return out
}

func (d *tddFixture) throughGreen() {d.start();d.register();d.red("tdd-red",d.tests);d.review("tdd-cases-review");d.green("tdd-green",d.code)}

func (d *tddFixture) rows() []map[string]any {
	d.f.t.Helper();iter:=d.f.client.Single().Query(context.Background(),spanner.Statement{SQL:`SELECT project_id,evidence_id,task_id,execution_task_id,slot_key,action,generation_id,parent_evidence_id,input_sha256,binding_json,result_json,producer_generation FROM TDDExecutionEvidence ORDER BY project_id,evidence_id`});defer iter.Stop()
	res,err:=collectQueryResult(iter);if err!=nil {d.f.t.Fatal(err)};return res.Rows
}

func tdCaptures(t *testing.T,path string) []map[string]any {
	t.Helper();data,err:=os.ReadFile(path);if err!=nil {t.Fatal(err)};var out []map[string]any
	for _,line:=range strings.Split(strings.TrimSpace(string(data)),"\n") {if line=="" {continue};var item map[string]any;if err:=json.Unmarshal([]byte(line),&item);err!=nil {t.Fatal(err)};out=append(out,item)};return out
}

func tdCapture(t *testing.T,path string,input map[string]any,args []string) {
	t.Helper();file,err:=os.OpenFile(path,os.O_CREATE|os.O_APPEND|os.O_WRONLY,0600);if err!=nil {os.Exit(52)};defer file.Close()
	_,err=file.Write(append(rpJSON(t,map[string]any{"bundle":input,"argv":args,"pid":os.Getpid(),"caller_env":os.Getenv("TDD_CALLER_SECRET")}), '\n'));if err!=nil {os.Exit(53)}
}

func tdAssertInput(t *testing.T,input map[string]any) {
	t.Helper();claimed:=input["input_sha256"];copy:=rpClone(t,input);delete(copy,"input_sha256");if claimed!=rpDigest(rpJSON(t,copy)) {os.Exit(54)}
}

// This is a controlled native-executor PROTOCOL MODEL, not qualification of
// the real VRT provider or proof that modelled Go events were run in production.
// Only the actual server invocation may create origin; fixtures never seed it.
func TestTDDExecutorChild(t *testing.T) {
	marker:=-1;for i,arg:=range os.Args {if arg=="tdd-executor-child" {marker=i;break}};if marker<0 {return};if len(os.Args)!=marker+3 {os.Exit(50)}
	control,capture:=os.Args[marker+1],os.Args[marker+2];data,err:=io.ReadAll(os.Stdin);if err!=nil {os.Exit(51)};var input map[string]any;if json.Unmarshal(data,&input)!=nil {os.Exit(51)};tdAssertInput(t,input);tdCapture(t,capture,input,os.Args[marker:])
	if input["schema_version"]!="velenza_tdd_executor_request/1" || len(input)!=7 {os.Exit(63)}
	identity,ok:=input["identity"].(map[string]any);if !ok || identity["project_id"]!="arbor" || identity["task_id"]!="REVIEW-TASK" || identity["binding_link_id"]!="REVIEW-BINDING" || identity["execution_task_id"]!="EXECUTION-QUEUE-shared-session" || identity["generation_id"]==nil || identity["action"]!=input["action"] {os.Exit(64)}
	if !reflect.DeepEqual(input["argv"],[]any{"/qualified/native/go","test","-json","-count=1","./..."}) {os.Exit(65)}
	modeBytes,err:=os.ReadFile(control);if err!=nil {os.Exit(55)};mode:=string(modeBytes)
	if mode=="timeout" || mode=="hold" {deadline:=time.Now().Add(4*time.Second);for time.Now().Before(deadline) {current,_:=os.ReadFile(control);if string(current)!="hold" && mode=="hold" {mode=string(current);break};time.Sleep(10*time.Millisecond)}}
	if mode=="signal" {process,_:=os.FindProcess(os.Getpid());_ = process.Kill();os.Exit(56)}
	if mode=="malformed" {fmt.Print("not JSON");os.Exit(1)};if mode=="oversize" {fmt.Print(strings.Repeat("x",140000));os.Exit(1)}
	source,ok:=input["source"].(map[string]any);if !ok {os.Exit(57)};sourceCopy:=rpClone(t,source);delete(sourceCopy,"sha256");if source["sha256"]!=rpDigest(rpJSON(t,sourceCopy)) {os.Exit(58)}
	files,ok:=source["files"].([]any);if !ok || len(files)==0 {os.Exit(59)};previousPath:="";for _,raw:=range files {file:=raw.(map[string]any);path:=file["path"].(string);if path<=previousPath || strings.HasPrefix(path,".git/") || file["sha256"]!=rpDigest([]byte(file["content"].(string))) {os.Exit(59)};previousPath=path}
	exitCode:=0;started:=input["operation"]!="capabilities";events:=[]any{}
	if started {
		failed:=input["action"]=="red";if failed {exitCode=1}
		events=append(events,map[string]any{"Action":"start","Package":"example.test/tddfixture"})
		if mode=="compile-error" {exitCode=1;events=append(events,map[string]any{"Action":"build-fail","ImportPath":"example.test/tddfixture","Output":"compile error: undefined identifier"})} else if mode!="zero-tests" {
			status:="pass";if failed {status="fail"};if mode=="skipped-only" {status="skip"}
			events=append(events,map[string]any{"Action":"run","Package":"example.test/tddfixture","Test":"TestAdd"},map[string]any{"Action":"output","Package":"example.test/tddfixture","Test":"TestAdd","Output":"cases_test.go: sum assertion\n"},map[string]any{"Action":status,"Package":"example.test/tddfixture","Test":"TestAdd"})
		}
		status:="pass";if exitCode!=0 {status="fail"};events=append(events,map[string]any{"Action":status,"Package":"example.test/tddfixture"})
	}
	var stream strings.Builder;for _,event:=range events {stream.Write(rpJSON(t,event));stream.WriteByte('\n')}
	now:=time.Now().UTC().Format(time.RFC3339Nano)
	output:=map[string]any{"schema_version":"velenza_tdd_executor_result/1","producer_generation":"tdd-executor-e1","input_sha256":input["input_sha256"],"source_sha256":source["sha256"],"argv":input["argv"],"capabilities":[]string{"native-go-test-json","immutable-source-v1","exact-argv-v1","descendant-custody-v1"},"completed":true,"process_started":started,"exit_code":exitCode,"signal":"","go_test_json":stream.String(),"provider_receipt":map[string]any{"schema_version":"velenza_native_go_receipt/1","allocation_id":fmt.Sprintf("test-owned-%d",os.Getpid()),"source_sha256":source["sha256"],"argv_sha256":rpDigest(rpJSON(t,input["argv"])),"exit_code":exitCode,"completed":true,"started_at":now,"finished_at":now}}
	switch mode {case "wrong-source":output["source_sha256"]=strings.Repeat("9",64);case "wrong-input":output["input_sha256"]=strings.Repeat("9",64);case "wrong-argv":output["argv"]=[]string{"/caller/go","test","./..."};case "missing-capability":output["capabilities"]=[]string{"native-go-test-json"};case "not-started":output["process_started"]=false;case "not-completed":output["completed"]=false;case "legacy-receipt":output=map[string]any{"allocation":"old-provider","exitCode":exitCode,"completed":true};case "claimed-exit":if exitCode==0 {exitCode=1} else {exitCode=0}}
	encoded:=rpJSON(t,output);if mode=="duplicate" {encoded=[]byte(strings.Replace(string(encoded),`"completed":true`,`"completed":false,"completed":true`,1))};_,_ = os.Stdout.Write(encoded);os.Exit(exitCode)
}

func TestTDDReviewChild(t *testing.T) {
	marker:=-1;for i,arg:=range os.Args {if arg=="tdd-review-child" {marker=i;break}};if marker<0 {return};if len(os.Args)!=marker+5 || os.Args[marker+3]!="--tools" || os.Args[marker+4]!="" {os.Exit(60)}
	data,err:=io.ReadAll(os.Stdin);if err!=nil {os.Exit(61)};var input map[string]any;if json.Unmarshal(data,&input)!=nil {os.Exit(61)};tdAssertInput(t,input);tdCapture(t,os.Args[marker+2],input,os.Args[marker:]);modeBytes,_:=os.ReadFile(os.Args[marker+1]);mode:=string(modeBytes)
	if mode=="malformed" {fmt.Print("not a review");os.Exit(0)};if mode=="timeout" {time.Sleep(3*time.Second)}
	blocking:=[]any{};if mode=="blocking" {blocking=append(blocking,map[string]any{"id":"R1","reason":"controlled independent refusal"})}
	flags:=map[string]any{"kind":input["kind"],"input_sha256":input["input_sha256"],"blocking":blocking,"production_boundary":true,"tautologies":false,"fixture_supplied_authority":false,"summary":"actual-tdd-review-child"}
	if input["kind"]=="tdd_reopen" {
		reopen,ok:=input["reopen"].(map[string]any);if !ok {os.Exit(62)};classification:="fixture_only"
		if !reflect.DeepEqual(reopen["previous_production_files"],reopen["production_files"]) {classification="production_changed"};if mode=="lie-fixture" {classification="fixture_only"}
		flags["schema_version"]="velenza_tdd_reopen_review/1";flags["classification"]=classification;flags["reviewed_files"]=tdManifest(reopen["files"]);flags["production_files"]=tdManifest(reopen["production_files"])
		if mode=="partial" {flags["reviewed_files"]=[]any{}}
	} else {
		if input["kind"]=="validate_tests" && input["tdd_red_ref"]==nil {os.Exit(66)}
		flags["schema_version"]="velenza_independent_review/1";flags["requirements"]=[]string{"R1","R2"};coverage:=[]any{}
		for _,requirement:=range []string{"R1","R2"} {for _,scenario:=range []string{"positive","refusal","exception"} {coverage=append(coverage,map[string]any{"requirement":requirement,"path":"cases_test.go","case":map[string]string{"positive":"TestAdd","refusal":"TestNegative","exception":"TestAdd"}[scenario],"assertion":map[string]string{"positive":"Add(2,3)","refusal":"Add(-2,1)","exception":"sum assertion"}[scenario],"boundary":"actual MCP executor origin and immutable Go source","scenario":scenario})}}
		flags["coverage"]=coverage
		if red,exists:=input["tdd_red_ref"];exists {flags["tdd_red_ref"]=red}
		if input["kind"]=="production" {production:=input["production"].(map[string]any);flags["production_base_revision"]=production["base_revision"];flags["production_revision"]=production["revision"];files:=[]any{};for _,raw:=range production["files"].([]any) {file:=raw.(map[string]any);files=append(files,map[string]any{"path":file["path"],"sha256":file["sha256"]})};flags["reviewed_files"]=files}
	}
	_,_ = os.Stdout.Write(rpJSON(t,flags));os.Exit(0)
}

func tdManifest(value any) []any {out:=[]any{};for _,raw:=range value.([]any) {file:=raw.(map[string]any);out=append(out,map[string]any{"path":file["path"],"sha256":file["sha256"]})};return out}

func TestTDDStartNeedsRealContextButNoSlotAndGrantsNoAssignment(t *testing.T) {
	d:=newTDDFixture(t);t.Setenv("TDD_CALLER_SECRET","must-not-enter-executor")
	before:=d.f.taskNote();out:=d.start()
	if !reflect.DeepEqual(before,d.f.taskNote()) || !reflect.DeepEqual(out["tdd_ref"],d.current()) || len(d.rows())!=1 {t.Fatal("start manufactured assignment or failed exact leaf/origin publication")}
	ref:=out["tdd_ref"].(map[string]any);if ref["action"]!="start" || ref["slot_key"]!=d.f.expectedSlotKey() || ref["revision"]!=d.base || ref["generation_id"]==nil {t.Fatal("start lost actual binding/base/generation")}
	d.f.allow("task_graph_tdd_get",ref)
	captured:=tdCaptures(t,d.capture);if len(captured)!=1 || captured[0]["caller_env"]!="" || captured[0]["bundle"].(map[string]any)["operation"]!="capabilities" {t.Fatal("start did not invoke actual pinned capability handshake with fixed environment")}
	retry:=d.f.allow("task_graph_tdd_produce",d.request("start","tdd-start",d.base));if retry["idempotent"]!=true || len(tdCaptures(t,d.capture))!=1 || len(d.rows())!=1 {t.Fatal("completed start retry reran executor or minted evidence")}
	leaf:=d.f.leafNote();leaf["ordinary"]="document about tests is still editable";d.f.updateNoteFor("REVIEW-TASK",leaf)
}

func TestTDDRealProcessRedReviewGreenAndImmutableMaterialization(t *testing.T) {
	d:=newTDDFixture(t);d.start();d.register()
	if out,err:=d.f.call("task_graph_review_produce",d.f.request);err==nil {t.Fatalf("review skipped required current RED: %#v",out)} else if !strings.Contains(err.Error(),"TDD_RED_REQUIRED") {t.Fatalf("review failed before actual RED binding gate: %v",err)}
	rpWrite(t,filepath.Join(d.f.repo,"operation.go"),[]byte("DIRTY-CALLER-SOURCE"));rpWrite(t,filepath.Join(d.f.repo,"current-only.txt"),[]byte("CALLER-PRIVATE-FILE"))
	d.red("tdd-red",d.tests);slot:=d.slot(d.f.taskNote())
	if slot["queue"]!="validate-tests" || !reflect.DeepEqual(slot["tdd_red_receipt"],d.redRef) || d.redRef["test_revision"]!=d.tests {t.Fatal("actual RED did not lock tests and publish expected stage/ref")}
	d.review("tdd-cases-review");slot=d.slot(d.f.taskNote());audit:=slot["tests"].(map[string]any)["tdd_audit_receipt"].(map[string]any)
	if !reflect.DeepEqual(audit["review_ref"],d.reviewRef) || !reflect.DeepEqual(audit["tdd_ref"],d.redRef) {t.Fatal("independent review/audit is not bound to exact current RED")}
	reviewCapture:=tdCaptures(t,d.f.capture);if !reflect.DeepEqual(reviewCapture[len(reviewCapture)-1]["bundle"].(map[string]any)["tdd_red_ref"],d.redRef) {t.Fatal("actual independent reviewer never received the RED origin binding")}
	d.green("tdd-green",d.code);slot=d.slot(d.f.taskNote());if slot["queue"]!="test-code" || !reflect.DeepEqual(slot["tdd_green_receipt"],d.greenRef) || len(d.rows())!=3 {t.Fatal("GREEN did not publish execution origin and test-code handoff")}
	d.f.allow("task_graph_tdd_get",d.greenRef)
	captured:=tdCaptures(t,d.capture);if len(captured)!=3 {t.Fatal("start/RED/GREEN did not each invoke the actual fixed child")}
	for _,item:=range captured {
		bundle:=item["bundle"].(map[string]any);source:=bundle["source"].(map[string]any);seen:=map[string]string{}
		for _,raw:=range source["files"].([]any) {file:=raw.(map[string]any);path:=file["path"].(string);if strings.Contains(path,".git") || path=="current-only.txt" || strings.Contains(file["content"].(string),"DIRTY-CALLER") {t.Fatal("mutable/synthetic source replaced immutable Git input")};seen[path]=file["content"].(string)}
		if seen["go.mod"]=="" || seen["embedded.go"]=="" || seen["testdata/message.txt"]!="committed embedded source input\n" {t.Fatal("materialization omitted real Go/embedded inputs")}
		if bundle["action"]=="red" && (seen["cases_test.go"]!=d.testBody || !strings.Contains(seen["operation.go"],"return 0")) {t.Fatal("RED did not use locked cases against actual preimplementation source")}
		if bundle["action"]=="green" && (seen["cases_test.go"]!=d.testBody || seen["operation.go"]!=d.codeBody) {t.Fatal("GREEN did not preserve identical tests and committed code")}
		if !reflect.DeepEqual(bundle["argv"],[]any{"/qualified/native/go","test","-json","-count=1","./..."}) {t.Fatal("executor argv was not the fixed administrator native-Go command")}
	}
}

func TestTDDExecutorFailuresNeverManufactureRedOrGreen(t *testing.T) {
	for _,phase:=range []string{"red","green"} {t.Run(phase,func(t *testing.T){
		t.Run("known-allow",func(t *testing.T){d:=newTDDFixture(t);d.throughGreen()})
		for _,mode:=range []string{"compile-error","not-started","not-completed","zero-tests","skipped-only","wrong-source","wrong-input","wrong-argv","missing-capability","legacy-receipt","claimed-exit","malformed","duplicate","oversize","timeout","signal"} {t.Run(mode,func(t *testing.T){
			d:=newTDDFixture(t);d.start();d.register();revision:=d.tests;if phase=="green" {d.red("red-before-failure",d.tests);d.review("review-before-failure");revision=d.code}
			beforeRows,beforeLeaf,beforeController:=d.rows(),d.f.leafNote(),d.f.taskNote();rpWrite(t,d.control,[]byte(mode));d.f.deny("task_graph_tdd_produce",d.request(phase,"invalid-execution",revision))
			if !reflect.DeepEqual(beforeRows,d.rows()) || !reflect.DeepEqual(beforeLeaf,d.f.leafNote()) || !reflect.DeepEqual(beforeController,d.f.taskNote()) {t.Fatal("failed actual executor published partial execution/progression")}
		})}
	})}
}

func TestTDDRequestScopeStageAndCallerAuthorityRefuse(t *testing.T) {
	for _,change:=range []string{"project","task","binding","controller","epoch","context","generation","queue","previous-ref","command","executable","argv","environment","receipt","verified","fixture-label","inarch","missing-source"} {t.Run(change,func(t *testing.T){
		d:=newTDDFixture(t);d.start();d.register();in:=d.request("red","bad-request",d.tests)
		switch change {case "project":in["project_id"]="other-project";case "task":in["task_id"]="OTHER-TASK";case "binding":in["binding_link_id"]="OTHER-BINDING";case "controller":in["execution_task_id"]="EXECUTION-QUEUE-other";case "epoch":in["epoch"]="old";case "context":in["context_sha256"]=strings.Repeat("9",64);case "generation":in["generation_id"]="forged-generation";case "queue":in["expected_queue"]="merge";case "previous-ref":in["previous_ref"]=map[string]any{"verified":true};case "command":in["command"]="go test ./...";case "executable":in["executable"]="/caller/runner";case "argv":in["argv"]=[]string{"sh","-c","true"};case "environment":in["env"]=map[string]any{"VERIFIED":"1"};case "receipt":in["receipt_path"]="/caller/receipt.json";case "verified":in["verified"]=true;case "fixture-label":in["fixture_only"]=true;case "inarch":in["revision"]="inarch:caller-label";case "missing-source":in["revision"]=strings.Repeat("a",40)}
		before:=d.rows();d.f.deny("task_graph_tdd_produce",in);if !reflect.DeepEqual(before,d.rows()) {t.Fatal("wrong scope/source/caller fields minted origin")}
	})}
}

func TestTDDStartMissingOrContradictoryActualAuthorityRefuses(t *testing.T) {
	for _,change:=range []string{"unowned","wrong-session","missing-plan","software-path-missing-owner","circuit-plan","duplicate-slot","wrong-controller-session"} {t.Run(change,func(t *testing.T){
		d:=newTDDFixture(t)
		switch change {
		case "unowned","wrong-session":column,value:="owner","UNOWNED";if change=="wrong-session" {column,value="session_id","other-session"};if _,err:=d.f.client.Apply(context.Background(),[]*spanner.Mutation{spanner.Update("Tasks",[]string{"project_id","id",column},[]any{"arbor","REVIEW-TASK",value})});err!=nil {t.Fatal(err)}
		case "missing-plan","software-path-missing-owner","circuit-plan":note:=d.f.noteFor("REVIEW-DELIVERY");plan:=note["project_queues"].(map[string]any);if change=="missing-plan" {delete(note,"project_queues")} else if change=="circuit-plan" {plan["kind"]="circuit"} else {plan["owners"]=plan["owners"].([]any)[:4]};d.f.updateNoteFor("REVIEW-DELIVERY",note)
		case "duplicate-slot":d.register();note:=d.f.taskNote();note["queue_assignments"]=append(note["queue_assignments"].([]any),rpClone(t,d.slot(note)));d.f.updateNote(note)
		case "wrong-controller-session":d.f.allow("task_graph_update",d.f.taskUpdate("arbor",d.f.executionID,map[string]any{"session_id":"wrong-session"}))
		}
		d.f.deny("task_graph_tdd_produce",d.request("start","invalid-start",d.base));if len(d.rows())!=0 || d.f.leafNote()["tdd_execution_ref"]!=nil {t.Fatal("missing authority created fake lineage or assignment")}
	})}
}

func TestTDDGreenRequiresExactCurrentIndependentReviewAndTests(t *testing.T) {
	for _,change:=range []string{"missing-review","changed-test-hash","changed-test-revision","stale-production","cross-review","old-generation"} {t.Run(change,func(t *testing.T){
		d:=newTDDFixture(t);d.start();d.register();d.red("tdd-red",d.tests)
		if change!="missing-review" {d.review("test-review")} else {note:=d.f.taskNote();d.slot(note)["queue"]="build-code";d.f.updateNote(note)}
		in:=d.request("green","bad-green",d.code)
		if change=="changed-test-hash" || change=="changed-test-revision" {note:=d.f.taskNote();tests:=d.slot(note)["tests"].(map[string]any);if change=="changed-test-hash" {tests["files"].([]any)[0].(map[string]any)["sha256"]=rpDigest([]byte("changed"))} else {tests["revision"]=d.repaired};d.f.updateNote(note)}
		if change=="stale-production" {in["revision"]=d.base};if change=="old-generation" {in["generation_id"]="old"}
		if change=="cross-review" {note:=d.f.taskNote();d.slot(note)["tests"].(map[string]any)["tdd_audit_receipt"].(map[string]any)["review_ref"].(map[string]any)["task_id"]="OTHER-TASK";d.f.denyOrigin("task_graph_update",d.f.taskUpdate("arbor",d.f.executionID,map[string]any{"note":string(rpJSON(t,note))}));return}
		before:=d.rows();d.f.deny("task_graph_tdd_produce",in);if !reflect.DeepEqual(before,d.rows()) {t.Fatal("GREEN accepted missing/stale review or changed frozen tests")}
	})}
}

func TestTDDReopenFixtureOnlyPreservesActualPriorProductionWithoutNewCommit(t *testing.T) {
	d:=newTDDFixture(t);d.throughGreen();d.productionReview();oldGreen:=rpClone(t,d.greenRef)
	out:=d.f.allow("task_graph_tdd_produce",d.request("reopen","fixture-reopen",d.repaired));ref:=out["tdd_ref"].(map[string]any);slot:=d.slot(d.f.taskNote())
	if slot["queue"]!="build-tests" || ref["generation_id"]==oldGreen["generation_id"] || slot["tests"].(map[string]any)["revision"]!=d.repaired || !reflect.DeepEqual(out["retained_production_ref"],d.productionRef) {t.Fatal("fixture-only reopen lost actual production proof or fresh test generation")}
	for _,key:=range []string{"tdd_red_receipt","tdd_green_receipt","acceptance"} {if slot[key]!=nil {t.Fatalf("reopen retained affected downstream %s",key)}}
	tests:=slot["tests"].(map[string]any);if tests["acceptance"]!=nil || tests["tdd_audit_receipt"]!=nil || !reflect.DeepEqual(slot["reopen_history"].([]any)[0],ref) {t.Fatal("reopen failed to invalidate review/audit or record exact immutable lineage")}
	got:=d.f.allow("task_graph_tdd_get",ref);if !reflect.DeepEqual(got["retained_production_ref"],d.productionRef) {t.Fatal("typed lineage reader did not reverify unchanged production origin")}
	d.f.deny("task_graph_tdd_get",oldGreen);d.f.deny("task_graph_review_get",d.productionRef)
	d.red("repaired-red",d.repaired);d.review("repaired-test-review");d.green("repaired-green",d.code)
	got=d.f.allow("task_graph_tdd_get",d.greenRef);if !reflect.DeepEqual(got["retained_production_ref"],d.productionRef) {t.Fatal("repaired GREEN demanded a fake production commit/review despite unchanged reviewed blobs")}
	last:=tdCaptures(t,d.capture);source:=last[len(last)-1]["bundle"].(map[string]any)["source"].(map[string]any)
	if source["production_revision"]!=d.code || source["test_revision"]!=d.repaired {t.Fatal("repaired GREEN did not bind the two actual immutable source layers")}
	seenRepairedRed:=false;for _,item:=range last {bundle:=item["bundle"].(map[string]any);src:=bundle["source"].(map[string]any);if bundle["action"]=="red" && src["test_revision"]==d.repaired {for _,raw:=range src["files"].([]any) {file:=raw.(map[string]any);if file["path"]=="operation.go" && strings.Contains(file["content"].(string),"return 0") {seenRepairedRed=true}}}};if !seenRepairedRed {t.Fatal("reopened RED was not run against the independently recorded preimplementation base")}
}

func TestTDDReopenRejectsFalseFixtureAuthorityAndProductionChangesDropRetention(t *testing.T) {
	for _,mode:=range []string{"valid-production-change","lie-fixture","partial","blocking","malformed","timeout","caller-fixture-label"} {t.Run(mode,func(t *testing.T){
		d:=newTDDFixture(t);d.throughGreen();d.productionReview();revision:=d.repaired;if mode=="valid-production-change" || mode=="lie-fixture" {revision=d.changed}
		in:=d.request("reopen","reopen-test",revision);if mode=="caller-fixture-label" {in["fixture_only"]=true} else if mode!="valid-production-change" {rpWrite(t,d.f.control,[]byte(mode))}
		beforeRows,beforeLeaf,beforeController:=d.rows(),d.f.leafNote(),d.f.taskNote()
		if mode=="valid-production-change" {out:=d.f.allow("task_graph_tdd_produce",in);got:=d.f.allow("task_graph_tdd_get",out["tdd_ref"].(map[string]any));if got["retained_production_ref"]!=nil || d.slot(d.f.taskNote())["acceptance"]!=nil {t.Fatal("changed production inherited fixture-only prior approval")};return}
		d.f.deny("task_graph_tdd_produce",in);if !reflect.DeepEqual(beforeRows,d.rows()) || !reflect.DeepEqual(beforeLeaf,d.f.leafNote()) || !reflect.DeepEqual(beforeController,d.f.taskNote()) {t.Fatal("failed/false reopen audit changed lineage/progression")}
	})}
}

func TestTDDConcurrentExactRetryAndStaleRequestCAS(t *testing.T) {
	d:=newTDDFixture(t);d.start();d.register();in:=d.request("red","concurrent-red",d.tests)
	var wg sync.WaitGroup;results:=make(chan map[string]any,2);failures:=make(chan error,2)
	for range 2 {wg.Add(1);go func(){defer wg.Done();out,err:=d.f.call("task_graph_tdd_produce",in);results<-out;failures<-err}()};wg.Wait();close(results);close(failures)
	for err:=range failures {if err!=nil {t.Fatal(err)}};var ref any;for out:=range results {if ref==nil {ref=out["tdd_ref"]} else if !reflect.DeepEqual(ref,out["tdd_ref"]) {t.Fatal("concurrent exact requests diverged")}}
	beforeCapture:=len(tdCaptures(t,d.capture));out:=d.f.allow("task_graph_tdd_produce",in);if out["idempotent"]!=true || len(d.rows())!=2 || len(tdCaptures(t,d.capture))!=beforeCapture {t.Fatal("completed retry reran execution or duplicated origin")}
	for _,change:=range []string{"revision","previous_ref","expected_queue"} {bad:=rpClone(t,in);bad[change]="changed";d.f.deny("task_graph_tdd_produce",bad)}
	stale:=rpClone(t,in);stale["request_id"]="different-request-same-old-stage";d.f.deny("task_graph_tdd_produce",stale)
}

func TestTDDExternalExecutionPreservesMetadataButRejectsChangedFrozenInput(t *testing.T) {
	for _,change:=range []string{"metadata","protected-input"} {t.Run(change,func(t *testing.T){
		d:=newTDDFixture(t);d.start();d.register();d.f.cfg["tdd_executor"].(map[string]any)["timeout_milliseconds"]=5000;d.f.start();rpWrite(t,d.control,[]byte("hold"))
		in:=d.request("red","held-red",d.tests);done:=make(chan error,1);go func(){_,err:=d.f.call("task_graph_tdd_produce",in);done<-err}();rpWaitCaptures(t,d.capture,2)
		note:=d.f.taskNote();if change=="metadata" {note["ordinary"]="concurrent metadata survives";extra:=rpClone(t,d.slotTemplate);extra["task_id"]="OTHER-SECOND";extra["worktree"]=d.f.recordedWorktree+"-second";note["queue_assignments"]=append(note["queue_assignments"].([]any),extra)} else {d.slot(note)["tests"].(map[string]any)["files"].([]any)[0].(map[string]any)["sha256"]=rpDigest([]byte("changed during run"))};d.f.updateNote(note)
		leaf:=d.f.leafNote();leaf["ordinary"]="concurrent leaf survives";d.f.updateNoteFor("REVIEW-TASK",leaf);rpWrite(t,d.control,[]byte("valid"));err:=<-done
		if change=="metadata" {if err!=nil || d.f.taskNote()["ordinary"]!="concurrent metadata survives" || d.f.leafNote()["ordinary"]!="concurrent leaf survives" || len(d.f.taskNote()["queue_assignments"].([]any))!=3 || len(tdCaptures(t,d.capture))!=2 {t.Fatalf("execution held transaction or lost unrelated metadata: %v",err)}} else if err==nil || len(d.rows())!=1 {t.Fatal("changed input was published after real external run")}
	})}
}

func TestTDDGenericRoutesAndTamperedOriginsNeverAuthorizeExecution(t *testing.T) {
	for _,change:=range []string{"leaf-reference","red-reference","green-reference","audit-reference","reopen-array","whole-note","storage-result","storage-binding"} {t.Run(change,func(t *testing.T){
		d:=newTDDFixture(t);d.throughGreen();before:=d.rows()
		if strings.HasPrefix(change,"storage-") {column:="result_json";if change=="storage-binding" {column="binding_json"};if _,err:=d.f.client.Apply(context.Background(),[]*spanner.Mutation{spanner.Update("TDDExecutionEvidence",[]string{"project_id","evidence_id",column},[]any{"arbor","tdd-green",`{"verified":true}`})});err!=nil {t.Fatal(err)};d.f.deny("task_graph_tdd_get",d.greenRef);return}
		id:="REVIEW-TASK";note:=d.f.leafNote();forged:=rpClone(t,d.greenRef);forged["task_id"]="OTHER-TASK"
		if change=="leaf-reference" {note["tdd_execution_ref"]=forged} else if change=="whole-note" {note=map[string]any{"ordinary":"replacement must not erase origin"}} else {id=d.f.executionID;note=d.f.taskNote();slot:=d.slot(note);switch change {case "red-reference":slot["tdd_red_receipt"]=forged;case "green-reference":slot["tdd_green_receipt"]=forged;case "audit-reference":slot["tests"].(map[string]any)["tdd_audit_receipt"]=map[string]any{"review_ref":d.reviewRef,"tdd_ref":forged};case "reopen-array":slot["reopen_history"]=[]any{forged}}}
		d.f.denyOrigin("task_graph_update",d.f.taskUpdate("arbor",id,map[string]any{"note":string(rpJSON(t,note)),"title":"must roll back"}));if !reflect.DeepEqual(before,d.rows()) {t.Fatal("generic mutation changed execution origin")}
		if change=="leaf-reference" {contract:=map[string]any{"canonical_name":"FORGED-TDD","task_ref":"FORGED-TDD","level":"task"};payload:=map[string]any{"title":"forged","pillar":"TEST","status":"not_started","owner":"UNOWNED","note":string(rpJSON(t,note))};d.f.denyOrigin("task_graph_create",map[string]any{"project_id":"arbor","node_kind":"task","stable_id":"FORGED-TDD","contract":contract,"payload":payload});d.f.denyOrigin("task_graph_import",map[string]any{"submissions":[]any{map[string]any{"node_kind":"task","stable_id":"FORGED-TDD","contract":contract,"payload":payload}}})}
	})}
}

func TestTDDMissingOrLegacyExecutorCapabilityRefusesWithoutBlockingDocuments(t *testing.T) {
	for _,missing:=range []string{"executor","descriptor","table","changed-pin","generation","legacy-protocol"} {t.Run(missing,func(t *testing.T){
		d:=newTDDFixture(t)
		if missing=="legacy-protocol" {rpWrite(t,d.control,[]byte("legacy-receipt"));d.f.deny("task_graph_tdd_produce",d.request("start","old-executor",d.base));if len(d.rows())!=0 {t.Fatal("legacy exit-only wrapper minted execution lineage")};return}
		d.start();d.register();switch missing {case "executor":delete(d.f.cfg,"tdd_executor");case "descriptor":delete(d.f.cfg["substrates"].([]any)[0].(map[string]any),"tdd_evidence_table");case "table":d.f.cfg["substrates"].([]any)[0].(map[string]any)["tdd_evidence_table"]="MissingTDDTable";case "changed-pin":program:=d.f.cfg["tdd_executor"].(map[string]any)["executable"].(string);file,err:=os.OpenFile(program,os.O_APPEND|os.O_WRONLY,0700);if err!=nil {t.Fatal(err)};_,err=file.WriteString("changed pinned bytes");_ = file.Close();if err!=nil {t.Fatal(err)};case "generation":d.f.cfg["tdd_executor"].(map[string]any)["generation"]="unqualified-replacement"};d.f.start()
		d.f.deny("task_graph_tdd_produce",d.request("red","no-capability",d.tests));d.f.deny("task_graph_tdd_get",d.startRef);leaf:=d.f.leafNote();leaf["ordinary"]="document with unavailable executor";d.f.updateNoteFor("REVIEW-TASK",leaf);if len(d.rows())!=1 {t.Fatal("missing executor capability changed lineage")}
	})}
}

func TestTDDTypedReadsRequireExclusiveCurrentScopeNotReviewOrOrdinaryNotes(t *testing.T) {
	d:=newTDDFixture(t);d.throughGreen();d.f.allow("task_graph_tdd_get",d.greenRef);before:=d.rows()
	for _,field:=range []string{"project_id","task_id","execution_project_id","execution_task_id","slot_key","binding_link_id","action","generation_id","revision","test_revision","production_revision","base_revision","epoch","context_sha256","input_sha256","producer_generation"} {bad:=rpClone(t,d.greenRef);bad[field]="wrong-but-shaped";if strings.Contains(field,"sha256") || field=="slot_key" {bad[field]=strings.Repeat("9",64)};d.f.deny("task_graph_tdd_get",bad)}
	d.f.deny("task_graph_tdd_get",d.reviewRef);d.f.deny("task_graph_review_get",d.greenRef)
	contract:=map[string]any{"artifact_type_kind":"finding","artifact_ref":"ordinary execution note"}
	d.f.allow("task_graph_create",map[string]any{"project_id":"arbor","node_kind":"artifact","stable_id":"tdd-green","contract":contract,"payload":map[string]any{"task_id":"REVIEW-TASK","note":string(rpJSON(t,d.greenRef))}})
	d.f.allow("task_graph_update",map[string]any{"project_id":"arbor","node_id":"tdd-green","contract":contract,"payload":map[string]any{"note":"ordinary editable execution-related prose"}})
	d.f.allow("task_graph_archive_worklink",map[string]any{"project_id":"arbor","task_id":"REVIEW-TASK","link_id":"tdd-green"});d.f.allow("task_graph_tdd_get",d.greenRef)
	d.f.deny("task_graph_query",map[string]any{"sql":"UPDATE TDDExecutionEvidence SET result_json = '{}' WHERE project_id = 'arbor'"})
	if !reflect.DeepEqual(before,d.rows()) {t.Fatal("generic note/ID collision/query changed exclusive execution origin")}
	note:=d.f.taskNote();d.slot(note)["tests"].(map[string]any)["revision"]=d.repaired;d.f.updateNote(note);d.f.deny("task_graph_tdd_get",d.greenRef)
}
