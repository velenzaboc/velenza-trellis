package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// reviewProducerFault is a transaction-failure seam, analogous to
// nativeWriterHandoverFault. Production leaves it nil. It supplies neither a
// reviewer nor review-origin evidence; the real process owns that evidence.
var reviewProducerFault func(stage string) error

type reviewInput struct {
	ProjectID string `json:"project_id,omitempty"`
	TaskID string `json:"task_id"`
	ExecutionProjectID string `json:"execution_project_id,omitempty"`
	ExecutionTaskID string `json:"execution_task_id,omitempty"`
	SlotID string `json:"slot_id,omitempty"`
	SlotKey string `json:"slot_key,omitempty"`
	BindingLinkID string `json:"binding_link_id"`
	Kind string `json:"kind"`
	Revision string `json:"revision"`
	BaseRevision string `json:"base_revision,omitempty"`
	Epoch string `json:"epoch"`
	ContextSHA256 string `json:"context_sha256"`
	ReviewID string `json:"review_id"`
	InputSHA256 string `json:"input_sha256,omitempty"`
	ProducerGeneration string `json:"producer_generation,omitempty"`
}

func (in *reviewInput) UnmarshalJSON(data []byte) error {
	if err := reviewClosedFields(data,map[string]bool{"project_id":true,"task_id":true,"execution_project_id":true,"execution_task_id":true,"slot_id":true,"slot_key":true,"binding_link_id":true,"kind":true,"revision":true,"base_revision":true,"epoch":true,"context_sha256":true,"review_id":true,"input_sha256":true,"producer_generation":true}); err != nil { return err }
	type wire reviewInput
	var value wire
	if err := reviewStrictJSON(data,&value); err != nil { return err }
	*in = reviewInput(value)
	return nil
}

type reviewQueryReader interface { Query(context.Context,spanner.Statement) *spanner.RowIterator }

type reviewTaskState struct {
	Owner, Session, Repo, Parent, RawNote string
	Note map[string]interface{}
}

type reviewState struct {
	Leaf, Controller reviewTaskState
	Binding nativeWorklinkState
	Slot map[string]interface{}
	Identity map[string]interface{}
	Frozen map[string]interface{}
	CAS string
}

func reviewReadTask(ctx context.Context,q reviewQueryReader,a *SpannerSubstrateAdapter,project,id string) (reviewTaskState,error) {
	iter := q.Query(ctx,spanner.Statement{SQL:`SELECT owner, session_id, repo, parent_id, note FROM `+a.Descriptor.TaskTable+` WHERE `+a.projectWhereAnd()+`id = @id`,Params:map[string]interface{}{"project_id":project,"id":id}})
	defer iter.Stop()
	row,err := iter.Next()
	if err != nil { return reviewTaskState{},fmt.Errorf("review task %s/%s unavailable: %w",project,id,err) }
	var owner,session,repo,parent,note spanner.NullString
	if err := row.Columns(&owner,&session,&repo,&parent,&note); err != nil { return reviewTaskState{},err }
	state := reviewTaskState{Owner:nullStringValue(owner),Session:nullStringValue(session),Repo:nullStringValue(repo),Parent:nullStringValue(parent),RawNote:nullStringValue(note)}
	if err := reviewStrictJSON([]byte(state.RawNote),&state.Note); err != nil || state.Note == nil { return reviewTaskState{},fmt.Errorf("review task note must be one strict object: %v",err) }
	return state,nil
}

func reviewReadBinding(ctx context.Context,q reviewQueryReader,a *SpannerSubstrateAdapter,project,task,id string) (nativeWorklinkState,error) {
	iter := q.Query(ctx,spanner.Statement{SQL:`SELECT `+nativeWorklinkStateColumns+` FROM `+a.Descriptor.WorklinkTable+` WHERE `+a.projectWhereAnd()+`task_id = @task_id`,Params:map[string]interface{}{"project_id":project,"task_id":task}})
	defer iter.Stop()
	var links []nativeWorklinkState
	var selected nativeWorklinkState
	for {
		row,err := iter.Next()
		if err == iterator.Done { break }
		if err != nil { return selected,err }
		link,err := nativeScanWorklinkState(row)
		if err != nil { return selected,err }
		links = append(links,link)
		if link.LinkID == id { selected = link }
	}
	if selected.LinkID == "" || selected.ArtifactType != "worktree" || selected.Status != worklinkRecordedStatus || selected.Repo == "" || selected.Branch == "" || selected.WorktreePath == "" || selected.Thread == "" || selected.SessionID == "" || nativeWriterBindingCount(links,selected.Repo) != 1 {
		return nativeWorklinkState{},fmt.Errorf("review requires exactly one coherent canonical active worktree binding")
	}
	return selected,nil
}

func reviewSlots(note map[string]interface{}) ([]map[string]interface{},error) {
	var slots []map[string]interface{}
	if raw,exists := note["queue_assignments"]; exists {
		array,ok := raw.([]interface{})
		if !ok { return nil,fmt.Errorf("queue_assignments must be an array") }
		for _, rawSlot := range array {
			slot,ok := rawSlot.(map[string]interface{})
			if !ok { return nil,fmt.Errorf("invalid execution slot") }
			slots = append(slots,slot)
		}
	}
	if raw,exists := note["queue_assignment"]; exists {
		slot,ok := raw.(map[string]interface{})
		if !ok { return nil,fmt.Errorf("legacy execution slot must be an object") }
		slots = append(slots,slot)
	}
	return slots,nil
}

func reviewSlotKey(project,task,controllerProject,controller string,binding nativeWorklinkState) string {
	var tuple strings.Builder
	for _, value := range []string{"velenza-review-slot/1",project,task,controllerProject,controller,binding.LinkID,nativeNormalizeRepository(binding.Repo),binding.WorktreePath,binding.Branch,binding.Thread,binding.SessionID} {
		fmt.Fprintf(&tuple,"%d:%s",len([]byte(value)),value)
	}
	return reviewHashBytes([]byte(tuple.String()))
}

func reviewWithoutAcceptance(tests map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for key,value := range tests { if key != "acceptance" && key != "tdd_audit_receipt" { out[key] = value } }
	return out
}

func reviewLoadState(ctx context.Context,q reviewQueryReader,a *SpannerSubstrateAdapter,env *Env,in reviewInput) (reviewState,error) {
	state := reviewState{}
	var err error
	state.Leaf,err = reviewReadTask(ctx,q,a,in.ProjectID,in.TaskID)
	if err != nil { return state,err }
	state.Binding,err = reviewReadBinding(ctx,q,a,in.ProjectID,in.TaskID,in.BindingLinkID)
	if err != nil { return state,err }
	binding := state.Binding
	if strings.TrimSpace(state.Leaf.Owner) == "" || strings.EqualFold(state.Leaf.Owner,"UNOWNED") || state.Leaf.Session != binding.SessionID || nativeNormalizeRepository(state.Leaf.Repo) != nativeNormalizeRepository(binding.Repo) { return state,fmt.Errorf("review leaf ownership and canonical binding disagree") }
	controller := "EXECUTION-QUEUE-"+binding.SessionID
	controllerProject,err := resolveNativeProjectID(env,a,"")
	if err != nil { return state,err }
	if in.TaskID == controller || (in.ExecutionTaskID != "" && in.ExecutionTaskID != controller) || (in.ExecutionProjectID != "" && in.ExecutionProjectID != controllerProject) { return state,fmt.Errorf("review controller must be the exact binding-session execution Task") }
	state.Controller,err = reviewReadTask(ctx,q,a,controllerProject,controller)
	if err != nil { return state,err }
	if state.Controller.Session != binding.SessionID { return state,fmt.Errorf("review controller session does not match canonical binding") }
	slots,err := reviewSlots(state.Controller.Note)
	if err != nil { return state,err }
	for _, slot := range slots {
		if slot["active"] != true || slot["project_id"] != in.ProjectID || slot["task_id"] != in.TaskID || nativeNormalizeRepository(stringFromMap(slot,"repo")) != nativeNormalizeRepository(binding.Repo) || slot["worktree"] != binding.WorktreePath { continue }
		if state.Slot != nil { return state,fmt.Errorf("ambiguous recorded review execution slot") }
		state.Slot = slot
	}
	if state.Slot == nil { return state,fmt.Errorf("no active recorded slot matches exact review leaf/repo/worktree") }
	slot := state.Slot
	scope,ok := slot["project_queue"].(map[string]interface{})
	if !ok || scope["project_id"] != in.ProjectID || stringFromMap(scope,"task_id") == "" || scope["epoch"] != in.Epoch || scope["context_sha256"] != in.ContextSHA256 { return state,fmt.Errorf("review scope is missing or stale") }
	if leafScope,exists := state.Leaf.Note["project_queue"]; exists && !reviewEqual(leafScope,scope) { return state,fmt.Errorf("recorded leaf scope contradicts selected slot") }
	delivery,err := reviewReadTask(ctx,q,a,in.ProjectID,stringFromMap(scope,"task_id"))
	if err != nil { return state,err }
	plan,ok := delivery.Note["project_queues"].(map[string]interface{})
	if !ok || plan["project_id"] != in.ProjectID || plan["task_id"] != scope["task_id"] || plan["epoch"] != in.Epoch || plan["context_sha256"] != in.ContextSHA256 || plan["kind"] != "software" { return state,fmt.Errorf("review delivery context is missing or stale") }
	validStage := false
	for _, queue := range []string{"build-tests","validate-tests","build-code","test-code","merge"} { if slot["queue"] == queue { validStage = true } }
	if !validStage { return state,fmt.Errorf("review slot is outside its software project path") }
	key := reviewSlotKey(in.ProjectID,in.TaskID,controllerProject,controller,binding)
	if (in.SlotKey != "" && in.SlotKey != key) || (in.SlotID != "" && in.SlotID != slot["slot_id"]) { return state,fmt.Errorf("review slot identity assertion does not match recorded state") }
	tests,ok := slot["tests"].(map[string]interface{})
	if !ok || stringFromMap(tests,"revision") == "" || stringFromMap(tests,"author") == "" { return state,fmt.Errorf("frozen executable test handoff required") }
	revision := stringFromMap(tests,"revision")
	if in.Kind == "production" {
		revision = stringFromMap(slot,"revision")
		if in.BaseRevision == "" || in.BaseRevision != slot["base_revision"] { return state,fmt.Errorf("production review requires exact recorded base") }
	} else if in.BaseRevision != "" { return state,fmt.Errorf("validation review must not claim production base") }
	if revision == "" || in.Revision != revision { return state,fmt.Errorf("review revision does not match actual frozen slot") }
	state.Identity = map[string]interface{}{"project_id":in.ProjectID,"task_id":in.TaskID,"execution_project_id":controllerProject,"execution_task_id":controller,"slot_key":key,"binding_link_id":binding.LinkID,"kind":in.Kind,"revision":revision,"epoch":in.Epoch,"context_sha256":in.ContextSHA256}
	if id,exists := slot["slot_id"]; exists {
		if value,ok := id.(string); !ok || value == "" { return state,fmt.Errorf("invalid recorded optional slot_id") }
		state.Identity["slot_id"] = id
	}
	if in.Kind == "production" { state.Identity["base_revision"] = in.BaseRevision }
	state.Frozen = map[string]interface{}{
		"identity":state.Identity,"binding":binding,"leaf_owner":state.Leaf.Owner,"leaf_session":state.Leaf.Session,"leaf_repo":state.Leaf.Repo,"leaf_parent":state.Leaf.Parent,
		"controller_owner":state.Controller.Owner,"controller_session":state.Controller.Session,"controller_repo":state.Controller.Repo,
		"tests":reviewWithoutAcceptance(tests),"governing_files":slot["governing_files"],"project_queue":scope,
	}
	if in.Kind == "production" { state.Frozen["production_files"] = slot["production_files"] }
	// Ordinary metadata and disjoint slots are deliberately outside this CAS.
	// Protected output is compared separately so a concurrent producer cannot
	// overwrite another publication based on a stale pre-review observation.
	protected := map[string]interface{}{"leaf":state.Leaf.Note["validation_acceptance"],"slot_acceptance":slot["acceptance"],"tests_acceptance":tests["acceptance"],"red":slot["tdd_red_receipt"],"audit":tests["tdd_audit_receipt"],"green":slot["tdd_green_receipt"],"reopen":slot["reopen_history"]}
	cas,err := reviewJSON(map[string]interface{}{"frozen":state.Frozen,"protected":protected})
	if err != nil { return state,err }
	state.CAS = reviewHashBytes(cas)
	return state,nil
}

type reviewMaterialized struct {
	Bundle map[string]interface{}
	Tests []reviewSourceFile
	Requirements []string
	Production *reviewProductionInput
}

func reviewMaterialize(ctx context.Context,state reviewState,config *ReviewProducerConfig) (reviewMaterialized,error) {
	out := reviewMaterialized{}
	var repository *ReviewRepositoryConfig
	for i := range config.Repositories {
		if nativeNormalizeRepository(config.Repositories[i].Repo) == nativeNormalizeRepository(state.Binding.Repo) { repository = &config.Repositories[i] }
	}
	if repository == nil { return out,fmt.Errorf("verified review repository acquisition is not configured") }
	tests := state.Slot["tests"].(map[string]interface{})
	testRevision := stringFromMap(tests,"revision")
	if err := reviewVerifyCommit(ctx,repository.Path,testRevision); err != nil { return out,err }
	var err error
	out.Tests,err = reviewAcquireFiles(ctx,repository.Path,testRevision,tests["files"])
	if err != nil { return out,err }
	ownerRevision := testRevision
	if state.Identity["kind"] == "production" { ownerRevision = stringFromMap(state.Identity,"revision") }
	if err := reviewVerifyCommit(ctx,repository.Path,ownerRevision); err != nil { return out,err }
	owners,err := reviewAcquireFiles(ctx,repository.Path,ownerRevision,state.Slot["governing_files"])
	if err != nil { return out,err }
	configuredOwner := false
	for _, file := range owners { if file.Path == repository.RequirementsPath { configuredOwner = true } }
	if !configuredOwner { return out,fmt.Errorf("full configured governing owner is required") }
	rawRequirements,ok := tests["requirements"].([]interface{})
	if !ok || len(rawRequirements) == 0 { return out,fmt.Errorf("frozen requirement identities required") }
	seen := map[string]bool{}
	for _, raw := range rawRequirements {
		req,ok := raw.(string)
		if !ok || strings.TrimSpace(req) == "" || seen[req] { return out,fmt.Errorf("invalid frozen requirement identity") }
		// Requirement IDs may be opaque owner anchors. The actual reviewer
		// receives their complete governing source and assertion bodies;
		// substring occurrence of an ID is not semantic coverage proof.
		seen[req] = true; out.Requirements = append(out.Requirements,req)
	}
	out.Bundle = map[string]interface{}{}
	for key,value := range state.Identity { out.Bundle[key] = value }
	out.Bundle["binding"] = state.Frozen["binding"]
	out.Bundle["test_revision"] = testRevision
	out.Bundle["test_author"] = tests["author"]
	out.Bundle["requirements"] = out.Requirements
	out.Bundle["test_files"] = out.Tests
	out.Bundle["governing_files"] = owners
	out.Bundle["producer_generation"] = config.Generation
	out.Bundle["producer_executable_sha256"] = config.ExecutableSHA256
	if state.Identity["kind"] == "production" {
		production,err := reviewAcquireProduction(ctx,repository.Path,stringFromMap(state.Identity,"base_revision"),stringFromMap(state.Identity,"revision"),state.Slot["production_files"])
		if err != nil { return out,err }
		out.Production = &production; out.Bundle["production"] = production
	}
	// Normalize the exact stdin value before computing its encoding/json digest;
	// a child decoding this same object sees the identical flat JSON projection.
	encoded,err := reviewJSON(out.Bundle)
	if err != nil { return out,err }
	var normalized map[string]interface{}
	if err := reviewStrictJSON(encoded,&normalized); err != nil { return out,err }
	encoded,err = reviewJSON(normalized)
	if err != nil { return out,err }
	normalized["input_sha256"] = reviewHashBytes(encoded)
	out.Bundle = normalized
	return out,nil
}

type reviewStoredBinding struct {
	SchemaVersion string `json:"schema_version"`
	Identity map[string]interface{} `json:"identity"`
	Frozen map[string]interface{} `json:"frozen"`
	Input map[string]interface{} `json:"input"`
	Process reviewCapturedProcess `json:"process"`
}

type reviewStoredRow struct {
	Project,ID,Task,ExecutionProject,ExecutionTask,Slot,Kind,Input,Binding,Result,Generation string
}

func reviewReadOrigin(ctx context.Context,q reviewQueryReader,table,project,id string) (reviewStoredRow,bool,error) {
	iter := q.Query(ctx,spanner.Statement{SQL:`SELECT project_id, review_id, task_id, execution_project_id, execution_task_id, slot_key, kind, input_sha256, binding_json, result_json, producer_generation FROM `+table+` WHERE project_id = @project_id AND review_id = @review_id`,Params:map[string]interface{}{"project_id":project,"review_id":id}})
	defer iter.Stop()
	row,err := iter.Next()
	if err == iterator.Done { return reviewStoredRow{},false,nil }
	if err != nil { return reviewStoredRow{},false,err }
	var stored reviewStoredRow
	err = row.Columns(&stored.Project,&stored.ID,&stored.Task,&stored.ExecutionProject,&stored.ExecutionTask,&stored.Slot,&stored.Kind,&stored.Input,&stored.Binding,&stored.Result,&stored.Generation)
	return stored,true,err
}

func reviewReference(state reviewState,id,input,generation string) map[string]interface{} {
	ref := map[string]interface{}{}
	for key,value := range state.Identity { ref[key] = value }
	ref["review_id"],ref["input_sha256"],ref["producer_generation"] = id,input,generation
	return ref
}

func reviewResponse(state reviewState,id string,material reviewMaterialized,result map[string]interface{},generation string,idempotent bool) map[string]interface{} {
	ref := reviewReference(state,id,stringFromMap(material.Bundle,"input_sha256"),generation)
	out := map[string]interface{}{}
	for key,value := range ref { out[key] = value }
	out["schema_version"],out["review_ref"],out["result"],out["idempotent"] = "velenza_review_result/1",ref,result,idempotent
	return out
}

func reviewCheckStored(stored reviewStoredRow,state reviewState,in reviewInput,material reviewMaterialized,config *ReviewProducerConfig) (map[string]interface{},error) {
	if stored.Project != in.ProjectID || stored.ID != in.ReviewID || stored.Task != in.TaskID || stored.ExecutionProject != state.Identity["execution_project_id"] || stored.ExecutionTask != state.Identity["execution_task_id"] || stored.Slot != state.Identity["slot_key"] || stored.Kind != in.Kind || stored.Input != material.Bundle["input_sha256"] || stored.Generation != config.Generation { return nil,fmt.Errorf("stored review scope/source/generation mismatch") }
	var binding reviewStoredBinding
	if err := reviewStrictJSON([]byte(stored.Binding),&binding); err != nil { return nil,err }
	if binding.SchemaVersion != "velenza_review_origin_binding/1" || !reviewEqual(binding.Identity,state.Identity) || !reviewEqual(binding.Frozen,state.Frozen) || !reviewEqual(binding.Input,material.Bundle) || !reviewProcessMatches(binding.Process,config) || binding.Process.Stdout != stored.Result { return nil,fmt.Errorf("stored review has stale/tampered execution binding") }
	return reviewValidateResult(stored.Result,material.Bundle,material.Tests,material.Requirements,material.Production)
}

func reviewCapability(ctx context.Context,env *Env,in *reviewInput) (*SpannerSubstrateAdapter,*spanner.Client,[]byte,error) {
	if in.Kind != "validate_tests" && in.Kind != "production" { return nil,nil,nil,fmt.Errorf("review kind must be validate_tests or production") }
	if in.TaskID == "" || in.BindingLinkID == "" || strings.TrimSpace(in.ReviewID) == "" || len(in.ReviewID) > 64 || in.Epoch == "" || !reviewDigestPattern.MatchString(in.ContextSHA256) { return nil,nil,nil,fmt.Errorf("exact review task/binding/request/generation required") }
	if err := validateReviewConfiguration(env.Config); err != nil { return nil,nil,nil,err }
	adapter,err := soleNativeFleetPlanAdapter(ctx,env)
	if err != nil || adapter == nil { return nil,nil,nil,fmt.Errorf("review requires existing native service: %v",err) }
	all,err := env.Substrates.All(ctx)
	if err != nil || len(all) != 1 { return nil,nil,nil,fmt.Errorf("review requires one unambiguous native substrate") }
	if adapter.Descriptor.ReadOnly || adapter.Descriptor.ReviewEvidenceTable == "" { return nil,nil,nil,fmt.Errorf("exclusive review evidence table capability not configured") }
	in.ProjectID,err = resolveNativeProjectID(env,adapter,in.ProjectID)
	if err != nil { return nil,nil,nil,err }
	program,err := reviewProgram(env.Config.ReviewProducer)
	if err != nil { return nil,nil,nil,err }
	if in.ProducerGeneration != "" && in.ProducerGeneration != env.Config.ReviewProducer.Generation { return nil,nil,nil,fmt.Errorf("review producer generation assertion mismatch") }
	client,err := adapter.getClient(ctx)
	return adapter,client,program,err
}

func handleReviewGet(ctx context.Context,env *Env,in reviewInput) (map[string]interface{},error) {
	a,client,_,err := reviewCapability(ctx,env,&in)
	if err != nil { return nil,err }
	read := client.ReadOnlyTransaction(); defer read.Close()
	state,err := reviewLoadState(ctx,read,a,env,in)
	if err != nil { return nil,err }
	stored,found,err := reviewReadOrigin(ctx,read,a.Descriptor.ReviewEvidenceTable,in.ProjectID,in.ReviewID)
	if err != nil { return nil,err }
	if !found { return nil,fmt.Errorf("authoritative review origin not found") }
	material,err := reviewMaterialize(ctx,state,env.Config.ReviewProducer)
	if err != nil { return nil,err }
	if in.InputSHA256 != "" && in.InputSHA256 != material.Bundle["input_sha256"] { return nil,fmt.Errorf("review input digest assertion mismatch") }
	result,err := reviewCheckStored(stored,state,in,material,env.Config.ReviewProducer)
	if err != nil { return nil,err }
	return reviewResponse(state,in.ReviewID,material,result,env.Config.ReviewProducer.Generation,true),nil
}

func handleReviewProduce(ctx context.Context,env *Env,in reviewInput) (map[string]interface{},error) {
	if in.InputSHA256 != "" || in.ProducerGeneration != "" || in.ExecutionProjectID != "" { return nil,fmt.Errorf("read-reference assertions do not authorize a producer request") }
	a,client,program,err := reviewCapability(ctx,env,&in)
	if err != nil { return nil,err }
	read := client.ReadOnlyTransaction()
	state,err := reviewLoadState(ctx,read,a,env,in)
	if err != nil { read.Close(); return nil,err }
	stored,found,err := reviewReadOrigin(ctx,read,a.Descriptor.ReviewEvidenceTable,in.ProjectID,in.ReviewID)
	read.Close()
	if err != nil { return nil,err }
	config := env.Config.ReviewProducer
	material,err := reviewMaterialize(ctx,state,config)
	if err != nil { return nil,err }
	if found {
		result,err := reviewCheckStored(stored,state,in,material,config)
		if err != nil { return nil,err }
		return reviewResponse(state,in.ReviewID,material,result,config.Generation,true),nil
	}
	input,err := reviewJSON(material.Bundle)
	if err != nil { return nil,err }
	captured,err := reviewRunProcess(ctx,config,program,input)
	if err != nil { return nil,err }
	result,err := reviewValidateResult(captured.Stdout,material.Bundle,material.Tests,material.Requirements,material.Production)
	if err != nil { return nil,err }
	bound,err := reviewJSON(reviewStoredBinding{"velenza_review_origin_binding/1",state.Identity,state.Frozen,material.Bundle,captured})
	if err != nil { return nil,err }
	var response map[string]interface{}
	_,err = client.ReadWriteTransaction(ctx,func(ctx context.Context,tx *spanner.ReadWriteTransaction) error {
		current,err := reviewLoadState(ctx,tx,a,env,in)
		if err != nil { return err }
		prior,exists,err := reviewReadOrigin(ctx,tx,a.Descriptor.ReviewEvidenceTable,in.ProjectID,in.ReviewID)
		if err != nil { return err }
		if exists {
			priorResult,err := reviewCheckStored(prior,current,in,material,config)
			if err != nil { return err }
			response = reviewResponse(current,in.ReviewID,material,priorResult,config.Generation,true)
			return nil
		}
		if current.CAS != state.CAS { return fmt.Errorf("review publication lost its exact frozen/protected generation") }
		params := map[string]interface{}{"project_id":in.ProjectID,"review_id":in.ReviewID,"task_id":in.TaskID,"execution_project_id":current.Identity["execution_project_id"],"execution_task_id":current.Identity["execution_task_id"],"slot_key":current.Identity["slot_key"],"kind":in.Kind,"input_sha256":material.Bundle["input_sha256"],"binding_json":string(bound),"result_json":captured.Stdout,"producer_generation":config.Generation,"created_at":env.Now().UTC()}
		_,err = tx.Update(ctx,spanner.Statement{SQL:`INSERT INTO `+a.Descriptor.ReviewEvidenceTable+` (project_id,review_id,task_id,execution_project_id,execution_task_id,slot_key,kind,input_sha256,binding_json,result_json,producer_generation,created_at) VALUES (@project_id,@review_id,@task_id,@execution_project_id,@execution_task_id,@slot_key,@kind,@input_sha256,@binding_json,@result_json,@producer_generation,@created_at)`,Params:params})
		if err != nil { return err }
		if err := reviewPublicationFault("after_evidence_insert"); err != nil { return err }
		ref := reviewReference(current,in.ReviewID,stringFromMap(material.Bundle,"input_sha256"),config.Generation)
		if in.Kind == "validate_tests" {
			tests := current.Slot["tests"].(map[string]interface{})
			reviewer := "server-review-process:"+config.Generation
			if reviewer == tests["author"] { return fmt.Errorf("test author cannot be the review process identity") }
			approval := map[string]interface{}{"schema_version":"velenza_validation_acceptance/1","project_id":in.ProjectID,"task_id":in.TaskID,"context_sha256":in.ContextSHA256,"author":tests["author"],"reviewer":reviewer,"blocking":0,"files":tests["files"],"requirements":tests["requirements"],"evidence":[]string{"task_graph_review_get:"+in.ProjectID+":"+in.ReviewID},"coverage":result["coverage"],"production_boundary":true,"tautologies":false,"fixture_supplied_authority":false,"review_ref":ref}
			approvalBytes,err := reviewConsumerJSON(approval)
			if err != nil { return err }
			current.Leaf.Note["validation_acceptance"] = json.RawMessage(approvalBytes)
			if err := reviewWriteNote(ctx,tx,a,in.ProjectID,in.TaskID,current.Leaf.Note,env.Now()); err != nil { return err }
			if err := reviewPublicationFault("after_leaf_publication"); err != nil { return err }
			tests["acceptance"] = map[string]interface{}{"task_id":in.TaskID,"sha256":reviewHashBytes(approvalBytes),"review_ref":ref}
		} else {
			current.Slot["acceptance"] = map[string]interface{}{"revision":in.Revision,"blocking":0,"review_ref":ref}
		}
		if err := reviewWriteNote(ctx,tx,a,stringFromMap(current.Identity,"execution_project_id"),stringFromMap(current.Identity,"execution_task_id"),current.Controller.Note,env.Now()); err != nil { return err }
		if err := reviewPublicationFault("after_execution_publication"); err != nil { return err }
		response = reviewResponse(current,in.ReviewID,material,result,config.Generation,false)
		return nil
	})
	if err != nil { return nil,fmt.Errorf("review publication refused: %w",err) }
	return response,nil
}

func reviewPublicationFault(stage string) error {
	if reviewProducerFault != nil { return reviewProducerFault(stage) }
	return nil
}

func reviewWriteNote(ctx context.Context,tx *spanner.ReadWriteTransaction,a *SpannerSubstrateAdapter,project,id string,note map[string]interface{},now time.Time) error {
	encoded,err := reviewConsumerJSON(note)
	if err != nil { return err }
	count,err := tx.Update(ctx,spanner.Statement{SQL:`UPDATE `+a.Descriptor.TaskTable+` SET note = @note, updated_at = @updated_at WHERE `+a.projectWhereAnd()+`id = @id`,Params:map[string]interface{}{"project_id":project,"id":id,"note":string(encoded),"updated_at":now.UTC()}})
	if err != nil { return err }
	if count != 1 { return fmt.Errorf("review publication requires exactly one current Task") }
	return nil
}
