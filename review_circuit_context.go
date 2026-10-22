package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
)

// This door preserves existing circuit workflow metadata. It does not produce
// independent software review origin, execute a validator or admit a circuit.
// Generic create/import and schema-v1 writers retain the unconditional fence.
func guardNativeReviewTaskWrite(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, env *Env, project, task string, before, after interface{}, fields map[string]interface{}) error {
	if err := guardReviewNoteWrite(before, after); err == nil { return nil }
	err := reviewCircuitNoteChange(ctx, tx, a, env, project, task, before, after, fields)
	if err != nil { return fmt.Errorf("REVIEW_ORIGIN_REQUIRED: protected write is outside the registered circuit bookkeeping boundary: %w", err) }
	return nil
}

func reviewCircuitNoteChange(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, env *Env, project, task string, before, after interface{}, fields map[string]interface{}) error {
	old, err := reviewProtectedProjection(before)
	if err != nil { return err }
	next, err := reviewProtectedProjection(after)
	if err != nil { return err }
	oldNote, err := reviewNoteObject(before)
	if err != nil { return err }
	newNote, err := reviewNoteObject(after)
	if err != nil { return err }
	// A changed protected publication must not store ambiguous duplicate-key
	// JSON that the next transaction cannot resolve to one circuit context.
	var encoded []byte
	if text, ok := after.(string); ok { encoded = []byte(text) } else { encoded, err = reviewJSON(after) }
	if err != nil { return err }
	var strictNote map[string]interface{}
	if err := reviewStrictJSON(encoded,&strictNote); err != nil || strictNote == nil { return fmt.Errorf("circuit publication requires one unambiguous note object") }
	stored, err := reviewReadTask(ctx, tx, a, project, task)
	if err != nil { return err }
	// Do not approve a prior identity while changing its execution coordinates
	// in the same write. Ordinary identity/metadata updates still use the echo door.
	for key, value := range map[string]string{"owner":stored.Owner,"session_id":stored.Session,"repo":stored.Repo,"parent_id":stored.Parent} {
		if incoming, exists := fields[key]; exists && incoming != value { return fmt.Errorf("protected publication changes Task %s", key) }
	}
	keys := map[string]bool{}
	for key := range old { keys[key] = true }
	for key := range next { keys[key] = true }
	for key := range keys {
		if reviewEqual(old[key], next[key]) { continue }
		if key == "validation_acceptance" {
			if reviewCircuitHasReference(old[key]) || reviewCircuitHasReference(next[key]) { return fmt.Errorf("software review reference cannot be changed by circuit metadata") }
			state, err := reviewCircuitLoadContext(ctx, tx, a, env, project, task)
			if err != nil { return err }
			if state.Slot["queue"] != "review-validation" || !reviewEqual(newNote["project_queue"], stored.Note["project_queue"]) { return fmt.Errorf("circuit review requires its prior review-validation scope") }
			if err := reviewCircuitValidateReceipt(next[key], state); err != nil { return err }
			delete(old, key); delete(next, key)
			continue
		}
		oldSlot, err := reviewCircuitFindSlot(oldNote, key)
		if err != nil { return err }
		newSlot, err := reviewCircuitFindSlot(newNote, key)
		if err != nil { return err }
		leafProject, leafTask := stringFromMap(oldSlot,"project_id"), stringFromMap(oldSlot,"task_id")
		state, err := reviewCircuitLoadContext(ctx, tx, a, env, leafProject, leafTask)
		if err != nil { return err }
		if state.ControllerProject != project || state.ControllerID != task || !reviewEqual(state.Slot, oldSlot) { return fmt.Errorf("publication is not the actual prior circuit controller slot") }
		oldTests, ok := oldSlot["tests"].(map[string]interface{})
		if !ok { return fmt.Errorf("prior circuit tests required") }
		newTests, ok := newSlot["tests"].(map[string]interface{})
		if !ok { return fmt.Errorf("proposed circuit tests required") }
		for _, coordinate := range []string{"active","project_id","task_id","repo","worktree","slot_id","project_queue"} {
			if !reviewEqual(oldSlot[coordinate],newSlot[coordinate]) { return fmt.Errorf("circuit publication changes frozen slot %s", coordinate) }
		}
		if oldSlot["queue"] != "review-validation" || newSlot["queue"] != "author-circuit" || !reviewEqual(reviewWithoutAcceptance(oldTests),reviewWithoutAcceptance(newTests)) { return fmt.Errorf("circuit authoring reference requires the unchanged reviewed handoff") }
		if reviewCircuitHasReference(oldTests["acceptance"]) || reviewCircuitHasReference(newTests["acceptance"]) { return fmt.Errorf("software reference cannot become a circuit reference") }
		acceptance, ok := newTests["acceptance"].(map[string]interface{})
		if !ok || len(acceptance) != 2 || acceptance["task_id"] != leafTask || !reviewDigestPattern.MatchString(stringFromMap(acceptance,"sha256")) { return fmt.Errorf("exact circuit leaf/hash reference required") }
		if err := reviewCircuitValidateReceipt(state.Leaf.Note["validation_acceptance"],state); err != nil { return err }
		digest, err := reviewCircuitApprovalHash(state.Leaf.RawNote)
		if err != nil || acceptance["sha256"] != digest { return fmt.Errorf("circuit reference does not resolve the current exact leaf approval hash") }
		// Remove only the one field this door owns. Every other protected field
		// on this slot, all other slots, and any root approval remain fenced.
		for _, projection := range []map[string]interface{}{old,next} {
			if protected, ok := projection[key].(map[string]interface{}); ok {
				delete(protected,"tests.acceptance")
				if len(protected) == 0 { delete(projection,key) }
			}
		}
	}
	if !reviewEqual(old,next) { return fmt.Errorf("write also changes software acceptance or protected lineage") }
	return nil
}

func reviewCircuitHasReference(value interface{}) bool {
	object, ok := value.(map[string]interface{})
	if !ok { return false }
	_, exists := object["review_ref"]
	return exists
}

func reviewCircuitFindSlot(note map[string]interface{}, key string) (map[string]interface{},error) {
	slots, err := reviewSlots(note)
	if err != nil { return nil,err }
	var found map[string]interface{}
	for _, slot := range slots {
		if reviewProtectedSlotKey(slot) != key { continue }
		if found != nil { return nil,fmt.Errorf("duplicate circuit slot identity") }
		found = slot
	}
	if found == nil { return nil,fmt.Errorf("circuit publication has no prior matching slot") }
	return found,nil
}

type reviewCircuitState struct {
	Leaf reviewTaskState
	Slot, Scope map[string]interface{}
	ControllerProject, ControllerID string
}

func reviewCircuitLoadContext(ctx context.Context, tx *spanner.ReadWriteTransaction, a *SpannerSubstrateAdapter, env *Env, project, task string) (reviewCircuitState,error) {
	out := reviewCircuitState{}
	if strings.TrimSpace(project) == "" || strings.TrimSpace(task) == "" { return out,fmt.Errorf("exact circuit leaf project and Task required") }
	leaf, err := reviewReadTask(ctx,tx,a,project,task)
	if err != nil { return out,err }
	out.Leaf = leaf
	links, err := nativeReadTaskWorklinksTx(ctx,tx,a,project,task)
	if err != nil { return out,err }
	var binding nativeWorklinkState
	for _, link := range links {
		if !nativeIsValidActiveBinding(link) || nativeNormalizeRepository(link.Repo) != nativeNormalizeRepository(leaf.Repo) { continue }
		if binding.LinkID != "" { return out,fmt.Errorf("ambiguous circuit canonical binding") }
		binding = link
	}
	if binding.LinkID == "" || binding.ArtifactType != "worktree" || binding.Status != worklinkRecordedStatus || strings.TrimSpace(binding.Repo) == "" || strings.TrimSpace(leaf.Owner) == "" || strings.EqualFold(leaf.Owner,"UNOWNED") || leaf.Session != binding.SessionID { return out,fmt.Errorf("circuit leaf and sole canonical worktree binding disagree") }
	out.ControllerProject, err = resolveNativeProjectID(env,a,"")
	if err != nil { return out,err }
	out.ControllerID = "EXECUTION-QUEUE-"+binding.SessionID
	if task == out.ControllerID { return out,fmt.Errorf("circuit leaf is its execution controller") }
	controller, err := reviewReadTask(ctx,tx,a,out.ControllerProject,out.ControllerID)
	if err != nil { return out,err }
	if controller.Session != binding.SessionID { return out,fmt.Errorf("circuit controller session contradicts its binding") }
	slots, err := reviewSlots(controller.Note)
	if err != nil { return out,err }
	for _, slot := range slots {
		if slot["active"] != true || slot["worktree"] != binding.WorktreePath { continue }
		if out.Slot != nil { return out,fmt.Errorf("multiple active slots occupy the circuit physical worktree") }
		out.Slot = slot
	}
	if out.Slot == nil || out.Slot["project_id"] != project || out.Slot["task_id"] != task || nativeNormalizeRepository(stringFromMap(out.Slot,"repo")) != nativeNormalizeRepository(binding.Repo) { return out,fmt.Errorf("no exact prior registered circuit execution slot") }
	if id, exists := out.Slot["slot_id"]; exists { if value, ok := id.(string); !ok || strings.TrimSpace(value) == "" { return out,fmt.Errorf("invalid recorded circuit slot ID") } }
	scope, ok := out.Slot["project_queue"].(map[string]interface{})
	if !ok || scope["project_id"] != project || stringFromMap(scope,"task_id") == "" || strings.TrimSpace(stringFromMap(scope,"epoch")) == "" || !reviewDigestPattern.MatchString(stringFromMap(scope,"context_sha256")) || !reviewEqual(leaf.Note["project_queue"],scope) { return out,fmt.Errorf("circuit leaf/slot scope is missing or contradictory") }
	out.Scope = scope
	delivery, err := reviewReadTask(ctx,tx,a,project,stringFromMap(scope,"task_id"))
	if err != nil { return out,err }
	plan, ok := delivery.Note["project_queues"].(map[string]interface{})
	if !ok || !reviewEqual(plan["schema_version"],1) || plan["kind"] != "circuit" || plan["project_id"] != project || plan["task_id"] != scope["task_id"] || plan["epoch"] != scope["epoch"] || plan["context_sha256"] != scope["context_sha256"] || strings.TrimSpace(stringFromMap(plan,"topic")) == "" || strings.TrimSpace(stringFromMap(plan,"rationale")) == "" { return out,fmt.Errorf("current circuit delivery plan is missing or stale") }
	mode := stringFromMap(plan,"mode")
	if mode != "subagents" && mode != "chats" { return out,fmt.Errorf("invalid circuit queue mode") }
	owners, ok := plan["owners"].([]interface{})
	if !ok || len(owners) != 5 { return out,fmt.Errorf("all five circuit queue owners required") }
	stages := map[string]bool{"author-validation":true,"review-validation":true,"author-circuit":true,"validate-circuit":true,"admit-circuit":true}
	seen := map[string]bool{}
	ownerKind := "subagent"; if mode == "chats" { ownerKind = "chat" }
	for _, raw := range owners {
		owner, ok := raw.(map[string]interface{})
		if !ok { return out,fmt.Errorf("invalid circuit queue owner") }
		queue, ref := stringFromMap(owner,"queue"), stringFromMap(owner,"ref")
		if !stages[queue] || strings.TrimSpace(ref) == "" || seen[ref] || owner["kind"] != ownerKind { return out,fmt.Errorf("circuit queue owners must be distinct and match their mode") }
		delete(stages,queue); seen[ref] = true
	}
	admission, ok := plan["admission"].(map[string]interface{})
	if !ok || admission["ceremony"] != "node.save" || strings.TrimSpace(stringFromMap(admission,"post_admission_check")) == "" { return out,fmt.Errorf("circuit admission ceremony/post-check required") }
	for _, key := range []string{"ci_required","pr_required","merge_required","commit_required"} { if value, exists := plan[key]; exists && value != false { return out,fmt.Errorf("software prerequisites cannot gate a circuit") } }
	return out,nil
}

func reviewCircuitValidateReceipt(value interface{}, state reviewCircuitState) error {
	receipt, ok := value.(map[string]interface{})
	if !ok || reviewCircuitHasReference(receipt) || receipt["schema_version"] != "velenza_validation_acceptance/1" || receipt["project_id"] != state.Scope["project_id"] || receipt["task_id"] != state.Slot["task_id"] || receipt["context_sha256"] != state.Scope["context_sha256"] || !reviewEqual(receipt["blocking"],0) || receipt["production_boundary"] != true || receipt["tautologies"] != false || receipt["fixture_supplied_authority"] != false { return fmt.Errorf("circuit review receipt has invalid scope, blockers, flags or software origin") }
	tests, ok := state.Slot["tests"].(map[string]interface{})
	if !ok || strings.TrimSpace(stringFromMap(tests,"author")) == "" || receipt["author"] != tests["author"] || strings.TrimSpace(stringFromMap(receipt,"reviewer")) == "" || receipt["reviewer"] == receipt["author"] || !reviewEqual(receipt["files"],tests["files"]) || !reviewEqual(receipt["requirements"],tests["requirements"]) { return fmt.Errorf("circuit receipt does not bind its actual author/files/requirements") }
	files, ok := tests["files"].([]interface{})
	if !ok || len(files) == 0 { return fmt.Errorf("circuit validation file hashes required") }
	paths := map[string]bool{}
	// These are registered hash coordinates, not source acquisition here. The
	// native circuit consumer verifies actual paths/bytes on its own host.
	for _, raw := range files { file, ok := raw.(map[string]interface{}); if !ok || strings.TrimSpace(stringFromMap(file,"path")) == "" || !reviewDigestPattern.MatchString(stringFromMap(file,"sha256")) { return fmt.Errorf("invalid circuit validation file/hash") }; paths[stringFromMap(file,"path")] = true }
	requirements, ok := tests["requirements"].([]interface{})
	if !ok || len(requirements) == 0 { return fmt.Errorf("circuit review requirements required") }
	reqs := map[string]bool{}
	for _, raw := range requirements { req, ok := raw.(string); if !ok || strings.TrimSpace(req) == "" { return fmt.Errorf("invalid circuit review requirement") }; reqs[req] = true }
	evidence, ok := receipt["evidence"].([]interface{})
	if !ok || len(evidence) == 0 { return fmt.Errorf("circuit review evidence required") }
	for _, raw := range evidence { text, ok := raw.(string); if !ok || strings.TrimSpace(text) == "" { return fmt.Errorf("invalid circuit review evidence") } }
	coverage, ok := receipt["coverage"].([]interface{})
	if !ok || len(coverage) == 0 { return fmt.Errorf("circuit review coverage required") }
	coveredReqs, coveredPaths := map[string]bool{}, map[string]bool{}
	for _, raw := range coverage {
		item, ok := raw.(map[string]interface{})
		if !ok { return fmt.Errorf("invalid circuit review coverage") }
		for _, key := range []string{"requirement","path","case","assertion","boundary","scenario"} { if strings.TrimSpace(stringFromMap(item,key)) == "" { return fmt.Errorf("incomplete circuit coverage %s",key) } }
		req, path := stringFromMap(item,"requirement"), stringFromMap(item,"path")
		if !reqs[req] || !paths[path] { return fmt.Errorf("circuit coverage is outside frozen requirements/files") }
		coveredReqs[req], coveredPaths[path] = true,true
	}
	if len(coveredReqs) != len(reqs) || len(coveredPaths) != len(paths) { return fmt.Errorf("every circuit requirement and file needs coverage") }
	return nil
}

func reviewCircuitApprovalHash(rawNote string) (string,error) {
	var note map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rawNote),&note); err != nil { return "",err }
	raw, exists := note["validation_acceptance"]
	if !exists { return "",fmt.Errorf("circuit approval missing") }
	decoder := json.NewDecoder(bytes.NewReader(raw))
	encoded, err := reviewCircuitStringify(decoder)
	if err != nil { return "",err }
	if _, err := decoder.Token(); err != io.EOF { return "",fmt.Errorf("invalid circuit approval JSON") }
	return reviewHashBytes(encoded),nil
}

// JSON.stringify hashes preserve object insertion order (integer-index keys
// first), while encoding/json sorts maps. Decode tokens to retain the actual
// stored receipt's order and normalize strings/numbers as its JS consumer does.
func reviewCircuitStringify(decoder *json.Decoder) ([]byte,error) {
	token, err := decoder.Token()
	if err != nil { return nil,err }
	if delim, ok := token.(json.Delim); ok {
		if delim == '[' {
			out := []byte{'['}; first := true
			for decoder.More() { child, err := reviewCircuitStringify(decoder); if err != nil { return nil,err }; if !first { out = append(out,',') }; first = false; out = append(out,child...) }
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') { return nil,fmt.Errorf("invalid approval array") }
			return append(out,']'),nil
		}
		if delim != '{' { return nil,fmt.Errorf("invalid approval delimiter") }
		type entry struct { key string; value []byte; index uint64; integer bool }
		entries := []entry{}
		for decoder.More() {
			rawKey, err := decoder.Token(); if err != nil { return nil,err }
			key, ok := rawKey.(string); if !ok { return nil,fmt.Errorf("invalid approval key") }
			child, err := reviewCircuitStringify(decoder); if err != nil { return nil,err }
			index, parseErr := strconv.ParseUint(key,10,32)
			entries = append(entries,entry{key,child,index,parseErr == nil && index < (1<<32)-1 && strconv.FormatUint(index,10) == key})
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim('}') { return nil,fmt.Errorf("invalid approval object") }
		sort.SliceStable(entries,func(i,j int) bool { if entries[i].integer != entries[j].integer { return entries[i].integer }; return entries[i].integer && entries[i].index < entries[j].index })
		out := []byte{'{'}
		for i, entry := range entries { key, err := reviewConsumerJSON(entry.key); if err != nil { return nil,err }; if i != 0 { out = append(out,',') }; out = append(out,key...); out = append(out,':'); out = append(out,entry.value...) }
		return append(out,'}'),nil
	}
	if number, ok := token.(float64); ok {
		if number == 0 { return []byte("0"),nil }
		absolute := math.Abs(number)
		if absolute >= 1e-6 && absolute < 1e21 { return []byte(strconv.FormatFloat(number,'f',-1,64)),nil }
		parts := strings.Split(strconv.FormatFloat(number,'e',-1,64),"e")
		exponent, err := strconv.Atoi(parts[1]); if err != nil { return nil,err }
		sign := ""; if exponent >= 0 { sign = "+" }
		return []byte(parts[0]+"e"+sign+strconv.Itoa(exponent)),nil
	}
	return reviewConsumerJSON(token)
}
