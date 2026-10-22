//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
)

// An empty emulator is provisioned by the existing integration bootstrap.
// Successful origin is supplied only by the real MCP server's child process.
func TestIntegrationReviewProducerFreshSubstrateAndExactReplay(t *testing.T) {
	requireEmulator(t)
	schema:=append(append([]string{},nativeCoverageSchema...),reviewProducerTestDDL)
	setupEmulatorDBWithSchema(context.Background(),t,schema)
	client,err:=spanner.NewClient(context.Background(),emulatorResource)
	if err!=nil {t.Fatal(err)}
	defer client.Close()
	f:=rpSourceFixture(t,client,emulatorResource)
	if f.fakeObserver {t.Fatal("real-tier consumer assertions must traverse actual MCP Task-get")}
	if len(f.rows())!=0 {t.Fatal("fresh substrate contains invented review authority")}
	first:=f.allow("task_graph_review_produce",f.request)
	leaf,controller:=f.leafNote(),f.taskNote()
	if leaf["validation_acceptance"]==nil || controller["validation_acceptance"]!=nil || f.slot(controller)["tests"].(map[string]any)["acceptance"]==nil {t.Fatal("fresh publication did not use separate leaf/controller Tasks")}
	ref:=first["review_ref"].(map[string]any)
	approval:=leaf["validation_acceptance"].(map[string]any)
	slot:=f.slot(controller);tests:=slot["tests"].(map[string]any);acceptance:=tests["acceptance"].(map[string]any)
	if approval["schema_version"]!="velenza_validation_acceptance/1" || approval["project_id"]!="arbor" || approval["task_id"]!="REVIEW-TASK" || approval["context_sha256"]!=f.request["context_sha256"] || approval["author"]!=tests["author"] || approval["reviewer"]==nil || approval["reviewer"]=="" || approval["reviewer"]==approval["author"] || approval["blocking"]!=float64(0) || approval["production_boundary"]!=true || approval["tautologies"]!=false || approval["fixture_supplied_authority"]!=false {t.Fatal("actual MCP leaf projection cannot satisfy the shared validation consumer")}
	if !reflect.DeepEqual(approval["files"],tests["files"]) || !reflect.DeepEqual(approval["requirements"],tests["requirements"]) || !reflect.DeepEqual(approval["review_ref"],ref) || !reflect.DeepEqual(acceptance["review_ref"],ref) || acceptance["task_id"]!="REVIEW-TASK" || len(approval["coverage"].([]any))==0 || len(approval["evidence"].([]any))==0 {t.Fatal("actual MCP controller/leaf consumer binding is incomplete")}
	// An explicit real Task-get, independent from the fixture's observers,
	// preserves the exact existing JSON.stringify-compatible approval hash.
	leafNode:=f.allow("task_graph_get",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK"})
	controllerNode:=f.allow("task_graph_get",map[string]any{"project_id":"arbor","node_id":f.executionID})
	var rawNote map[string]json.RawMessage
	if err:=json.Unmarshal([]byte(leafNode["current_version"].(map[string]any)["payload"].(map[string]any)["note"].(string)),&rawNote);err!=nil {t.Fatal(err)}
	var compact bytes.Buffer
	if err:=json.Compact(&compact,rawNote["validation_acceptance"]);err!=nil {t.Fatal(err)}
	if acceptance["sha256"]!=rpDigest(compact.Bytes()) || controllerNode["current_version"].(map[string]any)["payload"].(map[string]any)["session_id"]!="shared-session" {t.Fatal("actual MCP hash/session consumer projection changed")}
	f.allow("task_graph_review_get",ref)
	got:=f.allow("task_graph_review_get",f.request)
	retry:=f.allow("task_graph_review_produce",f.request)
	if first["review_id"]!=got["review_id"] || first["review_id"]!=retry["review_id"] || len(f.rows())!=1 {t.Fatal("fresh production/read/exact replay did not converge on one origin")}
	// The same real substrate and actual MCP reads must preserve a disjoint
	// hash-only circuit handoff without manufacturing software origin.
	beforeRows,beforeSoftware:=f.rows(),rpClone(t,f.slot(f.taskNote()))
	c:=rpCircuitFixture(t,f,false);c.publishAll()
	circuitLeaf,circuitController:=f.noteFor(c.leaf),f.taskNote()
	if !reflect.DeepEqual(circuitLeaf["validation_acceptance"],rpClone(t,c.receipt)) || c.slot(circuitController)["tests"].(map[string]any)["acceptance"].(map[string]any)["sha256"]!=c.approvalHash() || !reflect.DeepEqual(beforeSoftware,f.slot(circuitController)) || !reflect.DeepEqual(beforeRows,f.rows()) {t.Fatal("real MCP circuit handoff changed software origin or lost its hash binding")}
	f.allow("task_graph_review_get",ref)
}

func TestIntegrationReviewProducerRollsBackAfterAllThreeRealWrites(t *testing.T) {
	requireEmulator(t)
	schema:=append(append([]string{},nativeCoverageSchema...),reviewProducerTestDDL)
	setupEmulatorDBWithSchema(context.Background(),t,schema)
	client,err:=spanner.NewClient(context.Background(),emulatorResource)
	if err!=nil {t.Fatal(err)}
	defer client.Close()
	rpRequireAtomicRollback(t,rpSourceFixture(t,client,emulatorResource))
}
