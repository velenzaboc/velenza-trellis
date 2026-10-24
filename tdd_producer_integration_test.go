//go:build integration

package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
)

func tdIntegrationFixture(t *testing.T) *tddFixture {
	t.Helper();requireEmulator(t)
	schema:=append(append([]string{},nativeCoverageSchema...),reviewProducerTestDDL,tddProducerTestDDL)
	setupEmulatorDBWithSchema(context.Background(),t,schema)
	client,err:=spanner.NewClient(context.Background(),emulatorResource);if err!=nil {t.Fatal(err)};t.Cleanup(client.Close)
	d:=tdSourceFixture(t,rpSourceFixture(t,client,emulatorResource))
	if d.f.fakeObserver {t.Fatal("real TDD transaction/consumer proof cannot use the fake observer")}
	return d
}

// Real Spanner/MCP/process/storage proof with a controlled executor protocol
// model. Qualification of the actual remote native-Go executor is separate.
func TestIntegrationTDDProducerFreshSubstrateAndFixtureReopen(t *testing.T) {
	d:=tdIntegrationFixture(t);if len(d.rows())!=0 || len(d.f.rows())!=0 {t.Fatal("fresh substrate contains fixture-supplied origin")}
	d.throughGreen();d.productionReview()
	reopen:=d.f.allow("task_graph_tdd_produce",d.request("reopen","real-fixture-reopen",d.repaired))
	d.red("real-repaired-red",d.repaired);d.review("real-repaired-cases");d.green("real-repaired-green",d.code)
	leaf:=d.f.allow("task_graph_get",map[string]any{"project_id":"arbor","node_id":"REVIEW-TASK"})
	controller:=d.f.allow("task_graph_get",map[string]any{"project_id":"arbor","node_id":d.f.executionID})
	if leaf["current_version"]==nil || controller["current_version"]==nil || !reflect.DeepEqual(d.f.leafNote()["tdd_execution_ref"],d.greenRef) || !reflect.DeepEqual(d.slot(d.f.taskNote())["tdd_green_receipt"],d.greenRef) || d.slot(d.f.taskNote())["queue"]!="test-code" {t.Fatal("actual MCP leaf/controller consumers lost typed execution progression")}
	got:=d.f.allow("task_graph_tdd_get",d.greenRef)
	if !reflect.DeepEqual(got["retained_production_ref"],d.productionRef) || reopen["retained_production_ref"]==nil || len(d.rows())!=6 || len(d.f.rows())!=3 {t.Fatal("real lineage failed to preserve only verified unchanged production proof")}
	before:=len(tdCaptures(t,d.capture));retry:=d.f.allow("task_graph_tdd_get",d.greenRef);if !reflect.DeepEqual(got["tdd_ref"],retry["tdd_ref"]) || len(tdCaptures(t,d.capture))!=before {t.Fatal("typed read executed work or changed immutable origin")}
}

// spannertest does not roll back DML. Only this real-emulator case is an ACID
// oracle, and each fault must be reached AFTER its named actual write.
func TestIntegrationTDDProducerRollsBackAllThreeActualWrites(t *testing.T) {
	d:=tdIntegrationFixture(t);d.start();d.register()
	for _,stage:=range []string{"tdd_after_evidence_insert","tdd_after_leaf_publication","tdd_after_execution_publication"} {t.Run(stage,func(t *testing.T){
		beforeRows,beforeLeaf,beforeController:=d.rows(),d.f.leafNote(),d.f.taskNote();beforeReview:=d.f.rows();reached:=false
		reviewProducerFault=func(actual string)error {if actual==stage {reached=true;return errors.New("injected actual TDD publication failure")};return nil};t.Cleanup(func(){reviewProducerFault=nil})
		d.f.deny("task_graph_tdd_produce",d.request("red","rollback-"+stage,d.tests));reviewProducerFault=nil
		if !reached {t.Fatalf("actual %s was not reached; preflight refusal is not rollback proof",stage)}
		if !reflect.DeepEqual(beforeRows,d.rows()) || !reflect.DeepEqual(beforeLeaf,d.f.leafNote()) || !reflect.DeepEqual(beforeController,d.f.taskNote()) || !reflect.DeepEqual(beforeReview,d.f.rows()) {t.Fatal("actual TDD evidence/leaf/controller writes failed to roll back together")}
	})}
}
