//go:build integration

package main

import (
	"context"
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
	if len(f.rows())!=0 {t.Fatal("fresh substrate contains invented review authority")}
	first:=f.allow("task_graph_review_produce",f.request)
	leaf,controller:=f.leafNote(),f.taskNote()
	if leaf["validation_acceptance"]==nil || controller["validation_acceptance"]!=nil || f.slot(controller)["tests"].(map[string]any)["acceptance"]==nil {t.Fatal("fresh publication did not use separate leaf/controller Tasks")}
	got:=f.allow("task_graph_review_get",f.request)
	retry:=f.allow("task_graph_review_produce",f.request)
	if first["review_id"]!=got["review_id"] || first["review_id"]!=retry["review_id"] || len(f.rows())!=1 {t.Fatal("fresh production/read/exact replay did not converge on one origin")}
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
