package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type queryAdapter struct {
	fakeAdapter
	result QueryResult
	sql    string
	params map[string]interface{}
}

func (a *queryAdapter) ExecuteReadOnlyQuery(_ context.Context, sql string, params map[string]interface{}) (QueryResult, error) {
	a.sql, a.params = sql, params
	return a.result, nil
}

type queryRouter struct{ adapter SubstrateAdapter }

func (r queryRouter) Adapter(context.Context, NodeKindDescriptor) (SubstrateAdapter, error) {
	return r.adapter, nil
}
func (r queryRouter) All(context.Context) ([]SubstrateAdapter, error) {
	if r.adapter == nil {
		return nil, nil
	}
	return []SubstrateAdapter{r.adapter}, nil
}
func (r queryRouter) ByID(string) (SubstrateAdapter, error) { return r.adapter, nil }
func (r queryRouter) Close() error                          { return nil }

func TestHandleQueryFailsClosedAndForwardsOnlyReadOnlyQueries(t *testing.T) {
	ctx := context.Background()
	env := &Env{Config: SpecGraphConfig{Mode: ModeProduction}, Substrates: queryRouter{}}
	if _, err := handleQuery(ctx, env, queryInput{SQL: "SELECT 1"}); !errors.Is(err, ErrModeMismatch) {
		t.Fatalf("production query error = %v, want mode mismatch", err)
	}
	env.Config = SpecGraphConfig{Mode: ModeExperimentalBootstrap, ExperimentalQueryEnabled: true}
	for _, sql := range []string{"", "DELETE FROM Nodes", " INSERT INTO Nodes VALUES (1)"} {
		if _, err := handleQuery(ctx, env, queryInput{SQL: sql}); err == nil {
			t.Fatalf("unsafe query %q was accepted", sql)
		}
	}
	if _, err := handleQuery(ctx, env, queryInput{SQL: "SELECT 1"}); err == nil || !strings.Contains(err.Error(), "no substrates") {
		t.Fatalf("empty router error = %v", err)
	}

	adapter := &queryAdapter{result: QueryResult{Columns: []string{"count"}, Rows: []map[string]interface{}{{"count": int64(1)}}}}
	env.Substrates = queryRouter{adapter: adapter}
	params := map[string]interface{}{"limit": 1}
	got, err := handleQuery(ctx, env, queryInput{SQL: "  WITH x AS (SELECT 1) SELECT * FROM x", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows[0]["count"] != int64(1) || adapter.sql == "" || adapter.params["limit"] != 1 {
		t.Fatalf("query result/forwarding = %#v, sql=%q params=%v", got, adapter.sql, adapter.params)
	}
}

func TestHandleUseMaintainsOnlyTheRequestedSnapshotPin(t *testing.T) {
	pin := &snapshotPin{SnapshotID: "prior"}
	if got, err := handleUse(context.Background(), nil, useInput{}, pin); err != nil || got.SnapshotID != "prior" {
		t.Fatalf("read pin = %#v, %v", got, err)
	}
	if got, err := handleUse(context.Background(), nil, useInput{SnapshotID: "snap-2"}, pin); err != nil || got.SnapshotID != "snap-2" || pin.SnapshotID != "snap-2" {
		t.Fatalf("write pin = %#v, pin=%#v, err=%v", got, pin, err)
	}
}
