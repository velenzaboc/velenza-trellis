package main

import (
	"context"
	"fmt"
	"os"
)

// cmdReset drops and recreates the configured experimental Spanner database
// via the Spanner Admin API using the substrate's declared credential selector, then
// re-applies the schema. Refuses without --confirm. Refuses any database not
// matching the config's init_database_allowlist. No gcloud shell-out.
func cmdReset(ctx context.Context, args []string) error {
	confirmed := false
	for _, a := range args {
		switch a {
		case "--confirm":
			confirmed = true
		case "-h", "--help":
			fmt.Println(`velenza-trellis reset --confirm (task-graph reset --confirm) — drop + recreate the experimental database

Refuses without --confirm. Refuses any database whose name does not match
init_database_allowlist patterns. After drop+create, re-applies
bootstrap/schema_v1.sql via velenza-trellis init.`)
			return nil
		}
	}
	if !confirmed {
		return fmt.Errorf("refused: pass --confirm to drop and recreate the database")
	}

	cfg, err := commandLoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if len(cfg.Substrates) == 0 {
		return fmt.Errorf("no substrates registered in task-graph.config.json")
	}
	sub := cfg.Substrates[0]
	_, _, database, err := parseSpannerResource(sub.SpannerDatabase)
	if err != nil {
		return err
	}
	if err := allowlistCheck(cfg, database); err != nil {
		return err
	}

	// Schema-fingerprint gate: independently of the config-driven allowlist,
	// verify the database at the configured spanner_database actually carries
	// the experimental shape (exactly Nodes + Edges +
	// ArtifactAdmissionLog as its base tables — no more, no less). Refuses
	// on production-tier databases which carry many other base tables. Empty databases
	// (zero base tables) pass since there's nothing to lose by dropping.
	router := commandRouter(cfg)
	defer router.Close()
	adapter, err := router.ByID(sub.ID)
	if err != nil {
		return err
	}
	if err := adapter.VerifyExperimentalSchema(ctx); err != nil {
		return fmt.Errorf("refusing to drop %q: %w", sub.SpannerDatabase, err)
	}

	admin, err := commandAdminClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer admin.Close()

	fmt.Fprintf(os.Stderr, "velenza-trellis reset: dropping %s\n", sub.SpannerDatabase)
	if err := admin.DropDatabase(ctx, sub.SpannerDatabase); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "velenza-trellis reset: creating %s\n", sub.SpannerDatabase)
	if err := admin.CreateDatabase(ctx, sub.SpannerDatabase); err != nil {
		return err
	}

	if err := cmdInit(ctx, nil); err != nil {
		return fmt.Errorf("re-apply schema: %w", err)
	}
	fmt.Fprintln(os.Stderr, "velenza-trellis reset: empty experimental graph ready")
	return nil
}
