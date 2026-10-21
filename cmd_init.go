package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// commandAdmin is the narrow control-plane surface used by init/reset. The
// concrete implementation remains the Spanner Admin client; the interface
// keeps command orchestration testable without contacting a live database.
type commandAdmin interface {
	Close() error
	CreateDatabase(context.Context, string) error
	DropDatabase(context.Context, string) error
	ApplyDDL(context.Context, string, []string) error
}

var (
	commandLoadConfig = loadConfig
	commandAbsPath    = filepath.Abs
	commandReadFile   = os.ReadFile
	commandRouter     = func(cfg SpecGraphConfig) SubstrateRouter {
		return newRouter(cfg.GoogleCredentials, cfg.AllowSpannerEmulator, cfg.Substrates)
	}
	commandAdminClient = func(ctx context.Context, cfg SpecGraphConfig) (commandAdmin, error) {
		return newAdminClient(ctx, cfg.GoogleCredentials, cfg.AllowSpannerEmulator)
	}
)

// cmdInit applies bootstrap/schema_v1.sql to the configured experimental
// Spanner database via the Spanner Admin API using the substrate's
// declared credential selector. No gcloud shell-out. Idempotent: re-running on an
// already-initialized DB reports "already applied" and exits 0. Refuses any
// database not matching the config's init_database_allowlist glob patterns.
func cmdInit(ctx context.Context, args []string) error {
	// task-graph uses the generic single-Nodes-table schema_v1 shape
	// (Nodes + Edges + ArtifactAdmissionLog). Pass --schema to override.
	schemaPath := "bootstrap/schema_v1.sql"
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--schema":
			if i+1 >= len(args) {
				return fmt.Errorf("--schema requires a path")
			}
			schemaPath = args[i+1]
			i++
		case "-h", "--help":
			fmt.Println(`velenza-trellis init (task-graph init) — apply experimental schema

Reads DDL from bootstrap/schema_v1.sql (override with --schema <path>) and
applies it to the substrate's Spanner database via the Spanner Admin API.
Authenticates using the substrate's declared credential selector (no gcloud shell-out).
Refuses any database whose name does not match init_database_allowlist
patterns from task-graph.config.json.

Idempotent: re-running on an already-initialized database reports "already
applied" and exits 0.`)
			return nil
		}
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

	abs, _ := commandAbsPath(schemaPath)
	ddlBytes, err := commandReadFile(abs)
	if err != nil {
		return fmt.Errorf("schema file %q not readable: %w", abs, err)
	}
	statements := splitDDL(string(ddlBytes))
	if len(statements) == 0 {
		return fmt.Errorf("schema file %q produced zero DDL statements", abs)
	}

	fmt.Fprintf(os.Stderr, "velenza-trellis init: applying %d statements from %s to %s\n", len(statements), abs, sub.SpannerDatabase)

	// Idempotence probe via the data-plane adapter's CheckSchema.
	router := commandRouter(cfg)
	defer router.Close()
	adapter, _ := router.ByID(sub.ID)
	if err := adapter.CheckSchema(ctx); err == nil {
		fmt.Fprintln(os.Stderr, "velenza-trellis init: schema already applied — nothing to do")
		return nil
	}

	admin, err := commandAdminClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer admin.Close()

	if err := admin.ApplyDDL(ctx, sub.SpannerDatabase, statements); err != nil {
		return err
	}

	// Validate the configured task-graph tables after DDL application.
	if sub.NodeTable != "" {
		if err := adapter.CheckSchema(ctx); err != nil {
			return fmt.Errorf("post-apply schema verification failed: %w", err)
		}
	}
	fmt.Fprintln(os.Stderr, "velenza-trellis init: schema applied and verified")
	return nil
}

// parseSpannerResource extracts (project, instance, database) from a
// projects/X/instances/Y/databases/Z resource path.
func parseSpannerResource(uri string) (project, instance, database string, err error) {
	parts := strings.Split(uri, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "instances" || parts[4] != "databases" {
		return "", "", "", fmt.Errorf("invalid spanner_database resource %q (want projects/X/instances/Y/databases/Z)", uri)
	}
	return parts[1], parts[3], parts[5], nil
}

// allowlistCheck refuses any database name that doesn't match the config's
// init_database_allowlist glob patterns. Empty allowlist → refuses.
func allowlistCheck(cfg SpecGraphConfig, database string) error {
	if len(cfg.InitDatabaseAllowlist) == 0 {
		return fmt.Errorf("%w: init_database_allowlist empty in spec-graph.config.json — refusing", ErrForbiddenDatabaseTarget)
	}
	for _, pattern := range cfg.InitDatabaseAllowlist {
		matched, err := filepath.Match(pattern, database)
		if err != nil {
			return fmt.Errorf("invalid pattern %q in init_database_allowlist: %w", pattern, err)
		}
		if matched {
			return nil
		}
	}
	return fmt.Errorf("%w: database %q matches none of init_database_allowlist patterns %v", ErrForbiddenDatabaseTarget, database, cfg.InitDatabaseAllowlist)
}
