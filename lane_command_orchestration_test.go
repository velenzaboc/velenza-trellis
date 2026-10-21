package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type laneCommandAdapter struct {
	fakeAdapter
	checks    []error
	verifyErr error
}

func (a *laneCommandAdapter) CheckSchema(context.Context) error {
	if len(a.checks) == 0 {
		return nil
	}
	err := a.checks[0]
	a.checks = a.checks[1:]
	return err
}

func (a *laneCommandAdapter) VerifyExperimentalSchema(context.Context) error {
	return a.verifyErr
}

type laneCommandAdmin struct {
	closeErr  error
	createErr error
	dropErr   error
	ddlErr    error
	calls     []string
	ddl       []string
}

func (a *laneCommandAdmin) Close() error {
	a.calls = append(a.calls, "close")
	return a.closeErr
}
func (a *laneCommandAdmin) CreateDatabase(context.Context, string) error {
	a.calls = append(a.calls, "create")
	return a.createErr
}
func (a *laneCommandAdmin) DropDatabase(context.Context, string) error {
	a.calls = append(a.calls, "drop")
	return a.dropErr
}
func (a *laneCommandAdmin) ApplyDDL(_ context.Context, _ string, statements []string) error {
	a.calls = append(a.calls, "ddl")
	a.ddl = append([]string(nil), statements...)
	return a.ddlErr
}

func preserveCommandSeams(t *testing.T) {
	t.Helper()
	load := commandLoadConfig
	abs := commandAbsPath
	read := commandReadFile
	router := commandRouter
	admin := commandAdminClient
	t.Cleanup(func() {
		commandLoadConfig = load
		commandAbsPath = abs
		commandReadFile = read
		commandRouter = router
		commandAdminClient = admin
	})
}

func laneCommandConfig() SpecGraphConfig {
	cfg := validIdentityBoundConfig()
	cfg.Embedding = nil
	cfg = schemaConfig(cfg)
	cfg.InitDatabaseAllowlist = []string{"d"}
	return cfg
}

func installLaneCommandSuccess(cfg SpecGraphConfig, adapter SubstrateAdapter, admin commandAdmin) {
	commandLoadConfig = func() (SpecGraphConfig, error) { return cfg, nil }
	commandAbsPath = func(string) (string, error) { return "schema.sql", nil }
	commandReadFile = func(string) ([]byte, error) {
		return []byte("CREATE TABLE A (K INT64) PRIMARY KEY (K); CREATE INDEX IX ON A(K);"), nil
	}
	commandRouter = func(SpecGraphConfig) SubstrateRouter { return queryRouter{adapter: adapter} }
	commandAdminClient = func(context.Context, SpecGraphConfig) (commandAdmin, error) { return admin, nil }
}

func TestLaneCmdInitOrchestratesEverySafetyStage(t *testing.T) {
	preserveCommandSeams(t)
	if err := cmdInit(context.Background(), []string{"--help"}); err != nil {
		t.Fatal(err)
	}
	if err := cmdInit(context.Background(), []string{"--schema"}); err == nil || !strings.Contains(err.Error(), "requires a path") {
		t.Fatalf("missing schema argument error = %v", err)
	}

	t.Run("config_error", func(t *testing.T) {
		commandLoadConfig = func() (SpecGraphConfig, error) { return SpecGraphConfig{}, errors.New("config failure") }
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "load config") {
			t.Fatalf("config error = %v", err)
		}
	})

	t.Run("no_substrates", func(t *testing.T) {
		cfg := laneCommandConfig()
		cfg.Substrates = nil
		commandLoadConfig = func() (SpecGraphConfig, error) { return cfg, nil }
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "no substrates") {
			t.Fatalf("no-substrate error = %v", err)
		}
	})

	t.Run("bad_resource", func(t *testing.T) {
		cfg := laneCommandConfig()
		cfg.Substrates[0].SpannerDatabase = "bad"
		commandLoadConfig = func() (SpecGraphConfig, error) { return cfg, nil }
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "invalid spanner_database") {
			t.Fatalf("resource error = %v", err)
		}
	})

	t.Run("allowlist_refusal", func(t *testing.T) {
		cfg := laneCommandConfig()
		cfg.InitDatabaseAllowlist = []string{"other-*"}
		commandLoadConfig = func() (SpecGraphConfig, error) { return cfg, nil }
		if err := cmdInit(context.Background(), nil); !errors.Is(err, ErrForbiddenDatabaseTarget) {
			t.Fatalf("allowlist error = %v", err)
		}
	})

	t.Run("invalid_allowlist_pattern", func(t *testing.T) {
		cfg := laneCommandConfig()
		cfg.InitDatabaseAllowlist = []string{"["}
		commandLoadConfig = func() (SpecGraphConfig, error) { return cfg, nil }
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "invalid pattern") {
			t.Fatalf("pattern error = %v", err)
		}
	})

	t.Run("schema_read_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{}, &laneCommandAdmin{})
		commandReadFile = func(string) ([]byte, error) { return nil, errors.New("read failure") }
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "not readable") {
			t.Fatalf("read error = %v", err)
		}
	})

	t.Run("zero_statements", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{}, &laneCommandAdmin{})
		commandReadFile = func(string) ([]byte, error) { return []byte("-- comment only"), nil }
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "zero DDL statements") {
			t.Fatalf("zero-statement error = %v", err)
		}
	})

	t.Run("already_applied", func(t *testing.T) {
		cfg := laneCommandConfig()
		admin := &laneCommandAdmin{}
		installLaneCommandSuccess(cfg, &laneCommandAdapter{}, admin)
		if err := cmdInit(context.Background(), []string{"--schema", "custom.sql"}); err != nil {
			t.Fatal(err)
		}
		if len(admin.calls) != 0 {
			t.Fatalf("admin called for already-applied schema: %v", admin.calls)
		}
	})

	t.Run("admin_factory_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{checks: []error{errors.New("missing")}}, &laneCommandAdmin{})
		commandAdminClient = func(context.Context, SpecGraphConfig) (commandAdmin, error) { return nil, errors.New("admin failure") }
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "admin failure") {
			t.Fatalf("admin factory error = %v", err)
		}
	})

	t.Run("apply_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		admin := &laneCommandAdmin{ddlErr: errors.New("apply failure")}
		installLaneCommandSuccess(cfg, &laneCommandAdapter{checks: []error{errors.New("missing")}}, admin)
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "apply failure") {
			t.Fatalf("apply error = %v", err)
		}
	})

	t.Run("post_apply_verification", func(t *testing.T) {
		cfg := laneCommandConfig()
		admin := &laneCommandAdmin{}
		installLaneCommandSuccess(cfg, &laneCommandAdapter{checks: []error{errors.New("missing"), errors.New("still missing")}}, admin)
		if err := cmdInit(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "post-apply schema verification") {
			t.Fatalf("verification error = %v", err)
		}
	})

	t.Run("apply_success", func(t *testing.T) {
		cfg := laneCommandConfig()
		admin := &laneCommandAdmin{}
		installLaneCommandSuccess(cfg, &laneCommandAdapter{checks: []error{errors.New("missing"), nil}}, admin)
		if err := cmdInit(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		if len(admin.ddl) != 2 || strings.Join(admin.calls, ",") != "ddl,close" {
			t.Fatalf("admin calls=%v ddl=%v", admin.calls, admin.ddl)
		}
	})
}

func TestLaneCmdResetHonorsFingerprintAndLifecycle(t *testing.T) {
	preserveCommandSeams(t)
	if err := cmdReset(context.Background(), []string{"--help"}); err != nil {
		t.Fatal(err)
	}
	if err := cmdReset(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("confirmation error = %v", err)
	}

	t.Run("config_error", func(t *testing.T) {
		commandLoadConfig = func() (SpecGraphConfig, error) { return SpecGraphConfig{}, errors.New("config failure") }
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "load config") {
			t.Fatalf("config error = %v", err)
		}
	})

	t.Run("no_substrates", func(t *testing.T) {
		cfg := laneCommandConfig()
		cfg.Substrates = nil
		commandLoadConfig = func() (SpecGraphConfig, error) { return cfg, nil }
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "no substrates") {
			t.Fatalf("no-substrate error = %v", err)
		}
	})

	t.Run("router_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{}, &laneCommandAdmin{})
		commandRouter = func(SpecGraphConfig) SubstrateRouter { return failingRouter{err: errors.New("route failure")} }
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "route failure") {
			t.Fatalf("router error = %v", err)
		}
	})

	t.Run("fingerprint_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{verifyErr: errors.New("extra table")}, &laneCommandAdmin{})
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "refusing to drop") {
			t.Fatalf("fingerprint error = %v", err)
		}
	})

	t.Run("admin_factory_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{}, &laneCommandAdmin{})
		commandAdminClient = func(context.Context, SpecGraphConfig) (commandAdmin, error) { return nil, errors.New("admin failure") }
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "admin failure") {
			t.Fatalf("admin error = %v", err)
		}
	})

	t.Run("drop_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{}, &laneCommandAdmin{dropErr: errors.New("drop failure")})
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "drop failure") {
			t.Fatalf("drop error = %v", err)
		}
	})

	t.Run("create_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		installLaneCommandSuccess(cfg, &laneCommandAdapter{}, &laneCommandAdmin{createErr: errors.New("create failure")})
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "create failure") {
			t.Fatalf("create error = %v", err)
		}
	})

	t.Run("reapply_error", func(t *testing.T) {
		cfg := laneCommandConfig()
		admin := &laneCommandAdmin{}
		installLaneCommandSuccess(cfg, &laneCommandAdapter{checks: []error{errors.New("missing")}}, admin)
		commandReadFile = func(string) ([]byte, error) { return nil, errors.New("reapply read failure") }
		if err := cmdReset(context.Background(), []string{"--confirm"}); err == nil || !strings.Contains(err.Error(), "re-apply schema") {
			t.Fatalf("reapply error = %v", err)
		}
	})

	t.Run("success", func(t *testing.T) {
		cfg := laneCommandConfig()
		admin := &laneCommandAdmin{}
		installLaneCommandSuccess(cfg, &laneCommandAdapter{checks: []error{errors.New("missing"), nil}}, admin)
		if err := cmdReset(context.Background(), []string{"--confirm"}); err != nil {
			t.Fatal(err)
		}
		got := strings.Join(admin.calls, ",")
		if got != "drop,create,ddl,close,close" {
			t.Fatalf("admin lifecycle = %s", got)
		}
	})
}

func TestLaneRunCLIDispatchAndExitCodes(t *testing.T) {
	preserveCommandSeams(t)
	for _, args := range [][]string{{"task-graph"}, {"task-graph", "unknown"}} {
		code, err := runCLI(context.Background(), args)
		if err != nil || code != 2 {
			t.Fatalf("runCLI(%v) = (%d, %v), want (2, nil)", args, code, err)
		}
	}
	for _, help := range []string{"help", "-h", "--help"} {
		code, err := runCLI(context.Background(), []string{"task-graph", help})
		if err != nil || code != 0 {
			t.Fatalf("help %s = (%d, %v)", help, code, err)
		}
	}
	for _, command := range []string{"init", "reset"} {
		code, err := runCLI(context.Background(), []string{"task-graph", command, "--help"})
		if err != nil || code != 0 {
			t.Fatalf("%s help = (%d, %v)", command, code, err)
		}
	}

	commandLoadConfig = func() (SpecGraphConfig, error) { return SpecGraphConfig{}, context.Canceled }
	code, err := runCLI(context.Background(), []string{"task-graph", "init"})
	if err != nil || code != 0 {
		t.Fatalf("canceled init = (%d, %v)", code, err)
	}

	code, err = runCLI(context.Background(), []string{"task-graph", "reset"})
	if err == nil || code != 1 {
		t.Fatalf("unconfirmed reset = (%d, %v)", code, err)
	}
}
