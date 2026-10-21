package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGoogleClientConstructionIsIdentityBound(t *testing.T) {
	required := map[string][]string{
		"admin.go":                        {"TokenSource", "WithTokenSource", "WithoutAuthentication", "NewDatabaseAdminClient"},
		"substrate.go":                    {"TokenSource", "WithTokenSource", "WithoutAuthentication", "NewClient"},
		"mcp.go":                          {"RequestTokenSourceFactory", "NewAuth", "NewEmbedClient"},
		"bootstrap/cmd/spec-lint/main.go": {"RequestTokenSourceFactory", "NewAuth", "NewFlashClient"},
	}
	for path, names := range required {
		calls := parsedCallNames(t, path)
		for _, name := range names {
			if calls[name] == 0 {
				t.Errorf("%s: required call %s is absent", path, name)
			}
		}
	}
}

func TestEveryVertexClientConsumesTheValidatedProcessSelector(t *testing.T) {
	for _, path := range []string{
		"mcp.go",
		"bootstrap/cmd/spec-lint/main.go",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(string(data), "GoogleCredentials.RequestTokenSourceFactory("); got != 1 {
			t.Errorf("%s: GoogleCredentials.RequestTokenSourceFactory call count = %d, want 1", path, got)
		}
	}
}

func TestEveryConfigLoaderRejectsHiddenCredentialSelectors(t *testing.T) {
	for _, path := range []string{
		"config.go",
		"bootstrap/cmd/spec-lint/types.go",
	} {
		if got := parsedCallNames(t, path)["ValidateConfigCredentialShape"]; got != 1 {
			t.Errorf("%s: ValidateConfigCredentialShape call count = %d, want 1", path, got)
		}
	}
}

func TestRuntimeHasNoImplicitOrExportableGoogleCredentialConstructor(t *testing.T) {
	forbidden := map[string]bool{
		"FindDefaultCredentials": true,
		"CredentialsFromJSON":    true,
		"JWTConfigFromJSON":      true,
		"WithCredentialsFile":    true,
	}
	allowedGoogleConstructors := map[string]string{
		"spanner.NewClient":               "substrate.go",
		"database.NewDatabaseAdminClient": "admin.go",
	}
	typedImpersonationParserCalls := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == ".git" || strings.HasPrefix(path, ".git/") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		calls := parsedCallNames(t, path)
		for name := range forbidden {
			if calls[name] > 0 {
				t.Errorf("%s: forbidden ambient/exportable credential call %s", path, name)
			}
		}
		if count := calls["CredentialsFromJSONWithType"]; count > 0 {
			if filepath.ToSlash(path) != "internal/gcpauth/selector.go" {
				t.Errorf("%s: typed impersonation parsing must be centralized in internal/gcpauth/selector.go", path)
			}
			typedImpersonationParserCalls += count
		}
		for name, allowedPath := range allowedGoogleConstructors {
			if calls[name] > 0 && filepath.ToSlash(path) != allowedPath {
				t.Errorf("%s: %s must be centralized in %s", path, name, allowedPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if typedImpersonationParserCalls != 1 {
		t.Fatalf("CredentialsFromJSONWithType call count = %d, want exactly 1 in internal/gcpauth/selector.go", typedImpersonationParserCalls)
	}
}

func TestSpannerEmulatorBypassIsExplicitlyExperimental(t *testing.T) {
	descriptor := SubstrateDescriptor{ID: "test", SpannerDatabase: "projects/p/instances/i/databases/d"}

	t.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
	production := &SpannerSubstrateAdapter{Descriptor: descriptor, AllowEmulator: false}
	if _, err := production.getClient(context.Background()); err == nil || !strings.Contains(err.Error(), "forbidden outside experimental bootstrap") {
		t.Fatalf("production emulator error = %v", err)
	}

	t.Setenv("SPANNER_EMULATOR_HOST", "127.0.0.1:9010")
	wrongHost := &SpannerSubstrateAdapter{Descriptor: descriptor, AllowEmulator: true}
	if _, err := wrongHost.getClient(context.Background()); err == nil || !strings.Contains(err.Error(), "must equal") {
		t.Fatalf("non-canonical emulator host error = %v", err)
	}

	t.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
	experimental := &SpannerSubstrateAdapter{Descriptor: descriptor, AllowEmulator: true}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := experimental.getClient(ctx)
	if err != nil {
		t.Fatalf("experimental emulator client unexpectedly resolved credentials: %v", err)
	}
	if client == nil {
		t.Fatal("experimental emulator client is nil")
	}
	_ = experimental.Close()
}

func parsedCallNames(t *testing.T, path string) map[string]int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := make(map[string]int)
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			calls[fun.Name]++
		case *ast.SelectorExpr:
			calls[fun.Sel.Name]++
			if qualified := selectorCallName(fun); qualified != fun.Sel.Name {
				calls[qualified]++
			}
		}
		return true
	})
	return calls
}

func selectorCallName(selector *ast.SelectorExpr) string {
	parts := []string{selector.Sel.Name}
	var expression ast.Expr = selector.X
	for {
		switch typed := expression.(type) {
		case *ast.Ident:
			parts = append([]string{typed.Name}, parts...)
			return strings.Join(parts, ".")
		case *ast.SelectorExpr:
			parts = append([]string{typed.Sel.Name}, parts...)
			expression = typed.X
		default:
			return strings.Join(parts, ".")
		}
	}
}
