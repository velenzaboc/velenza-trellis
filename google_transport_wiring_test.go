package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// This case binds the independently exercised strict transport to the actual
// review-service Spanner constructor. It is source wiring evidence only; real
// Google endpoint negotiation remains an activation check.
func TestSpannerReviewClientConsumesStrictTLSWithoutReplacingPerRPCCredentials(t *testing.T) {
	calls := callsInFunction(t, "substrate.go", "getClient")
	for _, name := range []string{
		"TokenSource",
		"NewStrictGoogleGRPCDialOption",
		"WithTokenSource",
		"WithGRPCDialOption",
		"spanner.NewClient",
	} {
		if calls.count[name] != 1 {
			t.Errorf("getClient %s call count = %d, want exactly one", name, calls.count[name])
		}
	}
	if calls.count["WithGRPCConn"] != 0 {
		t.Fatal("getClient replaced Google client-library authentication with a caller-owned connection")
	}
	if calls.first["NewStrictGoogleGRPCDialOption"] >= calls.first["spanner.NewClient"] {
		t.Fatal("strict gRPC transport is not constructed before spanner.NewClient")
	}
	strictCall := calls.call["NewStrictGoogleGRPCDialOption"]
	if strictCall == nil || len(strictCall.Args) < 2 {
		t.Fatal("strict gRPC constructor must bind an authority and a non-secret negotiation observer")
	}
	authority, ok := strictCall.Args[0].(*ast.BasicLit)
	if !ok || authority.Kind != token.STRING {
		t.Fatal("strict gRPC authority is not a fixed string literal")
	}
	gotAuthority, err := strconv.Unquote(authority.Value)
	if err != nil || gotAuthority != "spanner.googleapis.com" {
		t.Fatalf("strict gRPC authority = %q, want spanner.googleapis.com", gotAuthority)
	}
	if identifier, ok := strictCall.Args[1].(*ast.Ident); ok && identifier.Name == "nil" {
		t.Fatal("strict gRPC transport discarded negotiated state")
	}
}

func TestProductionGoogleCallersCannotInjectAnAlternateHTTPClient(t *testing.T) {
	for _, path := range []string{"substrate.go", "mcp.go", "review_producer.go"} {
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "HTTPClient" {
				t.Errorf("%s injects oauth2.HTTPClient outside the credential selector", path)
			}
			return true
		})
	}
}

type functionCalls struct {
	count map[string]int
	first map[string]token.Pos
	call  map[string]*ast.CallExpr
}

func callsInFunction(t *testing.T, path, functionName string) functionCalls {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := functionCalls{count: map[string]int{}, first: map[string]token.Pos{}, call: map[string]*ast.CallExpr{}}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != functionName {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := transportCallName(call.Fun)
			if name == "" {
				return true
			}
			result.count[name]++
			if _, exists := result.first[name]; !exists {
				result.first[name] = call.Pos()
				result.call[name] = call
			}
			return true
		})
		return result
	}
	t.Fatalf("function %s not found in %s", functionName, path)
	return result
}

func transportCallName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		if qualifier, ok := typed.X.(*ast.Ident); ok {
			return qualifier.Name + "." + typed.Sel.Name
		}
		return typed.Sel.Name
	default:
		return ""
	}
}
