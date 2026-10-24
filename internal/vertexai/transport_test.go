package vertexai

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
)

type vertexTransportFunc func(*http.Request) (*http.Response, error)

func (f vertexTransportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestVertexStrictFactoryIsTheSharedGoogleTransport(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "auth.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok {
			qualifier, qualified := selector.X.(*ast.Ident)
			if qualified && qualifier.Name == "gcpauth" && selector.Sel.Name == "NewStrictGoogleHTTPClient" {
				found = true
			}
		}
		return true
	})
	if !found {
		t.Fatal("Vertex strict-client factory is not bound to gcpauth.NewStrictGoogleHTTPClient")
	}
}

func TestConfiguredVertexClientsBindStrictTransportToTheirExactAuthority(t *testing.T) {
	original := newStrictGoogleHTTPClient
	var authorities [][]string
	newStrictGoogleHTTPClient = func(allowed []string, timeout time.Duration, observer gcpauth.GoogleTLSObserver) (*http.Client, error) {
		authorities = append(authorities, append([]string(nil), allowed...))
		if timeout <= 0 || observer == nil {
			t.Fatalf("strict Vertex constructor lost timeout or negotiated-state observer")
		}
		return &http.Client{Transport: vertexTransportFunc(func(request *http.Request) (*http.Response, error) {
			return nil, errors.New("request was not expected in constructor test")
		})}, nil
	}
	t.Cleanup(func() { newStrictGoogleHTTPClient = original })

	auth := newAuthForTest("fixture-token")
	if _, err := NewEmbedClient(auth, EmbedConfig{
		Project: "example-gcp-project", Location: "us-central1", Model: "gemini-embedding-001",
		Dimensions: 4, MaxTextBytes: 1024,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFlashClient(auth, FlashConfig{
		Project: "example-gcp-project", Location: "us-central1", Model: "gemini-2.5-flash",
		MaxOutputTokens: 16,
	}); err != nil {
		t.Fatal(err)
	}
	if len(authorities) != 2 {
		t.Fatalf("strict Vertex constructors = %d, want two", len(authorities))
	}
	for index, got := range authorities {
		if len(got) != 1 || got[0] != "us-central1-aiplatform.googleapis.com" {
			t.Fatalf("Vertex authority[%d] = %v", index, got)
		}
	}
}

func TestBearerClientRetainsRedirectRefusalOnTheStrictTransport(t *testing.T) {
	original := newStrictGoogleHTTPClient
	var requests int
	newStrictGoogleHTTPClient = func(allowed []string, timeout time.Duration, observer gcpauth.GoogleTLSObserver) (*http.Client, error) {
		return &http.Client{Transport: vertexTransportFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			return &http.Response{
				StatusCode: http.StatusTemporaryRedirect,
				Header:     http.Header{"Location": []string{"https://attacker.invalid/steal"}},
				Body:       io.NopCloser(strings.NewReader("redirect")),
				Request:    request,
			}, nil
		})}, nil
	}
	t.Cleanup(func() { newStrictGoogleHTTPClient = original })

	client, err := newBearerHTTPClient(
		"https://us-central1-aiplatform.googleapis.com/v1/projects/example-gcp-project/locations/us-central1/publishers/google/models/gemini-embedding-001:predict",
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://us-central1-aiplatform.googleapis.com/start", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || requests != 1 {
		t.Fatalf("redirect response/count = (%d,%d), want (307,1)", response.StatusCode, requests)
	}
}

func TestVertexConstructionRefusesMissingOrInvalidStrictTransport(t *testing.T) {
	original := newStrictGoogleHTTPClient
	newStrictGoogleHTTPClient = func([]string, time.Duration, gcpauth.GoogleTLSObserver) (*http.Client, error) {
		return nil, errors.New("strict transport unavailable")
	}
	t.Cleanup(func() { newStrictGoogleHTTPClient = original })
	auth := newAuthForTest("fixture-token")

	if _, err := NewEmbedClient(auth, EmbedConfig{
		Project: "example-gcp-project", Location: "us-central1", Model: "gemini-embedding-001",
		Dimensions: 4, MaxTextBytes: 1024,
	}); err == nil || !strings.Contains(err.Error(), "strict transport unavailable") {
		t.Fatalf("NewEmbedClient error = %v", err)
	}
	if _, err := NewFlashClient(auth, FlashConfig{
		Project: "example-gcp-project", Location: "us-central1", Model: "gemini-2.5-flash",
		MaxOutputTokens: 16,
	}); err == nil || !strings.Contains(err.Error(), "strict transport unavailable") {
		t.Fatalf("NewFlashClient error = %v", err)
	}
}

func TestBearerClientRejectsNonHTTPSOrNonCanonicalTargetBeforeTransportConstruction(t *testing.T) {
	original := newStrictGoogleHTTPClient
	called := false
	newStrictGoogleHTTPClient = func([]string, time.Duration, gcpauth.GoogleTLSObserver) (*http.Client, error) {
		called = true
		return &http.Client{}, nil
	}
	t.Cleanup(func() { newStrictGoogleHTTPClient = original })

	for _, target := range []string{
		"http://us-central1-aiplatform.googleapis.com/v1/predict",
		"https://us-central1-aiplatform.googleapis.com.evil.invalid/v1/predict",
	} {
		if client, err := newBearerHTTPClient(target, time.Second); err == nil || client != nil {
			t.Fatalf("newBearerHTTPClient(%q) = (%v,%v), want refusal", target, client, err)
		}
	}
	if called {
		t.Fatal("invalid Vertex target reached strict transport construction")
	}
}
