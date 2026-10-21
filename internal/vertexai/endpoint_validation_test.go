package vertexai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/oauth2"
)

func TestEndpointBuildersRejectBearerTokenRedirectComponents(t *testing.T) {
	for _, tt := range []struct {
		name     string
		location string
		project  string
		model    string
	}{
		{name: "location_authority", location: "us-central1@evil.example", project: "example-gcp-project", model: "gemini-embedding-001"},
		{name: "location_path", location: "us-central1/path", project: "example-gcp-project", model: "gemini-embedding-001"},
		{name: "project_path", location: "us-central1", project: "example-gcp-project/locations/eu", model: "gemini-embedding-001"},
		{name: "model_query", location: "us-central1", project: "example-gcp-project", model: "gemini?alt=media"},
		{name: "model_fragment", location: "us-central1", project: "example-gcp-project", model: "gemini#fragment"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := PredictURL(tt.location, tt.project, tt.model); err == nil {
				t.Fatal("PredictURL() accepted redirect-capable component")
			}
			if _, err := GenerateContentURL(tt.location, tt.project, tt.model); err == nil {
				t.Fatal("GenerateContentURL() accepted redirect-capable component")
			}
		})
	}
}

func TestEndpointBuildersReturnCanonicalGoogleAuthority(t *testing.T) {
	predict, err := PredictURL("us-central1", "example-gcp-project", "gemini-embedding-001")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(predict, "https://us-central1-aiplatform.googleapis.com/") {
		t.Fatalf("PredictURL() = %q", predict)
	}
	generate, err := GenerateContentURL("us-central1", "123456789012", "gemini-2.5-flash")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(generate, "https://us-central1-aiplatform.googleapis.com/") {
		t.Fatalf("GenerateContentURL() = %q", generate)
	}
}

func TestBearerAuthenticatedClientsRefuseRedirects(t *testing.T) {
	auth, err := NewAuth(context.Background(), func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name         string
		responseBody string
		invoke       func(context.Context, string) error
	}{
		{
			name:         "embed",
			responseBody: `{"predictions":[{"embeddings":{"values":[1]}}]}`,
			invoke: func(ctx context.Context, target string) error {
				client, err := NewEmbedClient(auth, EmbedConfig{
					Project: "example-gcp-project", Location: "us-central1",
					Model: "gemini-embedding-001", Dimensions: 1, MaxTextBytes: 1024,
				})
				if err != nil {
					return err
				}
				client.target = target
				_, err = client.Embed(ctx, "text", TaskTypeRetrievalDocument)
				return err
			},
		},
		{
			name:         "flash",
			responseBody: `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`,
			invoke: func(ctx context.Context, target string) error {
				client, err := NewFlashClient(auth, FlashConfig{
					Project: "example-gcp-project", Location: "us-central1",
					Model: "gemini-2.5-flash", MaxOutputTokens: 16, Temperature: 0,
				})
				if err != nil {
					return err
				}
				client.target = target
				_, err = client.Generate(ctx, "prompt")
				return err
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var redirectedRequests atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				redirectedRequests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, tt.responseBody)
			}))
			defer destination.Close()

			redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
			}))
			defer redirector.Close()

			if err := tt.invoke(context.Background(), redirector.URL); err == nil {
				t.Fatal("bearer-authenticated request followed a redirect and succeeded")
			}
			if got := redirectedRequests.Load(); got != 0 {
				t.Fatalf("redirect destination received %d request(s), want 0", got)
			}
		})
	}
}
