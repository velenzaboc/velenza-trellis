package vertexai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func testAuth(t *testing.T) *Auth {
	t.Helper()
	auth, err := NewAuth(context.Background(), func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token", Expiry: time.Now().Add(time.Hour)}), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func TestEmbedRequestAndResponseContracts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantErr    string
		maxBytes   int
		wantVector []float32
	}{
		{name: "success_and_truncation", status: http.StatusOK, body: `{"predictions":[{"embeddings":{"values":[1,2]}}]}`, maxBytes: 3, wantVector: []float32{1, 2}},
		{name: "non_success_is_rejected", status: http.StatusForbidden, body: "denied", wantErr: "status 403", maxBytes: 64},
		{name: "empty_predictions_is_rejected", status: http.StatusOK, body: `{"predictions":[]}`, wantErr: "no predictions", maxBytes: 64},
		{name: "dimension_mismatch_is_rejected", status: http.StatusOK, body: `{"predictions":[{"embeddings":{"values":[1]}}]}`, wantErr: "dimensions mismatch", maxBytes: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
					t.Errorf("Authorization = %q", got)
				}
				if tc.name == "success_and_truncation" {
					var body string
					_, _ = fmt.Fscan(r.Body, &body)
					if !strings.Contains(body, `"content":"abc"`) {
						t.Errorf("request body did not carry truncated content: %s", body)
					}
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client := &EmbedClient{auth: testAuth(t), httpC: server.Client(), target: server.URL, cfg: EmbedConfig{Dimensions: 2, MaxTextBytes: tc.maxBytes}}
			got, err := client.Embed(context.Background(), "abcdef", TaskTypeRetrievalDocument)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Embed error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || len(got) != len(tc.wantVector) {
				t.Fatalf("Embed = %v, %v", got, err)
			}
			for i := range got {
				if got[i] != tc.wantVector[i] {
					t.Errorf("vector[%d] = %v, want %v", i, got[i], tc.wantVector[i])
				}
			}
		})
	}
}

func TestFlashResponseContracts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		want    string
		wantErr string
	}{
		{name: "success", status: http.StatusOK, body: `{"candidates":[{"content":{"parts":[{"text":"answer"}]}}]}`, want: "answer"},
		{name: "non_success", status: http.StatusBadGateway, body: "upstream unavailable", wantErr: "status 502"},
		{name: "malformed_json", status: http.StatusOK, body: `{`, wantErr: "decode"},
		{name: "no_candidates", status: http.StatusOK, body: `{"candidates":[]}`, wantErr: "no candidates"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
					t.Errorf("Authorization = %q", got)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client := &FlashClient{auth: testAuth(t), httpC: server.Client(), target: server.URL, cfg: FlashConfig{MaxOutputTokens: 16}}
			got, err := client.Generate(context.Background(), "prompt")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Generate error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("Generate = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestClientConstructionAndBearerInputsFailClosed(t *testing.T) {
	if _, err := NewAuth(nil, nil); err == nil || !strings.Contains(err.Error(), "construction context") {
		t.Fatalf("NewAuth(nil, nil) error = %v", err)
	}
	if _, err := NewAuth(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "factory is required") {
		t.Fatalf("NewAuth nil factory error = %v", err)
	}
	if _, err := NewEmbedClient(nil, EmbedConfig{}); err == nil || !strings.Contains(err.Error(), "auth is required") {
		t.Fatalf("NewEmbedClient nil auth error = %v", err)
	}
	if _, err := NewFlashClient(nil, FlashConfig{}); err == nil || !strings.Contains(err.Error(), "auth is required") {
		t.Fatalf("NewFlashClient nil auth error = %v", err)
	}
	if err := (*Auth)(nil).AttachBearer(nil); err == nil || !strings.Contains(err.Error(), "request/auth required") {
		t.Fatalf("AttachBearer nil inputs error = %v", err)
	}
}
