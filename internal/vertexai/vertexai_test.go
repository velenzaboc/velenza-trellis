package vertexai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// stubTokenSource produces a fixed access token; used to bypass real Google
// credential resolution in tests.
type stubTokenSource struct{ tok string }

func (s stubTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: s.tok}, nil
}

type tokenSourceFunc func() (*oauth2.Token, error)

func (f tokenSourceFunc) Token() (*oauth2.Token, error) { return f() }

func newAuthForTest(token string) *Auth {
	auth, err := NewAuth(context.Background(), func(context.Context) (oauth2.TokenSource, error) {
		return stubTokenSource{tok: token}, nil
	})
	if err != nil {
		panic(err)
	}
	return auth
}

func TestNewAuthUsesOnlyBoundContextualTokenFactory(t *testing.T) {
	auth, err := NewAuth(context.Background(), func(context.Context) (oauth2.TokenSource, error) {
		return stubTokenSource{tok: "bound-token"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "https://example.invalid", nil)
	if err := auth.AttachBearer(req); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer bound-token" {
		t.Fatalf("Authorization = %q, want %q", got, "Bearer bound-token")
	}
}

func TestNewAuthRejectsMissingBoundTokenFactory(t *testing.T) {
	if _, err := NewAuth(context.Background(), nil); err == nil {
		t.Fatal("expected missing token factory error")
	}
}

func TestAuthRejectsNilOrEmptyAccessToken(t *testing.T) {
	for _, source := range []oauth2.TokenSource{
		tokenSourceFunc(func() (*oauth2.Token, error) { return nil, nil }),
		stubTokenSource{},
	} {
		auth, err := NewAuth(context.Background(), func(context.Context) (oauth2.TokenSource, error) {
			return source, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodGet, "https://example.invalid", nil)
		if err := auth.AttachBearer(req); err == nil {
			t.Fatal("AttachBearer accepted a nil or empty access token")
		}
		if got := req.Header.Get("Authorization"); got != "" {
			t.Fatalf("Authorization = %q after rejected token", got)
		}
	}
}

// ---------------- URL builders ----------------

func TestPredictURL(t *testing.T) {
	got, err := PredictURL("us-central1", "proj-x", "gemini-embedding-001")
	if err != nil {
		t.Fatal(err)
	}
	want := "https://us-central1-aiplatform.googleapis.com/v1/projects/proj-x/locations/us-central1/publishers/google/models/gemini-embedding-001:predict"
	if got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestGenerateContentURL(t *testing.T) {
	got, err := GenerateContentURL("us-central1", "proj-x", "gemini-2.0-flash")
	if err != nil {
		t.Fatal(err)
	}
	want := "https://us-central1-aiplatform.googleapis.com/v1/projects/proj-x/locations/us-central1/publishers/google/models/gemini-2.0-flash:generateContent"
	if got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

// ---------------- Auth ----------------

func TestAuth_AttachBearer(t *testing.T) {
	a := newAuthForTest("test-token-123")
	req, _ := http.NewRequest("POST", "https://example", nil)
	if err := a.AttachBearer(req); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer test-token-123" {
		t.Errorf("Authorization header: got %q", got)
	}
}

func TestAuthTokenRefreshUsesRequestContext(t *testing.T) {
	auth, err := NewAuth(context.Background(), func(ctx context.Context) (oauth2.TokenSource, error) {
		return tokenSourceFunc(func() (*oauth2.Token, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example", nil)
	started := time.Now()
	err = auth.AttachBearer(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh error=%v, want request deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("request cancellation did not stop token refresh: %v", elapsed)
	}
}

func TestAuthTokenRefreshIsBoundedWithoutCallerDeadline(t *testing.T) {
	auth, err := NewAuth(context.Background(), func(ctx context.Context) (oauth2.TokenSource, error) {
		return tokenSourceFunc(func() (*oauth2.Token, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	auth.tokenTimeout = 20 * time.Millisecond
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example", nil)
	started := time.Now()
	err = auth.AttachBearer(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("background refresh error=%v, want internal deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("background token refresh exceeded internal bound: %v", elapsed)
	}
}

func TestAuthRefreshWaiterCanCancelWithoutWaitingForOwner(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	auth, err := NewAuth(context.Background(), func(ctx context.Context) (oauth2.TokenSource, error) {
		return tokenSourceFunc(func() (*oauth2.Token, error) {
			once.Do(func() { close(entered) })
			select {
			case <-release:
				return &oauth2.Token{AccessToken: "refreshed-token"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerDone := make(chan error, 1)
	ownerReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example", nil)
	go func() { ownerDone <- auth.AttachBearer(ownerReq) }()
	<-entered

	waiterCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	waiterReq, _ := http.NewRequestWithContext(waiterCtx, http.MethodPost, "https://example", nil)
	if err := auth.AttachBearer(waiterReq); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting refresh error=%v, want independent request cancellation", err)
	}
	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatalf("refresh owner failed after release: %v", err)
	}
}

func TestAuthRefreshWaiterRetriesAfterOwnerCancellation(t *testing.T) {
	firstRefreshStarted := make(chan struct{})
	var callsMu sync.Mutex
	tokenCalls := 0
	auth, err := NewAuth(context.Background(), func(ctx context.Context) (oauth2.TokenSource, error) {
		return tokenSourceFunc(func() (*oauth2.Token, error) {
			callsMu.Lock()
			tokenCalls++
			call := tokenCalls
			callsMu.Unlock()
			if call == 1 {
				close(firstRefreshStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &oauth2.Token{AccessToken: "waiter-token", Expiry: time.Now().Add(time.Hour)}, nil
		}), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerReq, _ := http.NewRequestWithContext(ownerCtx, http.MethodPost, "https://example", nil)
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- auth.AttachBearer(ownerReq) }()
	<-firstRefreshStarted

	waiterReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example", nil)
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- auth.AttachBearer(waiterReq) }()
	// Keep the first refresh in flight long enough for the second request to
	// become its waiter. The waiter owns a live context and must not inherit the
	// first request's cancellation outcome.
	time.Sleep(20 * time.Millisecond)
	cancelOwner()

	if err := <-ownerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh owner error=%v, want context cancellation", err)
	}
	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("live waiter inherited canceled refresh: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("live waiter did not retry the canceled refresh")
	}
	if got := waiterReq.Header.Get("Authorization"); got != "Bearer waiter-token" {
		t.Fatalf("Authorization = %q, want retried waiter token", got)
	}
}

func TestAuthRefreshWaiterSharesProviderCancellationWhenOwnerContextIsLive(t *testing.T) {
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var callsMu sync.Mutex
	tokenCalls := 0
	auth, err := NewAuth(context.Background(), func(context.Context) (oauth2.TokenSource, error) {
		return tokenSourceFunc(func() (*oauth2.Token, error) {
			callsMu.Lock()
			tokenCalls++
			callsMu.Unlock()
			select {
			case <-refreshStarted:
			default:
				close(refreshStarted)
			}
			<-releaseRefresh
			return nil, context.Canceled
		}), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	ownerDone := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example", nil)
		ownerDone <- auth.AttachBearer(req)
	}()
	<-refreshStarted

	waiterDone := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example", nil)
		waiterDone <- auth.AttachBearer(req)
	}()
	time.Sleep(20 * time.Millisecond)
	close(releaseRefresh)

	if err := <-ownerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh owner error=%v, want provider cancellation", err)
	}
	if err := <-waiterDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh waiter error=%v, want shared provider cancellation", err)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if tokenCalls != 1 {
		t.Fatalf("token source calls=%d, want one shared provider failure", tokenCalls)
	}
}

// ---------------- EmbedClient ----------------

func TestEmbedClient_TruncatesInput(t *testing.T) {
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"predictions":[{"embeddings":{"values":[0.1,0.2,0.3,0.4]}}]}`))
	}))
	defer srv.Close()

	c := &EmbedClient{
		auth:   newAuthForTest("t"),
		httpC:  srv.Client(),
		target: srv.URL,
		cfg:    EmbedConfig{Project: "p", Location: "l", Model: "m", Dimensions: 4, MaxTextBytes: 10},
	}
	vec, err := c.Embed(context.Background(), "this text is way longer than ten bytes", TaskTypeRetrievalDocument)
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) != 4 {
		t.Errorf("expected 4-dim vec, got %d", len(vec))
	}
	if !strings.Contains(receivedBody, `"content":"this text "`) {
		t.Errorf("expected truncated content in request body, got %q", receivedBody)
	}
}

func TestEmbedClient_DimensionsMismatchRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return a 3-dim vector even though config says 4.
		w.Write([]byte(`{"predictions":[{"embeddings":{"values":[0.1,0.2,0.3]}}]}`))
	}))
	defer srv.Close()
	c := &EmbedClient{
		auth: newAuthForTest("t"), httpC: srv.Client(), target: srv.URL,
		cfg: EmbedConfig{Dimensions: 4, MaxTextBytes: 1000},
	}
	if _, err := c.Embed(context.Background(), "x", TaskTypeRetrievalDocument); err == nil {
		t.Error("expected dimensions mismatch error")
	}
}

func TestEmbedClient_NonOKStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer srv.Close()
	c := &EmbedClient{
		auth: newAuthForTest("t"), httpC: srv.Client(), target: srv.URL,
		cfg: EmbedConfig{Dimensions: 4, MaxTextBytes: 1000},
	}
	_, err := c.Embed(context.Background(), "x", TaskTypeRetrievalDocument)
	if err == nil || !strings.Contains(err.Error(), "status 429") {
		t.Errorf("expected status-429 error, got %v", err)
	}
}

func TestEmbedClient_AttachesBearer(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"predictions":[{"embeddings":{"values":[0.1,0.2,0.3,0.4]}}]}`))
	}))
	defer srv.Close()
	c := &EmbedClient{
		auth: newAuthForTest("my-tok"), httpC: srv.Client(), target: srv.URL,
		cfg: EmbedConfig{Dimensions: 4, MaxTextBytes: 1000},
	}
	if _, err := c.Embed(context.Background(), "x", TaskTypeRetrievalDocument); err != nil {
		t.Fatal(err)
	}
	if receivedAuth != "Bearer my-tok" {
		t.Errorf("expected bearer my-tok, got %q", receivedAuth)
	}
}

// ---------------- FlashClient ----------------

func TestFlashClient_ParsesFirstCandidate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"the answer"}]}}]}`))
	}))
	defer srv.Close()
	c := &FlashClient{
		auth: newAuthForTest("t"), httpC: srv.Client(), target: srv.URL,
		cfg: FlashConfig{Project: "p", Location: "l", Model: "m", MaxOutputTokens: 100, Temperature: 0},
	}
	got, err := c.Generate(context.Background(), "what is x?")
	if err != nil {
		t.Fatal(err)
	}
	if got != "the answer" {
		t.Errorf("got %q", got)
	}
}

func TestFlashClient_EmptyCandidatesReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"candidates":[]}`))
	}))
	defer srv.Close()
	c := &FlashClient{
		auth: newAuthForTest("t"), httpC: srv.Client(), target: srv.URL,
		cfg: FlashConfig{MaxOutputTokens: 100, Temperature: 0},
	}
	if _, err := c.Generate(context.Background(), "x"); err == nil {
		t.Error("expected error on empty candidates")
	}
}

func TestFlashClient_PassesGenerationConfig(t *testing.T) {
	var received map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &received)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`))
	}))
	defer srv.Close()
	c := &FlashClient{
		auth: newAuthForTest("t"), httpC: srv.Client(), target: srv.URL,
		cfg: FlashConfig{MaxOutputTokens: 250, Temperature: 0.7},
	}
	if _, err := c.Generate(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	gc, ok := received["generationConfig"].(map[string]interface{})
	if !ok {
		t.Fatal("no generationConfig in request body")
	}
	if gc["maxOutputTokens"] != float64(250) {
		t.Errorf("maxOutputTokens: got %v", gc["maxOutputTokens"])
	}
	if gc["temperature"] != 0.7 {
		t.Errorf("temperature: got %v", gc["temperature"])
	}
}

func TestFlashClient_AttachesBearer(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`))
	}))
	defer srv.Close()
	c := &FlashClient{
		auth: newAuthForTest("my-tok"), httpC: srv.Client(), target: srv.URL,
		cfg: FlashConfig{MaxOutputTokens: 100, Temperature: 0},
	}
	if _, err := c.Generate(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if receivedAuth != "Bearer my-tok" {
		t.Errorf("expected bearer my-tok, got %q", receivedAuth)
	}
}
