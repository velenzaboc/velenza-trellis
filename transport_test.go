package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTransportHealthAndMCPRouting(t *testing.T) {
	mcpCalled := false
	handler := transportHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mcpCalled = true
		w.WriteHeader(http.StatusAccepted)
	}), "test-secret")

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK || mcpCalled {
		t.Fatalf("health route code=%d mcpCalled=%v", health.Code, mcpCalled)
	}

	mcpResponse := httptest.NewRecorder()
	mcpRequest := httptest.NewRequest(http.MethodPost, "/", nil)
	mcpRequest.Header.Set("Authorization", "Bearer test-secret")
	handler.ServeHTTP(mcpResponse, mcpRequest)
	if mcpResponse.Code != http.StatusAccepted || !mcpCalled {
		t.Fatalf("MCP route code=%d mcpCalled=%v", mcpResponse.Code, mcpCalled)
	}

	mcpCalled = false
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/", nil))
	if unauthorized.Code != http.StatusUnauthorized || mcpCalled {
		t.Fatalf("unauthorized route code=%d mcpCalled=%v", unauthorized.Code, mcpCalled)
	}
}

func TestResolveTransportSecretFailsClosed(t *testing.T) {
	t.Setenv("TASK_GRAPH_TEST_SECRET", "")
	if _, err := resolveTransportSecret(&TransportAuthConfig{Mode: "bearer_env", SecretEnv: "TASK_GRAPH_TEST_SECRET"}); err == nil {
		t.Fatal("empty configured transport secret must fail")
	}
}
