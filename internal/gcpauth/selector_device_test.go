package gcpauth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/oauth2"
)

const directDeviceIdentity = "workstation-cube@example-gcp-project.iam.gserviceaccount.com"

// This synthetic key exercises Google's existing service-account OAuth protocol,
// whose JWT algorithm is provider-constrained RS256. It is not a Velenza signing
// purpose, a production key, or a substitute for PQ-protected Google transport.
// https://developers.google.com/identity/protocols/oauth2/service-account
var directFixtureOnce sync.Once
var directFixturePEM string
var directFixtureError error

func directDeviceDocument(t *testing.T) string {
	t.Helper()
	directFixtureOnce.Do(func() {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			directFixtureError = err
			return
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			directFixtureError = err
			return
		}
		directFixturePEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	})
	if directFixtureError != nil {
		t.Fatal(directFixtureError)
	}
	body, err := json.Marshal(map[string]any{
		"type": "service_account",
		"project_id": "example-gcp-project",
		"private_key_id": "synthetic-provider-fixture-key",
		"private_key": directFixturePEM,
		"client_email": directDeviceIdentity,
		"client_id": "100000000000000000000",
		"auth_uri": "https://accounts.google.com/o/oauth2/auth",
		"token_uri": "https://oauth2.googleapis.com/token",
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/workstation-cube%40example-gcp-project.iam.gserviceaccount.com",
		"universe_domain": "googleapis.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func directDeviceSelector(t *testing.T) Selector {
	t.Helper()
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}
	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_CONFIG", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "GODEBUG"} {
		t.Setenv(key, "")
	}
	return Selector{
		CredentialsFile: writeCredentialFile(t, directDeviceDocument(t), 0o600),
		CredentialsProfile: "service_account",
		CredentialsIdentity: directDeviceIdentity,
	}
}

func TestDirectServiceAccountSelectionIsExplicit(t *testing.T) {
	valid := directDeviceSelector(t)
	if err := valid.Validate("google_credentials", false); err != nil {
		t.Fatalf("explicit device file rejected: %v", err)
	}
	for _, tc := range []struct {
		name string
		change func(*Selector)
		production bool
		want string
	}{
		{"missing file", func(s *Selector) { s.CredentialsFile = "" }, false, "credentials"},
		{"ambient ADC", func(s *Selector) { s.CredentialsFile = ""; s.CredentialsSource = CredentialsSourceApplicationDefault }, false, "credentials_file"},
		{"ambiguous carriers", func(s *Selector) { s.CredentialsSource = CredentialsSourceApplicationDefault }, false, "mutually exclusive"},
		{"missing identity", func(s *Selector) { s.CredentialsIdentity = "" }, false, "service-account email"},
		{"human identity", func(s *Selector) { s.CredentialsIdentity = "human@example.com" }, false, "service-account email"},
		{"production file", func(s *Selector) {}, true, "production requires"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := valid
			tc.change(&selected)
			if err := selected.Validate("google_credentials", tc.production); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("selection error = %v; want %q", err, tc.want)
			}
		})
	}
}

func TestDirectDeviceRefreshUsesExactIdentityWithoutImpersonation(t *testing.T) {
	selector := directDeviceSelector(t)
	originalADC := defaultADCPath
	defaultADCPath = func() (string, error) { return "", fmt.Errorf("shared ADC must never be resolved") }
	t.Cleanup(func() { defaultADCPath = originalADC })
	calls := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		if req.Method != http.MethodPost || req.URL.String() != "https://oauth2.googleapis.com/token" {
			t.Fatalf("unexpected provider route %s %s; no IAM impersonation hop is permitted", req.Method, req.URL)
		}
		if err := req.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got := req.Form.Get("grant_type"); got != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			t.Fatalf("unexpected OAuth grant %q", got)
		}
		parts := strings.Split(req.Form.Get("assertion"), ".")
		if len(parts) != 3 {
			t.Fatal("Google SDK did not produce a signed JWT assertion")
		}
		claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		var claims map[string]any
		if err := json.Unmarshal(claimsJSON, &claims); err != nil {
			t.Fatal(err)
		}
		if claims["iss"] != directDeviceIdentity || claims["aud"] != "https://oauth2.googleapis.com/token" || claims["scope"] != CloudPlatformScope {
			t.Fatalf("SDK assertion does not bind the selected device identity, token endpoint and scope")
		}
		if subject := claims["sub"]; subject != nil && subject != "" {
			t.Fatal("direct device assertion unexpectedly delegates a subject")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"access_token":"synthetic-device-token","token_type":"Bearer","expires_in":3600}`)), Request: req}, nil
	})
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: transport}))
	tokenSource, err := selector.TokenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	token, err := tokenSource.Token()
	if err != nil || token == nil || token.AccessToken != "synthetic-device-token" || calls != 1 {
		t.Fatalf("direct refresh after construction cancellation: error=%v calls=%d", err, calls)
	}
}

func TestDirectDeviceRequestRefreshRetainsCancellation(t *testing.T) {
	selector := directDeviceSelector(t)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, req.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: transport}))
	tokenSource, err := selector.RequestTokenSourceFactory(CloudPlatformScope)(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := tokenSource.Token(); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("request cancellation was lost: %v", err)
	}
}

func TestDirectDeviceCarrierClosesIdentityAndJSON(t *testing.T) {
	valid := directDeviceDocument(t)
	if err := validateServiceAccountConfiguration([]byte(valid), directDeviceIdentity); err != nil {
		t.Fatalf("real exported key shape rejected: %v", err)
	}
	for _, tc := range []struct { name, body, want string }{
		{"wrong identity", strings.Replace(valid, directDeviceIdentity, "other-device@example-gcp-project.iam.gserviceaccount.com", 1), "credentials_identity"},
		{"wrong type", strings.Replace(valid, `"type":"service_account"`, `"type":"authorized_user"`, 1), "service_account"},
		{"wrong token endpoint", strings.Replace(valid, "https://oauth2.googleapis.com/token", "https://attacker.invalid/token", 1), "token_uri"},
		{"wrong project", strings.Replace(valid, `"project_id":"example-gcp-project"`, `"project_id":"other-project"`, 1), "project_id"},
		{"wrong universe", strings.Replace(valid, `"universe_domain":"googleapis.com"`, `"universe_domain":"attacker.invalid"`, 1), "universe_domain"},
		{"wrong auth endpoint", strings.Replace(valid, "https://accounts.google.com/o/oauth2/auth", "https://attacker.invalid/auth", 1), "auth_uri"},
		{"unknown field", strings.TrimSuffix(valid, "}") + `,"token_url":"https://attacker.invalid/token"}`, "not allowed"},
		{"impersonation field", strings.TrimSuffix(valid, "}") + `,"service_account_impersonation_url":"https://iamcredentials.googleapis.com/"}`, "not allowed"},
		{"case variant", strings.TrimSuffix(valid, "}") + `,"CLIENT_EMAIL":"other@example.com"}`, "not allowed"},
		{"duplicate field", strings.Replace(valid, `"type":"service_account"`, `"type":"service_account","type":"service_account"`, 1), "duplicate JSON key"},
		{"trailing document", valid + `{}`, "trailing"},
		{"null", `null`, "object"},
		{"array", `[]`, "object"},
		{"malformed", `{`, "decode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateServiceAccountConfiguration([]byte(tc.body), directDeviceIdentity); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("carrier error = %v; want %q", err, tc.want)
			}
		})
	}
	for _, field := range []string{"client_email", "private_key", "private_key_id", "token_uri"} {
		t.Run("missing/"+field, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal([]byte(valid), &body); err != nil { t.Fatal(err) }
			delete(body, field)
			encoded, err := json.Marshal(body)
			if err != nil { t.Fatal(err) }
			if err := validateServiceAccountConfiguration(encoded, directDeviceIdentity); err == nil || !strings.Contains(err.Error(), field) {
				t.Fatalf("missing field error = %v; want %q", err, field)
			}
		})
	}
}

func TestDirectDeviceTokenSourceRejectsUnsafeFilesAndOverrides(t *testing.T) {
	for _, tc := range []struct { name, key, value string }{
		{"ambient credential", "GOOGLE_APPLICATION_CREDENTIALS", "alternate.json"},
		{"alternate SDK root", "CLOUDSDK_CONFIG", "alternate-gcloud"},
		{"SDK credential override", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "alternate.json"},
		{"PQ downgrade", "GODEBUG", "tlsmlkem=0"},
		{"PQ alternate downgrade", "GODEBUG", "tlssecpmlkem=0"},
		{"bearer logging", "GODEBUG", "http2debug=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector := directDeviceSelector(t)
			t.Setenv(tc.key, tc.value)
			if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.key)) {
				t.Fatalf("override error = %v; want %s refusal", err, tc.key)
			}
		})
	}
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Run("transport/"+key, func(t *testing.T) {
			selector := directDeviceSelector(t)
			t.Setenv(key, "attacker.invalid")
			if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(key)) {
				t.Fatalf("transport override error = %v; want %s refusal", err, key)
			}
		})
	}
	t.Run("non-owner-readable file", func(t *testing.T) {
		selector := directDeviceSelector(t)
		selector.CredentialsFile = writeCredentialFile(t, directDeviceDocument(t), 0o640)
		if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), nonOwnerReadableCredentialError) {
			t.Fatalf("unsafe credential file accepted: %v", err)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		selector := directDeviceSelector(t)
		selector.CredentialsFile = filepath.Join(t.TempDir(), "missing-device.json")
		if _, err := selector.TokenSource(context.Background()); err == nil {
			t.Fatal("missing device carrier accepted")
		}
	})
	t.Run("directory", func(t *testing.T) {
		selector := directDeviceSelector(t)
		selector.CredentialsFile = t.TempDir()
		if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("directory carrier accepted: %v", err)
		}
	})
	t.Run("oversized file", func(t *testing.T) {
		selector := directDeviceSelector(t)
		selector.CredentialsFile = writeCredentialFile(t, strings.Repeat(" ", int(maxCredentialConfigurationBytes)+1), os.FileMode(0o600))
		if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("oversized carrier accepted: %v", err)
		}
	})
}
