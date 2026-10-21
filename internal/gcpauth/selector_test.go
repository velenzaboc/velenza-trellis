package gcpauth

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

type fixedTokenSource struct{ token string }

func (s fixedTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: s.token}, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type fakeMetadataClient struct {
	onGCE bool
	email string
	err   error
	get   func(context.Context, string) (string, error)
}

func (f fakeMetadataClient) OnGCEWithContext(context.Context) bool { return f.onGCE }
func (f fakeMetadataClient) EmailWithContext(context.Context, string) (string, error) {
	return f.email, f.err
}
func (f fakeMetadataClient) GetWithContext(ctx context.Context, suffix string) (string, error) {
	if f.get == nil {
		return "", fmt.Errorf("unexpected metadata token request %q", suffix)
	}
	return f.get(ctx, suffix)
}

func TestSelectorValidateClosesADCUnion(t *testing.T) {
	validIdentity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	for _, tt := range []struct {
		name       string
		selector   Selector
		requireADC bool
		wantErr    string
	}{
		{
			name: "impersonated",
			selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault,
				CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: validIdentity},
		},
		{
			name: "attached",
			selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault,
				CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: validIdentity},
		},
		{name: "missing", wantErr: "credentials_source"},
		{name: "unknown_source", selector: Selector{CredentialsSource: "ambient_magic", CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: validIdentity}, wantErr: "unsupported credentials_source"},
		{name: "missing_profile", selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsIdentity: validIdentity}, wantErr: "credentials_profile"},
		{name: "unknown_profile", selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: "some_adc", CredentialsIdentity: validIdentity}, wantErr: "unsupported credentials_profile"},
		{name: "missing_identity", selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileAttachedServiceAccount}, wantErr: "credentials_identity"},
		{name: "human_identity", selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: "operator@example.com"}, wantErr: "service-account email"},
		{name: "file_in_production", requireADC: true, selector: Selector{CredentialsFile: "/tmp/adc.json", CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: validIdentity}, wantErr: "production requires"},
		{name: "ambiguous_carrier", selector: Selector{CredentialsFile: "/tmp/adc.json", CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: validIdentity}, wantErr: "mutually exclusive"},
		{name: "attached_file", selector: Selector{CredentialsFile: "/tmp/adc.json", CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: validIdentity}, wantErr: "attached_service_account requires"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.selector.Validate("google_credentials", tt.requireADC)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestTokenSourceRefusesMissingOrCanceledContextBeforeCredentialIO(t *testing.T) {
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: "task-graph-operator@example-gcp-project.iam.gserviceaccount.com",
	}
	if _, err := selector.TokenSource(nil); err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatalf("nil context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := selector.TokenSourceForRequest(ctx); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("canceled context error = %v", err)
	}
}

func TestImpersonatedTokenSourceValidatesExactCarrierBytes(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	path := writeCredentialFile(t, fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "source_credentials": {
    "type": "authorized_user",
    "client_id": "fixture.apps.googleusercontent.com",
    "client_secret": "fixture-secret",
    "refresh_token": "fixture-refresh",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}`, identity), 0o600)
	setDefaultADCPath(t, path)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("CLOUDSDK_CONFIG", "")

	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: identity,
	}
	ts, err := selector.TokenSource(context.Background(), "scope-a")
	if err != nil {
		t.Fatalf("TokenSource() unexpected error: %v", err)
	}
	if ts == nil {
		t.Fatal("TokenSource() returned nil")
	}
}

func TestImpersonatedTokenSourceOutlivesConstructionContext(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	path := writeCredentialFile(t, fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "quota_project_id": "example-gcp-project",
  "source_credentials": {
    "type": "authorized_user",
    "client_id": "fixture.apps.googleusercontent.com",
    "client_secret": "fixture-secret",
    "refresh_token": "fixture-refresh",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}`, identity), 0o600)
	setDefaultADCPath(t, path)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("CLOUDSDK_CONFIG", "")

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := req.Context().Err(); err != nil {
			return nil, fmt.Errorf("credential request inherited canceled construction context: %w", err)
		}
		body := `{"access_token":"source-token","token_type":"Bearer","expires_in":3600}`
		if strings.Contains(req.URL.Path, ":generateAccessToken") {
			if got := req.Header.Get("Authorization"); got != "Bearer source-token" {
				t.Fatalf("IAMCredentials Authorization = %q, want source token", got)
			}
			body = `{"accessToken":"target-token","expireTime":"2099-01-01T00:00:00Z"}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})
	constructionCtx, cancel := context.WithCancel(context.WithValue(
		context.Background(),
		oauth2.HTTPClient,
		&http.Client{Transport: transport},
	))
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: identity,
	}
	ts, err := selector.TokenSource(constructionCtx, CloudPlatformScope)
	if err != nil {
		t.Fatalf("TokenSource() unexpected error: %v", err)
	}
	cancel()

	token, err := ts.Token()
	if err != nil {
		t.Fatalf("Token() after construction context cancellation: %v", err)
	}
	if token == nil || token.AccessToken != "target-token" {
		t.Fatalf("Token() = %#v, want target-token", token)
	}
}

func TestImpersonatedRequestTokenSourceConsumesRefreshContext(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	path := writeCredentialFile(t, fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "quota_project_id": "example-gcp-project",
  "source_credentials": {
    "type": "authorized_user",
    "client_id": "fixture.apps.googleusercontent.com",
    "client_secret": "fixture-secret",
    "refresh_token": "fixture-refresh",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}`, identity), 0o600)
	setDefaultADCPath(t, path)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("CLOUDSDK_CONFIG", "")

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(100 * time.Millisecond):
			return nil, fmt.Errorf("refresh context was not propagated")
		}
	})
	refreshContext, cancel := context.WithCancel(context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: transport}))
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: identity,
	}
	tokenSource, err := selector.RequestTokenSourceFactory(CloudPlatformScope)(refreshContext)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := tokenSource.Token(); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("Token() error = %v, want request-context cancellation", err)
	}
}

func TestImpersonatedTokenSourceRejectsHostileCarrier(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	valid := func(target string) string {
		return fmt.Sprintf(`{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken","delegates":[],"quota_project_id":"example-gcp-project","source_credentials":{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"refresh","token_uri":"https://oauth2.googleapis.com/token"}}`, target)
	}
	selector := Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: identity}

	for _, tt := range []struct {
		name      string
		body      string
		mode      os.FileMode
		cloudSDK  string
		cloudRoot string
		wantError string
	}{
		{name: "wrong_target", body: valid("other-local@example-gcp-project.iam.gserviceaccount.com"), mode: 0o600, wantError: "does not match credentials_identity"},
		{name: "wrong_quota_project", body: strings.Replace(valid(identity), `"quota_project_id":"example-gcp-project"`, `"quota_project_id":"other-project"`, 1), mode: 0o600, wantError: "must match the impersonated service account project"},
		{name: "private_key", body: strings.TrimSuffix(valid(identity), "}") + `,"private_key":"forbidden"}`, mode: 0o600, wantError: "private_key"},
		{name: "nonempty_delegates", body: strings.Replace(valid(identity), `"delegates":[]`, `"delegates":["delegate@example-gcp-project.iam.gserviceaccount.com"]`, 1), mode: 0o600, wantError: "delegates"},
		{name: "case_variant_delegates", body: strings.Replace(valid(identity), `"delegates":[]`, `"delegates":[],"DELEGATES":["delegate@example-gcp-project.iam.gserviceaccount.com"]`, 1), mode: 0o600, wantError: "DELEGATES"},
		{name: "case_variant_impersonation_url", body: strings.Replace(valid(identity), `"delegates":[]`, `"SERVICE_ACCOUNT_IMPERSONATION_URL":"https://attacker.invalid/v1/token","delegates":[]`, 1), mode: 0o600, wantError: "SERVICE_ACCOUNT_IMPERSONATION_URL"},
		{name: "redirecting_source_token_uri", body: strings.Replace(valid(identity), "https://oauth2.googleapis.com/token", "https://attacker.invalid/token", 1), mode: 0o600, wantError: "source_credentials.token_uri"},
		{name: "case_variant_source_token_uri", body: strings.Replace(valid(identity), `"token_uri":"https://oauth2.googleapis.com/token"`, `"token_uri":"https://oauth2.googleapis.com/token","TOKEN_URI":"https://attacker.invalid/token"`, 1), mode: 0o600, wantError: "TOKEN_URI"},
		{name: "unknown_parser_field", body: strings.Replace(valid(identity), `"delegates":[]`, `"token_url":"https://attacker.invalid/token","delegates":[]`, 1), mode: 0o600, wantError: "token_url"},
		{name: "unexpected_target_scopes", body: strings.Replace(valid(identity), `"delegates":[]`, `"delegates":[],"scopes":["https://www.googleapis.com/auth/drive"]`, 1), mode: 0o600, wantError: "scopes must contain only"},
		{name: "multiple_target_scopes", body: strings.Replace(valid(identity), `"delegates":[]`, `"delegates":[],"scopes":["https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/drive"]`, 1), mode: 0o600, wantError: "scopes must contain only"},
		{name: "duplicate_key", body: strings.Replace(valid(identity), `"type":"impersonated_service_account"`, `"type":"impersonated_service_account","type":"impersonated_service_account"`, 1), mode: 0o600, wantError: "duplicate JSON key"},
		{name: "group_readable", body: valid(identity), mode: 0o640, wantError: nonOwnerReadableCredentialError},
		{name: "cloud_sdk_override", body: valid(identity), mode: 0o600, cloudSDK: "/tmp/human.json", wantError: "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"},
		{name: "cloud_sdk_config_override", body: valid(identity), mode: 0o600, cloudRoot: "/tmp/alternate-gcloud", wantError: "CLOUDSDK_CONFIG"},
		{name: "service_account_key", body: `{"type":"service_account","private_key_id":"x","private_key":"x"}`, mode: 0o600, wantError: "private_key"},
		{name: "incomplete_authorized_user", body: fmt.Sprintf(`{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken","quota_project_id":"example-gcp-project","source_credentials":{"type":"authorized_user","client_id":"id"}}`, identity), mode: 0o600, wantError: "client_secret"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := writeCredentialFile(t, tt.body, tt.mode)
			setDefaultADCPath(t, path)
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
			t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", tt.cloudSDK)
			t.Setenv("CLOUDSDK_CONFIG", tt.cloudRoot)
			_, err := selector.TokenSource(context.Background(), "scope-a")
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("TokenSource() error = %v, want substring %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateConfigCredentialShapeClosesHiddenSelectorsAndSecrets(t *testing.T) {
	valid := `{"mode":"experimental_bootstrap","google_credentials":{"credentials_source":"application_default","credentials_profile":"impersonated_service_account","credentials_identity":"task-graph-operator@example-gcp-project.iam.gserviceaccount.com"},"pipeline":{"cluster":{"embedding_batch_size":1}}}`
	for _, tt := range []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "closed_shape", body: valid},
		{name: "legacy_nested_file", body: strings.Replace(valid, `"embedding_batch_size":1`, `"embedding_batch_size":1,"credentials_file":"/tmp/key.json"`, 1), wantErr: "credentials_file"},
		{name: "case_variant_selector", body: strings.Replace(valid, `"embedding_batch_size":1`, `"embedding_batch_size":1,"CredentialsSource":"ambient"`, 1), wantErr: "CredentialsSource"},
		{name: "nested_second_selector", body: strings.Replace(valid, `"embedding_batch_size":1`, `"embedding_batch_size":1,"google_credentials":{}`, 1), wantErr: "google_credentials"},
		{name: "embedded_private_key", body: strings.Replace(valid, `"embedding_batch_size":1`, `"embedding_batch_size":1,"private_key":"forbidden"`, 1), wantErr: "private_key"},
		{name: "unknown_selector_member", body: strings.Replace(valid, `"credentials_identity":"task-graph-operator@example-gcp-project.iam.gserviceaccount.com"`, `"credentials_identity":"task-graph-operator@example-gcp-project.iam.gserviceaccount.com","credential_fallback":"ambient"`, 1), wantErr: "credential_fallback"},
		{name: "nested_quota_project", body: strings.Replace(valid, `"embedding_batch_size":1`, `"embedding_batch_size":1,"quota_project_id":"attacker-project"`, 1), wantErr: "quota_project_id"},
		{name: "nested_client_id", body: strings.Replace(valid, `"embedding_batch_size":1`, `"embedding_batch_size":1,"client_id":"hidden-client"`, 1), wantErr: "client_id"},
		{name: "nested_token_uri", body: strings.Replace(valid, `"embedding_batch_size":1`, `"embedding_batch_size":1,"token_uri":"https://attacker.invalid/token"`, 1), wantErr: "token_uri"},
		{name: "duplicate_selector", body: strings.Replace(valid, `"credentials_source":"application_default"`, `"credentials_source":"application_default","credentials_source":"application_default"`, 1), wantErr: "duplicate JSON key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfigCredentialShape([]byte(tt.body))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateConfigCredentialShape() unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateConfigCredentialShape() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateSpannerEmulatorHostClosesAuthenticationBypass(t *testing.T) {
	for _, tt := range []struct {
		name      string
		allow     bool
		host      string
		wantUse   bool
		wantError string
	}{
		{name: "unset", host: ""},
		{name: "canonical", allow: true, host: "localhost:9010", wantUse: true},
		{name: "not_allowed", host: "localhost:9010", wantError: "forbidden outside experimental bootstrap"},
		{name: "wrong_port", allow: true, host: "localhost:1", wantError: "must equal"},
		{name: "numeric_loopback_alias", allow: true, host: "127.0.0.1:9010", wantError: "must equal"},
		{name: "remote_host", allow: true, host: "attacker.invalid:9010", wantError: "must equal"},
		{name: "surrounding_whitespace", allow: true, host: " localhost:9010 ", wantError: "must equal"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			use, err := ValidateSpannerEmulatorHost(tt.host, tt.allow)
			if tt.wantError == "" {
				if err != nil || use != tt.wantUse {
					t.Fatalf("ValidateSpannerEmulatorHost() = (%v, %v), want (%v, nil)", use, err, tt.wantUse)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("ValidateSpannerEmulatorHost() error = %v, want substring %q", err, tt.wantError)
			}
		})
	}
}

func TestImpersonatedTokenSourceRejectsOverridesSymlinkAndMissingCarrier(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	selector := Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: identity}
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("CLOUDSDK_CONFIG", "")

	t.Run("google_application_credentials_override", func(t *testing.T) {
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/alternate.json")
		if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), "must be unset") {
			t.Fatalf("TokenSource() error = %v, want environment override rejection", err)
		}
	})

	t.Run("missing_well_known_carrier", func(t *testing.T) {
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
		setDefaultADCPath(t, filepath.Join(t.TempDir(), "missing-adc.json"))
		if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), "read impersonated ADC carrier") {
			t.Fatalf("TokenSource() error = %v, want missing well-known carrier rejection", err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		target := writeCredentialFile(t, `{}`, 0o600)
		link := filepath.Join(t.TempDir(), "adc-link.json")
		if err := os.Symlink(target, link); err != nil {
			if !credentialSymlinkFixtureUnavailable(err) {
				t.Fatal(err)
			}
			if modeErr := validateCredentialPathMode(os.ModeSymlink); modeErr == nil || !strings.Contains(modeErr.Error(), "symlink") {
				t.Fatalf("validateCredentialPathMode(symlink) error = %v, want symlink rejection", modeErr)
			}
			return
		}
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
		setDefaultADCPath(t, link)
		if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("TokenSource() error = %v, want symlink rejection", err)
		}
	})
}

func TestAttachedTokenSourceBindsMetadataIdentityAndDirectSource(t *testing.T) {
	identity := "task-graph-runtime@example-gcp-project.iam.gserviceaccount.com"
	originalMetadata := newMetadataClient
	originalCompute := computeTokenSource
	t.Cleanup(func() {
		newMetadataClient = originalMetadata
		computeTokenSource = originalCompute
	})
	newMetadataClient = func() metadataClient {
		return fakeMetadataClient{onGCE: true, email: identity}
	}
	var gotAccount string
	var gotScopes []string
	computeTokenSource = func(account string, scopes ...string) oauth2.TokenSource {
		gotAccount = account
		gotScopes = append([]string(nil), scopes...)
		return fixedTokenSource{token: "metadata-token"}
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("GCE_METADATA_HOST", "")

	selector := Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: identity}
	ts, err := selector.TokenSource(context.Background(), "scope-a", "scope-b")
	if err != nil {
		t.Fatal(err)
	}
	if ts == nil || gotAccount != identity || strings.Join(gotScopes, ",") != "scope-a,scope-b" {
		t.Fatalf("direct metadata source = (%v, %q, %v), want nonnil/%s/[scope-a scope-b]", ts, gotAccount, gotScopes, identity)
	}
}

func TestAttachedRequestTokenSourceConsumesRefreshContext(t *testing.T) {
	identity := "task-graph-runtime@example-gcp-project.iam.gserviceaccount.com"
	originalMetadata := newMetadataClient
	t.Cleanup(func() { newMetadataClient = originalMetadata })
	var suffix string
	newMetadataClient = func() metadataClient {
		return fakeMetadataClient{
			onGCE: true,
			email: identity,
			get: func(ctx context.Context, got string) (string, error) {
				suffix = got
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				default:
					return `{"access_token":"metadata-token","token_type":"Bearer","expires_in":3600}`, nil
				}
			},
		}
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("GCE_METADATA_HOST", "")

	selector := Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: identity}
	tokenSource, err := selector.RequestTokenSourceFactory("scope-a", "scope-b")(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	token, err := tokenSource.Token()
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != "metadata-token" || !strings.Contains(suffix, "service-accounts/task-graph-runtime@example-gcp-project.iam.gserviceaccount.com/token") || !strings.Contains(suffix, "scopes=scope-a%2Cscope-b") {
		t.Fatalf("request-bound metadata token = (%q, %q)", token.AccessToken, suffix)
	}
}

func TestAttachedTokenSourceRejectsAmbientOverridesAndWrongMetadataIdentity(t *testing.T) {
	identity := "task-graph-runtime@example-gcp-project.iam.gserviceaccount.com"
	selector := Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: identity}
	originalMetadata := newMetadataClient
	t.Cleanup(func() { newMetadataClient = originalMetadata })

	for _, tt := range []struct {
		name         string
		gac          string
		cloudSDK     string
		metadataHost string
		onGCE        bool
		email        string
		wantErr      string
	}{
		{name: "gac_override", gac: "/tmp/human.json", onGCE: true, email: identity, wantErr: "GOOGLE_APPLICATION_CREDENTIALS"},
		{name: "cloud_sdk_override", cloudSDK: "/tmp/human.json", onGCE: true, email: identity, wantErr: "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"},
		{name: "metadata_host_override", metadataHost: "attacker.invalid", onGCE: true, email: identity, wantErr: "GCE_METADATA_HOST"},
		{name: "not_google_hosted", onGCE: false, email: identity, wantErr: "Google-hosted metadata"},
		{name: "wrong_identity", onGCE: true, email: "default-compute@developer.gserviceaccount.com", wantErr: "does not match credentials_identity"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", tt.gac)
			t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", tt.cloudSDK)
			t.Setenv("GCE_METADATA_HOST", tt.metadataHost)
			newMetadataClient = func() metadataClient { return fakeMetadataClient{onGCE: tt.onGCE, email: tt.email} }
			if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("TokenSource() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func writeCredentialFile(t *testing.T, body string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "adc.json")
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	configureCredentialFixtureAccess(t, path, mode)
	return path
}

func setDefaultADCPath(t *testing.T, path string) {
	t.Helper()
	original := defaultADCPath
	defaultADCPath = func() (string, error) { return path, nil }
	t.Cleanup(func() { defaultADCPath = original })
}

// The GODEBUG and ambient-transport seals below are the task-graph half of the
// PQC transport floor. code-graph has carried them since 2026-08-28 (6da9314);
// task-graph had neither, so an ambient GODEBUG could silently disable the Go
// 1.27 default hybrid X25519MLKEM768 key exchange with nothing failing closed.

func pqcSealTestCarrier(t *testing.T, identity string) {
	t.Helper()
	path := writeCredentialFile(t, fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "source_credentials": {
    "type": "authorized_user",
    "client_id": "fixture.apps.googleusercontent.com",
    "client_secret": "fixture-secret",
    "refresh_token": "fixture-refresh",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}`, identity), 0o600)
	setDefaultADCPath(t, path)
	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "CLOUDSDK_CONFIG"} {
		t.Setenv(key, "")
	}
}

func TestValidateSecuritySensitiveGODEBUGRejectsPQCDowngradeAndBearerLogging(t *testing.T) {
	wantFragments := []string{"http2debug=", "tlsmlkem=0", "tlssecpmlkem=0"}
	if len(forbiddenGODEBUGFragments) != len(wantFragments) {
		t.Fatalf("forbidden GODEBUG fragment count = %d, want %d", len(forbiddenGODEBUGFragments), len(wantFragments))
	}
	for index, want := range wantFragments {
		if got := forbiddenGODEBUGFragments[index]; got != want {
			t.Fatalf("forbidden GODEBUG fragment[%d] = %q, want %q", index, got, want)
		}
	}
	for _, tt := range []struct{ name, value, want string }{
		{name: "disable x25519 ml-kem", value: "tlsmlkem=0", want: "tlsmlkem=0"},
		{name: "disable secp ml-kem", value: "tlssecpmlkem=0", want: "tlssecpmlkem=0"},
		{name: "pqc downgrade beside benign settings", value: "netdns=go,tlsmlkem=0", want: "tlsmlkem=0"},
		{name: "downgrade after benign duplicate", value: "tlssecpmlkem=1,tlssecpmlkem=0", want: "tlssecpmlkem=0"},
		{name: "bearer token logging", value: "http2debug=2", want: "http2debug="},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateSecuritySensitiveGODEBUG(tt.value); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
	for _, value := range []string{"", "netdns=go", "tlsmlkem=1", "tlssecpmlkem=1", "netdns=go,tlsmlkem=1"} {
		t.Run("allows "+value, func(t *testing.T) {
			if err := validateSecuritySensitiveGODEBUG(value); err != nil {
				t.Fatalf("validate GODEBUG %q: %v", value, err)
			}
		})
	}
}

func TestTokenSourceSealsAmbientTransportAcrossBothProfiles(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	// SPANNER_EMULATOR_HOST is deliberately NOT sealed here: task-graph owns a
	// narrow, opt-in emulator bypass (ValidateSpannerEmulatorHost) that code-graph
	// does not have. Sealing it would break that documented contract.
	wantOverrides := []string{
		"EXPERIMENTAL_GOOGLE_API_USE_S2A",
		"GCE_METADATA_HOST",
		"GRPC_BINARY_LOG_FILTER",
		"GRPC_ENFORCE_ALPN_ENABLED",
		"HTTP_PROXY",
		"HTTPS_PROXY",
		"GOOGLE_API_GO_EXPERIMENTAL_DISABLE_NEW_AUTH_LIB",
		"GOOGLE_API_GO_EXPERIMENTAL_ENABLE_NEW_AUTH_LIB",
		"GOOGLE_API_CERTIFICATE_CONFIG",
		"GOOGLE_API_USE_CLIENT_CERTIFICATE",
		"GOOGLE_API_USE_MTLS",
		"GOOGLE_API_USE_MTLS_ENDPOINT",
		"GOOGLE_AUTH_TRUST_BOUNDARY_ENABLED",
		"GOOGLE_CLOUD_DISABLE_DIRECT_PATH",
		"GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS",
		"GOOGLE_CLOUD_QUOTA_PROJECT",
		"GOOGLE_CLOUD_UNIVERSE_DOMAIN",
		"GOOGLE_SPANNER_DISABLE_DIRECT_ACCESS_BOUND_TOKEN",
		"GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS",
		"GOOGLE_SPANNER_ENABLE_GCP_FALLBACK",
		"GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API",
		"NO_PROXY",
		"SSL_CERT_DIR",
		"SSL_CERT_FILE",
		"SPANNER_MONITORING_HOST",
		"http_proxy",
		"https_proxy",
		"no_proxy",
	}
	if len(ambientGoogleTransportOverrideKeys) != len(wantOverrides) {
		t.Fatalf("ambient override count = %d, want %d", len(ambientGoogleTransportOverrideKeys), len(wantOverrides))
	}
	for index, want := range wantOverrides {
		if got := ambientGoogleTransportOverrideKeys[index]; got != want {
			t.Fatalf("ambient override[%d] = %q, want %q", index, got, want)
		}
	}
	for _, key := range ambientGoogleTransportOverrideKeys {
		if key == "SPANNER_EMULATOR_HOST" {
			t.Fatal("SPANNER_EMULATOR_HOST must not be sealed: task-graph owns a gated emulator bypass")
		}
	}

	pqcSealTestCarrier(t, identity)
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: identity,
	}
	for _, key := range wantOverrides {
		t.Run("impersonated/"+key, func(t *testing.T) {
			for _, override := range ambientGoogleTransportOverrideKeys {
				t.Setenv(override, "")
			}
			t.Setenv(key, "attacker.invalid")
			if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(key)+" must be unset") {
				t.Fatalf("ambient override error = %v", err)
			}
		})
	}
	t.Run("impersonated/GODEBUG", func(t *testing.T) {
		for _, key := range ambientGoogleTransportOverrideKeys {
			t.Setenv(key, "")
		}
		t.Setenv("GODEBUG", "tlsmlkem=0")
		if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "tlsmlkem=0") {
			t.Fatalf("GODEBUG downgrade error = %v", err)
		}
	})
}

func TestSealAmbientGoogleTransportPinsCertificateDiscoveryOff(t *testing.T) {
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}
	t.Setenv("GODEBUG", "")
	if err := sealAmbientGoogleTransport(); err != nil {
		t.Fatalf("seal ambient Google transport: %v", err)
	}
	for key, want := range map[string]string{
		"GOOGLE_API_USE_CLIENT_CERTIFICATE": sealedGoogleClientCertificate,
		"GOOGLE_API_USE_MTLS_ENDPOINT":      sealedGoogleMTLSEndpoint,
	} {
		if got := os.Getenv(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

// The emulator bypass must keep working: it is task-graph's own contract and the
// Makefile test targets depend on it.
func TestSpannerEmulatorBypassSurvivesTheTransportSeal(t *testing.T) {
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}
	t.Setenv("GODEBUG", "")
	t.Setenv("SPANNER_EMULATOR_HOST", SpannerEmulatorHost)
	if err := sealAmbientGoogleTransport(); err != nil {
		t.Fatalf("seal must tolerate the gated emulator host: %v", err)
	}
	use, err := ValidateSpannerEmulatorHost(SpannerEmulatorHost, true)
	if err != nil || !use {
		t.Fatalf("emulator bypass = %v, %v; want true, nil", use, err)
	}
}

// deviceSourceCarrier renders an impersonated ADC carrier whose source is a
// WORKSTATION device service-account key. The source object carries the full
// eleven-field shape gcloud actually exports (measured against a real
// ~/.config/velenza-backup/workstation-<host>.json), not an idealised subset — an
// allowlist derived from a trimmed fixture would reject every real key file.
func deviceSourceCarrier(identity string) string {
	return fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "source_credentials": {
    "type": "service_account",
    "project_id": "example-gcp-project",
    "private_key_id": "fixturekeyid0000000000000000000000000000",
    "private_key": "-----BEGIN PRIVATE KEY-----\nZml4dHVyZQ==\n-----END PRIVATE KEY-----\n",
    "client_email": "workstation-caller@example-gcp-project.iam.gserviceaccount.com",
    "client_id": "100000000000000000000",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/workstation-caller%%40example-gcp-project.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
  }
}`, identity)
}

// A device key is accepted as the impersonation SOURCE. This is what lets a
// workstation daemon authenticate from machine-held material instead of a human
// OAuth refresh token, whose expiry took the whole fleet offline on 2026-09-06.
func TestImpersonatedCarrierAcceptsDeviceServiceAccountSource(t *testing.T) {
	identity := "task-graph-local@example-gcp-project.iam.gserviceaccount.com"
	if err := validateImpersonatedConfiguration([]byte(deviceSourceCarrier(identity)), identity); err != nil {
		t.Fatalf("device-sourced carrier rejected: %v", err)
	}
}

// The private_key exemption is scoped to source_credentials and NOWHERE else,
// and every other exactness guarantee still holds for the device shape.
func TestImpersonatedCarrierRejectsHostileDeviceSource(t *testing.T) {
	identity := "task-graph-local@example-gcp-project.iam.gserviceaccount.com"
	base := deviceSourceCarrier(identity)
	for _, tt := range []struct {
		name    string
		carrier string
		wantErr string
	}{
		{
			// The exemption must not leak upward. Caught by the TOP-LEVEL
			// allowlist (selector.go validateCredentialObjectKeys), which admits
			// no private_key key at all — not by rejectForbiddenCredentialFields.
			name:    "private_key at top level",
			carrier: strings.Replace(base, `"delegates": [],`, `"delegates": [], "private_key": "leaked",`, 1),
			wantErr: "not allowed",
		},
		{
			name:    "unknown field in device source",
			carrier: strings.Replace(base, `"type": "service_account",`, `"type": "service_account", "sneaky": "x",`, 1),
			wantErr: "not allowed",
		},
		{
			// Built fresh rather than mutated from the device carrier: injecting
			// into that one produces a DUPLICATE private_key, which trips
			// rejectDuplicateJSONKeys first and would test the wrong gate.
			// authorized_user must still refuse private_key via its own allowlist.
			name: "private_key smuggled into an authorized_user source",
			carrier: fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "source_credentials": {
    "type": "authorized_user",
    "client_id": "fixture.apps.googleusercontent.com",
    "client_secret": "fixture-secret",
    "refresh_token": "fixture-refresh",
    "private_key": "leaked"
  }
}`, identity),
			wantErr: "not allowed",
		},
		{
			name:    "non-canonical client_email",
			carrier: strings.Replace(base, `"client_email": "workstation-caller@example-gcp-project.iam.gserviceaccount.com"`, `"client_email": "attacker@example.com"`, 1),
			wantErr: "canonical service-account email",
		},
		{
			name:    "empty private_key",
			carrier: strings.Replace(base, `"private_key": "-----BEGIN PRIVATE KEY-----\nZml4dHVyZQ==\n-----END PRIVATE KEY-----\n"`, `"private_key": ""`, 1),
			wantErr: "private_key is required",
		},
		{
			name:    "unknown source type",
			carrier: strings.Replace(base, `"type": "service_account",`, `"type": "external_account",`, 1),
			wantErr: "must be",
		},
		{
			name:    "token_uri repointed off Google",
			carrier: strings.Replace(base, `"token_uri": "https://oauth2.googleapis.com/token"`, `"token_uri": "https://evil.example/token"`, 1),
			wantErr: "token_uri must equal",
		},
		{
			name:    "auth_uri repointed off Google",
			carrier: strings.Replace(base, `"auth_uri": "https://accounts.google.com/o/oauth2/auth"`, `"auth_uri": "https://evil.example/auth"`, 1),
			wantErr: "auth_uri must equal",
		},
		{
			// The impersonation target stays pinned to credentials_identity: a
			// device key must not be able to redirect itself at another SA.
			name:    "impersonation target redirected",
			carrier: strings.Replace(base, identity, "other-service-agent-local@example-gcp-project.iam.gserviceaccount.com", 1),
			wantErr: "",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateImpersonatedConfiguration([]byte(tt.carrier), identity)
			if err == nil {
				t.Fatalf("hostile carrier %q was ACCEPTED", tt.name)
			}
			if tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

// The pre-existing human shape must be entirely unaffected by the new branch.
func TestImpersonatedCarrierStillAcceptsAuthorizedUserSource(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	carrier := fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "source_credentials": {
    "type": "authorized_user",
    "client_id": "fixture.apps.googleusercontent.com",
    "client_secret": "fixture-secret",
    "refresh_token": "fixture-refresh",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}`, identity)
	if err := validateImpersonatedConfiguration([]byte(carrier), identity); err != nil {
		t.Fatalf("authorized_user carrier regressed: %v", err)
	}
}
