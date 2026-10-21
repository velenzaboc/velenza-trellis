// Package gcpauth resolves one process-level, identity-bound Google credential
// selector. It deliberately does not use generic ADC discovery: the local arm
// validates an exact impersonated-ADC carrier and the hosted arm proves the
// attached metadata identity before constructing a metadata token source.
package gcpauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	CredentialsSourceApplicationDefault                = "application_default"
	CredentialsProfileImpersonatedServiceAccount       = "impersonated_service_account"
	CredentialsProfileAttachedServiceAccount           = "attached_service_account"
	CloudPlatformScope                                 = "https://www.googleapis.com/auth/cloud-platform"
	maxCredentialConfigurationBytes              int64 = 1 << 20
)

var serviceAccountEmailPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$`)

// Go 1.27 negotiates the hybrid X25519MLKEM768 key exchange by default, so the
// post-quantum property is present until something switches it off. tlsmlkem=0
// and tlssecpmlkem=0 are exactly that switch, and an ambient GODEBUG would have
// applied it silently; this process refuses to run instead. http2debug leaks
// bearer tokens into transport logs.
var forbiddenGODEBUGFragments = [...]string{
	"http2debug=",
	"tlsmlkem=0",
	"tlssecpmlkem=0",
}

// Ambient environment that can redirect, downgrade, or observe the Google
// transport this process builds. SPANNER_EMULATOR_HOST is deliberately absent:
// task-graph owns a narrow opt-in emulator bypass with its own guard
// (ValidateSpannerEmulatorHost, which demands both the experimental-bootstrap
// opt-in and the exact documented host), and sealing the key here would break
// that contract and the Makefile test targets that depend on it.
var ambientGoogleTransportOverrideKeys = [...]string{
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

const (
	sealedGoogleClientCertificate = "false"
	sealedGoogleMTLSEndpoint      = "never"
)

type metadataClient interface {
	OnGCEWithContext(context.Context) bool
	EmailWithContext(context.Context, string) (string, error)
	GetWithContext(context.Context, string) (string, error)
}

var (
	newMetadataClient  = func() metadataClient { return metadata.NewClient(nil) }
	computeTokenSource = func(account string, scopes ...string) oauth2.TokenSource {
		return google.ComputeTokenSource(account, scopes...)
	}
	defaultADCPath = func() (string, error) {
		const filename = "application_default_credentials.json"
		if runtime.GOOS == "windows" {
			root := strings.TrimSpace(os.Getenv("APPDATA"))
			if root == "" {
				return "", fmt.Errorf("APPDATA is required to resolve the well-known ADC carrier")
			}
			return filepath.Join(root, "gcloud", filename), nil
		}
		root := strings.TrimSpace(os.Getenv("HOME"))
		if root == "" {
			return "", fmt.Errorf("HOME is required to resolve the well-known ADC carrier")
		}
		return filepath.Join(root, ".config", "gcloud", filename), nil
	}
)

// Selector is the one credential selector shared by every Google client in a
// process. CredentialsFile is a bounded experimental compatibility carrier;
// production requires the application_default source plus one exact profile.
type Selector struct {
	CredentialsFile     string `json:"credentials_file,omitempty"`
	CredentialsSource   string `json:"credentials_source,omitempty"`
	CredentialsProfile  string `json:"credentials_profile,omitempty"`
	CredentialsIdentity string `json:"credentials_identity,omitempty"`
}

// Validate closes the selector before any client is constructed.
func (s Selector) Validate(label string, requireApplicationDefault bool) error {
	if label == "" {
		label = "google_credentials"
	}
	if s.CredentialsFile != "" && s.CredentialsSource != "" {
		return fmt.Errorf("%s: credentials_file and credentials_source are mutually exclusive", label)
	}
	if s.CredentialsFile == "" && s.CredentialsSource == "" {
		return fmt.Errorf("%s: credentials_source=%s is required", label, CredentialsSourceApplicationDefault)
	}
	if s.CredentialsSource != "" && s.CredentialsSource != CredentialsSourceApplicationDefault {
		return fmt.Errorf("%s: unsupported credentials_source %q", label, s.CredentialsSource)
	}
	if requireApplicationDefault && s.CredentialsFile != "" {
		return fmt.Errorf("%s: production requires credentials_source=%s", label, CredentialsSourceApplicationDefault)
	}
	if !serviceAccountEmailPattern.MatchString(s.CredentialsIdentity) {
		return fmt.Errorf("%s: credentials_identity %q is not a canonical user-managed service-account email", label, s.CredentialsIdentity)
	}
	switch s.CredentialsProfile {
	case CredentialsProfileImpersonatedServiceAccount:
		// Both the ADC environment carrier and the explicit experimental carrier
		// contain the same typed impersonation document and receive identical
		// byte-level validation in TokenSource.
	case CredentialsProfileAttachedServiceAccount:
		if s.CredentialsSource != CredentialsSourceApplicationDefault || s.CredentialsFile != "" {
			return fmt.Errorf("%s: attached_service_account requires credentials_source=%s", label, CredentialsSourceApplicationDefault)
		}
	case "":
		return fmt.Errorf("%s: credentials_profile is required", label)
	default:
		return fmt.Errorf("%s: unsupported credentials_profile %q", label, s.CredentialsProfile)
	}
	return nil
}

// TokenSource resolves the closed selector without falling back to ambient
// human ADC. The returned source is suitable for option.WithTokenSource.
func (s Selector) TokenSource(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return s.tokenSource(ctx, true, scopes...)
}

// TokenSourceForRequest resolves the same exact selector while binding any
// credential refresh I/O to ctx. It is for transports such as the Vertex REST
// wrapper that refresh before issuing a caller-owned request. Long-lived Google
// clients should use TokenSource so a construction context cannot poison later
// lazy refreshes.
func (s Selector) TokenSourceForRequest(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return s.tokenSource(ctx, false, scopes...)
}

// RequestTokenSourceFactory binds the selector and scopes without binding a
// refresh context. The caller supplies that context for each refresh.
func (s Selector) RequestTokenSourceFactory(scopes ...string) func(context.Context) (oauth2.TokenSource, error) {
	boundScopes := append([]string(nil), scopes...)
	return func(ctx context.Context) (oauth2.TokenSource, error) {
		return s.TokenSourceForRequest(ctx, boundScopes...)
	}
}

func (s Selector) tokenSource(ctx context.Context, detachRefresh bool, scopes ...string) (oauth2.TokenSource, error) {
	if ctx == nil {
		return nil, fmt.Errorf("google credentials context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("google credentials context: %w", err)
	}
	if err := s.Validate("google_credentials", false); err != nil {
		return nil, err
	}
	if err := sealAmbientGoogleTransport(); err != nil {
		return nil, err
	}
	if len(scopes) == 0 {
		scopes = []string{CloudPlatformScope}
	}
	switch s.CredentialsProfile {
	case CredentialsProfileImpersonatedServiceAccount:
		return s.impersonatedTokenSource(ctx, scopes, detachRefresh)
	case CredentialsProfileAttachedServiceAccount:
		return s.attachedTokenSource(ctx, scopes, detachRefresh)
	default:
		return nil, fmt.Errorf("google_credentials: unsupported credentials_profile %q", s.CredentialsProfile)
	}
}

// sealAmbientGoogleTransport closes the ambient environment before any Google
// client is constructed, then pins certificate discovery off. It runs for both
// credential profiles because the transport is shared by both.
func sealAmbientGoogleTransport() error {
	if err := validateSecuritySensitiveGODEBUG(os.Getenv("GODEBUG")); err != nil {
		return err
	}
	for _, key := range ambientGoogleTransportOverrideKeys {
		value := os.Getenv(key)
		switch key {
		case "GOOGLE_API_USE_CLIENT_CERTIFICATE":
			if value != "" && value != sealedGoogleClientCertificate {
				return fmt.Errorf("google_credentials: %s must be unset or equal %q; ambient Google transport overrides are forbidden", key, sealedGoogleClientCertificate)
			}
		case "GOOGLE_API_USE_MTLS_ENDPOINT":
			if value != "" && value != sealedGoogleMTLSEndpoint {
				return fmt.Errorf("google_credentials: %s must be unset or equal %q; ambient Google transport overrides are forbidden", key, sealedGoogleMTLSEndpoint)
			}
		default:
			if value != "" {
				return fmt.Errorf("google_credentials: %s must be unset; ambient Google transport overrides are forbidden", key)
			}
		}
	}
	for key, value := range map[string]string{
		"GOOGLE_API_USE_CLIENT_CERTIFICATE": sealedGoogleClientCertificate,
		"GOOGLE_API_USE_MTLS_ENDPOINT":      sealedGoogleMTLSEndpoint,
	} {
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("google_credentials: seal %s: %w", key, err)
		}
	}
	return nil
}

func validateSecuritySensitiveGODEBUG(value string) error {
	for _, fragment := range forbiddenGODEBUGFragments {
		if strings.Contains(value, fragment) {
			return fmt.Errorf("google_credentials: GODEBUG contains forbidden setting %q", fragment)
		}
	}
	return nil
}

func (s Selector) impersonatedTokenSource(ctx context.Context, scopes []string, detachRefresh bool) (oauth2.TokenSource, error) {
	if override := strings.TrimSpace(os.Getenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE")); override != "" {
		return nil, fmt.Errorf("google_credentials: CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE must be unset for identity-bound impersonation")
	}
	path := s.CredentialsFile
	if s.CredentialsSource == CredentialsSourceApplicationDefault {
		if override := strings.TrimSpace(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")); override != "" {
			return nil, fmt.Errorf("google_credentials: GOOGLE_APPLICATION_CREDENTIALS must be unset; impersonated application_default uses only the validated well-known gcloud ADC carrier")
		}
		if override := strings.TrimSpace(os.Getenv("CLOUDSDK_CONFIG")); override != "" {
			return nil, fmt.Errorf("google_credentials: CLOUDSDK_CONFIG must be unset; impersonated application_default uses only the well-known gcloud ADC location")
		}
		var err error
		path, err = defaultADCPath()
		if err != nil {
			return nil, fmt.Errorf("google_credentials: resolve well-known impersonated ADC carrier: %w", err)
		}
	} else if ambient := strings.TrimSpace(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")); ambient != "" {
		return nil, fmt.Errorf("google_credentials: GOOGLE_APPLICATION_CREDENTIALS must be unset when credentials_file is selected explicitly")
	}
	data, err := readOwnerOnlyRegularFile(path)
	if err != nil {
		return nil, fmt.Errorf("google_credentials: read impersonated ADC carrier: %w", err)
	}
	if err := validateImpersonatedConfiguration(data, s.CredentialsIdentity); err != nil {
		return nil, fmt.Errorf("google_credentials: validate impersonated ADC carrier: %w", err)
	}
	// A Google client keeps its token source for the client's full lifetime.
	// Spanner initializes lazily, so ctx can be the first request context; if
	// that cancellation leaked into the OAuth refresh source, every later token
	// refresh would fail after the first request completed. Copy only the OAuth
	// HTTP client onto a fresh lifetime context: retaining every request-scoped
	// context value would unnecessarily pin caller data for the client's life.
	credentialContext := ctx
	if detachRefresh {
		credentialContext = context.Background()
		if client, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); ok && client != nil {
			credentialContext = context.WithValue(credentialContext, oauth2.HTTPClient, client)
		}
	}
	credentials, err := google.CredentialsFromJSONWithType(credentialContext, data, google.ImpersonatedServiceAccount, scopes...)
	if err != nil {
		return nil, fmt.Errorf("google_credentials: parse validated impersonated ADC carrier: %w", err)
	}
	if credentials == nil || credentials.TokenSource == nil {
		return nil, fmt.Errorf("google_credentials: validated impersonated ADC carrier returned no token source")
	}
	return credentials.TokenSource, nil
}

func (s Selector) attachedTokenSource(ctx context.Context, scopes []string, detachRefresh bool) (oauth2.TokenSource, error) {
	if override := strings.TrimSpace(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")); override != "" {
		return nil, fmt.Errorf("google_credentials: GOOGLE_APPLICATION_CREDENTIALS must be unset for attached_service_account")
	}
	if override := strings.TrimSpace(os.Getenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE")); override != "" {
		return nil, fmt.Errorf("google_credentials: CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE must be unset for attached_service_account")
	}
	if override := strings.TrimSpace(os.Getenv("GCE_METADATA_HOST")); override != "" {
		return nil, fmt.Errorf("google_credentials: GCE_METADATA_HOST must be unset for attached_service_account")
	}
	client := newMetadataClient()
	if client == nil || !client.OnGCEWithContext(ctx) {
		return nil, fmt.Errorf("google_credentials: attached_service_account requires Google-hosted metadata")
	}
	email, err := client.EmailWithContext(ctx, "default")
	if err != nil {
		return nil, fmt.Errorf("google_credentials: resolve attached metadata identity: %w", err)
	}
	email = strings.TrimSpace(email)
	if email != s.CredentialsIdentity {
		return nil, fmt.Errorf("google_credentials: attached metadata identity %q does not match credentials_identity %q", email, s.CredentialsIdentity)
	}
	if !detachRefresh {
		return requestMetadataTokenSource{ctx: ctx, client: client, account: s.CredentialsIdentity, scopes: append([]string(nil), scopes...)}, nil
	}
	// Address the exact identity we just proved instead of re-resolving the
	// mutable "default" alias for the token request.
	source := computeTokenSource(s.CredentialsIdentity, scopes...)
	if source == nil {
		return nil, fmt.Errorf("google_credentials: metadata token source is unavailable")
	}
	return source, nil
}

type requestMetadataTokenSource struct {
	ctx     context.Context
	client  metadataClient
	account string
	scopes  []string
}

func (s requestMetadataTokenSource) Token() (*oauth2.Token, error) {
	tokenURI := "instance/service-accounts/" + url.PathEscape(s.account) + "/token"
	if len(s.scopes) > 0 {
		values := url.Values{}
		values.Set("scopes", strings.Join(s.scopes, ","))
		tokenURI += "?" + values.Encode()
	}
	raw, err := s.client.GetWithContext(s.ctx, tokenURI)
	if err != nil {
		return nil, err
	}
	var response oauth2.Token
	if err := json.NewDecoder(strings.NewReader(raw)).Decode(&response); err != nil {
		return nil, fmt.Errorf("google_credentials: invalid token JSON from metadata: %w", err)
	}
	if response.ExpiresIn == 0 || strings.TrimSpace(response.AccessToken) == "" {
		return nil, fmt.Errorf("google_credentials: incomplete token received from metadata")
	}
	token := &oauth2.Token{
		AccessToken: response.AccessToken,
		TokenType:   response.TokenType,
		Expiry:      time.Now().Add(time.Duration(response.ExpiresIn) * time.Second),
	}
	return token.WithExtra(map[string]any{
		"oauth2.google.tokenSource":    "compute-metadata",
		"oauth2.google.serviceAccount": s.account,
	}), nil
}

func readOwnerOnlyRegularFile(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("credential path is required")
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("credential path must be absolute")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if err := validateCredentialPathMode(pathInfo.Mode()); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return nil, fmt.Errorf("credential file identity changed while opening")
	}
	if err := validateCredentialFileAccess(file, openedInfo); err != nil {
		return nil, err
	}
	limited := io.LimitReader(file, maxCredentialConfigurationBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxCredentialConfigurationBytes {
		return nil, fmt.Errorf("credential configuration exceeds %d bytes", maxCredentialConfigurationBytes)
	}
	return data, nil
}

func validateCredentialPathMode(mode os.FileMode) error {
	if mode&os.ModeSymlink != 0 {
		return fmt.Errorf("credential path must not be a symlink")
	}
	if !mode.IsRegular() {
		return fmt.Errorf("credential path must be a regular file")
	}
	return nil
}

func validateImpersonatedConfiguration(data []byte, identity string) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	if err := validateCredentialObjectKeys(raw, "top-level", []string{
		"type", "service_account_impersonation_url", "delegates", "source_credentials", "scopes", "quota_project_id",
	}); err != nil {
		return err
	}
	// source_credentials carries its own type-scoped field rules below, and the
	// device shape legitimately holds a private_key there. Everything ELSE in the
	// document — top level, delegates, scopes, quota_project_id — stays under the
	// unconditional ban, so the exemption cannot be smuggled anywhere else.
	forbiddenScan := make(map[string]any, len(raw))
	for key, value := range raw {
		if key == "source_credentials" {
			continue
		}
		forbiddenScan[key] = value
	}
	if err := rejectForbiddenCredentialFields(forbiddenScan); err != nil {
		return err
	}
	if credentialType, _ := raw["type"].(string); credentialType != CredentialsProfileImpersonatedServiceAccount {
		return fmt.Errorf("top-level type must be %q", CredentialsProfileImpersonatedServiceAccount)
	}
	targetURL, _ := raw["service_account_impersonation_url"].(string)
	if err := validateImpersonationURL(targetURL, identity); err != nil {
		return err
	}
	if configuredQuotaProject, exists := raw["quota_project_id"]; exists {
		value, ok := configuredQuotaProject.(string)
		if !ok || value != serviceAccountProject(identity) {
			return fmt.Errorf("quota_project_id %q must match the impersonated service account project %q when present", value, serviceAccountProject(identity))
		}
	}
	source, ok := raw["source_credentials"].(map[string]any)
	if !ok {
		return fmt.Errorf("source_credentials object is required")
	}
	// Two source shapes are accepted, and ONLY these two.
	//
	// authorized_user — a human's gcloud ADC. Historically the only shape.
	//
	// service_account — the WORKSTATION's own device key, used as the base
	// credential this process impersonates FROM. Added 2026-09-06 so a daemon can
	// authenticate from machine-held material instead of a human OAuth refresh
	// token. That dependency was not theoretical: one human reauth wall
	// (invalid_grant / "reauth related error (invalid_rapt)") took the task-plan
	// write path down on all three workstations at once, and the sibling
	// history-rag daemons were found crash-looping on the identical error.
	//
	// This does NOT widen effective authority. Impersonation REPLACES the caller's
	// authority with the target's, and the target is still pinned to
	// credentials_identity by validateImpersonationURL below — unchanged. A broad
	// device key here yields exactly the narrow impersonated identity's rights.
	sourceType, _ := source["type"].(string)
	switch sourceType {
	case "authorized_user":
		if err := validateCredentialObjectKeys(source, "source_credentials", []string{
			"type", "client_id", "client_secret", "refresh_token", "token_uri", "rapt_token", "universe_domain", "account",
		}); err != nil {
			return err
		}
		for _, field := range []string{"client_id", "client_secret", "refresh_token"} {
			value, ok := source[field].(string)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("source_credentials.%s is required", field)
			}
		}
	case "service_account":
		// The allowlist is the REAL exported key shape, measured against
		// ~/.config/velenza-backup/workstation-<host>.json rather than an idealised subset:
		// auth_provider_x509_cert_url, auth_uri, client_id and client_x509_cert_url
		// are present in every gcloud-exported key. Omitting them would reject the
		// actual file with a confusing "is not allowed", so they are admitted
		// explicitly. They are structurally unused by the token construction path
		// (golang.org/x/oauth2/google reads only client_email, private_key,
		// private_key_id and token_uri), so admitting them grants nothing.
		if err := validateCredentialObjectKeys(source, "source_credentials", []string{
			"type", "project_id", "private_key_id", "private_key", "client_email", "client_id",
			"auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url", "universe_domain",
		}); err != nil {
			return err
		}
		for _, field := range []string{"client_email", "private_key_id", "private_key"} {
			value, ok := source[field].(string)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("source_credentials.%s is required", field)
			}
		}
		email, _ := source["client_email"].(string)
		if !serviceAccountEmailPattern.MatchString(email) {
			return fmt.Errorf("source_credentials.client_email %q is not a canonical service-account email", email)
		}
		if authURI, exists := source["auth_uri"]; exists {
			value, ok := authURI.(string)
			if !ok || value != "https://accounts.google.com/o/oauth2/auth" {
				return fmt.Errorf("source_credentials.auth_uri must equal %q when present", "https://accounts.google.com/o/oauth2/auth")
			}
		}
	default:
		return fmt.Errorf("source_credentials.type must be %q or %q", "authorized_user", "service_account")
	}
	if tokenURI, exists := source["token_uri"]; exists {
		value, ok := tokenURI.(string)
		if !ok || value != "https://oauth2.googleapis.com/token" {
			return fmt.Errorf("source_credentials.token_uri must equal %q when present", "https://oauth2.googleapis.com/token")
		}
	}
	if universeDomain, exists := source["universe_domain"]; exists {
		value, ok := universeDomain.(string)
		if !ok || value != "googleapis.com" {
			return fmt.Errorf("source_credentials.universe_domain must equal %q when present", "googleapis.com")
		}
	}
	for _, field := range []string{"rapt_token", "account"} {
		if value, exists := source[field]; exists {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("source_credentials.%s must be a string when present", field)
			}
		}
	}
	delegates, ok := raw["delegates"].([]any)
	if !ok || len(delegates) != 0 {
		return fmt.Errorf("delegates must be an empty array")
	}
	if configuredScopes, exists := raw["scopes"]; exists {
		scopes, ok := configuredScopes.([]any)
		if !ok || len(scopes) != 1 {
			return fmt.Errorf("scopes must contain only %q when present", CloudPlatformScope)
		}
		value, ok := scopes[0].(string)
		if !ok || value != CloudPlatformScope {
			return fmt.Errorf("scopes must contain only %q when present", CloudPlatformScope)
		}
	}
	return nil
}

func validateCredentialObjectKeys(object map[string]any, label string, allowedKeys []string) error {
	allowed := make(map[string]struct{}, len(allowedKeys))
	for _, key := range allowedKeys {
		allowed[key] = struct{}{}
	}
	for key := range object {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%s field %q is not allowed", label, key)
		}
	}
	return nil
}

func validateImpersonationURL(rawURL, identity string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("service_account_impersonation_url: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Host != "iamcredentials.googleapis.com" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("service_account_impersonation_url must use the canonical IAMCredentials authority")
	}
	wantPath := "/v1/projects/-/serviceAccounts/" + identity + ":generateAccessToken"
	if parsed.EscapedPath() != wantPath {
		return fmt.Errorf("service_account_impersonation_url target %q does not match credentials_identity %q", parsed.EscapedPath(), identity)
	}
	return nil
}

func serviceAccountProject(identity string) string {
	localAndProject := strings.TrimSuffix(identity, ".iam.gserviceaccount.com")
	separator := strings.LastIndexByte(localAndProject, '@')
	if separator < 0 || separator == len(localAndProject)-1 {
		return ""
	}
	return localAndProject[separator+1:]
}

func rejectForbiddenCredentialFields(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			switch key {
			case "private_key", "private_key_id":
				return fmt.Errorf("credential field %q is forbidden", key)
			}
			if err := rejectForbiddenCredentialFields(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := rejectForbiddenCredentialFields(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("credential JSON contains trailing data")
		}
		return fmt.Errorf("credential JSON trailing data: %w", err)
	}
	return nil
}

// ValidateConfigCredentialShape lets focused tools consume their intentional
// subset of the shared config envelope without accepting-and-ignoring a second
// credential selector or embedded credential material elsewhere in that
// envelope. Functional non-credential fields remain owned by each tool's
// subset parser.
func ValidateConfigCredentialShape(data []byte) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return fmt.Errorf("config credential shape: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("config credential shape: decode JSON: %w", err)
	}
	for key, value := range raw {
		if key == "google_credentials" {
			selector, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("config credential shape: google_credentials must be an object")
			}
			if err := validateCredentialObjectKeys(selector, "google_credentials", []string{
				"credentials_file", "credentials_source", "credentials_profile", "credentials_identity",
			}); err != nil {
				return fmt.Errorf("config credential shape: %w", err)
			}
			continue
		}
		if isCredentialBearingKey(key) {
			return fmt.Errorf("config credential shape: credential-bearing field %q is only allowed inside google_credentials", key)
		}
		if err := rejectCredentialFieldsOutsideSelector(value, key); err != nil {
			return err
		}
	}
	return nil
}

func rejectCredentialFieldsOutsideSelector(value any, path string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			childPath := path + "." + key
			if key == "google_credentials" || isCredentialBearingKey(key) {
				return fmt.Errorf("config credential shape: credential-bearing field %q is forbidden outside top-level google_credentials", childPath)
			}
			if err := rejectCredentialFieldsOutsideSelector(child, childPath); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range typed {
			if err := rejectCredentialFieldsOutsideSelector(child, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func isCredentialBearingKey(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(key))
	if strings.Contains(normalized, "credential") {
		return true
	}
	switch normalized {
	case "privatekey", "privatekeyid", "clientid", "clientsecret", "refreshtoken", "accesstoken",
		"tokenuri", "rapttoken", "quotaprojectid", "serviceaccountimpersonationurl":
		return true
	default:
		return false
	}
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode credential JSON: %w", err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode credential JSON key: %w", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("credential JSON object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("credential JSON object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("credential JSON array is not closed")
		}
	default:
		return fmt.Errorf("unexpected credential JSON delimiter %q", delimiter)
	}
	return nil
}
