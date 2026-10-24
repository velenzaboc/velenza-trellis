package gcpauth

import (
	"context"
	"crypto/mldsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testGoogleAuthority = "google.test"

// TestStrictGoogleTLSConfigRequiresTheApprovedHybridAndRecordsIt is a source-
// unit proof. Its private ML-DSA certificate and loopback peer do not assert
// anything about a Google endpoint; the later activation probe owns that fact.
func TestStrictGoogleTLSConfigRequiresTheApprovedHybridAndRecordsIt(t *testing.T) {
	server, roots := newGoogleTLSTestServer(t, testGoogleAuthority, []tls.CurveID{tls.X25519MLKEM768})
	var observations []GoogleTLSObservation
	config, err := newStrictGoogleTLSConfig(testGoogleAuthority, strictGoogleTransportOptions{
		rootCAs: roots,
		observe: func(observation GoogleTLSObservation) error {
			observations = append(observations, observation)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.MinVersion != tls.VersionTLS13 || config.MaxVersion != tls.VersionTLS13 {
		t.Fatalf("TLS versions = (%x,%x), want TLS 1.3 only", config.MinVersion, config.MaxVersion)
	}
	if len(config.CurvePreferences) != 1 || config.CurvePreferences[0] != tls.X25519MLKEM768 {
		t.Fatalf("CurvePreferences = %v, want only X25519MLKEM768", config.CurvePreferences)
	}
	if config.ServerName != testGoogleAuthority || config.InsecureSkipVerify || config.VerifyConnection == nil {
		t.Fatalf("strict config authority/verification = (%q,%v,%v)", config.ServerName, config.InsecureSkipVerify, config.VerifyConnection != nil)
	}

	connection := dialGoogleTLSTestServer(t, server.address, config)
	state := connection.ConnectionState()
	_ = connection.Close()
	server.wait(t)
	if state.Version != tls.VersionTLS13 || state.CurveID != tls.X25519MLKEM768 {
		t.Fatalf("negotiated state = version %x curve %v", state.Version, state.CurveID)
	}
	if len(observations) != 1 {
		t.Fatalf("observations = %d, want exactly one", len(observations))
	}
	if got := observations[0]; got.Authority != testGoogleAuthority || got.TLSVersion != tls.VersionTLS13 || got.CurveID != tls.X25519MLKEM768 || got.CipherSuite == 0 {
		t.Fatalf("observation = %+v", got)
	}
}

// The classical peer is an intentional [pqc-reference-only] negative control.
// It exists only to prove that a compatibility fallback cannot reach
// application success.
func TestStrictGoogleTLSConfigRefusesAClassicalOnlyPeerBeforeApplicationData(t *testing.T) {
	server, roots := newGoogleTLSTestServer(t, testGoogleAuthority, []tls.CurveID{tls.X25519})
	var observed atomic.Int32
	config, err := newStrictGoogleTLSConfig(testGoogleAuthority, strictGoogleTransportOptions{
		rootCAs: roots,
		observe: func(GoogleTLSObservation) error {
			observed.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := &net.Dialer{}
	raw, err := dialer.DialContext(ctx, "tcp", server.address)
	if err != nil {
		t.Fatal(err)
	}
	connection := tls.Client(raw, config)
	err = connection.HandshakeContext(ctx)
	_ = connection.Close()
	server.wait(t)
	if err == nil {
		t.Fatal("classical-only peer completed the strict handshake")
	}
	if observed.Load() != 0 {
		t.Fatal("a refused classical-only handshake emitted a successful observation")
	}
}

func TestStrictGoogleTLSConfigPreservesCertificateAndSNIValidation(t *testing.T) {
	server, roots := newGoogleTLSTestServer(t, testGoogleAuthority, []tls.CurveID{tls.X25519MLKEM768})
	config, err := newStrictGoogleTLSConfig("wrong.test", strictGoogleTransportOptions{rootCAs: roots})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := &net.Dialer{}
	raw, err := dialer.DialContext(ctx, "tcp", server.address)
	if err != nil {
		t.Fatal(err)
	}
	connection := tls.Client(raw, config)
	err = connection.HandshakeContext(ctx)
	_ = connection.Close()
	server.wait(t)
	if err == nil || !strings.Contains(err.Error(), "wrong.test") {
		t.Fatalf("wrong-SNI handshake error = %v", err)
	}
}

func TestImpersonatedRefreshConsumesTheStrictDefaultClientAfterContextDetachment(t *testing.T) {
	clearGoogleTransportEnvironment(t)
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

	var constructorCalls atomic.Int32
	var requestHosts []string
	original := newCredentialHTTPClient
	newCredentialHTTPClient = func() (*http.Client, error) {
		constructorCalls.Add(1)
		return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if err := request.Context().Err(); err != nil {
				return nil, fmt.Errorf("refresh inherited canceled construction context: %w", err)
			}
			requestHosts = append(requestHosts, request.URL.Hostname())
			body := `{"access_token":"source-token","token_type":"Bearer","expires_in":3600}`
			if request.URL.Hostname() == "iamcredentials.googleapis.com" {
				body = `{"accessToken":"target-token","expireTime":"2099-01-01T00:00:00Z"}`
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    request,
			}, nil
		})}, nil
	}
	t.Cleanup(func() { newCredentialHTTPClient = original })

	constructionContext, cancel := context.WithCancel(context.Background())
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: identity,
	}
	source, err := selector.TokenSource(constructionContext, CloudPlatformScope)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	token, err := source.Token()
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != "target-token" {
		t.Fatalf("token = %#v", token)
	}
	if constructorCalls.Load() != 1 {
		t.Fatalf("strict credential client constructors = %d, want one", constructorCalls.Load())
	}
	wantHosts := []string{"oauth2.googleapis.com", "iamcredentials.googleapis.com"}
	if fmt.Sprint(requestHosts) != fmt.Sprint(wantHosts) {
		t.Fatalf("refresh hosts = %v, want %v", requestHosts, wantHosts)
	}
}

func TestCredentialHTTPFactoryClosesItsGoogleAuthoritiesAndUsesStrictTransport(t *testing.T) {
	want := []string{"oauth2.googleapis.com", "iamcredentials.googleapis.com"}
	if fmt.Sprint(credentialHTTPAuthorities) != fmt.Sprint(want) {
		t.Fatalf("credential HTTP authorities = %v, want %v", credentialHTTPAuthorities, want)
	}
	clearGoogleTransportEnvironment(t)
	client, err := newCredentialHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	if client == nil || client.Transport == nil {
		t.Fatal("credential HTTP factory did not construct the shared strict transport")
	}
}

func TestStrictGoogleTransportStillRefusesAmbientPQDowngrades(t *testing.T) {
	for _, fragment := range []string{"tlsmlkem=0", "tlssecpmlkem=0"} {
		t.Run(fragment, func(t *testing.T) {
			clearGoogleTransportEnvironment(t)
			t.Setenv("GODEBUG", fragment)
			client, err := NewStrictGoogleHTTPClient(
				[]string{"oauth2.googleapis.com", "iamcredentials.googleapis.com"},
				time.Second,
				nil,
			)
			if err == nil || client != nil || !strings.Contains(err.Error(), fragment) {
				t.Fatalf("NewStrictGoogleHTTPClient() = (%v,%v), want %q refusal", client, err, fragment)
			}
		})
	}
}

type googleTLSTestServer struct {
	address string
	done    <-chan error
}

func (s googleTLSTestServer) wait(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("TLS fixture server did not finish")
	}
}

func newGoogleTLSTestServer(t *testing.T, authority string, curves []tls.CurveID) (googleTLSTestServer, *x509.CertPool) {
	t.Helper()
	key, err := mldsa.GenerateKey(mldsa.MLDSA65())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: authority},
		DNSNames:              []string{authority},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	tlsListener := tls.NewListener(listener, &tls.Config{
		MinVersion:       tls.VersionTLS13,
		MaxVersion:       tls.VersionTLS13,
		CurvePreferences: append([]tls.CurveID(nil), curves...),
		Certificates:     []tls.Certificate{certificate},
	})
	done := make(chan error, 1)
	go func() {
		connection, acceptErr := tlsListener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		done <- connection.(*tls.Conn).Handshake()
	}()
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return googleTLSTestServer{address: listener.Addr().String(), done: done}, roots
}

func dialGoogleTLSTestServer(t *testing.T, address string, config *tls.Config) *tls.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := &net.Dialer{}
	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	connection := tls.Client(raw, config)
	if err := connection.HandshakeContext(ctx); err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	return connection
}

func clearGoogleTransportEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("GODEBUG", "")
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("CLOUDSDK_CONFIG", "")
}
