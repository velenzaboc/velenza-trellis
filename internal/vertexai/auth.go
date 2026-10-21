// Package vertexai is the shared brick for Vertex AI access across the
// task-graph codebase. It owns Google credential resolution, the HTTP transport,
// and the per-endpoint URL construction. Per Arbor Grand Vision Principle
// 12 (matryoshka mandate: discover before you create; reuse by
// reference), every Vertex AI consumer in this repo — the main daemon's
// embedding path, the spec-cluster CLI, and the spec-dedup-verify CLI —
// imports this package; there is exactly ONE Vertex AI client surface.
//
// Per Vision Principle 7 (binary_is_never_sot), this package holds the
// transport mechanics only. Every value the wire request depends on —
// project, location, model, dimensions, credential selector,
// max_output_tokens, temperature, prompt content — is supplied by the
// caller, sourced from spec-graph.config.json or on-disk prompt files
// (never hardcoded).
package vertexai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

const defaultTokenRefreshTimeout = 30 * time.Second

var (
	vertexLocationPattern      = regexp.MustCompile(`^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	vertexProjectIDPattern     = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	vertexProjectNumberPattern = regexp.MustCompile(`^[0-9]{6,20}$`)
	vertexModelPattern         = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$`)
)

// Auth holds the selected Google oauth2 token source. Reused across every
// Vertex AI request the caller issues. Concurrent-safe.
type Auth struct {
	mu                    sync.Mutex
	tokenSourceForContext TokenSourceFactory
	cachedToken           *oauth2.Token
	refresh               *tokenRefresh
	tokenTimeout          time.Duration
}

// TokenSourceFactory reconstructs the exact validated credential source for
// one refresh under the refresh caller's context. A fixed oauth2.TokenSource
// cannot provide this guarantee because TokenSource.Token has no context
// parameter and commonly captures its construction context.
type TokenSourceFactory func(context.Context) (oauth2.TokenSource, error)

type tokenRefresh struct {
	done               chan struct{}
	token              *oauth2.Token
	err                error
	ownerContextFailed bool
}

// NewAuth binds Vertex transport to the validated process-level token-source
// factory. The construction call proves that the configured credential carrier
// and identity resolve now; the returned source is deliberately discarded so
// later refreshes are rebuilt under the request context that owns them.
func NewAuth(ctx context.Context, factory TokenSourceFactory) (*Auth, error) {
	if ctx == nil {
		return nil, fmt.Errorf("vertexai: construction context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("vertexai: construction context: %w", err)
	}
	if factory == nil {
		return nil, fmt.Errorf("vertexai: token source factory is required")
	}
	source, err := factory(ctx)
	if err != nil {
		return nil, fmt.Errorf("vertexai: validate token source factory: %w", err)
	}
	if source == nil {
		return nil, fmt.Errorf("vertexai: token source factory returned nil")
	}
	return &Auth{tokenSourceForContext: factory, tokenTimeout: defaultTokenRefreshTimeout}, nil
}

// newBearerHTTPClient refuses redirects after Authorization has been attached.
// Callers must validate and address the intended endpoint directly; a redirect
// is an error response, never a second bearer-token authority decision.
func newBearerHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// AttachBearer fetches a current oauth2 token and sets the request's
// Authorization header. Concurrent-safe.
func (a *Auth) AttachBearer(req *http.Request) error {
	if a == nil || req == nil {
		return fmt.Errorf("vertexai: bearer request/auth required")
	}
	timeout := a.tokenTimeout
	if timeout <= 0 {
		timeout = defaultTokenRefreshTimeout
	}
	tokenContext, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	tok, err := a.token(tokenContext)
	if err != nil {
		return fmt.Errorf("vertexai: get token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	return nil
}

func (a *Auth) token(ctx context.Context) (*oauth2.Token, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a.mu.Lock()
		if a.cachedToken != nil && a.cachedToken.Valid() {
			token := a.cachedToken
			a.mu.Unlock()
			return token, nil
		}
		if inFlight := a.refresh; inFlight != nil {
			a.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-inFlight.done:
				if inFlight.err == nil {
					return inFlight.token, nil
				}
				// The refresh owner binds credential I/O to its own request
				// context. A still-live waiter must retry after that owner is
				// canceled instead of inheriting a different request's failure.
				// Failures not caused by the owner's context are shared because
				// retrying a provider error once per waiter would amplify it.
				if inFlight.ownerContextFailed {
					continue
				}
				return nil, inFlight.err
			}
		}
		inFlight := &tokenRefresh{done: make(chan struct{})}
		a.refresh = inFlight
		factory := a.tokenSourceForContext
		a.mu.Unlock()

		var token *oauth2.Token
		source, err := factory(ctx)
		if err != nil {
			err = fmt.Errorf("construct request-bound token source: %w", err)
		} else if source == nil {
			err = fmt.Errorf("token source factory returned nil")
		} else {
			token, err = source.Token()
			if err == nil && (token == nil || !token.Valid()) {
				err = fmt.Errorf("token source returned an invalid token")
			}
		}

		a.mu.Lock()
		if err == nil {
			a.cachedToken = token
		}
		inFlight.token = token
		inFlight.err = err
		inFlight.ownerContextFailed = ctx.Err() != nil && errors.Is(err, ctx.Err())
		a.refresh = nil
		close(inFlight.done)
		a.mu.Unlock()
		return token, err
	}
}

// ValidateEndpointComponents closes every caller-controlled component before
// it can participate in a bearer-authenticated URL.
func ValidateEndpointComponents(location, project, model string) error {
	if !vertexLocationPattern.MatchString(location) {
		return fmt.Errorf("vertexai: location %q is not a canonical lowercase DNS label", location)
	}
	if !vertexProjectIDPattern.MatchString(project) && !vertexProjectNumberPattern.MatchString(project) {
		return fmt.Errorf("vertexai: project %q is not a canonical GCP project id or number", project)
	}
	if !vertexModelPattern.MatchString(model) {
		return fmt.Errorf("vertexai: model %q is not a canonical publisher model path segment", model)
	}
	return nil
}

func vertexEndpointURL(location, project, model, action string) (string, error) {
	if err := ValidateEndpointComponents(location, project, model); err != nil {
		return "", err
	}
	if action != "predict" && action != "generateContent" {
		return "", fmt.Errorf("vertexai: endpoint action %q is not supported", action)
	}
	expectedHost := location + "-aiplatform.googleapis.com"
	u := &url.URL{
		Scheme: "https",
		Host:   expectedHost,
		Path:   fmt.Sprintf("/v1/projects/%s/locations/%s/publishers/google/models/%s:%s", project, location, model, action),
	}
	canonical := u.String()
	parsed, err := url.Parse(canonical)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() != expectedHost || parsed.Port() != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("vertexai: constructed endpoint failed canonical Google authority validation")
	}
	return canonical, nil
}

// PredictURL constructs and validates the Vertex AI predict endpoint.
func PredictURL(location, project, model string) (string, error) {
	return vertexEndpointURL(location, project, model, "predict")
}

// GenerateContentURL constructs and validates the Vertex AI generateContent endpoint.
func GenerateContentURL(location, project, model string) (string, error) {
	return vertexEndpointURL(location, project, model, "generateContent")
}
