package gcpauth

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCredentialJSONRejectsDuplicateKeysAndMalformedDocuments(t *testing.T) {
	for _, tt := range []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "nested duplicate", document: `{"source":{"token":"first","token":"second"}}`, wantErr: "duplicate JSON key"},
		{name: "array duplicate", document: `[{"ok":true},{"scope":"a","scope":"b"}]`, wantErr: "duplicate JSON key"},
		{name: "trailing document", document: `{"one":1} {"two":2}`, wantErr: "trailing data"},
		{name: "unclosed object", document: `{"source":`, wantErr: "decode credential JSON"},
		{name: "unclosed array", document: `["value"`, wantErr: "decode credential JSON"},
		{name: "well formed scalar", document: `null`},
		{name: "well formed nested document", document: `{"source":[{"token":1},false]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectDuplicateJSONKeys([]byte(tt.document))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("rejectDuplicateJSONKeys() unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("rejectDuplicateJSONKeys() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestCredentialShapeRejectsSecretsAtEveryNestedBoundary(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   any
		wantErr string
	}{
		{name: "private key in object", value: map[string]any{"private_key": "secret"}, wantErr: "private_key"},
		{name: "key id in nested array", value: []any{map[string]any{"nested": []any{map[string]any{"private_key_id": "id"}}}}, wantErr: "private_key_id"},
		{name: "ordinary values", value: map[string]any{"items": []any{map[string]any{"label": "safe"}, float64(2)}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectForbiddenCredentialFields(tt.value)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("rejectForbiddenCredentialFields() unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("rejectForbiddenCredentialFields() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestCredentialShapeRejectsSelectorsOutsideTopLevelCarrier(t *testing.T) {
	for _, tt := range []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "selector in array", document: `{"pipeline":[{"credentials_identity":"attacker@example-gcp-project.iam.gserviceaccount.com"}]}`, wantErr: "pipeline[0].credentials_identity"},
		{name: "nested selector object", document: `{"pipeline":{"google_credentials":{}}}`, wantErr: "pipeline.google_credentials"},
		{name: "credential variant", document: `{"pipeline":{"credential-fallback":"ambient"}}`, wantErr: "pipeline.credential-fallback"},
		{name: "top level carrier must be object", document: `{"google_credentials":false}`, wantErr: "must be an object"},
		{name: "ordinary nested configuration", document: `{"pipeline":{"steps":[{"name":"safe","count":2}]}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfigCredentialShape([]byte(tt.document))
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

func TestImpersonationURLAndProjectStayCanonical(t *testing.T) {
	identity := "task-graph-operator@example-gcp-project.iam.gserviceaccount.com"
	canonical := "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/" + identity + ":generateAccessToken"
	for _, tt := range []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "canonical", url: canonical},
		{name: "http", url: strings.Replace(canonical, "https://", "http://", 1), wantErr: true},
		{name: "query", url: canonical + "?redirect=1", wantErr: true},
		{name: "fragment", url: canonical + "#fragment", wantErr: true},
		{name: "encoded identity", url: strings.Replace(canonical, "@", "%40", 1), wantErr: true},
		{name: "wrong method", url: strings.Replace(canonical, "generateAccessToken", "signBlob", 1), wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateImpersonationURL(tt.url, identity)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateImpersonationURL() error = %v, want error=%t", err, tt.wantErr)
			}
		})
	}
	for _, tt := range []struct {
		identity string
		want     string
	}{
		{identity: identity, want: "example-gcp-project"},
		{identity: "no-at-sign.iam.gserviceaccount.com", want: ""},
		{identity: "name@", want: ""},
	} {
		if got := serviceAccountProject(tt.identity); got != tt.want {
			t.Fatalf("serviceAccountProject(%q) = %q, want %q", tt.identity, got, tt.want)
		}
	}
}

func TestCredentialJSONDecoderConsumesNumbersWithoutFloatCoercion(t *testing.T) {
	decoder := json.NewDecoder(strings.NewReader(`{"number":9007199254740993}`))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		t.Fatalf("consumeJSONValue() unexpected error: %v", err)
	}
}
