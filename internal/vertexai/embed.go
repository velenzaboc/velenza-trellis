package vertexai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Task-type constants for the embedding endpoint.
const (
	TaskTypeRetrievalDocument = "RETRIEVAL_DOCUMENT"
	TaskTypeRetrievalQuery    = "RETRIEVAL_QUERY"
	defaultEmbedHTTPTimeout   = 30 * time.Second
)

// EmbedConfig is the per-call parameter bundle the embedding endpoint
// needs. Every field is REQUIRED — no defaults; no fallbacks. Per
// Vision Principle 7, callers source these from the live config.
type EmbedConfig struct {
	Project      string
	Location     string
	Model        string
	Dimensions   int
	MaxTextBytes int
}

// EmbedClient invokes the predict endpoint to produce a fixed-length
// embedding vector. Holds the transport (auth + http.Client + URL); the
// per-call configuration is passed in so one EmbedClient serves an
// entire CLI invocation against one Vertex AI tenancy.
type EmbedClient struct {
	auth   *Auth
	httpC  *http.Client
	target string
	cfg    EmbedConfig
}

// NewEmbedClient constructs a client. The returned client truncates
// input text to cfg.MaxTextBytes per request.
func NewEmbedClient(auth *Auth, cfg EmbedConfig) (*EmbedClient, error) {
	if auth == nil {
		return nil, fmt.Errorf("vertexai.embed: auth is required")
	}
	target, err := PredictURL(cfg.Location, cfg.Project, cfg.Model)
	if err != nil {
		return nil, err
	}
	return &EmbedClient{
		auth:   auth,
		httpC:  newBearerHTTPClient(defaultEmbedHTTPTimeout),
		target: target,
		cfg:    cfg,
	}, nil
}

// Embed produces an embedding vector under the named task_type. Returns
// an error if the response shape differs from the expected dimensions.
func (c *EmbedClient) Embed(ctx context.Context, text, taskType string) ([]float32, error) {
	if len(text) > c.cfg.MaxTextBytes {
		text = text[:c.cfg.MaxTextBytes]
	}
	body, err := json.Marshal(map[string]interface{}{
		"instances": []map[string]interface{}{{
			"content":   text,
			"task_type": taskType,
		}},
		"parameters": map[string]interface{}{
			"outputDimensionality": c.cfg.Dimensions,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("vertexai.embed marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := c.auth.AttachBearer(req); err != nil {
		return nil, err
	}
	resp, err := c.httpC.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vertexai.embed request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("vertexai.embed status %d: %s", resp.StatusCode, string(respBody))
	}
	var parsed struct {
		Predictions []struct {
			Embeddings struct {
				Values []float32 `json:"values"`
			} `json:"embeddings"`
		} `json:"predictions"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("vertexai.embed decode: %w", err)
	}
	if len(parsed.Predictions) == 0 {
		return nil, fmt.Errorf("vertexai.embed: response had no predictions")
	}
	vec := parsed.Predictions[0].Embeddings.Values
	if len(vec) != c.cfg.Dimensions {
		return nil, fmt.Errorf("vertexai.embed dimensions mismatch: got %d, want %d", len(vec), c.cfg.Dimensions)
	}
	return vec, nil
}
