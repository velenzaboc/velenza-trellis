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

const defaultFlashHTTPTimeout = 60 * time.Second

// FlashConfig is the per-call parameter bundle the generateContent
// endpoint needs. Every field REQUIRED.
type FlashConfig struct {
	Project         string
	Location        string
	Model           string
	MaxOutputTokens int
	Temperature     float64
}

// FlashClient invokes the generateContent endpoint to produce a single
// text response. Holds the transport; per-call configuration is passed
// in.
type FlashClient struct {
	auth   *Auth
	httpC  *http.Client
	target string
	cfg    FlashConfig
}

// NewFlashClient constructs a client.
func NewFlashClient(auth *Auth, cfg FlashConfig) (*FlashClient, error) {
	if auth == nil {
		return nil, fmt.Errorf("vertexai.flash: auth is required")
	}
	target, err := GenerateContentURL(cfg.Location, cfg.Project, cfg.Model)
	if err != nil {
		return nil, err
	}
	return &FlashClient{
		auth:   auth,
		httpC:  newBearerHTTPClient(defaultFlashHTTPTimeout),
		target: target,
		cfg:    cfg,
	}, nil
}

// Generate sends a single user-role prompt and returns the first
// candidate's first text part. Temperature + max output tokens applied
// from the client's config.
func (c *FlashClient) Generate(ctx context.Context, prompt string) (string, error) {
	body, err := json.Marshal(map[string]interface{}{
		"contents": []map[string]interface{}{{
			"role": "user",
			"parts": []map[string]interface{}{{"text": prompt}},
		}},
		"generationConfig": map[string]interface{}{
			"temperature":     c.cfg.Temperature,
			"maxOutputTokens": c.cfg.MaxOutputTokens,
		},
	})
	if err != nil {
		return "", fmt.Errorf("vertexai.flash marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.target, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := c.auth.AttachBearer(req); err != nil {
		return "", err
	}
	resp, err := c.httpC.Do(req)
	if err != nil {
		return "", fmt.Errorf("vertexai.flash request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("vertexai.flash status %d: %s", resp.StatusCode, string(respBody))
	}
	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("vertexai.flash decode: %w", err)
	}
	if len(parsed.Candidates) == 0 || len(parsed.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("vertexai.flash: response had no candidates")
	}
	return parsed.Candidates[0].Content.Parts[0].Text, nil
}
