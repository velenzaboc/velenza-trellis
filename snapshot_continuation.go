package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// snapshotContinuationVersion is part of the public continuation contract.
// Bumping it deliberately invalidates all outstanding continuations instead
// of attempting to reinterpret a cursor under different pagination semantics.
const snapshotContinuationVersion = "p2"

// snapshotContinuation is the signed, opaque resume coordinate for every
// capped collection in one project snapshot. It binds the collection cursors
// to the original Spanner read timestamp, project and page contract so a
// second page cannot silently become a different snapshot.
type snapshotContinuation struct {
	Version       string   `json:"v"`
	ProjectID     string   `json:"p"`
	Scope         string   `json:"s"`
	RootTaskID    string   `json:"root,omitempty"`
	ReadTimestamp string   `json:"r"`
	Revision      string   `json:"rev"`
	Limit         int      `json:"l"`
	ExpiresAt     string   `json:"e"`
	TasksAfter    []string `json:"t,omitempty"`
	DepsAfter     []string `json:"d,omitempty"`
	LinksAfter    []string `json:"w,omitempty"`
	// ScopeMissingTaskIDs accumulates missing prerequisites observed on prior
	// dependency pages. The fixed-timestamp membership discovery repeats on
	// every request, while dependency existence is evaluated page by page, so
	// this authenticated state is required to keep scope completeness monotonic.
	ScopeMissingTaskIDs []string `json:"sm,omitempty"`
	TasksTotal          int      `json:"tt"`
	DepsTotal           int      `json:"dt"`
	LinksTotal          int      `json:"wt"`
	TasksCollected      int      `json:"tc"`
	DepsCollected       int      `json:"dc"`
	LinksCollected      int      `json:"wc"`
}

// encodeSnapshotContinuation serializes a canonical payload and authenticates
// it with HMAC-SHA-256. The payload is opaque to clients; it is not encrypted,
// so it deliberately contains only retrieval coordinates and never secrets.
func encodeSnapshotContinuation(key []byte, continuation snapshotContinuation) (string, error) {
	if len(key) < 32 {
		return "", fmt.Errorf("snapshot continuation signing key unavailable")
	}
	payload, err := canonicalJSON(continuation)
	if err != nil {
		return "", fmt.Errorf("canonical continuation: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// decodeSnapshotContinuation validates authenticity before decoding or using
// any field. Every mismatch is rejected: accepting a cursor under a different
// project, page size or contract would permit gaps or cross-snapshot mixing.
func decodeSnapshotContinuation(encoded string, key []byte, projectID, scope, rootTaskID string, limit int, now time.Time) (snapshotContinuation, error) {
	if len(key) < 32 {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation signing key unavailable")
	}
	parts := strings.Split(encoded, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return snapshotContinuation{}, fmt.Errorf("invalid snapshot continuation encoding")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return snapshotContinuation{}, fmt.Errorf("decode snapshot continuation payload: %w", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return snapshotContinuation{}, fmt.Errorf("decode snapshot continuation signature: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation authentication failed")
	}
	var continuation snapshotContinuation
	if err := json.Unmarshal(payload, &continuation); err != nil {
		return snapshotContinuation{}, fmt.Errorf("decode snapshot continuation: %w", err)
	}
	if continuation.Version != snapshotContinuationVersion {
		return snapshotContinuation{}, fmt.Errorf("unsupported snapshot continuation version %q", continuation.Version)
	}
	if continuation.ProjectID != projectID {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation project mismatch")
	}
	if continuation.Scope != scope {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation scope mismatch")
	}
	if continuation.RootTaskID != rootTaskID {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation root task mismatch")
	}
	if continuation.Limit != limit {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation page limit mismatch")
	}
	if continuation.TasksTotal < 0 || continuation.DepsTotal < 0 || continuation.LinksTotal < 0 || continuation.TasksCollected < 0 || continuation.DepsCollected < 0 || continuation.LinksCollected < 0 {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation totals invalid")
	}
	readAt, err := time.Parse(time.RFC3339Nano, continuation.ReadTimestamp)
	if err != nil || readAt.IsZero() {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation read timestamp invalid")
	}
	if continuation.Revision != snapshotRevision(readAt) {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation revision mismatch")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, continuation.ExpiresAt)
	if err != nil || !now.Before(expiresAt) {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation expired")
	}
	if err := validateContinuationTuple(continuation.TasksAfter, 3, "tasks", 2); err != nil {
		return snapshotContinuation{}, err
	}
	if err := validateContinuationTuple(continuation.DepsAfter, 3, "dependencies", 0, 1, 2); err != nil {
		return snapshotContinuation{}, err
	}
	if err := validateContinuationTuple(continuation.LinksAfter, 2, "worklinks", 0, 1); err != nil {
		return snapshotContinuation{}, err
	}
	if len(continuation.LinksAfter) == 2 && parseRFC3339(continuation.LinksAfter[0]).IsZero() {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation worklinks created_at timestamp invalid")
	}
	if err := validateContinuationProgress(continuation.TasksCollected, continuation.TasksTotal, continuation.TasksAfter, "tasks"); err != nil {
		return snapshotContinuation{}, err
	}
	if err := validateContinuationProgress(continuation.DepsCollected, continuation.DepsTotal, continuation.DepsAfter, "dependencies"); err != nil {
		return snapshotContinuation{}, err
	}
	if err := validateContinuationProgress(continuation.LinksCollected, continuation.LinksTotal, continuation.LinksAfter, "worklinks"); err != nil {
		return snapshotContinuation{}, err
	}
	if continuation.Scope == snapshotScopeProject && len(continuation.ScopeMissingTaskIDs) > 0 {
		return snapshotContinuation{}, fmt.Errorf("snapshot continuation project scope missing task IDs invalid")
	}
	seenScopeMissing := make(map[string]struct{}, len(continuation.ScopeMissingTaskIDs))
	for _, id := range continuation.ScopeMissingTaskIDs {
		if id == "" {
			return snapshotContinuation{}, fmt.Errorf("snapshot continuation scope missing task ID invalid")
		}
		if _, duplicate := seenScopeMissing[id]; duplicate {
			return snapshotContinuation{}, fmt.Errorf("snapshot continuation scope missing task IDs duplicate")
		}
		seenScopeMissing[id] = struct{}{}
	}
	return continuation, nil
}

func validateContinuationProgress(collected, total int, cursor []string, collection string) error {
	if collected < total && len(cursor) == 0 {
		return fmt.Errorf("snapshot continuation %s cursor missing for incomplete collection", collection)
	}
	return nil
}

func validateContinuationTuple(tuple []string, want int, collection string, requiredNonEmpty ...int) error {
	if len(tuple) != 0 && len(tuple) != want {
		return fmt.Errorf("snapshot continuation %s ordering tuple invalid", collection)
	}
	for _, index := range requiredNonEmpty {
		if len(tuple) != 0 && (index < 0 || index >= len(tuple) || tuple[index] == "") {
			return fmt.Errorf("snapshot continuation %s ordering tuple invalid", collection)
		}
	}
	return nil
}
