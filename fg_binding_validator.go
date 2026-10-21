package main

import (
	"context"
	"path"
	"regexp"
	"strings"
	"time"
)

// FGBindingEnvelope is the read-side shape of testdata/fg_envelope.schema.json.
// No field is omitted when marshaled, including commit_sha.
type FGBindingEnvelope struct {
	ProjectID        string `json:"project_id"`
	TaskID           string `json:"task_id"`
	Repo             string `json:"repo"`
	Host             string `json:"host"`
	AccountOrHarness string `json:"account_or_harness"`
	Thread           string `json:"thread"`
	Session          string `json:"session"`
	AbsoluteWorktree string `json:"absolute_worktree"`
	Branch           string `json:"branch"`
	CommitSHA        string `json:"commit_sha"`
	PRorSpecRef      string `json:"pr_or_spec_ref"`
	CheckpointRef    string `json:"checkpoint_ref"`
	MeasuredAt       string `json:"measured_at"`
}

// FGBindingEvidence supplies coordinates absent from the native TaskLinks
// projection. They must be measured by the caller, never inferred from a
// branch, free-text note, URL, artifact_ref, or the row's creation timestamp.
type FGBindingEvidence struct {
	Host             string
	AccountOrHarness string
	PRorSpecRef      string
	CheckpointRef    string
	MeasuredAt       string
}

type FGBindingRule string

const (
	FGBindingRuleArtifactType         FGBindingRule = "artifact_type"
	FGBindingRulePlaceholderRef       FGBindingRule = "placeholder_ref"
	FGBindingRuleUnresolved           FGBindingRule = "unresolved"
	FGBindingRuleMissingField         FGBindingRule = "missing_field"
	FGBindingRuleMissingCommit        FGBindingRule = "missing_commit"
	FGBindingRuleInvalidCommitSHA     FGBindingRule = "invalid_commit_sha"
	FGBindingRuleUnknownHost          FGBindingRule = "unknown_host"
	FGBindingRuleInvalidRepo          FGBindingRule = "invalid_repo"
	FGBindingRuleRelativeWorktree     FGBindingRule = "relative_worktree"
	FGBindingRuleWorkspaceRoot        FGBindingRule = "workspace_root_worktree"
	FGBindingRuleSharedMirror         FGBindingRule = "shared_mirror_worktree"
	FGBindingRuleInvalidCheckpointRef FGBindingRule = "invalid_checkpoint_ref"
	FGBindingRuleInvalidMeasuredAt    FGBindingRule = "invalid_measured_at"
)

// FGBindingRejection identifies the first invalid input coordinate. Field uses
// the source name (session_id/worktree_path), even when the envelope renames it.
type FGBindingRejection struct {
	Rule  FGBindingRule `json:"rule"`
	Field string        `json:"field"`
}

func (e *FGBindingRejection) Error() string {
	return "fleet binding: " + string(e.Rule) + " (" + e.Field + ")"
}

var (
	fgBindingRepoPattern       = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	fgBindingCommitPattern     = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)
	fgBindingAbsolutePattern   = regexp.MustCompile(`^([A-Za-z]:[\\/]|/)`)
	fgBindingCheckpointPattern = regexp.MustCompile(`^.+#sha256=[0-9a-f]{64}$`)
	fgBindingMeasuredPattern   = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`)
)

// ValidateFGBinding maps one already-read active TaskLinks row and its explicit
// evidence to the landed envelope schema. The supplied registry must be an
// already-loaded, stable artifact-kind registry. This function performs no I/O,
// reads no clock or environment, and mutates neither input nor registry.
//
// Rules are checked in a fixed order and rejection returns a zero envelope.
// A populated repo requires a commit even though the broader evidence schema
// permits an empty commit for non-code evidence. Only binding, worktree, and
// checkpoint rows are accepted after the existing registry canonicalization.
func ValidateFGBinding(ctx context.Context, reg ArtifactKindRegistry, row WorklinkRow, evidence FGBindingEvidence) (FGBindingEnvelope, error) {
	reject := func(rule FGBindingRule, field string) (FGBindingEnvelope, error) {
		return FGBindingEnvelope{}, &FGBindingRejection{Rule: rule, Field: field}
	}
	// A slice keeps rejection precedence stable when several fields are invalid.
	fields := []struct{ name, value string }{
		{"project_id", row.ProjectID}, {"task_id", row.TaskID}, {"repo", row.Repo},
		{"host", evidence.Host}, {"account_or_harness", evidence.AccountOrHarness},
		{"thread", row.Thread}, {"session_id", row.SessionID}, {"worktree_path", row.WorktreePath},
		{"branch", row.Branch}, {"commit_sha", row.CommitSHA}, {"pr_or_spec_ref", evidence.PRorSpecRef},
		{"checkpoint_ref", evidence.CheckpointRef}, {"measured_at", evidence.MeasuredAt},
		{"artifact_ref", row.ArtifactRef}, {"artifact_type", row.ArtifactType},
	}
	for _, f := range fields {
		if strings.Contains(f.value, "UNRESOLVED") {
			return reject(FGBindingRuleUnresolved, f.name)
		}
	}
	if reg == nil {
		return reject(FGBindingRuleArtifactType, "artifact_type")
	}
	kind, found := reg.Canonicalize(ctx, row.ArtifactType)
	if !found || (kind != "binding" && kind != "worktree" && kind != "checkpoint") {
		return reject(FGBindingRuleArtifactType, "artifact_type")
	}
	if strings.HasPrefix(row.ArtifactRef, "current-task:") {
		return reject(FGBindingRulePlaceholderRef, "artifact_ref")
	}
	if row.Repo != "" && row.CommitSHA == "" {
		return reject(FGBindingRuleMissingCommit, "commit_sha")
	}
	for _, f := range fields {
		if f.value == "" {
			return reject(FGBindingRuleMissingField, f.name)
		}
	}
	if evidence.Host != "workstation-c" && evidence.Host != "workstation-a" && evidence.Host != "workstation-b" {
		return reject(FGBindingRuleUnknownHost, "host")
	}
	if !fgBindingRepoPattern.MatchString(row.Repo) {
		return reject(FGBindingRuleInvalidRepo, "repo")
	}
	if !fgBindingCommitPattern.MatchString(row.CommitSHA) {
		return reject(FGBindingRuleInvalidCommitSHA, "commit_sha")
	}
	if !fgBindingAbsolutePattern.MatchString(row.WorktreePath) {
		return reject(FGBindingRuleRelativeWorktree, "worktree_path")
	}
	// Normalize only for comparison, never change the returned evidence. Use
	// slash paths so Windows coordinates behave identically on every host.
	worktree := path.Clean(strings.ReplaceAll(row.WorktreePath, `\`, "/"))
	if len(worktree) >= 2 && worktree[1] == ':' {
		worktree = strings.ToLower(worktree)
	}
	if worktree == "d:/workspace" || worktree == "/workspace/arbor" || worktree == "/Users/developer/workspace" || worktree == "/home/developer/workspace" {
		return reject(FGBindingRuleWorkspaceRoot, "worktree_path")
	}
	for _, segment := range strings.Split(worktree, "/") {
		if segment == "repos" {
			return reject(FGBindingRuleSharedMirror, "worktree_path")
		}
	}
	if !fgBindingCheckpointPattern.MatchString(evidence.CheckpointRef) {
		return reject(FGBindingRuleInvalidCheckpointRef, "checkpoint_ref")
	}
	if _, err := time.Parse(time.RFC3339Nano, evidence.MeasuredAt); err != nil || !fgBindingMeasuredPattern.MatchString(evidence.MeasuredAt) {
		return reject(FGBindingRuleInvalidMeasuredAt, "measured_at")
	}
	return FGBindingEnvelope{
		ProjectID: row.ProjectID, TaskID: row.TaskID, Repo: row.Repo,
		Host: evidence.Host, AccountOrHarness: evidence.AccountOrHarness,
		Thread: row.Thread, Session: row.SessionID, AbsoluteWorktree: row.WorktreePath,
		Branch: row.Branch, CommitSHA: row.CommitSHA, PRorSpecRef: evidence.PRorSpecRef,
		CheckpointRef: evidence.CheckpointRef, MeasuredAt: evidence.MeasuredAt,
	}, nil
}
