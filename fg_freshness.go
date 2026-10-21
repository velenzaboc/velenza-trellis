package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// fg_freshness.go is MVP-FG-EVIDENCE-FRESHNESS-STAMP-20260911: a read-only fact
// report over a complete scoped snapshot. It writes nothing and judges nothing.

const (
	FGFreshnessLocalCheckoutRef = "HEAD"
	FGFreshnessOriginMainRef    = "origin/main"
)

type FGFreshnessFlag string

const (
	FGFreshnessStaleEvidence FGFreshnessFlag = "stale_evidence"
	FGFreshnessNoEvidence    FGFreshnessFlag = "no_evidence"
)

type FGFreshnessReachClass string

const (
	FGFreshnessReachable    FGFreshnessReachClass = "reachable"
	FGFreshnessNotReachable FGFreshnessReachClass = "not_reachable"
	FGFreshnessUnverifiable FGFreshnessReachClass = "unverifiable"
)

type FGFreshnessRefResult struct {
	Ref    string                `json:"ref"`
	Class  FGFreshnessReachClass `json:"class"`
	Reason string                `json:"reason,omitempty"`
}

type FGFreshnessCommit struct {
	ArtifactID   string                 `json:"artifact_id"`
	Repo         string                 `json:"repo,omitempty"`
	Branch       string                 `json:"branch,omitempty"`
	CommitSHA    string                 `json:"commit_sha,omitempty"`
	Reachability []FGFreshnessRefResult `json:"reachability"`
}

type FGFreshnessTask struct {
	ProjectID        string              `json:"project_id"`
	TaskID           string              `json:"task_id"`
	MeasuredAt       string              `json:"measured_at"`
	TaskUpdatedAt    string              `json:"task_updated_at"`
	NewestEvidenceAt string              `json:"newest_evidence_at,omitempty"`
	AgeSeconds       *float64            `json:"age_seconds,omitempty"`
	EvidenceCount    int                 `json:"evidence_count"`
	Flags            []FGFreshnessFlag   `json:"flags"`
	Commits          []FGFreshnessCommit `json:"commits"`
}

type FGFreshnessResult struct {
	MeasuredAt    string            `json:"measured_at"`
	WindowSeconds float64           `json:"window_seconds"`
	Tasks         []FGFreshnessTask `json:"tasks"`
}

type FGFreshnessReader interface {
	Reachability(ctx context.Context, repo, sha, ref string) FGFreshnessRefResult
}

// FGFreshnessOptions.MeasuredAt must be the snapshot's read_timestamp; Window
// and Refs have no defaults.
type FGFreshnessOptions struct {
	MeasuredAt string
	Window     time.Duration
	Refs       []string
	Reader     FGFreshnessReader
}

type fgFreshnessKey struct{ project, id string }

type fgFreshnessWork struct {
	task    FGFreshnessTask
	count   int
	newest  time.Time
	commits []WorklinkRow
}

var fgFreshnessRefPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

func fgFreshnessRefOK(ref string) bool {
	return fgFreshnessRefPattern.MatchString(ref) && !strings.Contains(ref, "..")
}

// Live producers emit Spanner-cast timestamps, so this reuses parseRFC3339's
// three layouts; an offset is mandatory in all of them.
func fgFreshnessParseTime(label, s string) (time.Time, error) {
	t := parseRFC3339(s)
	if t.IsZero() {
		return time.Time{}, fmt.Errorf("fleet freshness: %s %q is not a timestamp with an explicit offset", label, s)
	}
	return t.UTC(), nil
}

func fgFreshnessAge(label string, measured, at time.Time) (time.Duration, error) {
	age := measured.Sub(at)
	if age < 0 {
		return 0, fmt.Errorf("fleet freshness: %s %s is after measured_at %s", label, at.Format(time.RFC3339Nano), measured.Format(time.RFC3339Nano))
	}
	if age == math.MaxInt64 {
		return 0, fmt.Errorf("fleet freshness: %s %s is too old to express as an age", label, at.Format(time.RFC3339Nano))
	}
	return age, nil
}

func fgFreshnessSeconds(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e9 }

func RunFGFreshnessCheck(ctx context.Context, artifactReg ArtifactKindRegistry, snapshot ProjectSnapshotResult, opts FGFreshnessOptions) (FGFreshnessResult, error) {
	if err := ctx.Err(); err != nil {
		return FGFreshnessResult{}, err
	}
	if artifactReg == nil {
		return FGFreshnessResult{}, fmt.Errorf("%w: RunFGFreshnessCheck requires a non-nil ArtifactKindRegistry", ErrUnknownArtifactKind)
	}
	if !snapshot.Scope.Complete || !snapshot.Complete ||
		(snapshot.Scope.Mode != snapshotScopeExecutionSubtree && snapshot.Scope.Mode != snapshotScopeAcceptanceDependencyClosure) ||
		snapshot.Collections.Tasks.AuthorizedTotal > len(snapshot.Tasks) ||
		snapshot.Collections.Worklinks.AuthorizedTotal > len(snapshot.Worklinks) {
		return FGFreshnessResult{}, fmt.Errorf("fleet freshness: complete scoped snapshot required")
	}
	measured, err := fgFreshnessParseTime("measured_at", opts.MeasuredAt)
	if err != nil {
		return FGFreshnessResult{}, err
	}
	readAt, err := fgFreshnessParseTime("snapshot read_timestamp", snapshot.ReadTimestamp)
	if err != nil {
		return FGFreshnessResult{}, err
	}
	if !measured.Equal(readAt) {
		return FGFreshnessResult{}, fmt.Errorf("fleet freshness: measured_at %s is not the snapshot read_timestamp %s", opts.MeasuredAt, snapshot.ReadTimestamp)
	}
	if opts.Window <= 0 {
		return FGFreshnessResult{}, fmt.Errorf("fleet freshness: window must be a positive explicit duration")
	}
	if len(opts.Refs) == 0 {
		return FGFreshnessResult{}, fmt.Errorf("fleet freshness: at least one explicit ref is required")
	}
	for _, ref := range opts.Refs {
		if !fgFreshnessRefOK(ref) {
			return FGFreshnessResult{}, fmt.Errorf("fleet freshness: ref %q is not a plain ref name", ref)
		}
	}

	work := make([]*fgFreshnessWork, 0, len(snapshot.Tasks))
	byTask := make(map[fgFreshnessKey]*fgFreshnessWork, len(snapshot.Tasks))
	for _, row := range snapshot.Tasks {
		key := fgFreshnessKey{row.ProjectID, row.TaskID}
		if _, dup := byTask[key]; dup {
			return FGFreshnessResult{}, fmt.Errorf("fleet freshness: task %q appears more than once in project %q", row.TaskID, row.ProjectID)
		}
		updated, err := fgFreshnessParseTime(fmt.Sprintf("task %q updated_at", row.TaskID), row.UpdatedAt)
		if err != nil {
			return FGFreshnessResult{}, err
		}
		if _, err := fgFreshnessAge(fmt.Sprintf("task %q updated_at", row.TaskID), measured, updated); err != nil {
			return FGFreshnessResult{}, err
		}
		w := &fgFreshnessWork{task: FGFreshnessTask{
			ProjectID:     row.ProjectID,
			TaskID:        row.TaskID,
			MeasuredAt:    measured.Format(time.RFC3339Nano),
			TaskUpdatedAt: updated.Format(time.RFC3339Nano),
			Flags:         []FGFreshnessFlag{},
			Commits:       []FGFreshnessCommit{},
		}}
		byTask[key] = w
		work = append(work, w)
	}

	seenLink := make(map[fgFreshnessKey]struct{}, len(snapshot.Worklinks))
	for _, link := range snapshot.Worklinks {
		w, inScope := byTask[fgFreshnessKey{link.ProjectID, link.TaskID}]
		if !inScope {
			continue
		}
		linkKey := fgFreshnessKey{link.ProjectID, link.ArtifactID}
		if _, dup := seenLink[linkKey]; dup {
			return FGFreshnessResult{}, fmt.Errorf("fleet freshness: worklink %q appears more than once in project %q", link.ArtifactID, link.ProjectID)
		}
		seenLink[linkKey] = struct{}{}
		label := fmt.Sprintf("worklink %q created_at", link.ArtifactID)
		created, err := fgFreshnessParseTime(label, link.CreatedAt)
		if err != nil {
			return FGFreshnessResult{}, err
		}
		if _, err := fgFreshnessAge(label, measured, created); err != nil {
			return FGFreshnessResult{}, err
		}
		kind, found := artifactReg.Canonicalize(ctx, link.ArtifactType)
		if !found {
			return FGFreshnessResult{}, fmt.Errorf("%w: worklink %q has artifact_type literal %q not recognized by any artifact_kinds member's absorbs list", ErrUnknownArtifactKind, link.ArtifactID, link.ArtifactType)
		}
		w.count++
		if created.After(w.newest) {
			w.newest = created
		}
		if kind == "commit" || link.CommitSHA != "" {
			w.commits = append(w.commits, link)
		}
	}

	sort.Slice(work, func(i, j int) bool {
		if work[i].task.ProjectID != work[j].task.ProjectID {
			return work[i].task.ProjectID < work[j].task.ProjectID
		}
		return work[i].task.TaskID < work[j].task.TaskID
	})

	out := FGFreshnessResult{
		MeasuredAt:    measured.Format(time.RFC3339Nano),
		WindowSeconds: fgFreshnessSeconds(opts.Window),
		Tasks:         make([]FGFreshnessTask, 0, len(work)),
	}
	for _, w := range work {
		task := w.task
		if w.count == 0 {
			task.Flags = append(task.Flags, FGFreshnessNoEvidence)
		} else {
			age := measured.Sub(w.newest)
			seconds := fgFreshnessSeconds(age)
			task.NewestEvidenceAt = w.newest.Format(time.RFC3339Nano)
			task.AgeSeconds = &seconds
			if age > opts.Window {
				task.Flags = append(task.Flags, FGFreshnessStaleEvidence)
			}
		}
		task.EvidenceCount = w.count

		sort.Slice(w.commits, func(i, j int) bool { return w.commits[i].ArtifactID < w.commits[j].ArtifactID })
		for _, link := range w.commits {
			commit := FGFreshnessCommit{
				ArtifactID:   link.ArtifactID,
				Repo:         link.Repo,
				Branch:       link.Branch,
				CommitSHA:    link.CommitSHA,
				Reachability: make([]FGFreshnessRefResult, 0, len(opts.Refs)),
			}
			for _, ref := range opts.Refs {
				switch {
				case link.CommitSHA == "":
					commit.Reachability = append(commit.Reachability, FGFreshnessRefResult{Ref: ref, Class: FGFreshnessUnverifiable, Reason: "missing_commit_sha"})
				case !fgBindingCommitPattern.MatchString(link.CommitSHA):
					commit.Reachability = append(commit.Reachability, FGFreshnessRefResult{Ref: ref, Class: FGFreshnessUnverifiable, Reason: "invalid_commit_sha"})
				case opts.Reader == nil:
					commit.Reachability = append(commit.Reachability, FGFreshnessRefResult{Ref: ref, Class: FGFreshnessUnverifiable, Reason: "reachability_reader_not_configured"})
				default:
					result := opts.Reader.Reachability(ctx, link.Repo, link.CommitSHA, ref)
					if err := ctx.Err(); err != nil {
						return FGFreshnessResult{}, err
					}
					commit.Reachability = append(commit.Reachability, result)
				}
			}
			task.Commits = append(task.Commits, commit)
		}
		out.Tasks = append(out.Tasks, task)
	}
	return out, nil
}

type fgFreshnessGitRun func(ctx context.Context, dir string, args ...string) (exitCode int, stdout string, err error)

// FGFreshnessGit classifies reachability with git in a caller-named checkout.
// Dirs maps the worklink repo name (used only as a lookup key, never as a
// path or command text) to an absolute checkout directory. It never fetches.
type FGFreshnessGit struct {
	Dirs map[string]string
	run  fgFreshnessGitRun
}

func (g FGFreshnessGit) Reachability(ctx context.Context, repo, sha, ref string) FGFreshnessRefResult {
	unverifiable := func(reason string) FGFreshnessRefResult {
		return FGFreshnessRefResult{Ref: ref, Class: FGFreshnessUnverifiable, Reason: reason}
	}
	if !fgBindingCommitPattern.MatchString(sha) || !fgFreshnessRefOK(ref) {
		return unverifiable("invalid_argument")
	}
	dir := g.Dirs[repo]
	if repo == "" || dir == "" {
		return unverifiable("checkout_not_configured")
	}
	if !filepath.IsAbs(dir) {
		return unverifiable("checkout_path_not_absolute")
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return unverifiable("checkout_missing")
	}
	run := g.run
	if run == nil {
		run = fgFreshnessExecGit
	}
	code, _, err := run(ctx, dir, "merge-base", "--is-ancestor", sha, ref)
	switch {
	case err != nil:
		return unverifiable("git_unavailable")
	case code == 0:
		return FGFreshnessRefResult{Ref: ref, Class: FGFreshnessReachable}
	case code != 1:
		return unverifiable(fmt.Sprintf("git_exit_%d", code))
	}
	// Exit 1 means "not an ancestor" only when history is complete; a shallow
	// checkout reports a real ancestor as exit 1.
	code, out, err := run(ctx, dir, "rev-parse", "--is-shallow-repository")
	if err != nil || code != 0 {
		return unverifiable("shallow_state_unknown")
	}
	switch strings.TrimSpace(out) {
	case "false":
		return FGFreshnessRefResult{Ref: ref, Class: FGFreshnessNotReachable}
	case "true":
		return unverifiable("shallow_checkout")
	}
	return unverifiable("shallow_state_unknown")
}

func fgFreshnessExecGit(ctx context.Context, dir string, args ...string) (int, string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = fgFreshnessGitEnv(os.Environ())
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out), nil
	}
	if err != nil {
		return -1, "", err
	}
	return 0, string(out), nil
}

// An ambient GIT_DIR or similar would redirect git away from the named
// checkout; GIT_NO_LAZY_FETCH keeps a partial clone from touching the network.
func fgFreshnessGitEnv(environ []string) []string {
	out := make([]string, 0, len(environ)+1)
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY",
			"GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_COMMON_DIR", "GIT_NAMESPACE", "GIT_PREFIX":
			continue
		}
		out = append(out, kv)
	}
	return append(out, "GIT_NO_LAZY_FETCH=1")
}
