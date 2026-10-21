package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// fg_freshness_test.go covers MVP-FG-EVIDENCE-FRESHNESS-STAMP-20260911. The
// classification is pure; reachability is exercised through a recording fake
// runner (exit-code mapping, argument vector) and through real git in
// t.TempDir repositories (no network, no fetch).

const fgFreshnessNow = "2026-09-30T12:00:00Z"

func fgFreshnessRegistry(t *testing.T) ArtifactKindRegistry {
	t.Helper()
	b, err := os.ReadFile("task-graph.config.json")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Kinds []ArtifactKindDescriptor `json:"artifact_kinds"`
	}
	if err := json.Unmarshal(b, &config); err != nil {
		t.Fatal(err)
	}
	reg, err := newArtifactKindRegistry(config.Kinds)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func fgFreshnessJSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func fgFreshnessSnapshot(readAt string, tasks []TaskRow, links []WorklinkRow) ProjectSnapshotResult {
	return ProjectSnapshotResult{
		ProjectID: "arbor", ReadTimestamp: readAt, Complete: true,
		Scope: SnapshotScope{Mode: snapshotScopeExecutionSubtree, RootTaskID: "ROOT", Complete: true},
		Collections: SnapshotCollections{
			Tasks:     CollectionState{Returned: len(tasks), Collected: len(tasks), AuthorizedTotal: len(tasks), Complete: true},
			Worklinks: CollectionState{Returned: len(links), Collected: len(links), AuthorizedTotal: len(links), Complete: true},
		},
		Tasks: tasks, Worklinks: links,
	}
}

func fgFreshnessTask(id, updated string) TaskRow {
	return TaskRow{ProjectID: "arbor", TaskID: id, Status: "in_progress", UpdatedAt: updated}
}

func fgFreshnessLink(id, task, kind, created string) WorklinkRow {
	return WorklinkRow{ProjectID: "arbor", ArtifactID: id, TaskID: task, ArtifactType: kind, CreatedAt: created}
}

func fgFreshnessCommitLink(id, task, created, repo, sha string) WorklinkRow {
	row := fgFreshnessLink(id, task, "commit", created)
	row.Repo, row.Branch, row.CommitSHA = repo, "task/example", sha
	return row
}

func fgFreshnessOpts(window time.Duration, reader FGFreshnessReader, refs ...string) FGFreshnessOptions {
	if len(refs) == 0 {
		refs = []string{FGFreshnessOriginMainRef}
	}
	return FGFreshnessOptions{MeasuredAt: fgFreshnessNow, Window: window, Refs: refs, Reader: reader}
}

// fgFreshnessRun requires success and proves the check left its input unchanged.
func fgFreshnessRun(t *testing.T, reg ArtifactKindRegistry, snapshot ProjectSnapshotResult, opts FGFreshnessOptions) (FGFreshnessResult, string) {
	t.Helper()
	before := fgFreshnessJSON(t, snapshot)
	got, err := RunFGFreshnessCheck(context.Background(), reg, snapshot, opts)
	if err != nil {
		t.Fatalf("unexpected refusal: %v", err)
	}
	if fgFreshnessJSON(t, snapshot) != before {
		t.Fatal("check mutated its input snapshot")
	}
	return got, fgFreshnessJSON(t, got)
}

func fgFreshnessWant(windowSeconds string, tasks ...string) string {
	return `{"measured_at":"2026-09-30T12:00:00Z","window_seconds":` + windowSeconds + `,"tasks":[` + strings.Join(tasks, ",") + `]}`
}

func fgFreshnessWantTask(id, updated, newest, age string, count int, flags string) string {
	evidence := ""
	if newest != "" {
		evidence = `"newest_evidence_at":"` + newest + `","age_seconds":` + age + `,`
	}
	return fmt.Sprintf(`{"project_id":"arbor","task_id":"%s","measured_at":"2026-09-30T12:00:00Z","task_updated_at":"%s",%s"evidence_count":%d,"flags":%s,"commits":[]}`,
		id, updated, evidence, count, flags)
}

type fgFreshnessFakeReader struct {
	calls []string
	class FGFreshnessReachClass
}

func (f *fgFreshnessFakeReader) Reachability(_ context.Context, repo, sha, ref string) FGFreshnessRefResult {
	f.calls = append(f.calls, repo+"|"+sha+"|"+ref)
	return FGFreshnessRefResult{Ref: ref, Class: f.class}
}

type fgFreshnessGitCall struct {
	dir  string
	args []string
}

// fgFreshnessFakeRun scripts git results in call order and records each call.
type fgFreshnessFakeRun struct {
	calls   []fgFreshnessGitCall
	results []fgFreshnessGitResult
}

type fgFreshnessGitResult struct {
	code int
	out  string
	err  error
}

func (f *fgFreshnessFakeRun) run(_ context.Context, dir string, args ...string) (int, string, error) {
	f.calls = append(f.calls, fgFreshnessGitCall{dir: dir, args: slices.Clone(args)})
	if len(f.results) == 0 {
		return 0, "", errors.New("unscripted git call")
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r.code, r.out, r.err
}

const (
	fgFreshnessSHA1 = "1111111111111111111111111111111111111111"
	fgFreshnessSHA2 = "2222222222222222222222222222222222222222"
)

func fgFreshnessHermeticGit(t *testing.T) {
	t.Helper()
	// Git for Windows does not accept the NUL device as a config path.
	configPath := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(configPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", configPath)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
}

func fgFreshnessGitOut(t *testing.T, dir string, args ...string) string {
	t.Helper()
	full := append([]string{"-C", dir, "-c", "user.name=fg", "-c", "user.email=fg@example.invalid", "-c", "commit.gpgsign=false"}, args...)
	out, err := exec.Command("git", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

type fgFreshnessRepo struct{ dir, a, b, c, f string }

// main: A-B-C (HEAD=C); origin/main=B; feature: A-F.
func fgFreshnessBuildRepo(t *testing.T) fgFreshnessRepo {
	t.Helper()
	dir := t.TempDir()
	git := func(args ...string) string { return fgFreshnessGitOut(t, dir, args...) }
	git("init", "-q", "-b", "main")
	commit := func(msg string) string {
		git("commit", "-q", "--allow-empty", "-m", msg)
		return git("rev-parse", "HEAD")
	}
	r := fgFreshnessRepo{dir: dir}
	r.a = commit("A")
	r.b = commit("B")
	git("branch", "feature", r.a)
	git("checkout", "-q", "feature")
	r.f = commit("F")
	git("checkout", "-q", "main")
	git("update-ref", "refs/remotes/origin/main", r.b)
	r.c = commit("C")
	return r
}

func TestFGFreshness(t *testing.T) {
	reg := fgFreshnessRegistry(t)
	hour := time.Hour

	t.Run("positive_recent_evidence_inside_window_passes_with_exact_timestamps", func(t *testing.T) {
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
			[]WorklinkRow{
				fgFreshnessLink("L-1", "TASK-A", "dispatch", "2026-09-30T10:00:00Z"),
				fgFreshnessLink("L-2", "TASK-A", "validation", "2026-09-30T11:30:00Z"),
			})
		_, got := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, nil))
		want := fgFreshnessWant("3600", fgFreshnessWantTask("TASK-A", "2026-09-30T11:00:00Z", "2026-09-30T11:30:00Z", "1800", 2, `[]`))
		if got != want {
			t.Fatalf("result = %s\nwant     %s", got, want)
		}
	})

	t.Run("old_evidence_reports_stale_evidence_with_age_seconds_even_when_task_was_touched_recently", func(t *testing.T) {
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:59:00Z")},
			[]WorklinkRow{fgFreshnessLink("L-1", "TASK-A", "validation", "2026-09-27T12:00:00Z")})
		_, got := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(24*hour, nil))
		want := fgFreshnessWant("86400", fgFreshnessWantTask("TASK-A", "2026-09-30T11:59:00Z", "2026-09-27T12:00:00Z", "259200", 1, `["stale_evidence"]`))
		if got != want {
			t.Fatalf("result = %s\nwant     %s", got, want)
		}
	})

	t.Run("window_boundary_is_older_than_so_equal_age_is_fresh", func(t *testing.T) {
		for _, c := range []struct {
			name, newest, age string
			flags             string
		}{
			{"evidence_at_measured_at", "2026-09-30T12:00:00Z", "0", `[]`},
			{"age_equals_window", "2026-09-30T11:00:00Z", "3600", `[]`},
			{"one_millisecond_inside_window", "2026-09-30T11:00:00.001Z", "3599.999", `[]`},
			{"one_millisecond_past_window", "2026-09-30T10:59:59.999Z", "3600.001", `["stale_evidence"]`},
		} {
			t.Run(c.name, func(t *testing.T) {
				snap := fgFreshnessSnapshot(fgFreshnessNow,
					[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
					[]WorklinkRow{fgFreshnessLink("L-1", "TASK-A", "validation", c.newest)})
				_, got := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, nil))
				want := fgFreshnessWant("3600", fgFreshnessWantTask("TASK-A", "2026-09-30T11:00:00Z", strings.TrimSuffix(strings.TrimSuffix(c.newest, "Z"), "")+"Z", c.age, 1, c.flags))
				if got != want {
					t.Fatalf("result = %s\nwant     %s", got, want)
				}
			})
		}
	})

	t.Run("window_is_an_explicit_argument_with_no_baked_default", func(t *testing.T) {
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
			[]WorklinkRow{fgFreshnessLink("L-1", "TASK-A", "validation", "2026-09-30T10:00:00Z")})
		short, _ := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, nil))
		long, _ := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(3*hour, nil))
		if !reflect.DeepEqual(short.Tasks[0].Flags, []FGFreshnessFlag{FGFreshnessStaleEvidence}) {
			t.Fatalf("2h-old evidence with a 1h window flags = %v, want stale_evidence", short.Tasks[0].Flags)
		}
		if len(long.Tasks[0].Flags) != 0 {
			t.Fatalf("2h-old evidence with a 3h window flags = %v, want none", long.Tasks[0].Flags)
		}
		for _, window := range []time.Duration{0, -hour} {
			if got, err := RunFGFreshnessCheck(context.Background(), reg, snap, fgFreshnessOpts(window, nil)); err == nil || !reflect.DeepEqual(got, FGFreshnessResult{}) {
				t.Fatalf("window %v = (%+v, %v), want refusal and no result", window, got, err)
			}
		}
	})

	t.Run("no_evidence_is_flagged_no_evidence_and_never_stale_or_fresh", func(t *testing.T) {
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")}, nil)
		_, got := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, nil))
		want := fgFreshnessWant("3600", fgFreshnessWantTask("TASK-A", "2026-09-30T11:00:00Z", "", "", 0, `["no_evidence"]`))
		if got != want {
			t.Fatalf("result = %s\nwant     %s", got, want)
		}
	})

	t.Run("newest_evidence_is_the_maximum_over_this_tasks_active_worklinks_only", func(t *testing.T) {
		other := fgFreshnessLink("L-other-project", "TASK-A", "validation", "2026-09-30T11:59:00Z")
		other.ProjectID = "other"
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z"), fgFreshnessTask("TASK-B", "2026-09-30T11:00:00Z")},
			[]WorklinkRow{
				fgFreshnessLink("L-3", "TASK-A", "finding", "2026-09-30T09:00:00Z"),
				fgFreshnessLink("L-2", "TASK-A", "commit", "2026-09-30T11:10:00Z"),
				fgFreshnessLink("L-1", "TASK-A", "dispatch", "2026-09-30T08:00:00Z"),
				fgFreshnessLink("L-B", "TASK-B", "validation", "2026-09-30T11:45:00Z"),
				fgFreshnessLink("L-orphan", "TASK-NOT-IN-SCOPE", "validation", "garbage"),
				other,
			})
		got, _ := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, nil))
		a, b := got.Tasks[0], got.Tasks[1]
		if a.TaskID != "TASK-A" || a.NewestEvidenceAt != "2026-09-30T11:10:00Z" || a.EvidenceCount != 3 || *a.AgeSeconds != 3000 {
			t.Fatalf("TASK-A = %+v, want newest 11:10, 3 links, age 3000", a)
		}
		if b.TaskID != "TASK-B" || b.NewestEvidenceAt != "2026-09-30T11:45:00Z" || b.EvidenceCount != 1 || *b.AgeSeconds != 900 {
			t.Fatalf("TASK-B = %+v, want newest 11:45, 1 link, age 900", b)
		}
	})

	t.Run("equivalent_timezone_offsets_and_producer_formats_replay_identically", func(t *testing.T) {
		build := func(measured, readAt, updated, created string) string {
			snap := fgFreshnessSnapshot(readAt,
				[]TaskRow{fgFreshnessTask("TASK-A", updated)},
				[]WorklinkRow{fgFreshnessLink("L-1", "TASK-A", "validation", created)})
			opts := fgFreshnessOpts(hour, nil)
			opts.MeasuredAt = measured
			_, got := fgFreshnessRun(t, reg, snap, opts)
			return got
		}
		want := fgFreshnessWant("3600", fgFreshnessWantTask("TASK-A", "2026-09-30T11:00:00Z", "2026-09-30T11:30:00Z", "1800", 1, `[]`))
		for name, got := range map[string]string{
			"utc":                   build("2026-09-30T12:00:00Z", "2026-09-30T12:00:00Z", "2026-09-30T11:00:00Z", "2026-09-30T11:30:00Z"),
			"plus_two_and_spanner":  build("2026-09-30T14:00:00+02:00", "2026-09-30 12:00:00+00", "2026-09-30 13:00:00+02", "2026-09-30T11:30:00.000000Z"),
			"minus_half_hour_frac":  build("2026-09-30T05:30:00-06:30", "2026-09-30T12:00:00.0Z", "2026-09-30T11:00:00.0Z", "2026-09-30 11:30:00.000000+00:00"),
			"spanner_short_offsets": build("2026-09-30 12:00:00+00", "2026-09-30 14:00:00+02", "2026-09-30 11:00:00+00", "2026-09-30 13:30:00+02"),
		} {
			if got != want {
				t.Errorf("%s: result = %s\nwant %s", name, got, want)
			}
		}
	})

	t.Run("fixed_measured_at_replays_byte_identically_regardless_of_input_order", func(t *testing.T) {
		reader := &fgFreshnessFakeReader{class: FGFreshnessReachable}
		tasks := []TaskRow{
			fgFreshnessTask("TASK-C", "2026-09-30T11:00:00Z"),
			fgFreshnessTask("TASK-A", "2026-09-30T10:00:00Z"),
			fgFreshnessTask("TASK-B", "2026-09-30T09:00:00Z"),
		}
		links := []WorklinkRow{
			fgFreshnessCommitLink("L-3", "TASK-A", "2026-09-30T08:00:00Z", "task-graph", fgFreshnessSHA1),
			fgFreshnessCommitLink("L-1", "TASK-A", "2026-09-30T07:00:00Z", "task-graph", fgFreshnessSHA2),
			fgFreshnessLink("L-2", "TASK-B", "validation", "2026-09-30T06:00:00Z"),
			fgFreshnessLink("L-4", "TASK-C", "review", "2026-09-30T05:00:00Z"),
		}
		_, first := fgFreshnessRun(t, reg, fgFreshnessSnapshot(fgFreshnessNow, tasks, links), fgFreshnessOpts(hour, reader))
		rng := rand.New(rand.NewSource(7))
		for i := 0; i < 20; i++ {
			shuffledTasks, shuffledLinks := slices.Clone(tasks), slices.Clone(links)
			rng.Shuffle(len(shuffledTasks), func(a, b int) { shuffledTasks[a], shuffledTasks[b] = shuffledTasks[b], shuffledTasks[a] })
			rng.Shuffle(len(shuffledLinks), func(a, b int) { shuffledLinks[a], shuffledLinks[b] = shuffledLinks[b], shuffledLinks[a] })
			_, again := fgFreshnessRun(t, reg, fgFreshnessSnapshot(fgFreshnessNow, shuffledTasks, shuffledLinks), fgFreshnessOpts(hour, reader))
			if again != first {
				t.Fatalf("replay %d differs:\n%s\n%s", i, again, first)
			}
		}
		var got FGFreshnessResult
		if err := json.Unmarshal([]byte(first), &got); err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, task := range got.Tasks {
			ids = append(ids, task.TaskID)
		}
		if !slices.Equal(ids, []string{"TASK-A", "TASK-B", "TASK-C"}) {
			t.Fatalf("task order = %v, want sorted", ids)
		}
		var commitIDs []string
		for _, c := range got.Tasks[0].Commits {
			commitIDs = append(commitIDs, c.ArtifactID)
		}
		if !slices.Equal(commitIDs, []string{"L-1", "L-3"}) {
			t.Fatalf("commit order = %v, want sorted by artifact id", commitIDs)
		}
	})

	t.Run("acceptance_dependency_closure_scope_is_accepted", func(t *testing.T) {
		snap := fgFreshnessSnapshot(fgFreshnessNow, []TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")}, nil)
		snap.Scope.Mode = snapshotScopeAcceptanceDependencyClosure
		fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, nil))
	})

	t.Run("fail_closed_on_malformed_times", func(t *testing.T) {
		bad := []string{
			"", "garbage", "1760000000",
			"2026-09-30T12:00:00", "2026-09-30 12:00:00",
			"2026-09-30T12:00:00+0200", "2026-09-30 12:00:00+0200",
			"2026-13-30T12:00:00Z", "2026-09-31T12:00:00Z", "2026-09-30T25:00:00Z",
			" 2026-09-30T12:00:00Z", "2026-09-30T12:00:00Z ", "2026-09-30t12:00:00z",
			"0001-01-01T00:00:00Z",
		}
		valid := func() (ProjectSnapshotResult, FGFreshnessOptions) {
			return fgFreshnessSnapshot(fgFreshnessNow,
				[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
				[]WorklinkRow{fgFreshnessLink("L-1", "TASK-A", "validation", "2026-09-30T11:30:00Z")}), fgFreshnessOpts(hour, nil)
		}
		refuse := func(t *testing.T, snap ProjectSnapshotResult, opts FGFreshnessOptions) {
			t.Helper()
			got, err := RunFGFreshnessCheck(context.Background(), reg, snap, opts)
			if err == nil || !reflect.DeepEqual(got, FGFreshnessResult{}) {
				t.Fatalf("got (%+v, %v), want refusal and no usable result", got, err)
			}
		}
		for _, value := range bad {
			t.Run(fmt.Sprintf("measured_at=%q", value), func(t *testing.T) {
				snap, opts := valid()
				snap.ReadTimestamp, opts.MeasuredAt = value, value
				refuse(t, snap, opts)
			})
			t.Run(fmt.Sprintf("task_updated_at=%q", value), func(t *testing.T) {
				snap, opts := valid()
				snap.Tasks[0].UpdatedAt = value
				refuse(t, snap, opts)
			})
			t.Run(fmt.Sprintf("worklink_created_at=%q", value), func(t *testing.T) {
				snap, opts := valid()
				snap.Worklinks[0].CreatedAt = value
				refuse(t, snap, opts)
			})
		}
		t.Run("explicit_measured_at_must_be_the_snapshot_read_timestamp", func(t *testing.T) {
			snap, opts := valid()
			opts.MeasuredAt = "2026-09-30T12:00:01Z"
			refuse(t, snap, opts)
		})
		t.Run("evidence_after_measured_at_is_contradictory", func(t *testing.T) {
			snap, opts := valid()
			snap.Worklinks[0].CreatedAt = "2026-09-30T12:00:00.000000001Z"
			refuse(t, snap, opts)
		})
		t.Run("task_updated_after_measured_at_is_contradictory", func(t *testing.T) {
			snap, opts := valid()
			snap.Tasks[0].UpdatedAt = "2026-09-30T12:00:00.000000001Z"
			refuse(t, snap, opts)
		})
		t.Run("duplicate_task_key_is_ambiguous", func(t *testing.T) {
			snap, opts := valid()
			snap.Tasks = append(snap.Tasks, snap.Tasks[0])
			snap.Collections.Tasks.AuthorizedTotal = 2
			refuse(t, snap, opts)
		})
		t.Run("duplicate_worklink_key_is_ambiguous", func(t *testing.T) {
			snap, opts := valid()
			snap.Worklinks = append(snap.Worklinks, snap.Worklinks[0])
			snap.Collections.Worklinks.AuthorizedTotal = 2
			refuse(t, snap, opts)
		})
		t.Run("age_overflow_is_refused_not_saturated", func(t *testing.T) {
			snap, opts := valid()
			snap.Worklinks[0].CreatedAt = "1600-01-01T00:00:00Z"
			refuse(t, snap, opts)
		})
		t.Run("incomplete_or_unscoped_snapshot", func(t *testing.T) {
			for name, mutate := range map[string]func(*ProjectSnapshotResult){
				"scope_incomplete":         func(s *ProjectSnapshotResult) { s.Scope.Complete = false },
				"snapshot_incomplete":      func(s *ProjectSnapshotResult) { s.Complete = false },
				"project_wide_scope":       func(s *ProjectSnapshotResult) { s.Scope.Mode = "project" },
				"empty_scope_mode":         func(s *ProjectSnapshotResult) { s.Scope.Mode = "" },
				"tasks_capped":             func(s *ProjectSnapshotResult) { s.Collections.Tasks.AuthorizedTotal = len(s.Tasks) + 1 },
				"worklinks_capped":         func(s *ProjectSnapshotResult) { s.Collections.Worklinks.AuthorizedTotal = len(s.Worklinks) + 1 },
				"unknown_scope_mode_value": func(s *ProjectSnapshotResult) { s.Scope.Mode = "subtree" },
			} {
				t.Run(name, func(t *testing.T) {
					snap, opts := valid()
					mutate(&snap)
					refuse(t, snap, opts)
				})
			}
		})
		t.Run("nil_registry_and_unknown_artifact_type", func(t *testing.T) {
			snap, opts := valid()
			if _, err := RunFGFreshnessCheck(context.Background(), nil, snap, opts); !errors.Is(err, ErrUnknownArtifactKind) {
				t.Fatalf("nil registry err = %v, want ErrUnknownArtifactKind", err)
			}
			snap.Worklinks[0].ArtifactType = "zz_not_a_registered_kind"
			if _, err := RunFGFreshnessCheck(context.Background(), reg, snap, opts); !errors.Is(err, ErrUnknownArtifactKind) {
				t.Fatalf("unknown type err = %v, want ErrUnknownArtifactKind", err)
			}
		})
		t.Run("no_refs_or_cancelled_context", func(t *testing.T) {
			snap, opts := valid()
			opts.Refs = nil
			refuse(t, snap, opts)
			snap, opts = valid()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := RunFGFreshnessCheck(ctx, reg, snap, opts); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled ctx err = %v, want context.Canceled", err)
			}
		})
	})

	t.Run("malformed_commit_sha_is_unverifiable_evidence_and_never_refuses_the_snapshot", func(t *testing.T) {
		// Live task-plan shape: short hex such as 8093c449a0 and "deadbeef" in the typed commit_sha field.
		reader := &fgFreshnessFakeReader{class: FGFreshnessReachable}
		short := fgFreshnessCommitLink("L-short", "TASK-A", "2026-09-30T11:20:00Z", "task-graph", "8093c449a0")
		dead := fgFreshnessCommitLink("L-dead", "TASK-A", "2026-09-30T11:40:00Z", "task-graph", "deadbeef")
		dead.ArtifactType = "pr"
		good := fgFreshnessCommitLink("L-good", "TASK-B", "2026-09-30T11:30:00Z", "task-graph", fgFreshnessSHA1)
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{
				fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z"),
				fgFreshnessTask("TASK-B", "2026-09-30T10:00:00Z"),
				fgFreshnessTask("TASK-C", "2026-09-30T09:00:00Z"),
			},
			[]WorklinkRow{short, dead, good})
		_, got := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, reader, FGFreshnessLocalCheckoutRef, FGFreshnessOriginMainRef))
		invalid := `"class":"unverifiable","reason":"invalid_commit_sha"`
		reachable := `"class":"reachable"`
		commitJSON := func(id, sha, result string) string {
			return fmt.Sprintf(`{"artifact_id":"%s","repo":"task-graph","branch":"task/example","commit_sha":"%s","reachability":[{"ref":"HEAD",%s},{"ref":"origin/main",%s}]}`, id, sha, result, result)
		}
		withCommits := func(task string, commits ...string) string {
			return strings.TrimSuffix(task, `[]}`) + `[` + strings.Join(commits, ",") + `]}`
		}
		want := fgFreshnessWant("3600",
			withCommits(fgFreshnessWantTask("TASK-A", "2026-09-30T11:00:00Z", "2026-09-30T11:40:00Z", "1200", 2, `[]`),
				commitJSON("L-dead", "deadbeef", invalid), commitJSON("L-short", "8093c449a0", invalid)),
			withCommits(fgFreshnessWantTask("TASK-B", "2026-09-30T10:00:00Z", "2026-09-30T11:30:00Z", "1800", 1, `[]`),
				commitJSON("L-good", fgFreshnessSHA1, reachable)),
			fgFreshnessWantTask("TASK-C", "2026-09-30T09:00:00Z", "", "", 0, `["no_evidence"]`))
		if got != want {
			t.Fatalf("result = %s\nwant     %s", got, want)
		}
		wantCalls := []string{"task-graph|" + fgFreshnessSHA1 + "|HEAD", "task-graph|" + fgFreshnessSHA1 + "|origin/main"}
		if !reflect.DeepEqual(reader.calls, wantCalls) {
			t.Fatalf("reader calls = %v, want only the well-formed sha: %v", reader.calls, wantCalls)
		}
	})

	t.Run("hostile_or_malformed_commit_sha_values_never_reach_the_reader_and_never_refuse", func(t *testing.T) {
		badSHAs := []string{
			"abc", strings.Repeat("a", 39), strings.Repeat("a", 41), strings.Repeat("A", 40),
			"HEAD", "refs/heads/main", "--upload-pack=touch PWNED", "$(touch PWNED)", strings.Repeat("a", 39) + ";",
			" " + strings.Repeat("a", 40), strings.Repeat("a", 40) + "\n",
		}
		for _, sha := range badSHAs {
			t.Run(fmt.Sprintf("sha=%q", sha), func(t *testing.T) {
				reader := &fgFreshnessFakeReader{class: FGFreshnessReachable}
				snap := fgFreshnessSnapshot(fgFreshnessNow,
					[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
					[]WorklinkRow{fgFreshnessCommitLink("L-1", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", sha)})
				for _, r := range []FGFreshnessReader{reader, nil} {
					got, _ := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, r))
					if len(got.Tasks) != 1 || got.Tasks[0].EvidenceCount != 1 || got.Tasks[0].NewestEvidenceAt != "2026-09-30T11:00:00Z" || len(got.Tasks[0].Commits) != 1 {
						t.Fatalf("result = %+v, want one task with one evidence link and one commit row", got)
					}
					commit := got.Tasks[0].Commits[0]
					wantReach := []FGFreshnessRefResult{{Ref: FGFreshnessOriginMainRef, Class: FGFreshnessUnverifiable, Reason: "invalid_commit_sha"}}
					if commit.CommitSHA != sha || !reflect.DeepEqual(commit.Reachability, wantReach) {
						t.Fatalf("commit = %+v, want sha kept verbatim and %+v", commit, wantReach)
					}
				}
				if len(reader.calls) != 0 {
					t.Fatalf("reader called with malformed sha: %v", reader.calls)
				}
			})
		}
	})

	t.Run("fail_closed_on_malformed_ref_before_any_git_call", func(t *testing.T) {
		for _, ref := range []string{"", "-x", "--upload-pack=x", "a b", "$(id)", "x;y", "a\nb", "@{u}", "HEAD~1", "a:b", "a*", "a\\b", "\x00", "a..b", "a...b"} {
			t.Run(fmt.Sprintf("ref=%q", ref), func(t *testing.T) {
				reader := &fgFreshnessFakeReader{class: FGFreshnessReachable}
				snap := fgFreshnessSnapshot(fgFreshnessNow,
					[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
					[]WorklinkRow{fgFreshnessCommitLink("L-1", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", fgFreshnessSHA1)})
				got, err := RunFGFreshnessCheck(context.Background(), reg, snap, fgFreshnessOpts(hour, reader, FGFreshnessOriginMainRef, ref))
				if err == nil || !reflect.DeepEqual(got, FGFreshnessResult{}) {
					t.Fatalf("got (%+v, %v), want refusal", got, err)
				}
				if len(reader.calls) != 0 {
					t.Fatalf("reader called with malformed ref: %v", reader.calls)
				}
			})
		}
	})

	t.Run("commit_evidence_selection_uses_typed_fields_never_free_text", func(t *testing.T) {
		reader := &fgFreshnessFakeReader{class: FGFreshnessReachable}
		pr := fgFreshnessLink("L-pr", "TASK-A", "pr", "2026-09-30T11:00:00Z")
		pr.Repo, pr.CommitSHA = "task-graph", fgFreshnessSHA2
		freeText := fgFreshnessLink("L-note", "TASK-A", "dispatch", "2026-09-30T11:00:00Z")
		freeText.ArtifactRef, freeText.Note, freeText.URL = fgFreshnessSHA1, fgFreshnessSHA1, "https://example.invalid/commit/"+fgFreshnessSHA1
		noSHA := fgFreshnessLink("L-commit-no-sha", "TASK-A", "commit", "2026-09-30T11:00:00Z")
		noSHA.Repo, noSHA.ArtifactRef = "task-graph", fgFreshnessSHA1
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
			[]WorklinkRow{pr, freeText, noSHA})
		got, _ := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, reader, FGFreshnessLocalCheckoutRef, FGFreshnessOriginMainRef))
		commits := got.Tasks[0].Commits
		if len(commits) != 2 || commits[0].ArtifactID != "L-commit-no-sha" || commits[1].ArtifactID != "L-pr" {
			t.Fatalf("commits = %+v, want exactly the commit-kind row without sha and the pr row with a typed sha", commits)
		}
		for _, r := range commits[0].Reachability {
			if r.Class != FGFreshnessUnverifiable || r.Reason != "missing_commit_sha" {
				t.Fatalf("commit row with no typed sha = %+v, want unverifiable/missing_commit_sha", r)
			}
		}
		if want := []string{"task-graph|" + fgFreshnessSHA2 + "|HEAD", "task-graph|" + fgFreshnessSHA2 + "|origin/main"}; !slices.Equal(reader.calls, want) {
			t.Fatalf("reader calls = %v, want %v", reader.calls, want)
		}
	})

	t.Run("nil_reader_is_unverifiable_not_reachable", func(t *testing.T) {
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
			[]WorklinkRow{fgFreshnessCommitLink("L-1", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", fgFreshnessSHA1)})
		got, _ := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, nil))
		r := got.Tasks[0].Commits[0].Reachability[0]
		if r.Class != FGFreshnessUnverifiable || r.Reason != "reachability_reader_not_configured" {
			t.Fatalf("nil reader result = %+v", r)
		}
	})

	t.Run("git_adapter_runs_one_argument_vector_in_the_controlled_directory", func(t *testing.T) {
		dir := t.TempDir()
		fake := &fgFreshnessFakeRun{results: []fgFreshnessGitResult{{code: 0}}}
		got := FGFreshnessGit{Dirs: map[string]string{"task-graph": dir}, run: fake.run}.
			Reachability(context.Background(), "task-graph", fgFreshnessSHA1, "origin/main")
		want := FGFreshnessRefResult{Ref: "origin/main", Class: FGFreshnessReachable}
		if got != want {
			t.Fatalf("result = %+v, want %+v", got, want)
		}
		wantCalls := []fgFreshnessGitCall{{dir: dir, args: []string{"merge-base", "--is-ancestor", fgFreshnessSHA1, "origin/main"}}}
		if !reflect.DeepEqual(fake.calls, wantCalls) {
			t.Fatalf("git calls = %+v, want %+v", fake.calls, wantCalls)
		}
	})

	t.Run("git_adapter_refuses_unsafe_arguments_without_running_git", func(t *testing.T) {
		for _, c := range []struct{ name, sha, ref string }{
			{"short_sha", "abc", "HEAD"},
			{"option_as_sha", "--upload-pack=x", "HEAD"},
			{"option_as_ref", fgFreshnessSHA1, "--upload-pack=x"},
			{"revision_syntax_ref", fgFreshnessSHA1, "HEAD~1"},
			{"range_ref", fgFreshnessSHA1, "a..b"},
			{"empty_ref", fgFreshnessSHA1, ""},
		} {
			t.Run(c.name, func(t *testing.T) {
				fake := &fgFreshnessFakeRun{results: []fgFreshnessGitResult{{code: 0}}}
				got := FGFreshnessGit{Dirs: map[string]string{"task-graph": t.TempDir()}, run: fake.run}.
					Reachability(context.Background(), "task-graph", c.sha, c.ref)
				if got.Class != FGFreshnessUnverifiable || got.Reason != "invalid_argument" {
					t.Fatalf("result = %+v, want unverifiable/invalid_argument", got)
				}
				if len(fake.calls) != 0 {
					t.Fatalf("git ran with unsafe arguments: %+v", fake.calls)
				}
			})
		}
	})

	t.Run("git_environment_strips_repository_redirection_and_disables_lazy_fetch", func(t *testing.T) {
		got := fgFreshnessGitEnv([]string{
			"PATH=/usr/bin", "GIT_DIR=/x", "GIT_WORK_TREE=/x", "GIT_INDEX_FILE=/x", "GIT_OBJECT_DIRECTORY=/x",
			"GIT_ALTERNATE_OBJECT_DIRECTORIES=/x", "GIT_COMMON_DIR=/x", "GIT_NAMESPACE=x", "GIT_PREFIX=x",
			"GIT_CEILING_DIRECTORIES=/tmp", "GIT_NO_LAZY_FETCH=0",
		})
		want := []string{"PATH=/usr/bin", "GIT_CEILING_DIRECTORIES=/tmp", "GIT_NO_LAZY_FETCH=0", "GIT_NO_LAZY_FETCH=1"}
		if !slices.Equal(got, want) {
			t.Fatalf("env = %v, want %v", got, want)
		}
	})

	t.Run("git_adapter_exit_code_mapping", func(t *testing.T) {
		spawn := errors.New("exec: git not found")
		for _, c := range []struct {
			name      string
			results   []fgFreshnessGitResult
			wantClass FGFreshnessReachClass
			wantWhy   string
			wantCalls int
		}{
			{"exit_0_reachable", []fgFreshnessGitResult{{code: 0}}, FGFreshnessReachable, "", 1},
			{"exit_1_complete_history_not_reachable", []fgFreshnessGitResult{{code: 1}, {code: 0, out: "false\n"}}, FGFreshnessNotReachable, "", 2},
			{"exit_1_shallow_is_unverifiable", []fgFreshnessGitResult{{code: 1}, {code: 0, out: "true\n"}}, FGFreshnessUnverifiable, "shallow_checkout", 2},
			{"exit_1_shallow_probe_fails", []fgFreshnessGitResult{{code: 1}, {code: 128}}, FGFreshnessUnverifiable, "shallow_state_unknown", 2},
			{"exit_1_shallow_probe_spawn_error", []fgFreshnessGitResult{{code: 1}, {err: spawn}}, FGFreshnessUnverifiable, "shallow_state_unknown", 2},
			{"exit_1_shallow_probe_unrecognised_output", []fgFreshnessGitResult{{code: 1}, {code: 0, out: "--is-shallow-repository\n"}}, FGFreshnessUnverifiable, "shallow_state_unknown", 2},
			{"exit_128_unknown_object_or_ref", []fgFreshnessGitResult{{code: 128}}, FGFreshnessUnverifiable, "git_exit_128", 1},
			{"exit_129_usage", []fgFreshnessGitResult{{code: 129}}, FGFreshnessUnverifiable, "git_exit_129", 1},
			{"signal_exit_minus_one", []fgFreshnessGitResult{{code: -1}}, FGFreshnessUnverifiable, "git_exit_-1", 1},
			{"spawn_error", []fgFreshnessGitResult{{err: spawn}}, FGFreshnessUnverifiable, "git_unavailable", 1},
		} {
			t.Run(c.name, func(t *testing.T) {
				dir := t.TempDir()
				fake := &fgFreshnessFakeRun{results: c.results}
				got := FGFreshnessGit{Dirs: map[string]string{"task-graph": dir}, run: fake.run}.
					Reachability(context.Background(), "task-graph", fgFreshnessSHA1, "HEAD")
				if got.Class != c.wantClass || got.Reason != c.wantWhy || got.Ref != "HEAD" {
					t.Fatalf("result = %+v, want class %s reason %q", got, c.wantClass, c.wantWhy)
				}
				if len(fake.calls) != c.wantCalls {
					t.Fatalf("git calls = %d, want %d: %+v", len(fake.calls), c.wantCalls, fake.calls)
				}
				if c.wantCalls == 2 {
					if want := []string{"rev-parse", "--is-shallow-repository"}; !slices.Equal(fake.calls[1].args, want) || fake.calls[1].dir != dir {
						t.Fatalf("shallow probe = %+v, want %v in %s", fake.calls[1], want, dir)
					}
				}
			})
		}
	})

	t.Run("missing_checkout_reports_unverifiable_never_absent", func(t *testing.T) {
		fgFreshnessHermeticGit(t)
		emptyDir := t.TempDir()
		t.Setenv("GIT_CEILING_DIRECTORIES", filepath.Dir(emptyDir))
		for _, c := range []struct {
			name, repo string
			dirs       map[string]string
			wantWhy    string
		}{
			{"repo_not_configured", "task-graph", map[string]string{}, "checkout_not_configured"},
			{"nil_directory_map", "task-graph", nil, "checkout_not_configured"},
			{"empty_directory_value", "task-graph", map[string]string{"task-graph": ""}, "checkout_not_configured"},
			{"empty_repo_name_on_the_row", "", map[string]string{"task-graph": emptyDir}, "checkout_not_configured"},
			{"hostile_repo_name_is_only_a_map_key", "task-graph; touch PWNED $(id)", map[string]string{"task-graph": emptyDir}, "checkout_not_configured"},
			{"relative_directory", "task-graph", map[string]string{"task-graph": "relative/checkout"}, "checkout_path_not_absolute"},
			{"directory_does_not_exist", "task-graph", map[string]string{"task-graph": filepath.Join(emptyDir, "missing")}, "checkout_missing"},
			{"path_is_a_file_not_a_directory", "task-graph", map[string]string{"task-graph": func() string {
				p := filepath.Join(emptyDir, "file")
				if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
				return p
			}()}, "checkout_missing"},
			{"directory_is_not_a_git_checkout", "task-graph", map[string]string{"task-graph": emptyDir}, "git_exit_128"},
		} {
			t.Run(c.name, func(t *testing.T) {
				snap := fgFreshnessSnapshot(fgFreshnessNow,
					[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
					[]WorklinkRow{fgFreshnessCommitLink("L-1", "TASK-A", "2026-09-30T11:00:00Z", c.repo, fgFreshnessSHA1)})
				got, raw := fgFreshnessRun(t, reg, snap, fgFreshnessOpts(hour, FGFreshnessGit{Dirs: c.dirs}, FGFreshnessLocalCheckoutRef, FGFreshnessOriginMainRef))
				if strings.Contains(raw, "absent") {
					t.Fatalf("result uses the word absent: %s", raw)
				}
				if len(got.Tasks[0].Commits[0].Reachability) != 2 {
					t.Fatalf("reachability = %+v, want one result per ref", got.Tasks[0].Commits[0].Reachability)
				}
				for _, r := range got.Tasks[0].Commits[0].Reachability {
					if r.Class != FGFreshnessUnverifiable || r.Reason != c.wantWhy {
						t.Fatalf("result = %+v, want unverifiable/%s", r, c.wantWhy)
					}
				}
			})
		}
		if _, err := os.Stat("PWNED"); err == nil {
			t.Fatal("task data was interpreted as shell")
		}
	})

	t.Run("real_git_classifies_reachability_from_local_checkout_origin_main_and_named_branch", func(t *testing.T) {
		fgFreshnessHermeticGit(t)
		repo := fgFreshnessBuildRepo(t)
		unknown := strings.Repeat("7", 40)
		links := []WorklinkRow{
			fgFreshnessCommitLink("L-A", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", repo.a),
			fgFreshnessCommitLink("L-B", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", repo.b),
			fgFreshnessCommitLink("L-C", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", repo.c),
			fgFreshnessCommitLink("L-F", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", repo.f),
			fgFreshnessCommitLink("L-X", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", unknown),
		}
		snap := fgFreshnessSnapshot(fgFreshnessNow, []TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")}, links)
		opts := fgFreshnessOpts(hour, FGFreshnessGit{Dirs: map[string]string{"task-graph": repo.dir}},
			FGFreshnessLocalCheckoutRef, FGFreshnessOriginMainRef, "feature")
		got, raw := fgFreshnessRun(t, reg, snap, opts)
		if strings.Contains(raw, "absent") {
			t.Fatalf("result uses the word absent: %s", raw)
		}
		type cell struct{ class, reason string }
		const reach, not, unv = "reachable", "not_reachable", "unverifiable"
		want := map[string][3]cell{
			"L-A": {{reach, ""}, {reach, ""}, {reach, ""}},
			"L-B": {{reach, ""}, {reach, ""}, {not, ""}},
			"L-C": {{reach, ""}, {not, ""}, {not, ""}},
			"L-F": {{not, ""}, {not, ""}, {reach, ""}},
			"L-X": {{unv, "git_exit_128"}, {unv, "git_exit_128"}, {unv, "git_exit_128"}},
		}
		commits := got.Tasks[0].Commits
		if len(commits) != len(want) {
			t.Fatalf("commits = %d, want %d", len(commits), len(want))
		}
		for _, c := range commits {
			refs := []string{"HEAD", "origin/main", "feature"}
			if len(c.Reachability) != 3 {
				t.Fatalf("%s reachability = %+v", c.ArtifactID, c.Reachability)
			}
			for i, r := range c.Reachability {
				w := want[c.ArtifactID][i]
				if r.Ref != refs[i] || string(r.Class) != w.class || r.Reason != w.reason {
					t.Errorf("%s vs %s = %+v, want %+v", c.ArtifactID, refs[i], r, w)
				}
			}
			if c.Repo != "task-graph" || c.Branch != "task/example" {
				t.Errorf("%s echo = repo %q branch %q", c.ArtifactID, c.Repo, c.Branch)
			}
		}
	})

	t.Run("real_git_ignores_ambient_git_dir_and_work_tree", func(t *testing.T) {
		fgFreshnessHermeticGit(t)
		repo := fgFreshnessBuildRepo(t)
		decoy := filepath.Join(t.TempDir(), "decoy")
		t.Setenv("GIT_DIR", decoy)
		t.Setenv("GIT_WORK_TREE", decoy)
		t.Setenv("GIT_INDEX_FILE", filepath.Join(decoy, "index"))
		got := FGFreshnessGit{Dirs: map[string]string{"task-graph": repo.dir}}.
			Reachability(context.Background(), "task-graph", repo.a, "HEAD")
		if got.Class != FGFreshnessReachable {
			t.Fatalf("result = %+v, want reachable despite ambient GIT_DIR", got)
		}
	})

	t.Run("real_git_shallow_checkout_is_unverifiable_where_git_reports_a_false_non_ancestor", func(t *testing.T) {
		fgFreshnessHermeticGit(t)
		src := fgFreshnessBuildRepo(t)
		shallow := filepath.Join(t.TempDir(), "shallow")
		if out, err := exec.Command("git", "clone", "-q", "--depth", "1", "file://"+src.dir, shallow).CombinedOutput(); err != nil {
			t.Fatalf("clone: %v\n%s", err, out)
		}
		fgFreshnessGitOut(t, shallow, "fetch", "-q", "--depth", "1", "origin", src.a)
		// Control: the raw git answer in the shallow checkout is the false "not an ancestor".
		if err := exec.Command("git", "-C", shallow, "merge-base", "--is-ancestor", src.a, "HEAD").Run(); err == nil {
			t.Fatal("control: expected git to (wrongly) report A as not an ancestor of HEAD in the shallow checkout")
		}
		got := FGFreshnessGit{Dirs: map[string]string{"task-graph": shallow}}.
			Reachability(context.Background(), "task-graph", src.a, "HEAD")
		if got.Class != FGFreshnessUnverifiable || got.Reason != "shallow_checkout" {
			t.Fatalf("shallow result = %+v, want unverifiable/shallow_checkout", got)
		}
		full := FGFreshnessGit{Dirs: map[string]string{"task-graph": src.dir}}.
			Reachability(context.Background(), "task-graph", src.a, "HEAD")
		if full.Class != FGFreshnessReachable {
			t.Fatalf("complete checkout result = %+v, want reachable", full)
		}
	})

	t.Run("cancelled_context_during_reachability_returns_the_context_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		reader := fgFreshnessCancelReader{cancel: cancel}
		snap := fgFreshnessSnapshot(fgFreshnessNow,
			[]TaskRow{fgFreshnessTask("TASK-A", "2026-09-30T11:00:00Z")},
			[]WorklinkRow{fgFreshnessCommitLink("L-1", "TASK-A", "2026-09-30T11:00:00Z", "task-graph", fgFreshnessSHA1)})
		got, err := RunFGFreshnessCheck(ctx, reg, snap, fgFreshnessOpts(hour, reader))
		if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, FGFreshnessResult{}) {
			t.Fatalf("got (%+v, %v), want context.Canceled and no result", got, err)
		}
	})
}

type fgFreshnessCancelReader struct{ cancel context.CancelFunc }

func (r fgFreshnessCancelReader) Reachability(_ context.Context, _, _, ref string) FGFreshnessRefResult {
	r.cancel()
	return FGFreshnessRefResult{Ref: ref, Class: FGFreshnessUnverifiable, Reason: "git_exit_-1"}
}
