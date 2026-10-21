package main

import (
	"context"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// FGCheckpointOutcome is either a successful selection or a typed refusal.
type FGCheckpointOutcome string

const (
	FGCheckpointSelected     FGCheckpointOutcome = "selected"
	FGCheckpointAmbiguous    FGCheckpointOutcome = "ambiguous"
	FGCheckpointStale        FGCheckpointOutcome = "stale"
	FGCheckpointUnverifiable FGCheckpointOutcome = "unverifiable"
)

// FGCheckpointEvidence is immutable evidence parsed from a checkpoint file.
// A nil Epoch means that the checkpoint has no epoch and must be ordered by
// its worklink's parsed CreatedAt value.
type FGCheckpointEvidence struct {
	Epoch *uint64 `json:"epoch,omitempty"`
}

// FGSelectedCheckpoint is the normalized, verified checkpoint reference.
type FGSelectedCheckpoint struct {
	ArtifactID string  `json:"artifact_id"`
	Path       string  `json:"path"`
	SHA256     string  `json:"sha256"`
	Epoch      *uint64 `json:"epoch,omitempty"`
	CreatedAt  string  `json:"created_at"`
}

// FGCheckpointSelection reports one selected checkpoint or a typed refusal.
// Evidence contains sorted artifact IDs that establish the outcome.
type FGCheckpointSelection struct {
	Outcome    FGCheckpointOutcome   `json:"outcome"`
	Checkpoint *FGSelectedCheckpoint `json:"checkpoint,omitempty"`
	Evidence   []string              `json:"evidence,omitempty"`
}

type fgCheckpointCandidate struct {
	row       WorklinkRow
	path      string
	digest    string
	epoch     *uint64
	createdAt time.Time
}

// SelectFGCheckpoint selects the latest verifiable checkpoint for exactly one
// project/task identity. Worklinks are the caller's active projection. Epochs
// come from immutable checkpoint files; supersedingBindings must already have
// been classified by the caller as bindings that supersede the task's resume
// action. The function performs no I/O and does not mutate its inputs.
func SelectFGCheckpoint(
	ctx context.Context,
	reg ArtifactKindRegistry,
	projectID, taskID string,
	worklinks []WorklinkRow,
	evidence map[string]FGCheckpointEvidence,
	supersedingBindings []WorklinkRow,
) FGCheckpointSelection {
	candidates := make([]fgCheckpointCandidate, 0)
	invalid := make([]string, 0)
	for _, row := range worklinks {
		if row.ProjectID != projectID || row.TaskID != taskID || reg == nil {
			continue
		}
		kind, found := reg.Canonicalize(ctx, row.ArtifactType)
		if !found || kind != "checkpoint" {
			continue
		}
		path, digest, ok := parseFGCheckpointRef(row.ArtifactRef)
		createdAt, timeOK := parseFGCheckpointTime(row.CreatedAt)
		if !ok || !timeOK {
			invalid = append(invalid, row.ArtifactID)
			continue
		}
		var epoch *uint64
		if item, found := evidence[row.ArtifactID]; found && item.Epoch != nil {
			value := *item.Epoch
			epoch = &value
		}
		candidates = append(candidates, fgCheckpointCandidate{row: row, path: path, digest: digest, epoch: epoch, createdAt: createdAt})
	}
	if len(invalid) > 0 {
		return fgCheckpointRefusal(FGCheckpointUnverifiable, invalid...)
	}
	if len(candidates) == 0 {
		return FGCheckpointSelection{Outcome: FGCheckpointUnverifiable}
	}

	withEpoch := 0
	for _, candidate := range candidates {
		if candidate.epoch != nil {
			withEpoch++
		}
	}
	if withEpoch != 0 && withEpoch != len(candidates) {
		ids := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			ids = append(ids, candidate.row.ArtifactID)
		}
		return fgCheckpointRefusal(FGCheckpointUnverifiable, ids...)
	}

	latest := candidates[0]
	for _, candidate := range candidates[1:] {
		if fgCheckpointLater(candidate, latest, withEpoch > 0) {
			latest = candidate
		}
	}
	tied := make([]string, 0)
	for _, candidate := range candidates {
		if fgCheckpointEqualOrder(candidate, latest, withEpoch > 0) {
			tied = append(tied, candidate.row.ArtifactID)
		}
	}
	if len(tied) > 1 {
		return fgCheckpointRefusal(FGCheckpointAmbiguous, tied...)
	}

	invalidBindings := make([]string, 0)
	laterBindings := make([]string, 0)
	for _, binding := range supersedingBindings {
		createdAt, ok := parseFGCheckpointTime(binding.CreatedAt)
		if !ok {
			invalidBindings = append(invalidBindings, binding.ArtifactID)
			continue
		}
		if createdAt.After(latest.createdAt) {
			laterBindings = append(laterBindings, binding.ArtifactID)
		}
	}
	if len(invalidBindings) > 0 {
		return fgCheckpointRefusal(FGCheckpointUnverifiable, invalidBindings...)
	}
	if len(laterBindings) > 0 {
		laterBindings = append(laterBindings, latest.row.ArtifactID)
		return fgCheckpointRefusal(FGCheckpointStale, laterBindings...)
	}

	return FGCheckpointSelection{
		Outcome: FGCheckpointSelected,
		Checkpoint: &FGSelectedCheckpoint{
			ArtifactID: latest.row.ArtifactID,
			Path:       latest.path,
			SHA256:     latest.digest,
			Epoch:      latest.epoch,
			CreatedAt:  latest.row.CreatedAt,
		},
		Evidence: []string{latest.row.ArtifactID},
	}
}

func parseFGCheckpointRef(ref string) (string, string, bool) {
	separator := strings.LastIndexByte(ref, '#')
	if separator <= 0 || separator == len(ref)-1 {
		return "", "", false
	}
	path, digest := ref[:separator], ref[separator+1:]
	if strings.HasPrefix(digest, "sha256=") {
		digest = strings.TrimPrefix(digest, "sha256=")
	}
	if len(digest) != 64 {
		return "", "", false
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", "", false
	}
	return path, strings.ToLower(digest), true
}

func parseFGCheckpointTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if len(value) > 10 && value[10] == ' ' {
		value = value[:10] + "T" + value[11:]
	}
	if len(value) >= 3 {
		suffix := value[len(value)-3:]
		if (suffix[0] == '+' || suffix[0] == '-') && suffix[1] >= '0' && suffix[1] <= '9' && suffix[2] >= '0' && suffix[2] <= '9' {
			value += ":00"
		}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	return parsed, err == nil
}

func fgCheckpointLater(left, right fgCheckpointCandidate, epochOrder bool) bool {
	if epochOrder {
		return *left.epoch > *right.epoch
	}
	return left.createdAt.After(right.createdAt)
}

func fgCheckpointEqualOrder(left, right fgCheckpointCandidate, epochOrder bool) bool {
	if epochOrder {
		return *left.epoch == *right.epoch
	}
	return left.createdAt.Equal(right.createdAt)
}

func fgCheckpointRefusal(outcome FGCheckpointOutcome, ids ...string) FGCheckpointSelection {
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	return FGCheckpointSelection{Outcome: outcome, Evidence: ids}
}
