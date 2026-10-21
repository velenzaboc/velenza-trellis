package main

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// snapshot_coherence.go carries the coherence surface P3-GT-FP-SNAPSHOT asks a
// task-plan project snapshot to publish: revision, read timestamp, content
// hashes, authorized totals, cursors and cap state.
//
// Every value here is DERIVED from rows the caller already read inside ONE
// read-only transaction (see SpannerSubstrateAdapter.ReadOnlySnapshot). None of
// them issues its own read — a helper that read the store again would reopen
// the exact interleaving this file exists to close.
//
// Everything in this file is a pure function, so the whole surface is testable
// without Spanner.

// snapshotRevisionPrefix tags the revision token so a consumer cannot mistake
// it for a bare timestamp and do arithmetic on it. The token is OPAQUE: compare
// it for equality, do not parse it.
const snapshotRevisionPrefix = "fp1:"

// SnapshotHashes are deterministic content digests of each collection, plus one
// over the whole snapshot body.
//
// Determinism rests on each collection's query carrying a UNIQUE TOTAL ORDER
// (see nativeTasksQuery / nativeSnapshotDependenciesQuery /
// nativeSnapshotWorklinksQuery). Without that, ties could reorder between two
// reads of identical data and the hash would change with no change in content.
//
// Equal hash + equal revision means the same rows at the same read timestamp.
// Equal hash + DIFFERENT revision means the content did not change between the
// two timestamps, which is what lets a consumer skip a re-render.
type SnapshotHashes struct {
	Tasks        string `json:"tasks"`
	Dependencies string `json:"dependencies"`
	Worklinks    string `json:"worklinks"`
	Counts       string `json:"counts"`
	Snapshot     string `json:"snapshot"`
}

// CollectionState is the cap/cursor state of one capped collection.
type CollectionState struct {
	// Returned is how many rows this snapshot actually carries.
	Returned int `json:"returned"`
	// Collected is the number of rows returned across page one through this
	// page. It lets a consumer prove that a completed reassembly reached the
	// immutable authorized total without treating a page-local count as global.
	Collected int `json:"collected"`
	// AuthorizedTotal is how many rows the project holds for this collection
	// IN FULL at the snapshot's read timestamp, independent of the cap. It is
	// counted inside the same transaction as the rows, so
	// AuthorizedTotal > Returned always means "the cap truncated this", never
	// "the store moved between two reads".
	AuthorizedTotal int `json:"authorized_total"`
	// Capped is true when the cap truncated this collection.
	Capped bool `json:"capped"`
	// Limit is the per-collection cap that was in force.
	Limit int `json:"limit"`
	// Complete is false when this page is only a prefix of the collection.
	// Consumers must follow the authenticated continuation before treating the
	// collection as a complete portfolio inventory.
	Complete bool `json:"complete"`
	// NextCursor is the ordering key of the LAST row returned, encoded as a
	// canonical JSON array of that collection's ORDER BY terms. Empty when the
	// collection was not capped (nothing follows) or returned no rows.
	// Resuming means: fetch rows whose ordering tuple sorts strictly after
	// this one. P3-GT-PAGINATION consumes this.
	NextCursor string `json:"next_cursor,omitempty"`
}

// SnapshotCollections groups the per-collection cap/cursor state.
type SnapshotCollections struct {
	Tasks        CollectionState `json:"tasks"`
	Dependencies CollectionState `json:"dependencies"`
	Worklinks    CollectionState `json:"worklinks"`
}

// SnapshotScope identifies the server-owned membership used by a scoped
// project snapshot. Complete describes membership discovery, while each
// collection's CollectionState describes whether the returned rows hit the
// per-collection cap. MissingTaskIDs are exact task references encountered by
// the scope that were not present in Tasks at the same read timestamp.
type SnapshotScope struct {
	Mode            string   `json:"mode"`
	RootTaskID      string   `json:"root_task_id,omitempty"`
	Complete        bool     `json:"complete"`
	Capped          bool     `json:"capped"`
	MembershipCount int      `json:"membership_count"`
	MissingTaskIDs  []string `json:"missing_task_ids,omitempty"`
}

// snapshotRevision renders a Spanner read timestamp as the snapshot's opaque
// revision token. Two snapshots carrying the same revision were read at the
// same timestamp and therefore describe the same state.
func snapshotRevision(readTimestamp time.Time) string {
	return snapshotRevisionPrefix + readTimestamp.UTC().Format(time.RFC3339Nano)
}

// newCollectionState assembles one collection's cap state. The cursor is
// dropped when the collection was not capped, because a cursor that points at
// the end of a complete collection invites a pointless extra round trip.
func newCollectionState(returned, authorizedTotal, limit int, cursor string) CollectionState {
	return newPagedCollectionState(0, returned, authorizedTotal, limit, cursor)
}

func newPagedCollectionState(collectedBefore, returned, authorizedTotal, limit int, cursor string) CollectionState {
	collected := collectedBefore + returned
	// Completeness is determined by the immutable authorized total, never by
	// whether this page happened to return rows. A zero-row page with a
	// positive total is incomplete (and is rejected by the handler as
	// no-progress), not a falsely complete collection.
	capped := collected < authorizedTotal
	st := CollectionState{
		Returned:        returned,
		Collected:       collected,
		AuthorizedTotal: authorizedTotal,
		Capped:          capped,
		Limit:           limit,
		Complete:        !capped,
	}
	if capped {
		st.NextCursor = cursor
	}
	return st
}

// encodeCursor renders an ordering tuple as canonical JSON. A canonical JSON
// array is used rather than a delimiter-joined string because task ids, gap
// labels and artifact refs are free text — any separator character could occur
// inside a value, and JSON escaping is the only encoding that stays
// unambiguous without inventing a quoting rule.
func encodeCursor(parts []string) string {
	b, err := canonicalJSON(parts)
	if err != nil {
		// canonicalJSON on a []string cannot fail; a nil cursor is still the
		// safe answer if it somehow did, because an ABSENT cursor makes a
		// consumer restart the page rather than skip rows.
		return ""
	}
	return string(b)
}

// taskCursor encodes the last task row's ORDER BY tuple: pillar, status, id.
func taskCursor(rows []TaskRow) string {
	if len(rows) == 0 {
		return ""
	}
	last := rows[len(rows)-1]
	return encodeCursor([]string{last.Pillar, last.Status, last.TaskID})
}

// dependencyCursor encodes the last dependency row's ORDER BY tuple:
// task_id, depends_on_task_id, dep_id.
func dependencyCursor(rows []DependencyRow) string {
	if len(rows) == 0 {
		return ""
	}
	last := rows[len(rows)-1]
	return encodeCursor([]string{last.TaskID, last.DependsOnTaskID, last.DependencyID})
}

// worklinkCursor encodes the last worklink row's ORDER BY tuple:
// created_at, link_id.
func worklinkCursor(rows []WorklinkRow) string {
	if len(rows) == 0 {
		return ""
	}
	last := rows[len(rows)-1]
	return encodeCursor([]string{last.CreatedAt, last.ArtifactID})
}

// contentHash is the lowercase-hex SHA-256 of a value's canonical JSON.
func contentHash(v interface{}) (string, error) {
	b, err := canonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// snapshotContentHashes digests each collection plus the snapshot body.
//
// The snapshot-level hash is taken over the four COLLECTION HASHES, not over
// the raw rows again: that makes it cheap, and it makes the composition
// explicit — a snapshot hash changes exactly when one of its named parts does.
// It deliberately excludes revision, read timestamp and generated_at, so the
// hash answers "did the CONTENT change" rather than "was this read again".
func snapshotContentHashes(tasks []TaskRow, deps []DependencyRow, links []WorklinkRow, counts TaskCountsResult) (SnapshotHashes, error) {
	var (
		out SnapshotHashes
		err error
	)
	if out.Tasks, err = contentHash(tasks); err != nil {
		return SnapshotHashes{}, err
	}
	if out.Dependencies, err = contentHash(deps); err != nil {
		return SnapshotHashes{}, err
	}
	if out.Worklinks, err = contentHash(links); err != nil {
		return SnapshotHashes{}, err
	}
	if out.Counts, err = contentHash(counts); err != nil {
		return SnapshotHashes{}, err
	}
	out.Snapshot, err = contentHash([]string{out.Tasks, out.Dependencies, out.Worklinks, out.Counts})
	if err != nil {
		return SnapshotHashes{}, err
	}
	return out, nil
}
