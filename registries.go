package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// registries.go closes three task-plan-native free-text columns that had no
// registry behind them — P3-GT-FP-REGISTRIES:
//
//   - Tasks.status          -> TaskStatusRegistry / TaskStatusDescriptor
//   - TaskDeps.kind         -> DependencyKindRegistry / DependencyKindDescriptor
//   - TaskLinks.artifact_type -> ArtifactKindRegistry / ArtifactKindDescriptor
//
// Same shape as kinds.go (NodeKindRegistry) and edges.go (EdgeLabelRegistry):
// config-driven, binary holds field ACCESSORS for the mechanical fields it
// consumes, the underlying map carries any protocol-extra metadata verbatim.
//
// Reconciliation model (config-driven, per binary_is_never_sot): each
// descriptor carries an `absorbs` array of legacy free-text literals it
// claims — the EXPLICIT, reviewable mapping from the live column's historical
// values onto this closed member. `absorbs` is read-side/reporting only
// (Canonicalize) and is NEVER consulted to auto-translate a write — a NEW
// write must supply an already-canonical member name (Resolve), so the
// registry actually stays closed going forward instead of silently
// re-opening itself through the back door of the reconciliation table.

// ---------------- TaskStatusDescriptor / TaskStatusRegistry ----------------

// TaskStatusDescriptor wraps the raw config map for one task_statuses entry.
type TaskStatusDescriptor struct{ raw map[string]interface{} }

func NewTaskStatusDescriptor(raw map[string]interface{}) TaskStatusDescriptor {
	if raw == nil {
		raw = map[string]interface{}{}
	}
	return TaskStatusDescriptor{raw: raw}
}

func (d *TaskStatusDescriptor) UnmarshalJSON(b []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	d.raw = m
	return nil
}

func (d TaskStatusDescriptor) MarshalJSON() ([]byte, error) {
	if d.raw == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(d.raw)
}

func (d TaskStatusDescriptor) Raw() map[string]interface{} {
	if d.raw == nil {
		return map[string]interface{}{}
	}
	return d.raw
}

// Name is the canonical status value (required).
func (d TaskStatusDescriptor) Name() string { return descOptString(d.raw, "name") }

// Terminal flags a status with no in-flight execution remaining
// (documentation/reporting metadata; the binary does not special-case it —
// legal_next is what the transition checker actually consults).
func (d TaskStatusDescriptor) Terminal() bool { return descOptBool(d.raw, "terminal") }

// LegalNext lists the status names this status may transition to. A
// same-status "transition" (no-op set_status) is always legal regardless of
// this list — see taskStatusTransitionLegal.
func (d TaskStatusDescriptor) LegalNext() []string { return descOptStrings(d.raw, "legal_next") }

// Absorbs lists every legacy free-text Tasks.status literal this canonical
// member claims for reconciliation reads. Never consulted at write time.
func (d TaskStatusDescriptor) Absorbs() []string { return descOptStrings(d.raw, "absorbs") }

// unrecognizedTaskStatus is the canonical_status bucket the count surface
// uses for a stored status literal that no task_statuses member absorbs. The
// name is reserved: a member carrying it would merge its own rows with the
// unclaimed ones in a single count group.
const unrecognizedTaskStatus = "unrecognized"

// Validate refuses a descriptor missing required mechanical fields.
func (d TaskStatusDescriptor) Validate(index int) error {
	if _, ok := d.raw["name"].(string); !ok || d.Name() == "" {
		return fmt.Errorf("task_statuses[%d]: name required and must be a non-empty string", index)
	}
	if d.Name() == unrecognizedTaskStatus {
		return fmt.Errorf("task_statuses[%d]: name %q is reserved for status literals no member absorbs", index, d.Name())
	}
	if v, ok := d.raw["legal_next"]; ok && v != nil {
		if _, isArr := v.([]interface{}); !isArr {
			return fmt.Errorf("task_statuses[%d] (name=%q): legal_next must be a JSON array when set", index, d.Name())
		}
	}
	if v, ok := d.raw["absorbs"]; ok && v != nil {
		if _, isArr := v.([]interface{}); !isArr {
			return fmt.Errorf("task_statuses[%d] (name=%q): absorbs must be a JSON array when set", index, d.Name())
		}
	}
	return nil
}

// TaskStatusRegistry is the closed-set authority for Tasks.status values.
type TaskStatusRegistry interface {
	// Resolve returns the descriptor for an already-canonical status name,
	// or ErrUnknownTaskStatus. Used to gate every WRITE.
	Resolve(ctx context.Context, name string) (TaskStatusDescriptor, error)
	// Canonicalize maps a (possibly legacy, non-canonical) literal onto its
	// canonical member via the absorbs reconciliation, reporting found=false
	// if no member (and no member's absorbs list) claims it. Read-side only.
	Canonicalize(ctx context.Context, literal string) (canonical string, found bool)
	All(ctx context.Context) ([]TaskStatusDescriptor, error)
}

type fileTaskStatusRegistry struct {
	byName        map[string]TaskStatusDescriptor
	canonicalizer map[string]string
}

// newTaskStatusRegistry builds the registry AND its reconciliation index in
// one pass. Returns an error (never panics) if two members' names collide,
// or if the same legacy literal is absorbed by two different members, or a
// literal collides with a DIFFERENT member's own canonical name — the same
// "no double-claim" guarantee a Go map literal gets from the compiler,
// enforced here at config-load time since the mapping lives in JSON config
// per binary_is_never_sot.
func newTaskStatusRegistry(statuses []TaskStatusDescriptor) (TaskStatusRegistry, error) {
	r := &fileTaskStatusRegistry{
		byName:        make(map[string]TaskStatusDescriptor, len(statuses)),
		canonicalizer: map[string]string{},
	}
	claimedBy := map[string]string{}
	for _, s := range statuses {
		name := s.Name()
		if _, dup := r.byName[name]; dup {
			return nil, fmt.Errorf("task_statuses: duplicate member name %q", name)
		}
		r.byName[name] = s
		if err := claimLiteral(claimedBy, name, name); err != nil {
			return nil, fmt.Errorf("task_statuses: %w", err)
		}
		r.canonicalizer[name] = name
		for _, lit := range s.Absorbs() {
			if err := claimLiteral(claimedBy, lit, name); err != nil {
				return nil, fmt.Errorf("task_statuses: %w", err)
			}
			r.canonicalizer[lit] = name
		}
	}
	return r, nil
}

func (r *fileTaskStatusRegistry) Resolve(_ context.Context, name string) (TaskStatusDescriptor, error) {
	d, ok := r.byName[name]
	if !ok {
		return TaskStatusDescriptor{}, fmt.Errorf("%w: %q (add to task-graph.config.json task_statuses, or set TASK_GRAPH_CONFIG)", ErrUnknownTaskStatus, name)
	}
	return d, nil
}

func (r *fileTaskStatusRegistry) Canonicalize(_ context.Context, literal string) (string, bool) {
	c, ok := r.canonicalizer[literal]
	return c, ok
}

func (r *fileTaskStatusRegistry) All(_ context.Context) ([]TaskStatusDescriptor, error) {
	out := make([]TaskStatusDescriptor, 0, len(r.byName))
	for _, d := range r.byName {
		out = append(out, d)
	}
	sortStatusDescriptors(out)
	return out, nil
}

// taskStatusTransitionLegal reports whether a status transition is allowed.
// A same-status transition (idempotent set_status) is ALWAYS legal — the
// registry's legal_next lists only the genuinely different transitions, per
// the same design edges.go uses for ForbidSelfLoops (a separate declared
// property rather than requiring every source_kinds list to spell out the
// identity case).
func taskStatusTransitionLegal(cur TaskStatusDescriptor, next string) bool {
	if cur.Name() == next {
		return true
	}
	for _, n := range cur.LegalNext() {
		if n == next {
			return true
		}
	}
	return false
}

// checkTaskStatusWrite validates a proposed write of newLiteral into
// Tasks.status for a row whose CURRENT stored value is currentLiteral. Pure
// over the registry (no Spanner I/O) so it is directly unit-testable;
// nativeUpdateTaskFields calls it inside its ReadWriteTransaction after
// reading the row's current status. Decision order:
//
//  1. STATUS-PRESERVING WRITES NEVER FAIL. newLiteral == currentLiteral is
//     unconditionally legal — the get→update round trip passes the row's own
//     stored status back unchanged, the column value does not change, so
//     neither closed-registry membership nor the transition graph applies.
//     Before this rule existed, validating the INCOMING literal with Resolve
//     up front froze every row carrying a pre-registry legacy literal
//     (completed/resolved/audit_probe/…): its note/owner/parent_id could not
//     be updated through any status-carrying payload even though the caller
//     changed nothing about the status (the 2026-08-20 defect, hit live by
//     two independent lanes; they fell back to raw DML). The registry stays
//     closed: this branch can only re-write a value the row already carries,
//     never admit a new one.
//
//  2. A status CHANGE still only ever writes a canonical member — newLiteral
//     must Resolve. A non-member literal that IS absorbed by a member is
//     refused with the canonical name in the message (write "done", not
//     "completed"); a genuinely novel literal fails closed with the standard
//     ErrUnknownTaskStatus. `absorbs` remains read-side only — a write is
//     never auto-translated (see the reconciliation model note at the top of
//     this file).
//
//  3. The transition graph is checked from the CURRENT status, which is
//     Canonicalize'd (never Resolve'd) because the live table still carries
//     legacy literals — e.g. a row at "completed" transitions under the
//     legal_next of its canonical member "done".
func checkTaskStatusWrite(ctx context.Context, reg TaskStatusRegistry, taskID, currentLiteral, newLiteral string) error {
	if newLiteral == currentLiteral {
		return nil
	}
	newDesc, err := reg.Resolve(ctx, newLiteral)
	if err != nil {
		if canonical, found := reg.Canonicalize(ctx, newLiteral); found {
			return fmt.Errorf("%w: %q is a legacy literal absorbed by canonical member %q — write %q to change the status, or pass the row's current status back unchanged to preserve it", ErrUnknownTaskStatus, newLiteral, canonical, canonical)
		}
		return err
	}
	canonicalCurrent, found := reg.Canonicalize(ctx, currentLiteral)
	if !found {
		return fmt.Errorf("%w: task %q current status %q is not a recognized legacy literal in any task_statuses member's absorbs list", ErrUnknownTaskStatus, taskID, currentLiteral)
	}
	currentDesc, err := reg.Resolve(ctx, canonicalCurrent)
	if err != nil {
		return err
	}
	if !taskStatusTransitionLegal(currentDesc, newLiteral) {
		return fmt.Errorf("%w: task %q status %q (current literal %q) -> %q", ErrIllegalStatusTransition, taskID, currentDesc.Name(), currentLiteral, newDesc.Name())
	}
	return nil
}

func sortStatusDescriptors(ds []TaskStatusDescriptor) {
	sort.Slice(ds, func(i, j int) bool { return ds[i].Name() < ds[j].Name() })
}

// ---------------- DependencyKindDescriptor / DependencyKindRegistry ----------------

// DependencyKindDescriptor wraps the raw config map for one
// dependency_kinds entry (TaskDeps.kind closed registry).
type DependencyKindDescriptor struct{ raw map[string]interface{} }

func NewDependencyKindDescriptor(raw map[string]interface{}) DependencyKindDescriptor {
	if raw == nil {
		raw = map[string]interface{}{}
	}
	return DependencyKindDescriptor{raw: raw}
}

func (d *DependencyKindDescriptor) UnmarshalJSON(b []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	d.raw = m
	return nil
}

func (d DependencyKindDescriptor) MarshalJSON() ([]byte, error) {
	if d.raw == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(d.raw)
}

func (d DependencyKindDescriptor) Raw() map[string]interface{} {
	if d.raw == nil {
		return map[string]interface{}{}
	}
	return d.raw
}

// Name is the canonical TaskDeps.kind value (required).
func (d DependencyKindDescriptor) Name() string { return descOptString(d.raw, "name") }

// EdgeFamily groups the kind for closure_profile-style traversal (required)
// — "dep" for genuine execution-order gates, "ref" for non-blocking
// traceability/source-mapping relationships.
func (d DependencyKindDescriptor) EdgeFamily() string { return descOptString(d.raw, "edge_family") }

// Blocking marks a kind as a genuine execution-order gate: acyclicity is
// enforced ONLY over the subgraph of blocking=true edges (see
// wouldCreateDependencyCycle). A non-blocking kind (source_map) is a
// traceability reference and may legitimately form a cycle (e.g. two tasks
// mutually referencing each other's projections) without a scheduling
// problem.
func (d DependencyKindDescriptor) Blocking() bool { return descOptBool(d.raw, "blocking") }

// LifecycleMarker flags the one member ("archived") that is not a semantic
// dependency kind at all but the soft-delete marker TaskDeps.kind doubles
// as — the DDL has no separate status column, so archival is recorded as a
// kind value. A lifecycle-marker row is excluded from uniqueness/acyclicity
// checks (it is not an active edge) and is always a legal transition target
// from any other member (see dependencyKindTransitionLegal).
func (d DependencyKindDescriptor) LifecycleMarker() bool {
	return descOptBool(d.raw, "lifecycle_marker")
}

// LegalNext lists the kind names this kind may transition to.
func (d DependencyKindDescriptor) LegalNext() []string { return descOptStrings(d.raw, "legal_next") }

// Absorbs lists every legacy free-text TaskDeps.kind literal this canonical
// member claims for reconciliation reads.
func (d DependencyKindDescriptor) Absorbs() []string { return descOptStrings(d.raw, "absorbs") }

func (d DependencyKindDescriptor) Validate(index int) error {
	if _, ok := d.raw["name"].(string); !ok || d.Name() == "" {
		return fmt.Errorf("dependency_kinds[%d]: name required and must be a non-empty string", index)
	}
	if _, ok := d.raw["edge_family"].(string); !ok || d.EdgeFamily() == "" {
		return fmt.Errorf("dependency_kinds[%d] (name=%q): edge_family required and must be a non-empty string", index, d.Name())
	}
	if v, ok := d.raw["legal_next"]; ok && v != nil {
		if _, isArr := v.([]interface{}); !isArr {
			return fmt.Errorf("dependency_kinds[%d] (name=%q): legal_next must be a JSON array when set", index, d.Name())
		}
	}
	if v, ok := d.raw["absorbs"]; ok && v != nil {
		if _, isArr := v.([]interface{}); !isArr {
			return fmt.Errorf("dependency_kinds[%d] (name=%q): absorbs must be a JSON array when set", index, d.Name())
		}
	}
	return nil
}

type DependencyKindRegistry interface {
	Resolve(ctx context.Context, name string) (DependencyKindDescriptor, error)
	Canonicalize(ctx context.Context, literal string) (canonical string, found bool)
	All(ctx context.Context) ([]DependencyKindDescriptor, error)
}

type fileDependencyKindRegistry struct {
	byName        map[string]DependencyKindDescriptor
	canonicalizer map[string]string
}

func newDependencyKindRegistry(kinds []DependencyKindDescriptor) (DependencyKindRegistry, error) {
	r := &fileDependencyKindRegistry{
		byName:        make(map[string]DependencyKindDescriptor, len(kinds)),
		canonicalizer: map[string]string{},
	}
	claimedBy := map[string]string{}
	for _, k := range kinds {
		name := k.Name()
		if _, dup := r.byName[name]; dup {
			return nil, fmt.Errorf("dependency_kinds: duplicate member name %q", name)
		}
		r.byName[name] = k
		if err := claimLiteral(claimedBy, name, name); err != nil {
			return nil, fmt.Errorf("dependency_kinds: %w", err)
		}
		r.canonicalizer[name] = name
		for _, lit := range k.Absorbs() {
			if err := claimLiteral(claimedBy, lit, name); err != nil {
				return nil, fmt.Errorf("dependency_kinds: %w", err)
			}
			r.canonicalizer[lit] = name
		}
	}
	return r, nil
}

func (r *fileDependencyKindRegistry) Resolve(_ context.Context, name string) (DependencyKindDescriptor, error) {
	d, ok := r.byName[name]
	if !ok {
		return DependencyKindDescriptor{}, fmt.Errorf("%w: %q (add to task-graph.config.json dependency_kinds, or set TASK_GRAPH_CONFIG)", ErrUnknownDependencyKind, name)
	}
	return d, nil
}

func (r *fileDependencyKindRegistry) Canonicalize(_ context.Context, literal string) (string, bool) {
	c, ok := r.canonicalizer[literal]
	return c, ok
}

func (r *fileDependencyKindRegistry) All(_ context.Context) ([]DependencyKindDescriptor, error) {
	out := make([]DependencyKindDescriptor, 0, len(r.byName))
	for _, d := range r.byName {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

// dependencyKindTransitionLegal mirrors taskStatusTransitionLegal: a
// same-kind transition is always legal (idempotent), and transitioning INTO
// a lifecycle-marker kind (archive) is always legal from any current kind —
// matching nativeHandleUnlink's existing behavior of archiving regardless of
// the row's current semantic kind.
func dependencyKindTransitionLegal(cur DependencyKindDescriptor, next DependencyKindDescriptor) bool {
	if cur.Name() == next.Name() {
		return true
	}
	if next.LifecycleMarker() {
		return true
	}
	for _, n := range cur.LegalNext() {
		if n == next.Name() {
			return true
		}
	}
	return false
}

// depEdge is the pure, in-memory representation of one active TaskDeps row
// used by wouldCreateDependencyCycle. task_id/depends_on_task_id carry the
// same semantics as the TaskDeps columns: TaskID depends on
// DependsOnTaskID (DependsOnTaskID must complete first).
type depEdge struct {
	TaskID          string
	DependsOnTaskID string
}

// wouldCreateDependencyCycle reports whether inserting the edge
// (taskID depends on dependsOnTaskID) into the given set of existing active
// BLOCKING edges would close a cycle, and if so returns the cycle path
// (dependsOnTaskID -> ... -> taskID -> dependsOnTaskID) for the error
// message. Pure/no I/O — the caller (nativeInsertDep) fetches the live
// active blocking-edge set within its transaction and passes it in.
//
// A cycle closes iff dependsOnTaskID can already (transitively) reach
// taskID by following existing "depends on" edges — i.e. dependsOnTaskID
// already depends, directly or transitively, on taskID. Adding
// taskID -> dependsOnTaskID on top of that closes the loop.
//
// Bounded by construction: the BFS below carries a visited set, so it
// terminates in O(V+E) regardless of whether the INPUT edge set itself
// already contains a cycle (defensive — the live active-blocking graph is
// verified cycle-free as of this task's investigation, but a reader must
// never infinite-loop even if that ever stops holding).
func wouldCreateDependencyCycle(existing []depEdge, taskID, dependsOnTaskID string) (bool, []string) {
	if taskID == dependsOnTaskID {
		return true, []string{taskID, dependsOnTaskID}
	}
	adj := map[string][]string{}
	for _, e := range existing {
		adj[e.TaskID] = append(adj[e.TaskID], e.DependsOnTaskID)
	}
	// BFS from dependsOnTaskID looking for taskID, tracking predecessors to
	// reconstruct the path for the error message.
	visited := map[string]bool{dependsOnTaskID: true}
	queue := []string{dependsOnTaskID}
	pred := map[string]string{}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if n == taskID {
			path := []string{taskID}
			for cur := taskID; cur != dependsOnTaskID; {
				p, ok := pred[cur]
				if !ok {
					break
				}
				path = append(path, p)
				cur = p
			}
			// path is taskID -> ... -> dependsOnTaskID; reverse for
			// dependsOnTaskID -> ... -> taskID -> dependsOnTaskID (readable
			// as "the cycle this insert would close").
			reversed := make([]string, 0, len(path)+1)
			for i := len(path) - 1; i >= 0; i-- {
				reversed = append(reversed, path[i])
			}
			reversed = append(reversed, dependsOnTaskID)
			return true, reversed
		}
		for _, m := range adj[n] {
			if visited[m] {
				continue
			}
			visited[m] = true
			pred[m] = n
			queue = append(queue, m)
		}
	}
	return false, nil
}

// ---------------- ArtifactKindDescriptor / ArtifactKindRegistry ----------------

// ArtifactKindDescriptor wraps the raw config map for one artifact_kinds
// entry (TaskLinks.artifact_type closed registry).
type ArtifactKindDescriptor struct{ raw map[string]interface{} }

func NewArtifactKindDescriptor(raw map[string]interface{}) ArtifactKindDescriptor {
	if raw == nil {
		raw = map[string]interface{}{}
	}
	return ArtifactKindDescriptor{raw: raw}
}

func (d *ArtifactKindDescriptor) UnmarshalJSON(b []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	d.raw = m
	return nil
}

func (d ArtifactKindDescriptor) MarshalJSON() ([]byte, error) {
	if d.raw == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(d.raw)
}

func (d ArtifactKindDescriptor) Raw() map[string]interface{} {
	if d.raw == nil {
		return map[string]interface{}{}
	}
	return d.raw
}

// Name is the canonical TaskLinks.artifact_type value (required). Must fit
// the STRING(32) column — descOptString does not enforce this; Validate
// does, since it's a structural DDL constraint every member must satisfy.
func (d ArtifactKindDescriptor) Name() string { return descOptString(d.raw, "name") }

// Absorbs lists every legacy free-text TaskLinks.artifact_type literal this
// canonical member claims for reconciliation reads.
func (d ArtifactKindDescriptor) Absorbs() []string { return descOptStrings(d.raw, "absorbs") }

const artifactKindMaxNameLen = 32 // TaskLinks.artifact_type STRING(32)

func (d ArtifactKindDescriptor) Validate(index int) error {
	if _, ok := d.raw["name"].(string); !ok || d.Name() == "" {
		return fmt.Errorf("artifact_kinds[%d]: name required and must be a non-empty string", index)
	}
	if len(d.Name()) > artifactKindMaxNameLen {
		return fmt.Errorf("artifact_kinds[%d]: name %q is %d bytes, exceeds TaskLinks.artifact_type STRING(%d)", index, d.Name(), len(d.Name()), artifactKindMaxNameLen)
	}
	if v, ok := d.raw["absorbs"]; ok && v != nil {
		if _, isArr := v.([]interface{}); !isArr {
			return fmt.Errorf("artifact_kinds[%d] (name=%q): absorbs must be a JSON array when set", index, d.Name())
		}
	}
	return nil
}

type ArtifactKindRegistry interface {
	Resolve(ctx context.Context, name string) (ArtifactKindDescriptor, error)
	Canonicalize(ctx context.Context, literal string) (canonical string, found bool)
	All(ctx context.Context) ([]ArtifactKindDescriptor, error)
}

type fileArtifactKindRegistry struct {
	byName        map[string]ArtifactKindDescriptor
	canonicalizer map[string]string
}

func newArtifactKindRegistry(kinds []ArtifactKindDescriptor) (ArtifactKindRegistry, error) {
	r := &fileArtifactKindRegistry{
		byName:        make(map[string]ArtifactKindDescriptor, len(kinds)),
		canonicalizer: map[string]string{},
	}
	claimedBy := map[string]string{}
	for _, k := range kinds {
		name := k.Name()
		if _, dup := r.byName[name]; dup {
			return nil, fmt.Errorf("artifact_kinds: duplicate member name %q", name)
		}
		r.byName[name] = k
		if err := claimLiteral(claimedBy, name, name); err != nil {
			return nil, fmt.Errorf("artifact_kinds: %w", err)
		}
		r.canonicalizer[name] = name
		for _, lit := range k.Absorbs() {
			if err := claimLiteral(claimedBy, lit, name); err != nil {
				return nil, fmt.Errorf("artifact_kinds: %w", err)
			}
			r.canonicalizer[lit] = name
		}
	}
	return r, nil
}

func (r *fileArtifactKindRegistry) Resolve(_ context.Context, name string) (ArtifactKindDescriptor, error) {
	d, ok := r.byName[name]
	if !ok {
		return ArtifactKindDescriptor{}, fmt.Errorf("%w: %q (add to task-graph.config.json artifact_kinds, or set TASK_GRAPH_CONFIG)", ErrUnknownArtifactKind, name)
	}
	return d, nil
}

func (r *fileArtifactKindRegistry) Canonicalize(_ context.Context, literal string) (string, bool) {
	c, ok := r.canonicalizer[literal]
	return c, ok
}

func (r *fileArtifactKindRegistry) All(_ context.Context) ([]ArtifactKindDescriptor, error) {
	out := make([]ArtifactKindDescriptor, 0, len(r.byName))
	for _, d := range r.byName {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

// ---------------- shared helpers ----------------

// claimLiteral records that `literal` is claimed by canonical member `by`,
// refusing a second, different claim on the same literal. Used identically
// across all three registries' constructors to enforce the "no double-claim"
// property a Go map literal would give for free if the mapping lived in code
// instead of JSON config (per binary_is_never_sot it lives in config, so this
// check runs at registry-construction time instead of compile time).
func claimLiteral(claimedBy map[string]string, literal, by string) error {
	if existing, ok := claimedBy[literal]; ok && existing != by {
		return fmt.Errorf("literal %q claimed by both %q and %q — a legacy value may resolve to exactly one canonical member", literal, existing, by)
	}
	claimedBy[literal] = by
	return nil
}

func descOptString(raw map[string]interface{}, key string) string {
	if raw == nil {
		return ""
	}
	s, _ := raw[key].(string)
	return s
}

func descOptBool(raw map[string]interface{}, key string) bool {
	if raw == nil {
		return false
	}
	b, _ := raw[key].(bool)
	return b
}

func descOptStrings(raw map[string]interface{}, key string) []string {
	if raw == nil {
		return nil
	}
	v, ok := raw[key]
	if !ok || v == nil {
		return nil
	}
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ---------------- enforcement wiring helpers (fleet_plan_native.go call sites) ----------------

// requireTaskStatusRegistry / requireDependencyKindRegistry / requireArtifactKindRegistry
// fail closed (per VOLT fail_closed) rather than nil-panicking when an Env
// was constructed without the corresponding registry (e.g. an older test
// fixture or a config that carries no task_statuses/dependency_kinds/
// artifact_kinds section) — a missing registry must refuse every write, not
// silently accept anything.
func requireTaskStatusRegistry(env *Env) error {
	if env == nil || env.TaskStatuses == nil {
		return fmt.Errorf("%w: task_statuses registry not configured on this Env", ErrUnknownTaskStatus)
	}
	return nil
}

func requireDependencyKindRegistry(env *Env) error {
	if env == nil || env.DependencyKinds == nil {
		return fmt.Errorf("%w: dependency_kinds registry not configured on this Env", ErrUnknownDependencyKind)
	}
	return nil
}

func requireArtifactKindRegistry(env *Env) error {
	if env == nil || env.ArtifactKinds == nil {
		return fmt.Errorf("%w: artifact_kinds registry not configured on this Env", ErrUnknownArtifactKind)
	}
	return nil
}

// ---------------- readiness reporting (handleHealth) ----------------

// Registry-size keys reported by task_graph_health alongside the generic
// node_kinds / edge_labels / projections / substrates counts.
const (
	registrySizeKeyTaskStatuses    = "task_statuses"
	registrySizeKeyDependencyKinds = "dependency_kinds"
	registrySizeKeyArtifactKinds   = "artifact_kinds"
)

// fleetPlanNativeConfigured reports whether any CONFIGURED substrate declares
// the fleet_plan_native store. That is the only store whose write paths consult
// the three closed registries — every requireTaskStatusRegistry /
// requireDependencyKindRegistry / requireArtifactKindRegistry call site lives in
// fleet_plan_native.go. Read off SubstrateDescriptor.TaskStoreType, so this
// needs no adapter and performs no I/O.
func fleetPlanNativeConfigured(env *Env) bool {
	if env == nil {
		return false
	}
	for _, s := range env.Config.Substrates {
		if s.TaskStoreType == fleetPlanNativeStoreType {
			return true
		}
	}
	return false
}

// registryHealth reports the member count of each closed registry, plus the
// blocking issues that make the server structurally unable to accept a write.
//
// Sizes are sourced from the ENV registries rather than from Config so the
// reported number and the blocking verdict can never disagree: the write path
// consults env.<Registry>, so an Env whose registry is nil refuses every write
// no matter how many members Config declares, and must therefore report 0.
//
// The registries gate ONLY the fleet_plan_native write paths, so an unconfigured
// registry is write-blocking only when a fleet_plan_native substrate is
// configured. A schema_v1 deployment legitimately carries none — the config
// fields are `omitempty` and SpecGraphConfig.Validate imposes no non-empty rule
// on them — so a blanket "empty registry means not ready" would fail every such
// deployment rather than the one that actually cannot write. Sizes are reported
// unconditionally either way; only the blocking verdict is conditional.
//
// NOTE the production failure mode is an EMPTY registry, not a nil one: cmdMCP
// builds all three unconditionally and newTaskStatusRegistry(nil) returns a
// non-nil registry over an empty map, so a size test is what catches it. A
// nil-only test would report ready on exactly the config that refuses writes.
func registryHealth(ctx context.Context, env *Env) (map[string]int, []string) {
	type probe struct {
		key   string
		size  int
		err   error
		errIs error
	}
	probes := []probe{
		{key: registrySizeKeyTaskStatuses, errIs: ErrUnknownTaskStatus},
		{key: registrySizeKeyDependencyKinds, errIs: ErrUnknownDependencyKind},
		{key: registrySizeKeyArtifactKinds, errIs: ErrUnknownArtifactKind},
	}
	if env != nil {
		if env.TaskStatuses != nil {
			all, err := env.TaskStatuses.All(ctx)
			probes[0].size, probes[0].err = len(all), err
		}
		if env.DependencyKinds != nil {
			all, err := env.DependencyKinds.All(ctx)
			probes[1].size, probes[1].err = len(all), err
		}
		if env.ArtifactKinds != nil {
			all, err := env.ArtifactKinds.All(ctx)
			probes[2].size, probes[2].err = len(all), err
		}
	}

	sizes := make(map[string]int, len(probes))
	for _, p := range probes {
		sizes[p.key] = p.size
	}
	if !fleetPlanNativeConfigured(env) {
		return sizes, nil
	}

	var blocking []string
	for _, p := range probes {
		switch {
		case p.err != nil:
			blocking = append(blocking, fmt.Sprintf("registry %q: %s", p.key, p.err.Error()))
		case p.size == 0:
			blocking = append(blocking, fmt.Sprintf(
				"registry %q has no members but a %s substrate is configured: every write gated by it fails closed with %q (add %s to task-graph.config.json, or set TASK_GRAPH_CONFIG)",
				p.key, fleetPlanNativeStoreType, p.errIs.Error(), p.key))
		}
	}
	return sizes, blocking
}
