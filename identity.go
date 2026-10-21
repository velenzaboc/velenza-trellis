package main

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// identity.go implements the canonical identity grammar declared in
// OOD_GRAPH_PROTOCOL_v1 §3.symbol_identity_and_addressing:
//
//   - §3.section_path_grammar  →  SectionPath validation + normalization
//   - §3.version_coordinate_grammar  →  VersionCoordinate parse + serialize
//   - §3.graph_address_grammar  →  GraphAddress parse + serialize
//   - §3.graph_address_canonicalization  →  byte-equal round-trip property
//
// Every identity rule the binary enforces lives here. Materialization
// (Phase 5) consumes these helpers to derive stable graph ids and to
// canonicalize identity tuples before duplicate detection.

// ---------------- UUID validation ----------------

// uuidPattern matches the canonical 36-character lowercase UUID form per
// §3.identity_vocabulary.graph_object_id.uuid_form_required.
var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ValidateUUID returns nil if s is a canonical 36-character lowercase UUID.
// Uppercase, truncated, or non-hyphen-positioned UUIDs are rejected per
// §3.id_namespace_rules.uuid_case and §3.id_namespace_rules.uuid_truncation_allowed=false.
func ValidateUUID(s string) error {
	if !uuidPattern.MatchString(s) {
		return fmt.Errorf("invalid UUID %q: must be canonical 36-character lowercase form (8-4-4-4-12 hyphenated, lowercase hex)", s)
	}
	return nil
}

// ---------------- SectionPath ----------------

// snakeCaseKeyPattern is §3.section_path_grammar.snake_case_key_regex.
var snakeCaseKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// arrayIndexPattern is §3.section_path_grammar.array_index_regex.
// Leading zeros forbidden except for the literal "0".
var arrayIndexPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// SectionPathRoot is the canonical root marker per
// §3.section_path_grammar.root_symbol.
const SectionPathRoot = "$"

// ValidateSectionPath enforces §3.section_path_grammar against the input
// string. Returns nil for a well-formed path or an error naming the first
// rejection. Accepts the root "$", object segments ".<snake_case_key>",
// and array segments "[<zero_based_decimal_index>]". Trailing separators,
// empty segments, rendered headings, line numbers, and Markdown headings
// are all rejected per §3.section_path_grammar.normalization.
func ValidateSectionPath(p string) error {
	if p == "" {
		return fmt.Errorf("section_path empty: must start with %q root marker", SectionPathRoot)
	}
	if !strings.HasPrefix(p, SectionPathRoot) {
		return fmt.Errorf("section_path %q must start with %q root marker", p, SectionPathRoot)
	}
	rest := p[len(SectionPathRoot):]
	pos := len(SectionPathRoot)
	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			pos++
			// scan snake_case key up to next '.' or '['
			end := 0
			for end < len(rest) && rest[end] != '.' && rest[end] != '[' {
				end++
			}
			if end == 0 {
				return fmt.Errorf("section_path %q: empty object segment at pos %d", p, pos)
			}
			seg := rest[:end]
			if !snakeCaseKeyPattern.MatchString(seg) {
				return fmt.Errorf("section_path %q: segment %q at pos %d is not snake_case (must match %s)", p, seg, pos, snakeCaseKeyPattern.String())
			}
			rest = rest[end:]
			pos += end
		case '[':
			rest = rest[1:]
			pos++
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return fmt.Errorf("section_path %q: unterminated array segment at pos %d", p, pos)
			}
			idx := rest[:end]
			if !arrayIndexPattern.MatchString(idx) {
				return fmt.Errorf("section_path %q: array index %q at pos %d must be non-negative decimal without leading zero (except 0 itself)", p, idx, pos)
			}
			rest = rest[end+1:]
			pos += end + 1
		default:
			return fmt.Errorf("section_path %q: unexpected character %q at pos %d (segments must begin with '.' or '[')", p, rest[0], pos)
		}
	}
	return nil
}

// NormalizeSectionPath returns the canonical form of a section_path. The
// protocol's grammar is already byte-canonical when the input passes
// ValidateSectionPath, so normalization here is just validation; the
// function exists for symmetry with NormalizeGraphAddress and to give
// materialization a single canonical entrypoint.
func NormalizeSectionPath(p string) (string, error) {
	if err := ValidateSectionPath(p); err != nil {
		return "", err
	}
	return p, nil
}

// ---------------- VersionCoordinate ----------------

// VersionCoordinateKind enumerates the five forms in
// §3.version_coordinate_grammar.forms.
type VersionCoordinateKind string

const (
	VersionUnversioned        VersionCoordinateKind = "unversioned"
	VersionDocObjectLogical   VersionCoordinateKind = "docobject_logical"
	VersionDocObjectPinned    VersionCoordinateKind = "docobject_version"
	VersionOwnerSpec          VersionCoordinateKind = "owner_spec_version"
	VersionSourceNamed        VersionCoordinateKind = "source_named_version"
)

// VersionCoordinate is the parsed form of a §3.version_coordinate canonical
// string. The struct carries every input the binary may need to round-trip
// the coordinate via String().
type VersionCoordinate struct {
	Kind VersionCoordinateKind

	// DocObject* fields used when Kind is VersionDocObjectLogical
	// or VersionDocObjectPinned.
	DocObjectID string
	Version     int // 1-based; 0 means "not present" for VersionDocObjectLogical

	// OwnerSpec* fields used when Kind is VersionOwnerSpec.
	OwnerSpecID      string
	OwnerSpecVersion int

	// Source* fields used when Kind is VersionSourceNamed.
	SourceAuthorityID  string
	StableName         string // owner-spec-defined; percent-encoded on serialization
	SourceVersionToken string // owner-spec-defined; percent-encoded on serialization
}

// versionDocObjectPattern matches docobject:<doc_id>[@v<version>] per
// §3.version_coordinate_grammar.forms.docobject_logical and docobject_version.
var versionDocObjectPattern = regexp.MustCompile(`^docobject:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?:@v([1-9][0-9]*))?$`)

// versionOwnerSpecPattern matches spec:<owner_spec_id>@v<version>.
var versionOwnerSpecPattern = regexp.MustCompile(`^spec:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})@v([1-9][0-9]*)$`)

// versionSourcePattern matches source:<source_authority_id>:<stable_name>@<source_version_token>.
// stable_name and source_version_token are owner-spec-defined; this pattern
// captures the raw substrings and ParseVersionCoordinate percent-decodes
// them.
var versionSourcePattern = regexp.MustCompile(`^source:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):([^@]+)@(.+)$`)

// ParseVersionCoordinate accepts a canonical version_coordinate string
// per §3.version_coordinate_grammar and returns the structured form. Current
// and latest aliases are forbidden per §3.version_coordinate_grammar.normalization
// (current_keyword_allowed=false, latest_keyword_allowed=false).
func ParseVersionCoordinate(s string) (VersionCoordinate, error) {
	if s == "" {
		return VersionCoordinate{}, fmt.Errorf("version_coordinate empty")
	}
	if s == string(VersionUnversioned) {
		return VersionCoordinate{Kind: VersionUnversioned}, nil
	}
	if strings.HasPrefix(s, "docobject:") {
		m := versionDocObjectPattern.FindStringSubmatch(s)
		if m == nil {
			return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: invalid docobject form (expected docobject:<doc_id> or docobject:<doc_id>@v<n>)", s)
		}
		vc := VersionCoordinate{DocObjectID: m[1]}
		if m[2] == "" {
			vc.Kind = VersionDocObjectLogical
			vc.Version = 0
		} else {
			vc.Kind = VersionDocObjectPinned
			v, err := parsePositiveInt(m[2])
			if err != nil {
				return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: %w", s, err)
			}
			vc.Version = v
		}
		return vc, nil
	}
	if strings.HasPrefix(s, "spec:") {
		m := versionOwnerSpecPattern.FindStringSubmatch(s)
		if m == nil {
			return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: invalid spec form (expected spec:<owner_spec_id>@v<n>)", s)
		}
		v, err := parsePositiveInt(m[2])
		if err != nil {
			return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: %w", s, err)
		}
		return VersionCoordinate{
			Kind:             VersionOwnerSpec,
			OwnerSpecID:      m[1],
			OwnerSpecVersion: v,
		}, nil
	}
	if strings.HasPrefix(s, "source:") {
		m := versionSourcePattern.FindStringSubmatch(s)
		if m == nil {
			return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: invalid source form (expected source:<authority_id>:<stable_name>@<token>)", s)
		}
		name, err := url.PathUnescape(m[2])
		if err != nil {
			return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: percent-decode stable_name: %w", s, err)
		}
		token, err := url.PathUnescape(m[3])
		if err != nil {
			return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: percent-decode source_version_token: %w", s, err)
		}
		return VersionCoordinate{
			Kind:               VersionSourceNamed,
			SourceAuthorityID:  m[1],
			StableName:         name,
			SourceVersionToken: token,
		}, nil
	}
	// "current" / "latest" / arbitrary aliases reach here as no-match.
	return VersionCoordinate{}, fmt.Errorf("version_coordinate %q: unrecognized form (current/latest aliases forbidden per §3.version_coordinate_grammar.normalization)", s)
}

// String renders the VersionCoordinate back to its canonical string form.
// Percent-encoding for source_named coordinates uses uppercase hex per
// §3.graph_address_grammar.source_ref_encoding.
func (v VersionCoordinate) String() string {
	switch v.Kind {
	case VersionUnversioned:
		return string(VersionUnversioned)
	case VersionDocObjectLogical:
		return "docobject:" + v.DocObjectID
	case VersionDocObjectPinned:
		return fmt.Sprintf("docobject:%s@v%d", v.DocObjectID, v.Version)
	case VersionOwnerSpec:
		return fmt.Sprintf("spec:%s@v%d", v.OwnerSpecID, v.OwnerSpecVersion)
	case VersionSourceNamed:
		return fmt.Sprintf("source:%s:%s@%s",
			v.SourceAuthorityID,
			percentEncodeUpper(v.StableName),
			percentEncodeUpper(v.SourceVersionToken))
	default:
		return ""
	}
}

// ---------------- GraphAddress ----------------

// GraphAddress is the parsed form of a §3.graph_address canonical string.
// Round-trip: ParseGraphAddress(addr).String() == addr for any canonical
// input per §3.graph_address_canonicalization.equivalence_rule.
type GraphAddress struct {
	ObjectClass       string
	SourceAuthorityID string
	SourceRefKind     string // id | name | coordinate | path
	SourceRef         string // decoded; percent-encoded on serialization
	VersionCoordinate string // canonical version_coordinate string
	SectionPath       string // canonical section_path string
}

// String serializes the GraphAddress to its canonical form:
// "oodg://<class>/<authority>/<ref_kind>/<ref>?version=<vc>#<sp>"
// per §3.graph_address_grammar.canonical_form. Percent-encoding uses
// uppercase hex.
func (g GraphAddress) String() string {
	return fmt.Sprintf("oodg://%s/%s/%s/%s?version=%s#%s",
		g.ObjectClass,
		g.SourceAuthorityID,
		g.SourceRefKind,
		percentEncodeUpper(g.SourceRef),
		percentEncodeUpper(g.VersionCoordinate),
		percentEncodeFragmentUpper(g.SectionPath))
}

// graphAddressPattern matches the canonical scheme+authority+ref form.
// Query and fragment are parsed separately.
var graphAddressPathPattern = regexp.MustCompile(`^oodg://([a-z_][a-z0-9_]*)/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/(id|name|coordinate|path)/([^?#]+)$`)

// ParseGraphAddress accepts a canonical graph_address string and returns
// the structured form. Rejects non-uuid authority tokens, non-canonical
// object_class values (lowercase snake_case fail-closed), non-listed
// source_ref_kind values, missing version parameter, missing fragment,
// and any duplicate/extra query parameter per
// §3.graph_address_canonicalization.normalization_rules.
func ParseGraphAddress(s string) (GraphAddress, error) {
	if s == "" {
		return GraphAddress{}, fmt.Errorf("graph_address empty")
	}
	// Split fragment off first; everything before '#' is the path+query.
	hashIdx := strings.IndexByte(s, '#')
	if hashIdx < 0 {
		return GraphAddress{}, fmt.Errorf("graph_address %q: fragment required per §3.graph_address_grammar.fragment_required", s)
	}
	pathQuery := s[:hashIdx]
	fragEscaped := s[hashIdx+1:]

	// Split query off path.
	qIdx := strings.IndexByte(pathQuery, '?')
	if qIdx < 0 {
		return GraphAddress{}, fmt.Errorf("graph_address %q: version query parameter required per §3.graph_address_grammar.version_parameter_required", s)
	}
	pathPart := pathQuery[:qIdx]
	queryPart := pathQuery[qIdx+1:]

	// Parse path: oodg://<class>/<authority>/<ref_kind>/<ref>
	m := graphAddressPathPattern.FindStringSubmatch(pathPart)
	if m == nil {
		return GraphAddress{}, fmt.Errorf("graph_address %q: path part %q does not match oodg://<class>/<authority>/<ref_kind>/<ref> per §3.graph_address_grammar", s, pathPart)
	}
	refDecoded, err := url.PathUnescape(m[4])
	if err != nil {
		return GraphAddress{}, fmt.Errorf("graph_address %q: percent-decode source_ref: %w", s, err)
	}

	// Parse query: must be exactly "version=<value>" with no other params.
	if !strings.HasPrefix(queryPart, "version=") {
		return GraphAddress{}, fmt.Errorf("graph_address %q: query must be exactly version=<vc> per §3.graph_address_grammar.query_parameter_order", s)
	}
	versionEncoded := queryPart[len("version="):]
	if strings.ContainsRune(versionEncoded, '&') {
		return GraphAddress{}, fmt.Errorf("graph_address %q: additional query parameters forbidden per §3.graph_address_grammar.additional_query_parameters_allowed=false", s)
	}
	versionDecoded, err := url.QueryUnescape(versionEncoded)
	if err != nil {
		return GraphAddress{}, fmt.Errorf("graph_address %q: percent-decode version: %w", s, err)
	}
	// Validate the version_coordinate substring is well-formed (round-trip
	// through the §3 grammar).
	if _, err := ParseVersionCoordinate(versionDecoded); err != nil {
		return GraphAddress{}, fmt.Errorf("graph_address %q: %w", s, err)
	}

	// Parse fragment: must round-trip to a canonical section_path.
	fragDecoded, err := url.PathUnescape(fragEscaped)
	if err != nil {
		return GraphAddress{}, fmt.Errorf("graph_address %q: percent-decode fragment: %w", s, err)
	}
	if err := ValidateSectionPath(fragDecoded); err != nil {
		return GraphAddress{}, fmt.Errorf("graph_address %q: %w", s, err)
	}

	return GraphAddress{
		ObjectClass:       m[1],
		SourceAuthorityID: m[2],
		SourceRefKind:     m[3],
		SourceRef:         refDecoded,
		VersionCoordinate: versionDecoded,
		SectionPath:       fragDecoded,
	}, nil
}

// ---------------- helpers ----------------

// parsePositiveInt parses a positive base-10 integer string (no leading
// zeros except "0" itself; "0" rejected since the protocol's version field
// is "positive base-10 integer with no leading zero").
func parsePositiveInt(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty integer")
	}
	if s == "0" {
		return 0, fmt.Errorf("version must be positive, got 0")
	}
	if len(s) > 1 && s[0] == '0' {
		return 0, fmt.Errorf("version %q has leading zero", s)
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("version %q contains non-digit %q", s, r)
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

// percentEncodeUpper percent-encodes every byte that's not RFC3986
// unreserved, using uppercase hex escapes per
// §3.graph_address_grammar.source_ref_encoding.
func percentEncodeUpper(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUnreserved(c) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// percentEncodeFragmentUpper is the same as percentEncodeUpper but
// preserves the additional characters legal in URI fragments per RFC 3986
// (the protocol's §3.graph_address_grammar.fragment_value says "percent-
// encoded only for bytes not legal in URI fragments").
func percentEncodeFragmentUpper(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isFragmentSafe(c) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func isUnreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c == '-' || c == '.' || c == '_' || c == '~'
}

func isFragmentSafe(c byte) bool {
	// RFC 3986 fragment characters: unreserved / pct-encoded / sub-delims / ":" / "@" / "/" / "?"
	// section_path uses $ . [ ] which need preservation for readability.
	if isUnreserved(c) {
		return true
	}
	switch c {
	case '$', '.', '[', ']', '/', ':', '@', '?', '!', '&', '\'', '(', ')', '*', '+', ',', ';', '=':
		return true
	}
	return false
}
