// Package compatdiff runs narrowly-scoped Mongo gateway differential fixtures.
// It deliberately compares declared gateway shapes, not MongoDB conformance.
package compatdiff

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	FixtureSchema  = "treedb.mongo-gateway.compat-diff.fixture"
	FixtureVersion = 1
	ResultSchema   = "treedb.mongo-gateway.compat-diff.result"
	ResultVersion  = 1
	ReferenceImage = "mongo:7.0.14"
)

type Expectation string

const (
	ExpectedSupported Expectation = "supported"
	ExpectedRejected  Expectation = "rejected"
)

// Fixture is a decoded, versioned one-command compatibility case. The on-disk
// representation is canonical Extended JSON; the runner decodes it to BSON Raw
// before execution so type and field-order evidence is retained.
type Fixture struct {
	Schema            string      `json:"schema"`
	Version           int         `json:"version"`
	ID                string      `json:"id"`
	CapabilityID      string      `json:"capability_id"`
	Expectation       Expectation `json:"expectation"`
	ExpectedErrorCode int32       `json:"expected_error_code,omitempty"`
	Database          string      `json:"database"`
	Collection        string      `json:"collection"`
	Smoke             bool        `json:"smoke"`
	Seed              []bson.Raw  `json:"seed,omitempty"`
	// Setup is a bounded, ordered pre-command sequence (normally createIndexes)
	// run independently against both targets before the baseline snapshot.
	Setup                          []bson.Raw `json:"setup,omitempty"`
	Command                        bson.Raw   `json:"command"`
	IgnoreFields                   []string   `json:"ignore_fields,omitempty"`
	IgnoreStateFields              []string   `json:"ignore_state_fields,omitempty"`
	NormalizeFields                []string   `json:"normalize_fields,omitempty"`
	NormalizeResponseEnvelopeOrder bool       `json:"normalize_response_envelope_order,omitempty"`
	NormalizeCursorEnvelopeOrder   bool       `json:"normalize_cursor_envelope_order,omitempty"`
	NormalizeCursorNamespace       bool       `json:"normalize_cursor_namespace,omitempty"`
}

func (f Fixture) Validate() error {
	if f.Schema != FixtureSchema || f.Version != FixtureVersion {
		return fmt.Errorf("fixture %q has schema/version %q/v%d; want %q/v%d", f.ID, f.Schema, f.Version, FixtureSchema, FixtureVersion)
	}
	if f.ID == "" || f.CapabilityID == "" || f.Database == "" || f.Collection == "" || len(f.Command) == 0 {
		return fmt.Errorf("fixture requires id, capability_id, database, collection, and command")
	}
	if f.Expectation != ExpectedSupported && f.Expectation != ExpectedRejected {
		return fmt.Errorf("fixture %q has invalid expectation %q", f.ID, f.Expectation)
	}
	if f.Expectation == ExpectedRejected && f.ExpectedErrorCode == 0 {
		return fmt.Errorf("fixture %q requires expected_error_code for rejected expectation", f.ID)
	}
	seen := map[string]struct{}{}
	for _, fields := range [][]string{f.IgnoreFields, f.IgnoreStateFields, f.NormalizeFields} {
		for _, field := range fields {
			if field == "" || strings.HasPrefix(field, ".") || strings.HasSuffix(field, ".") || strings.Contains(field, "..") {
				return fmt.Errorf("fixture %q has invalid ignored field %q", f.ID, field)
			}
			if _, ok := seen[field]; ok {
				return fmt.Errorf("fixture %q repeats ignored field %q", f.ID, field)
			}
			seen[field] = struct{}{}
		}
	}
	return nil
}

type Error struct {
	Code             int32    `json:"code,omitempty"`
	Codes            []int32  `json:"codes,omitempty"`
	Labels           []string `json:"labels,omitempty"`
	Message          string   `json:"message,omitempty"`
	CommandRejection bool     `json:"command_rejection,omitempty"`
}

type Observation struct {
	Response      bson.Raw   `json:"-"`
	CursorReplies []bson.Raw `json:"-"`
	CursorError   *Error     `json:"cursor_error,omitempty"`
	Error         *Error     `json:"error,omitempty"`
	Baseline      []bson.Raw `json:"-"`
	State         []bson.Raw `json:"-"`
}

type Executor interface {
	Execute(context.Context, Fixture) (Observation, error)
}

// ReferenceUnavailable marks a harness/infrastructure failure. It must never
// be reported as a compatibility difference.
type ReferenceUnavailable struct{ Err error }

func (e ReferenceUnavailable) Error() string {
	if e.Err == nil {
		return "reference MongoDB unavailable"
	}
	return "reference MongoDB unavailable: " + e.Err.Error()
}
func (e ReferenceUnavailable) Unwrap() error { return e.Err }

type FixtureResult struct {
	ID           string                `json:"id"`
	CapabilityID string                `json:"capability_id"`
	Expectation  Expectation           `json:"expectation"`
	Status       string                `json:"status"`
	Reason       string                `json:"reason,omitempty"`
	Duration     time.Duration         `json:"duration_ns"`
	TreeDB       NormalizedObservation `json:"treedb"`
	Reference    NormalizedObservation `json:"reference"`
}

// NormalizedObservation is emitted so a mismatch is attributable without
// hiding BSON type/order evidence. Error messages are recorded but intentionally
// excluded from equality: fixture semantics compare error code and labels.
type NormalizedObservation struct {
	Response      any    `json:"response,omitempty"`
	CursorReplies []any  `json:"cursor_replies,omitempty"`
	CursorError   *Error `json:"cursor_error,omitempty"`
	Error         *Error `json:"error,omitempty"`
	Baseline      []any  `json:"baseline,omitempty"`
	State         []any  `json:"state,omitempty"`
}

type Result struct {
	Schema                  string `json:"schema"`
	Version                 int    `json:"version"`
	CapabilityIdentity      string `json:"capability_identity"`
	ReferenceImage          string `json:"reference_image"`
	ReferenceServerIdentity string `json:"reference_server_identity,omitempty"`
	// TreeDBTransportMode records the effective listener mode of the TreeDB
	// target for this artifact. It is environment evidence, not a capability
	// claim, so callers set it when they own the listener lifecycle.
	TreeDBTransportMode string          `json:"treedb_transport_mode,omitempty"`
	StartedAt           time.Time       `json:"started_at"`
	Duration            time.Duration   `json:"duration_ns"`
	Status              string          `json:"status"`
	Fixtures            []FixtureResult `json:"fixtures"`
}

func Run(ctx context.Context, capabilityIdentity string, fixtures []Fixture, tree, reference Executor) Result {
	started := time.Now().UTC()
	result := Result{Schema: ResultSchema, Version: ResultVersion, CapabilityIdentity: capabilityIdentity, ReferenceImage: ReferenceImage, StartedAt: started}
	for _, fixture := range fixtures {
		oneStarted := time.Now()
		row := FixtureResult{ID: fixture.ID, CapabilityID: fixture.CapabilityID, Expectation: fixture.Expectation}
		if err := fixture.Validate(); err != nil {
			row.Status, row.Reason = "harness-error", err.Error()
			row.Duration = time.Since(oneStarted)
			result.Fixtures = append(result.Fixtures, row)
			continue
		}
		treeObs, err := tree.Execute(ctx, fixture)
		if err != nil {
			row.Status, row.Reason = "harness-error", "TreeDB execution: "+err.Error()
			row.Duration = time.Since(oneStarted)
			result.Fixtures = append(result.Fixtures, row)
			continue
		}
		row.TreeDB = normalizedObservation(treeObs, fixture.IgnoreFields, fixture.IgnoreStateFields, fixture.NormalizeFields, fixture.NormalizeResponseEnvelopeOrder, fixture.NormalizeCursorEnvelopeOrder, fixture.NormalizeCursorNamespace)
		refObs, err := reference.Execute(ctx, fixture)
		if err != nil {
			var unavailable ReferenceUnavailable
			if errors.As(err, &unavailable) {
				row.Status, row.Reason = "reference-unavailable", unavailable.Error()
			} else {
				row.Status, row.Reason = "harness-error", "reference execution: "+err.Error()
			}
			row.Duration = time.Since(oneStarted)
			result.Fixtures = append(result.Fixtures, row)
			continue
		}
		row.Reference = normalizedObservation(refObs, fixture.IgnoreFields, fixture.IgnoreStateFields, fixture.NormalizeFields, fixture.NormalizeResponseEnvelopeOrder, fixture.NormalizeCursorEnvelopeOrder, fixture.NormalizeCursorNamespace)
		row.Status, row.Reason = compare(fixture, treeObs, refObs)
		row.Duration = time.Since(oneStarted)
		result.Fixtures = append(result.Fixtures, row)
	}
	result.Duration = time.Since(started)
	result.Status = "pass"
	for _, row := range result.Fixtures {
		if statusPriority(row.Status) > statusPriority(result.Status) {
			result.Status = row.Status
		}
	}
	return result
}

func statusPriority(status string) int {
	switch status {
	case "harness-error":
		return 3
	case "mismatch":
		return 2
	case "reference-unavailable":
		return 1
	default:
		return 0
	}
}

func normalizedObservation(observation Observation, ignoredResponse, ignoredState, normalized []string, normalizeResponseEnvelopeOrder, normalizeCursorEnvelopeOrder, normalizeCursorNamespace bool) NormalizedObservation {
	result := NormalizedObservation{Error: observation.Error, CursorError: observation.CursorError}
	tokens := newNormalizationTokens()
	tokens.normalizeCursorNamespace = normalizeCursorNamespace
	if len(observation.Response) > 0 {
		result.Response = normalizeResponseWithTokens(observation.Response, ignoredResponse, normalized, normalizeResponseEnvelopeOrder, normalizeCursorEnvelopeOrder, tokens)
	}
	if len(observation.CursorReplies) > 0 {
		result.CursorReplies = make([]any, len(observation.CursorReplies))
		for i, reply := range observation.CursorReplies {
			result.CursorReplies[i] = normalizeResponseWithTokens(reply, ignoredResponse, normalized, normalizeResponseEnvelopeOrder, normalizeCursorEnvelopeOrder, tokens)
		}
	}
	// cursor.ns is wire metadata only. State documents may legitimately contain
	// a user field with that path, so preserve it verbatim in mutation evidence.
	tokens.normalizeCursorNamespace = false
	if len(observation.Baseline) > 0 {
		result.Baseline = make([]any, len(observation.Baseline))
		for i, doc := range observation.Baseline {
			result.Baseline[i] = normalizeDocumentWithTokens(doc, "", ignoredState, normalized, tokens)
		}
	}
	if len(observation.State) > 0 {
		result.State = make([]any, len(observation.State))
		for i, doc := range observation.State {
			result.State[i] = normalizeDocumentWithTokens(doc, "", ignoredState, normalized, tokens)
		}
	}
	return result
}

func compare(f Fixture, tree, reference Observation) (string, string) {
	if f.Expectation == ExpectedRejected {
		if tree.Error == nil {
			return "mismatch", "expected TreeDB rejection, got success"
		}
		if !tree.Error.CommandRejection {
			return "harness-error", "expected TreeDB command rejection, got execution failure"
		}
		if tree.Error.Code != f.ExpectedErrorCode {
			return "mismatch", fmt.Sprintf("expected TreeDB rejection code=%d, got %s", f.ExpectedErrorCode, describeError(tree.Error))
		}
		if reference.Error != nil || !responseOK(reference.Response) {
			return "harness-error", fmt.Sprintf("expected reference execution, got error: %s", describeError(reference.Error))
		}
		if !sameDocuments(tree.Baseline, tree.State, nil) {
			return "mismatch", "expected-rejection fixture mutated TreeDB state"
		}
		return "pass", ""
	}
	if !sameError(tree.Error, reference.Error) {
		return "mismatch", fmt.Sprintf("error differs: TreeDB=%s reference=%s", describeError(tree.Error), describeError(reference.Error))
	}
	treeComparable, referenceComparable := normalizedComparableObservation(tree, f), normalizedComparableObservation(reference, f)
	if tree.Error != nil {
		if !sameNormalizedValues(treeComparable.State, referenceComparable.State) {
			return "mismatch", "post-command state differs after matching error"
		}
		return "pass", ""
	}
	if hasCursorTranscript(tree, reference) {
		if !sameNormalizedValues(treeComparable.Response, referenceComparable.Response) || !sameNormalizedValues(treeComparable.CursorReplies, referenceComparable.CursorReplies) || !sameError(tree.CursorError, reference.CursorError) {
			return "mismatch", "cursor reply sequence differs"
		}
	} else if !sameNormalizedValues(treeComparable.Response, referenceComparable.Response) {
		return "mismatch", "normalized command response differs"
	}
	if !hasCursorTranscript(tree, reference) && (!sameNormalizedValues(treeComparable.CursorReplies, referenceComparable.CursorReplies) || !sameError(tree.CursorError, reference.CursorError)) {
		return "mismatch", "cursor reply sequence differs"
	}
	if !sameNormalizedValues(treeComparable.State, referenceComparable.State) {
		return "mismatch", "post-command state differs"
	}
	return "pass", ""
}

func hasCursorTranscript(tree, reference Observation) bool {
	return len(tree.CursorReplies) > 0 || len(reference.CursorReplies) > 0 || tree.Response.Lookup("cursor").Type != 0 || reference.Response.Lookup("cursor").Type != 0
}

func sameCursorTranscript(tree, reference Observation, f Fixture) bool {
	return fmt.Sprintf("%#v", normalizeCursorTranscript(tree, f)) == fmt.Sprintf("%#v", normalizeCursorTranscript(reference, f))
}

func normalizeCursorTranscript(observation Observation, f Fixture) []any {
	tokens := newNormalizationTokens()
	tokens.normalizeCursorNamespace = f.NormalizeCursorNamespace
	result := []any{normalizeResponseWithTokens(observation.Response, f.IgnoreFields, f.NormalizeFields, f.NormalizeResponseEnvelopeOrder, f.NormalizeCursorEnvelopeOrder, tokens)}
	for _, reply := range observation.CursorReplies {
		result = append(result, normalizeResponseWithTokens(reply, f.IgnoreFields, f.NormalizeFields, f.NormalizeResponseEnvelopeOrder, f.NormalizeCursorEnvelopeOrder, tokens))
	}
	result = append(result, comparableError(observation.CursorError))
	return result
}

// normalizedComparableObservation intentionally uses one token table for the
// complete target observation. This preserves relationships such as an
// generated _id returned by a command also appearing in the post-state.
func normalizedComparableObservation(observation Observation, f Fixture) NormalizedObservation {
	return normalizedObservation(observation, f.IgnoreFields, f.IgnoreStateFields, f.NormalizeFields, f.NormalizeResponseEnvelopeOrder, f.NormalizeCursorEnvelopeOrder, f.NormalizeCursorNamespace)
}

func sameNormalizedValues(left, right any) bool {
	return fmt.Sprintf("%#v", left) == fmt.Sprintf("%#v", right)
}

func comparableError(err *Error) any {
	if err == nil {
		return nil
	}
	labels := append([]string(nil), err.Labels...)
	sort.Strings(labels)
	return []any{err.Code, err.Codes, labels}
}

func responseOK(raw bson.Raw) bool {
	ok, found := raw.Lookup("ok").AsInt64OK()
	return found && ok == 1
}

func sameError(a, b *Error) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Code != b.Code {
		return false
	}
	if fmt.Sprint(a.Codes) != fmt.Sprint(b.Codes) {
		return false
	}
	ax, bx := append([]string(nil), a.Labels...), append([]string(nil), b.Labels...)
	sort.Strings(ax)
	sort.Strings(bx)
	return strings.Join(ax, "\x00") == strings.Join(bx, "\x00")
}

func describeError(err *Error) string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("code=%d labels=%v", err.Code, err.Labels)
}

func sameDocuments(a, b []bson.Raw, ignored []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameRaw(a[i], b[i], ignored) {
			return false
		}
	}
	return true
}

func sameRaw(a, b bson.Raw, ignored []string) bool {
	return sameRawWithNormalization(a, b, ignored, nil)
}

func sameRawWithNormalization(a, b bson.Raw, ignored, normalized []string) bool {
	return fmt.Sprintf("%#v", normalizeDocument(a, "", ignored, normalized)) == fmt.Sprintf("%#v", normalizeDocument(b, "", ignored, normalized))
}

func sameResponseWithNormalization(a, b bson.Raw, ignored, normalized []string, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder bool) bool {
	return fmt.Sprintf("%#v", normalizeResponse(a, ignored, normalized, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder)) == fmt.Sprintf("%#v", normalizeResponse(b, ignored, normalized, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder))
}

// normalizeResponse can canonicalize only the command reply envelope. BSON
// order remains significant in every nested document, cursor payload, and state
// snapshot; fixtures must opt in because the top-level reply layout is transport
// metadata rather than a query/result ordering contract.
func normalizeResponse(raw bson.Raw, ignored, normalized []string, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder bool) any {
	return normalizeResponseWithTokens(raw, ignored, normalized, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder, newNormalizationTokens())
}

func normalizeResponseWithTokens(raw bson.Raw, ignored, normalized []string, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder bool, tokens *normalizationTokens) any {
	value := normalizeDocumentWithTokens(raw, "", ignored, normalized, tokens)
	if normalizeEnvelopeOrder {
		sortPairs(value)
	}
	if normalizeCursorEnvelopeOrder {
		for _, item := range value {
			pair := item.([]any)
			if pair[0] != "cursor" {
				continue
			}
			cursor, ok := pair[1].([]any)
			if ok && len(cursor) == 2 && cursor[0] == "document" {
				sortPairs(cursor[1].([]any))
			}
		}
	}
	return value
}

func sortPairs(pairs []any) {
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].([]any)[0].(string) < pairs[j].([]any)[0].(string) })
}

func sameCursorRepliesWithNormalization(a, b []bson.Raw, ignored, normalized []string, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameResponseWithNormalization(a[i], b[i], ignored, normalized, normalizeEnvelopeOrder, normalizeCursorEnvelopeOrder) {
			return false
		}
	}
	return true
}

func sameDocumentsWithNormalization(a, b []bson.Raw, ignored, normalized []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameRawWithNormalization(a[i], b[i], ignored, normalized) {
			return false
		}
	}
	return true
}

// normalizeDocument intentionally records BSON type bytes and original element
// order. It only omits exact, fixture-declared dotted paths.
func normalizeDocument(doc bson.Raw, prefix string, ignored, normalized []string) []any {
	return normalizeDocumentWithTokens(doc, prefix, ignored, normalized, newNormalizationTokens())
}

type normalizationTokens struct {
	values                   map[string]int
	next                     int
	normalizeCursorNamespace bool
}

func newNormalizationTokens() *normalizationTokens {
	return &normalizationTokens{values: make(map[string]int)}
}

func (t *normalizationTokens) token(value bson.RawValue) int {
	key := fmt.Sprintf("%d:%s", value.Type, base64.StdEncoding.EncodeToString(value.Value))
	if token, ok := t.values[key]; ok {
		return token
	}
	t.next++
	t.values[key] = t.next
	return t.next
}

func normalizeDocumentWithTokens(doc bson.Raw, prefix string, ignored, normalized []string, tokens *normalizationTokens) []any {
	elements, err := doc.Elements()
	if err != nil {
		return []any{[]any{"invalid", base64.StdEncoding.EncodeToString(doc)}}
	}
	out := make([]any, 0, len(elements))
	for _, element := range elements {
		path := element.Key()
		if prefix != "" {
			path = prefix + "." + path
		}
		if contains(ignored, path) {
			continue
		}
		if contains(normalized, path) {
			if path == "cursor.id" && cursorIDIsClosed(element.Value()) {
				out = append(out, []any{element.Key(), []any{"normalized-cursor-id", byte(element.Value().Type), "closed"}})
				continue
			}
			out = append(out, []any{element.Key(), []any{"normalized", byte(element.Value().Type), tokens.token(element.Value())}})
			continue
		}
		if tokens.normalizeCursorNamespace && path == "cursor.ns" && element.Value().Type == bson.TypeString {
			ns, _ := element.Value().StringValueOK()
			_, suffix, found := strings.Cut(ns, ".")
			if found && suffix != "" {
				out = append(out, []any{element.Key(), []any{"normalized-cursor-namespace", byte(element.Value().Type), suffix}})
				continue
			}
		}
		out = append(out, []any{element.Key(), normalizeValueWithTokens(element.Value(), path, ignored, normalized, tokens)})
	}
	return out
}

func cursorIDIsClosed(value bson.RawValue) bool {
	if id, ok := value.AsInt64OK(); ok {
		return id == 0
	}
	if id, ok := value.Int32OK(); ok {
		return id == 0
	}
	return false
}

func normalizeValue(value bson.RawValue, path string, ignored, normalized []string) any {
	return normalizeValueWithTokens(value, path, ignored, normalized, newNormalizationTokens())
}
func normalizeValueWithTokens(value bson.RawValue, path string, ignored, normalized []string, tokens *normalizationTokens) any {
	if doc, ok := value.DocumentOK(); ok {
		return []any{"document", normalizeDocumentWithTokens(doc, path, ignored, normalized, tokens)}
	}
	if arr, ok := value.ArrayOK(); ok {
		return []any{"array", normalizeDocumentWithTokens(bson.Raw(arr), path, ignored, normalized, tokens)}
	}
	return []any{byte(value.Type), base64.StdEncoding.EncodeToString(value.Value)}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
