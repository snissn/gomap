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
	Schema       string      `json:"schema"`
	Version      int         `json:"version"`
	ID           string      `json:"id"`
	CapabilityID string      `json:"capability_id"`
	Expectation  Expectation `json:"expectation"`
	Database     string      `json:"database"`
	Collection   string      `json:"collection"`
	Smoke        bool        `json:"smoke"`
	Seed         []bson.Raw  `json:"seed,omitempty"`
	Command      bson.Raw    `json:"command"`
	IgnoreFields []string    `json:"ignore_fields,omitempty"`
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
	seen := map[string]struct{}{}
	for _, field := range f.IgnoreFields {
		if field == "" || strings.HasPrefix(field, ".") || strings.HasSuffix(field, ".") || strings.Contains(field, "..") {
			return fmt.Errorf("fixture %q has invalid ignored field %q", f.ID, field)
		}
		if _, ok := seen[field]; ok {
			return fmt.Errorf("fixture %q repeats ignored field %q", f.ID, field)
		}
		seen[field] = struct{}{}
	}
	return nil
}

type Error struct {
	Code    int32    `json:"code,omitempty"`
	Labels  []string `json:"labels,omitempty"`
	Message string   `json:"message,omitempty"`
}

type Observation struct {
	Response bson.Raw   `json:"-"`
	Error    *Error     `json:"error,omitempty"`
	State    []bson.Raw `json:"-"`
}

type Executor interface {
	Execute(context.Context, Fixture) (Observation, error)
}

// ReferenceUnavailable marks a harness/infrastructure failure. It must never
// be reported as a compatibility difference.
type ReferenceUnavailable struct{ Err error }

func (e ReferenceUnavailable) Error() string {
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
	Response any    `json:"response,omitempty"`
	Error    *Error `json:"error,omitempty"`
	State    []any  `json:"state,omitempty"`
}

type Result struct {
	Schema                  string          `json:"schema"`
	Version                 int             `json:"version"`
	CapabilityIdentity      string          `json:"capability_identity"`
	ReferenceImage          string          `json:"reference_image"`
	ReferenceServerIdentity string          `json:"reference_server_identity,omitempty"`
	StartedAt               time.Time       `json:"started_at"`
	Duration                time.Duration   `json:"duration_ns"`
	Status                  string          `json:"status"`
	Fixtures                []FixtureResult `json:"fixtures"`
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
		row.TreeDB = normalizedObservation(treeObs, fixture.IgnoreFields)
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
		row.Reference = normalizedObservation(refObs, fixture.IgnoreFields)
		row.Status, row.Reason = compare(fixture, treeObs, refObs)
		row.Duration = time.Since(oneStarted)
		result.Fixtures = append(result.Fixtures, row)
	}
	result.Duration = time.Since(started)
	result.Status = "pass"
	for _, row := range result.Fixtures {
		if row.Status != "pass" {
			result.Status = row.Status
			break
		}
	}
	return result
}

func normalizedObservation(observation Observation, ignored []string) NormalizedObservation {
	result := NormalizedObservation{Error: observation.Error}
	if len(observation.Response) > 0 {
		result.Response = normalizeDocument(observation.Response, "", ignored)
	}
	if len(observation.State) > 0 {
		result.State = make([]any, len(observation.State))
		for i, doc := range observation.State {
			result.State[i] = normalizeDocument(doc, "", nil)
		}
	}
	return result
}

func compare(f Fixture, tree, reference Observation) (string, string) {
	if f.Expectation == ExpectedRejected {
		if tree.Error == nil {
			return "mismatch", "expected TreeDB rejection, got success"
		}
		if !sameDocuments(f.Seed, tree.State, nil) {
			return "mismatch", "expected-rejection fixture mutated TreeDB state"
		}
		return "pass", ""
	}
	if !sameError(tree.Error, reference.Error) {
		return "mismatch", fmt.Sprintf("error differs: TreeDB=%s reference=%s", describeError(tree.Error), describeError(reference.Error))
	}
	if tree.Error != nil {
		return "pass", ""
	}
	if !sameRaw(tree.Response, reference.Response, f.IgnoreFields) {
		return "mismatch", "normalized command response differs"
	}
	if !sameDocuments(tree.State, reference.State, nil) {
		return "mismatch", "post-command state differs"
	}
	return "pass", ""
}

func sameError(a, b *Error) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Code != b.Code {
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
	return fmt.Sprintf("%#v", normalizeDocument(a, "", ignored)) == fmt.Sprintf("%#v", normalizeDocument(b, "", ignored))
}

// normalizeDocument intentionally records BSON type bytes and original element
// order. It only omits exact, fixture-declared dotted paths.
func normalizeDocument(doc bson.Raw, prefix string, ignored []string) []any {
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
		out = append(out, []any{element.Key(), normalizeValue(element.Value(), path, ignored)})
	}
	return out
}

func normalizeValue(value bson.RawValue, path string, ignored []string) any {
	if doc, ok := value.DocumentOK(); ok {
		return []any{"document", normalizeDocument(doc, path, ignored)}
	}
	if arr, ok := value.ArrayOK(); ok {
		return []any{"array", normalizeDocument(bson.Raw(arr), path, ignored)}
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
