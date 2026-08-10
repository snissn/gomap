// mongo_gateway_compat_diff executes declared gateway fixture shapes against
// an in-process TreeDB gateway and an explicitly supplied MongoDB reference.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/compatdiff"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

type diskFixture struct {
	Schema                         string                 `json:"schema"`
	Version                        int                    `json:"version"`
	ID                             string                 `json:"id"`
	CapabilityID                   string                 `json:"capability_id"`
	Expectation                    compatdiff.Expectation `json:"expectation"`
	ExpectedErrorCode              int32                  `json:"expected_error_code,omitempty"`
	Database                       string                 `json:"database"`
	Collection                     string                 `json:"collection"`
	Smoke                          bool                   `json:"smoke"`
	Seed                           []json.RawMessage      `json:"seed"`
	Setup                          []json.RawMessage      `json:"setup"`
	Command                        json.RawMessage        `json:"command"`
	IgnoreFields                   []string               `json:"ignore_fields"`
	IgnoreStateFields              []string               `json:"ignore_state_fields"`
	NormalizeFields                []string               `json:"normalize_fields"`
	NormalizeResponseEnvelopeOrder bool                   `json:"normalize_response_envelope_order"`
	NormalizeCursorEnvelopeOrder   bool                   `json:"normalize_cursor_envelope_order"`
	NormalizeCursorNamespace       bool                   `json:"normalize_cursor_namespace"`
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("mongo_gateway_compat_diff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fixturesDir := fs.String("fixtures", "cmd/mongo_gateway_compat_diff/fixtures", "fixture directory")
	outDir := fs.String("out", "", "artifact output directory (required)")
	referenceURI := fs.String("reference-uri", "", "pinned reference MongoDB URI (required)")
	referenceImage := fs.String("reference-image", "external/unpinned", "reference image identity, if pinned")
	smoke := fs.Bool("smoke", false, "run only fixtures explicitly marked smoke")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *outDir == "" || *referenceURI == "" {
		fmt.Fprintln(stderr, "-out and -reference-uri are required; reference image is "+compatdiff.ReferenceImage)
		return 2
	}
	fixtures, err := loadFixtures(*fixturesDir, *smoke)
	if err != nil {
		fmt.Fprintln(stderr, "load fixtures:", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tree := target{uri: "", tree: true}
	referenceIdentity := ""
	reference := target{uri: *referenceURI, reference: true, identity: &referenceIdentity}
	result := compatdiff.Run(ctx, mongogateway.MongoGatewayCapabilityIdentity(), fixtures, tree, reference)
	result.ReferenceImage = *referenceImage
	result.ReferenceServerIdentity = referenceIdentity
	// target.open deliberately binds the TreeDB server to 127.0.0.1, so the
	// differential artifact makes its plaintext-loopback transport explicit.
	result.TreeDBTransportMode = "plaintext-loopback"
	if err := writeArtifacts(*outDir, result); err != nil {
		fmt.Fprintln(stderr, "write artifacts:", err)
		return 2
	}
	fmt.Fprintf(stdout, "compatibility differential result: %s (%d fixtures, %s)\n", result.Status, len(result.Fixtures), *outDir)
	if result.Status == "pass" {
		return 0
	}
	if result.Status == "reference-unavailable" {
		return 3
	}
	return 1
}

func loadFixtures(dir string, smokeOnly bool) ([]compatdiff.Fixture, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no *.json fixtures in %s", dir)
	}
	out := make([]compatdiff.Fixture, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var disk diskFixture
		if err := json.Unmarshal(data, &disk); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if smokeOnly && !disk.Smoke {
			continue
		}
		fixture := compatdiff.Fixture{Schema: disk.Schema, Version: disk.Version, ID: disk.ID, CapabilityID: disk.CapabilityID, Expectation: disk.Expectation, ExpectedErrorCode: disk.ExpectedErrorCode, Database: disk.Database, Collection: disk.Collection, Smoke: disk.Smoke, IgnoreFields: disk.IgnoreFields, IgnoreStateFields: disk.IgnoreStateFields, NormalizeFields: disk.NormalizeFields, NormalizeResponseEnvelopeOrder: disk.NormalizeResponseEnvelopeOrder, NormalizeCursorEnvelopeOrder: disk.NormalizeCursorEnvelopeOrder, NormalizeCursorNamespace: disk.NormalizeCursorNamespace}
		if fixture.Command, err = extJSON(disk.Command); err != nil {
			return nil, fmt.Errorf("%s command: %w", path, err)
		}
		for _, seed := range disk.Seed {
			value, err := extJSON(seed)
			if err != nil {
				return nil, fmt.Errorf("%s seed: %w", path, err)
			}
			fixture.Seed = append(fixture.Seed, value)
		}
		for _, setup := range disk.Setup {
			value, err := extJSON(setup)
			if err != nil {
				return nil, fmt.Errorf("%s setup: %w", path, err)
			}
			fixture.Setup = append(fixture.Setup, value)
		}
		if err := fixture.Validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if target, bound := commandCollection(fixture.Command); bound && target != fixture.Collection {
			return nil, fmt.Errorf("%s: command targets collection %q, fixture declares %q", path, target, fixture.Collection)
		}
		if !capabilityExpectationValid(fixture) {
			return nil, fmt.Errorf("%s: capability %q is absent from the manifest or expectation %q disagrees with its declared status", path, fixture.CapabilityID, fixture.Expectation)
		}
		out = append(out, fixture)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no selected fixtures in %s", dir)
	}
	return out, nil
}

func capabilityExpectationValid(fixture compatdiff.Fixture) bool {
	for _, capability := range mongogateway.MongoGatewayCapabilities().Capabilities {
		if capability.ID != fixture.CapabilityID {
			continue
		}
		if fixture.Expectation == compatdiff.ExpectedSupported {
			return capability.Status == mongogateway.MongoCapabilitySupported || capability.Status == mongogateway.MongoCapabilitySupportedSubset
		}
		return fixture.Expectation == compatdiff.ExpectedRejected && (capability.Status == mongogateway.MongoCapabilityRejected || capability.Status == mongogateway.MongoCapabilityNotImplemented)
	}
	return false
}

func extJSON(data []byte) (bson.Raw, error) {
	var value bson.Raw
	return value, bson.UnmarshalExtJSON(data, true, &value)
}

type target struct {
	uri             string
	tree, reference bool
	identity        *string
}

var databaseSequence atomic.Uint64

func (t target) Execute(ctx context.Context, fixture compatdiff.Fixture) (compatdiff.Observation, error) {
	if collection, bound := commandCollection(fixture.Command); bound && collection != fixture.Collection {
		return compatdiff.Observation{}, fmt.Errorf("command targets collection %q, fixture declares %q", collection, fixture.Collection)
	}
	uri, closeTarget, err := t.open(ctx)
	if err != nil {
		if t.reference {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: err}
		}
		return compatdiff.Observation{}, err
	}
	defer closeTarget()
	client, err := mongo.Connect(clientOptions(uri))
	if err != nil {
		if t.reference {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: err}
		}
		return compatdiff.Observation{}, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = client.Disconnect(cleanupCtx)
	}()
	if err := client.Ping(ctx, nil); err != nil {
		if t.reference {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: err}
		}
		return compatdiff.Observation{}, err
	}
	if t.reference && t.identity != nil && *t.identity == "" {
		buildInfo, err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Raw()
		if err != nil {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: fmt.Errorf("reference buildInfo: %w", err)}
		}
		version, _ := buildInfo.Lookup("version").StringValueOK()
		gitVersion, _ := buildInfo.Lookup("gitVersion").StringValueOK()
		*t.identity = strings.TrimSpace(version + " " + gitVersion)
		if *t.identity == "" {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: errors.New("reference buildInfo did not contain version identity")}
		}
	}
	db := client.Database(uniqueDatabaseName(fixture))
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = db.Drop(cleanupCtx)
	}()
	coll := db.Collection(fixture.Collection)
	if len(fixture.Seed) > 0 {
		docs := make([]any, len(fixture.Seed))
		for i := range fixture.Seed {
			docs[i] = fixture.Seed[i]
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			return compatdiff.Observation{}, t.seedError(ctx, err)
		}
	}
	for _, setup := range fixture.Setup {
		if _, err := db.RunCommand(ctx, setup).Raw(); err != nil {
			return compatdiff.Observation{}, t.seedError(ctx, err)
		}
	}
	baseline, err := snapshotDatabase(ctx, db)
	if err != nil {
		return compatdiff.Observation{}, t.executionError(err)
	}
	observation := compatdiff.Observation{Baseline: baseline}
	commandCtx, stopCommand := commandContext(ctx)
	defer stopCommand()
	response, err := db.RunCommand(commandCtx, fixture.Command).Raw()
	if err != nil {
		if command, ok := commandError(err); ok {
			observation.Error = command
		} else {
			return compatdiff.Observation{}, t.executionError(err)
		}
	} else {
		observation.Response = response
		if isCursorCommand(fixture.Command) {
			replies, cursorError, err := drainCursorReplies(commandCtx, db, response)
			if err != nil {
				return compatdiff.Observation{}, t.executionError(err)
			}
			observation.CursorReplies = replies
			observation.CursorError = cursorError
		}
	}
	state, err := snapshotDatabase(ctx, db)
	if err != nil {
		return compatdiff.Observation{}, t.executionError(err)
	}
	observation.State = state
	return observation, nil
}

func drainCursorReplies(ctx context.Context, db *mongo.Database, initial bson.Raw) ([]bson.Raw, *compatdiff.Error, error) {
	cursor, ok := initial.Lookup("cursor").DocumentOK()
	if !ok {
		return nil, nil, nil
	}
	id, ok := cursor.Lookup("id").AsInt64OK()
	namespace, _ := cursor.Lookup("ns").StringValueOK()
	_, collection, found := strings.Cut(namespace, ".")
	if !found || collection == "" {
		return nil, nil, fmt.Errorf("cursor response has invalid namespace %q", namespace)
	}
	if !ok || id == 0 {
		return nil, nil, nil
	}
	const maxGetMoreReplies = 128
	var replies []bson.Raw
	for range maxGetMoreReplies {
		reply, err := db.RunCommand(ctx, bson.D{{Key: "getMore", Value: id}, {Key: "collection", Value: collection}}).Raw()
		if err != nil {
			if command, ok := commandError(err); ok {
				return replies, command, nil
			}
			return replies, nil, err
		}
		replies = append(replies, reply)
		cursor, ok = reply.Lookup("cursor").DocumentOK()
		if !ok {
			return replies, nil, errors.New("getMore response omitted cursor")
		}
		id, ok = cursor.Lookup("id").AsInt64OK()
		if !ok {
			return replies, nil, errors.New("getMore response omitted cursor id")
		}
		if id == 0 {
			return replies, nil, nil
		}
	}
	return replies, nil, fmt.Errorf("cursor exceeded %d getMore replies", maxGetMoreReplies)
}

func clientOptions(uri string) *options.ClientOptions {
	// A client-wide Timeout is encoded as maxTimeMS by the Mongo driver. Fixtures
	// own command-level maxTimeMS, so use the run context for the harness bound.
	return options.Client().ApplyURI(uri).SetServerSelectionTimeout(3 * time.Second)
}

func commandContext(ctx context.Context) (context.Context, func()) {
	// A deadline makes the driver append maxTimeMS. Preserve the fixture wire
	// command by using a cancelable context without a deadline; propagate the
	// harness watchdog cancellation with AfterFunc.
	commandCtx, cancel := context.WithCancel(context.Background())
	stop := context.AfterFunc(ctx, cancel)
	return commandCtx, func() { stop(); cancel() }
}

func (t target) open(ctx context.Context) (string, func(), error) {
	if !t.tree {
		return t.uri, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "compatdiff-treedb-")
	if err != nil {
		return "", nil, err
	}
	standalone, err := mongogateway.OpenStandaloneServer(mongogateway.StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALDurable})
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = standalone.Close()
		_ = os.RemoveAll(dir)
		return "", nil, err
	}
	serveCtx, cancel := context.WithCancel(ctx)
	go func() { _ = standalone.Serve(serveCtx, ln) }()
	return "mongodb://" + ln.Addr().String() + "/?directConnection=true", func() { cancel(); _ = standalone.Close(); _ = os.RemoveAll(dir) }, nil
}

func (t target) executionError(err error) error {
	if t.reference {
		return compatdiff.ReferenceUnavailable{Err: err}
	}
	return err
}

// seedError preserves only reference infrastructure failures. A deterministic
// MongoDB seed rejection makes the fixture setup invalid; it is not comparison
// evidence and must not be presented as a retryable missing-reference error.
func (t target) seedError(ctx context.Context, err error) error {
	if !t.reference || !referenceTransportError(ctx, err) {
		return err
	}
	return t.executionError(err)
}

func referenceTransportError(ctx context.Context, err error) bool {
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return false
	}
	if mongo.IsNetworkError(err) {
		return true
	}
	var selection topology.ServerSelectionError
	if errors.As(err, &selection) {
		return true
	}
	var operation *net.OpError
	return errors.As(err, &operation)
}

func commandError(err error) (*compatdiff.Error, bool) {
	var command mongo.CommandError
	if errors.As(err, &command) {
		return &compatdiff.Error{Code: command.Code, Labels: command.Labels, Message: command.Message, CommandRejection: true}, true
	}
	var write mongo.WriteException
	if errors.As(err, &write) {
		codes := make([]int32, 0, len(write.WriteErrors)+1)
		message := ""
		for _, item := range write.WriteErrors {
			codes = append(codes, int32(item.Code))
			if message == "" {
				message = item.Message
			}
		}
		if write.WriteConcernError != nil {
			codes = append(codes, int32(write.WriteConcernError.Code))
			if message == "" {
				message = write.WriteConcernError.Message
			}
		}
		if len(codes) > 0 {
			return &compatdiff.Error{Code: codes[0], Codes: codes, Labels: write.Labels, Message: message, CommandRejection: true}, true
		}
	}
	return nil, false
}

func uniqueDatabaseName(fixture compatdiff.Fixture) string {
	return safeName(fixture.Database) + "_compatdiff_" + safeName(fixture.ID) + "_" + strconv.FormatInt(time.Now().UnixNano(), 36) + "_" + strconv.FormatUint(databaseSequence.Add(1), 36)
}
func isCursorCommand(command bson.Raw) bool {
	elements, err := command.Elements()
	if err != nil || len(elements) == 0 {
		return false
	}
	switch elements[0].Key() {
	case "find", "aggregate", "listCollections", "listIndexes":
		return true
	}
	return false
}

func commandCollection(command bson.Raw) (string, bool) {
	elements, err := command.Elements()
	if err != nil || len(elements) == 0 {
		return "", false
	}
	switch elements[0].Key() {
	case "find", "aggregate", "count", "distinct", "insert", "update", "delete", "createIndexes", "dropIndexes", "listIndexes":
		name, ok := elements[0].Value().StringValueOK()
		return name, ok
	default:
		return "", false
	}
}
func safeName(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, value)
}

func snapshotDatabase(ctx context.Context, db *mongo.Database) ([]bson.Raw, error) {
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var state []bson.Raw
	for _, name := range names {
		part, err := snapshotCollection(ctx, db.Collection(name))
		if err != nil {
			return nil, err
		}
		state = append(state, part...)
	}
	return state, nil
}

func snapshotCollection(ctx context.Context, coll *mongo.Collection) ([]bson.Raw, error) {
	indexes, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer indexes.Close(ctx)
	var indexDocs bson.A
	for indexes.Next(ctx) {
		indexDocs = append(indexDocs, append(bson.Raw(nil), indexes.Current...))
	}
	if err := indexes.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(indexDocs, func(i, j int) bool {
		left, _ := indexDocs[i].(bson.Raw).Lookup("name").StringValueOK()
		right, _ := indexDocs[j].(bson.Raw).Lookup("name").StringValueOK()
		return left < right
	})
	metadata, err := bson.Marshal(bson.D{{Key: "_compatdiff_metadata", Value: bson.D{{Key: "collection", Value: coll.Name()}, {Key: "indexes", Value: indexDocs}}}})
	if err != nil {
		return nil, err
	}
	cursor, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	docs := []bson.Raw{metadata}
	for cursor.Next(ctx) {
		docs = append(docs, append(bson.Raw(nil), cursor.Current...))
	}
	return docs, cursor.Err()
}

func writeArtifacts(out string, result compatdiff.Result) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "result.json"), append(data, '\n'), 0o644); err != nil {
		return err
	}
	var report strings.Builder
	fmt.Fprintf(&report, "# TreeDB Mongo gateway differential result\n\nStatus: **%s**  \nCapability identity: `%s`  \nTreeDB transport: `%s`  \nReference image: `%s`  \nReference server identity: `%s`  \nRuntime: %s\n\nError messages are recorded for diagnosis but equality deliberately compares error code and labels; BSON response/state evidence preserves type and field order.\n\n| Fixture | Capability | Expectation | Status | Reason |\n|---|---|---|---|---|\n", result.Status, result.CapabilityIdentity, result.TreeDBTransportMode, result.ReferenceImage, result.ReferenceServerIdentity, result.Duration)
	for _, row := range result.Fixtures {
		fmt.Fprintf(&report, "| %s | %s | %s | %s | %s |\n", row.ID, row.CapabilityID, row.Expectation, row.Status, strings.ReplaceAll(row.Reason, "|", "\\|"))
	}
	if err := os.WriteFile(filepath.Join(out, "result.md"), []byte(report.String()), 0o644); err != nil {
		return err
	}
	var tsv strings.Builder
	writer := csv.NewWriter(&tsv)
	writer.Comma = '\t'
	if err := writer.Write([]string{"id", "capability_id", "expectation", "status", "duration_ns", "reason"}); err != nil {
		return err
	}
	for _, row := range result.Fixtures {
		if err := writer.Write([]string{row.ID, row.CapabilityID, string(row.Expectation), row.Status, strconv.FormatInt(row.Duration.Nanoseconds(), 10), row.Reason}); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "result.tsv"), []byte(tsv.String()), 0o644)
}
