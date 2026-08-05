// mongo_gateway_compat_diff executes declared gateway fixture shapes against
// an in-process TreeDB gateway and an explicitly supplied MongoDB reference.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/compatdiff"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type diskFixture struct {
	Schema       string                 `json:"schema"`
	Version      int                    `json:"version"`
	ID           string                 `json:"id"`
	CapabilityID string                 `json:"capability_id"`
	Expectation  compatdiff.Expectation `json:"expectation"`
	Database     string                 `json:"database"`
	Collection   string                 `json:"collection"`
	Smoke        bool                   `json:"smoke"`
	Seed         []json.RawMessage      `json:"seed"`
	Command      json.RawMessage        `json:"command"`
	IgnoreFields []string               `json:"ignore_fields"`
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("mongo_gateway_compat_diff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fixturesDir := fs.String("fixtures", "cmd/mongo_gateway_compat_diff/fixtures", "fixture directory")
	outDir := fs.String("out", "", "artifact output directory (required)")
	referenceURI := fs.String("reference-uri", "", "pinned reference MongoDB URI (required)")
	smoke := fs.Bool("smoke", false, "run only fixtures marked smoke (all bundled fixtures are smoke)")
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
	result.ReferenceServerIdentity = referenceIdentity
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
		fixture := compatdiff.Fixture{Schema: disk.Schema, Version: disk.Version, ID: disk.ID, CapabilityID: disk.CapabilityID, Expectation: disk.Expectation, Database: disk.Database, Collection: disk.Collection, Smoke: disk.Smoke, IgnoreFields: disk.IgnoreFields}
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
		if err := fixture.Validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if !capabilityExpectationValid(fixture) {
			return nil, fmt.Errorf("%s: capability %q is absent from the manifest or expectation %q disagrees with its declared status", path, fixture.CapabilityID, fixture.Expectation)
		}
		if smokeOnly && !fixture.Smoke {
			continue
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

func (t target) Execute(ctx context.Context, fixture compatdiff.Fixture) (compatdiff.Observation, error) {
	uri, closeTarget, err := t.open(ctx)
	if err != nil {
		if t.reference {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: err}
		}
		return compatdiff.Observation{}, err
	}
	defer closeTarget()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetTimeout(20 * time.Second).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		if t.reference {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: err}
		}
		return compatdiff.Observation{}, err
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	if err := client.Ping(ctx, nil); err != nil {
		if t.reference {
			return compatdiff.Observation{}, compatdiff.ReferenceUnavailable{Err: err}
		}
		return compatdiff.Observation{}, err
	}
	if t.reference && t.identity != nil && *t.identity == "" {
		if buildInfo, err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Raw(); err == nil {
			version, _ := buildInfo.Lookup("version").StringValueOK()
			gitVersion, _ := buildInfo.Lookup("gitVersion").StringValueOK()
			*t.identity = strings.TrimSpace(version + " " + gitVersion)
		}
	}
	db := client.Database(safeName(fixture.Database) + "_compatdiff_" + safeName(fixture.ID))
	_ = db.Drop(ctx)
	coll := db.Collection(fixture.Collection)
	if len(fixture.Seed) > 0 {
		docs := make([]any, len(fixture.Seed))
		for i := range fixture.Seed {
			docs[i] = fixture.Seed[i]
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			return compatdiff.Observation{}, err
		}
	}
	observation := compatdiff.Observation{}
	if isCursorCommand(fixture.Command) {
		cursor, err := db.RunCommandCursor(ctx, fixture.Command)
		if err != nil {
			observation.Error = commandError(err)
		} else {
			var docs bson.A
			for cursor.Next(ctx) {
				docs = append(docs, cursor.Current)
			}
			if err := cursor.Err(); err != nil {
				observation.Error = commandError(err)
			} else {
				bytes, _ := bson.Marshal(bson.D{{Key: "ok", Value: int32(1)}, {Key: "cursor", Value: bson.D{{Key: "documents", Value: docs}}}})
				observation.Response = bytes
			}
			_ = cursor.Close(ctx)
		}
	} else {
		response, err := db.RunCommand(ctx, fixture.Command).Raw()
		if err != nil {
			observation.Error = commandError(err)
		} else {
			observation.Response = response
		}
	}
	state, err := snapshot(ctx, coll)
	if err != nil {
		return compatdiff.Observation{}, err
	}
	observation.State = state
	return observation, nil
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

func commandError(err error) *compatdiff.Error {
	var command mongo.CommandError
	if errors.As(err, &command) {
		return &compatdiff.Error{Code: command.Code, Labels: command.Labels, Message: command.Message}
	}
	return &compatdiff.Error{Message: err.Error()}
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
func safeName(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, value)
}

func snapshot(ctx context.Context, coll *mongo.Collection) ([]bson.Raw, error) {
	cursor, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []bson.Raw
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
	fmt.Fprintf(&report, "# TreeDB Mongo gateway differential result\n\nStatus: **%s**  \\nCapability identity: `%s`  \\nReference image: `%s`  \\nReference server identity: `%s`  \\nRuntime: %s\n\nError messages are recorded for diagnosis but equality deliberately compares error code and labels; BSON response/state evidence preserves type and field order.\n\n| Fixture | Capability | Expectation | Status | Reason |\n|---|---|---|---|---|\n", result.Status, result.CapabilityIdentity, result.ReferenceImage, result.ReferenceServerIdentity, result.Duration)
	for _, row := range result.Fixtures {
		fmt.Fprintf(&report, "| %s | %s | %s | %s | %s |\n", row.ID, row.CapabilityID, row.Expectation, row.Status, strings.ReplaceAll(row.Reason, "|", "\\|"))
	}
	return os.WriteFile(filepath.Join(out, "result.md"), []byte(report.String()), 0o644)
}
