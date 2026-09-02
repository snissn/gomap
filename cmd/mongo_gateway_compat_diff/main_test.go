package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/compatdiff"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func TestBundledFixturesLoadAndSmokeSelectionIsReal(t *testing.T) {
	all, err := loadFixtures("fixtures", false)
	if err != nil {
		t.Fatal(err)
	}
	smoke, err := loadFixtures("fixtures", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 11 || len(smoke) != 3 {
		t.Fatalf("all=%d smoke=%d", len(all), len(smoke))
	}
}

func TestWriteArtifactsEmitsJSONMarkdownAndTSV(t *testing.T) {
	dir := t.TempDir()
	result := compatdiff.Result{Schema: compatdiff.ResultSchema, Version: compatdiff.ResultVersion, Status: "pass", TreeDBTransportMode: "plaintext-loopback", Fixtures: []compatdiff.FixtureResult{{ID: "case", CapabilityID: "wire.ping-command", Expectation: compatdiff.ExpectedSupported, Status: "pass"}}}
	if err := writeArtifacts(dir, result); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"result.json", "result.md", "result.tsv"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	markdown, err := os.ReadFile(filepath.Join(dir, "result.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(markdown), `\\n`) {
		t.Fatal("markdown contains literal newline escapes")
	}
	if !strings.Contains(string(markdown), "TreeDB transport: `plaintext-loopback`") {
		t.Fatalf("markdown missing TreeDB transport metadata: %s", markdown)
	}
	jsonArtifact, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(jsonArtifact), `"treedb_transport_mode": "plaintext-loopback"`) {
		t.Fatalf("JSON missing TreeDB transport metadata: %s", jsonArtifact)
	}
}

func TestFixtureCapabilityMustExistAndMatchExpectation(t *testing.T) {
	dir := t.TempDir()
	data := []byte(`{"schema":"treedb.mongo-gateway.compat-diff.fixture","version":1,"id":"bad","capability_id":"missing","expectation":"supported","database":"db","collection":"c","command":{"ping":1}}`)
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixtures(dir, false); err == nil {
		t.Fatal("missing capability was accepted")
	}
}

func TestFixtureCommandTargetMustMatchDeclaredCollection(t *testing.T) {
	dir := t.TempDir()
	data := []byte(`{"schema":"treedb.mongo-gateway.compat-diff.fixture","version":1,"id":"wrong-target","capability_id":"crud.find-by-id-equality","expectation":"supported","database":"db","collection":"declared","command":{"find":"other","filter":{"_id":"x"}}}`)
	if err := os.WriteFile(filepath.Join(dir, "wrong.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixtures(dir, false); err == nil {
		t.Fatal("mismatched command target was accepted")
	}
}

func TestClientOptionsDoNotInjectCommandMaxTimeMS(t *testing.T) {
	if options := clientOptions("mongodb://127.0.0.1:27017"); options.Timeout != nil {
		t.Fatalf("client timeout must be unset so fixtures own maxTimeMS: %v", *options.Timeout)
	}
}

func TestReferenceSeedFailureIsReferenceUnavailable(t *testing.T) {
	err := target{reference: true}.seedError(context.Background(), &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})
	var unavailable compatdiff.ReferenceUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("reference seed failure was not classified as unavailable: %T %v", err, err)
	}
}

func TestReferenceSeedHarnessDeadlineIsNotUnavailable(t *testing.T) {
	err := target{reference: true}.seedError(context.Background(), context.DeadlineExceeded)
	var unavailable compatdiff.ReferenceUnavailable
	if errors.As(err, &unavailable) {
		t.Fatalf("harness deadline was classified unavailable: %T %v", err, err)
	}
}

func TestReferenceSemanticSeedFailureIsNotUnavailable(t *testing.T) {
	err := target{reference: true}.seedError(context.Background(), mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "duplicate key"}}})
	var unavailable compatdiff.ReferenceUnavailable
	if errors.As(err, &unavailable) {
		t.Fatalf("deterministic reference seed error was classified unavailable: %T %v", err, err)
	}
}

func TestReferenceSeedServerSelectionDeadlineIsUnavailableWhenHarnessIsLive(t *testing.T) {
	err := target{reference: true}.seedError(context.Background(), topology.ServerSelectionError{Wrapped: context.DeadlineExceeded})
	var unavailable compatdiff.ReferenceUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("live-harness server selection error was not unavailable: %T %v", err, err)
	}
}

func TestReferenceSeedServerSelectionDeadlineIsHarnessErrorWhenHarnessExpired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := target{reference: true}.seedError(ctx, topology.ServerSelectionError{Wrapped: context.DeadlineExceeded})
	var unavailable compatdiff.ReferenceUnavailable
	if errors.As(err, &unavailable) {
		t.Fatalf("expired harness context was classified unavailable: %T %v", err, err)
	}
}

func TestCommandContextNeverCarriesHarnessDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	for range []bson.Raw{mustRaw(t, bson.D{{Key: "find", Value: "c"}}), mustRaw(t, bson.D{{Key: "count", Value: "c"}, {Key: "maxTimeMS", Value: int32(5)}})} {
		command, stop := commandContext(ctx)
		defer stop()
		if _, hasDeadline := command.Deadline(); hasDeadline {
			t.Fatal("command context carries harness deadline and can inject maxTimeMS")
		}
	}
}

func TestCommandErrorClassifiesWriteException(t *testing.T) {
	got, ok := commandError(mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "duplicate key"}}, Labels: []string{"RetryableWriteError"}})
	if !ok || got.Code != 11000 || !got.CommandRejection || got.Message != "duplicate key" || len(got.Labels) != 1 {
		t.Fatalf("write exception not classified: %#v, %v", got, ok)
	}
}

func TestCommandErrorClassifiesAllWriteAndConcernCodes(t *testing.T) {
	got, ok := commandError(mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "duplicate"}, {Code: 121, Message: "validation"}}, WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "concern"}})
	if !ok || got.Code != 11000 || strings.Join([]string{strconv.Itoa(int(got.Codes[0])), strconv.Itoa(int(got.Codes[1])), strconv.Itoa(int(got.Codes[2]))}, ",") != "11000,121,64" {
		t.Fatalf("write causes were lost: %#v, %v", got, ok)
	}
}

func TestCommandErrorClassifiesWriteConcernOnly(t *testing.T) {
	got, ok := commandError(mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "concern"}})
	if !ok || got.Code != 64 || len(got.Codes) != 1 || got.Codes[0] != 64 || !got.CommandRejection {
		t.Fatalf("write concern-only error was not classified: %#v, %v", got, ok)
	}
}

func mustRaw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	value, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
