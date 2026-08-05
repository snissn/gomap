package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/compatdiff"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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
	if len(all) < 8 || len(smoke) != 3 {
		t.Fatalf("all=%d smoke=%d", len(all), len(smoke))
	}
}

func TestWriteArtifactsEmitsJSONMarkdownAndTSV(t *testing.T) {
	dir := t.TempDir()
	result := compatdiff.Result{Schema: compatdiff.ResultSchema, Version: compatdiff.ResultVersion, Status: "pass", Fixtures: []compatdiff.FixtureResult{{ID: "case", CapabilityID: "wire.ping-command", Expectation: compatdiff.ExpectedSupported, Status: "pass"}}}
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

func TestCommandContextNeverCarriesHarnessDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	for _, command := range []bson.Raw{mustRaw(t, bson.D{{Key: "find", Value: "c"}}), mustRaw(t, bson.D{{Key: "count", Value: "c"}, {Key: "maxTimeMS", Value: int32(5)}})} {
		if _, hasDeadline := commandContext(ctx, command).Deadline(); hasDeadline {
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

func mustRaw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	value, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
