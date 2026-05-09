package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRemainingJSONDocumentRemovesClickHouseTypedPaths(t *testing.T) {
	raw := []byte(`{"did":"did:plc:1","time_us":1732206349000167,"kind":"commit","commit":{"rev":"r1","operation":"create","collection":"app.bsky.feed.post","rkey":"k1","record":{"text":"hello"}}}`)
	encoded, err := remainingJSONDocument(raw, remainingShapeClickHouseTyped)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(encoded, &doc); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"did", "time_us", "kind"} {
		if _, ok := doc[key]; ok {
			t.Fatalf("top-level key %q was not removed from %s", key, encoded)
		}
	}
	commit, ok := doc["commit"].(map[string]any)
	if !ok {
		t.Fatalf("commit object missing or wrong type in %s", encoded)
	}
	for _, key := range []string{"operation", "collection"} {
		if _, ok := commit[key]; ok {
			t.Fatalf("commit key %q was not removed from %s", key, encoded)
		}
	}
	for _, key := range []string{"rev", "rkey", "record"} {
		if _, ok := commit[key]; !ok {
			t.Fatalf("commit key %q should remain in %s", key, encoded)
		}
	}
}

func TestRemainingBSONDocumentRemovesClickHouseTypedPaths(t *testing.T) {
	raw := []byte(`{"did":"did:plc:1","time_us":1732206349000167,"kind":"commit","commit":{"rev":"r1","operation":"create","collection":"app.bsky.feed.post","rkey":"k1","record":{"text":"hello"}}}`)
	encoded, err := remainingBSONDocument(raw, remainingShapeClickHouseTyped)
	if err != nil {
		t.Fatal(err)
	}
	doc := bson.Raw(encoded)
	for _, key := range []string{"did", "time_us", "kind"} {
		if value := doc.Lookup(key); value.Type != 0 {
			t.Fatalf("top-level key %q was not removed", key)
		}
	}
	commit := doc.Lookup("commit")
	if commit.Type != bson.TypeEmbeddedDocument {
		t.Fatalf("commit object missing or wrong type: %#v", commit)
	}
	commitDoc := commit.Document()
	for _, key := range []string{"operation", "collection"} {
		if value := commitDoc.Lookup(key); value.Type != 0 {
			t.Fatalf("commit key %q was not removed", key)
		}
	}
	for _, key := range []string{"rev", "rkey", "record"} {
		if value := commitDoc.Lookup(key); value.Type == 0 {
			t.Fatalf("commit key %q should remain", key)
		}
	}
}

func TestRemainingBSONDocumentPreservesIntegerNumbers(t *testing.T) {
	raw := []byte(`{"time_us":1732206349000167,"commit":{"record":{"reply_count":12,"score":1.5,"langs":["en"]}}}`)
	encoded, err := remainingBSONDocument(raw, remainingShapeConservative)
	if err != nil {
		t.Fatal(err)
	}
	doc := bson.Raw(encoded)
	if value := doc.Lookup("time_us"); value.Type != 0 {
		t.Fatal("time_us should be removed in conservative remaining shape")
	}
	record := doc.Lookup("commit").Document().Lookup("record").Document()
	replyCount := record.Lookup("reply_count")
	if replyCount.Type != bson.TypeInt64 {
		t.Fatalf("reply_count BSON type=%v want %v", replyCount.Type, bson.TypeInt64)
	}
	if got := replyCount.Int64(); got != 12 {
		t.Fatalf("reply_count=%d want 12", got)
	}
	score := record.Lookup("score")
	if score.Type != bson.TypeDouble {
		t.Fatalf("score BSON type=%v want %v", score.Type, bson.TypeDouble)
	}
}

func TestRemainingJSONDocumentConservativeOnlyRemovesTimeUS(t *testing.T) {
	raw := []byte(`{"did":"did:plc:1","time_us":1732206349000167,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`)
	encoded, err := remainingJSONDocument(raw, remainingShapeConservative)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(encoded, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["time_us"]; ok {
		t.Fatalf("time_us was not removed from %s", encoded)
	}
	for _, key := range []string{"did", "kind", "commit"} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("top-level key %q should remain in %s", key, encoded)
		}
	}
	commit := doc["commit"].(map[string]any)
	for _, key := range []string{"operation", "collection"} {
		if _, ok := commit[key]; !ok {
			t.Fatalf("commit key %q should remain in %s", key, encoded)
		}
	}
}

func TestMeasureRawJSONTreeDBSample(t *testing.T) {
	dbDir := t.TempDir()
	source := filepath.Join("..", "..", "testdata", "jsonbench_sample.jsonl")
	result, err := measureRawJSONTreeDB(context.Background(), []string{source}, 5, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 5 {
		t.Fatalf("rows=%d want 5", result.Rows)
	}
	if result.RawDocumentBytes == 0 {
		t.Fatal("raw document bytes were not recorded")
	}
	if result.AfterCompactBytes == 0 {
		t.Fatal("compacted raw TreeDB footprint was not recorded")
	}
	if !strings.Contains(result.StoredShape, "key/value") {
		t.Fatalf("stored shape %q does not describe raw key/value storage", result.StoredShape)
	}
	if err := validateRawJSONTreeDB([]string{source}, 5, dbDir); err != nil {
		t.Fatalf("validate raw JSON TreeDB: %v", err)
	}
}

func TestValidateRawJSONTreeDBDetectsMismatchedRows(t *testing.T) {
	dbDir := t.TempDir()
	source := filepath.Join("..", "..", "testdata", "jsonbench_sample.jsonl")
	if _, err := measureRawJSONTreeDB(context.Background(), []string{source}, 5, dbDir); err != nil {
		t.Fatal(err)
	}
	corrupt := filepath.Join(t.TempDir(), "corrupt.jsonl")
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), `"kind":"commit"`, `"kind":"changed"`, 1))
	if err := os.WriteFile(corrupt, data, 0o644); err != nil {
		t.Fatal(err)
	}
	err = validateRawJSONTreeDB([]string{corrupt}, 5, dbDir)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "validation mismatch") {
		t.Fatalf("validation error %q does not mention mismatch", err)
	}
}
