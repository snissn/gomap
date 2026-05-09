package main

import (
	"encoding/json"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRemainingJSONDocumentRemovesClickHouseTypedPaths(t *testing.T) {
	raw := []byte(`{"did":"did:plc:1","time_us":1732206349000167,"kind":"commit","commit":{"rev":"r1","operation":"create","collection":"app.bsky.feed.post","rkey":"k1","record":{"text":"hello"}}}`)
	encoded, err := remainingJSONDocument(raw)
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
	encoded, err := remainingBSONDocument(raw)
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
