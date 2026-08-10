package main

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalHNSWAttributionWriteGzipJSONLV1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queries.jsonl.gz")
	artifact, err := localHNSWAttributionWriteGzipJSONLV1(path, func(encoder *json.Encoder) (int, error) {
		for i := range 2 {
			if err := encoder.Encode(struct {
				Ordinal int `json:"ordinal"`
			}{Ordinal: i}); err != nil {
				return i, err
			}
		}
		return 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Schema != localHNSWAttributionSidecarSchemaV1 || artifact.Records != 2 || artifact.Bytes < 1 || !localHNSWAttributionSHA256V1(artifact.SHA256) {
		t.Fatalf("artifact=%+v", artifact)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	decoder := json.NewDecoder(r)
	for want := range 2 {
		var row struct {
			Ordinal int `json:"ordinal"`
		}
		if err := decoder.Decode(&row); err != nil || row.Ordinal != want {
			t.Fatalf("row=%+v err=%v", row, err)
		}
	}
	if _, err := localHNSWAttributionWriteGzipJSONLV1(path, func(*json.Encoder) (int, error) { return 1, nil }); err == nil {
		t.Fatal("expected exclusive publication failure")
	}
}
