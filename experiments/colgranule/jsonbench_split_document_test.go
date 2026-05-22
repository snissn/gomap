package colgranule

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestJSONBenchSplitDocumentParityWithColumnPartImage(t *testing.T) {
	source := "testdata/jsonbench_sample.jsonl"
	ds, err := LoadJSONBenchColumns(source, 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	part, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, 2, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: ds.Dictionaries})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	imagePart, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}

	file, err := os.Open(source)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	reversed := ReverseJSONBenchDictionaries(ds.Dictionaries)
	partScanner := imagePart.NewScanner()
	rawScanner := bufio.NewScanner(file)
	rawScanner.Buffer(make([]byte, 0, 256<<10), 8<<20)
	row := int64(0)
	for rawScanner.Scan() {
		raw := bytes.TrimSpace(rawScanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		retainedBytes, err := JSONBenchRetainedDocument(raw)
		if err != nil {
			t.Fatalf("JSONBenchRetainedDocument(row=%d): %v", row, err)
		}
		var retained map[string]any
		if err := DecodeJSONDocumentPreserveNumbers(retainedBytes, &retained); err != nil {
			t.Fatalf("Decode retained row=%d: %v", row, err)
		}
		assertJSONBenchDeclaredPathsRemoved(t, row, retained)
		values, err := JSONBenchDeclaredColumnValuesFromPart(partScanner, imagePart, reversed, row)
		if err != nil {
			t.Fatalf("JSONBenchDeclaredColumnValuesFromPart(row=%d): %v", row, err)
		}
		RestoreJSONBenchDeclaredColumns(retained, values)

		var original map[string]any
		if err := DecodeJSONDocumentPreserveNumbers(raw, &original); err != nil {
			t.Fatalf("Decode original row=%d: %v", row, err)
		}
		got := mustCanonicalJSON(t, retained)
		want := mustCanonicalJSON(t, original)
		if string(got) != string(want) {
			t.Fatalf("row %d split document parity mismatch\ngot  %s\nwant %s", row, got, want)
		}
		row++
	}
	if err := rawScanner.Err(); err != nil {
		t.Fatalf("scan source: %v", err)
	}
	if row != int64(ds.Rows) {
		t.Fatalf("validated rows=%d want %d", row, ds.Rows)
	}
}

func assertJSONBenchDeclaredPathsRemoved(t *testing.T, row int64, doc map[string]any) {
	t.Helper()
	for _, key := range []string{"did", "time_us", "kind"} {
		if _, ok := doc[key]; ok {
			t.Fatalf("row %d retained document still has top-level declared column %q", row, key)
		}
	}
	commit, ok := doc["commit"].(map[string]any)
	if !ok {
		return
	}
	for _, key := range []string{"operation", "collection"} {
		if _, ok := commit[key]; ok {
			t.Fatalf("row %d retained document still has commit declared column %q", row, key)
		}
	}
}

func mustCanonicalJSON(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
