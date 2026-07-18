package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/experiments/colgranule"
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

func TestWriteMarkdownRawTreeDBJSONReclaimText(t *testing.T) {
	raw := comparisonRaw{
		DataPath:   "fixture.json.gz",
		Limit:      1,
		Rows:       1,
		Files:      []string{"fixture.json.gz"},
		InputBytes: 1,
		ClickHouseLocal: clickHouseResult{
			TotalSize: 100,
			DataSize:  90,
			IndexSize: 10,
		},
		RawTreeDBJSON: remainingTreeDBResult{
			Enabled:              true,
			BeforeCompactBytes:   10,
			BeforeCompactFiles:   1,
			AfterCompactBytes:    5,
			AfterCompactFiles:    1,
			RewriteSourceBytes:   4,
			RewriteReclaimFiles:  1,
			RewriteReclaimBytes:  4,
			RawDocumentBytes:     10,
			CompactedBytesPerRow: 5,
		},
	}
	path := filepath.Join(t.TempDir(), "report.md")
	writeMarkdown(path, raw)
	md, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(md)
	if !strings.Contains(text, "Value-log rewrite reclaimed the observed source segments reported above") {
		t.Fatalf("markdown did not describe reclaimed raw TreeDB JSON sources:\n%s", text)
	}
	if strings.Contains(text, "stayed in the measured directory footprint") {
		t.Fatalf("markdown printed stale retention caveat despite reclaimed sources:\n%s", text)
	}

	raw.RawTreeDBJSON.RewriteReclaimFiles = 0
	raw.RawTreeDBJSON.RewriteReclaimBytes = 0
	writeMarkdown(path, raw)
	md, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(md), "stayed in the measured directory footprint") {
		t.Fatalf("markdown did not preserve retention caveat when source bytes were not reclaimed:\n%s", md)
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

func TestRestoreClickHouseTypedPathsLeavesMissingCommitAbsent(t *testing.T) {
	doc := map[string]any{
		"identity": map[string]any{"handle": "example.test"},
	}
	restoreClickHouseTypedPathsForTest(doc, typedJSONBenchValues{
		TimeUS:           1732206349164509,
		Did:              "did:plc:identity",
		Kind:             "identity",
		CommitOperation:  "",
		CommitCollection: "",
	})
	if _, ok := doc["commit"]; ok {
		t.Fatalf("identity row should not materialize empty commit object: %+v", doc["commit"])
	}
}

func TestRemainingPayloadPlusImageColumnsReconstructsJSONBenchRows(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "jsonbench_sample.jsonl")
	validateRemainingPayloadPlusImageColumnsReconstructsJSONBenchRows(t, source, 0, 2)
}

func TestRemainingPayloadPlusImageColumnsReconstructsLocalJSONBenchIfRequested(t *testing.T) {
	source, limit := localJSONBenchFullReconstructionSource(t)
	validateRemainingPayloadPlusImageColumnsReconstructsJSONBenchRows(t, source, limit, colgranule.DefaultRowsPerGranule)
}

func validateRemainingPayloadPlusImageColumnsReconstructsJSONBenchRows(t *testing.T, source string, limit int, rowsPerGranule int) {
	t.Helper()
	ds, err := colgranule.LoadJSONBenchColumns(source, limit)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	part, err := colgranule.BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, rowsPerGranule, colgranule.JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout: %v", err)
	}
	image, err := colgranule.BuildColumnPartImage(part, colgranule.ColumnPartImageOptions{Dictionaries: ds.Dictionaries})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := colgranule.ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	imagePart, err := colgranule.ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}

	reverseDictionaries := reverseJSONBenchDictionaries(ds.Dictionaries)
	scanner := imagePart.NewScanner()
	remainingEncoders := map[string]func([]byte) ([]byte, error){
		"json": func(raw []byte) ([]byte, error) {
			return remainingJSONDocument(raw, remainingShapeClickHouseTyped)
		},
		"bson": func(raw []byte) ([]byte, error) {
			return remainingBSONDocument(raw, remainingShapeClickHouseTyped)
		},
	}

	for name, encodeRemaining := range remainingEncoders {
		t.Run(name, func(t *testing.T) {
			row := int64(0)
			if err := scanJSONBenchFile(source, func(raw []byte) error {
				if row >= int64(ds.Rows) {
					return errStopScan
				}
				remaining, err := encodeRemaining(raw)
				if err != nil {
					return err
				}
				retained, err := decodeRetainedDocumentForTest(name, remaining)
				if err != nil {
					return err
				}
				typed, err := typedJSONBenchValuesFromImage(scanner, imagePart, reverseDictionaries, row)
				if err != nil {
					return err
				}
				restoreClickHouseTypedPathsForTest(retained, typed)

				var original map[string]any
				if err := decodeJSONPreserveNumbers(raw, &original); err != nil {
					return err
				}
				if got, want := canonicalJSONForTest(t, retained), canonicalJSONForTest(t, original); string(got) != string(want) {
					t.Fatalf("row %d reconstructed %s mismatch\ngot  %s\nwant %s", row, name, got, want)
				}
				row++
				return nil
			}); err != nil {
				t.Fatalf("scan %s: %v", name, err)
			}
			if row != int64(ds.Rows) {
				t.Fatalf("validated rows=%d want %d", row, ds.Rows)
			}
		})
	}
}

func TestRemainingTemplateV1PayloadPlusImageColumnsReconstructsJSONBenchRows(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "jsonbench_sample.jsonl")
	validateRemainingTemplateV1PayloadPlusImageColumnsReconstructsJSONBenchRows(t, source, 0, 2)
}

func TestRemainingTemplateV1MeasurementStorageReconstructsJSONBenchRows(t *testing.T) {
	source := filepath.Join("..", "..", "testdata", "jsonbench_sample.jsonl")
	validateRemainingTemplateV1MeasurementStorageReconstructsJSONBenchRows(t, source, 0, 2)
}

func TestRemainingTemplateV1MeasurementStorageReconstructsLocalJSONBenchIfRequested(t *testing.T) {
	source, limit := localJSONBenchFullReconstructionSource(t)
	validateRemainingTemplateV1MeasurementStorageReconstructsJSONBenchRows(t, source, limit, colgranule.DefaultRowsPerGranule)
}

func TestRemainingRewriteEvidenceAllowsDurableRootDeferredReclaim(t *testing.T) {
	// Exact counters from the macOS failure in run 29445375387. The rewrite
	// completed, but its source segments remained reachable from the older
	// independently recoverable durable-root slot.
	result := remainingTreeDBResult{
		RewriteRecordsCopied: 18,
		RewriteValueBytes:    1843,
		RewriteSourceBytes:   1600,
		RewriteReclaimFiles:  0,
		RewriteReclaimBytes:  0,
	}
	if err := validateRemainingRewriteEvidence(result); err != nil {
		t.Fatalf("durable-root deferred reclaim rejected: %v", err)
	}
}

func TestRemainingRewriteEvidenceRejectsInvalidCounters(t *testing.T) {
	valid := remainingTreeDBResult{
		RewriteRecordsCopied: 1,
		RewriteValueBytes:    128,
		RewriteSourceBytes:   256,
	}
	for _, tc := range []struct {
		name   string
		mutate func(*remainingTreeDBResult)
	}{
		{name: "no records copied", mutate: func(r *remainingTreeDBResult) { r.RewriteRecordsCopied = 0 }},
		{name: "no value bytes copied", mutate: func(r *remainingTreeDBResult) { r.RewriteValueBytes = 0 }},
		{name: "no source bytes selected", mutate: func(r *remainingTreeDBResult) { r.RewriteSourceBytes = 0 }},
		{name: "negative reclaim files", mutate: func(r *remainingTreeDBResult) { r.RewriteReclaimFiles = -1 }},
		{name: "negative reclaim bytes", mutate: func(r *remainingTreeDBResult) { r.RewriteReclaimBytes = -1 }},
		{name: "inconsistent reclaim", mutate: func(r *remainingTreeDBResult) { r.RewriteReclaimFiles = 1 }},
		{name: "over reclaim", mutate: func(r *remainingTreeDBResult) {
			r.RewriteReclaimFiles = 1
			r.RewriteReclaimBytes = 257
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := valid
			tc.mutate(&result)
			if err := validateRemainingRewriteEvidence(result); err == nil {
				t.Fatalf("invalid rewrite counters accepted: %+v", result)
			}
		})
	}
}

func validateRemainingTemplateV1PayloadPlusImageColumnsReconstructsJSONBenchRows(t *testing.T, source string, limit int, rowsPerGranule int) {
	t.Helper()
	ds, err := colgranule.LoadJSONBenchColumns(source, limit)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	part, err := colgranule.BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, rowsPerGranule, colgranule.JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout: %v", err)
	}
	image, err := colgranule.BuildColumnPartImage(part, colgranule.ColumnPartImageOptions{Dictionaries: ds.Dictionaries})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := colgranule.ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	imagePart, err := colgranule.ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open TreeDB: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := collections.NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "remaining",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatTemplateV1,
		},
	}); err != nil {
		t.Fatalf("create remaining collection: %v", err)
	}
	col, err := mgr.OpenCollection("remaining")
	if err != nil {
		t.Fatalf("open remaining collection: %v", err)
	}

	var ids [][]byte
	var docs [][]byte
	var storedIDs [][]byte
	var encoder collections.TemplateV1Encoder
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		resultIDs, err := col.InsertBatchWithTemplateV1Encoder(ids, docs, &encoder)
		if err != nil {
			return err
		}
		for _, id := range resultIDs {
			storedIDs = append(storedIDs, append([]byte(nil), id...))
		}
		ids = ids[:0]
		docs = docs[:0]
		return nil
	}
	row := uint64(1)
	if err := scanJSONBenchFile(source, func(raw []byte) error {
		if row > uint64(ds.Rows) {
			return errStopScan
		}
		doc, err := remainingDocument(raw, collections.DocumentFormatTemplateV1, remainingShapeClickHouseTyped)
		if err != nil {
			return err
		}
		ids = append(ids, documentID(row))
		docs = append(docs, doc)
		row++
		if len(ids) >= 64 {
			return flush()
		}
		return nil
	}); err != nil {
		t.Fatalf("prepare template-v1 retained docs: %v", err)
	}
	if err := flush(); err != nil {
		t.Fatalf("insert template-v1 retained docs: %v", err)
	}
	if len(storedIDs) != ds.Rows {
		t.Fatalf("stored template-v1 ids=%d want %d", len(storedIDs), ds.Rows)
	}

	reverseDictionaries := reverseJSONBenchDictionaries(ds.Dictionaries)
	scanner := imagePart.NewScanner()
	rowIndex := int64(0)
	if err := scanJSONBenchFile(source, func(raw []byte) error {
		if rowIndex >= int64(ds.Rows) {
			return errStopScan
		}
		stored, err := col.Get(storedIDs[int(rowIndex)])
		if err != nil {
			return err
		}
		retainedJSON, err := col.StoredDocumentJSON(stored)
		if err != nil {
			return err
		}
		retained, err := decodeRetainedDocumentForTest("json", retainedJSON)
		if err != nil {
			return err
		}
		typed, err := typedJSONBenchValuesFromImage(scanner, imagePart, reverseDictionaries, rowIndex)
		if err != nil {
			return err
		}
		restoreClickHouseTypedPathsForTest(retained, typed)

		var original map[string]any
		if err := decodeJSONPreserveNumbers(raw, &original); err != nil {
			return err
		}
		if got, want := canonicalJSONForTest(t, retained), canonicalJSONForTest(t, original); string(got) != string(want) {
			t.Fatalf("row %d reconstructed template-v1 mismatch\ngot  %s\nwant %s", rowIndex, got, want)
		}
		rowIndex++
		return nil
	}); err != nil {
		t.Fatalf("validate template-v1 retained docs: %v", err)
	}
	if rowIndex != int64(ds.Rows) {
		t.Fatalf("validated rows=%d want %d", rowIndex, ds.Rows)
	}
}

func validateRemainingTemplateV1MeasurementStorageReconstructsJSONBenchRows(t *testing.T, source string, limit int, rowsPerGranule int) {
	t.Helper()
	ds, imagePart := buildJSONBenchImagePartForReconstructionTest(t, source, limit, rowsPerGranule)
	dbDir := t.TempDir()
	result, err := measureRemainingTreeDB(context.Background(), []string{source}, ds.Rows, dbDir, collections.DocumentFormatTemplateV1, remainingShapeClickHouseTyped)
	if err != nil {
		t.Fatalf("measure template-v1 remaining TreeDB: %v", err)
	}
	if result.Rows != ds.Rows {
		t.Fatalf("measured rows=%d want %d", result.Rows, ds.Rows)
	}
	if err := validateRemainingRewriteEvidence(result); err != nil {
		t.Fatalf("remaining measurement value-log rewrite evidence: %v: %+v", err, result)
	}
	// Rewrite reclamation is reported only for files physically deleted during
	// this pass. An ingest segment may remain reachable from the independently
	// recoverable older durable-root slot until later publications supersede it.
	validateStoredRemainingCollectionReconstructsJSONBenchRows(t, source, ds, imagePart, dbDir, collections.DocumentFormatTemplateV1)
}

func validateRemainingRewriteEvidence(result remainingTreeDBResult) error {
	if result.RewriteRecordsCopied <= 0 {
		return fmt.Errorf("copied no value-log records")
	}
	if result.RewriteValueBytes <= 0 {
		return fmt.Errorf("copied no value-log bytes")
	}
	if result.RewriteSourceBytes <= 0 {
		return fmt.Errorf("selected no value-log source bytes")
	}
	if result.RewriteReclaimFiles < 0 || result.RewriteReclaimBytes < 0 {
		return fmt.Errorf("reported negative reclaim counters: files=%d bytes=%d", result.RewriteReclaimFiles, result.RewriteReclaimBytes)
	}
	if (result.RewriteReclaimFiles == 0) != (result.RewriteReclaimBytes == 0) {
		return fmt.Errorf("reported inconsistent reclaim counters: files=%d bytes=%d", result.RewriteReclaimFiles, result.RewriteReclaimBytes)
	}
	if result.RewriteReclaimBytes > result.RewriteSourceBytes {
		return fmt.Errorf("reclaimed %d bytes from %d selected source bytes", result.RewriteReclaimBytes, result.RewriteSourceBytes)
	}
	return nil
}

func buildJSONBenchImagePartForReconstructionTest(t *testing.T, source string, limit int, rowsPerGranule int) (*colgranule.JSONBenchDataset, *colgranule.ColumnPart) {
	t.Helper()
	ds, err := colgranule.LoadJSONBenchColumns(source, limit)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	part, err := colgranule.BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, rowsPerGranule, colgranule.JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout: %v", err)
	}
	image, err := colgranule.BuildColumnPartImage(part, colgranule.ColumnPartImageOptions{Dictionaries: ds.Dictionaries})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := colgranule.ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	imagePart, err := colgranule.ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	return &ds, imagePart
}

func validateStoredRemainingCollectionReconstructsJSONBenchRows(t *testing.T, source string, ds *colgranule.JSONBenchDataset, imagePart *colgranule.ColumnPart, dbDir string, format collections.DocumentFormat) {
	t.Helper()
	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dbDir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open remaining TreeDB for validation: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Errorf("close remaining TreeDB validation handle: %v", err)
		}
	}()
	col, err := collections.NewCollectionManager(backend).OpenCollection("jsonbench_remaining_" + string(format))
	if err != nil {
		t.Fatalf("open remaining collection for validation: %v", err)
	}
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatalf("create remaining materializer: %v", err)
	}
	defer func() {
		if err := materializer.Close(); err != nil {
			t.Errorf("close remaining materializer: %v", err)
		}
	}()

	reverseDictionaries := reverseJSONBenchDictionaries(ds.Dictionaries)
	scanner := imagePart.NewScanner()
	rowIndex := int64(0)
	if err := scanJSONBenchFile(source, func(raw []byte) error {
		if rowIndex >= int64(ds.Rows) {
			return errStopScan
		}
		id := documentID(uint64(rowIndex + 1))
		stored, err := col.Get(id)
		if err != nil {
			return fmt.Errorf("get stored retained row %d: %w", rowIndex, err)
		}
		retainedJSON, err := materializer.StoredDocumentJSON(stored)
		if err != nil {
			return fmt.Errorf("materialize stored retained row %d: %w", rowIndex, err)
		}
		retained, err := decodeRetainedDocumentForTest("json", retainedJSON)
		if err != nil {
			return fmt.Errorf("decode stored retained row %d: %w", rowIndex, err)
		}
		typed, err := typedJSONBenchValuesFromImage(scanner, imagePart, reverseDictionaries, rowIndex)
		if err != nil {
			return fmt.Errorf("read typed column values for row %d: %w", rowIndex, err)
		}
		restoreClickHouseTypedPathsForTest(retained, typed)

		var original map[string]any
		if err := decodeJSONPreserveNumbers(raw, &original); err != nil {
			return fmt.Errorf("decode source row %d: %w", rowIndex, err)
		}
		if got, want := canonicalJSONForTest(t, retained), canonicalJSONForTest(t, original); string(got) != string(want) {
			return fmt.Errorf("row %d reconstructed stored remaining mismatch\ngot  %s\nwant %s", rowIndex, got, want)
		}
		rowIndex++
		return nil
	}); err != nil {
		t.Fatalf("validate stored remaining collection: %v", err)
	}
	if rowIndex != int64(ds.Rows) {
		t.Fatalf("validated rows=%d want %d", rowIndex, ds.Rows)
	}
}

func localJSONBenchFullReconstructionSource(t *testing.T) (string, int) {
	t.Helper()
	if os.Getenv("JSONBENCH_VALIDATE_FULL_RECONSTRUCTION") == "" {
		t.Skip("JSONBENCH_VALIDATE_FULL_RECONSTRUCTION not set")
	}
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		t.Skip("JSONBENCH_DATA not set")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Skipf("local JSONBench fixture not present at %s", path)
	}
	if info.IsDir() {
		t.Skipf("JSONBENCH_DATA points to directory %s; full reconstruction harness expects a file", path)
	}
	limit := 0
	if raw := os.Getenv("JSONBENCH_VALIDATE_FULL_RECONSTRUCTION_LIMIT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			t.Fatalf("invalid JSONBENCH_VALIDATE_FULL_RECONSTRUCTION_LIMIT=%q", raw)
		}
		limit = parsed
	}
	return path, limit
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
	corrupted := strings.Replace(string(data), `"kind":"commit"`, `"kind":"changed"`, 1)
	if corrupted == string(data) {
		t.Fatal("sample fixture did not contain the expected corruption target")
	}
	data = []byte(corrupted)
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

type typedJSONBenchValues struct {
	TimeUS           int64
	Did              string
	Kind             string
	CommitOperation  string
	CommitCollection string
}

func typedJSONBenchValuesFromImage(scanner *colgranule.ColumnPartScanner, part *colgranule.ColumnPart, dictionaries map[string]map[int64]string, row int64) (typedJSONBenchValues, error) {
	locator, ok := part.LocatePrimaryID(row)
	if !ok {
		return typedJSONBenchValues{}, os.ErrNotExist
	}
	timeUS, err := scanner.ValueAt(locator, "time_us")
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	didCode, err := scanner.ValueAt(locator, "did_code")
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	kindCode, err := scanner.ValueAt(locator, "kind_code")
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	operationCode, err := scanner.ValueAt(locator, "commit_operation_code")
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	collectionCode, err := scanner.ValueAt(locator, "commit_collection_code")
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	did, err := dictionaryValueForTest(dictionaries, "did_code", didCode)
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	kind, err := dictionaryValueForTest(dictionaries, "kind_code", kindCode)
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	operation, err := dictionaryValueForTest(dictionaries, "commit_operation_code", operationCode)
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	collection, err := dictionaryValueForTest(dictionaries, "commit_collection_code", collectionCode)
	if err != nil {
		return typedJSONBenchValues{}, err
	}
	return typedJSONBenchValues{
		TimeUS:           timeUS,
		Did:              did,
		Kind:             kind,
		CommitOperation:  operation,
		CommitCollection: collection,
	}, nil
}

func reverseJSONBenchDictionaries(dictionaries map[string]map[string]int64) map[string]map[int64]string {
	out := make(map[string]map[int64]string, len(dictionaries))
	for name, values := range dictionaries {
		reversed := make(map[int64]string, len(values))
		for value, code := range values {
			reversed[code] = value
		}
		out[name] = reversed
	}
	return out
}

func dictionaryValueForTest(dictionaries map[string]map[int64]string, name string, code int64) (string, error) {
	values := dictionaries[name]
	if values == nil {
		return "", fmt.Errorf("missing dictionary %s", name)
	}
	value, ok := values[code]
	if !ok {
		return "", fmt.Errorf("missing dictionary %s code %d", name, code)
	}
	return value, nil
}

func decodeRetainedDocumentForTest(format string, raw []byte) (map[string]any, error) {
	var doc map[string]any
	switch format {
	case "json":
		if err := decodeJSONPreserveNumbers(raw, &doc); err != nil {
			return nil, err
		}
	case "bson":
		var bsonDoc bson.M
		if err := bson.Unmarshal(raw, &bsonDoc); err != nil {
			return nil, err
		}
		doc = normalizeBSONValueForTest(bsonDoc).(map[string]any)
	default:
		return nil, os.ErrInvalid
	}
	return doc, nil
}

func restoreClickHouseTypedPathsForTest(doc map[string]any, values typedJSONBenchValues) {
	doc["time_us"] = values.TimeUS
	doc["did"] = values.Did
	doc["kind"] = values.Kind
	if values.Kind != "commit" && values.CommitOperation == "" && values.CommitCollection == "" {
		return
	}
	commit, ok := doc["commit"].(map[string]any)
	if !ok {
		commit = make(map[string]any, 2)
	}
	commit["operation"] = values.CommitOperation
	commit["collection"] = values.CommitCollection
	doc["commit"] = commit
}

func canonicalJSONForTest(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func normalizeBSONValueForTest(value any) any {
	switch v := value.(type) {
	case bson.M:
		out := make(map[string]any, len(v))
		for key, nested := range v {
			out[key] = normalizeBSONValueForTest(nested)
		}
		return out
	case bson.D:
		out := make(map[string]any, len(v))
		for _, element := range v {
			out[element.Key] = normalizeBSONValueForTest(element.Value)
		}
		return out
	case bson.A:
		out := make([]any, len(v))
		for i, nested := range v {
			out[i] = normalizeBSONValueForTest(nested)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, nested := range v {
			out[i] = normalizeBSONValueForTest(nested)
		}
		return out
	default:
		return value
	}
}

func TestValidateEncodedPartParityRejectsMismatchedDigest(t *testing.T) {
	reference := []colgranule.JSONBenchQueryTiming{{
		Query:        "Q1",
		ResultRows:   3,
		ResultDigest: 10,
	}}
	encoded := []colgranule.JSONBenchPartQueryTiming{{
		Query:        "Q1",
		ResultRows:   3,
		ResultDigest: 11,
		Attempts: []colgranule.JSONBenchPartQueryAttempt{{
			ResultRows:   3,
			ResultDigest: 11,
		}},
	}}
	err := validateEncodedPartParity(reference, encoded)
	if err == nil {
		t.Fatal("expected encoded part parity error")
	}
	if !strings.Contains(err.Error(), "rows/digest") {
		t.Fatalf("parity error %q does not describe rows/digest mismatch", err)
	}
}

func TestValidateEncodedPartParityRejectsMismatchedAttempt(t *testing.T) {
	reference := []colgranule.JSONBenchQueryTiming{{
		Query:        "Q2",
		ResultRows:   4,
		ResultDigest: 20,
	}}
	encoded := []colgranule.JSONBenchPartQueryTiming{{
		Query:        "Q2",
		ResultRows:   4,
		ResultDigest: 20,
		Attempts: []colgranule.JSONBenchPartQueryAttempt{{
			ResultRows:   4,
			ResultDigest: 21,
		}},
	}}
	err := validateEncodedPartParity(reference, encoded)
	if err == nil {
		t.Fatal("expected encoded part attempt parity error")
	}
	if !strings.Contains(err.Error(), "attempt 0") {
		t.Fatalf("parity error %q does not describe attempt mismatch", err)
	}
}
