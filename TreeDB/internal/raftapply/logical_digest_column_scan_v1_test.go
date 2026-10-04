package raftapply

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestLogicalDigestV1ColumnScanMatchesPointReference(t *testing.T) {
	dir := t.TempDir()
	database := openApplyHarnessDB(t, dir)
	defer func() { _ = database.Close() }()
	manager := collections.NewCollectionManager(database)
	cfg := &collections.ColumnStoreConfig{
		Enabled:                 true,
		Columns:                 []collections.ColumnStoreColumn{{Name: "value", Path: "value", ValueType: collections.ColumnStoreValueInt64}},
		RetainedPayload:         collections.ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          collections.ColumnReconstructionRetainedPayloadAndColumns,
	}
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	// Append later IDs first, then older IDs in another generation. Replacement
	// and deletion also prevent an insert-only monotonic reconstruction proof.
	for _, start := range []int{300, 0} {
		ids, documents := make([][]byte, 300), make([][]byte, 300)
		for i := range ids {
			n := start + i
			ids[i] = []byte(fmt.Sprintf("d%04d", n))
			documents[i] = []byte(fmt.Sprintf(`{"value":%d,"retained":"row-%04d"}`, n, n))
		}
		if _, err := c.InsertBatch(ids, documents); err != nil {
			t.Fatal(err)
		}
	}
	if replaced, err := c.Replace([]byte("d0010"), []byte(`{"value":999,"retained":"replacement"}`)); err != nil || !replaced {
		t.Fatalf("replace: replaced=%v err=%v", replaced, err)
	}
	if deleted, err := c.DeleteBatch([][]byte{[]byte("d0020")}); err != nil || deleted != 1 {
		t.Fatalf("delete: deleted=%d err=%v", deleted, err)
	}
	check := func() LogicalDigestV1 {
		t.Helper()
		c, err := collections.NewCommandWALReplayCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		cfg := c.Meta().Options.ColumnStore
		if cfg == nil || cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != "tcs1" || cfg.RetainedPayload != collections.ColumnRetainedPayloadNonColumn || cfg.RetainedPayloadEncoding != collections.ColumnRetainedPayloadEncodingSemanticStreamV1 {
			t.Fatalf("fixture lost legacy semantic-stream reconstruction: %+v", cfg)
		}
		count := 0
		var previous []byte
		truncated, err := c.ScanDocumentsFunc(600, func(record collections.DocumentRecord) (bool, error) {
			if count > 0 && bytes.Compare(previous, record.ID) >= 0 {
				t.Fatalf("scan ID order: %q before %q", previous, record.ID)
			}
			previous = bytes.Clone(record.ID)
			count++
			return true, nil
		})
		stats := c.LastDocumentScanStats()
		if err != nil || truncated || count != 599 || !stats.GenericFallback || stats.CertifiedMonotonicPath || stats.LocatorLookupBatches < 3 || stats.MaxRecordWindow > 256 {
			t.Fatalf("generic bounded scan: count=%d truncated=%v stats=%+v err=%v", count, truncated, stats, err)
		}
		want := pointReferenceColumnLogicalDigestV1(t, database)
		got, err := LogicalDigestV1ForDB(database, LogicalDigestOptionsV1{})
		if err != nil || got != want {
			t.Fatalf("batched=%s point-reference=%s err=%v", got.Hex(), want.Hex(), err)
		}
		return got
	}
	want := check()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = openApplyHarnessDB(t, dir)
	if got := check(); got != want {
		t.Fatalf("reopen changed logical digest: got=%s want=%s", got.Hex(), want.Hex())
	}
}

// Reference the old sorted-primary-ID/GetInto path. This fixture has no vector
// preparation or split receipts; canonical catalog metadata and every current
// document are still framed exactly as V1, including count before records.
func pointReferenceColumnLogicalDigestV1(t *testing.T, database *backenddb.DB) LogicalDigestV1 {
	t.Helper()
	manager := collections.NewCommandWALReplayCollectionManager(database)
	metas, err := manager.ListCollections()
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Name < metas[j].Name })
	h := sha256.New()
	writeLogicalDigestField(h, "domain", []byte(logicalDigestDomainV1))
	writeLogicalDigestU64(h, "logical-version", 1)
	writeLogicalDigestField(h, "scope-rule", []byte(raftentry.ScopeRuleSingleGroupV1))
	writeLogicalDigestField(h, "database-scope", []byte(raftentry.DatabaseScopeDefaultV1))
	writeLogicalDigestField(h, "catalog-scope", []byte(raftentry.CatalogScopeDefaultV1))
	writeLogicalDigestU64(h, "collection-count", uint64(len(metas)))
	for _, meta := range metas {
		payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
		if err != nil {
			t.Fatal(err)
		}
		writeLogicalDigestField(h, "catalog-create-collection-payload", payload)
		c, err := manager.OpenCollection(meta.Name)
		if err != nil {
			t.Fatal(err)
		}
		var ids [][]byte
		truncated, err := c.ScanDocumentIDsFunc(maxInt(), func(id []byte) (bool, error) {
			ids = append(ids, id)
			return true, nil
		})
		if err != nil || truncated {
			t.Fatalf("reference primary IDs: truncated=%v err=%v", truncated, err)
		}
		sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i], ids[j]) < 0 })
		writeLogicalDigestU64(h, "collection-document-count", uint64(len(ids)))
		materializer, err := c.NewStoredDocumentJSONMaterializer()
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			document, found, err := c.GetInto(id, nil)
			if err != nil || !found {
				t.Fatalf("reference current document %q: found=%v err=%v", id, found, err)
			}
			jsonDoc, err := materializer.StoredDocumentJSON(document)
			if err != nil {
				t.Fatal(err)
			}
			writeLogicalDigestField(h, "collection-document-id", id)
			writeLogicalDigestField(h, "collection-document-json", jsonDoc)
		}
		if err := materializer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var out LogicalDigestV1
	copy(out[:], h.Sum(nil))
	return out
}
