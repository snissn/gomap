package collections

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

// The unchanged batch payload path is the byte oracle. This is a representation
// equivalence control, not a claimed behavioral RED: the public native ordinary
// ColumnGraph eligibility RED is retained separately by the coordinator.
func TestColumnGraphSerialRawAssetsMatchBatchConstruction(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatal(err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("a"), Vector: []float32{1, 0, 0}, InvNorm: 1,
			Adjacency:  []uint32{columnVectorGraphLayeredAdjacencyMagic, 2, 1, 1, 1, 1, 0},
			BaseRowRef: DocumentRowRef{Generation: 2, PartID: 1, RowIndex: 1, AppliedCommandLSN: 1}},
		{ID: []byte("long-document-b"), Vector: []float32{0, 1, 0}, InvNorm: 1,
			Adjacency:  []uint32{columnVectorGraphLayeredAdjacencyMagic, 2, 1, 0, 1, 0, 0},
			BaseRowRef: DocumentRowRef{Generation: 2, PartID: 1, RowIndex: 0, AppliedCommandLSN: 1}},
	}
	t.Run("adjacency", func(t *testing.T) {
		oldDir, serialDir := t.TempDir(), t.TempDir()
		payloads, err := prepareColumnVectorIndexStateAdjacencyPayloads(oldDir, "docs", *cfg, def, 10, rows)
		if err != nil {
			t.Fatal(err)
		}
		appender, err := newColumnVectorGraphAssetAppender(oldDir, payloads[0].Config, nil)
		if err != nil {
			t.Fatal(err)
		}
		want, appendErr := appendColumnVectorIndexStateAdjacencyPayloads(appender, 2, payloads)
		closeErr := closeColumnVectorGraphAssetAppender(appender, nil)
		if appendErr != nil || closeErr != nil {
			t.Fatalf("batch append=%v close=%v", appendErr, closeErr)
		}
		got, err := prepareColumnVectorIndexStateAdjacencyAssets(serialDir, "docs", *cfg, def, 2, 10, rows)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("serial descriptors=%+v batch=%+v", got, want)
		}
		for i := range want {
			assertSerialColumnGraphAssetBytes(t, oldDir, want[i].Ref, serialDir, got[i].Ref)
			if got[i].Ref.FileID != got[0].Ref.FileID {
				t.Fatal("adjacency family changed FileID")
			}
			if got[i].Ref.PartID != uint64(10+i) {
				t.Fatalf("part order=%d", got[i].Ref.PartID)
			}
		}
	})
	for _, inverseOnly := range []bool{false, true} {
		name := "all_row_refs"
		if inverseOnly {
			name = "inverse_only"
		}
		t.Run(name, func(t *testing.T) {
			oldDir, serialDir := t.TempDir(), t.TempDir()
			payloads, err := prepareColumnVectorGraphRowRefStatePayloads("docs", *cfg, def, 2, 20, rows, inverseOnly)
			if err != nil {
				t.Fatal(err)
			}
			appender, err := newColumnVectorGraphAssetAppender(oldDir, payloads[0].Config, nil)
			if err != nil {
				t.Fatal(err)
			}
			want, appendErr := appendColumnVectorGraphRowRefStatePayloads(appender, 2, payloads)
			closeErr := closeColumnVectorGraphAssetAppender(appender, nil)
			if appendErr != nil || closeErr != nil {
				t.Fatalf("batch append=%v close=%v", appendErr, closeErr)
			}
			got, err := prepareColumnVectorGraphRowRefStateAssetsWithStableAuthority(serialDir, "docs", *cfg, def, 2, 20, rows, nil, inverseOnly)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("serial descriptors=%+v batch=%+v", got, want)
			}
			for i := range want {
				assertSerialColumnGraphAssetBytes(t, oldDir, want[i].Ref, serialDir, got[i].Ref)
				if got[i].Ref.FileID != got[0].Ref.FileID {
					t.Fatal("row-ref family changed FileID")
				}
				if got[i].Ref.PartID != uint64(20+i) {
					t.Fatalf("part order=%d", got[i].Ref.PartID)
				}
			}
		})
	}
}

func assertSerialColumnGraphAssetBytes(t *testing.T, aDir string, a ColumnAssetRef, bDir string, b ColumnAssetRef) {
	t.Helper()
	read := func(dir string, ref ColumnAssetRef) []byte {
		path, err := columnAssetSegmentPath(dir, ref)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Offset < 0 || ref.Length < 0 || ref.Offset > int64(len(data))-ref.Length {
			t.Fatal("invalid asset span")
		}
		return data[ref.Offset : ref.Offset+ref.Length]
	}
	if !bytes.Equal(read(aDir, a), read(bDir, b)) {
		t.Fatal("serial asset differs from original batch bytes")
	}
}

func TestColumnGraphSerialAssetFailureClosesFamily(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		t.Fatal(err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	rows := []columnVectorGraphAssetRow{{Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 0, 0}}}
	dir := t.TempDir()
	if got, err := prepareColumnVectorIndexStateAdjacencyAssets(dir, "docs", *cfg, def, 2, ^uint64(0), rows); err == nil || got != nil {
		t.Fatalf("overflow must not return assets: got=%+v err=%v", got, err)
	}
	// A failed family must release the same segment lock and file authority.
	// This completes synchronously; no timing or goroutine witness is required.
	if _, err := prepareColumnVectorIndexStateAdjacencyAssets(dir, "docs", *cfg, def, 2, 1, rows); err != nil {
		t.Fatal(err)
	}
	rows[0].Adjacency = []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 1}
	if got, err := prepareColumnVectorIndexStateAdjacencyAssets(dir, "docs", *cfg, def, 2, 1, rows); err == nil || got != nil {
		t.Fatalf("malformed adjacency must not return assets: got=%+v err=%v", got, err)
	}
}
