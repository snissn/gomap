package collections

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func r1OwnedStorageFixture(t *testing.T) (*CollectionReadView, [][]byte) {
	t.Helper()
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	t.Cleanup(func() { _ = d.Close() })
	ids := make([][]byte, 48)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("owned-%03d", i))
		payload := []byte(fmt.Sprintf("{\"id\":%q}", string(ids[i])))
		if _, _, err := col.InsertTypedBatchWithStats([][]byte{ids[i]}, [][]byte{payload}, r1ReadColumns([]string{"stable content"}, []string{"u1"})); err != nil {
			t.Fatal(err)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	v, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	return v, ids
}

func TestR1OwnedStorageRecyclesAfterEmissionAndOwnsOutput(t *testing.T) {
	v, ids := r1OwnedStorageFixture(t)
	first, err := v.FetchDocumentsByID(ids[:1], DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owned := bytes.Clone(first.Results[0].Document)
	if len(v.pointRowBuffers[0]) == 0 {
		t.Fatal("missing lazy first slot")
	}
	pointer := &v.pointRowBuffers[0][0]
	for _, raw := range v.pointRowBuffers[1:] {
		if raw != nil {
			t.Fatal("ordinary one-row read eagerly allocated unused raw slots")
		}
	}
	var firstBlock *columnPhysicalRowReaderBlock
	for _, block := range v.pointRowBlocks {
		firstBlock = block
		break
	}
	if firstBlock == nil {
		t.Fatal("missing first decoded block")
	}
	files := make(map[uint32]struct{})
	for _, ref := range v.preparedMaterializer.AssetRefs {
		files[ref.Ref.FileID] = struct{}{}
	}
	if len(files) != 1 {
		t.Fatal("fixture must share one segment to exercise selective file retention")
	}
	var opens uint64
	for pass := 0; pass < 3; pass++ {
		got, err := v.FetchDocumentsByID(ids, DocumentFetchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i, result := range got.Results {
			if !result.Found || !bytes.Equal(result.ID, ids[i]) {
				t.Fatalf("row %d lost during recycling", i)
			}
			want := []byte(fmt.Sprintf("{\"content\":\"stable content\",\"user\":\"u1\",\"id\":%q}", string(ids[i])))
			r1RequireCompleteRow(t, result.Document, want)
		}
		if v.pointRowCreditUsed > v.pointRowCreditLimit || v.pointRowOpenFiles() > 32 || v.assetManager.ActiveHandles() > 32 || v.pointRowBuffersUsed > 32 {
			t.Fatal("recycled view exceeded resource bounds")
		}
		var capacity int64
		for _, raw := range v.pointRowBuffers {
			capacity += int64(cap(raw))
		}
		if capacity+v.pointRowBufferTableCredit() > v.pointRowCreditUsed {
			t.Fatal("retained idle capacities were not charged")
		}
		if pass == 0 {
			opens = v.assetCounters().fileOpens
		} else if v.assetCounters().fileOpens != opens {
			t.Fatal("image recycling reopened the retained source file")
		}
	}
	if v.pointRowCacheEvictions == 0 {
		t.Fatal("fixture did not cross an emission window")
	}
	if &v.pointRowBuffers[0][0] != pointer {
		t.Fatal("equal-sized raw slot was reallocated during recycling")
	}
	if firstBlock.raw != nil || firstBlock.header.Collection != nil || firstBlock.header.Namespace != nil {
		t.Fatal("released block retained borrowed raw/header aliases")
	}
	if !bytes.Equal(first.Results[0].Document, owned) {
		t.Fatal("earlier owned JSON changed after slot reuse")
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	for _, raw := range v.pointRowBuffers {
		if raw != nil {
			t.Fatal("closed view retained raw storage")
		}
	}
	if v.assetManager.ActiveHandles() != 0 || v.pointRowOpenFiles() != 0 || v.pointRowCreditUsed != 0 {
		t.Fatal("closed view retained resource ownership")
	}
	if !bytes.Equal(first.Results[0].Document, owned) {
		t.Fatal("earlier owned JSON changed after Close")
	}
}

func TestR1OwnedStorageRecycledReadRejectsChangedContents(t *testing.T) {
	for _, integrity := range []ColumnAssetReadIntegrity{ColumnAssetReadIntegrityVerify, ColumnAssetReadIntegrityCachedVerify} {
		t.Run(string(integrity), func(t *testing.T) {
			v, ids := r1OwnedStorageFixture(t)
			opts := DocumentFetchOptions{ColumnAssetReadIntegrity: integrity}
			first, err := v.FetchDocumentsByID(ids[:1], opts)
			if err != nil {
				t.Fatal(err)
			}
			owned := bytes.Clone(first.Results[0].Document)
			ref := v.columnSnapshotView.AssetRefs[0].Ref
			path, err := columnAssetSegmentPath(v.collection.db.ColumnAssetRootDir(), ref)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := readColumnPhysicalAssetFromManager(v.collection.db.ColumnAssetRootDir(), ref)
			if err != nil {
				t.Fatal(err)
			}
			offset := bytes.Index(raw, []byte("stable content"))
			if offset < 0 {
				t.Fatal("fixture has no content bytes")
			}
			if _, err := v.FetchDocumentsByID(ids[1:], opts); err != nil {
				t.Fatal(err)
			}
			if v.pointRowCacheEvictions == 0 {
				t.Fatal("fixture did not recycle")
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte("S"), ref.Offset+int64(offset)); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			// Restoring file size/mtime must not certify newly loaded recycled bytes.
			if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			if _, err := v.FetchDocumentsByID(ids[:1], opts); err == nil {
				t.Fatal("mutated recycled range escaped full-ref verification")
			}
			if v.assetManager.ActiveHandles() != 0 || v.pointRowOpenFiles() != 0 || v.pointRowCreditUsed != 0 {
				t.Fatal("failed checksum retained resource ownership")
			}
			for _, raw := range v.pointRowBuffers {
				if raw != nil {
					t.Fatal("failed load retained reusable partial storage")
				}
			}
			if !bytes.Equal(first.Results[0].Document, owned) {
				t.Fatal("failed reload changed earlier owned JSON")
			}
		})
	}
}

func TestR1OwnedStorageIntegrityChangeReleasesBothCaches(t *testing.T) {
	v, ids := r1OwnedStorageFixture(t)
	if _, err := v.FetchDocumentsByID(ids[:1], DocumentFetchOptions{}); err != nil {
		t.Fatal(err)
	}
	old := v.rowAssetReadCache
	if _, err := v.FetchDocumentsByID(ids[1:2], DocumentFetchOptions{ColumnAssetReadIntegrity: ColumnAssetReadIntegrityCachedVerify}); err != nil {
		t.Fatal(err)
	}
	if old == v.rowAssetReadCache || len(old.resourceHandles) != 0 || old.file != nil || len(old.files) != 0 {
		t.Fatal("integrity replacement retained old source owners")
	}
	if v.pointRowCreditUsed > v.pointRowCreditLimit {
		t.Fatal("replacement exceeded credit")
	}
}

// CRL2 current metadata, preserved full rows and column-owned vectors must
// remain simultaneously pinned until each complete JSON emission finishes.
func TestR1OwnedStorageMixedCRL2TypedSourcesRecycle(t *testing.T) {
	_, d, col, ids := seedTypedMetadata4769(t, 48, 8)
	defer d.Close()
	for _, id := range ids {
		if _, err := col.UpdateTypedMetadataByID([][]byte{id}, map[string]any{"meta.user_id": "new"}, nil, metadataGeneration4769(col)); err != nil {
			t.Fatal(err)
		}
	}
	want := make([][]byte, len(ids))
	for i, id := range ids {
		var err error
		want[i], err = col.Get(id)
		if err != nil {
			t.Fatal(err)
		}
	}
	v, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	first, err := v.FetchDocumentsByID(ids[:1], DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if v.pointRowBuffersUsed != 3 || len(v.pointRowBlocks) != 2 || v.assetManager.ActiveHandles() != 3 {
		t.Fatalf("CRL2/typed inputs not simultaneous: slots=%d blocks=%d handles=%d", v.pointRowBuffersUsed, len(v.pointRowBlocks), v.assetManager.ActiveHandles())
	}
	oldTyped := v.typedColumnReconstructionCache
	owned := bytes.Clone(first.Results[0].Document)
	for pass := 0; pass < 2; pass++ {
		got, err := v.FetchDocumentsByID(ids, DocumentFetchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i, result := range got.Results {
			r1RequireCompleteRow(t, result.Document, want[i])
		}
		if v.pointRowCreditUsed > v.pointRowCreditLimit || v.pointRowBuffersUsed > 32 || v.pointRowOpenFiles() > 32 || v.assetManager.ActiveHandles() > 32 {
			t.Fatal("mixed sources exceeded unchanged resource bounds")
		}
	}
	if v.pointRowCacheEvictions == 0 || oldTyped.Parts != nil || oldTyped.SelectivePart != nil || oldTyped.ReadCache != nil {
		t.Fatal("recycled typed owner retained old source aliases")
	}
	if !bytes.Equal(first.Results[0].Document, owned) {
		t.Fatal("mixed source reuse changed earlier owned output")
	}
}
