package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// These assertions enter through the public read APIs. They guard work and
// ownership that output parity alone cannot distinguish.
func TestR1CapturedReaderSharesValidatedMetadata(t *testing.T) {
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer d.Close()
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, r1ReadColumns([]string{"whole row"}, []string{"u1"})); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	a, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, v := range []*CollectionReadView{a, b} {
		if _, err := v.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if &a.columnSnapshotView.AssetRefs[0] != &b.columnSnapshotView.AssetRefs[0] {
		t.Fatal("public readers repeated immutable manifest preparation")
	}
	for _, v := range []*CollectionReadView{a, b} {
		if v.columnSnapshotView.Catalog != nil || v.columnSnapshotView.snapshot != nil {
			t.Fatal("shared metadata retained reader pins")
		}
		bound, err := v.materializerColumnSnapshotView(*v.catalog.meta.Options.ColumnStore)
		if err != nil || bound.Catalog != v.catalog || bound.snapshot != v.snapshot {
			t.Fatal("metadata rebound to another captured catalog")
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := b.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r1RequireCompleteRow(t, got.Results[0].Document, []byte(`{"content":"whole row","user":"u1","id":"a"}`))
}

func TestR1CapturedReaderBoundsBorrowedBlocks(t *testing.T) {
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer d.Close()
	ids := make([][]byte, 48)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%03d", i))
		if _, _, err := col.InsertTypedBatchWithStats([][]byte{ids[i]}, [][]byte{[]byte(`{"id":"` + string(ids[i]) + `"}`)}, r1ReadColumns([]string{"content"}, []string{"u1"})); err != nil {
			t.Fatal(err)
		}
		if err := col.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	v, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	got, err := v.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Results) != len(ids) {
		t.Fatal("partial result")
	}
	for i, r := range got.Results {
		if !r.Found || !bytes.Equal(r.ID, ids[i]) {
			t.Fatalf("row %d lost during bounded fetch", i)
		}
		r1RequireCompleteRow(t, r.Document, []byte(`{"content":"content","user":"u1","id":"`+string(ids[i])+`"}`))
	}
	if len(v.pointRowBlocks) > 32 {
		t.Fatalf("held reader retained %d row blocks, cap 32", len(v.pointRowBlocks))
	}
	if v.assetManager.ActiveHandles() > 32 {
		t.Fatalf("held reader retained %d asset handles, cap 32", v.assetManager.ActiveHandles())
	}
	if v.pointRowOpenFiles() > 32 || v.pointRowCreditUsed > v.pointRowCreditLimit || v.pointRowCacheEvictions == 0 {
		t.Fatalf("admission: files=%d used=%d limit=%d evictions=%d", v.pointRowOpenFiles(), v.pointRowCreditUsed, v.pointRowCreditLimit, v.pointRowCacheEvictions)
	}
	stats := v.assetManager.Stats()
	actual := stats.ActiveMappedBytes + stats.ActiveHeapCopyBytes + stats.ActiveDerivedMetadataBytes
	for _, block := range v.pointRowBlocks {
		// Encoded raw aliases the already charged mapped/heap handle.
		actual += block.residentBytes - int64(cap(block.raw))
	}
	if actual > v.pointRowCreditUsed {
		t.Fatalf("actual backing %d exceeds reserved credit %d", actual, v.pointRowCreditUsed)
	}
	owned := bytes.Clone(got.Results[0].Document)
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(owned, got.Results[0].Document) {
		t.Fatal("owned output changed after cache release")
	}
}

func TestR1GetIntoDoesNotAllocateWholeIntermediateDocument(t *testing.T) {
	measure := func(repeat int) (allocated, sourceBacking, outputBytes uint64) {
		_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
		defer d.Close()
		content := strings.Repeat("ordinary scalar string ", repeat)
		if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, r1ReadColumns([]string{content}, []string{"u1"})); err != nil {
			t.Fatal(err)
		}
		if err := col.Flush(); err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 0, len(content)+256)
		if _, found, err := col.GetInto([]byte("a"), dst); err != nil || !found {
			t.Fatalf("warm found=%t err=%v", found, err)
		}
		v, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := v.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{}); err != nil {
			t.Fatal(err)
		}
		for _, ref := range v.columnSnapshotView.AssetRefs {
			sourceBacking += uint64(ref.Ref.Length)
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		const reads = 10
		for i := 0; i < reads; i++ {
			got, found, err := col.GetInto([]byte("a"), dst)
			if err != nil || !found {
				t.Fatalf("found=%t err=%v", found, err)
			}
			if &got[0] != &dst[:cap(dst)][0] {
				t.Fatal("caller output buffer was not reused")
			}
			outputBytes = uint64(len(got))
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / reads, sourceBacking, outputBytes
	}
	small, smallSource, smallOutput := measure(3000)
	large, largeSource, largeOutput := measure(6000)
	// This is a growth invariant, not a performance adjustment: all source
	// backing remains charged in public B/op. Doubling the string may grow its
	// owned encoded asset once, but must not also grow a complete intermediate
	// output. Fixed metadata/workspace costs cancel between the two sizes.
	if largeSource <= smallSource || largeOutput <= smallOutput || large <= small {
		t.Fatal("fixture did not grow")
	}
	if large-small >= (largeSource-smallSource)+(largeOutput-smallOutput)/2 {
		t.Fatalf("GetInto growth allocated=%d source=%d output=%d; duplicate full output backing", large-small, largeSource-smallSource, largeOutput-smallOutput)
	}
}

func TestR1GetIntoAliasesReallocationAndErrorOwnership(t *testing.T) {
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer d.Close()
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, r1ReadColumns([]string{"whole row"}, []string{"u1"})); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, capacity := range []int{1, 256} {
		backing := make([]byte, capacity)
		backing[0] = 'a'
		got, found, err := col.GetInto(backing[:1], backing[:0])
		if err != nil || !found {
			t.Fatalf("found=%t err=%v", found, err)
		}
		r1RequireCompleteRow(t, got, []byte(`{"content":"whole row","user":"u1","id":"a"}`))
		if capacity == 256 && &got[0] != &backing[0] {
			t.Fatal("alias destination lost spare capacity")
		}
	}
	dst := []byte("caller owns this")
	got, found, err := col.GetInto([]byte("missing"), dst)
	if err != nil || found || len(got) != 0 || string(dst) != "caller owns this" {
		t.Fatalf("missing changed caller ownership: %q %t %v", got, found, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	got, found, err = col.GetInto([]byte("a"), dst)
	if err == nil || found || len(got) != 0 || string(dst) != "caller owns this" {
		t.Fatalf("error changed caller ownership: %q %t %v", got, found, err)
	}
}

func TestR1CapturedReaderFailedLoadReleasesAdmission(t *testing.T) {
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer d.Close()
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, r1ReadColumns([]string{"whole row"}, []string{"u1"})); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	v, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if _, err := v.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{}); err != nil {
		t.Fatal(err)
	}
	ref := v.columnSnapshotView.AssetRefs[0].Ref
	path := filepath.Join(v.rowAssetReadCache.segmentDir, columnAssetSegmentFileName(ref.FileID))
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.closeAssetReadCaches(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := v.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{}); err == nil {
		t.Fatal("truncated asset was accepted")
	}
	if v.pointRowOpenFiles() != 0 || v.assetManager.ActiveHandles() != 0 || v.pointRowCreditUsed != 0 || v.rowEmissionScratch != nil {
		t.Fatal("failed load retained admission or descriptors")
	}
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := v.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r1RequireCompleteRow(t, got.Results[0].Document, []byte(`{"content":"whole row","user":"u1","id":"a"}`))
}

func TestR1CapturedReaderOversizeOwnedFallback(t *testing.T) {
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer d.Close()
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, r1ReadColumns([]string{"whole row"}, []string{"u1"})); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	v, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	refs, err := v.LookupDocumentRowRefsByID([][]byte{[]byte("a")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	response := DocumentFetchResponse{Results: []DocumentFetchResult{{ID: []byte("a"), Found: true}}}
	ref := refs.Results[0].RowRef
	got, err := v.fetchColumnStoreDocumentsOwnedFallback(response, []DocumentRowRef{ref}, [][]byte{[]byte(`{"id":"a"}`)}, DocumentFetchOptions{}, nil, documentRowRefStrict, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	r1RequireCompleteRow(t, got.Results[0].Document, []byte(`{"content":"whole row","user":"u1","id":"a"}`))
	if got.Stats.RowRefFallbackScans != 1 || got.Stats.VisibilityScans != 1 || got.Stats.VisibilityPhysicalBytes == 0 {
		t.Fatalf("fallback work hidden: %+v", got.Stats)
	}
	ref.RowIndex++
	if _, err := v.fetchColumnStoreDocumentsOwnedFallback(response, []DocumentRowRef{ref}, [][]byte{[]byte(`{"id":"a"}`)}, DocumentFetchOptions{}, nil, documentRowRefStrict, nil, false); err == nil {
		t.Fatal("fallback accepted stale ref")
	}
	cfg := *v.catalog.meta.Options.ColumnStore
	if _, err := documentPointRowBlockCredit(columnManifestAssetRefForScan{Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Length: math.MaxInt64}, Rows: 1}, cfg); !errors.Is(err, errDocumentPointRowOversize) {
		t.Fatalf("overflow did not choose owned eligibility: %v", err)
	}
}

// Descriptor identity is not content immutability. A held reader may return its
// validated owned snapshot or reject an integrity change; it must never emit a
// changed mapped payload without validation. The owned output already returned
// to the caller must survive mutation, retry and view close.
func TestR1CapturedReaderHeldContentMutation(t *testing.T) {
	for _, forceReadAt := range []bool{false, true} {
		for _, integrity := range []ColumnAssetReadIntegrity{ColumnAssetReadIntegrityVerify, ColumnAssetReadIntegrityCachedVerify, ColumnAssetReadIntegritySkipChecksums} {
			t.Run(fmt.Sprintf("readat%t/%s", forceReadAt, integrity), func(t *testing.T) {
				_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
				defer d.Close()
				id := []byte("a")
				want := []byte(`{"content":"stable captured content","user":"u1","id":"a"}`)
				if _, _, err := col.InsertTypedBatchWithStats([][]byte{id}, [][]byte{[]byte(`{"id":"a"}`)}, r1ReadColumns([]string{"stable captured content"}, []string{"u1"})); err != nil {
					t.Fatal(err)
				}
				if err := col.Flush(); err != nil {
					t.Fatal(err)
				}
				v, err := col.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				defer v.Close()
				v.forceAssetReadAtFallbackForTest = forceReadAt
				opts := DocumentFetchOptions{ColumnAssetReadIntegrity: integrity}
				first, err := v.FetchDocumentsByID([][]byte{id}, opts)
				if err != nil {
					t.Fatal(err)
				}
				r1RequireCompleteRow(t, first.Results[0].Document, want)
				ref := v.columnSnapshotView.AssetRefs[0].Ref
				path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
				if err != nil {
					t.Fatal(err)
				}
				offset := bytes.Index(raw, []byte("stable captured content"))
				if offset < 0 {
					t.Fatal("fixture has no content bytes")
				}
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteAt([]byte("S"), ref.Offset+int64(offset)); err != nil {
					file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				second, err := v.FetchDocumentsByID([][]byte{id}, opts)
				if err != nil {
					if !strings.Contains(err.Error(), "checksum") {
						t.Fatal(err)
					}
					if v.assetManager.ActiveHandles() != 0 || v.pointRowOpenFiles() != 0 || v.pointRowCreditUsed != 0 {
						t.Fatal("failed integrity read retained admission")
					}
				} else {
					r1RequireCompleteRow(t, second.Results[0].Document, want)
				}
				if err := v.Close(); err != nil {
					t.Fatal(err)
				}
				r1RequireCompleteRow(t, first.Results[0].Document, want)
			})
		}
	}
}
