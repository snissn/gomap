package db

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafPageReadCacheVerifiedCOWViewSafetyAndConcurrency(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "leaf_vlog")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l255-000001.log")
	fileID, err := valuelog.EncodeFileID(255, 1)
	if err != nil {
		t.Fatal(err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	leaf := makeDistinctLeafPage(777)
	ptrs, err := w.AppendFrame(0, nil, []valuelog.Record{{RID: 1, Value: leaf}})
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	ptrVal := ptrs[0]
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	rawFile, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rawFile.Close()
	ptr, err := page.LeafLogPtrFromValuePtr(ptrVal)
	if err != nil {
		t.Fatal(err)
	}

	f := &valuelog.File{ID: fileID, Path: path, File: rawFile}
	canonical := valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	native := valuelog.COWReadLimits{MaxRecordBytes: 512 << 20, MaxRawBytes: 512 << 20, MaxValueBytes: 512 << 20}
	shape, _, err := f.InspectCOWLeafRecordPinned(ptrVal, canonical)
	if err != nil {
		t.Fatalf("InspectCOWLeafRecordPinned: %v", err)
	}

	identity := rootpublication.StableIdentity{Platform: "test", VolumeID: 1, ObjectID: [16]byte{1}, Generation: 1}
	wrongIdentity := rootpublication.StableIdentity{Platform: "test", VolumeID: 1, ObjectID: [16]byte{2}, Generation: 1}
	cache := newLeafPageReadCache(16)

	// Write-origin slot must never satisfy getVerifiedCOWViewLocked.
	cache.storeWrite(ptr, leaf)
	if _, _, _, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, canonical, nil); ok {
		t.Fatal("write-origin slot unexpectedly hit getVerifiedCOWViewLocked")
	}

	// Admit verified page WITH shape+identity certificate.
	cert := leafPageCOWShapeCert{shape: shape, identity: identity, valid: true}
	if admitted, err := admitVerifiedCachedLeafPageWithShape(cache, ptr, leaf, false, canonical, cert); err != nil || !admitted {
		t.Fatalf("admitVerifiedCachedLeafPageWithShape: admitted=%t err=%v", admitted, err)
	}

	// Valid hit returns zero-copy view matching leaf bytes and allows marking verified under RLock.
	data, slot, state, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, canonical, nil)
	if !ok || !bytes.Equal(data, leaf) || !state.RecordChecksumVerified || !state.CacheEntryPresent || state.PageChecksumVerified {
		t.Fatalf("first view hit: ok=%t state=%+v", ok, state)
	}
	slot.markCopiedPageChecksumVerifiedLocked()
	slot.ReleaseLeafLogPageView()

	// Second hit carries PageChecksumVerified=true.
	data, slot, state, ok = cache.getVerifiedCOWViewLocked(ptr, identity, f, canonical, nil)
	if !ok || !bytes.Equal(data, leaf) || !state.PageChecksumVerified {
		t.Fatalf("second view hit: ok=%t state=%+v", ok, state)
	}
	slot.ReleaseLeafLogPageView()

	// Wrong identity must miss cleanly without leaking RLock.
	if _, _, _, ok := cache.getVerifiedCOWViewLocked(ptr, wrongIdentity, f, canonical, nil); ok {
		t.Fatal("wrong StableIdentity hit getVerifiedCOWViewLocked")
	}

	// Tighter limits exceeding shape.RecordBytes must miss cleanly.
	tight := valuelog.COWReadLimits{MaxRecordBytes: 1, MaxRawBytes: 1, MaxValueBytes: 1}
	if _, _, _, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, tight, nil); ok {
		t.Fatal("tight COWReadLimits hit getVerifiedCOWViewLocked")
	}

	// checkShape refusal must miss cleanly without leaking RLock.
	if _, _, _, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, canonical, func(valuelog.COWRecordShape) bool { return false }); ok {
		t.Fatal("checkShape refusal hit getVerifiedCOWViewLocked")
	}

	// Disabled cache must miss cleanly.
	cache.disabled.Store(true)
	if _, _, _, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, canonical, nil); ok {
		t.Fatal("disabled cache hit getVerifiedCOWViewLocked")
	}
	cache.disabled.Store(false)

	// Dictionary shape must enforce cowReadPolicy dominance AND return empty proof state.
	dictShape := shape
	dictShape.Compressed = true
	dictShape.DictID = 99
	ptrDict := ptr
	ptrDict.Offset += 8192
	cache.storeWrite(ptrDict, leaf)
	// Manually adjust dictShape's ValidateCachedCOWLeafShape check by using original ptr with dict cert.
	cache.storeWrite(ptr, leaf)
	dictCert := leafPageCOWShapeCert{shape: dictShape, identity: identity, valid: true}
	if admitted, err := admitVerifiedCachedLeafPageWithShape(cache, ptr, leaf, true, native, dictCert); err != nil || !admitted {
		t.Fatalf("admit dict cert: admitted=%t err=%v", admitted, err)
	}
	// Stricter canonical limits must miss against broader native (512 MiB) credit.
	if _, _, _, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, canonical, nil); ok {
		t.Fatal("stricter canonical limits hit broader native dictionary credit")
	}
	// Native limits hit, and proof state MUST remain empty (never skips page CRC for dict leaves).
	data, slot, state, ok = cache.getVerifiedCOWViewLocked(ptr, identity, f, native, nil)
	if !ok || !bytes.Equal(data, leaf) || state != (leafPageReadCacheState{}) {
		t.Fatalf("native dict view hit: ok=%t state=%+v want empty state", ok, state)
	}
	slot.ReleaseLeafLogPageView()

	// Concurrent readers + write-origin overwrites + re-admissions under -race.
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if view, lease, _, hit := cache.getVerifiedCOWViewLocked(ptr, identity, f, native, nil); hit {
					if !bytes.Equal(view, leaf) {
						t.Errorf("corrupt view bytes under concurrent traffic")
					}
					lease.ReleaseLeafLogPageView()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			cache.storeWrite(ptr, leaf)
			_, _ = admitVerifiedCachedLeafPageWithShape(cache, ptr, leaf, false, canonical, cert)
		}
	}()
	wg.Wait()
}
