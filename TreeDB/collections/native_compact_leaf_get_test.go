package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Reopen an ordinary native collection so Get cannot answer from a pending
// logical source. Prove two selected inline documents occupy distinct compact
// physical leaves; the disabled-cache arm must decode on every public Get.
func nativeCompactLeafGetFixture(t testing.TB, cacheEntries int) (*backenddb.DB, *Collection, [][]byte, [][]byte, uint64) {
	t.Helper()
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.LeafPageReadCacheEntries = cacheEntries
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	var owner *treedb.DB
	var manager *CollectionManager
	closeOwner := func() error {
		var result error
		if manager != nil {
			result = manager.Close()
			manager = nil
		}
		if owner != nil {
			result = errors.Join(result, owner.Close())
			owner = nil
		}
		return result
	}
	t.Cleanup(func() {
		if err := closeOwner(); err != nil {
			t.Error(err)
		}
	})
	var backend *backenddb.DB
	open := func() {
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = NewCollectionManager(backend)
		if !manager.UsesNativeCommandWALApply() {
			t.Fatal("fixture did not activate the native public owner")
		}
	}
	open()
	const name = "compact_leaf_get"
	if _, err := manager.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{
		DocumentFormat: DocumentFormatJSON, DataRootStoragePolicy: RootStorageCompressed,
	}}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	ids, docs := make([][]byte, 4096), make([][]byte, 4096)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%04d", i))
		docs[i] = []byte(fmt.Sprintf(`{"id":%d,"value":"compact physical leaf document %04d"}`, i, i))
	}
	if _, err := c.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := closeOwner(); err != nil {
		t.Fatal(err)
	}
	open()
	c, err = manager.OpenCollection(name)
	if err != nil || c.nativeJSON == nil {
		t.Fatalf("reopen native collection: %v", err)
	}
	r, err := c.nativeJSON.Capture(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	if !r.domain.hasBase || r.domain.domain.baseRoot == 0 {
		t.Fatal("fixture lacks a persisted primary root")
	}
	leafReader, ok := r.capture.(backenddb.NativePhysicalLeafReader)
	if !ok {
		t.Fatal("fixture lacks the native physical leaf reader")
	}
	if _, ok := r.capture.(backenddb.NativePhysicalReader); !ok {
		t.Fatal("fixture lacks the ordinary Get physical reader")
	}
	var pointers [2]page.LeafLogPtr
	var compactBytes uint64
	for i, index := range []int{0, len(ids) - 1} {
		if _, resolved, err := r.domain.memoryEntry(ids[index]); resolved || err != nil {
			t.Fatalf("fixture key answered by logical overlay: resolved=%t err=%v", resolved, err)
		}
		var keyScratch, leafScratch [page.PageSize]byte
		entry, err := r.domain.base.GetEntryExactWithFixedScratch(ids[index], keyScratch[:], leafScratch[:], func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
			pointers[i] = ptr
			f, _, err := r.capture.Snapshot().PinnedValueLogFile(ptr.ValueLogFileID())
			if err != nil {
				return nil, err
			}
			shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), valuelog.COWReadLimits{
				MaxRecordBytes: 2 << 20, MaxRawBytes: valuelog.MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize,
			})
			if err != nil || shape.ValueBytes >= page.PageSize {
				return nil, fmt.Errorf("fixture leaf is not compact: value=%d err=%v", shape.ValueBytes, err)
			}
			compactBytes += shape.ValueBytes
			return leafReader.NativeLeafPage(ptr, dst)
		})
		if err != nil || entry.Flags&node.FlagPointer != 0 || !bytes.Equal(entry.Value, docs[index]) {
			t.Fatalf("fixture did not reach the inline physical document: %v", err)
		}
	}
	if pointers[0] == (page.LeafLogPtr{}) || pointers[0] == pointers[1] {
		t.Fatal("fixture must reach distinct compact physical leaves")
	}
	return backend, c, ids, docs, compactBytes / 2
}

func nativeCompactLeafStat(t testing.TB, stats map[string]string, name string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(stats["treedb.process.read_path.outer_leaf."+name], 10, 64)
	if err != nil {
		t.Fatalf("outer leaf stat %s: %v", name, err)
	}
	return v
}

func TestNativeCompactLeafGetOwnedResult(t *testing.T) {
	backend, c, ids, docs, _ := nativeCompactLeafGetFixture(t, -1)
	before := backend.Stats()
	held, err := c.Get(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		index := 0
		if i%2 == 0 {
			index = len(ids) - 1
		}
		got, err := c.Get(ids[index])
		if err != nil || !bytes.Equal(got, docs[index]) {
			t.Fatalf("public Get across scratch reuse: %v", err)
		}
		clear(got)
	}
	if !bytes.Equal(held, docs[0]) {
		t.Fatal("owned result changed after later public Gets closed their captures")
	}
	after := backend.Stats()
	if nativeCompactLeafStat(t, after, "point_loads_total") <= nativeCompactLeafStat(t, before, "point_loads_total") ||
		nativeCompactLeafStat(t, after, "checksum.verifications_total") <= nativeCompactLeafStat(t, before, "checksum.verifications_total") {
		t.Fatal("public Gets did not traverse and verify physical leaves")
	}
	for _, stat := range []string{"cache.capacity", "cache.hits", "cache.stores", "cache.read_miss_admission_stores"} {
		if nativeCompactLeafStat(t, after, stat) != 0 {
			t.Fatalf("disabled leaf cache active: %s", stat)
		}
	}
}

func BenchmarkCollectionGetByIDNativeCompactLeaf(b *testing.B) {
	for _, entries := range []int{-1, 128} {
		for _, parallel := range []bool{false, true} {
			b.Run(fmt.Sprintf("cache=%d/parallel=%t", entries, parallel), func(b *testing.B) {
				backend, c, ids, _, compactBytes := nativeCompactLeafGetFixture(b, entries)
				// Alternate two distant leaves. The enabled arm becomes a warm cache
				// control; the disabled arm includes a real compact decode every time.
				selected := [][]byte{ids[0], ids[len(ids)-1]}
				for i := 0; i < 128; i++ {
					if _, err := c.Get(selected[i%2]); err != nil {
						b.Fatal(err)
					}
				}
				before := backend.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				get := func(i int) {
					if _, err := c.Get(selected[i%2]); err != nil {
						b.Error(err)
					}
				}
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						i := 0
						for pb.Next() {
							get(i)
							i++
						}
					})
				} else {
					for i := 0; i < b.N; i++ {
						get(i)
					}
				}
				b.StopTimer()
				after := backend.Stats()
				for _, stat := range []struct{ name, unit string }{
					{"point_loads_total", "leaf-loads/op"}, {"checksum.verifications_total", "leaf-checks/op"}, {"cache.hits", "cache-hits/op"},
				} {
					b.ReportMetric(float64(nativeCompactLeafStat(b, after, stat.name)-nativeCompactLeafStat(b, before, stat.name))/float64(b.N), stat.unit)
				}
				b.ReportMetric(float64(compactBytes), "selected-value-B")
				b.ReportMetric(float64(nativeCompactLeafStat(b, after, "cache.capacity")), "cache-capacity")
			})
		}
	}
}
