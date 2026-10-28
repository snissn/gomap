package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func cowLeafCacheFixture(t *testing.T, entries int, compressed bool) (*DB, *backenddb.DB, []byte) {
	t.Helper()
	return cowLeafCacheModeFixture(t, entries, compressed, "cow_btree")
}

func cowLeafCacheModeFixture(t testing.TB, entries int, compressed bool, mode string) (*DB, *backenddb.DB, []byte) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: entries})
	if err != nil {
		t.Fatal(err)
	}
	compression := vlogCompressionOff
	if compressed {
		compression = vlogCompressionBlock
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: mode, FlushThreshold: 1 << 30,
		IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
		ValueLogCompression: uint8(compression), ValueLogBlockCodec: 0,
	})
	if err != nil {
		backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	want := bytes.Repeat([]byte("old-inline-value"), 8)
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return db, backend, want
}

func cowLeafCacheCounter(t *testing.T, backend *backenddb.DB, name string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(backend.Stats()["treedb.process.read_path.outer_leaf.cache."+name], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// The write-populated case first verifies write-origin pages, then reuses
// their decoded slots without another store. The cold reopen is tested below
// in TestCOWColdLeafCacheReopenAdmission, without write-origin backing.
func TestCOWPhysicalLeafCacheActivationAndCapturedCuts(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, entries := range []int{128, -1} {
			t.Run(fmt.Sprintf("compressed=%t/entries=%d", compressed, entries), func(t *testing.T) {
				db, backend, want := cowLeafCacheFixture(t, entries, compressed)
				old, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				basis := old.cowCut.basis
				beforeHits := cowLeafCacheCounter(t, backend, "hits")
				beforeStores := cowLeafCacheCounter(t, backend, "stores")
				// Actual public Get performs the original first full verified decode.
				if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("public first physical read: %q %v", got, err)
				}
				if compressed {
					db.cow.physicalMu.Lock()
					decodedSnappy := false
					if reader := db.cow.physicalReader; reader != nil {
						for _, codec := range reader.codecs {
							if codec != nil && codec.id == 0 && codec.codec == valuelog.BlockCodecSnappy {
								decodedSnappy = true
							}
						}
					}
					db.cow.physicalMu.Unlock()
					if !decodedSnappy {
						t.Fatal("compressed fixture did not perform original full Snappy decode")
					}
				}
				check := func(s *Snapshot, expected []byte) {
					t.Helper()
					got, err := s.Get([]byte("a000"))
					if err != nil || !bytes.Equal(got, expected) {
						t.Fatalf("point: %q %v", got, err)
					}
					got[0] ^= 1
					key, value, found, err := s.cowSeekGE([]byte("a000"), nil)
					if err != nil || !found || string(key) != "a000" || !bytes.Equal(value, expected) {
						t.Fatalf("successor: %q/%q found=%t %v", key, value, found, err)
					}
					key[0], value[0] = '!', '!'
				}
				for i := 0; i < 4; i++ {
					check(old, want)
				}
				// The native physical entry uses the same captured page/cache, while
				// retaining its codec policy and owned-output boundary.
				token, ok := basis.snapshot.StateToken()
				if !ok {
					t.Fatal("missing captured token")
				}
				entry, err := basis.nativeEntryAtRoot(db.cow, token.RootPageID, []byte("a000"), true)
				if err != nil || !bytes.Equal(entry.Value, want) {
					t.Fatalf("native physical entry: %q %v", entry.Value, err)
				}
				entry.Value[0] ^= 1
				afterHits := cowLeafCacheCounter(t, backend, "hits")
				if entries > 0 && afterHits <= beforeHits {
					t.Fatal("actual COW repeated physical reads did not hit decoded cache")
				}
				if entries < 0 && afterHits != beforeHits {
					t.Fatal("disabled cache reported COW hits")
				}
				if stores := cowLeafCacheCounter(t, backend, "stores"); stores != beforeStores {
					t.Fatalf("read path populated cache: stores %d -> %d", beforeStores, stores)
				}
				if err := db.Set([]byte("a000"), []byte("new")); err != nil {
					t.Fatal(err)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if db.cow.cut.basis == basis {
					t.Fatal("checkpoint did not replace captured backend basis")
				}
				current, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer current.Close()
				for i := 0; i < 4; i++ {
					check(old, want)
					check(current, []byte("new"))
				}
				t.Logf("initial COW cache hits %d -> %d; unchanged read stores=%d; compressed=%t enabled=%t", beforeHits, afterHits, beforeStores, compressed, entries > 0)
			})
		}
	}
}

func TestCOWPhysicalLeafCacheRequiresVerifiedRecordAndInspection(t *testing.T) {
	db, backend, want := cowLeafCacheFixture(t, 128, false)
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	basis := s.cowCut.basis
	var keyScratch, leafScratch [page.PageSize]byte
	var ptr page.LeafLogPtr
	stop := errors.New("capture physical leaf pointer")
	_, err = basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
		ptr = p
		return nil, stop
	})
	if !errors.Is(err, stop) || ptr.FileID == 0 {
		t.Fatalf("fixture did not traverse real logged leaf: %+v %v", ptr, err)
	}
	f, identity, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	if _, hit, err := basis.snapshot.CopyVerifiedCachedLeafPage(ptr, f, identity, leafScratch[:]); err != nil || hit {
		t.Fatalf("unverified write-origin entry accepted: hit=%t %v", hit, err)
	}
	wrongIdentity := identity
	wrongIdentity.Generation++
	if _, _, err := basis.snapshot.CopyVerifiedCachedLeafPage(ptr, f, wrongIdentity, leafScratch[:]); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("wrong identity: %v", err)
	}
	if _, _, err := basis.snapshot.CopyVerifiedCachedLeafPage(ptr, nil, identity, leafScratch[:]); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("wrong inspected handle: %v", err)
	}
	writer, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	var original [1]byte
	crcOffset := int64(ptr.Offset - 4)
	if _, err := writer.ReadAt(original[:], crcOffset); err != nil {
		t.Fatal(err)
	}
	// CRC corruption survives shape inspection but must fail full verification,
	// leaving the write-origin cache entry untrusted.
	if _, err := writer.WriteAt([]byte{original[0] ^ 1}, crcOffset); err != nil {
		t.Fatal(err)
	}
	defer writer.WriteAt(original[:], crcOffset)
	beforeHits := cowLeafCacheCounter(t, backend, "hits")
	if _, err := s.Get([]byte("a000")); !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("unverified cached record bypassed CRC: %v", err)
	}
	if _, err := db.GetAppend([]byte("a000"), []byte("keep")); !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("public point bypassed CRC: %v", err)
	}
	if _, hit, err := basis.snapshot.CopyVerifiedCachedLeafPage(ptr, f, identity, leafScratch[:]); err != nil || hit {
		t.Fatalf("failed full decode upgraded record: hit=%t %v", hit, err)
	}
	if _, err := writer.WriteAt(original[:], crcOffset); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("verified retry: %q %v", got, err)
	}
	if _, hit, err := basis.snapshot.CopyVerifiedCachedLeafPage(ptr, f, identity, leafScratch[:0:0]); err != nil || hit {
		t.Fatalf("insufficient admitted scratch accepted: hit=%t %v", hit, err)
	}
	if got, err := s.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("verified cache reuse: %q %v", got, err)
	}
	if cowLeafCacheCounter(t, backend, "hits") <= beforeHits {
		t.Fatal("verified retry did not enable actual COW cache hit")
	}
	// Even a verified cached page cannot bypass the original record inspector.
	versionOffset := int64(ptr.Offset)
	var version [1]byte
	if _, err := writer.ReadAt(version[:], versionOffset); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAt([]byte{version[0] ^ 1}, versionOffset); err != nil {
		t.Fatal(err)
	}
	defer writer.WriteAt(version[:], versionOffset)
	if _, err := s.Get([]byte("a000")); !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("verified cache bypassed record inspection: %v", err)
	}
	if _, err := db.Get([]byte("a000")); !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("public point bypassed record inspection: %v", err)
	}
	if _, err := writer.WriteAt(version[:], versionOffset); err != nil {
		t.Fatal(err)
	}
}

// A retained cut whose page was evicted by a later checkpoint must be able to
// reuse the original cache backing after repeated, fully verified reads. The
// one-slot cache makes eviction deterministic without populating it by a helper.
func TestCOWOrderedVerifiedReadRefillsEvictedLeaf(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			db, backend, want := cowLeafCacheModeFixture(t, 1, compressed, "cow_ordered")
			old, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			capture := func(s *Snapshot) page.LeafLogPtr {
				t.Helper()
				var keyScratch, leafScratch [page.PageSize]byte
				var ptr page.LeafLogPtr
				stop := errors.New("capture retained leaf")
				_, err := s.cowCut.basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
					ptr = p
					return nil, stop
				})
				if !errors.Is(err, stop) || ptr.FileID == 0 {
					t.Fatalf("no physical leaf: %+v %v", ptr, err)
				}
				return ptr
			}
			oldPtr := capture(old)
			if err := db.Set([]byte("a000"), []byte("replacement")); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			current, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			if capture(current) == oldPtr {
				t.Fatal("checkpoint did not replace the retained leaf")
			}
			stores := cowLeafCacheCounter(t, backend, "read_miss_admission_stores")
			hits := cowLeafCacheCounter(t, backend, "hits")
			file, _, err := old.cowCut.basis.snapshot.PinnedValueLogFile(oldPtr.ValueLogFileID())
			if err != nil {
				t.Fatal(err)
			}
			writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			var crc [1]byte
			crcOffset := int64(oldPtr.Offset - 4)
			if _, err := writer.ReadAt(crc[:], crcOffset); err != nil {
				t.Fatal(err)
			}
			defer writer.WriteAt(crc[:], crcOffset)
			if _, err := writer.WriteAt([]byte{crc[0] ^ 1}, crcOffset); err != nil {
				t.Fatal(err)
			}
			if _, err := old.Get([]byte("a000")); !errors.Is(err, valuelog.ErrCorrupt) {
				t.Fatalf("evicted corrupt record was admitted: %v", err)
			}
			if cowLeafCacheCounter(t, backend, "read_miss_admission_stores") != stores {
				t.Fatal("failed CRC populated an absent cache entry")
			}
			if _, err := writer.WriteAt(crc[:], crcOffset); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				got, err := old.Get([]byte("a000"))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("retained read %d: %q %v", i, got, err)
				}
			}
			if got := cowLeafCacheCounter(t, backend, "read_miss_admission_stores"); got != stores+1 {
				t.Fatalf("verified repeated reads did not refill evicted page: stores %d -> %d", stores, got)
			}
			if cowLeafCacheCounter(t, backend, "hits") <= hits {
				t.Fatal("refilled retained page did not produce a verified cache hit")
			}
			if got, err := current.Get([]byte("a000")); err != nil || string(got) != "replacement" {
				t.Fatalf("refill changed the current cut: %q %v", got, err)
			}
		})
	}
}
