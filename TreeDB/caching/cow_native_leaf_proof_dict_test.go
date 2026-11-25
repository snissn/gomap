//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func verifyNativeEntryAtRootDictionaryPolicy(t *testing.T, db *DB, backend *backenddb.DB, ptr page.LeafLogPtr, dictID uint64, key, want []byte) {
	t.Helper()
	capRaw, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer capRaw.Close()
	capture := capRaw.(*NativeSemanticCapture)
	basis := capture.cut.basis
	token, ok := basis.snapshot.StateToken()
	if !ok {
		t.Fatal("missing retained physical root")
	}

	f, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID != dictID {
		t.Fatalf("expected dictionary leaf dictID=%d, got %+v err=%v", dictID, shape, err)
	}

	hitsBefore := cowLeafCacheCounter(t, backend, "hits")
	marksBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_marks")
	verifiedHitsBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
	skipsBefore := tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal

	for i := 0; i < 8; i++ {
		entry, err := capture.NativeEntryAtRoot(token.RootPageID, key, true)
		if err != nil || !bytes.Equal(entry.Value, want) {
			t.Fatalf("dictionary NativeEntryAtRoot %d: got %q, err %v", i, entry.Value, err)
		}
		entry.Value[0] ^= 1
	}

	if cowLeafCacheCounter(t, backend, "hits") <= hitsBefore {
		t.Fatal("expected repeated dictionary NativeEntryAtRoot reads to hit verified leaf cache")
	}
	// Dictionary leaves must never carry or reuse a page-checksum proof.
	if got := cowLeafCacheCounter(t, backend, "page_checksum_verified_marks"); got != marksBefore {
		t.Fatalf("dictionary leaf unexpectedly marked page checksum verified: %d -> %d", marksBefore, got)
	}
	if got := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits"); got != verifiedHitsBefore {
		t.Fatalf("dictionary leaf unexpectedly recorded page_checksum_verified_hits: %d -> %d", verifiedHitsBefore, got)
	}
	if got := tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal; got != skipsBefore {
		t.Fatalf("dictionary leaf unexpectedly skipped page checksum verification: %d -> %d", skipsBefore, got)
	}

	// Missing independent dictionary capability must still be refused even when
	// the dictionary leaf is warm in the verified cache.
	savedProvider := capture.dictionary
	capture.dictionary = nil
	_, err = capture.NativeEntryAtRoot(token.RootPageID, key, true)
	capture.dictionary = savedProvider
	if !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
		t.Fatalf("warm cached dictionary leaf bypassed independent capability check: %v", err)
	}

	// Stricter COWReadLimits must still be enforced on every NativeEntryAtRoot
	// call (never bypassed by a FixedScratchLeafCache or broader cached credit).
	strictWorkspace, err := admitCOWReadWorkspace(db.cow.budget, valuelog.COWReadLimits{
		MaxRecordBytes: 1,
		MaxRawBytes:    1,
		MaxValueBytes:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	strictWorkspace.basis = basis.snapshot
	defer strictWorkspace.close()
	if _, err := basis.readNativeEntryWithWorkspace(db.cow, strictWorkspace, savedProvider, token.RootPageID, key, true, nil, false); err == nil {
		t.Fatal("warm cached dictionary leaf bypassed COWReadLimits inspection")
	}

	// A stricter canonical COWReadLimits policy than the native 512 MiB credit
	// must miss credit and consult the dictionary provider.
	db.cow.closePhysicalReader()
	refusal := errors.New("canonical policy definition refused")
	providerCalls := 0
	refusingProvider := cowDictionaryLeafAdmissionStore{
		DictStore: db.dictStore,
		prepare: func(_ uint64, _ valuelog.COWReadLimits, _ int, _ func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
			providerCalls++
			return nil, refusal
		},
	}
	strictWorkspace.limits = db.cow.readLimits()
	if _, err := basis.readNativeEntryWithWorkspace(db.cow, strictWorkspace, refusingProvider, token.RootPageID, key, true, nil, false); !errors.Is(err, refusal) || providerCalls != 1 {
		t.Fatalf("stricter COWReadLimits accepted broader native credit: calls=%d err=%v", providerCalls, err)
	}
}

func newCOWDictionaryCachedLeafFixture(t *testing.T, limits memtable.COWLimits, cacheEntries int) (*DB, *backenddb.DB, page.LeafLogPtr, uint64, []byte, []byte) {
	t.Helper()
	dir := t.TempDir()
	var store *dictdb.Store
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        filepath.Join(dir, "backend"),
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPageReadCacheEntries:   cacheEntries,
		VerifyOnRead:               false,
		ValueLog: backenddb.ValueLogOptions{
			DictLookup: func(id uint64) ([]byte, error) {
				if store == nil {
					return nil, valuelog.ErrMissingDict
				}
				return store.GetDictBytes(context.Background(), id)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	opts := Options{
		TransactionOwner:                    owner,
		DisableWAL:                          true,
		AllowUnsafe:                         true,
		MemtableMode:                        "cow_btree",
		COWMemtableLimits:                   limits,
		MemtableShards:                      2,
		FlushThreshold:                      1 << 30,
		IndexOuterLeavesInValueLog:          true,
		ValueLogPointerThreshold:            256,
		ValueLogCompression:                 uint8(vlogCompressionDict),
		ValueLogAutoPolicy:                  uint8(vlogAutoSize),
		ValueLogCompressionAutotune:         valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
		ValueLogDictIncompressibleHoldBytes: -1,
	}
	opts.ValueLogDictTrain.TrainBytes = -1
	db, err := Open(dir, backend, opts)
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	db.valueLogDictMaxK = 1
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}

	want := bytes.Repeat([]byte("dict-leaf-proof-policy-val|"), 4)
	samples := make([][]byte, 4)
	for sample := range samples {
		leaf := make([]byte, page.PageSize)
		builder := node.NewBuilderWithOptions(leaf, page.PageTypeLeaf, node.BuilderOptions{})
		for i := 0; i < 16; i++ {
			k := []byte(fmt.Sprintf("dict-%05d", sample*16+i))
			if err := builder.AddLeafEntry(k, want, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
		}
		builder.FinishNoNode()
		samples[sample], _, err = valuelog.MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			t.Fatal(err)
		}
	}
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       73,
		Contents: samples,
		History:  append([]byte(nil), samples[0]...),
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err = dictdb.Open(filepath.Join(dir, "dictdb"), backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	dictID, err := store.PutDictBytes(context.Background(), dictionary)
	if err == nil {
		err = store.SetCurrent(context.Background(), dictID)
	}
	if err == nil {
		err = store.SetK(context.Background(), dictID, 1)
	}
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	db.SetDictStore(store)
	backend.SetLeafPageLog(&cowPerfDictionaryLeafLog{
		cowPerfDictionaryLeafLifecycle: &cachingLeafPageLog{db: db, lane: &db.leafLog},
		db:                             db,
		dictID:                         dictID,
		dictionary:                     dictionary,
	})

	key := []byte("dict-00000")
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("dict-%05d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	var ks, ls [page.PageSize]byte
	var ptr page.LeafLogPtr
	stop := errors.New("capture dict leaf pointer")
	_, err = db.cow.cut.basis.snapshot.GetEntryExactWithFixedScratch(key, ks[:], ls[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
		ptr = p
		return nil, stop
	})
	if !errors.Is(err, stop) || ptr.FileID == 0 {
		t.Fatalf("expected dictionary outer leaf ptr, got %+v err=%v", ptr, err)
	}
	return db, backend, ptr, dictID, key, want
}

func TestNativeEntryAtRootDictionaryLeafNeverSkipsPolicy(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 512 << 20
	limits.MaxTotalBytes = 1 << 30
	limits.MaxRetiredBytes = 1 << 30

	t.Run("appender", func(t *testing.T) {
		db, backend, ptr, dictID, key, want := newCOWDictionaryCachedLeafFixture(t, limits, 128)
		verifyNativeEntryAtRootDictionaryPolicy(t, db, backend, ptr, dictID, key, want)
	})

	t.Run("packed", func(t *testing.T) {
		db, backend, ptr, dictID, want := cowNativePackedDictionaryLeafFixtureWithReadPolicy(t, false, limits, 128)
		verifyNativeEntryAtRootDictionaryPolicy(t, db, backend, ptr, dictID, []byte("pack-03000"), want)
	})
}
