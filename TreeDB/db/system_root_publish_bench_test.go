package db

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

type benchSingleKVIterator struct {
	key   []byte
	value []byte
	valid bool
}

func (it *benchSingleKVIterator) Valid() bool { return it != nil && it.valid }

func (it *benchSingleKVIterator) Next() { it.valid = false }

func (it *benchSingleKVIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.valid = bytes.Compare(key, it.key) <= 0
}

func (it *benchSingleKVIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.key
}

func (it *benchSingleKVIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.value
}

func (it *benchSingleKVIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline
	}
	return it.value, page.ValuePtr{}, node.FlagInline
}

func (it *benchSingleKVIterator) Key() []byte {
	return append([]byte(nil), it.UnsafeKey()...)
}

func (it *benchSingleKVIterator) Value() []byte {
	return append([]byte(nil), it.UnsafeValue()...)
}

func (it *benchSingleKVIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeKey()...)
}

func (it *benchSingleKVIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}

func (it *benchSingleKVIterator) IsDeleted() bool { return false }
func (it *benchSingleKVIterator) Error() error    { return nil }
func (it *benchSingleKVIterator) Close() error    { it.valid = false; return nil }
func (it *benchSingleKVIterator) Domain() ([]byte, []byte) {
	return nil, nil
}

func (it *benchSingleKVIterator) Len() int {
	if it.Valid() {
		return 1
	}
	return 0
}

func BenchmarkPublishSystemRootIterator_WarmSparseDelta(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	base := mustFrozenSystemMemtable(b, systemRangeKVs(2048, nil)...)
	if _, err := db.PublishSystemRootIterator(base.NewIterator(nil, nil)); err != nil {
		b.Fatalf("initial publish system root: %v", err)
	}
	left := mustFrozenSystemMemtable(b, systemRangeKVs(2048, map[int]string{17: "value-0017-left"})...)
	right := mustFrozenSystemMemtable(b, systemRangeKVs(2048, map[int]string{17: "value-0017-right"})...)

	start := db.systemRootPublishStatsSnapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table := left
		if i&1 == 1 {
			table = right
		}
		if _, err := db.PublishSystemRootIterator(table.NewIterator(nil, nil)); err != nil {
			b.Fatalf("warm sparse publish: %v", err)
		}
	}
	b.StopTimer()

	end := db.systemRootPublishStatsSnapshot()
	nativeApplies := end.warmNativeApplyAttempts - start.warmNativeApplyAttempts
	fallbacks := end.warmRebuildFallbacks - start.warmRebuildFallbacks
	if nativeApplies != uint64(b.N) {
		b.Fatalf("warmNativeApplyAttempts=%d want %d", nativeApplies, b.N)
	}
	if fallbacks != 0 {
		b.Fatalf("warmRebuildFallbacks=%d want 0", fallbacks)
	}
	b.ReportMetric(float64(nativeApplies), "warm_native_apply")
}

func BenchmarkPublishSystemRootIterator_WarmDenseDelta(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	base := mustFrozenSystemMemtable(b, systemRangeKVs(2048, nil)...)
	if _, err := db.PublishSystemRootIterator(base.NewIterator(nil, nil)); err != nil {
		b.Fatalf("initial publish system root: %v", err)
	}
	leftOverrides := make(map[int]string, 1024)
	rightOverrides := make(map[int]string, 1024)
	for i := 0; i < 1024; i++ {
		leftOverrides[i] = "dense-left"
		rightOverrides[i] = "dense-right"
	}
	left := mustFrozenSystemMemtable(b, systemRangeKVs(2048, leftOverrides)...)
	right := mustFrozenSystemMemtable(b, systemRangeKVs(2048, rightOverrides)...)

	start := db.systemRootPublishStatsSnapshot()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table := left
		if i&1 == 1 {
			table = right
		}
		if _, err := db.PublishSystemRootIterator(table.NewIterator(nil, nil)); err != nil {
			b.Fatalf("warm dense publish: %v", err)
		}
	}
	b.StopTimer()

	end := db.systemRootPublishStatsSnapshot()
	nativeApplies := end.warmNativeApplyAttempts - start.warmNativeApplyAttempts
	fallbacks := end.warmRebuildFallbacks - start.warmRebuildFallbacks
	if nativeApplies != 0 {
		b.Fatalf("warmNativeApplyAttempts=%d want 0", nativeApplies)
	}
	if fallbacks != uint64(b.N) {
		b.Fatalf("warmRebuildFallbacks=%d want %d", fallbacks, b.N)
	}
	b.ReportMetric(float64(fallbacks), "warm_rebuild_fallback")
}

func BenchmarkPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_WarmSingleRoot(b *testing.B) {
	benchmarkPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWarmSingleRoot(b, false, false)
}

func BenchmarkPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_WarmSingleRootReadOnlyPrepare(b *testing.B) {
	benchmarkPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWarmSingleRoot(b, true, false)
}

func BenchmarkPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_WarmSingleRootReadOnlyPrepareReuse(b *testing.B) {
	benchmarkPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWarmSingleRoot(b, true, true)
}

func benchmarkPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderWarmSingleRoot(b *testing.B, prepareReadOnly, reusePrepare bool) {
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(b, "root/a", "base").NewIterator(nil, nil))
	if err != nil {
		b.Fatalf("publish base root: %v", err)
	}
	left := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := left.Set([]byte("root/a"), []byte("left")); err != nil {
		b.Fatalf("set left delta: %v", err)
	}
	defer func() { _ = left.Close() }()
	right := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := right.Set([]byte("root/a"), []byte("right")); err != nil {
		b.Fatalf("set right delta: %v", err)
	}
	defer func() { _ = right.Close() }()

	ordered := []OrderedRootDeltaBatchPublishInput{{
		StoragePolicy:   OrderedRootStorageDefault,
		PrepareReadOnly: prepareReadOnly,
	}}
	var prepared zipper.ReadOnlyPrepareResult
	systemKey := []byte("sys/collections/users/primary")
	var systemValueBuf [20]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		delta := left
		if i&1 == 1 {
			delta = right
		}
		ordered[0].BaseRoot = baseRoot
		ordered[0].Delta = delta
		if prepareReadOnly && reusePrepare {
			ordered[0].ReadOnlyPrepareOptions = prepared.ReuseOptions()
			ordered[0].ReadOnlyPrepareResult = &prepared
		}
		_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			value := strconv.AppendUint(systemValueBuf[:0], rootIDs[0], 10)
			return &benchSingleKVIterator{
				key:   systemKey,
				value: value,
				valid: true,
			}, nil
		})
		if err != nil {
			b.Fatalf("publish batch group: %v", err)
		}
		baseRoot = rootIDs[0]
	}
}
