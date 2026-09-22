package caching

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func TestMVCCPhysicalVersionsShareMutableShard(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     16,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	logical := []byte{'d', 0, 'g', 'r', 'a', 'p', 'h'}
	want := -1
	for _, timestamp := range []uint64{1, 2, 99, ^uint64(0)} {
		physical, err := mvcckey.Encode(logical, timestamp)
		if err != nil {
			t.Fatalf("Encode(%d): %v", timestamp, err)
		}
		got := db.shardIndex(physical)
		if want < 0 {
			want = got
		} else if got != want {
			t.Fatalf("timestamp %d shard=%d want stable shard %d", timestamp, got, want)
		}
	}
	malformedSuffix, err := mvcckey.Encode(logical, 100)
	if err != nil {
		t.Fatalf("Encode malformed suffix base: %v", err)
	}
	malformedSuffix = append(malformedSuffix, 0)
	if got := db.shardIndex(malformedSuffix); got != want {
		t.Fatalf("malformed MVCC suffix shard=%d want logical-key shard %d", got, want)
	}

	raw := []byte("raw-key-keeps-legacy-hash")
	wantRaw := int(xxhash.Sum64(raw) & db.mutableShardMask)
	if got := db.shardIndex(raw); got != wantRaw {
		t.Fatalf("raw shard=%d want legacy full-key shard %d", got, wantRaw)
	}
	logicalShards := make(map[int]struct{})
	for i := 0; i < 64; i++ {
		physical, err := mvcckey.Encode([]byte(fmt.Sprintf("logical-%03d", i)), 1)
		if err != nil {
			t.Fatalf("Encode logical %d: %v", i, err)
		}
		logicalShards[db.shardIndex(physical)] = struct{}{}
	}
	if len(logicalShards) < 8 {
		t.Fatalf("64 logical keys reached only %d/16 shards", len(logicalShards))
	}
}

func TestSeekGEExactMVCCRangeKeepsSyntheticLowerBoundOnLogicalShard(t *testing.T) {
	logical := []byte("pruned-delete-anchor")
	version20, err := mvcckey.Encode(logical, 20)
	if err != nil {
		t.Fatalf("Encode version 20: %v", err)
	}
	version10, err := mvcckey.Encode(logical, 10)
	if err != nil {
		t.Fatalf("Encode version 10: %v", err)
	}
	lower, err := mvcckey.Encode(logical, 26)
	if err != nil {
		t.Fatalf("Encode lower: %v", err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		t.Fatalf("AppendKeyVersionsUpper: %v", err)
	}
	backend := NewMockBackend()
	backend.Set(version20, []byte("stale-20"))
	backend.Set(version10, []byte("stale-10"))
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     16,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Delete(version20); err != nil {
		t.Fatalf("Delete version 20: %v", err)
	}
	if err := db.Delete(version10); err != nil {
		t.Fatalf("Delete version 10: %v", err)
	}
	if key, value, found, err := db.SeekGE(lower, upper); err != nil || found {
		t.Fatalf("SeekGE after physical deletes: key=%x value=%q found=%t err=%v", key, value, found, err)
	}
}

func TestSeekGEExactMVCCRangeDoesNotHideMalformedSuffix(t *testing.T) {
	logical := []byte("malformed-record")
	lower, err := mvcckey.Encode(logical, 9)
	if err != nil {
		t.Fatalf("Encode lower: %v", err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		t.Fatalf("AppendKeyVersionsUpper: %v", err)
	}
	malformed := append(append([]byte(nil), lower...), 0)
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     16,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Set(malformed, []byte("corrupt")); err != nil {
		t.Fatalf("Set malformed suffix: %v", err)
	}
	key, value, found, err := db.SeekGE(lower, upper)
	if err != nil || !found || !bytes.Equal(key, malformed) || !bytes.Equal(value, []byte("corrupt")) {
		t.Fatalf("SeekGE malformed suffix: key=%x value=%q found=%t err=%v", key, value, found, err)
	}
}

func TestSeekGEExactMVCCRangeDoesNotHideOversizedMalformedSuffix(t *testing.T) {
	logical := []byte("oversized-malformed-record")
	lower, err := mvcckey.Encode(logical, 9)
	if err != nil {
		t.Fatalf("Encode lower: %v", err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		t.Fatalf("AppendKeyVersionsUpper: %v", err)
	}
	malformed := append(append([]byte(nil), lower...), bytes.Repeat([]byte{0xa5}, mvcckey.MaxEncodedKeySize-len(lower)+1)...)
	if len(malformed) <= mvcckey.MaxEncodedKeySize {
		t.Fatalf("malformed length=%d want > %d", len(malformed), mvcckey.MaxEncodedKeySize)
	}
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:         true,
		FlushThreshold:     1 << 30,
		MemtableShards:     16,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Set(malformed, []byte("corrupt")); err != nil {
		t.Fatalf("Set oversized malformed suffix: %v", err)
	}
	key, value, found, err := db.SeekGE(lower, upper)
	if err != nil || !found || !bytes.Equal(key, malformed) || !bytes.Equal(value, []byte("corrupt")) {
		t.Fatalf("SeekGE oversized malformed suffix: key_len=%d value=%q found=%t err=%v", len(key), value, found, err)
	}
}

func TestSeekGEExactMVCCRangeUsesSameShardImmutablePrecedence(t *testing.T) {
	logical := []byte("queued-version")
	version, err := mvcckey.Encode(logical, 20)
	if err != nil {
		t.Fatalf("Encode version: %v", err)
	}
	lower, err := mvcckey.Encode(logical, 26)
	if err != nil {
		t.Fatalf("Encode lower: %v", err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		t.Fatalf("AppendKeyVersionsUpper: %v", err)
	}

	t.Run("hit", func(t *testing.T) {
		db, err := Open(t.TempDir(), NewMockBackend(), Options{
			FlushThreshold:     1 << 30,
			MemtableShards:     16,
			MaxQueuedMemtables: -1,
			AllowUnsafe:        true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		db.flushMu.Lock()
		t.Cleanup(db.flushMu.Unlock)
		if err := db.Set(version, []byte("queued")); err != nil {
			t.Fatalf("Set queued version: %v", err)
		}
		rotatePointSuccessorMemtables(t, db)
		before := db.Stats()
		key, value, found, err := db.SeekGE(lower, upper)
		if err != nil || !found || !bytes.Equal(key, version) || !bytes.Equal(value, []byte("queued")) {
			t.Fatalf("SeekGE queued version: key=%x value=%q found=%t err=%v", key, value, found, err)
		}
		after := db.Stats()
		if got := pointSuccessorStatDelta(t, before, after, "treedb.cache.point_successor.queue_hits_total"); got != 1 {
			t.Fatalf("queue hits=%d want 1", got)
		}
	})

	t.Run("physical tombstone", func(t *testing.T) {
		backend := NewMockBackend()
		backend.Set(version, []byte("stale"))
		db, err := Open(t.TempDir(), backend, Options{
			FlushThreshold:     1 << 30,
			MemtableShards:     16,
			MaxQueuedMemtables: -1,
			AllowUnsafe:        true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		db.flushMu.Lock()
		t.Cleanup(db.flushMu.Unlock)
		if err := db.Delete(version); err != nil {
			t.Fatalf("Delete version: %v", err)
		}
		rotatePointSuccessorMemtables(t, db)
		if key, value, found, err := db.SeekGE(lower, upper); err != nil || found {
			t.Fatalf("SeekGE queued tombstone: key=%x value=%q found=%t err=%v", key, value, found, err)
		}
	})
}

func TestSeekGEExactMVCCRangeFallsBackForRangeSpans(t *testing.T) {
	logical := []byte("range-span-control")
	lower, err := mvcckey.Encode(logical, 9)
	if err != nil {
		t.Fatalf("Encode lower: %v", err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		t.Fatalf("AppendKeyVersionsUpper: %v", err)
	}
	backend := NewMockBackend()
	backend.Set(lower, []byte("published"))
	db, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 30,
		MemtableShards:     16,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("raw-range-a"), []byte("raw-range-z"), func() error { return nil }); err != nil {
		t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
	}
	if got := deleteRangeStatUint64(t, db.Stats(), "treedb.cache.range_span.active_spans"); got != 1 {
		t.Fatalf("active range spans=%d want 1", got)
	}
	before := db.Stats()
	key, value, found, err := db.SeekGE(lower, upper)
	if err != nil || !found || !bytes.Equal(key, lower) || !bytes.Equal(value, []byte("published")) {
		t.Fatalf("SeekGE with unrelated range span: key=%x value=%q found=%t err=%v", key, value, found, err)
	}
	after := db.Stats()
	if got := pointSuccessorStatDelta(t, before, after, "treedb.cache.point_successor.mutable_probes_total"); got != 16 {
		t.Fatalf("mutable probes=%d want generic all-shard fallback 16", got)
	}
}

func pointSuccessorStatDelta(t testing.TB, before, after map[string]string, name string) uint64 {
	t.Helper()
	start, startErr := strconv.ParseUint(before[name], 10, 64)
	end, endErr := strconv.ParseUint(after[name], 10, 64)
	if startErr != nil || endErr != nil || end < start {
		t.Fatalf("invalid %s counter %q -> %q", name, before[name], after[name])
	}
	return end - start
}

func TestSeekGEExactMVCCRangeUsesShardLocalInMemorySources(t *testing.T) {
	const queuedSources = 32
	logical := []byte("published-dgraph-posting")
	lower, err := mvcckey.Encode(logical, 50)
	if err != nil {
		t.Fatalf("Encode lower: %v", err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		t.Fatalf("AppendKeyVersionsUpper: %v", err)
	}
	backend := NewMockBackend()
	backend.Set(lower, []byte("published"))
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     16,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.flushMu.Lock()
	t.Cleanup(db.flushMu.Unlock)
	target := db.shardIndex(lower)
	created := 0
	for candidate := 0; created < queuedSources; candidate++ {
		key := []byte(fmt.Sprintf("unrelated-%06d", candidate))
		if db.shardIndex(key) == target {
			continue
		}
		if err := db.Set(key, []byte("queued")); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
		rotatePointSuccessorMemtables(t, db)
		created++
	}

	before := db.Stats()
	key, value, found, err := db.SeekGE(lower, upper)
	if err != nil || !found || !bytes.Equal(key, lower) || !bytes.Equal(value, []byte("published")) {
		t.Fatalf("SeekGE legacy/global published root: key=%x value=%q found=%t err=%v", key, value, found, err)
	}
	after := db.Stats()
	if got := pointSuccessorStatDelta(t, before, after, "treedb.cache.point_successor.mutable_probes_total"); got != 1 {
		t.Fatalf("mutable probes=%d want 1 logical-key shard", got)
	}
	if got := pointSuccessorStatDelta(t, before, after, "treedb.cache.point_successor.queue_probes_total"); got != 0 {
		t.Fatalf("queue probes=%d want 0 unrelated shard queues", got)
	}
	if got := pointSuccessorStatDelta(t, before, after, "treedb.cache.point_successor.backend_probes_total"); got != 1 {
		t.Fatalf("backend probes=%d want authoritative global root", got)
	}
	if got := pointSuccessorStatDelta(t, before, after, "treedb.cache.point_successor.sources_total"); got != 2 {
		t.Fatalf("sources=%d want target mutable plus global root", got)
	}
}

func BenchmarkSeekGEExactMVCCShardAffinityPublishedHit(b *testing.B) {
	const queuedSources = 96
	logical := []byte("published-dgraph-posting")
	lower, err := mvcckey.Encode(logical, 50)
	if err != nil {
		b.Fatalf("Encode lower: %v", err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		b.Fatalf("AppendKeyVersionsUpper: %v", err)
	}
	backend := NewMockBackend()
	backend.Set(lower, []byte("published"))
	db, err := Open(b.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     16,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	db.flushMu.Lock()
	b.Cleanup(db.flushMu.Unlock)
	target := db.shardIndex(lower)
	created := 0
	for candidate := 0; created < queuedSources; candidate++ {
		key := []byte(fmt.Sprintf("unrelated-%06d", candidate))
		if db.shardIndex(key) == target {
			continue
		}
		if err := db.Set(key, []byte("queued")); err != nil {
			b.Fatalf("Set(%q): %v", key, err)
		}
		db.mu.Lock()
		err = db.rotateMemtableLocked(false)
		db.mu.Unlock()
		if err != nil {
			b.Fatalf("rotateMemtableLocked: %v", err)
		}
		created++
	}
	forcedGenericUpper := append(append([]byte(nil), upper...), 0)
	for _, tc := range []struct {
		name  string
		upper []byte
	}{
		{name: "exact_mvcc_shard", upper: upper},
		{name: "forced_generic_control", upper: forcedGenericUpper},
	} {
		b.Run(tc.name, func(b *testing.B) {
			before := db.Stats()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key, value, found, err := db.SeekGE(lower, tc.upper)
				if err != nil || !found || !bytes.Equal(key, lower) || !bytes.Equal(value, []byte("published")) {
					b.Fatalf("SeekGE: key=%x value=%q found=%t err=%v", key, value, found, err)
				}
			}
			b.StopTimer()
			after := db.Stats()
			reportPointSuccessorBenchCounter(b, before, after, "treedb.cache.point_successor.sources_total", "sources/op")
			reportPointSuccessorBenchCounter(b, before, after, "treedb.cache.point_successor.mutable_probes_total", "mutable_probes/op")
			reportPointSuccessorBenchCounter(b, before, after, "treedb.cache.point_successor.queue_probes_total", "queue_probes/op")
			reportPointSuccessorBenchCounter(b, before, after, "treedb.cache.point_successor.backend_probes_total", "backend_probes/op")
		})
	}
}
