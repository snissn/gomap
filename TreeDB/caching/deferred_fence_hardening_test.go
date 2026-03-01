package caching

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func parseUintStat(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return n
}

func deferredFenceValue(i int, large bool) []byte {
	size := 160
	if large {
		size = 2048
	}
	v := bytes.Repeat([]byte{byte(i % 251)}, size)
	return append([]byte(fmt.Sprintf("v-%04d-", i)), v...)
}

func TestCachingDB_DeferredFence_WALOff_FlushThenCheckpointPreservesLatest(t *testing.T) {
	dir := t.TempDir()
	backendOpts := db.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: db.IndexOuterLeafModeV1,
		ValueLog: db.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           db.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     1 << 20,
			OuterLeafBlockRestartInterval: 16,
			OuterLeafBlobThresholdBytes:   256,
		},
	}
	backend, err := db.Open(backendOpts)
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                         true,
		DisableWAL:                          true,
		FlushThreshold:                      1 << 30,
		MemtableShards:                      1,
		IndexOuterLeafMode:                  db.IndexOuterLeafModeV1,
		ValueLogPointerThreshold:            1,
		ValueLogOuterLeafBlockCodec:         uint8(db.ValueLogBlockLZ4),
		ValueLogOuterLeafBlockTargetBytes:   1 << 20,
		ValueLogOuterLeafBlobThresholdBytes: 256,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}

	const totalKeys = 96
	live := make(map[string][]byte, totalKeys)
	deleted := make(map[string]struct{})

	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := deferredFenceValue(i, i%3 == 0)
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Set round1(%s): %v", key, err)
		}
		live[string(key)] = value
	}
	cache.flushAll(false)

	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		if i%5 == 0 {
			if err := cache.Delete(key); err != nil {
				t.Fatalf("Delete(%s): %v", key, err)
			}
			delete(live, string(key))
			deleted[string(key)] = struct{}{}
			continue
		}
		value := deferredFenceValue(1000+i, i%4 == 0)
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Set round2(%s): %v", key, err)
		}
		live[string(key)] = value
	}
	cache.flushAll(false)

	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	stats := cache.Stats()
	enqueued := parseUintStat(t, stats, "treedb.cache.v2_fenceptr.deferred_enqueued_keys")
	materialized := parseUintStat(t, stats, "treedb.cache.v2_fenceptr.deferred_materialized_keys")
	if enqueued == 0 || materialized == 0 {
		t.Fatalf("expected deferred work, enqueued=%d materialized=%d", enqueued, materialized)
	}
	if materialized > enqueued {
		t.Fatalf("materialized keys exceeds enqueued keys: materialized=%d enqueued=%d", materialized, enqueued)
	}

	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	leafEntries := countSnapshotLeafEntries(t, snap)
	_ = snap.Close()
	// v2_fenceptr stores fence anchors in index leaves; one leaf entry does not
	// necessarily map 1:1 with one logical live key.
	if leafEntries == 0 || leafEntries > len(live) {
		t.Fatalf("leaf entry count=%d out of bounds for live=%d", leafEntries, len(live))
	}

	for key, want := range live {
		got, err := cache.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%s) mismatch got_len=%d want_len=%d", key, len(got), len(want))
		}
	}
	for key := range deleted {
		got, err := cache.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(deleted %s): %v", key, err)
		}
		if got != nil {
			t.Fatalf("expected deleted key %s to be absent, got len=%d", key, len(got))
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("cache close: %v", err)
	}

	reopened, err := db.Open(backendOpts)
	if err != nil {
		t.Fatalf("backend reopen: %v", err)
	}
	defer reopened.Close()

	for key, want := range live {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("reopen Get(%s): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen Get(%s) mismatch got_len=%d want_len=%d", key, len(got), len(want))
		}
	}
	for key := range deleted {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("reopen Get(deleted %s): %v", key, err)
		}
		if got != nil {
			t.Fatalf("expected deleted key %s absent after reopen, got len=%d", key, len(got))
		}
	}
}

func TestCachingDB_DeferredFence_WALOn_FenceModeFlushCheckpointMatrix(t *testing.T) {
	cases := []struct {
		name          string
		walFenceMode  string
		forcePointers bool
	}{
		{name: "simple_inline_force_pointers", walFenceMode: string(db.ValueLogWALFenceModeSimpleInline), forcePointers: true},
		{name: "rid_join_mixed_sizes", walFenceMode: string(db.ValueLogWALFenceModeRIDJoin), forcePointers: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			backendOpts := db.Options{
				Dir:                dir,
				ChunkSize:          64 * 1024,
				IndexOuterLeafMode: db.IndexOuterLeafModeV1,
				ValueLog: db.ValueLogOptions{
					PointerThreshold:              1,
					ForcePointers:                 tc.forcePointers,
					WALFenceMode:                  db.ValueLogWALFenceMode(tc.walFenceMode),
					OuterLeafBlockCodec:           db.ValueLogBlockLZ4,
					OuterLeafBlockTargetBytes:     1 << 20,
					OuterLeafBlockRestartInterval: 16,
					OuterLeafBlobThresholdBytes:   256,
				},
			}
			backend, err := db.Open(backendOpts)
			if err != nil {
				t.Fatalf("backend open: %v", err)
			}

			cache, err := Open(dir, backend, Options{
				FlushThreshold:                      1 << 30,
				MemtableShards:                      1,
				IndexOuterLeafMode:                  db.IndexOuterLeafModeV1,
				ValueLogWALFenceMode:                tc.walFenceMode,
				ForceValueLogPointers:               tc.forcePointers,
				ValueLogPointerThreshold:            1,
				ValueLogOuterLeafBlockCodec:         uint8(db.ValueLogBlockLZ4),
				ValueLogOuterLeafBlockTargetBytes:   1 << 20,
				ValueLogOuterLeafBlobThresholdBytes: 256,
			})
			if err != nil {
				_ = backend.Close()
				t.Fatalf("cache open: %v", err)
			}

			const totalKeys = 80
			live := make(map[string][]byte, totalKeys)
			deleted := make(map[string]struct{})

			for i := 0; i < totalKeys; i++ {
				key := []byte(fmt.Sprintf("k%04d", i))
				value := deferredFenceValue(i, i%2 == 0)
				if err := cache.Set(key, value); err != nil {
					t.Fatalf("Set round1(%s): %v", key, err)
				}
				live[string(key)] = value
			}
			cache.flushAll(false)

			for i := 0; i < totalKeys; i++ {
				key := []byte(fmt.Sprintf("k%04d", i))
				if i%7 == 0 {
					if err := cache.Delete(key); err != nil {
						t.Fatalf("Delete(%s): %v", key, err)
					}
					delete(live, string(key))
					deleted[string(key)] = struct{}{}
					continue
				}
				value := deferredFenceValue(2000+i, i%3 == 0)
				if err := cache.SetSync(key, value); err != nil {
					t.Fatalf("SetSync round2(%s): %v", key, err)
				}
				live[string(key)] = value
			}
			cache.flushAll(false)

			if err := cache.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			stats := cache.Stats()
			if got := stats["treedb.cache.v2_fenceptr.wal_fence_mode"]; got != tc.walFenceMode {
				t.Fatalf("wal_fence_mode stat=%q want=%q", got, tc.walFenceMode)
			}
			enqueued := parseUintStat(t, stats, "treedb.cache.v2_fenceptr.deferred_enqueued_keys")
			materialized := parseUintStat(t, stats, "treedb.cache.v2_fenceptr.deferred_materialized_keys")
			if enqueued == 0 || materialized == 0 {
				t.Fatalf("expected deferred work, enqueued=%d materialized=%d", enqueued, materialized)
			}

			snap := backend.AcquireSnapshot()
			if snap == nil {
				t.Fatalf("snapshot nil")
			}
			leafEntries := countSnapshotLeafEntries(t, snap)
			_ = snap.Close()
			// v2_fenceptr stores fence anchors in index leaves; one leaf entry does
			// not necessarily map 1:1 with one logical live key.
			if leafEntries == 0 || leafEntries > len(live) {
				t.Fatalf("leaf entry count=%d out of bounds for live=%d", leafEntries, len(live))
			}

			if err := cache.Close(); err != nil {
				t.Fatalf("cache close: %v", err)
			}

			reopened, err := db.Open(backendOpts)
			if err != nil {
				t.Fatalf("backend reopen: %v", err)
			}
			defer reopened.Close()

			for key, want := range live {
				got, err := reopened.Get([]byte(key))
				if err != nil {
					t.Fatalf("reopen Get(%s): %v", key, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("reopen Get(%s) mismatch got_len=%d want_len=%d", key, len(got), len(want))
				}
			}
			for key := range deleted {
				got, err := reopened.Get([]byte(key))
				if err != nil {
					t.Fatalf("reopen Get(deleted %s): %v", key, err)
				}
				if got != nil {
					t.Fatalf("expected deleted key %s absent after reopen, got len=%d", key, len(got))
				}
			}
		})
	}
}
