package treedb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func cowPublicOwnedDictionary(t *testing.T, tag byte) ([]byte, []byte) {
	t.Helper()
	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = bytes.Repeat([]byte("generation-owned-dictionary-value/"), 512)
		samples[i][0] = tag
		samples[i][1] = byte(i)
	}
	history := append(bytes.Clone(samples[0]), samples[1]...)
	definition, err := zstd.BuildDict(zstd.BuildDictOptions{ID: uint32(tag) + 1, Contents: samples, History: history, Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest})
	if err != nil {
		t.Fatal(err)
	}
	return definition, samples[3]
}

// This uses the public producer, its actual encoded frame, a distinct Manager
// sharing the backend deletion authority, and both cached and backend cuts.
func TestCOWPublicCompressedDictionaryAndPhysicalPinLifetime(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, _ := cowPublicContractOpen(t, profile, func(o *Options) {
				o.DisableSideStores = false
				o.DisableBackgroundPrune = true
				o.BackgroundCheckpointInterval = -1
				o.BackgroundCheckpointIdleDuration = -1
				o.BackgroundIndexVacuumInterval = -1
				o.MaxWALBytes = -1
				o.ValueLog.Compression = ValueLogCompressionDict
				o.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
				o.ValueLog.DictAdaptiveRatio = -1
				EnableValueLogDictCompression(o)
			})
			store := dictdb.New(database.dictdb)
			definition, want := cowPublicOwnedDictionary(t, 'A')
			id, err := store.PutDictBytes(context.Background(), definition)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.SetCurrent(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			database.cached.SetDictStore(store)
			if err = database.Set([]byte("compressed"), want); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("missing old cut")
			}
			defer old.Close()
			entry, err := old.GetEntry([]byte("compressed"))
			if err != nil || entry.ValuePtr.FileID == 0 {
				t.Fatalf("pointer=%+v error=%v", entry, err)
			}
			registry := database.backend.ValueLogIdentityPinRegistry()
			other, err := valuelog.NewManagerWithStableResourcePinRegistry(database.backend.Dir()+"/value_vlog", registry)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			subset := other.CurrentSubsetNoRefresh(map[uint32]struct{}{entry.ValuePtr.FileID: {}})
			if subset == nil || subset.Files[entry.ValuePtr.FileID] == nil {
				t.Fatal("missing registered producer file")
			}
			file := subset.Files[entry.ValuePtr.FileID]
			shape, err := valuelog.InspectCOWRecord(file.File, entry.ValuePtr, valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20})
			identity, ok := other.StableSegmentIdentity(entry.ValuePtr.FileID)
			other.Release(subset)
			if err != nil || !shape.Compressed || shape.DictID != id {
				t.Fatalf("actual producer shape=%+v error=%v want dictionary=%d", shape, err, id)
			}
			if !ok {
				t.Fatal("missing physical identity")
			}
			if lease, err := registry.BeginDelete(identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
				if lease != nil {
					lease.Abort()
				}
				t.Fatalf("cross-manager delete while cached generation lives=%v", err)
			}
			cowPublicContractSnapshotValue(t, old, "compressed", want, true)
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			backendCut := database.AcquireSnapshot()
			if backendCut == nil {
				t.Fatal("missing backend cut")
			}
			defer backendCut.Close()
			cowPublicContractSnapshotValue(t, backendCut, "compressed", want, true)
			replacement, newValue := cowPublicOwnedDictionary(t, 'B')
			next, err := store.PutDictBytes(context.Background(), replacement)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.SetCurrent(context.Background(), next); err != nil {
				t.Fatal(err)
			}
			database.cached.SetDictStore(store)
			if err = database.Set([]byte("compressed"), newValue); err != nil {
				t.Fatal(err)
			}
			if err = database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if stats, err := database.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{BatchSize: 1, SyncEachBatch: true}); err != nil {
				t.Fatal(err)
			} else if stats.ValueRecordsCopied == 0 {
				t.Fatal("rewrite did not copy current compressed pointer")
			}
			if _, err = database.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
				t.Fatal(err)
			}
			cowPublicContractSnapshotValue(t, old, "compressed", want, true)
			cowPublicContractSnapshotValue(t, backendCut, "compressed", want, true)
			if got, err := database.Get([]byte("compressed")); err != nil || !bytes.Equal(got, newValue) {
				t.Fatalf("current after marker/GC=(%d,%v)", len(got), err)
			}
			if lease, err := registry.BeginDelete(identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
				if lease != nil {
					lease.Abort()
				}
				t.Fatalf("cross-manager delete while historical cuts live=%v", err)
			}
			old.Close()
			backendCut.Close()
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			// Delete admission is terminal after DB authority closes. The second
			// manager still owns its original observer edge until actual Close;
			// post-close readonly diagnostics prove drain without reopening authority.
			if lease, err := registry.BeginDelete(identity); !errors.Is(err, retainedalloc.ErrClosed) {
				if lease != nil {
					lease.Abort()
				}
				t.Fatalf("closed physical authority admitted deletion: %v", err)
			}
			if err := other.Close(); err != nil {
				t.Fatal(err)
			}
			stats := registry.Stats()
			if stats.ActivePins != 0 || stats.ActiveIdentities != 0 || registry.CleanupError() != nil {
				t.Fatalf("physical custody retained after all managers/cuts close: stats=%+v cleanup=%v", stats, registry.CleanupError())
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got, err := reopened.Get([]byte("compressed")); err != nil || !bytes.Equal(got, newValue) {
				t.Fatalf("reopened dictionary=(%d,%v)", len(got), err)
			}
		})
	}
}
