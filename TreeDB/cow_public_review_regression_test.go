package treedb

import (
	"bytes"
	"context"

	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/caching"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWPublicSnapshotReadCloseConcurrency(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			value := bytes.Repeat([]byte("close-owned"), 32)
			if err := database.Set([]byte("a"), value); err != nil {
				t.Fatal(err)
			}
			for repeat := 0; repeat < 32; repeat++ {
				snapshot := database.AcquireSnapshot()
				if snapshot == nil {
					t.Fatal("missing snapshot")
				}
				start := make(chan struct{})
				failures := make(chan error, 2)
				var readers sync.WaitGroup
				for _, exact := range []bool{false, true} {
					readers.Add(1)
					go func(exact bool) {
						defer readers.Done()
						<-start
						for i := 0; i < 64; i++ {
							if exact {
								entry, err := snapshot.GetEntryExact([]byte("a"))
								if errors.Is(err, ErrClosed) {
									continue
								}
								if err != nil || entry.ValuePtr.FileID == 0 {
									failures <- fmt.Errorf("exact: %+v, %v", entry, err)
									return
								}
							} else {
								got, err := snapshot.Get([]byte("a"))
								if errors.Is(err, ErrClosed) {
									continue
								}
								if err != nil || !bytes.Equal(got, value) {
									failures <- fmt.Errorf("Get: %q, %v", got, err)
									return
								}
							}
						}
					}(exact)
				}
				close(start)
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				readers.Wait()
				close(failures)
				for err := range failures {
					t.Error(err)
				}
			}
		})
	}
}

func TestCOWPublicIterateCallbackStorageAdmission(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile, func(opts *Options) {
				opts.ValueLog.ForcePointers = false
				opts.ValueLog.PointerThreshold = 4 << 20
			})
			value := bytes.Repeat([]byte("i"), 1<<20)
			if err := database.Set([]byte("a"), value); err != nil {
				t.Fatal(err)
			}
			snapshot := database.AcquireSnapshot()
			if snapshot == nil {
				t.Fatal("missing snapshot")
			}
			defer snapshot.Close()
			before := database.cached.COWMemoryStats()
			pressure, err := database.cached.AcquireCOWAllocation(memtable.DefaultCOWLimits().MaxInFlightBytes - before.ReservedBytes - (256 << 10))
			if err != nil {
				t.Fatal(err)
			}
			called := false
			err = snapshot.Iterate(nil, nil, func(_, _ []byte) error { called = true; return nil })
			pressure.Close()
			if !errors.Is(err, memtable.ErrCOWCapacity) || called {
				t.Fatalf("callback storage pressure: called=%v error=%v", called, err)
			}
			stop := errors.New("callback stop")
			err = snapshot.Iterate(nil, nil, func(key, got []byte) error {
				if string(key) != "a" || !bytes.Equal(got, value) {
					t.Fatal("wrong callback value")
				}
				charged := database.cached.COWMemoryStats()
				if charged.ReservedBytes < before.ReservedBytes+uint64(len(value)) {
					t.Fatal("callback backing is not retained in shared admission")
				}
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				if err := database.Set([]byte("b"), []byte("reentrant")); err != nil {
					t.Fatal(err)
				}
				return stop
			})
			if !errors.Is(err, stop) {
				t.Fatalf("callback reentry: %v", err)
			}
		})
	}
}

func TestCOWPublicEmptyBatchWriteAndSync(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			command := database.NewBatch()
			defer command.Close()
			before := database.Stats()
			if err := command.Write(); err != nil {
				t.Fatal(err)
			}
			if err := command.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if after := database.Stats(); after["treedb.cache.cow.publications_total"] != before["treedb.cache.cow.publications_total"] || after["treedb.cache.cow.prepare_calls_total"] != before["treedb.cache.cow.prepare_calls_total"] {
				t.Fatalf("empty command prepared/published: before=%+v after=%+v", before, after)
			}
			value := bytes.Repeat([]byte("empty-sync-predecessor"), 32)
			if err := command.Set([]byte("a"), value); err != nil {
				t.Fatal(err)
			}
			if err := command.Write(); err != nil {
				t.Fatal(err)
			}
			before = database.Stats()
			if err := command.Write(); err != nil {
				t.Fatal(err)
			}
			if err := command.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if profile != ProfileNoWALFast {
				after := database.Stats()
				if statMapUint64(t, after, "treedb.command_wal.durable_wal_lsn") < statMapUint64(t, before, "treedb.command_wal.live_accepted_max_lsn") {
					t.Fatal("empty sync did not cover the preceding accepted command")
				}
				if profile == ProfileCommandWALRelaxed && statMapUint64(t, after, "treedb.command_wal.file_sync.calls_total") <= statMapUint64(t, before, "treedb.command_wal.file_sync.calls_total") {
					t.Fatal("empty sync did not sync the preceding relaxed command")
				}
			}
			if after := database.Stats(); after["treedb.cache.cow.publications_total"] != before["treedb.cache.cow.publications_total"] || after["treedb.cache.cow.prepare_calls_total"] != before["treedb.cache.cow.prepare_calls_total"] {
				t.Fatalf("reset command prepared/published: before=%+v after=%+v", before, after)
			}
			if profile == ProfileNoWALFast {
				if got, err := database.backend.Get([]byte("a")); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("empty sync omitted predecessor checkpoint: %q, %v", got, err)
				}
			}
		})
	}
}

func TestCOWPublicSelectedSourceRewriteKeepsOldCuts(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, _ := cowPublicContractOpen(t, profile)
			value := bytes.Repeat([]byte("selected-rewrite"), 64)
			before := database.Stats()
			if _, err := database.CompactStorage(context.Background(), CompactStorageOptions{UnsafeValueLogReclaimFencedUnreferenced: true}); !errors.Is(err, caching.ErrCOWUnsupported) {
				t.Fatalf("specialized pre-held fence refusal: %v", err)
			}
			if after := database.Stats(); after["treedb.cache.cow.handoffs_total"] != before["treedb.cache.cow.handoffs_total"] {
				t.Fatal("unsupported compaction option checkpointed before refusal")
			}
			if err := database.Set([]byte("a"), value); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("missing old cut")
			}
			defer old.Close()
			entry, err := old.GetEntry([]byte("a"))
			if err != nil || entry.ValuePtr.FileID == 0 {
				t.Fatalf("pointer entry=%+v error=%v", entry, err)
			}
			stats, err := database.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{SourceFileIDs: []uint32{entry.ValuePtr.FileID}, BatchSize: 1, SyncEachBatch: true})
			if err != nil {
				t.Fatal(err)
			}
			if stats.ValueRecordsCopied == 0 {
				t.Fatal("selected rewrite copied no values")
			}
			cowPublicContractSnapshotValue(t, old, "a", value, true)
			current := database.AcquireSnapshot()
			if current == nil {
				t.Fatal("missing current cut")
			}
			cowPublicContractSnapshotValue(t, current, "a", value, true)
			_, revision, err := current.GetVersioned([]byte("a"))
			if err != nil || revision != entry.Revision {
				t.Fatalf("rewrite changed logical revision: got=%d want=%d error=%v", revision, entry.Revision, err)
			}
			current.Close()
			if _, err := database.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
				t.Fatalf("ordinary compaction after selected rewrite: %v", err)
			}
			if err := database.Set([]byte("a"), []byte("late")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			cowPublicContractSnapshotValue(t, old, "a", value, true)
			if got, err := database.Get([]byte("a")); err != nil || string(got) != "late" {
				t.Fatalf("late value=%q error=%v", got, err)
			}
			_, lateRevision, err := database.GetVersioned([]byte("a"))
			if err != nil || lateRevision == 0 {
				t.Fatalf("late revision=%d error=%v", lateRevision, err)
			}
			old.Close()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got, revision, err := reopened.GetVersioned([]byte("a")); err != nil || string(got) != "late" || revision != lateRevision {
				t.Fatalf("reopened value=%q revision=%d want=%d error=%v", got, revision, lateRevision, err)
			}
		})
	}
}

func TestCOWPublicEmptyCheckpointLeafRegistrationProgress(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, _ := cowPublicContractOpen(t, profile)
			value := bytes.Repeat([]byte("leaf-registration"), 64)
			if err := database.Set([]byte("a"), value); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("missing old cut")
			}
			defer old.Close()
			for i := 0; i < 2; i++ {
				finish, err := database.cached.BeginValueLogMaintenanceFence(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				finish()
				if err := database.cached.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := database.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
				t.Fatal(err)
			}
			cowPublicContractSnapshotValue(t, old, "a", value, true)
			if err := database.Set([]byte("a"), []byte("after-empty-checkpoint")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			old.Close()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got, err := reopened.Get([]byte("a")); err != nil || string(got) != "after-empty-checkpoint" {
				t.Fatalf("reopened value=%q error=%v", got, err)
			}
		})
	}
}
