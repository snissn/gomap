package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// The oracle is the writer's actual file size before/after an append, rather
// than the pointer-length resolver whose units these tests are checking.
func physicalAccountingFixture(t *testing.T, db *DB, grouped, fallback, dead bool) ([]page.ValuePtr, int64, int64) {
	t.Helper()
	id, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	dir := ValueLogDirPath(db.dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	var ptrs []page.ValuePtr
	if grouped {
		ptrs, err = w.AppendFrame(0, nil, []valuelog.Record{
			{RID: 1, Value: bytes.Repeat([]byte("a"), 256)},
			{RID: 2, Value: bytes.Repeat([]byte("b"), 256)},
		})
	} else {
		// The ordinary record format remains supported by readers and raw copy.
		raw := make([]byte, valuelog.HeaderSize+256)
		raw[4] = valuelog.Version
		binary.LittleEndian.PutUint64(raw[8:16], 1)
		binary.LittleEndian.PutUint32(raw[16:20], 256)
		copy(raw[valuelog.HeaderSize:], bytes.Repeat([]byte("a"), 256))
		binary.LittleEndian.PutUint32(raw[:4], crc.Checksum(raw[4:]))
		var ptr page.ValuePtr
		ptr, err = w.AppendRawRecord(raw, uint32(len(raw)-4))
		ptrs = []page.ValuePtr{ptr}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	live := info.Size()
	if dead {
		w, err = valuelog.NewWriter(path, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append(0, nil, 3, bytes.Repeat([]byte("dead"), 64)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	registerTestValueLogProducer(t, db.dir, path, id)
	// A newer lane tail makes the fixture a sealed rewrite candidate.
	appendPointersInNewSegment(t, db.dir, 0, 2, 100, 1, func(int) []byte { return []byte("tail") })
	if fallback {
		for i := range ptrs {
			if grouped {
				ptrs[i].Length = page.ValuePtrMarkGrouped(0, uint8(i))
			} else {
				ptrs[i].Length = 0
			}
		}
	}
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	return ptrs, live, info.Size() - live
}

func TestValueLogPhysicalLiveBytes_FileSpanOracle(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, fallback := range []bool{false, true} {
			for _, dead := range []bool{false, true} {
				t.Run(fmt.Sprintf("grouped=%t/fallback=%t/dead=%t", grouped, fallback, dead), func(t *testing.T) {
					db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
					if err != nil {
						t.Fatal(err)
					}
					defer closeNoErr(t, db)
					ptrs, wantLive, wantStale := physicalAccountingFixture(t, db, grouped, fallback, dead)
					b := db.NewBatch().(*Batch)
					keys := len(ptrs)
					if grouped {
						keys = 256 // aliases span several index leaves, but one physical frame
					}
					for i := 0; i < keys; i++ {
						ptr := ptrs[i%len(ptrs)]
						if err := b.SetPointer([]byte(fmt.Sprintf("value/%d", i)), ptr); err != nil {
							t.Fatal(err)
						}
					}
					if err := b.WriteSync(); err != nil {
						t.Fatal(err)
					}
					closeNoErr(t, b)
					snap := db.AcquireSnapshot()
					defer closeNoErr(t, snap)
					// One explicit root guarantees the direct projection path.
					got, err := db.maintenanceReachabilityScan(context.Background(), snap, maintenanceReachabilityScanOptions{
						Collectors:      maintenanceReachabilityValueLogLiveBytes,
						ExplicitRootIDs: []uint64{snap.state.RootPageID},
					})
					if err != nil {
						t.Fatal(err)
					}
					if got.valueLogLiveBytesBySegment[ptrs[0].FileID] != wantLive {
						t.Fatalf("physical live bytes=%d want file span=%d", got.valueLogLiveBytesBySegment[ptrs[0].FileID], wantLive)
					}
					chunks, err := db.estimateValueLogLiveBytesByChunk(context.Background(), 1<<20)
					if err != nil {
						t.Fatal(err)
					}
					if got := chunks[valueLogChunkKey{fileID: ptrs[0].FileID, chunkOffset: 0}]; got != wantLive {
						t.Fatalf("chunk physical live bytes=%d want file span=%d", got, wantLive)
					}
					plan, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
					if err != nil {
						t.Fatal(err)
					}
					if plan.ValueLogRewritePlan.SelectedBytesStale != wantStale {
						t.Fatalf("selected stale=%d want actual dead span=%d; plan=%+v", plan.ValueLogRewritePlan.SelectedBytesStale, wantStale, plan.ValueLogRewritePlan)
					}
				})
			}
		}
	}
}

func TestValueLogPhysicalLiveBytes_MemoizedProtectedRoots(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprintf("grouped=%t", grouped), func(t *testing.T) {
			db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoErr(t, db)
			ptrs, wantLive, _ := physicalAccountingFixture(t, db, grouped, true, false)
			b := db.NewBatch().(*Batch)
			if err := b.SetPointer([]byte("current"), ptrs[0]); err != nil {
				t.Fatal(err)
			}
			if err := b.WriteSync(); err != nil {
				t.Fatal(err)
			}
			closeNoErr(t, b)
			var protected uint64
			wantRefs, wantDedupe := uint64(1), uint64(0)
			if grouped {
				protected, err = db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "protected", ptrs[1]).NewIterator(nil, nil))
				wantRefs, wantDedupe = 2, 1
			} else {
				protected, err = db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "protected", "inline").NewIterator(nil, nil))
			}
			if err != nil {
				t.Fatal(err)
			}
			snap := db.AcquireSnapshot()
			defer closeNoErr(t, snap)
			for i := 0; i < 2; i++ {
				got, err := db.maintenanceReachabilityScan(context.Background(), snap, maintenanceReachabilityScanOptions{
					Collectors:      maintenanceReachabilityValueLogRefCounts | maintenanceReachabilityValueLogLiveBytes,
					ExplicitRootIDs: []uint64{snap.state.RootPageID, protected},
				})
				if err != nil {
					t.Fatal(err)
				}
				if got.valueLogLiveBytesBySegment[ptrs[0].FileID] != wantLive || got.valueLogRefCounts[ptrs[0].FileID] != wantRefs {
					t.Fatalf("shared frame must count once across roots: result=%+v want physical=%d", got, wantLive)
				}
				if got.counters.GroupedRecordDedupeHits != wantDedupe {
					t.Fatalf("dedupe hits=%d want %d", got.counters.GroupedRecordDedupeHits, wantDedupe)
				}
			}
		})
	}
}

func TestValueLogPhysicalLiveBytes_ExhaustiveRolloverReopen(t *testing.T) {
	db := openCompactStorageRewritePolicyFixture(t, 24, 4, 512)
	dir := db.dir
	defer func() {
		if db != nil {
			closeNoErr(t, db)
		}
	}()
	opts := CompactStorageOptions{Mode: CompactStorageExhaustive, SyncEachPhase: true,
		ValueLogRewriteBatchSize: 1, ValueLogRewriteMaxSegmentBytes: 1024}
	stats, err := db.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ValueLogRewrite.SegmentsAfter <= stats.ValueLogRewrite.SegmentsBefore+1 {
		t.Fatalf("fixture did not force multiple output segments: %+v", stats.ValueLogRewrite)
	}
	if stats.RemainingDebt.ValueLogRewriteBytes != 0 || stats.RemainingDebt.ValueLogRewriteSegments != 0 {
		t.Fatalf("sealed all-live output retained false rewrite debt: %+v", stats.RemainingDebt)
	}
	// Old durable roots must remain protected until the existing horizon moves.
	if stats.RemainingDebt.ValueLogGCSegments == 0 {
		t.Fatal("expected retained-root GC debt")
	}
	advancePastRetainedDurableSlotForTest(t, db)
	converged, err := db.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if converged.RemainingDebt.ValueLogRewriteSegments != 0 || converged.RemainingDebt.ValueLogRewriteBytes != 0 ||
		converged.RemainingDebt.ValueLogGCSegments != 0 || converged.RemainingDebt.ValueLogGCBytes != 0 ||
		converged.RemainingDebt.LeafPackGenerations != 0 || converged.RemainingDebt.LeafPackBytes != 0 ||
		converged.RemainingDebt.LeafGCGenerations != 0 || converged.RemainingDebt.LeafGCBytes != 0 ||
		converged.RemainingDebt.ZeroByteValueLogFiles != 0 {
		t.Fatalf("value-log/leaf debt did not converge: %+v", converged.RemainingDebt)
	}
	if runtime.GOOS == "windows" {
		phase := compactStorageIndexVacuumPhase(t, converged)
		if !phase.Required || phase.Status != CompactStoragePhaseStatusUnsupported {
			t.Fatalf("windows index-vacuum phase=%+v want required unsupported", phase)
		}
		if converged.FullyCompacted || converged.PolicyFullyCompacted || converged.ByteMinimized || !converged.RemainingDebt.IndexVacuumRequired {
			t.Fatalf("windows unsupported vacuum overstated convergence: %+v", converged)
		}
	} else if !converged.PolicyFullyCompacted || !converged.ByteMinimized {
		t.Fatalf("not converged: %+v", converged.RemainingDebt)
	}
	closeNoErr(t, db)
	db = nil
	db, err = Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 24; i++ {
		value, err := db.Get([]byte(fmt.Sprintf("source-live-%06d", i)))
		if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{byte('a' + i%23)}, 512)) {
			t.Fatalf("reopen value %d: length=%d error=%v", i, len(value), err)
		}
	}
}
