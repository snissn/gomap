package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

var errCOWCheckpointWALFence = errors.New("test: COW checkpoint WAL fence refused")

type cowCheckpointWALBackend struct {
	*backenddb.DB
	failRotation, failFlush bool
	rotations, flushes      int
}

func (b *cowCheckpointWALBackend) RotateCommandWALActiveSegment(sync bool) error {
	b.rotations++
	if b.failRotation {
		return errCOWCheckpointWALFence
	}
	return b.DB.RotateCommandWALActiveSegment(sync)
}
func (b *cowCheckpointWALBackend) FlushCommandWAL(sync bool) error {
	b.flushes++
	if b.failFlush {
		return errCOWCheckpointWALFence
	}
	return b.DB.FlushCommandWAL(sync)
}

// There is deliberately no public raw notification for this native-only frame.
// The original accepted ticket is the coverage proof, including an empty final
// raw batch. Wrapper coverage must not replace it or cause a separate commit.
func TestCOWCheckpointCompleteCoverageAndWALFence(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, mode := range []string{"checkpoint", "close", "rotation-error", "flush-error"} {
		t.Run(mode, func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, true)
			capture, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
			if err != nil {
				t.Fatal(err)
			}
			intent, err := backend.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
			if err != nil {
				t.Fatal(err)
			}
			publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence()}
			result, err := backend.WriteNativePublication(publication, intent, false)
			if err != nil || !result.LogicalInstalled || result.AssignedLSN != 1 || result.Seal.Valid() {
				t.Fatalf("unsealed original native frame=%+v %v", result, err)
			}
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			original := cache.cow.prefix.LogicalFrontiers()
			if original.C != 1 || original.S.CanonicalLSN != 0 || backend.CommandWALActiveBytes() == 0 {
				t.Fatalf("missing pending original frame: %+v", original)
			}
			wal := &cowCheckpointWALBackend{DB: backend, failRotation: mode == "rotation-error", failFlush: mode == "flush-error"}
			cache.backend = wal
			cache.SetCommandWALCheckpointPublishHook(func(bool) (uint64, []backenddb.CommandWALLSNRange, error) {
				t.Error("COW consulted per-producer coverage")
				return 99, []backenddb.CommandWALLSNRange{{First: 99, Last: 99}}, errors.New("obsolete producer range")
			})
			cleanups := 0
			cache.SetCommandWALCheckpointCleanupHook(func(bool) error {
				cleanups++
				if backend.State().AppliedCommandLSN != 1 {
					t.Error("cleanup ran without complete applied cut")
				}
				return nil
			})
			if mode == "rotation-error" || mode == "flush-error" {
				if err := cache.Checkpoint(); !errors.Is(err, errCOWCheckpointWALFence) {
					t.Fatalf("original fence error=%v", err)
				}
				failed := cache.cow.prefix.LogicalFrontiers()
				if failed.V != original.V || failed.C != original.C || failed.S != original.S || backend.State().AppliedCommandLSN != 0 || cleanups != 0 {
					t.Fatalf("failed fence published/dropped prefix: %+v -> %+v cleanup=%d", original, failed, cleanups)
				}
				wal.failRotation, wal.failFlush = false, false
			}
			if mode == "close" {
				if err := cache.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				before := backend.State().CommitSeq
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				frontier := cache.cow.prefix.LogicalFrontiers()
				if frontier.S.CanonicalLSN != 1 || frontier.S.LogicalSequence != original.V || backend.State().CommitSeq != before+1 {
					t.Fatalf("not one exact accepted-cut publication: %+v", frontier)
				}
				if backend.CommandWALActiveBytes() != 0 || cleanups != 1 {
					t.Fatalf("native-only completion lost rotation/cleanup: bytes=%d cleanup=%d", backend.CommandWALActiveBytes(), cleanups)
				}
				if err := cache.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if wal.rotations == 0 || wal.flushes == 0 || cleanups != 1 {
				t.Fatalf("missing WAL/completion work: rotate=%d flush=%d cleanup=%d", wal.rotations, wal.flushes, cleanups)
			}
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 {
				t.Fatalf("original native cleanup debt: %+v", stats)
			}
		})
	}
}
