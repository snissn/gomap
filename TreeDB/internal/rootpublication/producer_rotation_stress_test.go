package rootpublication_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func stableProducerRotationRetryResourcePlateau(t *testing.T) {
	const iterations = 320
	root := t.TempDir()
	valueDir := filepath.Join(root, "value_vlog")
	if err := os.Mkdir(valueDir, 0o700); err != nil {
		t.Fatal(err)
	}
	valueWriter, err := valuelog.NewWriter(filepath.Join(valueDir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer valueWriter.Close()
	commandJournal, err := commitlog.OpenCommandJournal(filepath.Join(root, "wal"), commitlog.CommandJournalOptions{Lane: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer commandJournal.Close()

	retryErr := errors.New("injected pre-meta retry")
	attempts := make(map[uint64]uint8, iterations)
	expectedNamespaceSyncs := func(seq uint64) uint64 {
		// The first value-log segment and every command-WAL segment contribute
		// one creation witness. Later value-log rotations retain the closed
		// segment's creation witness and add the newly active segment's witness.
		if seq > 1 && seq%2 == 1 {
			return 2
		}
		return 1
	}
	coordinator, err := rootpublication.New(rootpublication.Options{Publisher: rootpublication.PublisherFunc(func(_ context.Context, candidate *rootpublication.PreparedRootCandidate) rootpublication.PublishResult {
		seq := candidate.Frontier().CommitSeq()
		attempts[seq]++
		resources := candidate.Resources()
		if attempts[seq] == 1 {
			if err := resources.FlushThrough(); err != nil {
				return rootpublication.PublishResult{Outcome: rootpublication.PublishRetryableFailure, Err: err}
			}
			return rootpublication.PublishResult{Outcome: rootpublication.PublishRetryableFailure, Err: retryErr}
		}
		if err := resources.SyncThrough(); err != nil {
			return rootpublication.PublishResult{Outcome: rootpublication.PublishRetryableFailure, Err: err}
		}
		stats := resources.Stats(time.Now())
		if len(stats) != 1 || stats[0].PendingCount != 2 || stats[0].Flushes != 2 || stats[0].Syncs != 2 || stats[0].NamespaceSyncs != expectedNamespaceSyncs(seq) {
			return rootpublication.PublishResult{Outcome: rootpublication.PublishRetryableFailure, Err: fmt.Errorf("unexpected resource operation counts: %+v", stats)}
		}
		return rootpublication.PublishResult{Outcome: rootpublication.PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := coordinator.Stop(context.Background()); err != nil {
			t.Errorf("stop coordinator: %v", err)
		}
	}()

	beforeFDs, fdErr := os.ReadDir("/dev/fd")
	checkFDs := fdErr == nil
	maxFDs := len(beforeFDs)
	commandEnvelope := commitlog.CommandEnvelope{
		Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV, PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}
	var warmHeap runtime.MemStats
	for i := 0; i < iterations; i++ {
		seq := uint64(i + 1)
		var set *rootpublication.StableResourceSet
		var kind rootpublication.ResourceKind
		if i%2 == 0 {
			if _, err := valueWriter.Append(0, nil, seq, []byte("rotation-stress-value")); err != nil {
				t.Fatal(err)
			}
			closedID := valueWriter.FileID()
			activeID := closedID + 1
			activeName := fmt.Sprintf("%06d.vlog", activeID)
			rotation, err := valueWriter.RotateToWithStableResources(filepath.Join(valueDir, activeName), activeID, false,
				valuelog.StableResourceRegistration{
					LogicalLane: "main", Generation: uint64(closedID), DiagnosticPath: filepath.Join("maindb", "value_vlog", fmt.Sprintf("%06d.vlog", closedID)),
					Reachability: rootpublication.ReachabilityValueLogPointer,
				},
				valuelog.StableResourceRegistration{
					LogicalLane: "main", Generation: uint64(activeID), DiagnosticPath: filepath.Join("maindb", "value_vlog", activeName),
					Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: uint64(activeID),
					NamespaceOperation: rootpublication.NamespaceCreate,
				})
			if err != nil {
				t.Fatal(err)
			}
			builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
			if err := builder.Add(rotation.TakeClosed()); err != nil {
				rotation.Release()
				t.Fatal(err)
			}
			if err := builder.Add(rotation.TakeActive()); err != nil {
				rotation.Release()
				t.Fatal(err)
			}
			set, err = builder.Freeze()
			rotation.Release()
			if err != nil {
				t.Fatal(err)
			}
			kind = rootpublication.ResourceValueLog
		} else {
			if _, err := commandJournal.AppendCommand(commandEnvelope); err != nil {
				t.Fatal(err)
			}
			rotation, err := commandJournal.RotateActiveSegmentWithStableResources(false)
			if err != nil {
				t.Fatal(err)
			}
			builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityCommandWALRotated, rootpublication.ReachabilityCommandWALActive)
			if err := builder.Add(rotation.TakeClosed()); err != nil {
				rotation.Release()
				t.Fatal(err)
			}
			if err := builder.Add(rotation.TakeActive()); err != nil {
				rotation.Release()
				t.Fatal(err)
			}
			set, err = builder.Freeze()
			rotation.Release()
			if err != nil {
				t.Fatal(err)
			}
			kind = rootpublication.ResourceCommandWAL
		}
		if set.Len() != 2 {
			set.Release()
			t.Fatalf("rotation %d physical pins=%d want 2", seq, set.Len())
		}
		stats := set.Stats(time.Now())
		if len(stats) != 1 || stats[0].PendingCount != 2 || stats[0].NamespaceSyncs != expectedNamespaceSyncs(seq) {
			set.Release()
			t.Fatalf("rotation %d capture stats=%+v", seq, stats)
		}
		candidate, err := rootpublication.NewPreparedRootCandidate(rootpublication.CandidateSpec{
			Frontier: rootpublication.NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set,
		})
		if err != nil {
			set.Release()
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			candidate.AbandonResources()
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), seq); !errors.Is(err, retryErr) {
			t.Fatalf("rotation %d first publish=%v want injected retry", seq, err)
		}
		pending := resourceStatsForKind(t, coordinator.Stats().Resources, kind)
		if pending.PendingCount != 2 || pending.ActivePins != 2 || pending.Flushes != 2 || pending.Syncs != 0 || pending.NamespaceSyncs != expectedNamespaceSyncs(seq) {
			t.Fatalf("rotation %d retry stats=%+v", seq, pending)
		}
		if err := coordinator.WaitThrough(context.Background(), seq); err != nil {
			t.Fatalf("rotation %d retry publish: %v", seq, err)
		}
		released := resourceStatsForKind(t, coordinator.Stats().Resources, kind)
		if released.PendingCount != 0 || released.ActivePins != 0 || released.PinHighWater != 2 {
			t.Fatalf("rotation %d released stats=%+v", seq, released)
		}
		if checkFDs && (i+1)%32 == 0 {
			entries, err := os.ReadDir("/dev/fd")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) > maxFDs {
				maxFDs = len(entries)
			}
			if got, wantMax := len(entries), len(beforeFDs)+4; got > wantMax {
				t.Fatalf("descriptor count grew from %d to %d after %d mixed rotations", len(beforeFDs), got, i+1)
			}
		}
		if i+1 == 64 {
			runtime.GC()
			runtime.ReadMemStats(&warmHeap)
		}
	}
	stats := coordinator.Stats()
	if stats.PendingCommits != 0 || stats.PreMetaFailures != iterations || stats.Retries != iterations || stats.PublishCalls != iterations*2 {
		t.Fatalf("final coordinator stats=%+v", stats)
	}
	if checkFDs && maxFDs > len(beforeFDs)+4 {
		t.Fatalf("descriptor high water grew from %d to %d", len(beforeFDs), maxFDs)
	}
	runtime.GC()
	var finalHeap runtime.MemStats
	runtime.ReadMemStats(&finalHeap)
	const retainedHeapAllowance = 8 << 20
	if finalHeap.HeapAlloc > warmHeap.HeapAlloc+retainedHeapAllowance {
		t.Fatalf("retained heap grew from %d to %d bytes after warmup; allowance=%d", warmHeap.HeapAlloc, finalHeap.HeapAlloc, retainedHeapAllowance)
	}
}

func resourceStatsForKind(t *testing.T, stats []rootpublication.ResourceKindStats, kind rootpublication.ResourceKind) rootpublication.ResourceKindStats {
	t.Helper()
	for _, candidate := range stats {
		if candidate.Kind == kind {
			return candidate
		}
	}
	t.Fatalf("missing resource stats for kind %q: %+v", kind, stats)
	return rootpublication.ResourceKindStats{}
}
