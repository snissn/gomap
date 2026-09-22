package rootpublication

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCoordinatorEnqueueRetainedClosureAllocation(t *testing.T) {
	const entries = 128
	const ridCount = 512
	file := writeStableResourceFixture(t, t.TempDir(), "retained.bin", "0123456789abcdef")
	candidates := make([]*PreparedRootCandidate, 3)
	sets := make([]*StableResourceSet, len(candidates))
	for i := range candidates {
		builder := NewStableResourceSetBuilder(ReachabilityQueryReadyBase)
		for j := 0; j < entries; j++ {
			var identity [16]byte
			binary.LittleEndian.PutUint64(identity[:], uint64(j+1))
			rids := make([]uint64, ridCount)
			for k := range rids {
				rids[k] = uint64(k*4 + i + 1)
			}
			frontier := NewRIDFrontier(rids)
			frontier.Bytes, frontier.MaxLSN = uint64(i+1), uint64(i+1)
			// Retained files repeat their complete ref set across candidates;
			// one append file adds a ref while retaining a larger exact prefix.
			obligationCount := 2
			if j == entries-1 {
				obligationCount = 256 + i
			}
			obligations := make([]StableLogicalObligation, obligationCount)
			for k := range obligations {
				obligations[k] = stableLogicalObligationFixture(uint64(k+1), uint32(k+1), fmt.Sprint(k))
				obligations[k].FileID = uint64(j + 1)
			}
			token, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceQueryReadyAsset, LogicalLane: "retained", ResourceID: fmt.Sprint(j), Generation: 1,
				DiagnosticPath: "retained.bin", File: file, Frontier: frontier, ContentSynced: true,
				StableIdentityOverride: StableIdentity{Platform: "count-test", ObjectID: identity},
				Reachability:           ReachabilityQueryReadyBase,
				LogicalObligations:     obligations,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(token.LogicalObligations()); got != obligationCount {
				t.Fatalf("registered obligations=%d want %d", got, obligationCount)
			}
			if err := builder.Add(token); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		sets[i], err = builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		seq := uint64(i + 1)
		candidates[i], err = NewPreparedRootCandidate(CandidateSpec{
			Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: sets[i],
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(candidates[i].AbandonResources)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	coordinator, err := New(Options{Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
		if candidate.Frontier().CommitSeq() == 1 {
			close(started)
			<-release
		}
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		stopClean(t, coordinator)
	})
	if err := coordinator.Enqueue(context.Background(), candidates[0]); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- coordinator.WaitThrough(context.Background(), 1) }()
	<-started
	var wantCoalesces uint64
	for i := 1; i < len(candidates); i++ {
		oracle, err := UnionStableResourceSets(sets[:i+1]...)
		if err != nil || oracle.Len() != entries {
			t.Fatalf("full union count=%d err=%v", oracle.Len(), err)
		}
		beforeDescriptors := make([][]StableResourceDescriptor, i+1)
		for j := range beforeDescriptors {
			beforeDescriptors[j] = sets[j].Descriptors()
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		err = coordinator.Enqueue(context.Background(), candidates[i])
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatal(err)
		}
		bytes := after.TotalAlloc - before.TotalAlloc
		t.Logf("pending=%d entries=%d RIDs/entry=%d enqueue bytes=%d mallocs=%d", i, entries, ridCount, bytes, after.Mallocs-before.Mallocs)
		// Admission needs bounded representative/index state, not copies of the
		// accumulated RID payload or a materialized durable union. The generous
		// bound includes logical-obligation reconciliation and coordinator work.
		if bytes > 1<<20 {
			t.Errorf("pending=%d enqueue allocated %d bytes, want <= 1 MiB", i, bytes)
		}
		wantCoalesces += uint64(entries * i)
		stats := coordinator.Stats()
		if stats.ResourceCoalesces != wantCoalesces || stats.VisibleCommitSeq != uint64(i+1) {
			t.Fatalf("coalesces=%d visible=%d want %d/%d", stats.ResourceCoalesces, stats.VisibleCommitSeq, wantCoalesces, i+1)
		}
		for j := range beforeDescriptors {
			if sets[j].Owner() != ResourceOwnerCoordinator || !reflect.DeepEqual(beforeDescriptors[j], sets[j].Descriptors()) {
				t.Fatal("enqueue changed captured resource contents or lost coordinator ownership")
			}
		}
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitThrough(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	for i, set := range sets {
		if set.Owner() != ResourceOwnerReleased {
			t.Fatalf("published input %d owner=%v", i, set.Owner())
		}
	}
}

// The full materialized union remains the error/count oracle. Exercise both
// its linear and indexed representative paths using publicly constructed sets.
func TestCoordinatorEnqueueUnionCountParity(t *testing.T) {
	for _, padding := range []int{0, stableResourceEntryLinearLookupLimit + 1} {
		for _, tc := range []struct {
			name       string
			change     func(int, *StableResourceSpec)
			namespaces bool
			wantCounts [3]int
			rejectAt   int
		}{
			{name: "advancing sparse frontier", wantCounts: [3]int{1, 1, 1}},
			{name: "logical identity changed", rejectAt: 2, wantCounts: [3]int{1}, change: func(i int, s *StableResourceSpec) {
				s.ResourceID = "same-logical"
				if i == 2 {
					s.StableIdentityOverride = StableIdentity{Platform: "other", ObjectID: [16]byte{99}}
				}
			}},
			{name: "immutable generations remain separate", wantCounts: [3]int{1, 2, 3}, change: func(i int, s *StableResourceSpec) {
				s.Kind, s.Generation, s.Reachability = ResourceOuterLeafPack, uint64(i), ReachabilityOuterLeafPackedPointer
			}},
			{name: "immutable cross generation digest", rejectAt: 2, wantCounts: [3]int{1}, change: func(i int, s *StableResourceSpec) {
				s.Kind, s.Generation, s.Reachability = ResourceOuterLeafPack, uint64(i), ReachabilityOuterLeafPackedPointer
				if i == 2 {
					s.Digest[0]++
				}
			}},
			{name: "mutable kinds remain separate", wantCounts: [3]int{1, 2, 2}, change: func(i int, s *StableResourceSpec) {
				s.Kind = ResourceValueLog
				if i == 2 {
					s.Kind = ResourceOuterLeafLog
				}
			}},
			{name: "mutable generations remain separate", wantCounts: [3]int{1, 2, 3}, change: func(i int, s *StableResourceSpec) {
				s.Kind, s.Generation = ResourceValueLog, uint64(i)
			}},
			{name: "mutable header conflict", rejectAt: 2, wantCounts: [3]int{1}, change: func(i int, s *StableResourceSpec) {
				s.Kind = ResourceValueLog
				if i == 2 {
					s.Digest[0]++
				}
			}},
			{name: "stability conflict", rejectAt: 2, wantCounts: [3]int{1}, change: func(i int, s *StableResourceSpec) {
				s.Kind, s.Reachability = ResourceOuterLeafPack, ReachabilityOuterLeafPackedPointer
				if i == 2 {
					s.Kind, s.Reachability = ResourceValueLog, ReachabilityValueLogPointer
				}
			}},
			{name: "third alias obligation conflict", rejectAt: 3, wantCounts: [3]int{1, 1}, change: func(i int, s *StableResourceSpec) {
				s.LogicalObligations = nil
				if i >= 2 {
					s.LogicalObligations = []StableLogicalObligation{stableLogicalObligationFixture(1, uint32(i), fmt.Sprint(i))}
				}
			}},
			{name: "third alias namespace conflict", namespaces: true, rejectAt: 3, wantCounts: [3]int{1, 1}},
			{name: "namespace representative then old logical identity", namespaces: true, wantCounts: [3]int{1, 1, 2}, change: func(i int, s *StableResourceSpec) {
				if i == 3 {
					s.ResourceID, s.Namespace = "alias-1", nil
					s.StableIdentityOverride = StableIdentity{Platform: "other", ObjectID: [16]byte{99}}
				}
			}},
		} {
			t.Run(fmt.Sprintf("padding=%d/%s", padding, tc.name), func(t *testing.T) {
				dir := t.TempDir()
				file := writeStableResourceFixture(t, dir, "aliases.bin", "0123456789abcdef")
				coordinator, err := New(Options{Clock: NewFakeClock(time.Unix(0, 0)), Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
					return PublishResult{Outcome: PublishSucceeded}
				})})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { stopClean(t, coordinator) })
				var sets []*StableResourceSet
				var accepted uint64
				var coalesces uint64
				for i := 1; i <= 3; i++ {
					frontier := NewRIDFrontier([]uint64{uint64(i), uint64(i + 8)})
					frontier.Bytes, frontier.MaxLSN = uint64(i), uint64(i)
					spec := StableResourceSpec{
						Kind: ResourceQueryReadyAsset, LogicalLane: "aliases", ResourceID: fmt.Sprintf("alias-%d", i), Generation: 1,
						DiagnosticPath: "aliases.bin", File: file, Frontier: frontier, ContentSynced: true,
						Digest: sha256.Sum256([]byte("same physical header")), Reachability: ReachabilityQueryReadyBase,
						LogicalObligations: []StableLogicalObligation{stableLogicalObligationFixture(uint64(i), uint32(i), fmt.Sprint(i))},
					}
					if tc.namespaces && i >= 2 {
						parent, err := os.Open(dir)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = parent.Close() })
						namespace, err := newStableNamespaceToken(StableNamespaceSpec{
							Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate,
							NewName: fmt.Sprintf("name-%d", i), DiagnosticPath: "aliases.bin",
						}, &countingNamespaceAdapter{})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(namespace.Release)
						if err := namespace.Stabilize(); err != nil {
							t.Fatal(err)
						}
						spec.Namespace = namespace
					}
					if tc.change != nil {
						tc.change(i, &spec)
					}
					for j := range spec.LogicalObligations {
						spec.LogicalObligations[j].Reachability = spec.Reachability
					}
					token, err := NewStableResourceToken(spec)
					if err != nil {
						t.Fatal(err)
					}
					builder := NewStableResourceSetBuilder()
					if i == 1 {
						for j := 1; j <= padding; j++ {
							if err := builder.Add(distinctPhysicalTokenFixture(t, file, uint64(j))); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := builder.Add(token); err != nil {
						t.Fatal(err)
					}
					set, err := builder.Freeze()
					if err != nil {
						t.Fatal(err)
					}
					sets = append(sets, set)
					seq := uint64(i)
					candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(candidate.AbandonResources)
					before := set.Descriptors()
					oracle, oracleErr := UnionStableResourceSets(sets...)
					err = coordinator.Enqueue(context.Background(), candidate)
					if tc.rejectAt == i {
						if !errors.Is(oracleErr, ErrResourceConflict) || err == nil || err.Error() != oracleErr.Error() {
							t.Fatalf("enqueue error=%v full-union error=%v want matching resource conflicts", err, oracleErr)
						}
						stats := coordinator.Stats()
						if stats.VisibleCommitSeq != accepted || stats.ResourceCoalesces != coalesces || stats.ResourceConflicts != 1 || stats.RejectedCandidates != 1 || set.Owner() != ResourceOwnerCandidate {
							t.Fatalf("rejection changed accepted prefix or ownership: stats=%+v owner=%v", stats, set.Owner())
						}
					} else {
						if err != nil || oracleErr != nil {
							t.Fatalf("enqueue=%v union=%v", err, oracleErr)
						}
						if got, want := oracle.Len(), padding+tc.wantCounts[i-1]; got != want {
							t.Fatalf("union count=%d want %d", got, want)
						}
						coalesces += uint64(stableSetPhysicalCount(sets) - oracle.Len())
						stats := coordinator.Stats()
						if stats.ResourceCoalesces != coalesces || set.Owner() != ResourceOwnerCoordinator {
							t.Fatalf("accepted counts/ownership=%+v/%v", stats, set.Owner())
						}
						accepted = seq
					}
					if !reflect.DeepEqual(before, set.Descriptors()) {
						t.Fatal("admission mutated source obligations/frontier")
					}
					if tc.rejectAt == i {
						break
					}
				}
				if err := coordinator.WaitThrough(context.Background(), accepted); err != nil {
					t.Fatal(err)
				}
				for _, set := range sets[:accepted] {
					if set.Owner() != ResourceOwnerReleased {
						t.Fatalf("accepted input owner=%v after publish", set.Owner())
					}
				}
			})
		}
	}
}
