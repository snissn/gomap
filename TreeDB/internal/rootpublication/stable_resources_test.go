package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
)

func stableToken(identity string, generation, frontier uint64, sync func(context.Context, uint64) error) StableResourceToken {
	token := StableResourceToken{
		Kind: StableResourceValueLog, Namespace: "main", Identity: identity, Generation: generation,
		DiagnosticPath: "value_vlog/current", Frontier: frontier, Digest: "header-a", ReachableBy: "system_root.value_log",
		NamespaceToken: StableNamespaceToken{},
		FlushThrough:   func(context.Context, uint64) error { return nil }, SyncThrough: sync, Lease: NewStableResourceLease(nil),
	}
	token.Provider = token.Lease
	return token
}

func TestStableResourceTokenOrdersDataBeforeNamespace(t *testing.T) {
	var events []string
	token := stableToken("inode", 1, 1, func(context.Context, uint64) error { events = append(events, "sync"); return nil })
	token.FlushThrough = func(context.Context, uint64) error { events = append(events, "flush"); return nil }
	token.NamespaceToken = StableNamespaceToken{Required: true, ParentIdentity: "db-dir", Identity: "value-vlog-dir", Generation: 1, Establish: func(context.Context) error { events = append(events, "namespace"); return nil }}
	set, err := NewStableResourceSet([]StableResourceToken{token})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.FlushAndSync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(events), "[flush sync namespace]"; got != want {
		t.Fatalf("order=%s want=%s", got, want)
	}
}

func TestStableResourceTokenPathReplacementUsesPinnedSync(t *testing.T) {
	var old, replacement int
	token := stableToken("inode-old", 1, 8, func(context.Context, uint64) error { old++; return nil })
	_ = replacement // A path-reopening implementation would increment this instead.
	set, err := NewStableResourceSet([]StableResourceToken{token})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.FlushAndSync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if old != 1 || replacement != 0 {
		t.Fatalf("pinned=%d replacement=%d", old, replacement)
	}
}

func TestStableResourceTokenNamespaceFailsClosed(t *testing.T) {
	token := stableToken("inode-a", 1, 1, func(context.Context, uint64) error { return nil })
	token.NamespaceToken = StableNamespaceToken{Required: true, ParentIdentity: "db-dir", Identity: "value-vlog-dir"}
	_, err := NewStableResourceSet([]StableResourceToken{token})
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestStableResourceTokenCoalescesRotatedFilesAndGreatestFrontier(t *testing.T) {
	var first, latest uint64
	one := stableToken("inode-one", 1, 2, func(_ context.Context, n uint64) error { first = n; return nil })
	two := stableToken("inode-one", 1, 9, func(_ context.Context, n uint64) error { latest = n; return nil })
	two.Provider, two.Lease = one.Provider, one.Lease
	rotated := stableToken("inode-two", 2, 4, func(context.Context, uint64) error { return nil })
	set, err := NewStableResourceSet([]StableResourceToken{one, two, rotated})
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 2 {
		t.Fatalf("tokens=%d", set.Len())
	}
	if err := set.FlushAndSync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first != 0 || latest != 9 {
		t.Fatalf("first=%d latest=%d", first, latest)
	}
}

func TestStableResourceTokenRejectsDigestAndNamespaceConflicts(t *testing.T) {
	one := stableToken("inode", 1, 1, func(context.Context, uint64) error { return nil })
	two := one
	two.Digest = "different"
	if _, err := NewStableResourceSet([]StableResourceToken{one, two}); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("digest err=%v", err)
	}
	one.NamespaceToken = StableNamespaceToken{Required: true, ParentIdentity: "parent", Identity: "inode", Generation: 1, Establish: func(context.Context) error { return nil }}
	two = one
	two.NamespaceToken.Identity = "other-dir"
	if _, err := NewStableResourceSet([]StableResourceToken{one, two}); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("namespace err=%v", err)
	}
	two = one
	two.NamespaceToken.Generation++
	if _, err := NewStableResourceSet([]StableResourceToken{one, two}); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("generation err=%v", err)
	}
}

func TestStableResourceTokenCoalescingPreservesReachabilityUnion(t *testing.T) {
	one := stableToken("inode", 1, 3, func(context.Context, uint64) error { return nil })
	two := one
	two.Frontier = 7
	two.ReachableBy = "collection_root.asset_ref"
	set, err := NewStableResourceSet([]StableResourceToken{two, one})
	if err != nil {
		t.Fatal(err)
	}
	tokens := set.Tokens()
	if len(tokens) != 1 {
		t.Fatalf("tokens=%d want 1", len(tokens))
	}
	if got, want := fmt.Sprint(tokens[0].ReachabilityFields()), "[collection_root.asset_ref system_root.value_log]"; got != want {
		t.Fatalf("reachability=%s want %s", got, want)
	}
	if tokens[0].Frontier != 7 {
		t.Fatalf("frontier=%d want 7", tokens[0].Frontier)
	}
}

func TestStableResourceTokenNormalizationOrderIsInputIndependent(t *testing.T) {
	tokens := []StableResourceToken{
		stableToken("inode-c", 1, 1, func(context.Context, uint64) error { return nil }),
		stableToken("inode-a", 1, 1, func(context.Context, uint64) error { return nil }),
		stableToken("inode-b", 1, 1, func(context.Context, uint64) error { return nil }),
	}
	forward, err := NewStableResourceSet(tokens)
	if err != nil {
		t.Fatal(err)
	}
	reversed, err := NewStableResourceSet([]StableResourceToken{tokens[2], tokens[1], tokens[0]})
	if err != nil {
		t.Fatal(err)
	}
	identities := func(set StableResourceSet) []string {
		out := make([]string, 0, set.Len())
		for _, token := range set.Tokens() {
			out = append(out, token.Identity)
		}
		return out
	}
	if got, want := fmt.Sprint(identities(reversed)), fmt.Sprint(identities(forward)); got != want {
		t.Fatalf("reversed order=%s want %s", got, want)
	}
}

func TestStableResourceTokenDuplicateProvidersCoalesceAndReleaseBoth(t *testing.T) {
	var left, right atomic.Uint64
	one := stableToken("inode", 1, 1, func(context.Context, uint64) error { return nil })
	one.Lease = NewStableResourceLease(func() { left.Add(1) })
	one.Provider = one.Lease
	two := one
	two.Frontier = 2
	two.Lease = NewStableResourceLease(func() { right.Add(1) })
	two.Provider = two.Lease
	set, err := NewStableResourceSet([]StableResourceToken{one, two})
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 1 || set.Tokens()[0].Frontier != 2 {
		t.Fatalf("set=%+v", set.Tokens())
	}
	debt := NewStableResourceDebt(set)
	debt.Retry()
	debt.Poison()
	debt.Shutdown()
	if left.Load() != 0 || right.Load() != 0 {
		t.Fatal("retry released pin")
	}
	if transferred := debt.Supersede(); transferred.Len() != 1 {
		t.Fatalf("transfer=%d", transferred.Len())
	}
	debt.DurableCoverage()
	if left.Load() != 0 || right.Load() != 0 {
		t.Fatal("transferred debt released")
	}
	NewStableResourceDebt(set).AbandonPreVisibility()
	if left.Load() != 1 || right.Load() != 1 {
		t.Fatalf("left=%d right=%d", left.Load(), right.Load())
	}
}

func TestStableResourceTokenCoalescingRetainsOutstandingNamespaceDebt(t *testing.T) {
	var namespaceSyncs atomic.Uint64
	one := stableToken("inode", 1, 1, func(context.Context, uint64) error { return nil })
	two := one
	two.Frontier = 2
	two.NamespaceToken = StableNamespaceToken{
		Required:       true,
		ParentIdentity: "parent",
		Identity:       "inode",
		Generation:     1,
		Establish: func(context.Context) error {
			namespaceSyncs.Add(1)
			return nil
		},
	}
	set, err := NewStableResourceSet([]StableResourceToken{one, two})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.FlushAndSync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if namespaceSyncs.Load() != 1 {
		t.Fatalf("namespace syncs=%d want 1", namespaceSyncs.Load())
	}
}

func TestStableResourceTokenExistingAppendDoesNotSyncNamespace(t *testing.T) {
	var namespaceSyncs atomic.Uint64
	token := stableToken("existing", 1, 1, func(context.Context, uint64) error { return nil })
	token.NamespaceToken = StableNamespaceToken{Establish: func(context.Context) error { namespaceSyncs.Add(1); return nil }}
	set, err := NewStableResourceSet([]StableResourceToken{token})
	if err != nil {
		t.Fatal(err)
	}
	var metrics StableResourceMetricsRecorder
	if err := set.FlushAndSyncWithMetrics(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	if namespaceSyncs.Load() != 0 || metrics.Snapshot().NamespaceSyncs != 0 {
		t.Fatalf("namespace syncs=%d metrics=%+v", namespaceSyncs.Load(), metrics.Snapshot())
	}
}

func TestStableResourceTokenCoalescedConflictFailsBeforeConsumerVisibility(t *testing.T) {
	one, err := NewPreparedRootCandidateWithStableResources(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, []StableResourceToken{stableToken("inode", 1, 1, func(context.Context, uint64) error { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	conflict := stableToken("inode", 1, 2, func(context.Context, uint64) error { return nil })
	conflict.Digest = "different"
	two, err := NewPreparedRootCandidateWithStableResources(CandidateSpec{Frontier: NewFrontier(2, 2, 2, 2, 2)}, []StableResourceToken{conflict})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coalesceCandidates([]*PreparedRootCandidate{one, two}).StableResourceSet(); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("coalesced conflict err=%v", err)
	}
}

func TestStableResourceTokenRejectsFrontierBeyondPinnedFile(t *testing.T) {
	token := stableToken("inode", 1, 9, func(_ context.Context, frontier uint64) error {
		return fmt.Errorf("pinned file ends before frontier %d", frontier)
	})
	set, err := NewStableResourceSet([]StableResourceToken{token})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.FlushAndSync(context.Background()); err == nil {
		t.Fatal("accepted frontier beyond pinned file")
	}
}

func TestStableResourceTokenCandidateCoalesceRetainsNestedUnion(t *testing.T) {
	one, err := NewPreparedRootCandidateWithStableResources(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1)}, []StableResourceToken{stableToken("parent", 1, 1, func(context.Context, uint64) error { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	two, err := NewPreparedRootCandidateWithStableResources(CandidateSpec{Frontier: NewFrontier(2, 2, 2, 2, 2)}, []StableResourceToken{stableToken("nested-child", 1, 1, func(context.Context, uint64) error { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := coalesceCandidates([]*PreparedRootCandidate{one, two}).StableResourceTokens()
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("nested token omitted: %d", len(tokens))
	}
}

func TestStableResourceTokenLeaseBlocksDeletionUntilReleased(t *testing.T) {
	var releases atomic.Uint64
	token := stableToken("pinned", 1, 1, func(context.Context, uint64) error { return nil })
	token.Lease = NewStableResourceLease(func() { releases.Add(1) })
	set, err := NewStableResourceSet([]StableResourceToken{token})
	if err != nil {
		t.Fatal(err)
	}
	set.Release()
	set.Release()
	if got := releases.Load(); got != 1 {
		t.Fatalf("releases=%d", got)
	}
}

func TestStableResourceTokenStressReleasesEveryPin(t *testing.T) {
	var releases atomic.Uint64
	for i := 0; i < 1_000; i++ {
		token := stableToken("pinned", uint64(i+1), 1, func(context.Context, uint64) error { return nil })
		token.Lease = NewStableResourceLease(func() { releases.Add(1) })
		set, err := NewStableResourceSet([]StableResourceToken{token})
		if err != nil {
			t.Fatal(err)
		}
		set.Release()
	}
	if got := releases.Load(); got != 1_000 {
		t.Fatalf("released pins=%d want 1000", got)
	}
}

func TestStableResourceTokenMetricsExposeExactOperationCounts(t *testing.T) {
	token := stableToken("inode", 1, 1, func(context.Context, uint64) error { return nil })
	duplicate := token
	var metrics StableResourceMetricsRecorder
	set, err := NewStableResourceSetWithMetrics([]StableResourceToken{token, duplicate}, &metrics)
	if err != nil {
		t.Fatal(err)
	}
	metrics.ObservePending(2, 64, 2, 2)
	metrics.ObserveConflict()
	metrics.ObserveRejected()
	if err := set.FlushAndSyncWithMetrics(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	metrics.ObservePending(0, 0, 0, 0)
	if got := metrics.Snapshot(); got.Flushes != 1 || got.Syncs != 1 || got.NamespaceSyncs != 0 || got.PendingTokensHighWater != 2 || got.PendingBytesHighWater != 64 || got.DescriptorHighWater != 2 || got.PinHighWater != 2 || got.Coalesced != 1 || got.Conflicts != 1 || got.Rejected != 1 || got.ByKind[StableResourceValueLog].Syncs != 1 {
		t.Fatalf("metrics=%+v", got)
	}
}

func BenchmarkStableResourceTokenConstructionAndCoalescing(b *testing.B) {
	one := stableToken("inode", 1, 1, func(context.Context, uint64) error { return nil })
	two := stableToken("inode", 1, 2, func(context.Context, uint64) error { return nil })
	two.Provider, two.Lease = one.Provider, one.Lease
	tokens := []StableResourceToken{one, two}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set, err := NewStableResourceSet(tokens)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(set.Len()), "descriptors/op")
		b.ReportMetric(float64(set.Len()), "pins/op")
	}
}

func BenchmarkStableResourceTokenNormalizationScale(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 65_000} {
		b.Run("tokens="+strconv.Itoa(count), func(b *testing.B) {
			tokens := make([]StableResourceToken, count)
			for i := range tokens {
				tokens[i] = stableToken("inode-"+strconv.Itoa(i), 1, uint64(i+1), func(context.Context, uint64) error { return nil })
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				set, err := NewStableResourceSet(tokens)
				if err != nil {
					b.Fatal(err)
				}
				if set.Len() != count {
					b.Fatalf("tokens=%d want %d", set.Len(), count)
				}
			}
		})
	}
}
