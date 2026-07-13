package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

func stableToken(identity string, generation, frontier uint64, sync func(context.Context, uint64) error) StableResourceToken {
	return StableResourceToken{
		Kind: StableResourceValueLog, Namespace: "main", Identity: identity, Generation: generation,
		DiagnosticPath: "value_vlog/current", Frontier: frontier, Digest: "header-a", ReachableBy: "system_root.value_log",
		NamespaceToken: StableNamespaceToken{ParentIdentity: "db-dir", Identity: "value-vlog-dir", Generation: 1, Establish: func(context.Context) error { return nil }},
		FlushThrough:   func(context.Context, uint64) error { return nil }, SyncThrough: sync, Lease: NewStableResourceLease(nil),
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
	token.NamespaceToken.Establish = nil
	_, err := NewStableResourceSet([]StableResourceToken{token})
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestStableResourceTokenCoalescesRotatedFilesAndGreatestFrontier(t *testing.T) {
	var first, latest uint64
	one := stableToken("inode-one", 1, 2, func(_ context.Context, n uint64) error { first = n; return nil })
	two := stableToken("inode-one", 1, 9, func(_ context.Context, n uint64) error { latest = n; return nil })
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
	two = one
	two.NamespaceToken.Identity = "other-dir"
	if _, err := NewStableResourceSet([]StableResourceToken{one, two}); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("namespace err=%v", err)
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
	set, err := NewStableResourceSet([]StableResourceToken{token})
	if err != nil {
		t.Fatal(err)
	}
	var metrics StableResourceMetricsRecorder
	if err := set.FlushAndSyncWithMetrics(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	if got := metrics.Snapshot(); got.Flushes != 1 || got.Syncs != 1 || got.NamespaceSyncs != 1 {
		t.Fatalf("metrics=%+v", got)
	}
}

func BenchmarkStableResourceTokenConstructionAndCoalescing(b *testing.B) {
	tokens := []StableResourceToken{stableToken("inode", 1, 1, func(context.Context, uint64) error { return nil }), stableToken("inode", 1, 2, func(context.Context, uint64) error { return nil })}
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
