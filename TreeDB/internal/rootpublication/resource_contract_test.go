package rootpublication

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func writeStableResourceFixture(t testing.TB, dir, name, contents string) *os.File {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func stableTokenFixture(t testing.TB, dir, name string, generation, frontier uint64, reachability ReachabilityField, digest string, options ...func(*StableResourceSpec)) *StableResourceToken {
	t.Helper()
	file := writeStableResourceFixture(t, dir, name, "original-resource-bytes")
	spec := StableResourceSpec{
		Kind:           ResourceValueLog,
		LogicalLane:    "main",
		ResourceID:     name,
		Generation:     generation,
		DiagnosticPath: filepath.Join("maindb", "value_vlog", name),
		File:           file,
		Frontier:       DurableFrontier{Bytes: frontier},
		Digest:         sha256.Sum256([]byte(digest)),
		Reachability:   reachability,
	}
	for _, option := range options {
		option(&spec)
	}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatalf("NewStableResourceToken: %v", err)
	}
	return token
}

func TestStableResourceTokenSyncUsesPinnedIdentityAfterRenameRecreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.vlog")
	file := writeStableResourceFixture(t, dir, "000001.vlog", "old-identity")
	var synced atomic.Bool
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "1", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/000001.vlog", File: file,
		Frontier: DurableFrontier{Bytes: uint64(len("old-identity"))}, Reachability: ReachabilityValueLogPointer,
		SyncThrough: func(pinned *os.File, _ DurableFrontier) error {
			got := make([]byte, len("old-identity"))
			if _, err := pinned.ReadAt(got, 0); err != nil {
				return err
			}
			if string(got) != "old-identity" {
				return errors.New("sync callback observed path replacement")
			}
			synced.Store(true)
			return pinned.Sync()
		},
	})
	if err != nil {
		t.Fatalf("register token: %v", err)
	}
	t.Cleanup(token.Release)
	if err := os.Rename(path, filepath.Join(dir, "rotated.vlog")); err != nil {
		t.Fatalf("rename original: %v", err)
	}
	if err := os.WriteFile(path, []byte("new-identity"), 0o600); err != nil {
		t.Fatalf("recreate path: %v", err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough: %v", err)
	}
	if !synced.Load() {
		t.Fatal("pinned sync callback was not invoked")
	}
}

func TestStableResourceSetRejectsDataStableNamespaceUnstable(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "000001.vlog", DiagnosticPath: "maindb/value_vlog/000001.vlog",
	})
	if err != nil {
		t.Fatalf("namespace token: %v", err)
	}
	token := stableTokenFixture(t, dir, "000001.vlog", 1, 8, ReachabilityValueLogPointer, "header", func(spec *StableResourceSpec) {
		spec.Namespace = namespace
	})
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := builder.Freeze(); !errors.Is(err, ErrNamespaceUnstable) {
		t.Fatalf("Freeze error = %v, want ErrNamespaceUnstable", err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatalf("Stabilize: %v", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatalf("Freeze after namespace sync: %v", err)
	}
	set.Release()
}

func TestStableResourceSetRetainsRotatedIdentitiesAndGreatestFrontier(t *testing.T) {
	dir := t.TempDir()
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	first := stableTokenFixture(t, dir, "000001.vlog", 1, 8, ReachabilityValueLogPointer, "header-one")
	firstAdvanced := stableTokenFixture(t, dir, "000001-copy.vlog", 1, 16, ReachabilityValueLogPointer, "header-one", func(spec *StableResourceSpec) {
		spec.StableIdentityOverride = first.Identity()
		spec.ResourceID = "000001.vlog"
	})
	second := stableTokenFixture(t, dir, "000002.vlog", 2, 8, ReachabilityValueLogPointer, "header-two")
	third := stableTokenFixture(t, dir, "000003.vlog", 3, 8, ReachabilityValueLogPointer, "header-three")
	for _, token := range []*StableResourceToken{first, firstAdvanced, second, third} {
		if err := builder.Add(token); err != nil {
			t.Fatalf("Add generation %d: %v", token.Generation(), err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	defer set.Release()
	if got := set.Len(); got != 3 {
		t.Fatalf("set.Len()=%d want 3 rotated identities", got)
	}
	if got := set.FrontierFor(first.Identity(), 1).Bytes; got != 16 {
		t.Fatalf("coalesced frontier=%d want 16", got)
	}
}

func TestStableResourceSetRejectsFrontierIdentityDigestAndGenerationConflicts(t *testing.T) {
	dir := t.TempDir()
	t.Run("frontier beyond file", func(t *testing.T) {
		file := writeStableResourceFixture(t, dir, "short.vlog", "short")
		_, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "short", Generation: 1,
			DiagnosticPath: "short.vlog", File: file, Frontier: DurableFrontier{Bytes: 99},
			Reachability: ReachabilityValueLogPointer,
		})
		if !errors.Is(err, ErrFrontierBeyondResource) {
			t.Fatalf("error=%v want ErrFrontierBeyondResource", err)
		}
	})
	t.Run("digest", func(t *testing.T) {
		first := stableTokenFixture(t, dir, "digest-a", 1, 1, ReachabilityColumnManifest, "a", func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
		})
		second := stableTokenFixture(t, dir, "digest-b", 1, 1, ReachabilityColumnManifest, "b", func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.ResourceID = first.ResourceID()
			spec.StableIdentityOverride = first.Identity()
		})
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(first); err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(second); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("error=%v want ErrResourceConflict", err)
		}
		builder.Abandon()
	})
	t.Run("logical generation identity", func(t *testing.T) {
		first := stableTokenFixture(t, dir, "generation-a", 7, 1, ReachabilityValueLogPointer, "same")
		second := stableTokenFixture(t, dir, "generation-b", 7, 1, ReachabilityValueLogPointer, "same", func(spec *StableResourceSpec) {
			spec.ResourceID = first.ResourceID()
		})
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(first); err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(second); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("error=%v want ErrResourceConflict", err)
		}
		builder.Abandon()
	})
}

func TestStableResourceNestedUnionMustResolveEveryRequiredChild(t *testing.T) {
	dir := t.TempDir()
	child := NewStableResourceSetBuilder(ReachabilityColumnManifest, ReachabilityVectorGraphPack)
	if err := child.Add(stableTokenFixture(t, dir, "column.asset", 1, 4, ReachabilityColumnManifest, "column")); err != nil {
		t.Fatal(err)
	}
	if _, err := child.Freeze(); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("child Freeze error=%v want ErrUnresolvedResource", err)
	}
	if err := child.Add(stableTokenFixture(t, dir, "vector.asset", 1, 4, ReachabilityVectorGraphPack, "vector", func(spec *StableResourceSpec) {
		spec.Kind = ResourceVectorGraphPack
	})); err != nil {
		t.Fatal(err)
	}
	childSet, err := child.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	parent := NewStableResourceSetBuilder(ReachabilityCollectionSystemRoot, ReachabilityColumnManifest, ReachabilityVectorGraphPack)
	if err := parent.Add(stableTokenFixture(t, dir, "index.db", 1, 4, ReachabilityCollectionSystemRoot, "index", func(spec *StableResourceSpec) {
		spec.Kind = ResourceIndex
	})); err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(childSet); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	set, err := parent.Freeze()
	if err != nil {
		t.Fatalf("parent Freeze: %v", err)
	}
	set.Release()
}

func TestPinnedResourceBlocksExplicitDeletionUntilRelease(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "pinned.vlog", 1, 4, ReachabilityValueLogPointer, "pinned")
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	guard := set.DeletionGuard()
	if err := guard.Check(token.Identity(), token.Generation()); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("Check while pinned=%v want ErrResourcePinned", err)
	}
	set.Release()
	if err := guard.Check(token.Identity(), token.Generation()); err != nil {
		t.Fatalf("Check after release: %v", err)
	}
}

type unsupportedNamespaceAdapter struct{}

func (unsupportedNamespaceAdapter) Identity(*os.File) (StableIdentity, error) {
	return StableIdentity{Platform: "test", ObjectID: [16]byte{1}}, nil
}

func (unsupportedNamespaceAdapter) Sync(*os.File) error {
	return ErrNamespacePersistenceUnsupported
}

func TestUnsupportedNamespaceFailsBeforeCandidateVisibility(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	ns, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "new.vlog", DiagnosticPath: "new.vlog",
	}, unsupportedNamespaceAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ns.Stabilize(); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Stabilize=%v want unsupported", err)
	}
	token := stableTokenFixture(t, dir, "new.vlog", 1, 4, ReachabilityValueLogPointer, "new", func(spec *StableResourceSpec) { spec.Namespace = ns })
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Freeze(); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Freeze=%v want unsupported", err)
	}
	if builder.State() != ResourceOwnerBuilder {
		t.Fatalf("builder owner=%v want builder after rejection", builder.State())
	}
	builder.Abandon()
}

func TestResourceOwnershipSuccessRetryStopAndPoison(t *testing.T) {
	newCandidate := func(t *testing.T, seq uint64, released *atomic.Uint64) *PreparedRootCandidate {
		dir := t.TempDir()
		token := stableTokenFixture(t, dir, "owned.vlog", seq, 4, ReachabilityValueLogPointer, "owned", func(spec *StableResourceSpec) {
			spec.OnRelease = func() { released.Add(1) }
		})
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{
			Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set,
		})
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}

	t.Run("success releases exactly once", func(t *testing.T) {
		var released atomic.Uint64
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		candidate := newCandidate(t, 1, &released)
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if got := released.Load(); got != 1 {
			t.Fatalf("release count=%d want 1", got)
		}
		if err := coordinator.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("retry retains", func(t *testing.T) {
		var released atomic.Uint64
		called := make(chan struct{}, 1)
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			called <- struct{}{}
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("retry")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), newCandidate(t, 1, &released)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.WaitThrough(ctx, 1); err == nil {
			t.Fatal("WaitThrough unexpectedly succeeded")
		}
		<-called
		if got := released.Load(); got != 0 {
			t.Fatalf("release count after retry=%d want 0", got)
		}
		if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
			t.Fatalf("Stop=%v want ErrPublicationStopped", err)
		}
		handoff := coordinator.TakeRecoveryHandoff()
		if handoff.Len() != 1 {
			t.Fatalf("handoff len=%d want 1", handoff.Len())
		}
		handoff.Release()
		if got := released.Load(); got != 1 {
			t.Fatalf("release count after handoff=%d want 1", got)
		}
	})

	t.Run("ambiguous poison hands off", func(t *testing.T) {
		var released atomic.Uint64
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("unknown meta")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), newCandidate(t, 1, &released)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.WaitThrough(ctx, 1); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("WaitThrough=%v want recovery required", err)
		}
		if got := released.Load(); got != 0 {
			t.Fatalf("release count before handoff=%d want 0", got)
		}
		handoff := coordinator.TakeRecoveryHandoff()
		handoff.Release()
		if got := released.Load(); got != 1 {
			t.Fatalf("release count after handoff=%d want 1", got)
		}
		_ = coordinator.Stop(context.Background())
	})
}

func TestConflictingCandidateRejectedBeforeVisibleFrontier(t *testing.T) {
	dir := t.TempDir()
	makeCandidate := func(seq uint64, digest string) *PreparedRootCandidate {
		token := stableTokenFixture(t, dir, "candidate-"+digest, 1, 4, ReachabilityColumnManifest, digest, func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.LogicalLane = "columns"
			spec.ResourceID = "manifest-generation-1"
			spec.StableIdentityOverride = StableIdentity{Platform: "test", ObjectID: [16]byte{7}, Generation: 1}
		})
		builder := NewStableResourceSetBuilder(ReachabilityColumnManifest)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}
	coordinator, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	first := makeCandidate(1, "a")
	if err := coordinator.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := makeCandidate(2, "b")
	if err := coordinator.Enqueue(context.Background(), second); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("second Enqueue=%v want ErrResourceConflict", err)
	}
	stats := coordinator.Stats()
	if stats.VisibleCommitSeq != 1 || stats.ResourceConflicts != 1 || stats.RejectedCandidates != 1 {
		t.Fatalf("stats after rejection=%+v", stats)
	}
	second.AbandonResources()
	if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("Stop=%v", err)
	}
	coordinator.TakeRecoveryHandoff().Release()
}

func TestStableResourceMetricsSeparateFileAndNamespaceOperations(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "metrics.vlog", DiagnosticPath: "metrics.vlog",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	var flushes, syncs atomic.Uint64
	token := stableTokenFixture(t, dir, "metrics.vlog", 1, 4, ReachabilityValueLogPointer, "metrics", func(spec *StableResourceSpec) {
		spec.Namespace = namespace
		spec.FlushThrough = func(*os.File, DurableFrontier) error { flushes.Add(1); return nil }
		spec.SyncThrough = func(*os.File, DurableFrontier) error { syncs.Add(1); return nil }
	})
	if err := token.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].Flushes != 1 || stats[0].Syncs != 1 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("resource stats=%+v", stats)
	}
	if flushes.Load() != 1 || syncs.Load() != 1 {
		t.Fatalf("callbacks flush=%d sync=%d", flushes.Load(), syncs.Load())
	}
	set.Release()
}

func BenchmarkStableResourceTokenConstruction(b *testing.B) {
	dir := b.TempDir()
	file := writeStableResourceFixture(b, dir, "bench.vlog", "benchmark-resource")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "bench", Generation: uint64(i + 1),
			DiagnosticPath: "bench.vlog", File: file, Frontier: DurableFrontier{Bytes: 4}, Reachability: ReachabilityValueLogPointer,
		})
		if err != nil {
			b.Fatal(err)
		}
		token.Release()
	}
}

func BenchmarkStableResourceSetCoalesce(b *testing.B) {
	dir := b.TempDir()
	sets := make([]*StableResourceSet, 8)
	for i := range sets {
		builder := NewStableResourceSetBuilder()
		token := stableTokenFixture(b, dir, "coalesce-"+string(rune('a'+i)), uint64(i+1), 4, ReachabilityValueLogPointer, "bench")
		if err := builder.Add(token); err != nil {
			b.Fatal(err)
		}
		var err error
		sets[i], err = builder.Freeze()
		if err != nil {
			b.Fatal(err)
		}
		defer sets[i].Release()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := UnionStableResourceSets(sets...); err != nil {
			b.Fatal(err)
		}
	}
}

func TestStableResourceTokenPinnedReadRemainsUsable(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "read.vlog", 1, 4, ReachabilityValueLogPointer, "read")
	defer token.Release()
	buf := make([]byte, 4)
	if _, err := token.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
}
