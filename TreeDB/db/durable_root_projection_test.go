package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type projectionResourceFixtureV1 struct {
	kind         rootpublication.ResourceKind
	reachability rootpublication.ReachabilityField
	obligations  []rootpublication.StableLogicalObligation
}

func projectionResourceSetV1(t testing.TB, fixtures ...projectionResourceFixtureV1) *rootpublication.StableResourceSet {
	t.Helper()
	dir := t.TempDir()
	builder := rootpublication.NewStableResourceSetBuilder()
	for i, fixture := range fixtures {
		path := filepath.Join(dir, fmt.Sprintf("projection-resource-%d", i))
		file, err := os.Create(path)
		if err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		if _, err := file.Write(bytes.Repeat([]byte{byte(i + 1)}, 4096)); err != nil {
			_ = file.Close()
			builder.Abandon()
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			builder.Abandon()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
			Kind: fixture.kind, LogicalLane: "projection", ResourceID: fmt.Sprint(i + 1),
			Generation: uint64(i + 1), DiagnosticPath: filepath.Base(path), File: file,
			Frontier: rootpublication.DurableFrontier{Bytes: 4096}, Digest: sha256.Sum256([]byte(path)),
			Reachability: fixture.reachability, LogicalObligations: fixture.obligations, ContentSynced: true,
		})
		if err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			t.Fatal(err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	return set
}

func TestProjectRebuiltOlderRootDurableResourcesPreservesExactExternalClosure(t *testing.T) {
	obligation := rootpublication.StableLogicalObligation{
		Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "projection",
		Generation: 3, PartID: 7, FileID: 3, Offset: 16, Length: 32, Checksum: 41,
		Reachability: rootpublication.ReachabilityColumnManifest,
		Digest:       sha256.Sum256([]byte("projection-logical-obligation")),
	}
	source := projectionResourceSetV1(t,
		projectionResourceFixtureV1{rootpublication.ResourceIndex, rootpublication.ReachabilityIndexFile, nil},
		projectionResourceFixtureV1{rootpublication.ResourceValueLog, rootpublication.ReachabilityValueLogPointer, nil},
		projectionResourceFixtureV1{rootpublication.ResourceOuterLeafLog, rootpublication.ReachabilityOuterLeafRawPointer, nil},
		projectionResourceFixtureV1{rootpublication.ResourceColumnAsset, rootpublication.ReachabilityColumnManifest, []rootpublication.StableLogicalObligation{obligation}},
		projectionResourceFixtureV1{rootpublication.ResourceVectorGraphPack, rootpublication.ReachabilityVectorGraphPack, nil},
	)
	projected, supported, err := projectRebuiltOlderRootDurableResourcesV1(source)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		t.Fatal("exact external closure unexpectedly required fallback")
	}
	source.Release()
	defer projected.Release()
	manifest, _, err := projected.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	entries := manifest.Entries()
	if len(entries) != 4 {
		t.Fatalf("projected entries=%d want 4", len(entries))
	}
	for _, entry := range entries {
		if entry.Kind == rootpublication.ResourceIndex {
			t.Fatal("projection retained old index namespace")
		}
	}
	var foundLogical bool
	for _, entry := range entries {
		if entry.Kind == rootpublication.ResourceColumnAsset {
			foundLogical = reflect.DeepEqual(entry.LogicalObligations, []rootpublication.StableLogicalObligation{obligation})
		}
	}
	if !foundLogical {
		t.Fatal("projection did not preserve the exact column logical obligation")
	}
}

func TestProjectRebuiltOlderRootDurableResourcesMatchesExactScan(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("stable manifest namespace replacement is intentionally unsupported on Windows")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	database.SetLeafPageLog(leafLog)
	defer leafLog.Close()
	writeLeafGenerationKeys(t, database, "projection-outer", 512, 'p')
	value := bytes.Repeat([]byte("value-log"), 64)
	pointers := appendPointersInNewSegment(t, dir, 0, 83, 830_000, 1, func(int) []byte { return value })
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("pointer"), pointers[0]); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	if selected := database.durableRoot.slotResources[database.durableRoot.slot]; selected != nil {
		if err := builder.Merge(selected); err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
	}
	source, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	defer source.Release()

	projected, supported, err := projectRebuiltOlderRootDurableResourcesV1(source)
	if err != nil || !supported {
		t.Fatalf("project closure=(supported=%v, err=%v), want supported", supported, err)
	}
	defer projected.Release()
	idx := database.idx.Load()
	exact, work, err := database.captureRebuiltIndexDurableResourcesWithWorkV1(idx.pager, database.meta, source)
	if err != nil {
		t.Fatal(err)
	}
	defer exact.Release()
	if !work.ExactCandidateScan {
		t.Fatal("dual-compute control did not execute an exact scan")
	}
	projectedManifest, _, err := projected.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	exactManifest, _, err := exact.DependencyManifestV1()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projectedManifest.Entries(), exactManifest.Entries()) {
		t.Fatalf("projected closure differs from exact scan\nprojected=%+v\nexact=%+v", projectedManifest.Entries(), exactManifest.Entries())
	}
	wantKinds := map[rootpublication.ResourceKind]bool{
		rootpublication.ResourceValueLog: false, rootpublication.ResourceOuterLeafLog: false,
	}
	for _, entry := range projectedManifest.Entries() {
		if _, ok := wantKinds[entry.Kind]; ok {
			wantKinds[entry.Kind] = true
		}
	}
	for kind, found := range wantKinds {
		if !found {
			t.Fatalf("dual-compute fixture missing %q", kind)
		}
	}
}

func TestProjectRebuiltOlderRootDurableResourcesRejectsUnsupportedPolicy(t *testing.T) {
	source := projectionResourceSetV1(t,
		projectionResourceFixtureV1{rootpublication.ResourceQueryReadyAsset, rootpublication.ReachabilityQueryReadyBase, nil},
	)
	defer source.Release()
	projected, supported, err := projectRebuiltOlderRootDurableResourcesV1(source)
	if err != nil {
		t.Fatal(err)
	}
	if supported || projected != nil {
		if projected != nil {
			projected.Release()
		}
		t.Fatal("unsupported source policy did not select visible exact-scan fallback")
	}
}

func TestRebuiltOlderRootIndexAuthorityRequiresExactNamespaceGenerationAndIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index-authority.db")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := rootpublication.SyncStableFile(file); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	parentGeneration, err := rootpublication.StableNamespaceParentGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: file, ParentGeneration: parentGeneration,
		Operation: rootpublication.NamespaceCreate, NewName: filepath.Base(path), DiagnosticPath: filepath.Base(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		namespace.Release()
		t.Fatal(err)
	}
	const generation = 17
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceIndex, LogicalLane: "index", ResourceID: "older",
		Generation: generation, DiagnosticPath: filepath.Base(path), File: file,
		Frontier: rootpublication.DurableFrontier{Bytes: 0}, Digest: sha256.Sum256([]byte("index-authority")),
		Reachability: rootpublication.ReachabilityIndexFile, Namespace: namespace, ContentSynced: true,
	})
	if err != nil {
		namespace.Release()
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		token.Release()
		builder.Abandon()
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	defer resources.Release()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if !rebuiltOlderRootIndexAuthorityV1(resources, identity, generation) {
		t.Fatal("exact index authority rejected")
	}
	if rebuiltOlderRootIndexAuthorityV1(resources, identity, generation+1) {
		t.Fatal("mismatched index generation accepted")
	}
	other, err := os.Create(filepath.Join(dir, "other.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	otherIdentity, err := rootpublication.StableIdentityFromFile(other)
	if err != nil {
		t.Fatal(err)
	}
	if rebuiltOlderRootIndexAuthorityV1(resources, otherIdentity, generation) {
		t.Fatal("mismatched physical index accepted")
	}
	missingNamespace := projectionResourceSetV1(t,
		projectionResourceFixtureV1{rootpublication.ResourceIndex, rootpublication.ReachabilityIndexFile, nil},
	)
	defer missingNamespace.Release()
	if rebuiltOlderRootIndexAuthorityV1(missingNamespace, missingNamespace.Descriptors()[0].Identity(), 1) {
		t.Fatal("index without exact namespace authority accepted")
	}
}

func TestVacuumIndexOnlineBindsDescriptorlessOlderRootClosure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	tests := []struct {
		name           string
		mutate         func(*RecoverableRootSet, recoverableRootKey)
		wantProjection uint64
		wantReason     string
	}{
		{name: "exact-binding", wantProjection: 1},
		{name: "generation-mismatch", mutate: func(roots *RecoverableRootSet, _ recoverableRootKey) {
			roots.rootResourceIndexID++
		}, wantReason: rebuiltDurableResourceFallbackIdentity},
		{name: "ambiguous-identity", mutate: func(roots *RecoverableRootSet, _ recoverableRootKey) {
			roots.rootResourceIndex = rootpublication.StableIdentity{}
		}, wantReason: rebuiltDurableResourceFallbackIdentity},
		{name: "missing-root-closure", mutate: func(roots *RecoverableRootSet, key recoverableRootKey) {
			delete(roots.rootResources, key)
		}, wantReason: rebuiltDurableResourceFallbackMissingSource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			pointer := appendPointersInNewSegment(t, dir, 0, 84, 840_000, 1, func(int) []byte {
				return bytes.Repeat([]byte("v"), 512)
			})[0]
			if err := database.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			batch := database.NewBatch().(*Batch)
			if err := batch.SetPointer([]byte("older"), pointer); err != nil {
				t.Fatal(err)
			}
			if err := batch.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if err := batch.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("newer"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if publication := database.rootPublication; publication != nil && publication.coordinator != nil {
				if err := publication.coordinator.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
			}

			database.maintenanceMu.Lock()
			roots, err := database.captureRecoverableRootSetWithMaintenanceLockHeld(context.Background())
			if err != nil {
				database.maintenanceMu.Unlock()
				t.Fatal(err)
			}
			defer roots.Release()
			olderRecord := roots.durable.slotRecord[roots.durable.slot^1]
			key := recoverableRootIdentity(RecoverableRoot{
				CommitSeq: olderRecord.CommitSeq, UserRootPageID: olderRecord.UserRootPageID,
				SystemRootPageID: olderRecord.SystemRootPageID, AppliedCommandLSN: olderRecord.AppliedCommandLSN,
				MaxEntryRevision: olderRecord.MaxEntryRevision,
			})
			source, ok := roots.rootResources[key]
			if !ok || source == nil {
				database.maintenanceMu.Unlock()
				t.Fatal("older root has no captured exact closure")
			}
			var valueLog, index bool
			for _, descriptor := range source.Descriptors() {
				valueLog = valueLog || descriptor.Kind() == rootpublication.ResourceValueLog
				index = index || descriptor.Kind() == rootpublication.ResourceIndex
			}
			if !valueLog || index {
				database.maintenanceMu.Unlock()
				t.Fatalf("older closure kinds=%+v, want external value-log dependency and no index descriptor", source.Descriptors())
			}
			if test.mutate != nil {
				test.mutate(roots, key)
			}
			var stats VacuumOnlineStats
			err = database.vacuumIndexOnlineRebuildV1(context.Background(), false, nil, roots, nil, time.Now(), &stats)
			database.maintenanceMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			wantFallback := uint64(0)
			wantScan := uint64(0)
			if test.wantReason != "" {
				wantFallback, wantScan = 1, 1
			}
			if stats.OlderRootProjections != test.wantProjection || stats.OlderRootProjectionFallbacks != wantFallback || stats.OlderRootProjectionFallbackReason != test.wantReason || stats.OlderRootExactCandidateScans != wantScan {
				t.Fatalf("binding stats=%+v, want projection=%d fallback=%d reason=%q exact-scan=%d", stats, test.wantProjection, wantFallback, test.wantReason, wantScan)
			}
		})
	}
}

func TestVacuumIndexOnlineBindsDescriptorlessCurrentRootClosure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	tests := []struct {
		name         string
		mutate       func(*RecoverableRootSet, recoverableRootKey)
		wantScan     bool
		wantFallback string
	}{
		{name: "exact-binding"},
		{name: "generation-mismatch", mutate: func(roots *RecoverableRootSet, _ recoverableRootKey) {
			roots.rootResourceIndexID++
		}, wantScan: true, wantFallback: rebuiltDurableResourceFallbackIdentity},
		{name: "ambiguous-identity", mutate: func(roots *RecoverableRootSet, _ recoverableRootKey) {
			roots.rootResourceIndex = rootpublication.StableIdentity{}
		}, wantScan: true, wantFallback: rebuiltDurableResourceFallbackIdentity},
		{name: "missing-root-closure", mutate: func(roots *RecoverableRootSet, key recoverableRootKey) {
			delete(roots.rootResources, key)
		}, wantScan: true, wantFallback: rebuiltDurableResourceFallbackMissingSource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{
				Dir:                        dir,
				DisableBackgroundPrune:     true,
				IndexOuterLeavesInValueLog: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{})
			if err != nil {
				t.Fatal(err)
			}
			database.SetLeafPageLog(leafLog)
			defer leafLog.Close()
			writeLeafGenerationKeys(t, database, "current-projection", 256, 'c')
			pointer := appendPointersInNewSegment(t, dir, 0, 86, 860_000, 1, func(int) []byte {
				return bytes.Repeat([]byte("v"), 512)
			})[0]
			if err := database.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			batch := database.NewBatch().(*Batch)
			if err := batch.SetPointer([]byte("pointer"), pointer); err != nil {
				t.Fatal(err)
			}
			if err := batch.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if err := batch.Close(); err != nil {
				t.Fatal(err)
			}

			database.maintenanceMu.Lock()
			roots, err := database.captureRecoverableRootSetWithMaintenanceLockHeld(context.Background())
			if err != nil {
				database.maintenanceMu.Unlock()
				t.Fatal(err)
			}
			defer roots.Release()
			visibleRoot := RecoverableRoot{
				CommitSeq:         roots.visible.CommitSeq,
				UserRootPageID:    roots.visible.RootPageID,
				SystemRootPageID:  roots.visible.SystemRootPageID,
				AppliedCommandLSN: roots.visible.AppliedCommandLSN,
				MaxEntryRevision:  uint64(roots.visible.MaxEntryRevision),
			}
			key := recoverableRootIdentity(visibleRoot)
			source, ok := roots.rootResources[key]
			if !ok || source == nil {
				database.maintenanceMu.Unlock()
				t.Fatal("visible root has no captured exact closure")
			}
			var valueLog, outerLeaf bool
			for _, descriptor := range source.Descriptors() {
				valueLog = valueLog || descriptor.Kind() == rootpublication.ResourceValueLog
				outerLeaf = outerLeaf || descriptor.Kind() == rootpublication.ResourceOuterLeafLog
			}
			if !valueLog || !outerLeaf {
				database.maintenanceMu.Unlock()
				t.Fatalf("visible closure kinds=%+v, want value-log and outer-leaf dependencies", source.Descriptors())
			}
			if test.mutate != nil {
				test.mutate(roots, key)
			}
			var stats VacuumOnlineStats
			err = database.vacuumIndexOnlineRebuildV1(context.Background(), false, nil, roots, nil, time.Now(), &stats)
			database.maintenanceMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if stats.ExactCandidateScan != test.wantScan {
				t.Fatalf("current-root stats=%+v, want exact-scan=%t", stats, test.wantScan)
			}
			if got := stats.DurableResourceProjectionFallbackReason; got != test.wantFallback {
				t.Fatalf("current-root stats=%+v, want fallback reason %q", stats, test.wantFallback)
			}
			if !test.wantScan && stats.DurableResourceProjections != 1 {
				t.Fatalf("current-root stats=%+v, want one projection", stats)
			}
			if test.wantScan && stats.DurableResourceProjectionFallbacks != 1 {
				t.Fatalf("current-root stats=%+v, want one projection fallback", stats)
			}
		})
	}
}

func TestVacuumIndexOnlineRecapturesVisibleClosureBeforeCurrentRootProjection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	rawLeafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{MaxSegmentBytes: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	leafLog, ok := rawLeafLog.(*rewriteWriter)
	if !ok {
		t.Fatalf("leaf log type=%T, want *rewriteWriter", rawLeafLog)
	}
	database.SetLeafPageLog(rawLeafLog)
	defer func() { _ = rawLeafLog.Close() }()

	writeLeafGenerationKeys(t, database, "seed", 1024, 'a')
	if err := database.SetSync([]byte("seed-overwrite"), bytes.Repeat([]byte("o"), 32)); err != nil {
		t.Fatal(err)
	}
	beforeLeafPath, beforeLeafFileID := currentLeafSegmentOrFatal(t, leafLog)
	if beforeLeafPath == "" || beforeLeafFileID == 0 {
		t.Fatalf("before leaf segment=(%q,%d), want non-zero segment", beforeLeafPath, beforeLeafFileID)
	}

	var (
		hookErr      error
		tailValue    = bytes.Repeat([]byte("tail-value-"), 32)
		rangeMissing []byte
		once         sync.Once
	)
	testHookVacuumAfterBaseSnapshot = func() {
		once.Do(func() {
			tailPtr := appendPointersInNewSegment(t, dir, 0, 88, 880_000, 1, func(int) []byte { return tailValue })[0]
			if err := database.RefreshValueLogSet(); err != nil {
				hookErr = err
				return
			}
			writeLeafGenerationKeyRange(t, database, "tail", 0, 512, 'b')
			mutation := database.NewBatch().(*Batch)
			defer func() { _ = mutation.Close() }()
			if err := mutation.DeleteRange([]byte("seed-0200"), []byte("seed-0240")); err != nil {
				hookErr = err
				return
			}
			if err := mutation.Set([]byte("seed-overwrite"), bytes.Repeat([]byte("n"), 32)); err != nil {
				hookErr = err
				return
			}
			if err := mutation.SetPointer([]byte("tail-pointer"), tailPtr); err != nil {
				hookErr = err
				return
			}
			if err := mutation.WriteSync(); err != nil {
				hookErr = err
				return
			}
			rangeMissing = leafGenerationKey("seed", 220)
		})
	}
	t.Cleanup(func() { testHookVacuumAfterBaseSnapshot = nil })

	if err := database.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hookErr != nil {
		t.Fatalf("concurrent tail mutation: %v", hookErr)
	}
	stats := database.VacuumOnlineStats()
	if stats.DeferredCutovers == 0 {
		t.Fatalf("stats=%+v, want deferred cutover after stale visible closure", stats)
	}
	if !stats.ExactCandidateScan || stats.DurableResourceProjections != 0 || stats.DurableResourceProjectionFallbacks != 1 || stats.DurableResourceProjectionFallbackReason != rebuiltDurableResourceFallbackOuterLeafDelta {
		t.Fatalf("stats=%+v, want exact fallback after replayed outer-leaf delta", stats)
	}
	afterLeafPath, afterLeafFileID := currentLeafSegmentOrFatal(t, leafLog)
	if afterLeafPath == "" || afterLeafFileID == 0 {
		t.Fatalf("after leaf segment=(%q,%d), want non-zero segment", afterLeafPath, afterLeafFileID)
	}
	if beforeLeafFileID == afterLeafFileID {
		t.Fatalf("leaf log file id before=%d after=%d, want rotation during stale-root replay proof", beforeLeafFileID, afterLeafFileID)
	}
	if got, err := database.Get([]byte("tail-pointer")); err != nil || !bytes.Equal(got, tailValue) {
		t.Fatalf("Get(tail-pointer)=(%q,%v), want tail pointer value", got, err)
	}
	if got, err := database.Get([]byte("seed-overwrite")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte("n"), 32)) {
		t.Fatalf("Get(seed-overwrite)=(%q,%v), want overwritten value", got, err)
	}
	if has, err := database.Has(rangeMissing); err != nil || has {
		t.Fatalf("Has(%q)=(%v,%v), want deleted key missing", rangeMissing, has, err)
	}
	if got, err := database.Get([]byte("tail-0511")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte("b"), 32)) {
		t.Fatalf("Get(tail-0511)=(%q,%v), want post-recapture leaf-log value", got, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if got, err := reopened.Get([]byte("tail-pointer")); err != nil || !bytes.Equal(got, tailValue) {
		t.Fatalf("reopen Get(tail-pointer)=(%q,%v), want tail pointer value", got, err)
	}
	if got, err := reopened.Get([]byte("seed-overwrite")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte("n"), 32)) {
		t.Fatalf("reopen Get(seed-overwrite)=(%q,%v), want overwritten value", got, err)
	}
	if has, err := reopened.Has(rangeMissing); err != nil || has {
		t.Fatalf("reopen Has(%q)=(%v,%v), want deleted key missing", rangeMissing, has, err)
	}
	if got, err := reopened.Get([]byte("tail-0511")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte("b"), 32)) {
		t.Fatalf("reopen Get(tail-0511)=(%q,%v), want post-recapture leaf-log value", got, err)
	}
}

func TestVacuumIndexOnlineFallsBackAfterSameSegmentOuterLeafReplay(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	rawLeafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{MaxSegmentBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	leafLog := rawLeafLog.(*rewriteWriter)
	database.SetLeafPageLog(rawLeafLog)
	writeLeafGenerationKeys(t, database, "seed", 128, 'a')
	_, beforeFileID := currentLeafSegmentOrFatal(t, leafLog)

	var hookErr error
	testHookVacuumAfterBaseSnapshot = func() {
		testHookVacuumAfterBaseSnapshot = nil
		hookErr = database.SetSync([]byte("replayed"), bytes.Repeat([]byte("r"), 64))
	}
	t.Cleanup(func() { testHookVacuumAfterBaseSnapshot = nil })
	if err := database.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	_, afterFileID := currentLeafSegmentOrFatal(t, leafLog)
	if afterFileID != beforeFileID {
		t.Fatalf("leaf log rotated from %d to %d; test requires same-segment frontier growth", beforeFileID, afterFileID)
	}
	stats := database.VacuumOnlineStats()
	if !stats.ExactCandidateScan || stats.DurableResourceProjectionFallbackReason != rebuiltDurableResourceFallbackOuterLeafDelta {
		t.Fatalf("stats=%+v, want exact fallback after same-segment replay", stats)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rawLeafLog.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if got, err := reopened.Get([]byte("replayed")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte("r"), 64)) {
		t.Fatalf("reopen Get(replayed)=(%q,%v)", got, err)
	}
}

func BenchmarkRebuiltOlderRootDurableResourceCaptureV1(b *testing.B) {
	dir := b.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()
	pointer := appendPointersInNewSegmentBench(b, dir, 0, 85, 850_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("p"), 512)
	})[0]
	if err := database.RefreshValueLogSet(); err != nil {
		b.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	for i := 0; i < 10_000; i++ {
		if err := batch.Set([]byte(fmt.Sprintf("inline/%05d", i)), []byte("v")); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.SetPointer([]byte("pointer"), pointer); err != nil {
		b.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	source := database.durableRoot.slotResources[database.durableRoot.slot]
	if source == nil {
		b.Fatal("missing exact durable closure")
	}
	idx := database.idx.Load()

	b.Run("exact-scan", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resources, work, err := database.captureRebuiltIndexDurableResourcesWithWorkV1(idx.pager, database.meta, source)
			if err != nil {
				b.Fatal(err)
			}
			resources.Release()
			if !work.ExactCandidateScan {
				b.Fatal("exact capture skipped scan")
			}
		}
	})
	b.Run("projection", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resources, supported, err := projectRebuiltOlderRootDurableResourcesV1(source)
			if err != nil || !supported {
				b.Fatalf("projection=(supported=%v, err=%v)", supported, err)
			}
			resources.Release()
		}
	})
}

func BenchmarkVacuumIndexOnlineOlderRootProjectionOuterLeavesProduction(b *testing.B) {
	dir := b.TempDir()
	database, err := Open(Options{
		Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	database.SetLeafPageLog(leafLog)
	defer leafLog.Close()
	defer database.Close()
	batch := database.NewBatch().(*Batch)
	for i := 0; i < 100_000; i++ {
		if err := batch.Set([]byte(fmt.Sprintf("outer/%06d", i)), bytes.Repeat([]byte("v"), 32)); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := database.SetSync([]byte("newest-only"), []byte("v")); err != nil {
		b.Fatal(err)
	}
	var total, olderCapture time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := database.VacuumIndexOnline(context.Background()); err != nil {
			b.Fatal(err)
		}
		stats := database.VacuumOnlineStats()
		if stats.OlderRootExactCandidateScans != 0 || stats.OlderRootProjections != 1 || stats.OlderRootProjectionFallbacks != 0 || stats.OlderRootProjectionFallbackReason != "" {
			b.Fatalf("projection stats=%+v, want one projection and zero exact scans/fallbacks", stats)
		}
		total += stats.TotalDuration
		olderCapture += stats.OlderRootDurableResourceCaptureDuration
	}
	b.StopTimer()
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "vacuum-total-ns/op")
	b.ReportMetric(float64(olderCapture.Nanoseconds())/float64(b.N), "older-capture-ns/op")
}

func BenchmarkVacuumIndexOnlineCurrentRootProjectionOuterLeavesProduction(b *testing.B) {
	dir := b.TempDir()
	database, err := Open(Options{
		Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	database.SetLeafPageLog(leafLog)
	defer leafLog.Close()
	defer database.Close()
	batch := database.NewBatch().(*Batch)
	for i := 0; i < 100_000; i++ {
		if err := batch.Set([]byte(fmt.Sprintf("outer/%06d", i)), bytes.Repeat([]byte("v"), 32)); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	pointer := appendPointersInNewSegmentBench(b, dir, 0, 87, 870_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("p"), 512)
	})[0]
	if err := database.RefreshValueLogSet(); err != nil {
		b.Fatal(err)
	}
	pointerBatch := database.NewBatch().(*Batch)
	if err := pointerBatch.SetPointer([]byte("pointer"), pointer); err != nil {
		b.Fatal(err)
	}
	if err := pointerBatch.WriteSync(); err != nil {
		b.Fatal(err)
	}
	if err := pointerBatch.Close(); err != nil {
		b.Fatal(err)
	}
	var total, currentCapture time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := database.VacuumIndexOnline(context.Background()); err != nil {
			b.Fatal(err)
		}
		stats := database.VacuumOnlineStats()
		if stats.ExactCandidateScan || stats.DurableResourceProjections != 1 || stats.DurableResourceProjectionFallbacks != 0 || stats.DurableResourceProjectionFallbackReason != "" {
			b.Fatalf("projection stats=%+v, want current-root projection and zero exact scans/fallbacks", stats)
		}
		total += stats.TotalDuration
		currentCapture += stats.DurableResourceCaptureDuration
	}
	b.StopTimer()
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "vacuum-total-ns/op")
	b.ReportMetric(float64(currentCapture.Nanoseconds())/float64(b.N), "current-capture-ns/op")
}
