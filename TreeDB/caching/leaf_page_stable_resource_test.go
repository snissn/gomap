//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCachingLeafPageLogStableBatchPinsExactRawSegment(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog: true,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = cached.Close() }()

	stable, ok := newCachingLeafPageLog(cached, &cached.leafLog).(backenddb.LeafPageStableBatchLog)
	if !ok {
		t.Fatal("cached leaf-page log does not expose stable batch capture")
	}
	pages := [][]byte{
		buildSparseLeafPageForLeafLogTestWithTag(t, 's'),
		buildSparseLeafPageForLeafLogTestWithTag(t, 't'),
	}
	ptrs, resources, err := stable.AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatalf("stable append: %v", err)
	}
	if resources == nil {
		t.Fatal("stable append returned nil resources")
	}
	defer resources.Release()
	if len(ptrs) != len(pages) {
		t.Fatalf("pointer count=%d want %d", len(ptrs), len(pages))
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != 1 {
		t.Fatalf("resource count=%d want 1", len(descriptors))
	}
	descriptor := descriptors[0]
	if descriptor.Kind() != rootpublication.ResourceOuterLeafLog {
		t.Fatalf("kind=%q want %q", descriptor.Kind(), rootpublication.ResourceOuterLeafLog)
	}
	fields := descriptor.ReachabilityFields()
	if len(fields) != 1 || fields[0] != rootpublication.ReachabilityOuterLeafRawPointer {
		t.Fatalf("reachability=%v", fields)
	}
	if descriptor.Frontier().Bytes == 0 {
		t.Fatal("captured frontier is empty")
	}
	if _, err := cached.valueLogIdentityPins.BeginDelete(descriptor.Identity()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("delete pinned raw segment error=%v want ErrResourcePinned", err)
	}
	stats := resources.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("stable resource stats=%+v want one creation namespace sync", stats)
	}

	segmentLog, ok := newCachingLeafPageLog(cached, &cached.leafLog).(interface {
		CurrentValueLogSegment() (string, uint32, bool)
	})
	if !ok {
		t.Fatal("cached leaf-page log does not expose current segment lookup")
	}
	segmentPath, _, ok := segmentLog.CurrentValueLogSegment()
	if !ok {
		t.Fatal("missing current leaf segment")
	}
	moved := segmentPath + ".moved"
	if err := os.Rename(segmentPath, moved); err != nil {
		t.Fatalf("rename captured segment: %v", err)
	}
	if err := os.WriteFile(segmentPath, bytes.Repeat([]byte{0xee}, page.PageSize), 0o600); err != nil {
		t.Fatalf("create path replacement: %v", err)
	}
	token := resources.Tokens()[0]
	got := make([]byte, 8)
	if _, err := token.ReadAt(got, int64(ptrs[0].Offset)); err != nil {
		t.Fatalf("read pinned segment after path replacement: %v", err)
	}
	if bytes.Equal(got, bytes.Repeat([]byte{0xee}, len(got))) {
		t.Fatal("stable token reopened the replacement pathname")
	}
	if filepath.Clean(token.DiagnosticPath()) == filepath.Clean(segmentPath) {
		t.Fatal("diagnostic path must be DB-relative")
	}
	if got, want := filepath.Dir(filepath.FromSlash(token.DiagnosticPath())), "leaf_vlog"; got != want {
		t.Fatalf("diagnostic directory=%q want %q", got, want)
	}
}

func TestCachingLeafPageLogStablePreparedBatchCapturesEveryReferencedSegment(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog: true,
		FlushApplyConcurrency:      4,
		ValueLogCompression:        uint8(vlogCompressionBlock),
		ValueLogMaxSegmentBytes:    512,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = cached.Close() }()

	const count = valuelog.MaxFrameK*2 + 8
	pages := make([][]byte, count)
	prepared := make([][]byte, count)
	for i := range pages {
		pages[i] = buildSparseLeafPageForLeafLogTestWithTag(t, byte(i))
		encoded, _, encodeErr := valuelog.MaybeCompactLeafLogPayloadTo(nil, pages[i])
		if encodeErr != nil {
			t.Fatalf("prepare leaf %d: %v", i, encodeErr)
		}
		prepared[i] = encoded
	}
	stable := newCachingLeafPageLog(cached, &cached.leafLog).(backenddb.LeafPagePreparedStableBatchLog)
	ptrs, resources, err := stable.AppendPreparedLeafPagesWithStableResources(pages, prepared)
	if err != nil {
		t.Fatalf("stable prepared append: %v", err)
	}
	if resources == nil {
		t.Fatal("stable prepared append returned nil resources")
	}
	defer resources.Release()
	if len(ptrs) != count {
		t.Fatalf("pointer count=%d want %d", len(ptrs), count)
	}
	referenced := make(map[uint32]struct{})
	for _, ptr := range ptrs {
		referenced[ptr.ValueLogFileID()] = struct{}{}
	}
	if len(referenced) < 2 {
		t.Fatalf("batch referenced %d segments; test did not exercise rotation", len(referenced))
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != len(referenced) {
		t.Fatalf("captured descriptors=%d referenced segments=%d", len(descriptors), len(referenced))
	}
	for _, descriptor := range descriptors {
		fileID := uint32(descriptor.Generation())
		if _, ok := referenced[fileID]; !ok {
			t.Fatalf("captured unrelated segment file_id=%d referenced=%v", fileID, referenced)
		}
		delete(referenced, fileID)
	}
	if len(referenced) != 0 {
		t.Fatalf("missing captured segment IDs: %v", referenced)
	}
	namespaceTokens := 0
	for _, token := range resources.Tokens() {
		if token.Namespace() != nil {
			namespaceTokens++
		}
	}
	if namespaceTokens != len(descriptors) {
		t.Fatalf("namespace-bearing tokens=%d descriptors=%d want exact create evidence for every newly created referenced segment", namespaceTokens, len(descriptors))
	}
	stats := resources.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != uint64(namespaceTokens) {
		t.Fatalf("stable multi-segment stats=%+v want %d namespace syncs", stats, namespaceTokens)
	}
}

func TestCachingLeafPageLogStablePreparedChildRefsReturnOwnedResources(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{IndexOuterLeavesInValueLog: true, RelaxedSync: true, AllowUnsafe: true})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = cached.Close() }()

	pages := [][]byte{
		buildSparseLeafPageForLeafLogTestWithTag(t, 'c'),
		buildSparseLeafPageForLeafLogTestWithTag(t, 'd'),
	}
	prepared := make([][]byte, len(pages))
	for i := range pages {
		encoded, _, encodeErr := valuelog.MaybeCompactLeafLogPayloadTo(nil, pages[i])
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		prepared[i] = encoded
	}
	stable := newCachingLeafPageLog(cached, &cached.leafLog).(backenddb.LeafPagePreparedChildRefStableBatchLog)
	refs, resources, err := stable.AppendPreparedLeafPageChildRefsWithStableResources(pages, prepared, make([]page.ChildRef, 0, len(pages)))
	if err != nil {
		t.Fatalf("stable prepared child refs: %v", err)
	}
	if resources == nil || resources.Len() == 0 {
		t.Fatal("stable prepared child refs returned no resources")
	}
	defer resources.Release()
	if len(refs) != len(pages) {
		t.Fatalf("ref count=%d want %d", len(refs), len(pages))
	}
	for i, ref := range refs {
		if !ref.IsLeafLog() || ref.Log.FileID == 0 {
			t.Fatalf("ref %d=%+v is not a leaf-log reference", i, ref)
		}
	}
}

func TestCachingLeafPageLogStableCaptureExcludesFollowingConcurrentRotation(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog: true,
		ValueLogMaxSegmentBytes:    1,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = cached.Close() }()

	entered := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	cached.testBeforeVlogUnlock = func(laneID int) {
		if laneID != leafLogLaneID || !first.CompareAndSwap(false, true) {
			return
		}
		close(entered)
		<-release
	}
	stableResult := make(chan struct {
		ptr       page.LeafLogPtr
		resources *rootpublication.StableResourceSet
		err       error
	}, 1)
	log := newCachingLeafPageLog(cached, &cached.leafLog)
	stableLog, ok := log.(backenddb.LeafPageStableLog)
	if !ok {
		t.Fatal("cached leaf-page log does not implement stable append")
	}
	stablePage := buildSparseLeafPageForLeafLogTestWithTag(t, 'q')
	ordinaryPage := buildSparseLeafPageForLeafLogTestWithTag(t, 'r')
	go func() {
		ptr, resources, appendErr := stableLog.AppendLeafPageWithStableResources(stablePage)
		stableResult <- struct {
			ptr       page.LeafLogPtr
			resources *rootpublication.StableResourceSet
			err       error
		}{ptr: ptr, resources: resources, err: appendErr}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stable append did not reach capture boundary")
	}
	ordinaryResult := make(chan struct {
		ptr page.LeafLogPtr
		err error
	}, 1)
	go func() {
		ptr, appendErr := log.AppendLeafPage(ordinaryPage)
		ordinaryResult <- struct {
			ptr page.LeafLogPtr
			err error
		}{ptr: ptr, err: appendErr}
	}()
	select {
	case result := <-ordinaryResult:
		close(release)
		t.Fatalf("ordinary append crossed stable capture mutex: ptr=%+v err=%v", result.ptr, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	stable := <-stableResult
	if stable.err != nil {
		t.Fatalf("stable append: %v", stable.err)
	}
	if stable.resources == nil {
		t.Fatal("stable append returned nil resources")
	}
	defer stable.resources.Release()
	ordinary := <-ordinaryResult
	if ordinary.err != nil {
		t.Fatalf("ordinary append: %v", ordinary.err)
	}
	if ordinary.ptr.FileID == stable.ptr.FileID {
		t.Fatalf("ordinary append did not rotate: stable=%d ordinary=%d", stable.ptr.FileID, ordinary.ptr.FileID)
	}
	descriptors := stable.resources.Descriptors()
	if len(descriptors) != 1 || uint32(descriptors[0].Generation()) != stable.ptr.ValueLogFileID() {
		t.Fatalf("stable descriptors=%v want only file_id=%d", descriptors, stable.ptr.ValueLogFileID())
	}
}

func TestCachingLeafPageLogStableCertifiesRelaxedOrdinaryRotationBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog: true,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = cached.Close() }()

	log := newCachingLeafPageLog(cached, &cached.leafLog)
	stable := log.(backenddb.LeafPageStableLog)
	first, firstResources, err := stable.AppendLeafPageWithStableResources(buildSparseLeafPageForLeafLogTestWithTag(t, 'u'))
	if err != nil {
		t.Fatalf("first stable append: %v", err)
	}
	firstResources.Release()

	cached.leafLog.vlogMu.Lock()
	err = cached.rotateValueLogMuHeld(&cached.leafLog)
	rotatedWriter := cached.leafLog.vlog
	rotatedSize := rotatedWriter.Size()
	cached.leafLog.vlogMu.Unlock()
	if err != nil {
		t.Fatalf("relaxed ordinary rotation: %v", err)
	}
	if rotatedSize != 0 {
		t.Fatalf("relaxed successor size=%d want 0", rotatedSize)
	}

	fail := errors.New("injected creation certification failure")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceOuterLeaf && event.Point == durabilitycut.BeforeNewFileDirectorySync {
			return fail
		}
		return nil
	})
	_, rejectedResources, err := stable.AppendLeafPageWithStableResources(buildSparseLeafPageForLeafLogTestWithTag(t, 'v'))
	restore()
	if !errors.Is(err, fail) {
		t.Fatalf("stable append after relaxed rotation error=%v want injected failure", err)
	}
	if rejectedResources != nil {
		rejectedResources.Release()
		t.Fatal("failed certification returned stable resources")
	}
	if got := rotatedWriter.Size(); got != rotatedSize {
		t.Fatalf("failed certification mutated successor size: before=%d after=%d", rotatedSize, got)
	}

	beforeSyncs := rotatedWriter.(interface {
		DurabilityStats() valuelog.DurabilityStats
	}).DurabilityStats().DirectorySyncCalls
	second, secondResources, err := stable.AppendLeafPageWithStableResources(buildSparseLeafPageForLeafLogTestWithTag(t, 'w'))
	if err != nil {
		t.Fatalf("retry stable append: %v", err)
	}
	defer secondResources.Release()
	afterSyncs := rotatedWriter.(interface {
		DurabilityStats() valuelog.DurabilityStats
	}).DurabilityStats().DirectorySyncCalls
	if afterSyncs != beforeSyncs+1 {
		t.Fatalf("retry namespace syncs: before=%d after=%d want +1", beforeSyncs, afterSyncs)
	}
	if second.FileID == first.FileID {
		t.Fatalf("ordinary rotation did not advance leaf segment: first=%d second=%d", first.FileID, second.FileID)
	}
	descriptors := secondResources.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Generation() != uint64(second.ValueLogFileID()) {
		t.Fatalf("retry resources=%v want exact generation %d", descriptors, second.ValueLogFileID())
	}
}

func TestCachingLeafPageLogStableSteadyAppendHasNoPinOrNamespaceGrowth(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{IndexOuterLeavesInValueLog: true, RelaxedSync: true, AllowUnsafe: true})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = cached.Close() }()

	stable := newCachingLeafPageLog(cached, &cached.leafLog).(backenddb.LeafPageStableBatchLog)
	pages := [][]byte{
		buildSparseLeafPageForLeafLogTestWithTag(t, 'g'),
		buildSparseLeafPageForLeafLogTestWithTag(t, 'h'),
	}
	_, warmResources, err := stable.AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatalf("warm stable append: %v", err)
	}
	warmResources.Release()
	writer, ok := cached.leafLog.vlog.(interface {
		DurabilityStats() valuelog.DurabilityStats
	})
	if !ok {
		t.Fatal("leaf writer does not expose durability counters")
	}
	baselineSyncs := writer.DurabilityStats().DirectorySyncCalls
	baselineIdentities := cached.valueLogIdentityPins.ActiveIdentities()
	for iteration := 0; iteration < 100; iteration++ {
		_, resources, appendErr := stable.AppendLeafPagesWithStableResources(pages)
		if appendErr != nil {
			t.Fatalf("stable append %d: %v", iteration, appendErr)
		}
		resources.Release()
		if pins := cached.valueLogIdentityPins.ActivePins(); pins != 0 {
			t.Fatalf("stable append %d left %d active pins", iteration, pins)
		}
		if identities := cached.valueLogIdentityPins.ActiveIdentities(); identities != baselineIdentities {
			t.Fatalf("stable append %d identities=%d want baseline %d", iteration, identities, baselineIdentities)
		}
	}
	if syncs := writer.DurabilityStats().DirectorySyncCalls; syncs != baselineSyncs {
		t.Fatalf("steady stable appends added directory syncs: before=%d after=%d", baselineSyncs, syncs)
	}
}

func BenchmarkCachingLeafPageLogStableBatch(b *testing.B) {
	for _, stable := range []bool{false, true} {
		name := "ordinary"
		if stable {
			name = "stable"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
			if err != nil {
				b.Fatal(err)
			}
			cached, err := Open(dir, backend, Options{IndexOuterLeavesInValueLog: true, RelaxedSync: true, AllowUnsafe: true})
			if err != nil {
				_ = backend.Close()
				b.Fatal(err)
			}
			defer func() { _ = cached.Close() }()
			pages := [][]byte{
				buildSparseLeafPageForLeafLogTestWithTag(b, 'i'),
				buildSparseLeafPageForLeafLogTestWithTag(b, 'j'),
				buildSparseLeafPageForLeafLogTestWithTag(b, 'k'),
				buildSparseLeafPageForLeafLogTestWithTag(b, 'l'),
			}
			log := newCachingLeafPageLog(cached, &cached.leafLog)
			stableLog, stableOK := log.(backenddb.LeafPageStableBatchLog)
			batchLog, batchOK := log.(backenddb.LeafPageBatchLog)
			if stable && !stableOK {
				b.Fatal("cached leaf-page log does not implement stable batch append")
			}
			if !stable && !batchOK {
				b.Fatal("cached leaf-page log does not implement batch append")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if stable {
					_, resources, appendErr := stableLog.AppendLeafPagesWithStableResources(pages)
					if appendErr != nil {
						b.Fatal(appendErr)
					}
					resources.Release()
					continue
				}
				if _, appendErr := batchLog.AppendLeafPages(pages); appendErr != nil {
					b.Fatal(appendErr)
				}
			}
		})
	}
}
