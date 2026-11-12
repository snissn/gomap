package caching

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

const laptopGraphOnlyPackChildEnv = "TREEDB_LAPTOP_GRAPH_ONLY_PACK_CHILD"
const laptopGraphOnlyPackCompleted = "LAPTOP_GRAPH_ONLY_PACK_COMPLETED"

type laptopGraphOnlyPackHold struct {
	DB         *backenddb.DB
	published  chan backenddb.LeafGenerationPackRunOnceStats
	release    chan struct{}
	holdExited chan struct{} // preallocated; close means callback is returning
}

// Public graph calls have no cancellation contract. Isolate the original calls
// so a failed join cannot race database teardown or leak into another test.
func enterLaptopGraphOnlyPackChild(t *testing.T) bool {
	t.Helper()
	if selected := os.Getenv(laptopGraphOnlyPackChildEnv); selected != "" {
		if selected != t.Name() {
			t.Fatalf("INVALID_WITNESS child selector=%q want=%q", selected, t.Name())
		}
		t.Cleanup(func() {
			if !t.Failed() && !t.Skipped() {
				t.Log(laptopGraphOnlyPackCompleted)
			}
		})
		return true
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.timeout=90s", "-test.v")
	cmd.WaitDelay = 5 * time.Second
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, laptopGraphOnlyPackChildEnv+"=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, laptopGraphOnlyPackChildEnv+"="+t.Name())
	output, err := cmd.CombinedOutput()
	if err != nil || ctx.Err() != nil || !bytes.Contains(output, []byte(laptopGraphOnlyPackCompleted)) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("isolated leaf/graph witness failed: err=%v deadline=%v\n%s", err, ctx.Err(), output)
	}
	t.Logf("isolated leaf/graph witness:\n%s", output)
	return false
}

// The ordinary graph profile is immutable and must be supplied at Open.
// privateCOWPackFixture's empty-profile relaxed backend cannot admit this graph.
// Keep the original native backend, native bridge, pack geometry and finite limits.
func laptopGraphOnlyPackFixture(t *testing.T) (*DB, *laptopGraphOnlyPackHold, backenddb.Options) {
	t.Helper()
	t.Setenv(envDisableVlogGenerationLoop, "1")
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")
	t.Setenv(envEnableLeafGenerationPackMaintenance, "0")
	dir := t.TempDir()
	opts := backenddb.Options{Dir: filepath.Join(dir, "backend"), CommandWAL: true,
		ResolvedProfile: backenddb.ProfileCommandWALDurable, Durability: backenddb.DurabilityDurable,
		DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true}
	native, err := backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := native.BindPrivateCOWOwner(owner); err != nil {
		if closeErr := native.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	cache, err := Open(dir, native, Options{TransactionOwner: owner, MemtableMode: "cow_ordered", ExternalCommandWAL: true,
		MemtableShards: 2, FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true,
		ValueLogCompression: uint8(vlogCompressionOff), ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold), ValueLogGenerationLeafSegmentTargetBytes: 8 << 20})
	if err != nil {
		if closeErr := native.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !cache.closing.Load() {
			if err := cache.Close(); err != nil {
				t.Errorf("original cache Close: %v", err)
			}
		}
	})
	if err := native.BindNativeSemanticBridge(owner, cache); err != nil {
		t.Fatal(err)
	}
	// Binding alone preserves raw-only eligibility. Activate while the cache
	// still owns its original backend and before any legacy manager can escape.
	if err := native.ActivateNativeSemanticOwner(); err != nil {
		t.Fatalf("INVALID_WITNESS native owner activation: %v", err)
	}
	cache.testSkipVlogCheckpointKick = true
	skipRetainedPrune(cache)
	held := &laptopGraphOnlyPackHold{DB: native,
		published: make(chan backenddb.LeafGenerationPackRunOnceStats, 1), release: make(chan struct{}), holdExited: make(chan struct{})}
	// Native publication requires the original concrete backend. Observe the
	// same post-backend/pre-adoption boundary without installing an adapter.
	cache.testAfterPrivateCOWLeafPack = func(ctx context.Context, stats backenddb.LeafGenerationPackRunOnceStats) error {
		// Close before returning on release, context expiry or unwind. A pending
		// packDone alone cannot prove that this post-pack callback still holds.
		defer close(held.holdExited)
		held.published <- stats
		select {
		case <-held.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return cache, held, opts
}

func laptopGraphOnlyPackManifestRoot(t *testing.T, snapshot *backenddb.Snapshot, name string) uint64 {
	t.Helper()
	state, live := snapshot.StateToken()
	if !live {
		t.Fatal("INVALID_WITNESS unavailable manifest snapshot")
	}
	raw, err := snapshot.GetAtRoot(state.SystemRootPageID, []byte("collections/root/"+name+"/column/manifest"))
	if err != nil || len(raw) != 8 || binary.BigEndian.Uint64(raw) == 0 {
		t.Fatalf("INVALID_WITNESS physical manifest descriptor=%x err=%v", raw, err)
	}
	return binary.BigEndian.Uint64(raw)
}

func laptopGraphOnlyPackCachedManifestRoot(t *testing.T, cache *DB, name string) uint64 {
	t.Helper()
	cut := cache.cow.retainCut()
	if cut == nil {
		t.Fatal("INVALID_WITNESS missing cache cut")
	}
	defer func() {
		if err := cache.cow.releaseCut(cut); err != nil {
			t.Error(err)
		}
	}()
	return laptopGraphOnlyPackManifestRoot(t, cut.basis.snapshot, name)
}

func laptopGraphOnlyPackFillerDocument(round int) []byte {
	return []byte(fmt.Sprintf(`{"payload":"%s"}`, strings.Repeat(fmt.Sprintf("%d", round), 128)))
}

// Native collection activation rejects the raw conditional-transaction adapter
// used by preparePrivateCOWPack. Keep its real 4096/2048 copy geometry using
// supported public JSON insert/update calls, with no alternate write authority.
func prepareLaptopGraphOnlyPackPublicFiller(t *testing.T, cache *DB, manager *collections.CollectionManager) *collections.Collection {
	t.Helper()
	const name = "leaf_pack_filler"
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name}); err != nil {
		t.Fatalf("INVALID_WITNESS create public filler: %v", err)
	}
	filler, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatalf("INVALID_WITNESS open public filler: %v", err)
	}
	for round := 1; round <= 4; round++ {
		count := 2048
		if round == 1 {
			count = 4096
		}
		ids := make([][]byte, count)
		for i := range ids {
			ids[i] = []byte(fmt.Sprintf("pack-%05d", i))
		}
		document := laptopGraphOnlyPackFillerDocument(round)
		if round == 1 {
			documents := make([][]byte, count)
			for i := range documents {
				documents[i] = document
			}
			got, err := filler.InsertBatch(ids, documents)
			if err != nil || len(got) != count {
				t.Fatalf("INVALID_WITNESS public filler insert count=%d err=%v", len(got), err)
			}
			for i := range got {
				if !bytes.Equal(got[i], ids[i]) {
					t.Fatalf("INVALID_WITNESS public filler insert ID%d=%q want=%q", i, got[i], ids[i])
				}
			}
		} else {
			previous := laptopGraphOnlyPackFillerDocument(round - 1)
			items := make([]collections.UpdateBatchItem, count)
			for i := range items {
				items[i] = collections.UpdateBatchItem{DocumentID: ids[i], Update: func(current []byte) ([]byte, bool, error) {
					if !bytes.Equal(current, previous) {
						return nil, false, fmt.Errorf("unexpected previous public filler value: %s", current)
					}
					return document, true, nil
				}}
			}
			got, err := filler.UpdateBatch(items)
			if err != nil || len(got) != count {
				t.Fatalf("INVALID_WITNESS public filler update round%d count=%d err=%v", round, len(got), err)
			}
			for i, result := range got {
				if !result.Matched || !result.Modified {
					t.Fatalf("INVALID_WITNESS public filler round%d ID%d result=%+v", round, i, result)
				}
			}
		}
		if err := cache.Checkpoint(); err != nil {
			t.Fatalf("INVALID_WITNESS public filler round%d checkpoint: %v", round, err)
		}
		if round == 1 {
			cache.leafLog.vlogMu.Lock()
			rotateErr := cache.rotateValueLogMuHeld(&cache.leafLog)
			cache.leafLog.vlogMu.Unlock()
			if rotateErr != nil {
				t.Fatalf("INVALID_WITNESS seal public filler generation: %v", rotateErr)
			}
		}
	}
	return filler
}

func checkLaptopGraphOnlyPackPublicFiller(t *testing.T, phase string, filler *collections.Collection) {
	t.Helper()
	first, replaced := laptopGraphOnlyPackFillerDocument(1), laptopGraphOnlyPackFillerDocument(4)
	for i := 0; i < 4096; i++ {
		id := []byte(fmt.Sprintf("pack-%05d", i))
		want := first
		if i < 2048 {
			want = replaced
		}
		got, err := filler.Get(id)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s public filler %q=%s want=%s err=%v", phase, id, got, want, err)
		}
	}
}

// A separately named graph-only port of the exact #5373 fixture at40288d98
// (reference SHA256 f918d59343ea036f39fa751a6b2ed517a4256ef9a060c2010cab5d0eb048a72d).
// Compression stays OFF. This is neither joined dictionary proof nor attribution
// of the historical race7 failure. The low-level native fixture retains the
// original inline-sync command ACK, not public Open's completion-group contract.
// The post-pack hook is local reviewed instrumentation: main78e has no such hook.
// Original errors are retained, but source preparation is not observed RED/GREEN.
func TestPrivateCOWLeafPackPreservesAcceptedOrdinaryGraphLaptopDiagnostic(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	if !enterLaptopGraphOnlyPackChild(t) {
		return
	}
	t.Log("GRAPH_ONLY_LAPTOP_DIAGNOSTIC compression=off original_durable_native_owner=true dictionary_proof=false")
	cache, backend, reopenOptions := laptopGraphOnlyPackFixture(t)
	budget := cache.cow.budget
	if budget.Limits() != memtable.DefaultCOWLimits() {
		t.Fatal("INVALID_WITNESS changed original finite COW limits")
	}
	manager := collections.NewCollectionManager(backend.DB)
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	meta := collections.CollectionMeta{Name: "leaf_graph_docs", Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
		Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadFull, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: collections.TypedStorageOwnerColumnPart}},
	}}, VectorIndexes: []collections.VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: collections.VectorMetricCosine, Strategy: collections.VectorIndexStrategyColumnGraph}}}
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	const rows = 128
	ids, documents := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%04d", i))
		documents[i] = []byte(fmt.Sprintf(`{"embedding":[%d,1]}`, i+1))
	}
	if got, err := col.InsertBatch(ids, documents); err != nil || len(got) != rows {
		t.Fatalf("INVALID_WITNESS public seed count=%d err=%v", len(got), err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Persist the target manifest before sealing the real partly-live generation.
	// Public native collection calls supply the filler; no map or root is forged.
	filler := prepareLaptopGraphOnlyPackPublicFiller(t, cache, manager)
	checkLaptopGraphOnlyPackPublicFiller(t, "before pack", filler)
	reader, err := collections.NewNativeJSONManager(backend.DB)
	if err != nil {
		t.Fatal(err)
	}
	old, err := reader.Capture(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if old != nil {
			if err := old.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	checkRows := func(phase string, get func([]byte) ([]byte, error)) {
		t.Helper()
		for i, id := range ids {
			got, err := get(id)
			if err != nil || !bytes.Equal(got, documents[i]) {
				t.Fatalf("%s public row %q=%s want=%s err=%v", phase, id, got, documents[i], err)
			}
		}
	}
	checkRows("before pack", old.Get)
	oldRoot := laptopGraphOnlyPackCachedManifestRoot(t, cache, meta.Name)
	before, err := backend.DB.ObserveNativeProducer()
	if err != nil || before.UnderPressure {
		t.Fatalf("INVALID_WITNESS producer requires a drain: %+v err=%v", before, err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(backend.release) }) }
	packDone, graphDone := make(chan error, 1), make(chan error, 1)
	packJoined, graphStarted, graphJoined := false, false, false
	defer func() {
		release()
		for _, job := range []struct {
			name   string
			done   <-chan error
			joined bool
		}{{"pack", packDone, packJoined}, {"graph", graphDone, !graphStarted || graphJoined}} {
			if job.joined {
				continue
			}
			select {
			case err := <-job.done:
				if err != nil {
					t.Errorf("%s completion: %v", job.name, err)
				}
			case <-time.After(15 * time.Second):
				t.Errorf("INVALID_WITNESS %s failed to join after release", job.name)
				// The isolated child's bounded Go timeout owns the last join.
				// Never close captures or database beneath an original live call.
				<-job.done
			}
		}
	}()
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envDisableVlogGenerationLoop, "0")
	go func() { _, err := cache.runPrivateCOWLeafPackSlice(); packDone <- err }()
	select {
	case stats := <-backend.published:
		if !stats.Ran || stats.Pack.BytesCopied == 0 {
			t.Fatalf("INVALID_WITNESS no real pack: %+v", stats)
		}
	case err := <-packDone:
		packJoined = true
		t.Fatalf("INVALID_WITNESS pack missed publication hold: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("INVALID_WITNESS no pack publication")
	}
	newRoot := func() uint64 {
		physical := backend.DB.AcquireSnapshot()
		if physical == nil {
			t.Fatal("INVALID_WITNESS no published physical snapshot")
		}
		defer func() {
			if err := physical.Close(); err != nil {
				t.Error(err)
			}
		}()
		return laptopGraphOnlyPackManifestRoot(t, physical, meta.Name)
	}()
	if newRoot == oldRoot {
		t.Fatalf("INVALID_WITNESS pack did not relocate target manifest root=%d", oldRoot)
	}
	cachedRoot := laptopGraphOnlyPackCachedManifestRoot(t, cache, meta.Name)
	if cachedRoot != oldRoot || cache.maintenanceActive.Load() {
		t.Fatalf("INVALID_WITNESS cache already adopted/fenced old=%d cached=%d", oldRoot, cachedRoot)
	}
	pressure, err := backend.DB.ObserveNativeProducer()
	if err != nil || pressure.UnderPressure {
		t.Fatalf("INVALID_WITNESS held-pack producer pressure: %+v err=%v", pressure, err)
	}
	expectedLSN := backend.DB.CommandWALNextLSN()
	observer, err := backenddb.NewNativeInstallObserverForTest(backend.DB, commitlog.CommandKindCollectionRebuildVectorIndex, expectedLSN)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	graphStarted = true
	go func() {
		status, err := col.RebuildVectorIndex("vec")
		if err == nil && (!status.Loaded || status.RebuildNeeded) {
			err = fmt.Errorf("incomplete public graph: %+v", status)
		}
		graphDone <- err
	}()
	select {
	case event := <-observer.Events():
		if observer.Dropped() || !event.Assigned || !event.Consumed || !event.LogicalInstalled || !event.WALCompleted || !event.WALStable || event.AssignedLSN != expectedLSN || event.LogicalSequence <= before.LogicalSequence {
			t.Fatalf("INVALID_WITNESS graph acceptance=%+v dropped=%t", event, observer.Dropped())
		}
		t.Logf("accepted graph lsn=%d sequence=%d cached_manifest=%d published_manifest=%d", event.AssignedLSN, event.LogicalSequence, oldRoot, newRoot)
	case err := <-graphDone:
		graphJoined = true
		t.Fatalf("INVALID_WITNESS graph missed accepted-before-adoption interval: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("INVALID_WITNESS graph acceptance blocked")
	}
	// The accepted graph must remain pending while the original pack still owns
	// its flush fence. An already completed operation is not this held witness.
	// These nonblocking guards do not extend the original five-second slice.
	select {
	case err := <-packDone:
		packJoined = true
		t.Fatalf("INVALID_WITNESS pack completed before explicit release: %v", err)
	default:
	}
	select {
	case err := <-graphDone:
		graphJoined = true
		t.Fatalf("INVALID_WITNESS graph completed before adoption release: %v", err)
	default:
	}
	// The original five-second context may expire before packDone is sent.
	// Observe this explicit callback-lifetime marker AFTER the graph ACK and
	// both pending checks; a closed marker invalidates, never extends, the hold.
	select {
	case <-backend.holdExited:
		t.Fatal("INVALID_WITNESS post-pack callback exited before explicit release")
	default:
	}
	t.Logf("GRAPH_ONLY_PENDING_BEFORE_RELEASE expected_lsn=%d pack_pending=true graph_pending=true hold_callback_open=true", expectedLSN)
	release()
	var packOutcome, graphOutcome error
	select {
	case packOutcome = <-packDone:
		packJoined = true
	case <-time.After(15 * time.Second):
		// Still attempt the other original join before examining either error.
		// The existing deferred final join prevents teardown beneath a live call.
	}
	select {
	case graphOutcome = <-graphDone:
		graphJoined = true
	case <-time.After(15 * time.Second):
	}
	t.Logf("GRAPH_ONLY_ORIGINAL_JOINED_OUTCOMES pack_joined=%t graph_joined=%t pack=%v graph=%v", packJoined, graphJoined, packOutcome, graphOutcome)
	if !packJoined || !graphJoined {
		t.Fatalf("UNQUALIFIED_GRAPH_ONLY_FAILURE incomplete original joins; pack=%v graph=%v", packOutcome, graphOutcome)
	}
	if observer.Dropped() {
		t.Fatalf("UNQUALIFIED_GRAPH_ONLY_FAILURE duplicate graph acceptance; original_pack=%v original_graph=%v", packOutcome, graphOutcome)
	}
	if outcome := errors.Join(packOutcome, graphOutcome); outcome != nil {
		t.Fatalf("GRAPH_ONLY_ORIGINAL_FAILURE after both joins: original_pack=%v original_graph=%v joined=%v", packOutcome, graphOutcome, outcome)
	}
	checkRows("held old read after adoption", old.Get)
	checkRows("current after graph", col.Get)
	checkLaptopGraphOnlyPackPublicFiller(t, "after graph", filler)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err := cache.Close(); err != nil {
		t.Fatalf("original cache Close: %v", err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.Views != 0 || stats.ExternalLeases != 0 || stats.Sources != 0 || stats.Generations != 0 {
		t.Fatalf("original finite budget retains owners: %+v", stats)
	}
	reopened, err := backenddb.Open(reopenOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	reopenedManager := collections.NewCollectionManager(reopened)
	defer func() {
		if err := reopenedManager.Close(); err != nil {
			t.Error(err)
		}
	}()
	reopenedCol, err := reopenedManager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	checkRows("reopened persisted rows", reopenedCol.Get)
	reopenedFiller, err := reopenedManager.OpenCollection("leaf_pack_filler")
	if err != nil {
		t.Fatal(err)
	}
	checkLaptopGraphOnlyPackPublicFiller(t, "reopened persisted filler", reopenedFiller)
	status, err := reopenedCol.VectorIndexStatus("vec")
	if err != nil || !status.Loaded || status.RebuildNeeded {
		t.Fatalf("reopened persisted graph=%+v err=%v", status, err)
	}
}
