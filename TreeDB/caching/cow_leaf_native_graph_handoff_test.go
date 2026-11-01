package caching

import (
	"bytes"
	"context"
	"encoding/binary"
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

const privateCOWGraphPackChildEnv = "TREEDB_PRIVATE_COW_GRAPH_PACK_CHILD"
const privateCOWGraphPackCompleted = "PRIVATE_COW_GRAPH_PACK_COMPLETED"

// Public graph calls have no cancellation contract. Isolate the original calls
// so a failed join cannot race database teardown or leak into another test.
func enterPrivateCOWGraphPackChild(t *testing.T) bool {
	t.Helper()
	if selected := os.Getenv(privateCOWGraphPackChildEnv); selected != "" {
		if selected != t.Name() {
			t.Fatalf("INVALID_WITNESS child selector=%q want=%q", selected, t.Name())
		}
		t.Cleanup(func() {
			if !t.Failed() && !t.Skipped() {
				t.Log(privateCOWGraphPackCompleted)
			}
		})
		return true
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.timeout=90s", "-test.v")
	cmd.WaitDelay = 5 * time.Second
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, privateCOWGraphPackChildEnv+"=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, privateCOWGraphPackChildEnv+"="+t.Name())
	output, err := cmd.CombinedOutput()
	if err != nil || ctx.Err() != nil || !bytes.Contains(output, []byte(privateCOWGraphPackCompleted)) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("isolated leaf/graph witness failed: err=%v deadline=%v\n%s", err, ctx.Err(), output)
	}
	t.Logf("isolated leaf/graph witness:\n%s", output)
	return false
}

// The ordinary graph profile is immutable and must be supplied at Open.
// privateCOWPackFixture's empty-profile relaxed backend cannot admit this graph.
// Keep its actual held backend, native bridge, pack geometry and finite limits.
func privateCOWGraphPackFixture(t *testing.T) (*DB, *privateCOWPackHeldBackend, backenddb.Options) {
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
	cache.testSkipVlogCheckpointKick = true
	skipRetainedPrune(cache)
	held := &privateCOWPackHeldBackend{DB: native}
	cache.backend = held
	return cache, held, opts
}

func privateCOWGraphPackManifestRoot(t *testing.T, snapshot *backenddb.Snapshot, name string) uint64 {
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

func privateCOWGraphPackCachedManifestRoot(t *testing.T, cache *DB, name string) uint64 {
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
	return privateCOWGraphPackManifestRoot(t, cut.basis.snapshot, name)
}

// A causal regression for physical leaf-basis adoption, not attribution of the
// historical race7 failure. This low-level native fixture uses the original
// inline-sync command ACK; it does not qualify public Open's completion group.
// Both graph completion and original Close must succeed; a guard refusal is RED.
func TestPrivateCOWLeafPackPreservesAcceptedOrdinaryGraph(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	if !enterPrivateCOWGraphPackChild(t) {
		return
	}
	cache, backend, reopenOptions := privateCOWGraphPackFixture(t)
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
	// The maintained helper writes/checkpoints/rotates; no map or root is forged.
	preparePrivateCOWPack(t, cache, backend.DB)
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
	oldRoot := privateCOWGraphPackCachedManifestRoot(t, cache, meta.Name)
	before, err := backend.DB.ObserveNativeProducer()
	if err != nil || before.UnderPressure {
		t.Fatalf("INVALID_WITNESS producer requires a drain: %+v err=%v", before, err)
	}
	backend.published = make(chan backenddb.LeafGenerationPackRunOnceStats, 1)
	backend.release = make(chan struct{})
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
		return privateCOWGraphPackManifestRoot(t, physical, meta.Name)
	}()
	if newRoot == oldRoot {
		t.Fatalf("INVALID_WITNESS pack did not relocate target manifest root=%d", oldRoot)
	}
	cachedRoot := privateCOWGraphPackCachedManifestRoot(t, cache, meta.Name)
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
	release()
	select {
	case err := <-packDone:
		packJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("INVALID_WITNESS physical basis adoption did not join")
	}
	select {
	case err := <-graphDone:
		graphJoined = true
		if err != nil {
			t.Fatalf("accepted graph completion after physical adoption: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("INVALID_WITNESS accepted graph completion did not join")
	}
	if observer.Dropped() {
		t.Fatal("INVALID_WITNESS duplicate graph acceptance")
	}
	checkRows("held old read after adoption", old.Get)
	checkRows("current after graph", col.Get)
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
	status, err := reopenedCol.VectorIndexStatus("vec")
	if err != nil || !status.Loaded || status.RebuildNeeded {
		t.Fatalf("reopened persisted graph=%+v err=%v", status, err)
	}
}
