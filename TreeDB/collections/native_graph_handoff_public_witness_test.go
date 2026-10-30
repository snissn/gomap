package collections

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const nativeGraphHandoffWitnessChildEnv = "TREEDB_NATIVE_GRAPH_HANDOFF_WITNESS_CHILD"
const nativeGraphHandoffWitnessSuccess = "NATIVE_GRAPH_HANDOFF_WITNESS_COMPLETED"

// Go testing matches slash-separated components independently. Anchoring only
// the whole name permits changed-manifest to match unchanged-manifest's suffix.
func nativeGraphHandoffWitnessRunPattern(name string) string {
	parts := strings.Split(name, "/")
	for i := range parts {
		parts[i] = "^" + regexp.QuoteMeta(parts[i]) + "$"
	}
	return strings.Join(parts, "/")
}

func TestNativeGraphHandoffWitnessRunPattern(t *testing.T) {
	const parent = "TestNativeOrdinaryGraphAcceptedAcrossCapturedManifestBuild"
	for _, selected := range []string{"sequential", "changed-manifest", "unchanged-manifest"} {
		pattern := strings.Split(nativeGraphHandoffWitnessRunPattern(parent+"/"+selected), "/")
		if len(pattern) != 2 || !regexp.MustCompile(pattern[0]).MatchString(parent) || regexp.MustCompile(pattern[0]).MatchString(parent+"Extra") {
			t.Fatalf("invalid parent selector: %q", pattern)
		}
		for _, candidate := range []string{"sequential", "changed-manifest", "unchanged-manifest", selected+"Extra"} {
			if got := regexp.MustCompile(pattern[1]).MatchString(candidate); got != (candidate == selected) {
				t.Fatalf("selected=%q candidate=%q matched=%v", selected, candidate, got)
			}
		}
	}
	if got := nativeGraphHandoffWitnessRunPattern("Parent/a.b"); got != "^Parent$/^a\\.b$" {
		t.Fatalf("literal component was not quoted: %q", got)
	}
}

// Worker APIs have no cancellation contract. Keep the original public calls in
// a child so a failed join cannot leak a worker into another test or race DB
// teardown against that worker. Normal and race runs use the same test binary.
func enterNativeGraphHandoffWitnessChild(t *testing.T) bool {
	t.Helper()
	if selected := os.Getenv(nativeGraphHandoffWitnessChildEnv); selected != "" {
		if selected != t.Name() {
			t.Fatalf("INVALID_WITNESS child selector=%q want=%q", selected, t.Name())
		}
		// Registered before fixture construction; runs after fixture defers and only
		// certifies a normal, non-skipped completion of the whole child case.
		t.Cleanup(func() {
			if !t.Failed() && !t.Skipped() {
				t.Log(nativeGraphHandoffWitnessSuccess)
			}
		})
		return true
	}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run="+nativeGraphHandoffWitnessRunPattern(t.Name()), "-test.count=1", "-test.timeout=2m", "-test.v")
	cmd.WaitDelay = 5 * time.Second
	prefix := nativeGraphHandoffWitnessChildEnv + "="
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, prefix) {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, prefix+t.Name())
	// CombinedOutput waits/reaps the child even on context cancellation. Retain
	// timeout stacks and assertion output instead of retrying an invalid setup.
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("INVALID_WITNESS child process deadline err=%v deadline=%v:\n%s", err, ctx.Err(), output)
	}
	if err != nil {
		// Preserve the child's discriminator: a valid ROOT_HANDOFF_WITNESS
		// refusal is a failed assertion, not an invalid setup by definition.
		t.Fatalf("isolated witness child failed: %v\n%s", err, output)
	}
	if !bytes.Contains(output, []byte(nativeGraphHandoffWitnessSuccess)) || bytes.Contains(output, []byte("INVALID_WITNESS")) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("INVALID_WITNESS child missing clean completion:\n%s", output)
	}
	t.Logf("isolated witness completion:\n%s", output)
	return false
}

// A controlled public-path witness, not attribution of the original race7 job.
// Every setup miss is an explicit failure, not a skipped or weakened assertion.
// The existing documentservice4096/128 cold-hybrid test remains unchanged.
func TestNativeOrdinaryGraphAcceptedAcrossCapturedManifestBuild(t *testing.T) {
	for _, mode := range []string{"sequential", "changed-manifest", "unchanged-manifest"} {
		t.Run(mode, func(t *testing.T) {
			overlap := mode != "sequential"
			unchanged := mode == "unchanged-manifest"
			if !ordinaryColumnStableAuthorityEnabled() {
				t.Skip("stable relative namespace unavailable")
			}
			if !enterNativeGraphHandoffWitnessChild(t) {
				return
			}
			clearNativeOrdinaryPendingGraphCodecEnv(t)
			opts := nativeTypedGroupOptions(t.TempDir()) // unchanged default finite admission limits
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			defer func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			}()
			meta := nativeOrdinaryPendingGraphMeta()
			if _, err := manager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			var pause *Collection
			if unchanged {
				if _, err := manager.CreateCollection(&CollectionMeta{Name: "build_pause", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
					Enabled: true, Columns: []ColumnStoreColumn{{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset}},
					RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
				}}}); err != nil {
					t.Fatal(err)
				}
				pause, err = manager.OpenCollection("build_pause")
				if err != nil {
					t.Fatal(err)
				}
			}
			insert := func(start, end int) {
				t.Helper()
				ids := make([][]byte, end-start)
				docs := make([][]byte, end-start)
				for i := range ids {
					n := start + i
					ids[i] = []byte(fmt.Sprintf("doc-%04d", n))
					docs[i] = []byte(fmt.Sprintf(`{"embedding":[%d,1]}`, n+1))
				}
				if _, err := col.InsertBatch(ids, docs); err != nil {
					t.Fatalf("INVALID_WITNESS seed[%d:%d]: %v", start, end, err)
				}
			}
			// This controlled witness needs one physical prefix and one pending
			// public batch, not the original failure's 4096-row capacity load.
			// Keep the 128-row batch shape and default finite admission limits;
			// acceptance below remains mandatory and fails closed. The original
			// documentservice 4096/128 test remains independently unchanged.
			insert(0, 128)
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			insert(128, 256)
			if !overlap {
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if status, err := col.RebuildVectorIndex("vec"); err != nil || !status.Loaded || status.RebuildNeeded {
					t.Fatalf("sequential control: %+v %v", status, err)
				}
				return
			}
			if unchanged {
				// Drain all target rows, then create real pending assets only
				// in the independent collection. Its old build has no target
				// manifest result to hand off; graph construction still overlaps.
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if _, _, err := pause.InsertTypedBatchWithStats([][]byte{[]byte("pending")}, [][]byte{[]byte(`{"id":"pending"}`)}, []TypedColumnBatch{{Name: "label", Strings: []string{"pending"}}}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := backend.ObserveNativeProducer()
			if err != nil || before.UnderPressure {
				t.Fatalf("INVALID_WITNESS producer would join held worker: %+v %v", before, err)
			}
			old, err := col.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := old.Close(); err != nil {
					t.Error(err)
				}
			}()
			if old.capture.NativeAllocator().(*memtable.COWBudget).Limits() != memtable.DefaultCOWLimits() {
				t.Fatal("INVALID_WITNESS changed finite limits")
			}
			manifest, ok := old.collection.domains.Get("column/manifest")
			primary, pok := old.collection.domains.Get("primary")
			if !ok || !pok || manifest.baseRoot == 0 || (!unchanged && primary.typed == nil) {
				t.Fatal("INVALID_WITNESS no real manifest/pending typed boundary")
			}
			oldRoot, boundary := manifest.baseRoot, primary.typed
			oldEngine, ok := old.capture.State().(*nativeEngineVersion)
			if !ok {
				t.Fatal("INVALID_WITNESS no original engine capture")
			}
			var pauseIdentity uint64
			if unchanged {
				if primary.typed != nil {
					t.Fatal("INVALID_WITNESS no-root-change target still has typed suffix")
				}
				if _, pending := oldEngine.pending.Get(old.collection.incarnation); pending {
					t.Fatal("INVALID_WITNESS no-root-change target in old pending inventory")
				}
				pauseState, found := oldEngine.names.Get("build_pause")
				if !found {
					t.Fatal("INVALID_WITNESS missing independent pause collection")
				}
				pauseIdentity = pauseState.incarnation
				if _, pending := oldEngine.pending.Get(pauseIdentity); !pending {
					t.Fatal("INVALID_WITNESS independent pause has no real pending inventory")
				}
			}
			for d := boundary; d != nil; d = d.previous {
				if d.data.graph != nil {
					t.Fatal("INVALID_WITNESS old prefix already contains graph")
				}
			}
			entered, release, oldDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			var holdOnce, releaseOnce sync.Once
			restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error { holdOnce.Do(func() { close(entered); <-release }); return nil })
			defer restore()
			graphDone := make(chan error, 1)
			oldJoined, graphStarted, graphJoined := false, false, false
			var laterCaptures []*NativeJSONRead
			defer func() {
				releaseOnce.Do(func() { close(release) })
				for _, job := range []struct {
					name   string
					done   <-chan error
					joined bool
				}{{"old checkpoint", oldDone, oldJoined}, {"graph", graphDone, !graphStarted || graphJoined}} {
					if job.joined {
						continue
					}
					select {
					case err := <-job.done:
						if err != nil {
							t.Errorf("%s completion: %v", job.name, err)
						}
					case <-time.After(15 * time.Second):
						t.Errorf("INVALID_WITNESS %s did not join after release", job.name)
						// Do not unwind into capture/manager/DB Close with a
						// live worker. Only this isolated child waits; its Go
						// timeout prints stacks and its parent kills/reaps it
						// if necessary. No cancellation contract is invented.
						<-job.done
					}
				}
				// Captures acquired after worker launch must not register
				// later defers: those would run BEFORE this join on failure.
				for i := len(laterCaptures) - 1; i >= 0; i-- {
					if err := laterCaptures[i].Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			go func() { oldDone <- owner.Checkpoint() }()
			select {
			case <-entered:
			case err := <-oldDone:
				oldJoined = true
				t.Fatalf("INVALID_WITNESS old checkpoint missed typed hook: %v", err)
			case <-time.After(15 * time.Second):
				t.Fatal("INVALID_WITNESS old typed hook not reached")
			}
			pressure, err := backend.ObserveNativeProducer()
			if err != nil || pressure.UnderPressure {
				t.Fatalf("INVALID_WITNESS held-build producer pressure: %+v %v", pressure, err)
			}
			expectedLSN := backend.CommandWALNextLSN()
			observer, err := backenddb.NewNativeInstallObserverForTest(backend, commitlog.CommandKindCollectionRebuildVectorIndex, expectedLSN)
			if err != nil {
				t.Fatal(err)
			}
			defer observer.Close()
			graphStarted = true
			go func() { _, err := col.RebuildVectorIndex("vec"); graphDone <- err }()
			var event backenddb.NativeInstalledObservationForTest
			select {
			case event = <-observer.Events():
			case err := <-graphDone:
				graphJoined = true
				t.Fatalf("INVALID_WITNESS graph did not reach accepted suffix: %v", err)
			case <-time.After(15 * time.Second):
				t.Fatal("INVALID_WITNESS graph acceptance blocked; release original build")
			}
			if observer.Dropped() || !event.WALCompleted || !event.WALStable || event.AssignedLSN != expectedLSN || event.LogicalSequence <= before.LogicalSequence {
				t.Fatalf("INVALID_WITNESS unqualified acceptance: %+v dropped=%t", event, observer.Dropped())
			}
			accepted, err := col.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			laterCaptures = append(laterCaptures, accepted)
			acceptedManifest, mok := accepted.collection.domains.Get("column/manifest")
			acceptedPrimary, aok := accepted.collection.domains.Get("primary")
			if !mok || !aok || acceptedManifest.baseRoot != oldRoot || accepted.collection.incarnation != old.collection.incarnation {
				t.Fatal("INVALID_WITNESS graph capture changed basis/incarnation")
			}
			graph := acceptedPrimary.typed
			if graph == nil || graph.data.graph == nil || graph.appliedLSN != event.AssignedLSN || graph.data.graph.sourceRoot != oldRoot {
				t.Fatalf("INVALID_WITNESS no exact original recipe root=%d lsn=%d", oldRoot, event.AssignedLSN)
			}
			found := unchanged && boundary == nil && graph.previous == nil
			for d := graph.previous; d != nil; d = d.previous {
				if d == boundary {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("INVALID_WITNESS graph is not newer than original captured boundary")
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-oldDone:
				oldJoined = true
				if err != nil {
					t.Fatalf("old captured build: %v", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("INVALID_WITNESS old build did not complete")
			}
			// oldDone synchronizes the completed worker's writes. The held
			// original capture keeps this exact engine and its ledger alive.
			// Inspect it BEFORE joining graphDone: the graph's own later
			// checkpoint must not supply the old-build root-change witness.
			build := oldEngine.build
			if build == nil || !build.complete || build.failure != nil || build.handoffs == 0 {
				t.Fatal("INVALID_WITNESS no completed original handoff ledger")
			}
			var oldBuiltRoot uint64
			if !unchanged {
				matches := 0
				for _, result := range build.built {
					if result.collection == old.collection.incarnation && result.name == "column/manifest" {
						matches++
						if result.oldBaseRoot != oldRoot || result.root == 0 || result.root == oldRoot {
							t.Fatalf("INVALID_WITNESS old target manifest basis=%d want=%d root=%d", result.oldBaseRoot, oldRoot, result.root)
						}
						oldBuiltRoot = result.root
					}
				}
				if matches != 1 {
					t.Fatalf("INVALID_WITNESS original target manifest results=%d want=1", matches)
				}
			}
			if unchanged {
				// The exact original captured engine owns this build ledger.
				// Read it only after the old worker joined. Its complete real
				// result inventory must exclude every target domain and typed
				// work item; the handoff therefore cannot rewrite that manifest.
				pauseBuilt := false
				for _, result := range build.built {
					if result.collection == old.collection.incarnation {
						t.Fatal("INVALID_WITNESS old build touched graph collection")
					}
					if result.collection == pauseIdentity && result.name == "column/manifest" && result.root != result.oldBaseRoot {
						pauseBuilt = true
					}
				}
				for _, work := range build.typed {
					if work.collection == old.collection.incarnation {
						t.Fatal("INVALID_WITNESS old build had graph typed work")
					}
				}
				if !pauseBuilt {
					t.Fatal("INVALID_WITNESS independent build did not advance its own manifest")
				}
			}
			var graphErr error
			select {
			case graphErr = <-graphDone:
				graphJoined = true
			case <-time.After(15 * time.Second):
				t.Fatal("INVALID_WITNESS graph did not complete after old handoff")
			}
			current, err := col.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatalf("INVALID_WITNESS post-handoff capture=%v; original graph completion=%v", err, graphErr)
			}
			laterCaptures = append(laterCaptures, current)
			currentManifest, ok := current.collection.domains.Get("column/manifest")
			if !ok || (!unchanged && currentManifest.baseRoot == oldRoot) {
				t.Fatal("INVALID_WITNESS no actual manifest-root advance")
			}
			if graphErr != nil && unchanged {
				t.Fatalf("no-root-change control: accepted_lsn=%d original_manifest=%d: %v", event.AssignedLSN, oldRoot, graphErr)
			}
			if graphErr != nil {
				currentPrimary, ok := current.collection.domains.Get("primary")
				preserved := false
				if ok {
					for d := currentPrimary.typed; d != nil; d = d.previous {
						if d.data == graph.data && d.appliedLSN == event.AssignedLSN {
							preserved = true
							break
						}
					}
				}
				if !preserved {
					t.Fatalf("INVALID_WITNESS original graph suffix not retained: %v", graphErr)
				}
				if strings.Contains(graphErr.Error(), "ordinary graph basis") {
					t.Fatalf("ROOT_HANDOFF_WITNESS original_recipe=%d old_build_manifest=%d current_manifest=%d accepted_lsn=%d logical_seq=%d: %v", oldRoot, oldBuiltRoot, currentManifest.baseRoot, event.AssignedLSN, event.LogicalSequence, graphErr)
				}
				t.Fatalf("distinct accepted graph failure (not attributed to root guard): %v", graphErr)
			}
			if observer.Dropped() {
				t.Fatal("INVALID_WITNESS duplicate/full observation")
			}
			// Success remains subject to existing hybrid/cold/reopen qualification; this
			// focused control only proves the accepted graph survived its old prefix.
			for _, n := range []int{0, 128, 4095} {
				id := []byte(fmt.Sprintf("doc-%04d", n))
				want := []byte(fmt.Sprintf(`{"embedding":[%d,1]}`, n+1))
				if got, err := current.Get(id); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("post-handoff row %q: %s %v", id, got, err)
				}
			}
			if status, err := col.nativeVectorGraphStatus("vec"); err != nil || !status.Loaded || status.RebuildNeeded {
				t.Fatalf("post-handoff graph status: %+v %v", status, err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
