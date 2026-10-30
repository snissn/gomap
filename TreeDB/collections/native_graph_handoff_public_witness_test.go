package collections

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// A controlled public-path witness, not attribution of the original race7 job.
// Every setup miss is an explicit failure, not a skipped or weakened assertion.
// The existing documentservice4096/128 cold-hybrid test remains unchanged.
func TestNativeOrdinaryGraphAcceptedAcrossCapturedManifestBuild(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlap-%t", overlap), func(t *testing.T) {
			if !ordinaryColumnStableAuthorityEnabled() {
				t.Skip("stable relative namespace unavailable")
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
			insert(0, 128)
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for start := 128; start < 4096; start += 128 {
				insert(start, start+128)
			}
			if !overlap {
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if _, err := col.RebuildVectorIndex("vec"); err != nil {
					t.Fatalf("sequential control: %v", err)
				}
				return
			}
			before, err := backend.ObserveNativeProducer()
			if err != nil || before.UnderPressure {
				t.Fatalf("INVALID_WITNESS producer would join held worker: %+v %v", before, err)
			}
			old, err := col.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			if old.capture.NativeAllocator().(*memtable.COWBudget).Limits() != memtable.DefaultCOWLimits() {
				t.Fatal("INVALID_WITNESS changed finite limits")
			}
			manifest, ok := old.collection.domains.Get("column/manifest")
			primary, pok := old.collection.domains.Get("primary")
			if !ok || !pok || manifest.baseRoot == 0 || primary.typed == nil {
				t.Fatal("INVALID_WITNESS no real manifest/pending typed boundary")
			}
			oldRoot, boundary := manifest.baseRoot, primary.typed
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
			defer accepted.Close()
			acceptedManifest, mok := accepted.collection.domains.Get("column/manifest")
			acceptedPrimary, aok := accepted.collection.domains.Get("primary")
			if !mok || !aok || acceptedManifest.baseRoot != oldRoot || accepted.collection.incarnation != old.collection.incarnation {
				t.Fatal("INVALID_WITNESS graph capture changed basis/incarnation")
			}
			graph := acceptedPrimary.typed
			if graph == nil || graph.data.graph == nil || graph.appliedLSN != event.AssignedLSN || graph.data.graph.sourceRoot != oldRoot {
				t.Fatalf("INVALID_WITNESS no exact original recipe root=%d lsn=%d", oldRoot, event.AssignedLSN)
			}
			found := false
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
			var graphErr error
			select {
			case graphErr = <-graphDone:
				graphJoined = true
			case <-time.After(15 * time.Second):
				t.Fatal("INVALID_WITNESS graph did not complete after old handoff")
			}
			current, err := col.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			currentManifest, ok := current.collection.domains.Get("column/manifest")
			if !ok || currentManifest.baseRoot == oldRoot {
				t.Fatal("INVALID_WITNESS no actual manifest-root advance")
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
					t.Fatalf("ROOT_HANDOFF_WITNESS original_recipe=%d current_manifest=%d accepted_lsn=%d logical_seq=%d: %v", oldRoot, currentManifest.baseRoot, event.AssignedLSN, event.LogicalSequence, graphErr)
				}
				t.Fatalf("distinct accepted graph failure (not attributed to root guard): %v", graphErr)
			}
			if observer.Dropped() {
				t.Fatal("INVALID_WITNESS duplicate/full observation")
			}
			// Success remains subject to existing hybrid/cold/reopen qualification; this
			// focused control only proves the accepted graph survived its old prefix.
			for _, id := range []string{"doc-0000", "doc-0128", "doc-4095"} {
				if _, err := current.Get([]byte(id)); err != nil {
					t.Fatalf("post-handoff row %q: %v", id, err)
				}
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
