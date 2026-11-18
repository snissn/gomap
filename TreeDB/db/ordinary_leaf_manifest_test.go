package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOrdinaryLeafManifestPublicationRoutes(t *testing.T) {
	for _, route := range []string{"queued", "inline"} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			database, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
			t.Cleanup(func() {
				if database != nil {
					closeLeafGenerationPackPublicationTestDB(t, database, leafLog)
				}
			})
			seed := prepareLeafGenerationPackTestCandidate(t, database, leafLog, 512)
			pack, err := database.LeafGenerationPack(ctx, LeafGenerationPackOptions{
				GenerationIDs: []uint64{seed.generation.GenerationID}, Force: true, Sync: true,
			})
			if err != nil || len(pack.CreatedFileIDs) == 0 {
				t.Fatalf("maintained pack: %+v %v", pack, err)
			}
			if route == "inline" {
				disableRootPublicationForLegacyVacuumBenchmark(t, database)
			}

			type authority struct {
				revision uint64
				digest   [32]byte
			}
			slots := func() (out map[uint64]authority) {
				t.Helper()
				capture, err := database.CaptureRecoverableRootSetForInspection(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := capture.ReleaseWithError(); err != nil {
						t.Error(err)
					}
				}()
				out = make(map[uint64]authority)
				for _, root := range capture.Roots() {
					if !root.Durable {
						continue
					}
					resources := capture.resourcesForRoot(root)
					if resources == nil {
						t.Fatalf("missing resources for durable commit %d", root.CommitSeq)
					}
					if err := resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
						manifest, exact, err := loadSelectedDurableLeafGenerationManifestTokens(tokens)
						if err == nil && exact {
							for _, token := range tokens {
								if token.Kind() == rootpublication.ResourceOuterLeafManifest {
									out[root.CommitSeq] = authority{manifest.ManifestRevision, token.Digest()}
								}
							}
						}
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
				return out
			}
			initial, _ := database.StateToken()
			older := slots()[initial.CommitSeq]
			if older.revision == 0 || older.digest == ([32]byte{}) {
				t.Fatal("pack did not install exact manifest authority")
			}

			store := database.leafGenerationManifestStore
			syncTotals := func() [2]uint64 {
				return [2]uint64{store.durabilityCounters.ContentSyncs.Load(), store.durabilityCounters.NamespaceSyncs.Load()}
			}
			immutableNames := func() []string {
				t.Helper()
				names, err := filepath.Glob(filepath.Join(LeafLogDirPath(dir), "manifest.durable.*.json"))
				if err != nil {
					t.Fatal(err)
				}
				return names
			}
			priorRevisions := immutableNames()
			compatibilityPath := leafGenerationManifestPath(LeafLogDirPath(dir))
			readCompatibility := func() []byte {
				t.Helper()
				data, err := os.ReadFile(compatibilityPath)
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			priorView := readCompatibility()
			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			_, taggedFileID := currentLeafSegmentOrFatal(t, leafLog)
			rawFileID := page.ValueLogSegmentID(taggedFileID)
			key := []byte("pack-concurrent-0000")
			cut := errors.New("refuse ordinary stable manifest before publication")
			store.hooks.BeforeTempCreate = func() error { return cut }
			err = database.SetSync(key, []byte("rotated"))
			store.hooks.BeforeTempCreate = nil
			checkRefusal := func(err, want error) {
				t.Helper()
				if !errors.Is(err, want) || CommitPublicationAccepted(err) {
					t.Fatalf("publication refusal: %v", err)
				}
				if state, _ := database.StateToken(); state != initial {
					t.Fatalf("refused operation changed visible state: before=%+v after=%+v", initial, state)
				}
				if !slices.Equal(immutableNames(), priorRevisions) || !bytes.Equal(readCompatibility(), priorView) {
					t.Fatalf("refused operation changed manifest namespace: before=%v after=%v", priorRevisions, immutableNames())
				}
			}
			checkRefusal(err, cut)
			if route == "queued" {
				database.testFailDurableRootVisibleInstall.Store(true)
				err = database.SetSync(key, []byte("rotated"))
				database.testFailDurableRootVisibleInstall.Store(false)
				checkRefusal(err, errTestDurableRootVisibleInstallFailpoint)
			}

			var retryCuts atomic.Int32
			var retrying atomic.Bool
			var blocked atomic.Bool
			syncEntered := make(chan struct{})
			continueSync := make(chan struct{})
			retrying.Store(route == "inline")
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.BeforeDependencyFileSync && event.Resource == durabilitycut.ResourceOuterLeaf &&
					strings.HasPrefix(filepath.Base(event.Path), "manifest.durable.") && blocked.CompareAndSwap(false, true) {
					close(syncEntered)
					<-continueSync
				}
				if retrying.Load() && event.Point == durabilitycut.BeforeMetaWrite && event.Resource == durabilitycut.ResourceMeta && retryCuts.Add(1) == 1 {
					return cut
				}
				return nil
			})
			defer restore()
			previousSyncs := syncTotals()
			batch := database.NewBatch()
			if err := batch.Set(key, []byte("rotated")); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- errors.Join(batch.WriteSync(), batch.Close()) }()
			select {
			case <-syncEntered:
			case err := <-done:
				close(continueSync)
				t.Fatalf("write returned before blocking in the immutable manifest sync: %v", err)
			case <-time.After(10 * time.Second):
				close(continueSync)
				t.Fatal("write did not reach the immutable manifest sync")
			}
			if !database.commitMu.TryLock() {
				t.Error("immutable manifest sync holds commitMu")
			} else {
				database.commitMu.Unlock()
			}
			if !database.writeMu.TryLock() {
				t.Error("immutable manifest sync holds writeMu")
			} else {
				database.writeMu.Unlock()
			}
			if !database.mu.TryLock() {
				t.Error("immutable manifest sync holds the visible state lock")
			} else {
				database.mu.Unlock()
			}
			if !database.rootReuseMu.TryRLock() {
				t.Error("immutable manifest sync blocks reader admission")
			} else {
				database.rootReuseMu.RUnlock()
			}
			if database.durablePublishMu.TryLock() {
				database.durablePublishMu.Unlock()
				t.Error("blocked manifest lost next-root sequencing")
			}
			if state, _ := database.StateToken(); state != initial {
				t.Errorf("root became visible before manifest sync: before=%+v after=%+v", initial, state)
			}
			close(continueSync)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("write did not finish after the immutable manifest sync resumed")
			}
			retrying.Store(false)
			if route == "inline" && retryCuts.Load() != 2 {
				t.Fatalf("inline write did not retry once with the same prepared authority: cuts=%d", retryCuts.Load())
			}
			if got := syncTotals(); got != [2]uint64{previousSyncs[0] + 1, previousSyncs[1] + 1} {
				t.Fatalf("rotated leaf manifest producer syncs: before=%v after=%v", previousSyncs, got)
			}
			current, _ := database.StateToken()
			currentSlots := slots()
			latest := currentSlots[current.CommitSeq]
			if latest.revision <= older.revision || latest.digest == older.digest || currentSlots[initial.CommitSeq] != older {
				t.Fatalf("successor or predecessor authority is wrong: before=%+v after=%+v", older, currentSlots)
			}
			state := database.state.Load()
			if state.LeafGenerations.FileToGeneration[rawFileID] == 0 || len(database.snapshotLeafGenerationPendingFileIDs(0)) != 0 {
				t.Fatalf("rotated file %d is uncovered or pending IDs were not consumed", rawFileID)
			}

			previousSyncs = syncTotals()
			if err := database.SetSync(key, []byte("steady")); err != nil {
				t.Fatal(err)
			}
			steady, _ := database.StateToken()
			if current := slots()[steady.CommitSeq]; current != latest || syncTotals() != previousSyncs {
				t.Fatalf("steady write replaced manifest authority: prior=%+v got=%+v syncs=%v->%v", latest, current, previousSyncs, syncTotals())
			}
			operational, err := database.PrepareLeafGenerationManifestStableClosure()
			if err != nil {
				t.Fatal(err)
			}
			operationalRevision := operational.Revision()
			if err := operational.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if operationalRevision <= latest.revision {
				t.Fatalf("operational-only replacement revision=%d, exact revision=%d", operationalRevision, latest.revision)
			}
			database.durablePublishMu.Lock()
			// A cache entry with the same revision and different digest gives
			// no coverage credit to the current slot's retained token.
			database.ordinaryLeafManifestCache.authority.digest[0] ^= 1
			database.ordinaryLeafManifestCache.view = &leafGenerationView{}
			database.durablePublishMu.Unlock()
			previousSyncs = syncTotals()
			for _, value := range []string{"after operational replacement", "steady"} {
				if err := database.SetSync(key, []byte(value)); err != nil {
					t.Fatal(err)
				}
				state, _ := database.StateToken()
				if current := slots()[state.CommitSeq]; current != latest || syncTotals() != previousSyncs {
					t.Fatalf("operational revision displaced exact authority: prior=%+v got=%+v syncs=%v->%v", latest, current, previousSyncs, syncTotals())
				}
			}
			t.Logf("route=%s rotated_raw=%d packed_revision=%d successor_revision=%d retry_cuts=%d manifest_syncs=%v",
				route, rawFileID, older.revision, latest.revision, retryCuts.Load(), [2]uint64{1, 1})

			closeLeafGenerationPackPublicationTestDB(t, database, leafLog)
			database = nil
			reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			if _, err := reopened.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{}); err != nil {
				t.Fatal(err)
			}
			if value, err := reopened.Get(key); err != nil || string(value) != "steady" {
				t.Fatalf("reopened value=%q err=%v", value, err)
			}
		})
	}
}

func TestOrdinaryLeafManifestWALRefusalStopsNextWriter(t *testing.T) {
	for _, route := range []string{"queued", "inline", "serialized-queued", "serialized-inline"} {
		t.Run(route, func(t *testing.T) {
			requireLeafGenerationPackPromotionSupport(t)
			dir := t.TempDir()
			opts := leafGenerationPackPublicationTestOptions(dir)
			opts.CommandWAL = true
			opts.Durability = DurabilityWALOnRelaxed
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
			leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
			database.SetLeafPageLog(leafLog)
			t.Cleanup(func() {
				if database != nil {
					_ = database.Close()
					if err := leafLog.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			seed := prepareLeafGenerationPackTestCandidate(t, database, leafLog, 512)
			if pack, err := database.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
				GenerationIDs: []uint64{seed.generation.GenerationID}, Force: true, Sync: true, ReserveRIDs: leafLog.reserveRecordRIDs,
			}); err != nil || len(pack.CreatedFileIDs) == 0 {
				t.Fatalf("maintained pack: %+v %v", pack, err)
			}
			if strings.HasSuffix(route, "inline") {
				disableRootPublicationForLegacyVacuumBenchmark(t, database)
			}
			before, _ := database.StateToken()
			beforeNextLSN := database.CommandWALNextLSN()
			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}

			syncEntered := make(chan struct{})
			continueSync := make(chan struct{})
			cut := errors.New("refuse manifest content sync after raw command append")
			var blocked atomic.Bool
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.BeforeDependencyFileSync && event.Resource == durabilitycut.ResourceOuterLeaf &&
					strings.HasPrefix(filepath.Base(event.Path), "manifest.durable.") && blocked.CompareAndSwap(false, true) {
					close(syncEntered)
					<-continueSync
					return cut
				}
				return nil
			})
			defer restore()
			write := func(key, value string, serialized bool) error {
				batch := database.NewBatch()
				if err := batch.Set([]byte(key), []byte(value)); err != nil {
					return errors.Join(err, batch.Close())
				}
				if serialized {
					// Enter the actual fallback directly so its coverage does not depend
					// on scheduling enough optimistic conflicts.
					err := func() error {
						b := batch.(*Batch)
						release, err := database.LockCommandWALPublishWithBarriers()
						if err != nil {
							return err
						}
						defer release()
						revision := database.assignBatchEntryRevisions(b.batch)
						intent, err := database.prepareRawKVCommandWALIntent(b, true)
						if err != nil {
							return err
						}
						defer releaseUnassignedCommandWALIntent(intent)
						return b.writeSerialized(true, intent, revision, nil)
					}()
					return errors.Join(err, batch.Close())
				}
				return errors.Join(batch.WriteSync(), batch.Close())
			}
			first := make(chan error, 1)
			serialized := strings.HasPrefix(route, "serialized-")
			go func() { first <- write("pack-concurrent-0000", "replay this command", serialized) }()
			select {
			case <-syncEntered:
			case err := <-first:
				close(continueSync)
				t.Fatalf("first writer returned before manifest content sync: %v", err)
			case <-time.After(10 * time.Second):
				close(continueSync)
				t.Fatal("first writer did not reach manifest content sync")
			}
			if got := database.CommandWALNextLSN(); got != beforeNextLSN+1 {
				t.Errorf("manifest sync was not after one assigned command: next LSN=%d want=%d", got, beforeNextLSN+1)
			}
			second := make(chan error, 1)
			go func() { second <- write("raw-rollover-second", "must not append", false) }()
			if serialized {
				// Stop the caller in pending-pointer cleanup after finalization.
				// A separate publisher must see poison as soon as it owns the gate.
				database.pendingValueLogAppendMu.Lock()
				gatePoison := make(chan bool, 1)
				go func() {
					database.durablePublishMu.Lock()
					gatePoison <- errors.Is(database.commandWALPoisonedError(), ErrRecoveryRequired)
					database.durablePublishMu.Unlock()
				}()
				close(continueSync)
				select {
				case poisoned := <-gatePoison:
					if !poisoned {
						t.Error("serialized writer released the durable gate before poisoning its unapplied WAL frame")
					}
				case <-time.After(10 * time.Second):
					t.Error("durable publication gate did not release after manifest refusal")
				}
				database.pendingValueLogAppendMu.Unlock()
			} else {
				close(continueSync)
			}
			select {
			case err := <-first:
				if !errors.Is(err, cut) || CommitPublicationAccepted(err) {
					t.Errorf("first writer refusal: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("first writer did not finish after refusal")
			}
			select {
			case err := <-second:
				if !errors.Is(err, ErrRecoveryRequired) {
					t.Errorf("following writer did not observe the poisoned command prefix: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("following writer did not finish after refusal")
			}
			after, _ := database.StateToken()
			if after != before || database.CommandWALNextLSN() != beforeNextLSN+1 {
				t.Errorf("refusal advanced visible state or assigned a second command: before=%+v after=%+v next=%d", before, after, database.CommandWALNextLSN())
			}
			if err := database.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
				t.Errorf("root publication did not stay poisoned: %v", err)
			}
			if err := database.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
			if pins := database.valueLogIdentityPins.ActivePins(); pins != 0 {
				t.Errorf("close retained %d stable identity pins", pins)
			}
			if err := leafLog.Close(); err != nil {
				t.Error(err)
			}
			database = nil
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			value, err := reopened.Get([]byte("pack-concurrent-0000"))
			if err != nil || string(value) != "replay this command" {
				t.Fatalf("reopen did not replay the sole assigned command: %q %v", value, err)
			}
			value, err = reopened.Get([]byte("raw-rollover-second"))
			if err != nil || value != nil {
				t.Fatalf("following writer became visible after reopen: %q %v", value, err)
			}
		})
	}
}
