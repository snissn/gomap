package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOrdinaryLeafManifestReachabilityAndOwnership(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		reachable bool
	}{
		{name: "reachable_pack_refuses_publication", reachable: true},
		{name: "releases_projected_candidate_on_stage_failure"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			database, leafLog := openLeafGenerationPackPublicationTestDB(t, t.TempDir())
			registry := database.valueLogIdentityPins
			t.Cleanup(func() {
				closeLeafGenerationPackPublicationTestDB(t, database, leafLog)
				if got := registry.ActivePins(); got != 0 {
					t.Errorf("retained stable physical pins after close: %d", got)
				}
				if err := registry.CleanupError(); err != nil {
					t.Error(err)
				}
			})
			const keys = 512
			seed := prepareLeafGenerationPackTestCandidate(t, database, leafLog, keys)
			pack, err := database.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
				GenerationIDs: []uint64{seed.generation.GenerationID}, Force: true, Sync: true,
			})
			if err != nil || len(pack.CreatedFileIDs) != 1 {
				t.Fatalf("publish pack: %+v, %v", pack, err)
			}
			packed := pack.CreatedFileIDs[0]
			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("pack-tail-0030"), []byte("seal the pack")); err != nil {
				t.Fatal(err)
			}
			if !scenario.reachable {
				writeLeafGenerationKeys(t, database, "pack-concurrent", keys, 'c')
			}
			before, ok := database.StateToken()
			if !ok {
				t.Fatal("missing visible state")
			}
			next := page.MetaPageBody{CommitSeq: before.CommitSeq, UserRootPageID: before.RootPageID, SystemRootPageID: before.SystemRootPageID}
			references, err := database.scanCandidateValueLogReferencesV1(database.idx.Load(), next, false)
			if _, reachable := references[page.ValueLogFileID(packed)]; err != nil || reachable != scenario.reachable {
				t.Fatalf("current roots reference pack %d: %t, want %t, err=%v", packed, reachable, scenario.reachable, err)
			}

			// Omit the pack only from the operational view. Its physical file,
			// immutable manifest and recoverable slot dependencies are real.
			database.durablePublishMu.Lock()
			basis := database.recoverableDurableBasisLocked()
			selected := database.durableRoot.slotResources[database.durableRoot.slot]
			database.mu.Lock()
			original := database.leafGenerationManifest
			faulty := original.clone()
			index := slices.IndexFunc(faulty.Generations, func(generation leafGenerationRecord) bool {
				return slices.Contains(generation.FileIDs, packed)
			})
			sealed := index >= 0 && faulty.Generations[index].State == leafGenerationStateSealed
			if sealed {
				faulty.Generations[index].State = leafGenerationStateDeleted
				database.leafGenerationManifest = faulty
			}
			database.mu.Unlock()
			database.durablePublishMu.Unlock()
			if !sealed {
				t.Fatalf("operational manifest has no sealed pack %d", packed)
			}
			defer func() {
				database.durablePublishMu.Lock()
				database.mu.Lock()
				database.leafGenerationManifest = original
				database.mu.Unlock()
				database.durablePublishMu.Unlock()
			}()
			var identity rootpublication.StableIdentity
			if err := selected.WalkPhysicalResourceIdentities([]rootpublication.ResourceKind{rootpublication.ResourceOuterLeafPack},
				func(_ rootpublication.ResourceKind, generation uint64, candidate rootpublication.StableIdentity, _ [32]byte) error {
					if generation == uint64(page.ValueLogFileID(packed)) {
						identity = candidate
					}
					return nil
				}); err != nil {
				t.Fatal(err)
			}
			pins := registry.PinCount(identity)
			if pins == 0 {
				t.Fatalf("earlier slot does not pin the exact pack %d", packed)
			}
			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			cut := errors.New("refuse immutable manifest after pack projection")
			store := database.leafGenerationManifestStore
			store.hooks.BeforeTempCreate = func() error { return cut }
			key := []byte("pack-tail-0031")
			err = database.SetSync(key, []byte("rotating write"))
			store.hooks.BeforeTempCreate = nil
			if scenario.reachable {
				want := fmt.Sprintf("publication leaf-generation manifest omits leaf file %d", packed)
				if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !strings.Contains(err.Error(), want) {
					t.Fatalf("live pack must fail the original coverage check: %v", err)
				}
			} else if !errors.Is(err, cut) {
				t.Fatalf("unreachable pack prevented manifest preparation: %v", err)
			}
			if CommitPublicationAccepted(err) {
				t.Fatalf("refused publication was accepted: %v", err)
			}
			if after, _ := database.StateToken(); after != before {
				t.Fatalf("refused root advanced visible state: before=%+v after=%+v", before, after)
			}
			database.durablePublishMu.Lock()
			sameBasis := basis.equal(database.recoverableDurableBasisLocked())
			database.durablePublishMu.Unlock()
			if !sameBasis || registry.PinCount(identity) != pins {
				t.Fatalf("refused candidate changed durable basis or predecessor pins: unchanged=%t, pins=%d want %d", sameBasis, registry.PinCount(identity), pins)
			}
			if got, err := database.Get(key); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{'z'}, 32)) {
				t.Fatalf("refused write altered the existing row: %q, %v", got, err)
			}
		})
	}
}

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
			if _, err := os.Stat(compatibilityPath); !os.IsNotExist(err) {
				t.Fatalf("stable open must not create manifest.json: %v", err)
			}
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
				if !slices.Equal(immutableNames(), priorRevisions) {
					t.Fatalf("refused operation changed manifest namespace: before=%v after=%v", priorRevisions, immutableNames())
				}
				if _, err := os.Stat(compatibilityPath); !os.IsNotExist(err) {
					t.Fatalf("refused operation created manifest.json: %v", err)
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
				base := filepath.Base(event.Path)
				if event.Point == durabilitycut.BeforeDependencyFileSync && event.Resource == durabilitycut.ResourceOuterLeaf &&
					(strings.HasPrefix(base, "manifest.durable.") || strings.HasPrefix(base, "manifest.stage.tmp.")) && blocked.CompareAndSwap(false, true) {
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
				base := filepath.Base(event.Path)
				if event.Point == durabilitycut.BeforeDependencyFileSync && event.Resource == durabilitycut.ResourceOuterLeaf &&
					(strings.HasPrefix(base, "manifest.durable.") || strings.HasPrefix(base, "manifest.stage.tmp.")) && blocked.CompareAndSwap(false, true) {
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
					return errors.Join(writeSerializedRawBatchForManifestTest(batch.(*Batch)), batch.Close())
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

// Enter the actual raw batch fallback without depending on enough optimistic
// conflicts to exhaust its retry budget.
func writeSerializedRawBatchForManifestTest(b *Batch) error {
	release, err := b.db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return err
	}
	defer release()
	revision := b.db.assignBatchEntryRevisions(b.batch)
	intent, err := b.db.prepareRawKVCommandWALIntent(b, true)
	if err != nil {
		return err
	}
	defer releaseUnassignedCommandWALIntent(intent)
	return b.writeSerialized(true, intent, revision, nil)
}

// TestBindPublicationLeafManifestPackedProjection verifies the pack-publication
// branch of bindPublicationLeafManifest directly on every platform (including
// macOS where full stable-namespace promotion is unavailable):
//   - An older GC-retired/deleted pack token still conservatively inherited from
//     visibleBase is projected out when unreachable from the candidate roots.
//   - A still-reachable pack omitted from the staged manifest still fails closed.
func TestBindPublicationLeafManifestPackedProjection(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		reachable bool
	}{
		{name: "unreachable_retired_pack_is_projected", reachable: false},
		{name: "reachable_omitted_pack_refuses_publication", reachable: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{
				Dir:                        dir,
				IndexOuterLeavesInValueLog: true,
				DisableBackgroundPrune:     true,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
			leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
			database.SetLeafPageLog(leafLog)
			registry := database.valueLogIdentityPins
			t.Cleanup(func() {
				if database != nil {
					_ = database.Close()
				}
				_ = leafLog.Close()
				if got := registry.ActivePins(); got != 0 {
					t.Errorf("retained stable physical pins after close: %d", got)
				}
				if err := registry.CleanupError(); err != nil {
					t.Error(err)
				}
			})

			writeLeafGenerationKeys(t, database, "packed-proj", 256, 'a')
			livePath, liveTaggedID := currentLeafSegmentOrFatal(t, leafLog)
			liveRawID := page.ValueLogSegmentID(liveTaggedID)

			// Create an unreachable retired pack file (seq 13 = 2139095053) and a
			// newly promoted pack file (seq 14) on disk.
			retiredPath, retiredTaggedID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 13)
			promotedPath, promotedTaggedID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 14)
			if err := os.WriteFile(retiredPath, []byte("retired-pack-segment"), 0o600); err != nil {
				t.Fatalf("write retired pack: %v", err)
			}
			if err := os.WriteFile(promotedPath, []byte("promoted-pack-segment"), 0o600); err != nil {
				t.Fatalf("write promoted pack: %v", err)
			}
			retiredRawID := page.ValueLogSegmentID(retiredTaggedID)
			promotedRawID := page.ValueLogSegmentID(promotedTaggedID)

			// Build the candidate's StableResourceSet containing the newly promoted
			// pack plus an omitted pack token (either unreachable seq 13, or the
			// live reachable segment for the fail-closed case).
			omittedPath := retiredPath
			omittedTaggedID := retiredTaggedID
			omittedRawID := retiredRawID
			if scenario.reachable {
				omittedPath = livePath
				omittedTaggedID = liveTaggedID
				omittedRawID = liveRawID
			}

			addPackToken := func(builder *rootpublication.StableResourceSetBuilder, path string, taggedID uint32) {
				t.Helper()
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("open %s: %v", path, err)
				}
				defer f.Close()
				gen := uint64(taggedID)
				token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
					Kind:           rootpublication.ResourceOuterLeafPack,
					LogicalLane:    "leaf-generation-pack",
					ResourceID:     strconv.FormatUint(gen, 10),
					Generation:     gen,
					DiagnosticPath: filepath.Base(path),
					File:           f,
					Frontier:       rootpublication.DurableFrontier{Bytes: 1},
					Digest:         sha256.Sum256([]byte(path)),
					Reachability:   rootpublication.ReachabilityOuterLeafPackedPointer,
				})
				if err != nil {
					t.Fatalf("NewStableResourceToken(%d): %v", taggedID, err)
				}
				if err := builder.Add(token); err != nil {
					token.Release()
					t.Fatalf("builder.Add(%d): %v", taggedID, err)
				}
			}

			builder := rootpublication.NewStableResourceSetBuilder()
			addPackToken(builder, omittedPath, omittedTaggedID)
			addPackToken(builder, promotedPath, promotedTaggedID)
			resources, err := builder.Freeze()
			if err != nil {
				builder.Abandon()
				t.Fatalf("builder.Freeze: %v", err)
			}

			state := database.state.Load()
			if state == nil || state.LeafGenerations == nil || state.LeafGenerations.sourceManifest == nil {
				resources.Release()
				t.Fatal("missing source leaf-generation manifest")
			}
			packManifest := state.LeafGenerations.sourceManifest.clone()
			if scenario.reachable {
				// Omit the live segment from the manifest so it is uncovered yet reachable.
				for i := range packManifest.Generations {
					if slices.Contains(packManifest.Generations[i].FileIDs, liveRawID) {
						packManifest.Generations[i].FileIDs = nil
					}
				}
			}

			// Exercise bindPublicationLeafManifest in stable mode with a cut hook
			// right after coverage & pack projection so this runs on all platforms.
			cut := errors.New("reached stable manifest preparation after pack projection")
			origStore := database.leafGenerationManifestStore
			stubStore := &leafGenerationManifestStore{
				leafDir:            LeafLogDirPath(dir),
				registry:           database.valueLogIdentityPins,
				mode:               leafGenerationManifestStable,
				stableCapability:   func() bool { return true },
				durabilityCounters: origStore.durabilityCounters,
				hooks: leafGenerationManifestStoreHooks{
					BeforeTempCreate: func() error { return cut },
				},
			}
			parentDir, err := os.Open(LeafLogDirPath(dir))
			if err != nil {
				resources.Release()
				t.Fatalf("open leaf dir: %v", err)
			}
			stubStore.parent = parentDir
			database.leafGenerationManifestStore = stubStore
			defer func() {
				database.leafGenerationManifestStore = origStore
				_ = parentDir.Close()
			}()

			next := page.MetaPageBody{
				CommitSeq:        state.CommitSeq + 1,
				UserRootPageID:   state.RootPageID,
				SystemRootPageID: state.SystemRootPageID,
			}
			opts := finalizeCommitOptions{
				packedRootPublication: &packedRootPublication{
					afterActivation:          func() {},
					afterManifestPreparation: func() error { return nil },
				},
			}
			_, _, err = database.bindPublicationLeafManifest(resources, database.idx.Load(), next, packManifest, []uint32{promotedRawID}, &opts)
			if scenario.reachable {
				want := fmt.Sprintf("publication leaf-generation manifest omits leaf file %d", omittedRawID)
				if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !strings.Contains(err.Error(), want) {
					t.Fatalf("reachable omitted pack err=%v, want %q", err, want)
				}
			} else if !errors.Is(err, cut) {
				t.Fatalf("unreachable retired pack should have been projected before manifest prep, got err=%v", err)
			}
		})
	}
}

// TestBindPublicationLeafManifestOuterLeafProjection verifies that
// bindPublicationLeafManifest projects unreachable retired outer-leaf files
// across both ResourceOuterLeafLog and ResourceOuterLeafPack (single-kind and
// mixed) while preserving fail-closed refusal for any reachable omitted file
// and leaking zero physical pins on both projection and error paths.
func TestBindPublicationLeafManifestOuterLeafProjection(t *testing.T) {
	for _, scenario := range []struct {
		name               string
		includeLiveRaw     bool
		includeRetiredRaw  bool
		includeRetiredPack bool
		includeLivePack    bool
		omitLiveFromMan    bool
		wantErrUnresolved  bool
	}{
		{
			name:              "unreachable_retired_raw_leaf_log_is_projected",
			includeLiveRaw:    true,
			includeRetiredRaw: true,
		},
		{
			name:              "reachable_omitted_raw_leaf_log_refuses_publication",
			includeLiveRaw:    true,
			omitLiveFromMan:   true,
			wantErrUnresolved: true,
		},
		{
			name:               "mixed_unreachable_raw_and_pack_are_projected",
			includeLiveRaw:     true,
			includeRetiredRaw:  true,
			includeRetiredPack: true,
		},
		{
			name:              "mixed_unreachable_raw_with_reachable_pack_refuses",
			includeRetiredRaw: true,
			includeLivePack:   true,
			omitLiveFromMan:   true,
			wantErrUnresolved: true,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{
				Dir:                        dir,
				IndexOuterLeavesInValueLog: true,
				DisableBackgroundPrune:     true,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
			leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
			database.SetLeafPageLog(leafLog)
			registry := database.valueLogIdentityPins
			t.Cleanup(func() {
				if database != nil {
					_ = database.Close()
				}
				_ = leafLog.Close()
				if got := registry.ActivePins(); got != 0 {
					t.Errorf("retained stable physical pins after close: %d", got)
				}
				if err := registry.CleanupError(); err != nil {
					t.Error(err)
				}
			})

			writeLeafGenerationKeys(t, database, "outer-leaf-proj", 256, 'a')
			livePath, liveTaggedID := currentLeafSegmentOrFatal(t, leafLog)
			liveRawID := page.ValueLogSegmentID(liveTaggedID)

			retiredRawPath, retiredRawTaggedID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 234)
			retiredPackPath, retiredPackTaggedID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 235)
			promotedPath, promotedTaggedID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 236)
			if err := os.WriteFile(retiredRawPath, []byte("retired-raw-leaf-segment"), 0o600); err != nil {
				t.Fatalf("write retired raw leaf: %v", err)
			}
			if err := os.WriteFile(retiredPackPath, []byte("retired-pack-segment"), 0o600); err != nil {
				t.Fatalf("write retired pack: %v", err)
			}
			if err := os.WriteFile(promotedPath, []byte("promoted-pack-segment"), 0o600); err != nil {
				t.Fatalf("write promoted pack: %v", err)
			}
			promotedRawID := page.ValueLogSegmentID(promotedTaggedID)

			addOuterLeafToken := func(builder *rootpublication.StableResourceSetBuilder, kind rootpublication.ResourceKind, path string, taggedID uint32) rootpublication.StableIdentity {
				t.Helper()
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("open %s: %v", path, err)
				}
				defer f.Close()
				logicalLane := "leaf-generation-pack"
				reachability := rootpublication.ReachabilityOuterLeafPackedPointer
				if kind == rootpublication.ResourceOuterLeafLog {
					logicalLane = "db/outer-leaf-raw"
					reachability = rootpublication.ReachabilityOuterLeafRawPointer
				}
				fileIdentity, err := rootpublication.StableIdentityFromFile(f)
				if err != nil {
					t.Fatalf("StableIdentityFromFile(%s): %v", path, err)
				}
				if err := registry.Observe(fileIdentity); err != nil {
					t.Fatalf("registry.Observe(%s): %v", path, err)
				}
				gen := uint64(taggedID)
				token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
					Kind:                kind,
					LogicalLane:         logicalLane,
					ResourceID:          strconv.FormatUint(gen, 10),
					Generation:          gen,
					DiagnosticPath:      filepath.Base(path),
					File:                f,
					Frontier:            rootpublication.DurableFrontier{Bytes: 1},
					Digest:              sha256.Sum256([]byte(path)),
					Reachability:        reachability,
					PinRegistry:         registry,
					OriginalObservation: registry,
				})
				if err != nil {
					t.Fatalf("NewStableResourceToken(%s, %d): %v", kind, taggedID, errors.Join(err, registry.Unobserve(fileIdentity)))
				}
				identity := token.Identity()
				if err := builder.Add(token); err != nil {
					token.Release()
					t.Fatalf("builder.Add(%s, %d): %v", kind, taggedID, err)
				}
				return identity
			}

			var liveRawIdentity, retiredRawIdentity, retiredPackIdentity, promotedPackIdentity rootpublication.StableIdentity
			builder := rootpublication.NewStableResourceSetBuilder()
			if scenario.includeRetiredRaw {
				retiredRawIdentity = addOuterLeafToken(builder, rootpublication.ResourceOuterLeafLog, retiredRawPath, retiredRawTaggedID)
			}
			if scenario.includeLiveRaw {
				liveRawIdentity = addOuterLeafToken(builder, rootpublication.ResourceOuterLeafLog, livePath, liveTaggedID)
			}
			if scenario.includeRetiredPack {
				retiredPackIdentity = addOuterLeafToken(builder, rootpublication.ResourceOuterLeafPack, retiredPackPath, retiredPackTaggedID)
			}
			if scenario.includeLivePack {
				_ = addOuterLeafToken(builder, rootpublication.ResourceOuterLeafPack, livePath, liveTaggedID)
			}
			promotedPackIdentity = addOuterLeafToken(builder, rootpublication.ResourceOuterLeafPack, promotedPath, promotedTaggedID)

			resources, err := builder.Freeze()
			if err != nil {
				builder.Abandon()
				t.Fatalf("builder.Freeze: %v", err)
			}

			state := database.state.Load()
			if state == nil || state.LeafGenerations == nil || state.LeafGenerations.sourceManifest == nil {
				resources.Release()
				t.Fatal("missing source leaf-generation manifest")
			}
			packManifest := state.LeafGenerations.sourceManifest.clone()
			if scenario.omitLiveFromMan {
				for i := range packManifest.Generations {
					if slices.Contains(packManifest.Generations[i].FileIDs, liveRawID) {
						packManifest.Generations[i].FileIDs = nil
					}
				}
			}

			cut := errors.New("reached stable manifest preparation after outer-leaf projection")
			origStore := database.leafGenerationManifestStore
			stubStore := &leafGenerationManifestStore{
				leafDir:            LeafLogDirPath(dir),
				registry:           database.valueLogIdentityPins,
				mode:               leafGenerationManifestStable,
				stableCapability:   func() bool { return true },
				durabilityCounters: origStore.durabilityCounters,
				hooks: leafGenerationManifestStoreHooks{
					BeforeTempCreate: func() error {
						if scenario.includeRetiredRaw {
							if got := registry.PinCount(retiredRawIdentity); got != 0 {
								return fmt.Errorf("projected retired raw leaf still pinned at manifest prep: pins=%d", got)
							}
						}
						if scenario.includeRetiredPack {
							if got := registry.PinCount(retiredPackIdentity); got != 0 {
								return fmt.Errorf("projected retired pack still pinned at manifest prep: pins=%d", got)
							}
						}
						if scenario.includeLiveRaw {
							if got := registry.PinCount(liveRawIdentity); got == 0 {
								return fmt.Errorf("live raw leaf lost pin before manifest prep")
							}
						}
						if got := registry.PinCount(promotedPackIdentity); got != 1 {
							return fmt.Errorf("promoted pack pin count at manifest prep=%d, want 1 (intermediate set leaked?)", got)
						}
						return cut
					},
				},
			}
			parentDir, err := os.Open(LeafLogDirPath(dir))
			if err != nil {
				resources.Release()
				t.Fatalf("open leaf dir: %v", err)
			}
			stubStore.parent = parentDir
			database.leafGenerationManifestStore = stubStore
			defer func() {
				database.leafGenerationManifestStore = origStore
				_ = parentDir.Close()
			}()

			next := page.MetaPageBody{
				CommitSeq:        state.CommitSeq + 1,
				UserRootPageID:   state.RootPageID,
				SystemRootPageID: state.SystemRootPageID,
			}
			opts := finalizeCommitOptions{
				packedRootPublication: &packedRootPublication{
					afterActivation:          func() {},
					afterManifestPreparation: func() error { return nil },
				},
			}
			_, _, err = database.bindPublicationLeafManifest(resources, database.idx.Load(), next, packManifest, []uint32{promotedRawID}, &opts)
			if scenario.wantErrUnresolved {
				want := fmt.Sprintf("publication leaf-generation manifest omits leaf file %d", liveRawID)
				if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !strings.Contains(err.Error(), want) {
					t.Fatalf("reachable omitted outer-leaf err=%v, want %q", err, want)
				}
			} else if !errors.Is(err, cut) {
				t.Fatalf("unreachable retired outer-leaf files should have been projected before manifest prep, got err=%v", err)
			}
		})
	}
}

// TestPackedLeafManifestProjectsGCRetiredPackAcrossConsecutivePacks reproduces
// the end-to-end Linux sequence: Pack #1 -> overwrite keys -> LeafGenerationGC
// retires Pack #1 while visibleBase still holds its ResourceOuterLeafPack token
// -> Pack #2 runs without an intervening rotating ordinary write and must
// project out the unreachable retired pack instead of failing with
// "publication leaf-generation manifest omits leaf file <id>".
func TestPackedLeafManifestProjectsGCRetiredPackAcrossConsecutivePacks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	database, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	registry := database.valueLogIdentityPins
	t.Cleanup(func() {
		if database != nil {
			closeLeafGenerationPackPublicationTestDB(t, database, leafLog)
		}
		if got := registry.ActivePins(); got != 0 {
			t.Errorf("retained stable physical pins after close: %d", got)
		}
		if err := registry.CleanupError(); err != nil {
			t.Error(err)
		}
	})

	const keys = 512
	seed := prepareLeafGenerationPackTestCandidate(t, database, leafLog, keys)
	pack1, err := database.LeafGenerationPack(ctx, LeafGenerationPackOptions{
		GenerationIDs: []uint64{seed.generation.GenerationID},
		Force:         true,
		Sync:          true,
		ReserveRIDs:   leafLog.reserveRecordRIDs,
	})
	if err != nil || len(pack1.CreatedFileIDs) != 1 {
		t.Fatalf("pack1: stats=%+v err=%v", pack1, err)
	}
	pack1RawID := pack1.CreatedFileIDs[0]

	// Rotate onto a fresh raw generation, overwrite all keys across two durable
	// commits so both meta slots stop referencing pack1, then rotate once more
	// to create a sealed raw generation candidate for pack2.
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationKeys(t, database, "pack-concurrent", keys, 'c')
	if err := database.SetSync([]byte("pack-tail-0030"), []byte("advance-slot-1")); err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("pack-tail-0031"), []byte("advance-slot-2")); err != nil {
		t.Fatal(err)
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationKeys(t, database, "pack-concurrent", keys/2, 'd')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("pack-tail-0032"), []byte("seal-pack2-candidate")); err != nil {
		t.Fatal(err)
	}

	// Run GC: pack1 is unreachable from both recoverable meta slots, so GC
	// deletes/prunes pack1 from db.leafGenerationManifest while visibleBase
	// still retains pack1's ResourceOuterLeafPack token.
	gcStats, err := database.LeafGenerationGC(ctx, LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if state := database.state.Load(); state == nil || state.LeafGenerations == nil || leafManifestContainsActiveFile(state.LeafGenerations.sourceManifest, pack1RawID) {
		t.Fatalf("expected GC to retire/prune pack1 raw file %d (gcStats=%+v)", pack1RawID, gcStats)
	}

	// Immediately run Pack #2 without any intervening rotating ordinary write.
	plan, err := database.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	var candidateIDs []uint64
	for _, gen := range plan.Generations {
		if gen.Eligible && !gen.WholeGenerationGCEligible {
			candidateIDs = append(candidateIDs, gen.GenerationID)
		}
	}
	if len(candidateIDs) == 0 {
		t.Fatalf("expected at least one sealed candidate for pack2, plan=%+v", plan)
	}
	pack2, err := database.LeafGenerationPack(ctx, LeafGenerationPackOptions{
		GenerationIDs: candidateIDs,
		Force:         true,
		Sync:          true,
		ReserveRIDs:   leafLog.reserveRecordRIDs,
	})
	if err != nil || len(pack2.CreatedFileIDs) == 0 {
		t.Fatalf("pack2 after GC-retired pack1 failed: stats=%+v err=%v", pack2, err)
	}

	// Reopen and verify all live keys remain readable.
	closeLeafGenerationPackPublicationTestDB(t, database, leafLog)
	database = nil
	reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	val, err := reopened.Get([]byte("pack-tail-0032"))
	if err != nil || string(val) != "seal-pack2-candidate" {
		t.Fatalf("reopened pack-tail-0032=%q err=%v", val, err)
	}
}
