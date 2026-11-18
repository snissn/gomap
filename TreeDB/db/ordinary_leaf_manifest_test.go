package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

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
			if route == "inline" {
				retrying.Store(true)
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if retrying.Load() && event.Point == durabilitycut.BeforeMetaWrite && event.Resource == durabilitycut.ResourceMeta && retryCuts.Add(1) == 1 {
						return cut
					}
					return nil
				})
				defer restore()
			}
			previousSyncs := syncTotals()
			if err := database.SetSync(key, []byte("rotated")); err != nil {
				t.Fatal(err)
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
