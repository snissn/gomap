package db

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// BenchmarkDependencyDirectoryV2OwnedDeltaPublication is a lower DB publisher
// diagnostic, not a collection/Dgraph benchmark. Run with -benchtime=32x. Loop
// keeps the history/file setup and the old-root/final-drain checks untimed and
// invokes each history only once (no b.N calibration/setup repetition).
func BenchmarkDependencyDirectoryV2OwnedDeltaPublication(b *testing.B) {
	for _, history := range []int{128, 1024, 8192} {
		b.Run(fmt.Sprintf("history_%d", history), func(b *testing.B) {
			opts := Options{Dir: b.TempDir(), Durability: DurabilityWALOffRelaxed, ResolvedProfile: ProfileNoWALFast, DisableBackgroundPrune: true}
			cfg := formatConfigFromOptions(opts)
			cfg.RequiredFeatures = appendRequiredFormatFeature(cfg.RequiredFeatures, RequiredFeatureDependencyDirectoryV2)
			if err := SaveFormatConfig(opts.Dir, cfg); err != nil {
				b.Fatal(err)
			}
			database, err := Open(opts)
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if database != nil {
					if err := database.Close(); err != nil {
						b.Error(err)
					}
				}
			}()
			idx, registry := database.idx.Load(), database.StableResourceIdentityPinRegistry()
			owner := registry.MetadataOwner()
			if idx == nil || idx.primary != nil || idx.metadata != owner || !database.dependencyDirectoryRequiredFeature || database.durableRoot.record.Directory.RootPageID == 0 {
				b.Fatal("not the enabled V2/default DATA original-owner route")
			}
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				b.Fatal(err)
			}
			enrollment, err := owner.Enroll(budget)
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				// Keep original admission available through actual DB cleanup.
				if database != nil {
					if err := database.Close(); err != nil {
						b.Error(err)
					}
					database = nil
				}
				enrollment.Close()
				budget.Close()
			}()
			file, err := os.CreateTemp(b.TempDir(), "owned-column-asset")
			if err != nil {
				b.Fatal(err)
			}
			defer file.Close()
			// One prewritten/synced physical identity, range and frontier is shared
			// by every producer in every history cell. No timed file replacement.
			const fileBytes = 128 << 10
			if _, err = file.Write(make([]byte, fileBytes)); err != nil {
				b.Fatal(err)
			}
			if err = file.Sync(); err != nil {
				b.Fatal(err)
			}
			identity, err := rootpublication.StableIdentityFromFile(file)
			if err != nil {
				b.Fatal(err)
			}
			digest := sha256.Sum256(make([]byte, 8))
			obligation := func(id int) rootpublication.StableLogicalObligation {
				return rootpublication.StableLogicalObligation{Class: "asset", Kind: "part", Namespace: "directory-cost", Digest: digest, Generation: 1, PartID: uint64(id), FileID: 1, Offset: int64(id-1) * 8, Length: 8, Reachability: rootpublication.ReachabilityColumnManifest}
			}
			produce := func(values []rootpublication.StableLogicalObligation) (*rootpublication.StableResourceSet, error) {
				if err := registry.Observe(identity); err != nil {
					return nil, err
				}
				token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{MetadataOwner: owner, PinRegistry: registry, OriginalObservation: registry, Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "1", Generation: 1, DiagnosticPath: "columns/1", File: file, Frontier: rootpublication.DurableFrontier{Bytes: fileBytes}, Reachability: rootpublication.ReachabilityColumnManifest, LogicalObligations: values, ContentSynced: true})
				if err != nil {
					return nil, errors.Join(err, registry.Unobserve(identity))
				}
				builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(owner)
				if err != nil {
					return nil, errors.Join(err, token.ReleaseWithError())
				}
				if err = builder.Add(token); err != nil {
					return nil, errors.Join(err, token.ReleaseWithError(), builder.AbandonWithError())
				}
				set, err := builder.Freeze()
				if err != nil {
					return nil, errors.Join(err, builder.AbandonWithError())
				}
				builder, token = nil, nil
				return set, nil
			}
			publish := func(resources *rootpublication.StableResourceSet) error {
				database.durablePublishMu.Lock()
				defer database.durablePublishMu.Unlock()
				database.rootReuseMu.Lock()
				defer database.rootReuseMu.Unlock()
				next := database.meta
				next.CommitSeq = database.durableRoot.record.CommitSeq + 1
				// prepare consumes resources on both outcomes; execute keeps its
				// original failed candidate in the existing DB failure custody.
				candidate, err := database.prepareDurableRootCandidateV1(idx, next, nil, resources, false)
				if err != nil {
					return err
				}
				published, err := database.executeDurableRootCandidateV1(candidate)
				if err != nil {
					return err
				}
				database.meta = published
				return nil
			}
			initialValues := make([]rootpublication.StableLogicalObligation, history)
			for i := range initialValues {
				initialValues[i] = obligation(i + 1)
			}
			initial, err := produce(initialValues)
			if err != nil {
				b.Fatal(err)
			}
			if err = publish(initial); err != nil {
				b.Fatal(err)
			}
			initial, initialValues = nil, nil
			old, err := rootpublication.CloneStableResourceSetExcludingKinds(database.durableRoot.slotResources[database.durableRoot.slot])
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if old != nil {
					if err := old.ReleaseWithError(); err != nil {
						b.Error(err)
					}
				}
			}()
			oldDirectory, err := old.DependencyDirectoryV2()
			if err != nil || oldDirectory == nil || oldDirectory.Reference().LogicalCount != uint64(history) {
				b.Fatalf("initial owned directory: %v", err)
			}
			beforeOwner := owner.Bytes()
			beforeRecords := database.durableRootDirectoryRecordsEncoded.Load()
			beforeBytes := database.durableRootDirectoryBytesEncoded.Load()
			beforePages := database.durableRootDirectoryPagesWritten.Load()
			appendOne := func(value rootpublication.StableLogicalObligation) (retErr error) {
				var inherited, producer *rootpublication.StableResourceSet
				var builder *rootpublication.StableResourceSetBuilder
				defer func() {
					if inherited != nil {
						retErr = errors.Join(retErr, inherited.ReleaseWithError())
					}
					if producer != nil {
						retErr = errors.Join(retErr, producer.ReleaseWithError())
					}
					if builder != nil {
						retErr = errors.Join(retErr, builder.AbandonWithError())
					}
				}()
				inherited, err = rootpublication.CloneStableResourceSetExcludingKinds(database.durableRoot.slotResources[database.durableRoot.slot])
				if err != nil {
					return err
				}
				values := []rootpublication.StableLogicalObligation{value}
				producer, err = produce(values)
				if err != nil {
					return err
				}
				builder, err = rootpublication.NewStableResourceSetBuilderWithMetadata(owner)
				if err != nil {
					return err
				}
				if err = builder.Merge(inherited); err != nil {
					return err
				}
				inherited = nil
				work, err := builder.MergeAppendOnlyLogicalObligations(producer, rootpublication.StableLogicalObligationMutation{ScopedFields: []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest}, Added: values})
				if err != nil {
					return err
				}
				producer = nil
				if work.AppendOnlyFastPath != 1 || work.AppendOnlyCollisionFallbacks != 0 {
					return fmt.Errorf("selected original-owner append did not activate: %+v", work)
				}
				resources, err := builder.Freeze()
				if err != nil {
					return err
				}
				builder = nil
				return publish(resources)
			}
			b.ReportAllocs()
			ops := 0
			for b.Loop() {
				if ops >= 32 {
					b.Fatal("requires the declared -benchtime=32x; no extra operations authorized")
				}
				if err := appendOne(obligation(history + ops + 1)); err != nil {
					b.Fatal(err)
				}
				ops++
			}
			if ops != 32 || b.N != 32 {
				b.Fatalf("requires exactly 32 operations: ops=%d N=%d", ops, b.N)
			}
			b.ReportMetric(float64(database.durableRootDirectoryRecordsEncoded.Load()-beforeRecords)/32, "directory-records/op")
			b.ReportMetric(float64(database.durableRootDirectoryBytesEncoded.Load()-beforeBytes)/32, "directory-encoded-B/op")
			b.ReportMetric(float64(database.durableRootDirectoryPagesWritten.Load()-beforePages)/32, "directory-pages/op")
			heldOwner := owner.Bytes()
			if oldDirectory.Reference().LogicalCount != uint64(history) {
				b.Fatal("held original root changed")
			}
			if _, got, found, err := oldDirectory.LookupLogical(obligation(1)); err != nil || !found || got != obligation(1) {
				b.Fatalf("held original key changed: found=%t err=%v", found, err)
			}
			if _, _, found, err := oldDirectory.LookupLogical(obligation(history + 1)); err != nil || found {
				b.Fatalf("held root saw newer member: found=%t err=%v", found, err)
			}
			for slot, set := range database.durableRoot.slotResources {
				directory, err := set.DependencyDirectoryV2()
				want := uint64(history + 31)
				if slot == int(database.durableRoot.slot) {
					want++
				}
				if err != nil || directory == nil || set.MetadataOwner() != owner || directory.Reference() != database.durableRoot.slotRecord[slot].Directory || directory.Reference().PhysicalCount != 1 || directory.Reference().LogicalCount != want {
					b.Fatalf("actual V2 slot %d has wrong original directory/counts: err=%v", slot, err)
				}
			}
			oldDirectory = nil
			if err := old.ReleaseWithError(); err != nil {
				b.Fatal(err)
			}
			old = nil
			releasedOwner := owner.Bytes()
			if err := database.Close(); err != nil {
				b.Fatal(err)
			}
			database, idx = nil, nil
			enrollment.Close()
			budget.Close()
			closedOwner := owner.Bytes()
			if closedOwner != 0 || budget.Stats().TotalBytes != 0 || registry.ActivePins() != 0 {
				b.Fatalf("final original custody retained: owner=%d budget=%+v pins=%d", closedOwner, budget.Stats(), registry.ActivePins())
			}
			b.Logf("history=%d operations=%d physical=1 synced_frontier=%d owner_B_before=%d owner_B_held=%d owner_B_released=%d owner_B_closed=%d", history, ops, fileBytes, beforeOwner, heldOwner, releasedOwner, closedOwner)
		})
	}
}
