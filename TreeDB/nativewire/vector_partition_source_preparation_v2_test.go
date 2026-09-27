package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func TestPrepareVectorPartitionSourcesV2UsesCompletedDurableOwnerImports(t *testing.T) {
	for _, remoteOwner := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remoteOwner), func(t *testing.T) {
			dir := t.TempDir()
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV2, backenddb.RequiredFeatureDependencyDirectoryV2}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
				t.Fatal(err)
			}
			open := func() *backenddb.DB {
				d, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
				if err != nil {
					t.Fatal(err)
				}
				return d
			}
			d := open()
			defer func() { d.Close() }()
			meta := collections.CollectionMeta{Name: "source", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, DisableBufferedIndexedAsyncFlush: true, ColumnStore: &collections.ColumnStoreConfig{Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, Owner: collections.TypedStorageOwnerColumnPart, VectorDims: 2}}}}, VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 2, Strategy: collections.VectorIndexStrategyColumnGraph}}}
			manager := collections.NewCollectionManager(d)
			if _, err := manager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: meta.Name}
			catalogInput := raftplacement.CatalogV1{Features: raftplacement.DefaultFeatureSet(), Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-b"}, LeaderHint: "node-a"}, {ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"}, {ID: "group-c", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-b"}}}
			catalogInput.Features.Required = append(catalogInput.Features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
			catalog, err := raftplacement.Validate(catalogInput)
			if err != nil {
				t.Fatal(err)
			}
			shards := []raftplacement.SourceShardV2{{ShardID: "source-a", GroupID: "group-a", Start: 0, End: ^uint64(0)}}
			if remoteOwner {
				shards[0].End = ^uint64(0) / 2
				shards = append(shards, raftplacement.SourceShardV2{ShardID: "source-c", GroupID: "group-c", Start: shards[0].End + 1, End: ^uint64(0)})
			}
			rawMap, err := catalog.CanonicalSourceShardMapV2(raftplacement.SourceShardMapV2{Format: raftplacement.SourceShardMapFormatV2, Collection: ref, Epoch: 1, TokenAlgorithm: raftplacement.DocumentIDTokenAlgorithmV2, Shards: shards})
			if err != nil {
				t.Fatal(err)
			}
			ownership, err := catalog.ValidateSourceShardMapV2(rawMap)
			if err != nil {
				t.Fatal(err)
			}
			var mapDigest, definition [sha256.Size]byte
			raw, _ := hex.DecodeString(rawMap.Digest)
			copy(mapDigest[:], raw)
			definitionHex := collections.VectorIndexDefinitionDigestV1(c.Meta().VectorIndexes[0])
			raw, _ = hex.DecodeString(definitionHex)
			copy(definition[:], raw)
			schema, err := collections.VectorPartitionSourceSchemaDigestV2(*c.Meta().Options.ColumnStore)
			if err != nil {
				t.Fatal(err)
			}
			input := collections.VectorPartitionSourceImportChunkV2{Snapshot: collections.VectorPartitionSourceSnapshotV2{Version: 2, CollectionScope: "default/default/source", ShardID: "source-a", SnapshotRevision: 1, OrdinalNamespace: "source-a", SourceMapEpoch: 1, SourceMapDigest: mapDigest, SchemaDigest: schema, IndexDefinitionDigest: definition, Encoding: source.SourceSnapshotEncodingV2, Dimensions: 2, RowCount: 2, RowsPerChunk: 1}, IndexName: "embedding", VectorColumn: "embedding", GroupID: "group-a", DocumentRevisions: []uint64{17}}
			importRow := func(id string) collections.VectorPartitionSourceImportProgressV2 {
				for suffix := 0; ; suffix++ {
					candidate := fmt.Sprintf("%s-%d", id, suffix)
					shard, err := ownership.ResolveDocumentID([]byte(candidate))
					if err != nil {
						t.Fatal(err)
					}
					if shard.GroupID == "group-a" {
						id = candidate
						break
					}
				}
				p, err := c.ImportVectorPartitionSourceChunkV2(ownership.SourceMapV2(), input, [][]byte{[]byte(id)}, [][]byte{[]byte(fmt.Sprintf(`{"id":%q}`, id))}, []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0}}}})
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			first := importRow("first")
			identity := raftplacement.VectorPartitionLifecycleIdentityV1{SourceFormat: 2, Generation: 1, Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: definitionHex}, SourceV2: raftplacement.VectorPartitionLifecycleSourceIdentityV2{SourceMapEpoch: 1, SourceMapDigest: rawMap.Digest, GraphProfileDigest: strings.Repeat("a", 64), PlacementDigest: strings.Repeat("b", 64)}}
			var appliedAuthority *raftplacement.CatalogMetaAuthorityV1
			var combinedIdentity raftplacement.VectorPartitionLifecycleIdentityV1
			selected := first.Snapshot
			duplicate, omit := false, false
			owner := VectorPartitionOwnerSourceInputV2{GroupID: "group-a", Collection: c, Walk: func(ctx context.Context, visit func(collections.VectorPartitionSourceSnapshotV2) error) error {
				if omit {
					return nil
				}
				if err := visit(selected); err != nil {
					return err
				}
				if duplicate {
					return visit(selected)
				}
				return nil
			}}
			prepare := func() ([]raftplacement.VectorPartitionSourceOwnerPreparationV2, error) {
				prepared, err := PrepareVectorPartitionOwnerSourceV2(t.Context(), identity, ownership, owner)
				if err != nil {
					return nil, err
				}
				return []raftplacement.VectorPartitionSourceOwnerPreparationV2{prepared}, nil
			}
			if _, err := prepare(); err == nil {
				t.Fatal("incomplete import admitted")
			}
			input.ChunkIndex = 1
			input.DocumentRevisions = []uint64{23}
			completed := importRow("second")
			selected = completed.Snapshot
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			aggregates, err := prepare()
			if err != nil {
				t.Fatal(err)
			}
			if len(aggregates) != 1 || aggregates[0].ShardCount != 1 {
				t.Fatalf("aggregates=%+v", aggregates)
			}

			if remoteOwner {
				if _, err := PrepareVectorPartitionSourcesV2(t.Context(), identity, ownership, []VectorPartitionOwnerSourceInputV2{owner}); !errors.Is(err, raftplacement.ErrVectorPartitionLifecycleGuard) {
					t.Fatalf("missing remote owner: %v", err)
				}
			} else {
				// Exercise the production derivation wrapper through real catalog Raft.
				// Source group-a and ANN readiness group-b are intentionally distinct.
				harness, err := raftplacement.OpenCatalogMetaLifecycleHarnessV1(t.Context(), raftplacement.CatalogMetaLifecycleHarnessOptionsV1{Catalog: catalogInput, Prefix: "source-preparation"})
				if err != nil {
					t.Fatal(err)
				}
				defer harness.Close()
				status, ok := harness.LeaderAuthority().Status()
				if !ok {
					t.Fatal("catalog status missing")
				}
				identity.Index.CollectionIncarnation = 1
				identity.Index.IndexEpoch = 1
				identity.Index.CatalogEpoch = status.Epoch
				identity.Index.CatalogDigest = status.Digest
				identity.SourceV2.PlacementDigest = ""
				annOwner := VectorPartitionOwnerANNInputV2{GroupID: "group-b", Walk: func(ctx context.Context, visit func(source.ANNRecordV2) error) error {
					if err := visit(source.ANNRecordV2{Domain: &source.ANNDomainV2{DomainID: 0, LogicalPackID: "pack-0", MembershipCount: 1}}); err != nil {
						return err
					}
					return visit(source.ANNRecordV2{Member: &source.ANNMemberV2{Kind: "home", Source: source.ANNSourceRowIdentityV2{SourceOwner: "group-a", ShardID: selected.ShardID, SnapshotRevision: selected.SnapshotRevision, SnapshotDigest: selected.Digest, Ordinal: 1, DocumentRevision: 23}}})
				}}
				record, err := BeginPreparedVectorPartitionBuildV2(t.Context(), harness.LifecycleCoordinator(), identity, ownership, []VectorPartitionOwnerSourceInputV2{owner}, []VectorPartitionOwnerANNInputV2{annOwner}, []raftcluster.GroupID{"group-b"}, 0, 1)
				if err != nil {
					t.Fatal(err)
				}
				if len(record.SourceOwners) != 1 || record.SourceOwners[0].GroupID != "group-a" || len(record.RequiredGroups) != 1 || record.RequiredGroups[0] != "group-b" {
					t.Fatalf("conflated source/ANN owners: %+v", record)
				}
				identity = record.Identity
				appliedAuthority = harness.LeaderAuthority()
				staged, err := BuildAndStagePreparedVectorPartitionSourceV2(t.Context(), appliedAuthority, identity, "node-a", ownership, []VectorPartitionOwnerSourceInputV2{owner})
				if err != nil {
					t.Fatal(err)
				}
				if staged.State != "building" || staged.PagedRootV2.LocalSourceShardCount != 1 || len(staged.PagedRootV2.ANNOwners) != 0 {
					t.Fatalf("source-only stage=%+v", staged)
				}
				if _, err := BuildAndStagePreparedVectorPartitionSourceV2(t.Context(), appliedAuthority, identity, "node-b", ownership, []VectorPartitionOwnerSourceInputV2{owner}); err == nil {
					t.Fatal("ANN-only node staged partial source projection")
				}

				combinedIdentity = identity
				combinedIdentity.Generation++
				combinedIdentity.SourceV2.GraphProfileDigest = collections.VectorPartitionGraphProfileDigestV2()
				combinedRecord, err := BeginPreparedVectorPartitionBuildV2(t.Context(), harness.LifecycleCoordinator(), combinedIdentity, ownership, []VectorPartitionOwnerSourceInputV2{owner}, []VectorPartitionOwnerANNInputV2{annOwner}, []raftcluster.GroupID{"group-b"}, 0, 1)
				if err != nil {
					t.Fatal(err)
				}
				combinedIdentity = combinedRecord.Identity
				combined, err := BuildAndStagePreparedVectorPartitionV2(t.Context(), appliedAuthority, combinedIdentity, "node-b", ownership, []VectorPartitionOwnerSourceInputV2{owner}, []VectorPartitionOwnerANNInputV2{annOwner})
				if err != nil {
					t.Fatal(err)
				}
				if combined.PagedRootV2.LocalDomainCount != 1 || combined.PagedRootV2.LocalMembershipCount != 1 || len(combined.PagedRootV2.SourceOwners) != 1 || len(combined.PagedRootV2.ANNOwners) != 1 {
					t.Fatalf("combined projection: %+v", combined.PagedRootV2)
				}

			}
			root, err := raftplacement.VectorPartitionSourceOwnerSetDigestV2(aggregates)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = open()
			c, err = collections.NewCollectionManager(d).OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			owner.Collection = c
			if appliedAuthority != nil {
				session, err := OpenPreparedVectorPartitionSourceV2(t.Context(), appliedAuthority, identity, "node-a", ownership, c)
				if err != nil {
					t.Fatal(err)
				}
				row, readErr := session.ReadSourceRowV2(t.Context(), collections.VectorPartitionSourceRowIdentityV2{SourceOwner: "group-a", ShardID: selected.ShardID, SnapshotRevision: selected.SnapshotRevision, SnapshotDigest: selected.Digest, Ordinal: 1, DocumentRevision: 23})
				closeErr := session.Close()
				if readErr != nil || closeErr != nil || !strings.HasPrefix(string(row.DocumentID), "second-") {
					t.Fatalf("prepared reopen row=%+v read=%v close=%v", row, readErr, closeErr)
				}
			}
			if combinedIdentity.Generation != 0 {
				session, err := OpenPreparedVectorPartitionSourceV2(t.Context(), appliedAuthority, combinedIdentity, "node-b", ownership, c)
				if err != nil {
					t.Fatal(err)
				}
				domain, err := session.OpenDomainV2(t.Context(), "group-b", 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
				results, err := domain.SearchLocalV2(t.Context(), []float32{1, 0}, 1, 16)
				closeErr := domain.Close()
				if err != nil || closeErr != nil || len(results) != 1 || results[0].Source.DocumentRevision != 23 || !strings.HasPrefix(string(results[0].DocumentID), "second-") {
					t.Fatalf("committed build/reopen query: %+v %v %v", results, err, closeErr)
				}
			}
			reopened, err := prepare()
			if err != nil {
				t.Fatal(err)
			}
			reopenedRoot, err := raftplacement.VectorPartitionSourceOwnerSetDigestV2(reopened)
			if err != nil || reopenedRoot != root {
				t.Fatalf("reopen root=%q err=%v", reopenedRoot, err)
			}
			// A second replica imports another revision first, so its physical
			// directory generation differs while the selected source is identical.
			replicaDir := t.TempDir()
			if err := backenddb.SaveFormatConfig(replicaDir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV2, backenddb.RequiredFeatureDependencyDirectoryV2}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
				t.Fatal(err)
			}
			replica, err := backenddb.Open(backenddb.Options{Dir: replicaDir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			replicaManager := collections.NewCollectionManager(replica)
			if _, err := replicaManager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
			originalCollection, originalInput := c, input
			c, err = replicaManager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			for _, revision := range []uint64{2, 1} {
				input.Snapshot.SnapshotRevision = revision
				input.ChunkIndex = 0
				input.DocumentRevisions = []uint64{17}
				prefix := ""
				if revision == 2 {
					prefix = "prior-"
				}
				importRow(prefix + "first")
				input.ChunkIndex = 1
				input.DocumentRevisions = []uint64{23}
				actual := importRow(prefix + "second")
				if revision == 1 && actual.Snapshot != completed.Snapshot {
					t.Fatal("replica semantic snapshot differs")
				}
			}
			if err := replica.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			owner.Collection = c
			replicaOwners, err := prepare()
			if err != nil {
				t.Fatal(err)
			}
			replicaRoot, err := raftplacement.VectorPartitionSourceOwnerSetDigestV2(replicaOwners)
			if err != nil || replicaRoot != root || replicaOwners[0].CompletionEvidenceDigest == aggregates[0].CompletionEvidenceDigest {
				t.Fatalf("physical replica evidence affected semantic root: root=%s owners=%+v err=%v", replicaRoot, replicaOwners, err)
			}
			c, input = originalCollection, originalInput
			owner.Collection = c
			duplicate = true
			if _, err := prepare(); err == nil {
				t.Fatal("duplicate admitted")
			}
			duplicate = false
			omit = true
			if _, err := prepare(); err == nil {
				t.Fatal("omission admitted")
			}
			omit = false
			owner.GroupID = "foreign"
			if _, err := prepare(); err == nil {
				t.Fatal("foreign owner admitted")
			}
			owner.GroupID = "group-a"
			selected.SnapshotRevision++
			if _, err := prepare(); err == nil {
				t.Fatal("forged descriptor admitted")
			}
			selected = completed.Snapshot
			identity.Source.Generation = 1
			if _, err := prepare(); err == nil {
				t.Fatal("mixed V1/V2 admitted")
			}
			identity.Source.Generation = 0
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := PrepareVectorPartitionSourcesV2(canceled, identity, ownership, []VectorPartitionOwnerSourceInputV2{owner}); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled: %v", err)
			}

		})
	}
}
