package collections

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func TestVectorPartitionPagedGraphV2BuildStageReopen(t *testing.T) {
	dir, d, c := openSourceImportDirectoryCollectionV2(t)
	defer func() { _ = d.Close() }()
	ownership, input := sourceImportFixtureV2(t, c, 2)
	input.DocumentRevisions = []uint64{91, 37}
	ids, retained, columns := sourceImportRowsV2("a", "b")
	progress, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	semantic := source.NewOwnerSnapshotSetHashV2("group-a")
	source.WriteOwnerSnapshotIdentityV2(semantic, progress.Snapshot)
	owners := []source.SourceOwnerCommitmentV2{{GroupID: "group-a", ShardCount: 1, SnapshotSetDigest: hex.EncodeToString(semantic.Sum(nil))}}
	snapshots, err := source.SourceOwnerSetDigestV2(owners)
	if err != nil {
		t.Fatal(err)
	}
	domain := source.ANNDomainV2{DomainID: 7, LogicalPackID: "logical-seven", MembershipCount: 2}
	members := []source.ANNMemberV2{
		{Kind: "home", Source: source.ANNSourceRowIdentityV2{SourceOwner: "group-a", ShardID: progress.Snapshot.ShardID, SnapshotRevision: progress.Snapshot.SnapshotRevision, SnapshotDigest: progress.Snapshot.Digest, Ordinal: 0, DocumentRevision: 91}},
		{Kind: "overlap", Source: source.ANNSourceRowIdentityV2{SourceOwner: "group-a", ShardID: progress.Snapshot.ShardID, SnapshotRevision: progress.Snapshot.SnapshotRevision, SnapshotDigest: progress.Snapshot.Digest, Ordinal: 1, DocumentRevision: 37}},
	}
	acc, err := source.NewANNOwnerAccumulatorV2("group-b")
	if err != nil {
		t.Fatal(err)
	}
	if err := acc.BeginDomain(domain); err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if err := acc.AddMember(m); err != nil {
			t.Fatal(err)
		}
	}
	commitment, err := acc.Commitment()
	if err != nil {
		t.Fatal(err)
	}
	ann := []source.ANNOwnerCommitmentV2{commitment}
	placement, err := source.ANNOwnerSetDigestV2(ann)
	if err != nil {
		t.Fatal(err)
	}
	prepared := VectorPartitionPreparedInputV2{Generation: 3, Collection: c.name, IndexName: input.IndexName, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(c.Meta().VectorIndexes[0]), SourceMapEpoch: ownership.Epoch(), SourceMapDigest: ownership.Digest(), SnapshotSetDigest: snapshots, GraphProfileDigest: VectorPartitionGraphProfileDigestV2(), PlacementDigest: placement, Owners: owners, ANNOwners: ann, LocalSourceOwners: []string{"group-a"}, LocalANNOwners: []string{"group-b"}}
	sources := func(_ context.Context, visit func(string, VectorPartitionSourceSnapshotV2) error) error {
		return visit("group-a", progress.Snapshot)
	}
	calls := 0
	intent := func(_ context.Context, visit func(string, source.ANNRecordV2) error) error {
		calls++
		if err := visit("group-b", source.ANNRecordV2{Domain: &domain}); err != nil {
			return err
		}
		for _, m := range members {
			m := m
			if err := visit("group-b", source.ANNRecordV2{Member: &m}); err != nil {
				return err
			}
		}
		return nil
	}
	// Forged or unavailable source input cannot install any local generation.
	bad := prepared
	bad.Generation = 2
	wrong := members[1]
	wrong.Source.DocumentRevision++
	_, err = c.BuildAndStageVectorPartitionProjectionV2(t.Context(), bad, ownership, sources, func(_ context.Context, visit func(string, source.ANNRecordV2) error) error {
		if err := visit("group-b", source.ANNRecordV2{Domain: &domain}); err != nil {
			return err
		}
		if err := visit("group-b", source.ANNRecordV2{Member: &members[0]}); err != nil {
			return err
		}
		return visit("group-b", source.ANNRecordV2{Member: &wrong})
	})
	if err == nil {
		t.Fatal("wrong source revision staged")
	}
	store, err := OpenExistingVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenWithContext(t.Context(), c.name, input.IndexName, 2); err == nil {
		t.Fatal("failed intent installed a generation")
	}
	staged, err := c.BuildAndStageVectorPartitionProjectionV2(t.Context(), prepared, ownership, sources, intent)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || staged.PagedRootV2.LocalDomainCount != 1 || staged.PagedRootV2.LocalMembershipCount != 2 || staged.PagedRootV2.LocalPhysicalPackCount != 7 {
		t.Fatalf("calls=%d root=%+v", calls, staged.PagedRootV2)
	}
	retry, err := c.BuildAndStageVectorPartitionProjectionV2(t.Context(), prepared, ownership, sources, func(context.Context, func(string, source.ANNRecordV2) error) error {
		return errors.New("retry consumed mutable intent")
	})
	if err != nil || retry.IntegrityDigest != staged.IntegrityDigest {
		t.Fatalf("retry: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openTypedMinimaDB(t, dir)
	c, err = NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		session, err := c.OpenVectorPartitionPagedSourceSessionV2(t.Context(), prepared, ownership)
		if err != nil {
			t.Fatal(err)
		}
		if len(session.verifiedANNOwners) != 1 || session.verifiedANNOwners[0] != "group-b" {
			t.Fatal("unverified owner")
		}
		if _, err := session.ReadSourceRowV2(t.Context(), members[1].Source); err != nil {
			t.Fatal(err)
		}
		graph, err := session.OpenDomainV2(t.Context(), "group-b", 7)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.OpenDomainV2(t.Context(), "group-a", 7); err == nil {
			t.Fatal("source owner admitted as ANN owner")
		}
		if _, err := session.OpenDomainV2(t.Context(), "group-b", 8); err == nil {
			t.Fatal("missing domain opened")
		}
		row, err := session.ReadSourceRowV2(t.Context(), members[0].Source)
		if err != nil {
			t.Fatal(err)
		}
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
		// The domain owns an independent generation pin and provenance after close.
		for query := 0; query < 2; query++ {
			results, err := graph.SearchLocalV2(t.Context(), row.Values, 2, 16)
			if err != nil || len(results) != 2 {
				t.Fatalf("local query: results=%v err=%v", results, err)
			}
			for _, result := range results {
				expected := members[result.Source.Ordinal]
				if result.Source != expected.Source || result.MembershipKind != expected.Kind || string(result.DocumentID) != []string{"a", "b"}[result.Source.Ordinal] {
					t.Fatalf("provenance: %+v", result)
				}
			}
		}
		if err := graph.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := graph.SearchLocalV2(t.Context(), row.Values, 1, 16); err == nil {
			t.Fatal("closed graph searched")
		}
	}
	if err := walkVectorPartitionManifestAssetsV2(t.Context(), d.ColumnAssetRootDir(), c.Meta().Options.ColumnStore.AssetManager.Namespace, staged, nil); err != nil {
		t.Fatal(err)
	}
}
