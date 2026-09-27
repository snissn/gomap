package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func TestVectorPartitionPagedSourceSessionV2VerifiesCompletedOwner(t *testing.T) {
	_, d, c := openSourceImportDirectoryCollectionV2(t)
	defer d.Close()
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
	digest, err := source.SourceOwnerSetDigestV2(owners)
	if err != nil {
		t.Fatal(err)
	}
	def := VectorIndexDefinitionDigestV1(c.Meta().VectorIndexes[0])
	prepared := VectorPartitionPreparedInputV2{LocalSourceOwners: []string{"group-a"}, Generation: 3, Collection: c.name, IndexName: input.IndexName, IndexDefinitionDigest: def, SourceMapEpoch: ownership.Epoch(), SourceMapDigest: ownership.Digest(), SnapshotSetDigest: digest, GraphProfileDigest: strings.Repeat("a", 64), PlacementDigest: strings.Repeat("b", 64), Owners: owners}
	lease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	n := uint64(0)
	emit := func(p VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error) {
		raw, err := EncodeVectorPartitionDirectoryPageV2(p)
		if err != nil {
			return VectorPartitionAssetV1{}, err
		}
		n++
		refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(d.ColumnAssetRootDir(), *c.Meta().Options.ColumnStore, 9981, []StableColumnPhysicalAssetAppend{{Payload: raw, Kind: ColumnAssetKindTCS1HNSWSearchPack, Generation: 3, PartID: n}}, d.StableResourceIdentityPinRegistry(), lease)
		if err != nil {
			return VectorPartitionAssetV1{}, err
		}
		defer resources.Release()
		if err := resources.SyncThrough(); err != nil {
			return VectorPartitionAssetV1{}, err
		}
		sum := sha256.Sum256(raw)
		return VectorPartitionAssetV1{ID: fmt.Sprintf("page-%d", n), Ref: refs[0], Bytes: uint64(len(raw)), Checksum: hex.EncodeToString(sum[:])}, nil
	}
	root, err := writeVectorPartitionDirectoryV2(t.Context(), "source", 3, func(visit func(VectorPartitionDirectoryRecordV2) error) error {
		return visit(VectorPartitionDirectoryRecordV2{Owner: "group-a", Snapshot: &progress.Snapshot})
	}, emit)
	if err != nil {
		t.Fatal(err)
	}
	root.ID = "vector_partition_source_root_v2"
	m := VectorPartitionManifestV1{Format: VectorPartitionManifestFormatV2, State: "building", Collection: c.name, IndexName: input.IndexName, IndexDefinitionDigest: def, Generation: 3, PagedRootV2: &VectorPartitionPagedRootV2{SourceMapEpoch: ownership.Epoch(), SourceMapDigest: ownership.Digest(), SourceSnapshotSetDigest: digest, GraphProfileDigest: prepared.GraphProfileDigest, PlacementDigest: prepared.PlacementDigest, SourceOwners: []string{"group-a"}, LocalSourceShardCount: 1, LocalSourceRowCount: 2, SourceShardDirectory: root}}
	m.Canonicalize()
	session := &VectorPartitionPagedSourceSessionV2{collection: c, manifest: m, ownership: ownership, snapshot: d.AcquireSnapshot()}
	defer session.Close()
	if err := session.verifyPreparedSourcesV2(t.Context(), prepared); err != nil {
		t.Fatal(err)
	}
	identity := VectorPartitionSourceRowIdentityV2{SourceOwner: "group-a", ShardID: progress.Snapshot.ShardID, SnapshotRevision: progress.Snapshot.SnapshotRevision, SnapshotDigest: progress.Snapshot.Digest, Ordinal: 1, DocumentRevision: 37}
	for i := 0; i < 3; i++ {
		row, err := session.ReadSourceRowV2(t.Context(), identity)
		if err != nil || string(row.DocumentID) != "b" {
			t.Fatalf("selected row=%+v err=%v", row, err)
		}
	}
	identity.DocumentRevision++
	if _, err := session.ReadSourceRowV2(t.Context(), identity); err == nil {
		t.Fatal("wrong document revision admitted")
	}
	changed := prepared
	changed.Owners = append([]source.SourceOwnerCommitmentV2(nil), owners...)
	changed.Owners[0].ShardCount++
	changed.SnapshotSetDigest, _ = source.SourceOwnerSetDigestV2(changed.Owners)
	if err := session.verifyPreparedSourcesV2(t.Context(), changed); err == nil {
		t.Fatal("forged owner count admitted")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ReadSourceRowV2(t.Context(), identity); err == nil {
		t.Fatal("released session admitted")
	}
	identity.DocumentRevision--
	staged, err := c.BuildAndStageVectorPartitionSourceProjectionV2(t.Context(), prepared, ownership, func(_ context.Context, visit func(string, VectorPartitionSourceSnapshotV2) error) error {
		return visit("group-a", progress.Snapshot)
	})
	if err != nil {
		t.Fatal(err)
	}
	if staged.State != "building" || staged.PagedRootV2.LocalSourceShardCount != 1 {
		t.Fatalf("staged=%+v", staged)
	}
	retry, err := c.BuildAndStageVectorPartitionSourceProjectionV2(t.Context(), prepared, ownership, func(context.Context, func(string, VectorPartitionSourceSnapshotV2) error) error {
		t.Fatal("retry reopened producer")
		return nil
	})
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(staged, retry) {
		t.Fatalf("exact retry: %v", err)
	}
	opened, err := c.OpenVectorPartitionPagedSourceSessionV2(t.Context(), prepared, ownership)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if row, err := opened.ReadSourceRowV2(t.Context(), identity); err != nil || string(row.DocumentID) != "b" {
		t.Fatalf("staged reopen=%+v err=%v", row, err)
	}
}
