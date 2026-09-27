package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
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
	prepared.ANNOwners = []source.ANNOwnerCommitmentV2{{GroupID: "group-b", DomainCount: 1, MembershipCount: 1, MembershipDigest: strings.Repeat("c", 64)}}
	prepared.PlacementDigest, err = source.ANNOwnerSetDigestV2(prepared.ANNOwners)
	if err != nil {
		t.Fatal(err)
	}
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
	refs, _, err := c.vectorPartitionReachabilityRefsV1(nil)
	if err != nil || len(refs) != 1 || refs[0] != staged.PagedRootV2.SourceShardDirectory.Ref {
		t.Fatalf("complete source-only GC closure=%+v err=%v", refs, err)
	}
	if err := ValidateVectorPartitionSnapshotNamespaceV1(d.Dir()); err != nil {
		t.Fatalf("source-only snapshot closure: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	ref := staged.PagedRootV2.SourceShardDirectory.Ref
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0}, ref.Offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if broken, err := c.OpenVectorPartitionPagedSourceSessionV2(t.Context(), prepared, ownership); err == nil {
		_ = broken.Close()
		t.Fatal("corrupt source page exposed a prepared session")
	}
	if _, _, err := c.vectorPartitionReachabilityRefsV1(nil); err == nil {
		t.Fatal("corrupt source page admitted by GC closure")
	}
	if err := ValidateVectorPartitionSnapshotNamespaceV1(d.Dir()); err == nil {
		t.Fatal("corrupt source page admitted by snapshot closure")
	}
	if pins := vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, prepared.Generation); pins != 0 {
		t.Fatalf("failed open leaked generation pins: %d", pins)
	}
}
