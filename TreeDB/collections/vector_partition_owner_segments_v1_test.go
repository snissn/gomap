package collections

import (
	"errors"
	"os"
	"testing"
)

func TestVectorPartitionOwnerSeparatedDomainSegmentsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, definition := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, 0, 0}},
		{id: "b", vector: []float32{0, 1, 0}},
		{id: "c", vector: []float32{0, 0, 1}},
	})
	defer database.Close()
	if _, err := collection.RebuildVectorIndex(definition.Name); err != nil {
		t.Fatal(err)
	}
	source, err := collection.VectorPartitionSourceIdentityV1(definition.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := testVectorPartitionManifestV1()
	manifest.State, manifest.RouterGeneration, manifest.RouterAsset, manifest.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	manifest.Collection, manifest.IndexName, manifest.IndexDefinitionDigest = collection.name, definition.Name, VectorIndexDefinitionDigestV1(definition)
	manifest.Generation = 612
	manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, manifest.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	manifest.PartitionCount, manifest.DomainCount = 3, 2
	manifest.DomainPacks = []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1}, {DomainID: 1, PackID: 2}}
	manifest.Placements = []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "owner-b"}, {PartitionID: 1, GroupID: "owner-b"}, {PartitionID: 2, GroupID: "owner-c"}}
	manifest.Memberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 1}, {VectorOrdinal: 2, PartitionID: 2}}
	manifest.OverlapMemberships = nil
	manifest.Canonicalize()
	inputs := make([]VectorPartitionSearchAssetV1, 3)
	for partition := range inputs {
		inputs[partition] = VectorPartitionSearchAssetV1{Source: source, Generation: manifest.Generation, PartitionID: uint32(partition), Dimensions: definition.Dimensions}
	}
	for name, segments := range map[string]map[string]uint32{
		"missing owner":  {"owner-b": 1961},
		"shared segment": {"owner-b": 1961, "owner-c": 1961},
		"unused owner":   {"owner-b": 1961, "owner-c": 1962, "other": 1963},
	} {
		if _, resources, err := collection.MaterializeVectorPartitionOwnerSeparatedLocalSearchAssetsV1(definition.Name, manifest, segments, inputs); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
			if resources != nil {
				resources.Release()
			}
			t.Fatalf("%s: err=%v", name, err)
		}
	}
	assets, resources, err := collection.MaterializeVectorPartitionOwnerSeparatedLocalSearchAssetsV1(definition.Name, manifest, map[string]uint32{"owner-b": 1961, "owner-c": 1962}, inputs)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	chunks := 0
	for _, asset := range assets {
		wantFileID := uint32(1961)
		if asset.PartitionID == 2 {
			wantFileID = 1962
		}
		if asset.Ref.FileID != wantFileID {
			t.Fatalf("asset %q domain=%d fileID=%d want=%d", asset.ID, asset.PartitionID, asset.Ref.FileID, wantFileID)
		}
		if asset.PartitionID == 0 {
			chunks++
		}
	}
	if chunks < 2 {
		t.Fatalf("multi-pack domain has only %d assets", chunks)
	}
	ownerSegments, ownerBytes := vectorPartitionTestSegmentExtentV1(t, database.ColumnAssetRootDir(), assets)
	if ownerSegments != 2 {
		t.Fatalf("owner segment count=%d want=2", ownerSegments)
	}
	t.Logf("owner-separated: segments=%d physical-bytes=%d assets=%d", ownerSegments, ownerBytes, len(assets))
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		t.Fatal(err)
	}
	for _, domain := range []uint32{0, 2} {
		searcher, err := collection.OpenVectorPartitionLocalSearcherForGenerationV1(definition.Name, manifest.Generation, domain)
		if err != nil {
			t.Fatal(err)
		}
		query := []float32{1, 0, 0}
		if domain == 2 {
			query = []float32{0, 0, 1}
		}
		found, searchErr := searcher.Search(query, 1)
		closeErr := searcher.Close()
		if searchErr != nil || closeErr != nil || len(found) != 1 {
			t.Fatalf("domain %d search=%+v searchErr=%v closeErr=%v", domain, found, searchErr, closeErr)
		}
	}
	legacy, legacyResources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(definition.Name, manifest, 1964, inputs)
	if err != nil {
		t.Fatal(err)
	}
	defer legacyResources.Release()
	for _, asset := range legacy {
		if asset.Ref.FileID != 1964 {
			t.Fatalf("legacy asset %q moved to segment %d", asset.ID, asset.Ref.FileID)
		}
	}
	legacySegments, legacyBytes := vectorPartitionTestSegmentExtentV1(t, database.ColumnAssetRootDir(), legacy)
	if legacySegments != 1 {
		t.Fatalf("legacy segment count=%d want=1", legacySegments)
	}
	t.Logf("legacy: segments=%d physical-bytes=%d assets=%d", legacySegments, legacyBytes, len(legacy))
}

func vectorPartitionTestSegmentExtentV1(t *testing.T, rootDir string, assets []VectorPartitionAssetV1) (int, int64) {
	t.Helper()
	seen := make(map[uint32]bool)
	var bytes int64
	for _, asset := range assets {
		if seen[asset.Ref.FileID] {
			continue
		}
		seen[asset.Ref.FileID] = true
		path, err := columnAssetSegmentPath(rootDir, asset.Ref)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		bytes += info.Size()
	}
	return len(seen), bytes
}
