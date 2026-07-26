package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

// m8ProductionMultiGroupAssetsV1 is persistent M3 asset materialization for
// the later M8 topology. It deliberately contains no transport or Raft logic.
type m8ProductionMultiGroupAssetsV1 struct {
	dir             string
	db              *backenddb.DB
	collection      *collections.Collection
	manifest        collections.VectorPartitionManifestV1
	router          *collections.VectorPartitionRouterV1
	status          collections.VectorPartitionRouterRuntimeStatusV1
	groups          []string
	assetSetDigests map[string]string
}

func newM8ProductionMultiGroupAssetsV1(vectors [][]float64, groups []string, partitions int) (_ *m8ProductionMultiGroupAssetsV1, err error) {
	if len(vectors) == 0 || len(vectors[0]) == 0 || len(groups) < 2 || partitions < 4 {
		return nil, errors.New("M8 production assets require vectors, two groups, and four partitions")
	}
	seen := map[string]bool{}
	for _, group := range groups {
		if group == "" || seen[group] {
			return nil, errors.New("M8 production assets require distinct nonempty groups")
		}
		seen[group] = true
	}
	dir, err := os.MkdirTemp("", "treedb-m8-production-assets-*")
	if err != nil {
		return nil, err
	}
	h := &m8ProductionMultiGroupAssetsV1{dir: dir, groups: append([]string(nil), groups...), assetSetDigests: map[string]string{}}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	if err = backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		return nil, err
	}
	if h.db, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true}); err != nil {
		return nil, err
	}
	meta := partitionCollectionMetaWithDegree("m8_production_vectors", len(vectors[0]), partitionHNSWDegree)
	meta.Options.ColumnStore.AssetManager = &collections.ColumnAssetManagerConfig{Kind: collections.ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "m8_production_assets"}
	manager := collections.NewCollectionManager(h.db)
	if _, err = manager.CreateCollection(meta); err != nil {
		return nil, err
	}
	if h.collection, err = manager.OpenCollection(meta.Name); err != nil {
		return nil, err
	}
	if err = insertPartitionRows(h.collection, vectors, 0, 1); err != nil {
		return nil, err
	}
	if err = h.collection.Flush(); err != nil {
		return nil, err
	}
	if _, err = h.collection.RebuildVectorIndex(partitionHNSWIndex); err != nil {
		return nil, err
	}
	source, err := h.collection.VectorPartitionSourceIdentityV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	snapshot, rows, err := h.collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	if snapshot != source {
		return nil, errors.New("M8 source changed while materializing")
	}
	generation := source.Generation + 1
	h.manifest = collections.VectorPartitionManifestV1{State: "building", Collection: meta.Name, IndexName: partitionHNSWIndex, IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(meta.VectorIndexes[0]), SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount, Generation: generation, PartitionCount: uint32(partitions), BalancePolicy: "round_robin_disjoint_v1"}
	routerParts := make([]vectorpartition.RouterPartitionV1, partitions)
	for part := 0; part < partitions; part++ {
		h.manifest.Placements = append(h.manifest.Placements, collections.VectorPartitionPlacementV1{PartitionID: uint32(part), GroupID: string(groups[part%len(groups)])})
		routerParts[part].PartitionID = uint32(part)
	}
	for _, row := range rows {
		part := int(row.VectorOrdinal % uint64(partitions))
		h.manifest.Memberships = append(h.manifest.Memberships, collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: uint32(part)})
		routerParts[part].Vectors = append(routerParts[part].Vectors, vectorpartition.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: row.Values})
	}
	h.manifest.Canonicalize()
	inputs := make([]collections.VectorPartitionSearchAssetV1, partitions)
	for part := range inputs {
		inputs[part] = collections.VectorPartitionSearchAssetV1{Source: source, Generation: generation, PartitionID: uint32(part), Dimensions: len(vectors[0])}
	}
	assets, res, err := h.collection.MaterializeVectorPartitionLocalSearchAssetsV1(partitionHNSWIndex, h.manifest, 5001, inputs)
	if err != nil {
		return nil, err
	}
	if res != nil {
		res.Release()
	}
	h.manifest.Assets = assets
	h.manifest.Canonicalize()
	if err = h.collection.PublishVectorPartitionManifestV1(h.manifest, nil); err != nil {
		return nil, err
	}
	if _, err = h.collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), h.manifest, routerParts, collections.VectorPartitionRouterBuildOptionsV1{Config: vectorpartition.DefaultRouterConfigV1(), AssetFileID: 5002, AssetPartID: uint64(partitions) + 1, M: partitionHNSWDegree, EfConstruction: 128, EfSearch: 128}); err != nil {
		return nil, err
	}
	if h.router, _, err = h.collection.OpenVectorPartitionRouterV1(partitionHNSWIndex); err != nil {
		return nil, err
	}
	h.status = h.router.Status()
	// Router publication returns the only canonical ready manifest: it includes
	// the shared router asset and ready-set identity in addition to local packs.
	h.manifest = h.status.Manifest
	if h.status.Manifest.State != "ready" || h.status.Manifest.Generation != generation {
		return nil, fmt.Errorf("M8 router status=%+v", h.status)
	}
	for _, group := range groups {
		h.assetSetDigests[group] = m8GroupAssetSetDigestV1(group, h.manifest)
	}
	return h, nil
}

func (h *m8ProductionMultiGroupAssetsV1) RouterSource() nativewire.VectorPartitionCoordinatorRouterSourceV1 {
	return nativewire.CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: h.collection}
}
func (h *m8ProductionMultiGroupAssetsV1) Close() error {
	if h == nil {
		return nil
	}
	var errs []error
	if h.router != nil {
		errs = append(errs, h.router.Close())
		h.router = nil
	}
	if h.db != nil {
		errs = append(errs, h.db.Close())
		h.db = nil
	}
	if h.dir != "" {
		errs = append(errs, os.RemoveAll(h.dir))
		h.dir = ""
	}
	return errors.Join(errs...)
}
func m8GroupAssetSetDigestV1(group string, manifest collections.VectorPartitionManifestV1) string {
	var fields []string
	for _, asset := range manifest.Assets {
		for _, placement := range manifest.Placements {
			if asset.PartitionID == placement.PartitionID && placement.GroupID == group {
				fields = append(fields, fmt.Sprintf("%d/%s/%d/%s", asset.PartitionID, asset.ID, asset.Bytes, asset.Checksum))
			}
		}
	}
	if manifest.RouterAsset.ID != "" {
		fields = append(fields, fmt.Sprintf("router/%s/%d/%s/%d/%d", manifest.RouterAsset.ID, manifest.RouterAsset.Bytes, manifest.RouterAsset.Checksum, manifest.RouterGeneration, manifest.Generation))
	}
	sort.Strings(fields)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s", group, fields)))
	return hex.EncodeToString(sum[:])
}
