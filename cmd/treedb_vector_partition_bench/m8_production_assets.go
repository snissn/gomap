package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

// m8ProductionMultiGroupAssetsV1 is persistent M3 asset materialization for
// the later M8 topology. It deliberately contains no transport or Raft logic.
type m8ProductionMultiGroupAssetsV1 struct {
	dir             string
	owned           bool
	db              *backenddb.DB
	collection      *collections.Collection
	manifest        collections.VectorPartitionManifestV1
	router          *collections.VectorPartitionRouterV1
	status          collections.VectorPartitionRouterRuntimeStatusV1
	groups          []string
	assetSetDigests map[string]string
	descriptor      *m3VariantDescriptorV1
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
	h := &m8ProductionMultiGroupAssetsV1{dir: dir, owned: true, groups: append([]string(nil), groups...), assetSetDigests: map[string]string{}}
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
		routerParts[part].Vectors = append(routerParts[part].Vectors, vectorpartition.RouterVectorV1{
			Ordinal:        row.VectorOrdinal,
			Values:         row.Values,
			MembershipKind: string(collections.VectorPartitionMembershipHomeV1),
		})
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
	return m8TopologyRouterSourceV1{collection: h.collection, manifest: h.manifest}
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
	if h.owned && h.dir != "" {
		errs = append(errs, os.RemoveAll(h.dir))
	}
	h.dir = ""
	return errors.Join(errs...)
}

// openM8ProductionMultiGroupExistingAssetsV1 opens an existing persistent M3
// asset directory strictly read-only. The topology receives a canonical clone
// of its manifest with only group placements relabeled; local pack files and
// the retained source lifecycle stay unchanged.
func openM8ProductionMultiGroupExistingAssetsV1(dir string, groups []string, partitions int, fixture fixtureManifest, vectors [][]float64) (_ *m8ProductionMultiGroupAssetsV1, err error) {
	if dir == "" || len(groups) < 2 {
		return nil, errors.New("M8 existing assets require a directory and two groups")
	}
	h, err := openM8ProductionExistingAssetSetV1(dir)
	if err != nil {
		return nil, err
	}
	h.groups = append([]string(nil), groups...)
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	if err = m8ValidateExistingAssetsFixtureV1(h.collection, h.status.Manifest, fixture, vectors); err != nil {
		return nil, err
	}
	if _, statErr := os.Stat(filepath.Join(dir, m3VariantDescriptorFileV1)); statErr == nil {
		if err = m8BindRetainedM3DescriptorV1(h, fixture); err != nil {
			return nil, err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	if h.status.Manifest.PartitionCount != uint32(partitions) {
		return nil, fmt.Errorf("retained M8 manifest partitions=%d want configured %d", h.status.Manifest.PartitionCount, partitions)
	}
	h.manifest, err = m8RelabelTopologyManifestV1(h.status.Manifest, groups)
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		h.assetSetDigests[group] = m8GroupAssetSetDigestV1(group, h.manifest)
	}
	return h, nil
}

// m8ValidateRetainedM3ProvenanceV1 rejects retained M3 output that cannot
// truthfully participate in this M8 execution before topology construction.
func m8ValidateRetainedM3ProvenanceV1(cfg config, descriptor m3VariantDescriptorV1, executableSHA256 string) error {
	if descriptor.BuildDirty {
		return fmt.Errorf("M8 retained variant %s was built from a dirty worktree", descriptor.VariantID)
	}
	if descriptor.BaseSHA != cfg.baseSHA || descriptor.HeadSHA != cfg.headSHA {
		return fmt.Errorf("M8 retained variant %s revision does not match configured revision", descriptor.VariantID)
	}
	if descriptor.ExecutableSHA256 != executableSHA256 {
		return fmt.Errorf("M8 retained variant %s executable does not match the current benchmark executable", descriptor.VariantID)
	}
	return nil
}

func openM8ProductionExistingAssetSetV1(dir string) (_ *m8ProductionMultiGroupAssetsV1, err error) {
	return openM8ProductionExistingAssetSetModeV1(dir, true)
}

func openM8ProductionExistingAssetSetModeV1(dir string, readOnly bool) (_ *m8ProductionMultiGroupAssetsV1, err error) {
	info, statErr := os.Stat(dir)
	if statErr != nil {
		return nil, fmt.Errorf("M8 existing assets directory: %w", statErr)
	}
	if !info.IsDir() {
		return nil, errors.New("M8 existing assets path is not a directory")
	}
	h := &m8ProductionMultiGroupAssetsV1{dir: dir, assetSetDigests: map[string]string{}}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	h.db, err = backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: readOnly, DisableBackgroundPrune: true})
	if err != nil {
		mode := "read-only"
		if !readOnly {
			mode = "read-write"
		}
		return nil, fmt.Errorf("open retained M8 assets %s: %w", mode, err)
	}
	manager := collections.NewCollectionManager(h.db)
	collectionName, nameErr := m8ExistingCollectionNameV1(manager)
	if nameErr != nil {
		return nil, nameErr
	}
	h.collection, err = manager.OpenCollection(collectionName)
	if err != nil {
		return nil, fmt.Errorf("open retained M8 collection %q: %w", collectionName, err)
	}
	h.router, _, err = h.collection.OpenVectorPartitionRouterV1(partitionHNSWIndex)
	if err != nil {
		return nil, fmt.Errorf("open retained M8 router: %w", err)
	}
	h.status = h.router.Status()
	h.manifest = h.status.Manifest
	return h, nil
}

type localHNSWVariantHarnessV1 struct {
	assets     *m8ProductionMultiGroupAssetsV1
	resources  interface{ Release() }
	searchers  []*collections.VectorPartitionLocalSearcherV1
	packAssets []collections.VectorPartitionAssetV1
}

func (h *localHNSWVariantHarnessV1) Close() error {
	if h == nil {
		return nil
	}
	var err error
	for i, searcher := range h.searchers {
		if searcher != nil {
			err = errors.Join(err, searcher.Close())
			h.searchers[i] = nil
		}
	}
	if h.resources != nil {
		h.resources.Release()
		h.resources = nil
	}
	if h.assets != nil {
		err = errors.Join(err, h.assets.Close())
		h.assets = nil
	}
	return err
}

// materializeRetainedLocalHNSWVariantV1 makes a disposable reflink clone and
// builds one graph variant against the literal retained manifest. It never
// publishes a variant manifest and uses the retained router handle for routes.
func materializeRetainedLocalHNSWVariantV1(source *m8ProductionMultiGroupAssetsV1, tempRoot string, variant collections.VectorPartitionLocalGraphVariantV1, fileID uint32) (_ *localHNSWVariantHarnessV1, err error) {
	if source == nil || source.collection == nil || source.router == nil || tempRoot == "" || fileID == 0 {
		return nil, errors.New("retained local HNSW variant inputs")
	}
	if source.manifest.IntegrityDigest != source.status.Manifest.IntegrityDigest || source.manifest.ReadySetDigest != source.status.Manifest.ReadySetDigest {
		return nil, errors.New("retained local HNSW variant requires raw retained manifest")
	}
	if _, err := collections.VectorPartitionLocalGraphVariantIdentityV1(variant); err != nil {
		return nil, err
	}
	clone, err := os.MkdirTemp(tempRoot, "treedb-4105-variant-*")
	if err != nil {
		return nil, err
	}
	if output, err := exec.Command("cp", "-a", "--reflink=auto", source.dir+"/.", clone).CombinedOutput(); err != nil {
		_ = os.RemoveAll(clone)
		return nil, fmt.Errorf("reflink clone retained DB: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := backenddb.RebindDurableRootSnapshotV1(clone); err != nil {
		_ = os.RemoveAll(clone)
		return nil, err
	}
	owned, err := openM8ProductionExistingAssetSetModeV1(clone, false)
	if err != nil {
		_ = os.RemoveAll(clone)
		return nil, err
	}
	owned.owned = true
	if owned.manifest.IntegrityDigest != source.manifest.IntegrityDigest || owned.manifest.SourceChecksum != source.manifest.SourceChecksum || owned.manifest.Generation != source.manifest.Generation {
		return nil, errors.Join(errors.New("reflink clone retained manifest identity mismatch"), owned.Close())
	}
	inputs := make([]collections.VectorPartitionSearchAssetV1, source.manifest.PartitionCount)
	sourceID := collections.VectorPartitionSourceIdentityV1{Generation: source.manifest.SourceGeneration, Checksum: source.manifest.SourceChecksum, SchemaHash: source.manifest.SourceSchemaHash, RowCount: source.manifest.SourceRowCount}
	var dimensions int
	for _, candidate := range owned.collection.Meta().VectorIndexes {
		if candidate.Name == source.manifest.IndexName && candidate.Dimensions > 0 {
			dimensions = candidate.Dimensions
			break
		}
	}
	if dimensions == 0 {
		return nil, errors.Join(errors.New("retained index definition missing"), owned.Close())
	}
	for p := range inputs {
		inputs[p] = collections.VectorPartitionSearchAssetV1{Source: sourceID, Generation: source.manifest.Generation, PartitionID: uint32(p), Dimensions: dimensions}
	}
	assets, resources, err := owned.collection.MaterializeVectorPartitionLocalSearchAssetsVariantV1(source.manifest.IndexName, source.manifest, fileID, inputs, variant)
	if err != nil {
		return nil, errors.Join(err, owned.Close())
	}
	h := &localHNSWVariantHarnessV1{assets: owned, resources: resources, packAssets: append([]collections.VectorPartitionAssetV1(nil), assets...), searchers: make([]*collections.VectorPartitionLocalSearcherV1, len(assets))}
	defer func() {
		if err != nil {
			err = errors.Join(err, h.Close())
		}
	}()
	if len(assets) != len(inputs) {
		return nil, errors.New("retained variant asset count mismatch")
	}
	for p, asset := range assets {
		if asset.PartitionID != uint32(p) {
			return nil, errors.New("retained variant partition ordering mismatch")
		}
		h.searchers[p], err = owned.collection.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(context.Background(), source.manifest.IndexName, source.manifest, asset)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

func m8BindRetainedM3DescriptorV1(h *m8ProductionMultiGroupAssetsV1, fixture fixtureManifest) error {
	if h == nil || h.collection == nil || h.router == nil {
		return errors.New("retained M8 assets are not open")
	}
	dir := h.dir
	descriptorPath := filepath.Join(dir, m3VariantDescriptorFileV1)
	if _, statErr := os.Stat(descriptorPath); statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return errors.New("retained M8 assets are missing M3 descriptor")
		}
		return fmt.Errorf("stat retained M8 descriptor: %w", statErr)
	}
	descriptor, err := m3ReadVariantDescriptorV1(dir)
	if err != nil {
		return err
	}
	if err := m3DescriptorMatchesManifestV1(descriptor, fixture, h.status.Manifest, h.status.ModelDigest, h.status.Config); err != nil {
		return err
	}
	var indexDefinitionDigest string
	for _, index := range h.collection.MetaView().VectorIndexes {
		if index.Name == partitionHNSWIndex {
			indexDefinitionDigest = collections.VectorIndexDefinitionDigestV1(index)
			break
		}
	}
	if indexDefinitionDigest != descriptor.IndexDefinitionDigest {
		return errors.New("retained M8 descriptor source index definition does not match collection metadata")
	}
	if descriptor.SourceOrdinalDigest != "" {
		_, rows, err := h.collection.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
		if err != nil {
			return errors.New("retained M8 source ordinals are unavailable")
		}
		digest, err := m3SourceOrdinalDigestV1(rows)
		if err != nil || digest != descriptor.SourceOrdinalDigest {
			return errors.New("retained M8 source ordinal mapping does not match descriptor")
		}
	}
	if _, err := m3PartitionLocalGraphVariantV1(descriptor.PartitionHNSWM, m3DescriptorPartitionHNSWEfCV1(descriptor)); err != nil {
		return errors.New("retained M8 descriptor local HNSW construction is not production-selected")
	}
	assetStatus, err := h.collection.VectorPartitionStatusV1(partitionHNSWIndex, h.status.Manifest.Generation)
	if err != nil {
		return fmt.Errorf("verify retained M8 partition assets: %w", err)
	}
	if !assetStatus.Ready || !assetStatus.Active || assetStatus.Manifest.IntegrityDigest != h.status.Manifest.IntegrityDigest ||
		assetStatus.MissingAssets != 0 || assetStatus.CorruptAssets != 0 || assetStatus.StaleAssets != 0 {
		return fmt.Errorf("retained M8 partition assets are unavailable: ready=%t active=%t missing=%d corrupt=%d stale=%d", assetStatus.Ready, assetStatus.Active, assetStatus.MissingAssets, assetStatus.CorruptAssets, assetStatus.StaleAssets)
	}
	h.descriptor = &descriptor
	return nil
}

// m8ValidateExistingAssetsFixtureV1 prevents a retained M3 corpus from being
// measured under an unrelated CLI fixture label. The fixture checksum covers
// the generated corpus, so verify the authoritative FP32 source and stable
// IDs directly before queries and exact truth are constructed from it.
func m8ValidateExistingAssetsFixtureV1(collection *collections.Collection, manifest collections.VectorPartitionManifestV1, fixture fixtureManifest, vectors [][]float64) error {
	if collection == nil || len(vectors) != fixture.Vectors || fixture.Dimensions < 1 {
		return errors.New("retained M8 assets cannot verify fixture corpus")
	}
	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil {
		return fmt.Errorf("read retained M8 source rows: %w", err)
	}
	if source.Generation != manifest.SourceGeneration || source.Checksum != manifest.SourceChecksum || source.SchemaHash != manifest.SourceSchemaHash || source.RowCount != manifest.SourceRowCount {
		return errors.New("retained M8 manifest source identity is stale")
	}
	if len(rows) != fixture.Vectors || source.RowCount != uint64(fixture.Vectors) {
		return fmt.Errorf("retained M8 source rows=%d want fixture vectors=%d", len(rows), fixture.Vectors)
	}
	seen := make([]bool, fixture.Vectors)
	for ordinal, row := range rows {
		id := string(row.DocumentID)
		if len(row.Values) != fixture.Dimensions {
			return fmt.Errorf("retained M8 source row %d does not match fixture shape", ordinal)
		}
		if !m8FixtureDocumentIDValidV1(id, fixture.Vectors) {
			return fmt.Errorf("retained M8 source row %d has invalid fixture document ID %q", ordinal, id)
		}
		fixtureOrdinal, parseErr := strconv.Atoi(strings.TrimPrefix(id, "doc-"))
		if parseErr != nil || fixtureOrdinal < 0 || fixtureOrdinal >= fixture.Vectors || seen[fixtureOrdinal] {
			return fmt.Errorf("retained M8 source row %d has invalid fixture document ID %q", ordinal, id)
		}
		seen[fixtureOrdinal] = true
		for dimension, value := range row.Values {
			if value != float32(vectors[fixtureOrdinal][dimension]) {
				return fmt.Errorf("retained M8 source row %d dimension %d does not match fixture", ordinal, dimension)
			}
		}
	}
	return nil
}

func m8ExistingCollectionNameV1(manager *collections.CollectionManager) (string, error) {
	metas, err := manager.ListCollections()
	if err != nil {
		return "", fmt.Errorf("list retained M8 collections: %w", err)
	}
	var names []string
	for _, meta := range metas {
		for _, index := range meta.VectorIndexes {
			if index.Name == partitionHNSWIndex {
				names = append(names, meta.Name)
				break
			}
		}
	}
	if len(names) != 1 {
		return "", fmt.Errorf("retained M8 assets need exactly one collection with index %q, found %v", partitionHNSWIndex, names)
	}
	return names[0], nil
}

func m8RelabelTopologyManifestV1(local collections.VectorPartitionManifestV1, groups []string) (collections.VectorPartitionManifestV1, error) {
	if len(groups) < 2 {
		return collections.VectorPartitionManifestV1{}, errors.New("M8 topology requires two groups")
	}
	seen := map[string]bool{}
	for _, group := range groups {
		if group == "" || seen[group] {
			return collections.VectorPartitionManifestV1{}, errors.New("M8 topology requires distinct nonempty groups")
		}
		seen[group] = true
	}
	if err := local.Validate(collections.DefaultVectorPartitionManifestLimits()); err != nil {
		return collections.VectorPartitionManifestV1{}, fmt.Errorf("M8 retained local manifest: %w", err)
	}
	cloned := local
	cloned.Placements = append([]collections.VectorPartitionPlacementV1(nil), local.Placements...)
	for i := range cloned.Placements {
		cloned.Placements[i].GroupID = groups[int(cloned.Placements[i].PartitionID)%len(groups)]
	}
	cloned.Canonicalize()
	if err := cloned.Validate(collections.DefaultVectorPartitionManifestLimits()); err != nil {
		return collections.VectorPartitionManifestV1{}, fmt.Errorf("M8 relabeled topology manifest: %w", err)
	}
	return cloned, nil
}

type m8TopologyRouterSourceV1 struct {
	collection *collections.Collection
	manifest   collections.VectorPartitionManifestV1
}

func (s m8TopologyRouterSourceV1) OpenVectorPartitionCoordinatorRouterV1(ctx context.Context, index string, generation uint64) (nativewire.VectorPartitionCoordinatorRouterV1, error) {
	router, err := (nativewire.CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: s.collection}).OpenVectorPartitionCoordinatorRouterV1(ctx, index, generation)
	if err != nil {
		return nil, err
	}
	return m8TopologyRouterV1{VectorPartitionCoordinatorRouterV1: router, manifest: s.manifest}, nil
}

type m8TopologyRouterV1 struct {
	nativewire.VectorPartitionCoordinatorRouterV1
	manifest collections.VectorPartitionManifestV1
}

func (r m8TopologyRouterV1) Status() collections.VectorPartitionRouterRuntimeStatusV1 {
	status := r.VectorPartitionCoordinatorRouterV1.Status()
	status.Manifest = r.manifest
	return status
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
	sum := sha256.Sum256([]byte(group + "\n" + strings.Join(fields, "\n")))
	return hex.EncodeToString(sum[:])
}
