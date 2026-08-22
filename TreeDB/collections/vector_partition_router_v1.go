package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	internalcrc "github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

const (
	vectorPartitionRouterRecordMagicV1   = uint32(0x564b5231) // VKR1
	vectorPartitionRouterRecordVersionV1 = uint16(1)
	VectorPartitionRouterModeExactV1     = "exact"
	VectorPartitionRouterModeApproxV1    = "approximate"
)

// ErrVectorPartitionRouterCandidateCoverageV1 marks a bounded candidate set
// that is valid but collapses to fewer distinct partitions than requested.
var ErrVectorPartitionRouterCandidateCoverageV1 = errors.New("collections: vector partition router candidate coverage shortfall")

type VectorPartitionRouterBuildOptionsV1 struct {
	Config         internalrouter.RouterConfigV1
	AssetFileID    uint32
	AssetPartID    uint64
	M              int
	EfConstruction int
	EfSearch       int
}

type VectorPartitionRouterBuildStatusV1 struct {
	Generation                     uint64
	ModelDigest                    string
	BuildNanos                     uint64
	AppendNanos                    uint64
	PublishNanos                   uint64
	Vectors                        uint64
	Partitions                     uint64
	Representatives                uint64
	MinRepresentativesPerPartition uint64
	MaxRepresentativesPerPartition uint64
	HierarchyNodes                 uint64
	LloydIterations                uint64
	EmptyRepairs                   uint64
	RouterBytes                    uint64
	FailureReason                  string
}

type VectorPartitionRouterOpenStatusV1 struct {
	Generation      uint64
	ModelDigest     string
	OpenNanos       uint64
	RouterBytes     uint64
	Representatives uint64
	Partitions      uint64
	MappedBytes     uint64
	HeapCopyBytes   uint64
	ActiveHandles   int64
	FailureReason   string
}

type VectorPartitionRouterSearchOptionsV1 struct {
	Mode            string
	CandidateBudget int
	PartitionProbes int
}

type VectorPartitionRouterPartitionScoreV1 struct {
	PartitionID           uint32
	Distance              float64
	WinningRepresentative int
	WinningSourceOrdinal  uint64
}

type VectorPartitionRouterSearchStatusV1 struct {
	Mode            string
	SearchNanos     uint64
	CandidateBudget uint64
	PartitionProbes uint64
	Candidates      uint64
	Edges           uint64
	Selected        uint64
	WinningDistance float64
	FailureReason   string
}

type VectorPartitionRouterSearchResultV1 struct {
	Partitions []VectorPartitionRouterPartitionScoreV1
	Status     VectorPartitionRouterSearchStatusV1
}

type VectorPartitionRouterRuntimeStatusV1 struct {
	Manifest        VectorPartitionManifestV1
	Config          internalrouter.RouterConfigV1
	ModelDigest     string
	Representatives uint64
	Partitions      uint64
	RouterBytes     uint64
	OpenNanos       uint64
	Searches        uint64
	SearchFailures  uint64
	Candidates      uint64
	Edges           uint64
	Selected        uint64
	ActiveHandles   int64
}

// VectorPartitionRouterSourceRowV1 is one authoritative vector-index row in
// the exact ordinal space used by M1 memberships and M4 representatives.
type VectorPartitionRouterSourceRowV1 struct {
	VectorOrdinal uint64
	DocumentID    []byte
	Values        []float32
}

type vectorPartitionRouterRecordV1 struct {
	RouterGeneration   uint64
	ModelDigest        [sha256.Size]byte
	PartitionID        uint32
	SourceOrdinal      uint64
	LeafNodeID         uint32
	Depth              uint16
	MemberCount        uint32
	Config             internalrouter.RouterConfigV1
	Metrics            internalrouter.RouterBuildMetricsV1
	HNSWM              uint32
	HNSWEfConstruction uint32
	HNSWEfSearch       uint32
	Path               []vectorPartitionRouterPathNodeV1
}

type vectorPartitionRouterPathNodeV1 struct {
	NodeID      uint32
	MemberCount uint32
}

type VectorPartitionRouterV1 struct {
	manifest    VectorPartitionManifestV1
	model       internalrouter.RouterModelV1
	modelDigest string
	viewToModel []int
	view        *columnHNSWSearchPackPreparedView
	pin         *VectorPartitionReaderPinV1
	openNanos   uint64

	closeMu sync.RWMutex
	closed  atomic.Bool
	scratch sync.Pool

	searches       atomic.Uint64
	searchFailures atomic.Uint64
	candidates     atomic.Uint64
	edges          atomic.Uint64
	selected       atomic.Uint64
}

// BuildAndPublishVectorPartitionRouterV1 turns an already-persisted M1
// building generation into a ready generation. Publication is the only
// visibility point; cancellation or any validation failure leaves it building.
func (c *Collection) BuildAndPublishVectorPartitionRouterV1(ctx context.Context, building VectorPartitionManifestV1, partitions []internalrouter.RouterPartitionV1, opts VectorPartitionRouterBuildOptionsV1) (status VectorPartitionRouterBuildStatusV1, resultErr error) {
	started := time.Now()
	status.Generation = building.Generation
	fail := func(err error) (VectorPartitionRouterBuildStatusV1, error) {
		status.BuildNanos = elapsedNanosVPR(started)
		if err != nil {
			status.FailureReason = err.Error()
		}
		return status, err
	}
	if c == nil || c.db == nil {
		return fail(errors.New("collections: closed collection"))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	building.Canonicalize()
	if building.State != "building" {
		return fail(errors.New("collections: vector partition router build requires a building manifest"))
	}
	if err := building.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		return fail(err)
	}
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		return fail(err)
	}
	stored, err := store.Open(c.name, building.IndexName, building.Generation)
	if err != nil {
		return fail(err)
	}
	storedBytes, storedErr := EncodeVectorPartitionManifestV1(stored)
	buildingBytes, buildingErr := EncodeVectorPartitionManifestV1(building)
	if storedErr != nil || buildingErr != nil || !bytes.Equal(storedBytes, buildingBytes) {
		return fail(errors.New("collections: vector partition router building generation differs from durable state"))
	}
	if building.Collection != c.name {
		return fail(errors.New("collections: vector partition router collection mismatch"))
	}
	if err := c.validateVectorPartitionSourceIdentityV1(building); err != nil {
		return fail(err)
	}
	def, err := c.vectorPartitionRouterDefinitionV1(building.IndexName)
	if err != nil {
		return fail(err)
	}
	if building.IndexDefinitionDigest != VectorIndexDefinitionDigestV1(def) {
		return fail(errors.New("collections: vector partition router index definition digest mismatch"))
	}
	if opts.Config == (internalrouter.RouterConfigV1{}) {
		opts.Config = internalrouter.DefaultRouterConfigV1()
	}
	if err := internalrouter.ValidateRouterConfigV1(opts.Config); err != nil {
		return fail(err)
	}
	if building.SourceRowCount > uint64(opts.Config.MaxVectors) {
		return fail(fmt.Errorf("collections: vector partition router source rows=%d exceed configured limit=%d", building.SourceRowCount, opts.Config.MaxVectors))
	}
	if def.Dimensions > opts.Config.MaxDimensions {
		return fail(fmt.Errorf("collections: vector partition router dimensions=%d exceed configured limit=%d", def.Dimensions, opts.Config.MaxDimensions))
	}
	searchDef := def
	if opts.M > 0 {
		searchDef.M = opts.M
	}
	if opts.EfConstruction > 0 {
		searchDef.EfConstruction = opts.EfConstruction
	}
	if opts.EfSearch > 0 {
		searchDef.EfSearch = opts.EfSearch
	}
	searchDef, err = normalizeVectorIndexDefinition(searchDef)
	if err != nil {
		return fail(err)
	}
	finalMemberships := uint64(len(building.Memberships) + len(building.OverlapMemberships))
	if finalMemberships > uint64(opts.Config.MaxVectors) {
		return fail(fmt.Errorf("collections: vector partition router final memberships=%d exceed configured limit=%d", finalMemberships, opts.Config.MaxVectors))
	}
	maxRepresentatives, ok := checkedVectorPartitionRouterRepresentativeBoundV1(
		finalMemberships, building.PartitionCount, opts.Config.RepresentativesPerPartition,
	)
	if !ok {
		return fail(errors.New("collections: vector partition router representative preflight overflow"))
	}
	if maxRepresentatives > uint64(opts.Config.MaxRepresentatives) {
		return fail(fmt.Errorf("collections: vector partition router representatives=%d exceed configured limit=%d", maxRepresentatives, opts.Config.MaxRepresentatives))
	}
	scalarWork, ok := checkedVectorPartitionRouterScalarWorkV1(building, opts.Config, def.Dimensions)
	if !ok {
		return fail(errors.New("collections: vector partition router scalar-work preflight overflow"))
	}
	if scalarWork > uint64(opts.Config.MaxScalarWork) {
		return fail(fmt.Errorf("collections: vector partition router scalar work=%d exceed configured limit=%d", scalarWork, opts.Config.MaxScalarWork))
	}
	estimatedBytes, err := estimateVectorPartitionRouterPackShapeBytesV1(
		maxRepresentatives, def.Dimensions, opts.Config.MaxDepth, searchDef,
	)
	if err != nil {
		return fail(err)
	}
	if estimatedBytes > opts.Config.MaxRouterBytes {
		return fail(fmt.Errorf("collections: vector partition router estimated bytes=%d exceed configured cap=%d", estimatedBytes, opts.Config.MaxRouterBytes))
	}
	if err := validateVectorPartitionRouterInputV1(building, partitions, def.Dimensions); err != nil {
		return fail(err)
	}
	authoritativePartitions, err := c.authoritativeVectorPartitionRouterInputV1(building, def, partitions)
	if err != nil {
		return fail(err)
	}
	model, err := internalrouter.BuildRouterV1(authoritativePartitions, opts.Config)
	if err != nil {
		return fail(err)
	}
	modelDigest, err := internalrouter.RouterDigestWithContextV1(ctx, model)
	if err != nil {
		return fail(err)
	}
	status.ModelDigest = modelDigest
	status.Vectors = uint64(model.Metrics.Vectors)
	status.Partitions = uint64(model.Metrics.Partitions)
	status.Representatives = uint64(model.Metrics.Representatives)
	representativesPerPartition := make(map[uint32]uint64, model.Metrics.Partitions)
	for _, representative := range model.Representatives {
		representativesPerPartition[representative.PartitionID]++
	}
	for _, count := range representativesPerPartition {
		if status.MinRepresentativesPerPartition == 0 || count < status.MinRepresentativesPerPartition {
			status.MinRepresentativesPerPartition = count
		}
		if count > status.MaxRepresentativesPerPartition {
			status.MaxRepresentativesPerPartition = count
		}
	}
	status.HierarchyNodes = uint64(model.Metrics.HierarchyNodes)
	status.LloydIterations = uint64(model.Metrics.LloydIterations)
	status.EmptyRepairs = uint64(model.Metrics.EmptyRepairs)
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	raw, err := buildVectorPartitionRouterPackV1(building, model, modelDigest, searchDef)
	if err != nil {
		return fail(err)
	}
	if uint64(len(raw)) > model.Config.MaxRouterBytes {
		return fail(fmt.Errorf("collections: vector partition router bytes=%d exceed configured cap=%d", len(raw), model.Config.MaxRouterBytes))
	}
	status.BuildNanos = elapsedNanosVPR(started)

	cfg := c.meta.Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		return fail(errors.New("collections: vector partition router requires column asset storage"))
	}
	if opts.AssetFileID == 0 {
		opts.AssetFileID = columnAssetM12ASegmentFileID
	}
	if opts.AssetPartID == 0 {
		opts.AssetPartID = uint64(building.PartitionCount) + 1
	}
	lease, err := c.db.AcquireStableResourceCaptureLease()
	if err != nil {
		return fail(err)
	}
	defer lease.Release()
	appendStarted := time.Now()
	refs, routerResources, err := AppendColumnPhysicalAssetsWithStableResources(
		c.db.ColumnAssetRootDir(), *cfg, opts.AssetFileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload: raw, Kind: ColumnAssetKindTCS1HNSWSearchPack,
			Generation: building.Generation, PartID: opts.AssetPartID,
		}},
		c.db.StableResourceIdentityPinRegistry(), lease,
	)
	status.AppendNanos = elapsedNanosVPR(appendStarted)
	if err != nil {
		return fail(err)
	}
	if len(refs) != 1 || routerResources == nil {
		if routerResources != nil {
			routerResources.Release()
		}
		return fail(errors.New("collections: vector partition router append returned incomplete authority"))
	}
	defer func() {
		if routerResources != nil {
			routerResources.Release()
		}
	}()
	existingResources, err := captureVectorPartitionRouterExistingAssetsV1(
		c.db.ColumnAssetRootDir(), building.Assets, c.db.StableResourceIdentityPinRegistry(),
	)
	if err != nil {
		return fail(err)
	}
	defer func() {
		if existingResources != nil {
			existingResources.Release()
		}
	}()
	builder := rootpublication.NewStableResourceSetBuilder()
	if err := builder.Merge(existingResources); err != nil {
		builder.Abandon()
		return fail(err)
	}
	existingResources = nil
	if err := builder.Merge(routerResources); err != nil {
		builder.Abandon()
		return fail(err)
	}
	routerResources = nil
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return fail(err)
	}
	sum := sha256.Sum256(raw)
	ready := building
	ready.State = "ready"
	ready.RouterGeneration = ready.Generation
	ready.RouterAsset = VectorPartitionAssetV1{
		ID:          "router/krt-hnsw-v1/" + modelDigest,
		Checksum:    hex.EncodeToString(sum[:]),
		Bytes:       uint64(len(raw)),
		PartitionID: 0,
		Ref:         refs[0],
	}
	actualRepresentatives := make([]VectorPartitionMembershipV1, 0, len(model.Representatives))
	for _, representative := range model.Representatives {
		actualRepresentatives = append(actualRepresentatives, VectorPartitionMembershipV1{
			VectorOrdinal: representative.SourceOrdinal,
			PartitionID:   representative.PartitionID,
		})
	}
	sort.Slice(actualRepresentatives, func(i, j int) bool {
		if actualRepresentatives[i].VectorOrdinal != actualRepresentatives[j].VectorOrdinal {
			return actualRepresentatives[i].VectorOrdinal < actualRepresentatives[j].VectorOrdinal
		}
		return actualRepresentatives[i].PartitionID < actualRepresentatives[j].PartitionID
	})
	if len(ready.Representatives) != 0 && !equalVectorPartitionMembershipsV1(actualRepresentatives, ready.Representatives) {
		resources.Release()
		return fail(errors.New("collections: vector partition router representatives differ from building manifest"))
	}
	ready.Representatives = actualRepresentatives
	ready.Canonicalize()
	status.RouterBytes = ready.RouterAsset.Bytes
	if err := ctx.Err(); err != nil {
		resources.Release()
		return fail(err)
	}
	publishStarted := time.Now()
	err = c.PublishVectorPartitionManifestV1(ready, resources)
	status.PublishNanos = elapsedNanosVPR(publishStarted)
	if err != nil {
		return fail(err)
	}
	status.BuildNanos = elapsedNanosVPR(started)
	return status, nil
}

func (c *Collection) vectorPartitionRouterDefinitionV1(index string) (VectorIndexDefinition, error) {
	for _, candidate := range c.meta.VectorIndexes {
		if candidate.Name == index {
			return candidate, nil
		}
	}
	return VectorIndexDefinition{}, fmt.Errorf("collections: unknown vector index %q", index)
}

// ReadVectorPartitionRouterSourceRowsV1 returns a bounded owned snapshot of the
// current typed FP32 source. Callers use the document ID to derive memberships;
// BuildAndPublishVectorPartitionRouterV1 reopens and bit-verifies the source so
// a stale or caller-modified snapshot still fails closed.
func (c *Collection) ReadVectorPartitionRouterSourceRowsV1(index string) (_ VectorPartitionSourceIdentityV1, _ []VectorPartitionRouterSourceRowV1, resultErr error) {
	var identity VectorPartitionSourceIdentityV1
	def, err := c.vectorPartitionRouterDefinitionV1(index)
	if err != nil {
		return identity, nil, err
	}
	reader, err := c.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		return identity, nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, reader.Close())
	}()
	if reader.typedVectorSource == nil {
		return identity, nil, errors.New("collections: authoritative vector partition router typed vector source is unavailable")
	}
	if reader.documentIDSource == nil {
		return identity, nil, errors.New("collections: authoritative vector partition router document-ID source is unavailable")
	}
	if reader.graph.RowCount < 1 || reader.graph.RowCount > internalrouter.DefaultRouterConfigV1().MaxVectors {
		return identity, nil, fmt.Errorf("collections: authoritative vector partition router rows=%d outside supported bound", reader.graph.RowCount)
	}
	identity = VectorPartitionSourceIdentityV1{
		Generation: reader.graph.BaseManifestGeneration,
		Checksum:   reader.graph.BaseManifestChecksum,
		SchemaHash: reader.graph.BaseSchemaHash,
		RowCount:   uint64(reader.graph.RowCount),
	}
	rows := make([]VectorPartitionRouterSourceRowV1, reader.graph.RowCount)
	for ordinal := range rows {
		values, _, _, ok := reader.typedVectorSource.vectorForOrdinal(ordinal)
		if !ok || len(values) != def.Dimensions {
			return VectorPartitionSourceIdentityV1{}, nil, fmt.Errorf("collections: authoritative vector partition router source ordinal %d is unavailable", ordinal)
		}
		id, ok := reader.documentIDForOrdinal(ordinal)
		if !ok || len(id) == 0 {
			return VectorPartitionSourceIdentityV1{}, nil, fmt.Errorf("collections: authoritative vector partition router document ID %d is unavailable", ordinal)
		}
		rows[ordinal] = VectorPartitionRouterSourceRowV1{
			VectorOrdinal: uint64(ordinal),
			DocumentID:    append([]byte(nil), id...),
			Values:        append([]float32(nil), values...),
		}
	}
	return identity, rows, nil
}

func validateVectorPartitionRouterInputV1(manifest VectorPartitionManifestV1, partitions []internalrouter.RouterPartitionV1, dimensions int) error {
	if len(partitions) != int(manifest.PartitionCount) {
		return fmt.Errorf("collections: vector partition router inputs=%d want partitions=%d", len(partitions), manifest.PartitionCount)
	}
	expected := make(map[uint32]map[uint64]string, manifest.PartitionCount)
	for partitionID := uint32(0); partitionID < manifest.PartitionCount; partitionID++ {
		expected[partitionID] = make(map[uint64]string)
	}
	for _, membership := range manifest.Memberships {
		expected[membership.PartitionID][membership.VectorOrdinal] = string(VectorPartitionMembershipHomeV1)
	}
	for _, membership := range manifest.OverlapMemberships {
		expected[membership.PartitionID][membership.VectorOrdinal] = string(VectorPartitionMembershipOverlapV1)
	}
	seenPartitions := make(map[uint32]struct{}, len(partitions))
	for _, partition := range partitions {
		if partition.PartitionID >= manifest.PartitionCount {
			return fmt.Errorf("collections: vector partition router partition %d is outside count=%d", partition.PartitionID, manifest.PartitionCount)
		}
		if _, exists := seenPartitions[partition.PartitionID]; exists {
			return fmt.Errorf("collections: duplicate vector partition router partition %d", partition.PartitionID)
		}
		seenPartitions[partition.PartitionID] = struct{}{}
		if len(partition.Vectors) != len(expected[partition.PartitionID]) {
			return fmt.Errorf("collections: vector partition router partition %d vectors=%d want final memberships=%d", partition.PartitionID, len(partition.Vectors), len(expected[partition.PartitionID]))
		}
		seenVectors := make(map[uint64]struct{}, len(partition.Vectors))
		for _, vector := range partition.Vectors {
			if len(vector.Values) != dimensions {
				return fmt.Errorf("collections: vector partition router vector %d dimensions=%d want %d", vector.Ordinal, len(vector.Values), dimensions)
			}
			kind, exists := expected[partition.PartitionID][vector.Ordinal]
			if !exists || vector.MembershipKind != string(kind) {
				return fmt.Errorf("collections: vector partition router vector %d is not a final member of partition %d", vector.Ordinal, partition.PartitionID)
			}
			if _, exists := seenVectors[vector.Ordinal]; exists {
				return fmt.Errorf("collections: duplicate vector partition router vector %d in partition %d", vector.Ordinal, partition.PartitionID)
			}
			seenVectors[vector.Ordinal] = struct{}{}
		}
	}
	return nil
}

func (c *Collection) authoritativeVectorPartitionRouterInputV1(manifest VectorPartitionManifestV1, def VectorIndexDefinition, partitions []internalrouter.RouterPartitionV1) (_ []internalrouter.RouterPartitionV1, resultErr error) {
	reader, err := c.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		return nil, fmt.Errorf("collections: open authoritative vector partition router source: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, reader.Close())
	}()
	if reader.graph.BaseManifestGeneration != manifest.SourceGeneration ||
		reader.graph.BaseManifestChecksum != manifest.SourceChecksum ||
		reader.graph.BaseSchemaHash != manifest.SourceSchemaHash ||
		uint64(reader.graph.RowCount) != manifest.SourceRowCount {
		return nil, errors.New("collections: authoritative vector partition router source identity mismatch")
	}
	if reader.typedVectorSource == nil {
		return nil, errors.New("collections: authoritative vector partition router typed vector source is unavailable")
	}
	authoritative := make([]internalrouter.RouterPartitionV1, len(partitions))
	for partitionOrdinal, partition := range partitions {
		authoritative[partitionOrdinal].PartitionID = partition.PartitionID
		authoritative[partitionOrdinal].Vectors = make([]internalrouter.RouterVectorV1, len(partition.Vectors))
		for vectorOrdinal, vector := range partition.Vectors {
			if vector.Ordinal > math.MaxInt {
				return nil, fmt.Errorf("collections: vector partition router ordinal %d overflows source reader", vector.Ordinal)
			}
			source, _, _, ok := reader.typedVectorSource.vectorForOrdinal(int(vector.Ordinal))
			if !ok || len(source) != def.Dimensions {
				return nil, fmt.Errorf("collections: authoritative vector partition router source ordinal %d is unavailable", vector.Ordinal)
			}
			for dimension := range source {
				if math.Float32bits(source[dimension]) != math.Float32bits(vector.Values[dimension]) {
					return nil, fmt.Errorf("collections: vector partition router vector %d dimension %d differs from authoritative source", vector.Ordinal, dimension)
				}
			}
			authoritative[partitionOrdinal].Vectors[vectorOrdinal] = internalrouter.RouterVectorV1{
				Ordinal:        vector.Ordinal,
				Values:         append([]float32(nil), source...),
				MembershipKind: vector.MembershipKind,
			}
		}
	}
	return authoritative, nil
}

func buildVectorPartitionRouterPackV1(manifest VectorPartitionManifestV1, model internalrouter.RouterModelV1, modelDigest string, def VectorIndexDefinition) ([]byte, error) {
	digest, err := hex.DecodeString(modelDigest)
	if err != nil || len(digest) != sha256.Size {
		return nil, errors.New("collections: invalid vector partition router model digest")
	}
	nodes := make(map[uint32]internalrouter.RouterHierarchyNodeV1, len(model.Nodes))
	for _, node := range model.Nodes {
		nodes[node.NodeID] = node
	}
	rows := make([]columnVectorGraphAssetRow, len(model.Representatives))
	for ordinal, representative := range model.Representatives {
		record := vectorPartitionRouterRecordV1{
			RouterGeneration:   manifest.Generation,
			PartitionID:        representative.PartitionID,
			SourceOrdinal:      representative.SourceOrdinal,
			LeafNodeID:         representative.LeafNodeID,
			Depth:              representative.Depth,
			MemberCount:        representative.MemberCount,
			Config:             model.Config,
			Metrics:            model.Metrics,
			HNSWM:              uint32(def.M),
			HNSWEfConstruction: uint32(def.EfConstruction),
			HNSWEfSearch:       uint32(def.EfSearch),
		}
		copy(record.ModelDigest[:], digest)
		for _, nodeID := range representative.Path {
			node, exists := nodes[nodeID]
			if !exists {
				return nil, fmt.Errorf("collections: vector partition router hierarchy node %d missing", nodeID)
			}
			record.Path = append(record.Path, vectorPartitionRouterPathNodeV1{NodeID: nodeID, MemberCount: node.MemberCount})
		}
		id, err := encodeVectorPartitionRouterRecordV1(record)
		if err != nil {
			return nil, err
		}
		rows[ordinal] = columnVectorGraphAssetRow{
			ID:     id,
			Vector: append([]float32(nil), representative.Values...),
			// BuildRouterV1 already emits normalized centers. Preserve those
			// exact float32 bits so the persisted pack reconstructs the same
			// canonical model digest rather than normalizing a second time.
			InvNorm: 1,
			BaseRowRef: DocumentRowRef{
				DocumentID: id, Generation: manifest.SourceGeneration,
				PartID:   uint64(representative.PartitionID) + 1,
				RowIndex: ordinal, AppliedCommandLSN: 1,
			},
		}
	}
	if err := buildColumnVectorGraphAdjacencyWithConstructionPolicyV1(rows, def, nil, true, nil); err != nil {
		return nil, err
	}
	graph := columnVectorGraphManifestSnapshot{
		IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding,
		Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch,
		BaseManifestGeneration: manifest.SourceGeneration,
		BaseManifestChecksum:   manifest.SourceChecksum,
		BaseSchemaHash:         manifest.SourceSchemaHash,
		GraphSchemaHash:        manifest.SourceSchemaHash,
		RowCount:               len(rows),
	}
	input, err := buildColumnHNSWSearchPackInput(def, graph, rows)
	if err != nil {
		return nil, err
	}
	input.MembershipDigest = vectorPartitionRouterFinalMembershipDigestV1(manifest)
	return encodeColumnHNSWSearchPack(input)
}

// vectorPartitionRouterFinalMembershipDigestV1 binds a router asset to the
// exact canonical home-plus-overlap relation, independently of its selected
// representatives. Source identity is included so an ordinal cannot be reused
// against a different vector generation.
func vectorPartitionRouterFinalMembershipDigestV1(manifest VectorPartitionManifestV1) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte("treedb/vector-partition-router-final-membership/v1"))
	var encoded [8]byte
	for _, value := range []uint64{manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, manifest.SourceRowCount, manifest.Generation} {
		binary.BigEndian.PutUint64(encoded[:], value)
		h.Write(encoded[:])
	}
	for kind, memberships := range [][]VectorPartitionMembershipV1{manifest.Memberships, manifest.OverlapMemberships} {
		binary.BigEndian.PutUint64(encoded[:], uint64(kind))
		h.Write(encoded[:])
		binary.BigEndian.PutUint64(encoded[:], uint64(len(memberships)))
		h.Write(encoded[:])
		for _, membership := range memberships {
			binary.BigEndian.PutUint64(encoded[:], membership.VectorOrdinal)
			h.Write(encoded[:])
			binary.BigEndian.PutUint32(encoded[:4], membership.PartitionID)
			h.Write(encoded[:4])
		}
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func checkedVectorPartitionRouterRepresentativeBoundV1(sourceRows uint64, partitions uint32, representativesPerPartition int) (uint64, bool) {
	if sourceRows == 0 || partitions == 0 || representativesPerPartition < 1 {
		return 0, false
	}
	if uint64(partitions) > math.MaxUint64/uint64(representativesPerPartition) {
		return 0, false
	}
	bound := uint64(partitions) * uint64(representativesPerPartition)
	if bound > sourceRows {
		bound = sourceRows
	}
	return bound, bound > 0
}

func checkedVectorPartitionRouterScalarWorkV1(manifest VectorPartitionManifestV1, cfg internalrouter.RouterConfigV1, dimensions int) (uint64, bool) {
	if dimensions < 1 || manifest.PartitionCount == 0 {
		return 0, false
	}
	counts := make([]uint64, manifest.PartitionCount)
	for _, membership := range manifest.Memberships {
		if membership.PartitionID >= manifest.PartitionCount {
			return 0, false
		}
		counts[membership.PartitionID]++
	}
	for _, membership := range manifest.OverlapMemberships {
		if membership.PartitionID >= manifest.PartitionCount {
			return 0, false
		}
		counts[membership.PartitionID]++
	}
	var pairs uint64
	for _, count := range counts {
		budget := min(count, uint64(cfg.RepresentativesPerPartition))
		if budget != 0 && count > math.MaxUint64/budget {
			return 0, false
		}
		product := count * budget
		if pairs > math.MaxUint64-product {
			return 0, false
		}
		pairs += product
	}
	work := pairs
	for _, multiplier := range []uint64{uint64(cfg.BranchFactor), uint64(cfg.MaxIterations), uint64(dimensions)} {
		if multiplier != 0 && work > math.MaxUint64/multiplier {
			return 0, false
		}
		work *= multiplier
	}
	return work, true
}

func estimateVectorPartitionRouterPackShapeBytesV1(rows uint64, dimensions, maxDepth int, def VectorIndexDefinition) (uint64, error) {
	if rows == 0 || dimensions < 1 || maxDepth < 1 || def.M < 1 {
		return 0, errors.New("collections: cannot estimate invalid vector partition router pack")
	}
	multiply := func(values ...uint64) (uint64, error) {
		product := uint64(1)
		for _, value := range values {
			if value != 0 && product > math.MaxUint64/value {
				return 0, errors.New("collections: vector partition router byte estimate overflow")
			}
			product *= value
		}
		return product, nil
	}
	add := func(total *uint64, value uint64) error {
		if *total > math.MaxUint64-value {
			return errors.New("collections: vector partition router byte estimate overflow")
		}
		*total += value
		return nil
	}
	// This intentionally overestimates a 64-layer native HNSW pack so the
	// configured persisted-byte cap is checked before row/adjacency allocation.
	recordBytes := uint64(212 + (maxDepth+1)*8)
	components := [][]uint64{
		{rows, uint64(dimensions), 4},
		{rows, 4},
		{rows + 1, uint64(columnHNSWSearchPackMaxLayersDefault), 8},
		{rows, uint64(def.M), uint64(columnHNSWSearchPackMaxLayersDefault), 8},
		{rows, 32},
		{rows + 1, 8},
		{rows, recordBytes},
	}
	total := uint64(4096)
	for _, component := range components {
		value, err := multiply(component...)
		if err != nil {
			return 0, err
		}
		if err := add(&total, value); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func captureVectorPartitionRouterExistingAssetsV1(root string, assets []VectorPartitionAssetV1, registry *rootpublication.IdentityPinRegistry) (*rootpublication.StableResourceSet, error) {
	if registry == nil {
		return nil, errors.New("collections: vector partition router asset capture requires identity registry")
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	for _, asset := range assets {
		path, err := columnAssetSegmentPath(root, asset.Ref)
		if err != nil {
			builder.Abandon()
			return nil, err
		}
		parent, err := rootpublication.OpenStableParent(filepath.Dir(path))
		if err != nil {
			builder.Abandon()
			return nil, err
		}
		file, err := rootpublication.OpenStableChildFile(parent, filepath.Base(path), os.O_RDONLY, 0)
		if err != nil {
			parent.Close()
			builder.Abandon()
			return nil, err
		}
		if err := verifyVectorPartitionRouterStableAssetV1(file, asset.Ref); err != nil {
			file.Close()
			parent.Close()
			builder.Abandon()
			return nil, fmt.Errorf("collections: vector partition router asset %q: %w", asset.ID, err)
		}
		namespace, err := stableColumnAssetNamespaceToken(parent, file, asset.Ref)
		if err == nil {
			err = namespace.Stabilize()
		}
		if err != nil {
			if namespace != nil {
				namespace.Release()
			}
			file.Close()
			parent.Close()
			builder.Abandon()
			return nil, err
		}
		token, err := stableColumnAssetResourceTokenWithRegistry(file, asset.Ref, namespace, registry)
		namespace.Release()
		file.Close()
		parent.Close()
		if err != nil {
			builder.Abandon()
			return nil, err
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			return nil, err
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	return resources, nil
}

// verifyVectorPartitionRouterStableAssetV1 verifies the exact stable child
// handle in bounded memory. Partition packs may be multi-gigabyte assets, so
// publication must never materialize an existing pack merely to recapture its
// reachability token.
func verifyVectorPartitionRouterStableAssetV1(file *os.File, ref ColumnAssetRef) error {
	if file == nil {
		return errors.New("collections: vector partition router stable asset file is nil")
	}
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return err
	}
	reader := io.NewSectionReader(file, ref.Offset, ref.Length)
	var buffer [64 << 10]byte
	var checksum uint32
	var total int64
	for {
		n, err := reader.Read(buffer[:])
		if n > 0 {
			checksum = internalcrc.Update(checksum, buffer[:n])
			total += int64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	if total != ref.Length {
		return io.ErrUnexpectedEOF
	}
	if checksum != ref.Checksum {
		return fmt.Errorf("collections: column physical asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
	}
	return nil
}

// OpenVectorPartitionRouterV1 validates and pins the currently active ready
// generation under the M1 storage barrier.
func (c *Collection) OpenVectorPartitionRouterV1(index string) (*VectorPartitionRouterV1, VectorPartitionRouterOpenStatusV1, error) {
	return c.OpenVectorPartitionRouterWithContextV1(context.Background(), index)
}

// OpenVectorPartitionRouterWithContextV1 is the cancellation-aware M4 open
// used by the bounded M6 coordinator. The legacy entry point remains a
// background-context wrapper.
func (c *Collection) OpenVectorPartitionRouterWithContextV1(ctx context.Context, index string) (*VectorPartitionRouterV1, VectorPartitionRouterOpenStatusV1, error) {
	return c.openVectorPartitionRouterWithContextV1(ctx, index, func(ctx context.Context, store *VectorPartitionStoreV1) (VectorPartitionManifestV1, error) {
		return store.OpenActiveWithContext(ctx, c.name, index)
	})
}

// OpenPreparedVectorPartitionRouterForGenerationWithContextV1 opens one exact
// locally prepared generation without consulting the standalone active
// pointer. Replicated lifecycle authority remains responsible for admitting
// that generation before the caller performs routing or shard dispatch.
func (c *Collection) OpenPreparedVectorPartitionRouterForGenerationWithContextV1(ctx context.Context, index string, generation uint64) (*VectorPartitionRouterV1, VectorPartitionRouterOpenStatusV1, error) {
	if generation == 0 {
		return nil, VectorPartitionRouterOpenStatusV1{FailureReason: "collections: invalid vector partition router generation"}, errors.New("collections: invalid vector partition router generation")
	}
	return c.openVectorPartitionRouterWithContextV1(ctx, index, func(ctx context.Context, store *VectorPartitionStoreV1) (VectorPartitionManifestV1, error) {
		loaded, present, err := store.loadVectorPartitionLifecycleAuthorityWithContextV1(ctx, c.name, index)
		if err != nil {
			return VectorPartitionManifestV1{}, err
		}
		entry, ok := loaded.state.Generations[generation]
		if !present || !ok || entry.Manifest == nil || entry.Deleting || entry.Manifest.State != "ready" {
			return VectorPartitionManifestV1{}, fmt.Errorf("%w: generation %d is not prepared and ready", ErrVectorPartitionManifestInvalid, generation)
		}
		return vectorPartitionLifecycleManifestWithContextV1(ctx, loaded.state, generation, false)
	})
}

func (c *Collection) openVectorPartitionRouterWithContextV1(
	ctx context.Context,
	index string,
	load func(context.Context, *VectorPartitionStoreV1) (VectorPartitionManifestV1, error),
) (*VectorPartitionRouterV1, VectorPartitionRouterOpenStatusV1, error) {
	var router *VectorPartitionRouterV1
	var status VectorPartitionRouterOpenStatusV1
	started := time.Now()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		status.FailureReason = err.Error()
		return nil, status, err
	}
	if c == nil || c.db == nil || load == nil {
		status.FailureReason = "collections: closed collection"
		return nil, status, errors.New(status.FailureReason)
	}
	err := WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
		if err != nil {
			return err
		}
		manifest, err := load(ctx, store)
		if err != nil {
			return err
		}
		if manifest.State != "ready" ||
			manifest.RouterGeneration != manifest.Generation ||
			manifest.RouterAsset.Ref.Generation != manifest.Generation {
			return errors.New("collections: vector partition router generation is not ready")
		}
		if len(manifest.Representatives) == 0 {
			return errors.New("collections: vector partition router ready generation has no representative mapping")
		}
		if err := c.validateVectorPartitionSourceIdentityV1(manifest); err != nil {
			return err
		}
		opened, err := c.openVectorPartitionRouterManifestWithContextV1(ctx, manifest)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return closeVectorPartitionRouterViewAfterOpenErrorV1(opened.view, err)
		}
		key := vectorPartitionReaderPinKeyV1(c.db.Dir(), c.name, index, manifest.Generation)
		if key == "" {
			return closeVectorPartitionRouterViewAfterOpenErrorV1(opened.view, errors.New("collections: invalid vector partition router pin root"))
		}
		vectorPartitionReaderPinsV1.Lock()
		vectorPartitionReaderPinsV1.counts[key]++
		vectorPartitionReaderPinsV1.Unlock()
		opened.pin = &VectorPartitionReaderPinV1{key: key}
		router = opened
		return nil
	})
	status.OpenNanos = elapsedNanosVPR(started)
	if err != nil {
		status.FailureReason = err.Error()
		return nil, status, err
	}
	router.openNanos = status.OpenNanos
	status.Generation = router.manifest.Generation
	status.ModelDigest = router.modelDigest
	status.RouterBytes = router.manifest.RouterAsset.Bytes
	status.Representatives = uint64(len(router.model.Representatives))
	status.Partitions = uint64(router.model.Metrics.Partitions)
	status.MappedBytes = router.view.mappedBytes
	status.HeapCopyBytes = router.view.heapCopyBytes
	status.ActiveHandles = router.view.activeHandles
	return router, status, nil
}

func (c *Collection) openVectorPartitionRouterManifestV1(manifest VectorPartitionManifestV1) (*VectorPartitionRouterV1, error) {
	return c.openVectorPartitionRouterManifestWithContextV1(context.Background(), manifest)
}

func (c *Collection) openVectorPartitionRouterManifestWithContextV1(ctx context.Context, manifest VectorPartitionManifestV1) (*VectorPartitionRouterV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if manifest.RouterAsset.Bytes > internalrouter.DefaultRouterConfigV1().MaxRouterBytes {
		return nil, errors.New("collections: vector partition router asset exceeds the format allocation cap")
	}
	def, err := c.vectorPartitionRouterDefinitionV1(manifest.IndexName)
	if err != nil {
		return nil, err
	}
	if VectorIndexDefinitionDigestV1(def) != manifest.IndexDefinitionDigest {
		return nil, errors.New("collections: vector partition router index definition is stale")
	}
	asset := columnVectorIndexStateAssetSnapshot{
		Role:             columnVectorIndexStateAssetRoleHNSWSearchPack,
		AssetID:          manifest.RouterAsset.ID,
		LogicalType:      columnVectorIndexStateLogicalTypeSearchPack,
		PhysicalEncoding: columnVectorIndexStateEncodingHNSWSearchPackV1,
		RowCount:         len(manifest.Representatives),
		SourceSchemaHash: manifest.SourceSchemaHash,
		Ref:              manifest.RouterAsset.Ref,
		AssetBytes:       int64(manifest.RouterAsset.Bytes),
	}
	path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		return nil, err
	}
	manager := mappedresource.NewManager()
	handle, err := manager.AcquireFileRange(
		columnHNSWSearchPackMappedResourceKey(asset),
		mappedresource.Scope{
			Kind:       mappedresource.ScopePreparedSearch,
			ID:         "vector_partition_router_v1/" + manifest.IndexName + "/" + strconv.FormatUint(manifest.Generation, 10),
			Collection: c.name, Namespace: asset.Ref.Namespace, Generation: manifest.Generation,
			Reason: "vector partition router prepared view",
		},
		path,
		mappedresource.AcquireOptions{
			Reason: "vector partition router prepared view", ValidationMode: mappedresource.ValidationVerify,
			PreferMapped: true, AllowHeapCopy: true,
			ResourceRoot: c.db.ColumnAssetRootDir(), ResourcePath: path,
		},
	)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, handle.Release())
	}
	view, err := openVectorPartitionRouterPreparedViewWithContextV1(ctx, manager, handle, columnHNSWSearchPackDecodeOptions{
		ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{
			ManifestGeneration: manifest.SourceGeneration,
			ManifestChecksum:   manifest.SourceChecksum,
			SchemaHash:         manifest.SourceSchemaHash,
		},
		ExpectedMembershipDigest: vectorPartitionRouterFinalMembershipDigestV1(manifest),
	})
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	raw := handle.Bytes()
	for start := 0; start < len(raw); start += 1 << 20 {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, view.Close())
		}
		end := min(start+(1<<20), len(raw))
		if _, err := hash.Write(raw[start:end]); err != nil {
			return nil, errors.Join(err, view.Close())
		}
	}
	if hex.EncodeToString(hash.Sum(nil)) != manifest.RouterAsset.Checksum {
		return nil, errors.Join(errors.New("collections: vector partition router sha256 mismatch"), view.Close())
	}
	if view.Header.Dimensions != def.Dimensions || view.Header.Rows == 0 ||
		len(manifest.Representatives) != 0 && view.Header.Rows != len(manifest.Representatives) {
		return nil, errors.Join(errors.New("collections: vector partition router pack shape mismatch"), view.Close())
	}
	model, digest, viewToModel, err := decodeVectorPartitionRouterModelWithContextV1(ctx, view, manifest)
	if err != nil {
		return nil, errors.Join(err, view.Close())
	}
	if manifest.RouterAsset.Bytes > model.Config.MaxRouterBytes {
		return nil, errors.Join(errors.New("collections: vector partition router asset exceeds its persisted byte cap"), view.Close())
	}
	router := &VectorPartitionRouterV1{manifest: manifest, model: model, modelDigest: digest, view: view, viewToModel: viewToModel}
	router.scratch.New = func() any { return &columnVectorGraphNativeSearchScratch{} }
	return router, nil
}

func openVectorPartitionRouterPreparedViewWithContextV1(ctx context.Context, manager *mappedresource.Manager, handle *mappedresource.Handle, opts columnHNSWSearchPackDecodeOptions) (*columnHNSWSearchPackPreparedView, error) {
	view, err := newColumnHNSWSearchPackPreparedViewFromHandleWithContext(ctx, manager, handle, opts)
	if err != nil {
		return nil, errors.Join(err, handle.Release())
	}
	return view, nil
}

func closeVectorPartitionRouterViewAfterOpenErrorV1(view *columnHNSWSearchPackPreparedView, cause error) error {
	if view == nil {
		return cause
	}
	return errors.Join(cause, view.Close())
}

func decodeVectorPartitionRouterModelV1(view *columnHNSWSearchPackPreparedView, manifest VectorPartitionManifestV1) (internalrouter.RouterModelV1, string, []int, error) {
	return decodeVectorPartitionRouterModelWithContextV1(context.Background(), view, manifest)
}

func decodeVectorPartitionRouterModelWithContextV1(ctx context.Context, view *columnHNSWSearchPackPreparedView, manifest VectorPartitionManifestV1) (internalrouter.RouterModelV1, string, []int, error) {
	var model internalrouter.RouterModelV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return model, "", nil, err
	}
	if view == nil || len(view.DocumentIDOffsets) != view.Header.Rows+1 {
		return model, "", nil, errors.New("collections: vector partition router record offsets are unavailable")
	}
	nodes := make(map[uint32]internalrouter.RouterHierarchyNodeV1)
	var digest [sha256.Size]byte
	viewKeys := make([]uint64, view.Header.Rows)
	for ordinal := 0; ordinal < view.Header.Rows; ordinal++ {
		if ordinal&255 == 0 {
			if err := ctx.Err(); err != nil {
				return model, "", nil, err
			}
		}
		start, end := view.DocumentIDOffsets[ordinal], view.DocumentIDOffsets[ordinal+1]
		if end < start || end > uint64(len(view.DocumentIDBytes)) {
			return model, "", nil, errors.New("collections: vector partition router record range is invalid")
		}
		record, err := decodeVectorPartitionRouterRecordV1(view.DocumentIDBytes[start:end])
		if err != nil {
			return model, "", nil, err
		}
		if record.RouterGeneration != manifest.Generation {
			return model, "", nil, errors.New("collections: vector partition router generation mismatch")
		}
		if int(record.HNSWM) != view.Header.M || int(record.HNSWEfConstruction) != view.Header.EfConstruction || int(record.HNSWEfSearch) != view.Header.EfSearch {
			return model, "", nil, errors.New("collections: vector partition router HNSW build metadata mismatch")
		}
		if ordinal == 0 {
			digest = record.ModelDigest
			model = internalrouter.RouterModelV1{
				Format: "treedb_vector_partition_router_v1",
				Config: record.Config, Dimensions: view.Header.Dimensions, Metrics: record.Metrics,
			}
		} else if record.ModelDigest != digest || record.Config != model.Config || record.Metrics != model.Metrics {
			return model, "", nil, errors.New("collections: vector partition router build metadata is inconsistent")
		}
		viewKeys[ordinal] = uint64(record.PartitionID)<<32 | uint64(record.LeafNodeID)
		path := make([]uint32, len(record.Path))
		for pathOrdinal, pathNode := range record.Path {
			path[pathOrdinal] = pathNode.NodeID
			node := internalrouter.RouterHierarchyNodeV1{
				NodeID: pathNode.NodeID, PartitionID: record.PartitionID,
				Depth: uint16(pathOrdinal), MemberCount: pathNode.MemberCount,
				Leaf: pathOrdinal == len(record.Path)-1,
			}
			if pathOrdinal > 0 {
				node.ParentNodeID = record.Path[pathOrdinal-1].NodeID
			}
			if current, exists := nodes[node.NodeID]; exists {
				if current.NodeID != node.NodeID || current.ParentNodeID != node.ParentNodeID ||
					current.PartitionID != node.PartitionID || current.Depth != node.Depth ||
					current.MemberCount != node.MemberCount {
					return model, "", nil, errors.New("collections: vector partition router hierarchy is inconsistent")
				}
				if !node.Leaf {
					current.Leaf = false
					nodes[node.NodeID] = current
				}
			} else {
				nodes[node.NodeID] = node
			}
		}
		base := ordinal * view.Header.VectorStride
		values := append([]float32(nil), view.NormalizedVectors[base:base+view.Header.Dimensions]...)
		model.Representatives = append(model.Representatives, internalrouter.RouterRepresentativeV1{
			PartitionID: record.PartitionID, SourceOrdinal: record.SourceOrdinal,
			LeafNodeID: record.LeafNodeID, Depth: record.Depth, MemberCount: record.MemberCount,
			Path: path, Values: values,
		})
	}
	nodeOrdinal := 0
	for _, node := range nodes {
		if nodeOrdinal&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return model, "", nil, err
			}
		}
		model.Nodes = append(model.Nodes, node)
		nodeOrdinal++
	}
	if err := ctx.Err(); err != nil {
		return model, "", nil, err
	}
	if err := sortDecodedVectorPartitionRouterModelWithContextV1(ctx, &model); err != nil {
		return model, "", nil, err
	}
	gotDigest, err := internalrouter.RouterDigestWithContextV1(ctx, model)
	if err != nil {
		return model, "", nil, err
	}
	wantDigest := hex.EncodeToString(digest[:])
	if gotDigest != wantDigest {
		return model, "", nil, errors.New("collections: vector partition router model digest mismatch")
	}
	expectedRepresentatives := append([]VectorPartitionMembershipV1(nil), manifest.Representatives...)
	actualRepresentatives := make([]VectorPartitionMembershipV1, len(model.Representatives))
	for i, representative := range model.Representatives {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return model, "", nil, err
			}
		}
		actualRepresentatives[i] = VectorPartitionMembershipV1{VectorOrdinal: representative.SourceOrdinal, PartitionID: representative.PartitionID}
	}
	if err := sortVectorPartitionSliceWithContextV1(ctx, actualRepresentatives, func(a, b VectorPartitionMembershipV1) bool {
		if a.VectorOrdinal != b.VectorOrdinal {
			return a.VectorOrdinal < b.VectorOrdinal
		}
		return a.PartitionID < b.PartitionID
	}); err != nil {
		return model, "", nil, err
	}
	if len(expectedRepresentatives) == 0 || !equalVectorPartitionMembershipsV1(actualRepresentatives, expectedRepresentatives) {
		return model, "", nil, errors.New("collections: vector partition router representative manifest mismatch")
	}
	modelOrdinal := make(map[uint64]int, len(model.Representatives))
	for ordinal, representative := range model.Representatives {
		if ordinal&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return model, "", nil, err
			}
		}
		modelOrdinal[uint64(representative.PartitionID)<<32|uint64(representative.LeafNodeID)] = ordinal
	}
	viewToModel := make([]int, len(viewKeys))
	for ordinal, key := range viewKeys {
		if ordinal&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return model, "", nil, err
			}
		}
		mapped, exists := modelOrdinal[key]
		if !exists {
			return model, "", nil, errors.New("collections: vector partition router view/model mapping is incomplete")
		}
		viewToModel[ordinal] = mapped
	}
	return model, gotDigest, viewToModel, nil
}

func sortDecodedVectorPartitionRouterModelWithContextV1(ctx context.Context, model *internalrouter.RouterModelV1) error {
	if model == nil {
		return errors.New("collections: nil vector partition router model")
	}
	if err := sortVectorPartitionSliceWithContextV1(ctx, model.Nodes, func(a, b internalrouter.RouterHierarchyNodeV1) bool {
		return a.NodeID < b.NodeID
	}); err != nil {
		return err
	}
	return sortVectorPartitionSliceWithContextV1(ctx, model.Representatives, func(a, b internalrouter.RouterRepresentativeV1) bool {
		if a.PartitionID != b.PartitionID {
			return a.PartitionID < b.PartitionID
		}
		return a.LeafNodeID < b.LeafNodeID
	})
}

func equalVectorPartitionMembershipsV1(left, right []VectorPartitionMembershipV1) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (r *VectorPartitionRouterV1) Search(query []float32, opts VectorPartitionRouterSearchOptionsV1) (VectorPartitionRouterSearchResultV1, error) {
	return r.SearchWithContextV1(context.Background(), query, opts)
}

// SearchWithContextV1 preserves M4 ordering while making representative scans
// and ranking cancellable for M6 deadlines.
func (r *VectorPartitionRouterV1) SearchWithContextV1(ctx context.Context, query []float32, opts VectorPartitionRouterSearchOptionsV1) (result VectorPartitionRouterSearchResultV1, resultErr error) {
	started := time.Now()
	if ctx == nil {
		ctx = context.Background()
	}
	result.Status.Mode = opts.Mode
	result.Status.CandidateBudget = uint64(max(opts.CandidateBudget, 0))
	result.Status.PartitionProbes = uint64(max(opts.PartitionProbes, 0))
	fail := func(err error) (VectorPartitionRouterSearchResultV1, error) {
		result.Status.SearchNanos = elapsedNanosVPR(started)
		if err != nil {
			result.Status.FailureReason = err.Error()
		}
		if r != nil {
			r.searchFailures.Add(1)
		}
		return result, err
	}
	if r == nil {
		return fail(errors.New("collections: vector partition router is nil"))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	r.closeMu.RLock()
	defer r.closeMu.RUnlock()
	if r.closed.Load() || r.view == nil {
		return fail(errors.New("collections: vector partition router is closed"))
	}
	r.searches.Add(1)
	if opts.Mode == "" {
		opts.Mode = VectorPartitionRouterModeApproxV1
		result.Status.Mode = opts.Mode
	}
	if opts.PartitionProbes < 1 {
		return fail(errors.New("collections: vector partition router probes must be positive"))
	}
	if opts.CandidateBudget < 1 {
		return fail(errors.New("collections: vector partition router candidate budget must be positive"))
	}
	normalized, err := normalizeVectorPartitionRouterQueryV1(query, r.model.Dimensions)
	if err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	var candidates []vectorPartitionRouterCandidateV1
	switch opts.Mode {
	case VectorPartitionRouterModeExactV1:
		if opts.CandidateBudget < len(r.model.Representatives) {
			return fail(fmt.Errorf("collections: exact vector partition router candidate budget=%d below representative count=%d", opts.CandidateBudget, len(r.model.Representatives)))
		}
		candidates = make([]vectorPartitionRouterCandidateV1, len(r.model.Representatives))
		for ordinal, representative := range r.model.Representatives {
			if ordinal&255 == 0 {
				if err := ctx.Err(); err != nil {
					return fail(err)
				}
			}
			candidates[ordinal] = vectorPartitionRouterCandidateV1{
				ordinal: ordinal, score: cosineDotVectorPartitionRouterV1(normalized, representative.Values),
			}
		}
		result.Status.Candidates = uint64(len(candidates))
	case VectorPartitionRouterModeApproxV1:
		if opts.CandidateBudget > len(r.model.Representatives) {
			return fail(fmt.Errorf("collections: approximate vector partition router candidate budget=%d outside [1,%d]", opts.CandidateBudget, len(r.model.Representatives)))
		}
		scratch := r.scratch.Get().(*columnVectorGraphNativeSearchScratch)
		defer r.scratch.Put(scratch)
		native, stats, err := r.view.searchCosineWithContext(ctx, query, columnVectorGraphNativeSearchOptions{
			TopK: opts.CandidateBudget, EfSearch: opts.CandidateBudget,
			CandidateLimit: opts.CandidateBudget,
			// Router search consumes only representative ordinals and scores.
			// Keep the hot path off document-ID and row-ref materialization.
			OmitResultMaterialization: true,
		}, scratch)
		if err != nil {
			return fail(err)
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		for _, candidate := range native {
			if candidate.Ordinal < 0 || candidate.Ordinal >= len(r.viewToModel) {
				return fail(errors.New("collections: vector partition router native ordinal is invalid"))
			}
			candidates = append(candidates, vectorPartitionRouterCandidateV1{ordinal: r.viewToModel[candidate.Ordinal], score: candidate.Score})
		}
		result.Status.Candidates = stats.Candidates
		result.Status.Edges = stats.Edges
	default:
		return fail(fmt.Errorf("collections: unsupported vector partition router mode %q", opts.Mode))
	}
	result.Partitions, err = rankVectorPartitionRouterCandidatesWithContextV1(ctx, r.model.Representatives, candidates, opts.PartitionProbes)
	if err != nil {
		return fail(err)
	}
	result.Status.Selected = uint64(len(result.Partitions))
	if len(result.Partitions) > 0 {
		result.Status.WinningDistance = result.Partitions[0].Distance
	}
	result.Status.SearchNanos = elapsedNanosVPR(started)
	r.candidates.Add(result.Status.Candidates)
	r.edges.Add(result.Status.Edges)
	r.selected.Add(result.Status.Selected)
	return result, nil
}

type vectorPartitionRouterCandidateV1 struct {
	ordinal int
	score   float64
}

func rankVectorPartitionRouterCandidatesV1(representatives []internalrouter.RouterRepresentativeV1, candidates []vectorPartitionRouterCandidateV1, probes int) ([]VectorPartitionRouterPartitionScoreV1, error) {
	return rankVectorPartitionRouterCandidatesWithContextV1(context.Background(), representatives, candidates, probes)
}

func rankVectorPartitionRouterCandidatesWithContextV1(ctx context.Context, representatives []internalrouter.RouterRepresentativeV1, candidates []vectorPartitionRouterCandidateV1, probes int) ([]VectorPartitionRouterPartitionScoreV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	best := make(map[uint32]VectorPartitionRouterPartitionScoreV1)
	for i, candidate := range candidates {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if candidate.ordinal < 0 || candidate.ordinal >= len(representatives) || math.IsNaN(candidate.score) || math.IsInf(candidate.score, 0) {
			return nil, errors.New("collections: vector partition router candidate is invalid")
		}
		representative := representatives[candidate.ordinal]
		distance := 1 - candidate.score
		if distance < 0 && distance > -1e-6 {
			distance = 0
		}
		current, exists := best[representative.PartitionID]
		if !exists || distance < current.Distance ||
			distance == current.Distance && candidate.ordinal < current.WinningRepresentative {
			best[representative.PartitionID] = VectorPartitionRouterPartitionScoreV1{
				PartitionID: representative.PartitionID, Distance: distance,
				WinningRepresentative: candidate.ordinal, WinningSourceOrdinal: representative.SourceOrdinal,
			}
		}
	}
	if len(best) < probes {
		return nil, fmt.Errorf("%w: candidate budget reached only %d unique partitions for %d probes", ErrVectorPartitionRouterCandidateCoverageV1, len(best), probes)
	}
	result := make([]VectorPartitionRouterPartitionScoreV1, 0, len(best))
	for _, score := range best {
		result = append(result, score)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Distance != result[j].Distance {
			return result[i].Distance < result[j].Distance
		}
		return result[i].PartitionID < result[j].PartitionID
	})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result[:probes], nil
}

func normalizeVectorPartitionRouterQueryV1(query []float32, dimensions int) ([]float32, error) {
	if len(query) != dimensions {
		return nil, fmt.Errorf("collections: vector partition router query dimensions=%d want %d", len(query), dimensions)
	}
	invNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		return nil, fmt.Errorf("collections: vector partition router query: %w", err)
	}
	normalized := make([]float32, len(query))
	for i := range query {
		normalized[i] = query[i] * invNorm
	}
	return normalized, nil
}

func cosineDotVectorPartitionRouterV1(left, right []float32) float64 {
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot
}

func (r *VectorPartitionRouterV1) Status() VectorPartitionRouterRuntimeStatusV1 {
	if r == nil {
		return VectorPartitionRouterRuntimeStatusV1{}
	}
	status := VectorPartitionRouterRuntimeStatusV1{
		Manifest: r.manifest, Config: r.model.Config, ModelDigest: r.modelDigest,
		Representatives: uint64(len(r.model.Representatives)),
		Partitions:      uint64(r.model.Metrics.Partitions),
		RouterBytes:     r.manifest.RouterAsset.Bytes, OpenNanos: r.openNanos,
		Searches: r.searches.Load(), SearchFailures: r.searchFailures.Load(),
		Candidates: r.candidates.Load(), Edges: r.edges.Load(), Selected: r.selected.Load(),
	}
	r.closeMu.RLock()
	if !r.closed.Load() && r.view != nil {
		status.ActiveHandles = r.view.activeHandles
	}
	r.closeMu.RUnlock()
	return status
}

func (r *VectorPartitionRouterV1) Close() error {
	if r == nil {
		return nil
	}
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed.Swap(true) {
		return nil
	}
	var err error
	if r.view != nil {
		err = r.view.Close()
		r.view = nil
	}
	if r.pin != nil {
		r.pin.Release()
		r.pin = nil
	}
	return err
}

func encodeVectorPartitionRouterRecordV1(record vectorPartitionRouterRecordV1) ([]byte, error) {
	if record.RouterGeneration == 0 || record.PartitionID > math.MaxInt32 || record.MemberCount == 0 ||
		len(record.Path) == 0 || len(record.Path) > 65 || int(record.Depth)+1 != len(record.Path) ||
		record.Path[len(record.Path)-1].NodeID != record.LeafNodeID {
		return nil, errors.New("collections: invalid vector partition router record")
	}
	if err := internalrouter.ValidateRouterConfigV1(record.Config); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	binary.Write(&b, binary.LittleEndian, vectorPartitionRouterRecordMagicV1)
	binary.Write(&b, binary.LittleEndian, vectorPartitionRouterRecordVersionV1)
	binary.Write(&b, binary.LittleEndian, uint16(0))
	binary.Write(&b, binary.LittleEndian, record.RouterGeneration)
	b.Write(record.ModelDigest[:])
	binary.Write(&b, binary.LittleEndian, record.PartitionID)
	binary.Write(&b, binary.LittleEndian, record.SourceOrdinal)
	binary.Write(&b, binary.LittleEndian, record.LeafNodeID)
	binary.Write(&b, binary.LittleEndian, record.Depth)
	binary.Write(&b, binary.LittleEndian, uint16(len(record.Path)))
	binary.Write(&b, binary.LittleEndian, record.MemberCount)
	writeVectorPartitionRouterConfigV1(&b, record.Config)
	writeVectorPartitionRouterMetricsV1(&b, record.Metrics)
	binary.Write(&b, binary.LittleEndian, record.HNSWM)
	binary.Write(&b, binary.LittleEndian, record.HNSWEfConstruction)
	binary.Write(&b, binary.LittleEndian, record.HNSWEfSearch)
	for _, node := range record.Path {
		binary.Write(&b, binary.LittleEndian, node.NodeID)
		binary.Write(&b, binary.LittleEndian, node.MemberCount)
	}
	return b.Bytes(), nil
}

func decodeVectorPartitionRouterRecordV1(raw []byte) (record vectorPartitionRouterRecordV1, err error) {
	reader := bytes.NewReader(raw)
	var magic uint32
	var version, reserved, pathCount uint16
	read := func(value any) bool {
		if err != nil {
			return false
		}
		err = binary.Read(reader, binary.LittleEndian, value)
		return err == nil
	}
	read(&magic)
	read(&version)
	read(&reserved)
	read(&record.RouterGeneration)
	if err == nil {
		_, err = reader.Read(record.ModelDigest[:])
	}
	read(&record.PartitionID)
	read(&record.SourceOrdinal)
	read(&record.LeafNodeID)
	read(&record.Depth)
	read(&pathCount)
	read(&record.MemberCount)
	if err != nil || magic != vectorPartitionRouterRecordMagicV1 || version != vectorPartitionRouterRecordVersionV1 || reserved != 0 ||
		pathCount == 0 || pathCount > 65 || int(record.Depth)+1 != int(pathCount) {
		return record, errors.New("collections: malformed vector partition router record")
	}
	record.Config, err = readVectorPartitionRouterConfigV1(reader)
	if err != nil {
		return record, err
	}
	record.Metrics, err = readVectorPartitionRouterMetricsV1(reader)
	if err != nil {
		return record, err
	}
	read(&record.HNSWM)
	read(&record.HNSWEfConstruction)
	read(&record.HNSWEfSearch)
	if err != nil || record.HNSWM == 0 || record.HNSWEfConstruction == 0 || record.HNSWEfSearch == 0 {
		return record, errors.New("collections: malformed vector partition router HNSW metadata")
	}
	record.Path = make([]vectorPartitionRouterPathNodeV1, pathCount)
	for i := range record.Path {
		if !read(&record.Path[i].NodeID) || !read(&record.Path[i].MemberCount) {
			return record, errors.New("collections: truncated vector partition router hierarchy path")
		}
	}
	if reader.Len() != 0 || record.RouterGeneration == 0 || record.MemberCount == 0 ||
		record.Path[len(record.Path)-1].NodeID != record.LeafNodeID ||
		record.Path[len(record.Path)-1].MemberCount != record.MemberCount {
		return record, errors.New("collections: invalid vector partition router record")
	}
	if err := internalrouter.ValidateRouterConfigV1(record.Config); err != nil {
		return record, err
	}
	return record, nil
}

func writeVectorPartitionRouterConfigV1(b *bytes.Buffer, cfg internalrouter.RouterConfigV1) {
	binary.Write(b, binary.LittleEndian, cfg.Seed)
	for _, value := range []int{cfg.BranchFactor, cfg.LeafSize, cfg.RepresentativesPerPartition, cfg.MaxDepth, cfg.MaxIterations, cfg.MaxVectors, cfg.MaxDimensions, cfg.MaxRepresentatives} {
		binary.Write(b, binary.LittleEndian, uint32(value))
	}
	binary.Write(b, binary.LittleEndian, cfg.MaxScalarWork)
	binary.Write(b, binary.LittleEndian, cfg.MaxRouterBytes)
}

func readVectorPartitionRouterConfigV1(reader *bytes.Reader) (internalrouter.RouterConfigV1, error) {
	var seed int64
	values := make([]uint32, 8)
	var maxWork int64
	if err := binary.Read(reader, binary.LittleEndian, &seed); err != nil {
		return internalrouter.RouterConfigV1{}, err
	}
	for i := range values {
		if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
			return internalrouter.RouterConfigV1{}, err
		}
	}
	if err := binary.Read(reader, binary.LittleEndian, &maxWork); err != nil {
		return internalrouter.RouterConfigV1{}, err
	}
	var maxRouterBytes uint64
	if err := binary.Read(reader, binary.LittleEndian, &maxRouterBytes); err != nil {
		return internalrouter.RouterConfigV1{}, err
	}
	return internalrouter.RouterConfigV1{
		Seed: seed, BranchFactor: int(values[0]), LeafSize: int(values[1]),
		RepresentativesPerPartition: int(values[2]), MaxDepth: int(values[3]),
		MaxIterations: int(values[4]), MaxVectors: int(values[5]),
		MaxDimensions: int(values[6]), MaxRepresentatives: int(values[7]), MaxScalarWork: maxWork,
		MaxRouterBytes: maxRouterBytes,
	}, nil
}

func writeVectorPartitionRouterMetricsV1(b *bytes.Buffer, metrics internalrouter.RouterBuildMetricsV1) {
	for _, value := range []int{
		metrics.Partitions, metrics.Vectors, metrics.Representatives, metrics.HierarchyNodes,
		metrics.LloydIterations, metrics.EmptyRepairs, metrics.StoppedLeafSize,
		metrics.StoppedMaxDepth, metrics.StoppedNoSplit,
	} {
		binary.Write(b, binary.LittleEndian, uint64(value))
	}
}

func readVectorPartitionRouterMetricsV1(reader *bytes.Reader) (internalrouter.RouterBuildMetricsV1, error) {
	values := make([]uint64, 9)
	for i := range values {
		if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
			return internalrouter.RouterBuildMetricsV1{}, err
		}
		if values[i] > math.MaxInt {
			return internalrouter.RouterBuildMetricsV1{}, errors.New("collections: vector partition router metric overflows int")
		}
	}
	return internalrouter.RouterBuildMetricsV1{
		Partitions: int(values[0]), Vectors: int(values[1]), Representatives: int(values[2]),
		HierarchyNodes: int(values[3]), LloydIterations: int(values[4]), EmptyRepairs: int(values[5]),
		StoppedLeafSize: int(values[6]), StoppedMaxDepth: int(values[7]), StoppedNoSplit: int(values[8]),
	}, nil
}

func elapsedNanosVPR(started time.Time) uint64 {
	elapsed := time.Since(started).Nanoseconds()
	if elapsed <= 0 {
		return 1
	}
	return uint64(elapsed)
}
