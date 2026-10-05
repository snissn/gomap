package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

const VectorSourcePopulationEncodingV1 = "id-le32-fp32-le-v1"

// These are refusal ceilings, not a capacity or latency promise. Asset images
// and manifest preparation have separate ceilings before their decoders run.
const vectorPopulationMaxPartBytesV1 = 64 << 20
const vectorPopulationMaxManifestBytesV1 = 8 << 20
const vectorPopulationMaxManifestRecordsV1 = 4096

type VectorSourcePopulationLimitsV1 struct {
	MaxRows, MaxIDBytes, MaxSourceRecordBytes, MaxTotalBytes, MaxInspected uint64
}

type VectorSourcePopulationExpectationV1 struct {
	Rows       uint64
	Dimensions int
	SHA256     string
	Limits     VectorSourcePopulationLimitsV1
}

// SourceRecordBytes counts retained primary payloads and, when stripped from
// those payloads, the fixed-D vector projection. It is not reconstructed full
// document size. TotalBytes also charges metadata, loaded assets and hash input.
type VectorSourcePopulationProofV1 struct {
	Rows                                                              uint64
	Dimensions                                                        int
	Encoding, SHA256                                                  string
	SourceRecordBytes, HashedBytes, AssetBytes, TotalBytes, Inspected uint64
}

func ValidateVectorSourcePopulationExpectationV1(p VectorSourcePopulationExpectationV1) error {
	l := p.Limits
	if len(p.SHA256) != 64 {
		return errors.New("collections: invalid source population digest length")
	}
	digest, err := hex.DecodeString(p.SHA256)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != p.SHA256 || p.Dimensions < 1 || p.Dimensions > 4096 || l.MaxRows < 1 || l.MaxRows > 65536 || p.Rows > l.MaxRows || l.MaxIDBytes < 1 || l.MaxIDBytes > 65536 || l.MaxSourceRecordBytes < uint64(p.Dimensions)*4 || l.MaxSourceRecordBytes > 1<<20 || l.MaxTotalBytes < 1 || l.MaxTotalBytes > 512<<20 || l.MaxInspected < 1 || l.MaxInspected > 1<<20 {
		return errors.New("collections: invalid bounded source population expectation")
	}
	return nil
}

// ProveVectorSourcePopulationV1 observes all CURRENT primary+overlay winners,
// not the immutable graph's original source. Its prepared owner supplies the
// publication exclusion; the caller supplies current-FSM/quorum/WAL fences.
// This proves source vectors only, never reverse enumeration of the live graph.
func (owner *CommandWALAdmittedCollection) ProveVectorSourcePopulationV1(ctx context.Context, manifest VectorPartitionManifestV1, expected VectorSourcePopulationExpectationV1) (out VectorSourcePopulationProofV1, err error) {
	defer func() {
		if err != nil {
			out = VectorSourcePopulationProofV1{}
		}
	}()
	if err = owner.validate(); err != nil {
		return out, err
	}
	if err = ctx.Err(); err != nil {
		return out, err
	}
	if err = ValidateVectorSourcePopulationExpectationV1(expected); err != nil {
		return out, err
	}
	c := owner.collection
	if manifest.Collection != c.collectionName() {
		return out, ErrVectorIndexPartitionLiveMismatchV1
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return out, ErrVectorIndexPartitionLiveUnavailableV1
	}
	defer func() { err = errors.Join(err, snap.Close()) }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return out, err
	}
	if catalog == nil || !VectorPartitionLiveDocumentProofSupportedV1(catalog.meta) {
		return out, ErrVectorIndexPartitionLiveUnavailableV1
	}
	def, found := findVectorIndex(catalog.meta.VectorIndexes, manifest.IndexName)
	if !found || VectorIndexDefinitionDigestV1(def) != manifest.IndexDefinitionDigest || def.Dimensions != expected.Dimensions {
		return out, ErrVectorIndexPartitionLiveMismatchV1
	}
	path, err := parseVectorFieldPath(def.Field)
	if err != nil {
		return out, err
	}
	out.Dimensions, out.Encoding = def.Dimensions, VectorSourcePopulationEncodingV1
	l := expected.Limits
	charge := func(n uint64) error {
		if n > l.MaxTotalBytes-out.TotalBytes {
			return errors.New("collections: source population byte limit")
		}
		out.TotalBytes += n
		return ctx.Err()
	}
	inspect := func(n uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if n > l.MaxInspected-out.Inspected {
			return errors.New("collections: source population inspection limit")
		}
		out.Inspected += n
		return nil
	}
	var projection *vectorPopulationColumnProjectionV1
	needsProjection := columnStoreNeedsRetainedPayloadTransform(catalog.meta)
	defer func() {
		if projection != nil {
			err = errors.Join(err, projection.close())
		}
	}()
	// Include tombstones so physical inspection is bounded even when no live row
	// is returned. The existing merge also charges superseded overlay entries.
	inspectionError := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if out.Inspected >= l.MaxInspected {
			return errors.New("collections: source population inspection limit")
		}
		return nil
	}
	if err = inspectionError(); err != nil {
		return out, err
	}
	it, err := collectionIteratorAtCatalogRootWithInspectionError(snap, catalog, catalog.primaryRootName, nil, nil, true, int(l.MaxInspected-out.Inspected), func(n int) { out.Inspected += uint64(n) }, inspectionError)
	if err != nil {
		return out, err
	}
	if it == nil {
		if expected.Rows != 0 || expected.SHA256 != hex.EncodeToString(sha256.New().Sum(nil)) {
			return out, ErrVectorIndexPartitionLiveMismatchV1
		}
		out.SHA256 = expected.SHA256
		return out, ctx.Err()
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	h := sha256.New()
	var word [4]byte
	var previous []byte
	vector := make([]float32, 0, def.Dimensions)
	vectorBytes := make([]byte, 4*def.Dimensions)
	for it.Valid() {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		id := it.UnsafeKey()
		if len(id) == 0 || uint64(len(id)) > l.MaxIDBytes || out.Rows >= l.MaxRows || out.Rows >= expected.Rows || previous != nil && bytes.Compare(previous, id) >= 0 {
			return out, ErrVectorIndexPartitionLiveMismatchV1
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && uint64(ptr.Length) > l.MaxSourceRecordBytes {
			return out, errors.New("collections: source population retained pointer limit")
		}
		document := it.UnsafeValue()
		if uint64(len(document)) > l.MaxSourceRecordBytes {
			return out, errors.New("collections: source population record limit")
		}
		recordBytes := uint64(len(document))
		if !needsProjection {
			var present bool
			vector, present, err = vectorFromJSONFieldAppend(document, path, vector, def.Dimensions)
			if err != nil || !present {
				return out, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
			}
		} else {
			if uint64(def.Dimensions)*4 > l.MaxSourceRecordBytes-recordBytes {
				return out, errors.New("collections: source population projected record limit")
			}
			recordBytes += uint64(def.Dimensions) * 4
			if projection == nil {
				projection, err = c.prepareVectorPopulationProjectionV1(ctx, snap, catalog, def, charge, inspect)
				if err != nil {
					return out, err
				}
			}
			vector, err = projection.vector(ctx, id, vector, charge, inspect, &out.AssetBytes)
			if err != nil {
				return out, err
			}
		}
		if err = validateIngestVectors([][]float32{vector}, def); err != nil {
			return out, err
		}
		hashed := uint64(4+len(id)) + uint64(def.Dimensions)*4
		if err = charge(recordBytes + hashed); err != nil {
			return out, err
		}
		out.SourceRecordBytes += recordBytes
		out.HashedBytes += hashed
		binary.LittleEndian.PutUint32(word[:], uint32(len(id)))
		_, _ = h.Write(word[:])
		_, _ = h.Write(id)
		for d, v := range vector {
			binary.LittleEndian.PutUint32(vectorBytes[4*d:], math.Float32bits(v))
		}
		_, _ = h.Write(vectorBytes)
		previous = append(previous[:0], id...)
		out.Rows++
		it.Next()
	}
	if err = it.Error(); err != nil {
		return out, err
	}
	out.SHA256 = hex.EncodeToString(h.Sum(nil))
	if out.Rows != expected.Rows || out.SHA256 != expected.SHA256 {
		return out, ErrVectorIndexPartitionLiveMismatchV1
	}
	return out, ctx.Err()
}

// One generation's borrowed raw FP32 blocks and scalar row coordinates suffice;
// no collection-sized vector array or cache of decoded generations is retained.
type vectorPopulationColumnProjectionV1 struct {
	view         CollectionReadView
	locatorRoots []uint64
	cfg          ColumnStoreConfig
	fields       []TypedStorageField
	selected     []bool
	field        int
	root         uint64
	prepared     *columnPhysicalScanSnapshotView
	read         columnPhysicalAssetReadCache
	generation   uint64
	decoded      typedColumnPartDecodedValues
}

func (p *vectorPopulationColumnProjectionV1) close() error {
	p.decoded = typedColumnPartDecodedValues{}
	return p.read.close()
}

func (c *Collection) prepareVectorPopulationProjectionV1(ctx context.Context, snap *backenddb.Snapshot, catalog *collectionCatalog, def VectorIndexDefinition, charge func(uint64) error, inspect func(uint64) error) (*vectorPopulationColumnProjectionV1, error) {
	if catalog.meta.Options.ColumnStore == nil {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	cfg := *catalog.meta.Options.ColumnStore
	p := &vectorPopulationColumnProjectionV1{view: CollectionReadView{collection: c, snapshot: snap, catalog: catalog}, cfg: cfg, fields: columnStoreTypedColumnPartFields(cfg), field: -1, root: catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))}
	p.locatorRoots = catalog.rootStack(collectionColumnRowLocatorRootName(catalog.meta.Name))
	p.selected = make([]bool, len(p.fields))
	for i, f := range p.fields {
		if f.Path == def.Field && f.ValueType == ColumnStoreValueFloat32Vector && !f.Nullable && f.VectorDims == def.Dimensions {
			p.field = i
			p.selected[i] = true
		}
	}
	if p.field < 0 || cfg.AssetManager == nil || p.root == 0 {
		return nil, ErrVectorIndexPartitionLiveUnavailableV1
	}
	if cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnSourceDirectoryFormatV2 {
		// Bound physical metadata BEFORE the existing prepared metadata loader
		// copies/decodes it. No asset is opened by this preparation.
		it, err := snap.IteratorAtRootWithOptions(p.root, nil, nil, backenddb.IteratorOptions{IncludeTombstones: true})
		if err != nil {
			return nil, err
		}
		var metadata uint64
		var records int
		for it.Valid() {
			// Reserve both this bounded pass and the existing metadata loader's
			// subsequent physical pass before it copies/decodes the records.
			if err = inspect(2); err != nil {
				break
			}
			value, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				err = errors.New("collections: source population manifest must be inline")
				break
			}
			records++
			n := uint64(len(it.UnsafeKey())) + uint64(len(value))
			if records > vectorPopulationMaxManifestRecordsV1 || n > vectorPopulationMaxManifestBytesV1-metadata {
				err = errors.New("collections: source population manifest limit")
				break
			}
			metadata += n
			if err = charge(n); err != nil {
				break
			}
			it.Next()
		}
		err = errors.Join(err, it.Error(), it.Close())
		if err != nil {
			return nil, err
		}
		// The prepared loader also validates identity with one point probe.
		if err := inspect(1); err != nil {
			return nil, err
		}
		physical, err := p.view.materializerColumnSnapshotView(cfg)
		if err != nil {
			return nil, err
		}
		p.prepared = &physical
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	read, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		return nil, err
	}
	p.read = read
	return p, nil
}

func (p *vectorPopulationColumnProjectionV1) vector(ctx context.Context, id []byte, dst []float32, charge func(uint64) error, inspect func(uint64) error, assetBytes *uint64) ([]float32, error) {
	var ref DocumentRowRef
	for _, root := range p.locatorRoots {
		if err := inspect(1); err != nil {
			return nil, err
		}
		entry, err := p.view.snapshot.GetEntryAtRoot(root, id)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 || len(entry.Value) > columnPrimaryRowLocatorValueSize+32 {
			return nil, ErrVectorIndexPartitionLiveMismatchV1
		}
		if err = charge(uint64(len(entry.Value))); err != nil {
			return nil, err
		}
		// CRL2 metadata-only rows preserve the CURRENT canonical vector through
		// their scoring coordinates. They are not immutable graph-source IDs.
		ref, err = decodeColumnScoringRowLocatorBorrowedID(id, entry.Value)
		if err != nil {
			return nil, err
		}
		break
	}
	if ref.Generation == 0 {
		return nil, ErrVectorIndexPartitionLiveMismatchV1
	}
	if p.generation != ref.Generation {
		p.decoded = typedColumnPartDecodedValues{}
		if p.prepared == nil {
			// Validate/charge the two inline records on this exact snapshot
			// before the existing directory helper decodes their variable fields.
			var metadata uint64
			for _, key := range [][]byte{newColumnManifestIdentityRecordKey(), columnManifestPartRecordKey(ref.Generation, typedColumnPartAssetPartID)} {
				if err := inspect(1); err != nil {
					return nil, err
				}
				entry, err := p.view.snapshot.GetEntryAtRoot(p.root, key)
				if err != nil {
					return nil, err
				}
				n := uint64(len(key)) + uint64(len(entry.Value))
				if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 || n > vectorPopulationMaxManifestBytesV1-metadata {
					return nil, errors.New("collections: source population directory record limit")
				}
				metadata += n
				if err = charge(n); err != nil {
					return nil, err
				}
			}
			// The directory helper reads its identity and this part reference.
			// Reserve both point probes before calling either decoder.
			if err := inspect(2); err != nil {
				return nil, err
			}
		}
		cache := typedColumnPartReconstructionCache{Prepared: p.prepared}
		asset, found, err := p.view.collection.typedColumnPartRefForGenerationWithCache(p.view.snapshot, p.root, p.cfg, ref.Generation, &cache)
		if err != nil || !found {
			return nil, errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
		}
		if asset.Ref.Length <= 0 || asset.Ref.Length > vectorPopulationMaxPartBytesV1 || asset.Rows <= 0 || asset.Rows > 65536 {
			return nil, errors.New("collections: source population part limit")
		}
		if err = inspect(uint64(asset.Rows)); err != nil {
			return nil, err
		}
		if err = charge(uint64(asset.Ref.Length)); err != nil {
			return nil, err
		}
		*assetBytes += uint64(asset.Ref.Length)
		raw, err := p.read.read(asset.Ref, nil)
		if err != nil {
			return nil, err
		}
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			return nil, err
		}
		if image.Rows != asset.Rows {
			return nil, ErrVectorIndexPartitionLiveMismatchV1
		}
		var decodedBytes uint64
		for _, section := range image.Sections {
			if section.RawBytes < 0 || uint64(section.RawBytes) > vectorPopulationMaxPartBytesV1-decodedBytes {
				return nil, errors.New("collections: source population decoded part limit")
			}
			decodedBytes += uint64(section.RawBytes)
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		part, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: p.fields, SchemaVersion: uint32(p.cfg.SchemaHash)}, image)
		if err != nil {
			return nil, err
		}
		if part.Part.Descriptor.RowCount != asset.Rows {
			return nil, ErrVectorIndexPartitionLiveMismatchV1
		}
		column := part.Columns[p.field]
		if column.Definition.FixedWidthElements != p.fields[p.field].VectorDims || column.Definition.Encoding != typedcolumn.EncodingRawFloat32Vector || column.Definition.Compression != typedcolumn.CompressionNone {
			return nil, ErrVectorIndexPartitionLiveUnavailableV1
		}
		// This path borrows the read cache until the NEXT generation load. Refuse
		// generic/compressed-vector reconstruction instead of expanding all rows.
		p.decoded, err = part.scanDecodedValuesSelectedForReconstruction(p.selected, true)
		if err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		p.generation = ref.Generation
	}
	d := p.decoded
	row := ref.RowIndex
	if row < 0 || row >= len(d.PrimaryIDs) || p.field >= len(d.RawFloat32Columns) {
		return nil, ErrVectorIndexPartitionLiveMismatchV1
	}
	if len(d.RowByPrimaryID) > 0 {
		row = d.RowByPrimaryID[row]
	} else if d.PrimaryIDs[row] != int64(row) {
		return nil, ErrVectorIndexPartitionLiveMismatchV1
	}
	return typedColumnPointFloat32VectorAppend(dst, d.RawFloat32Columns[p.field], row)
}
