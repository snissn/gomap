package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const vectorPartitionSearchAssetMagicV1 = "VPS1"
const vectorPartitionSearchAssetVersionV1 uint32 = 1
const vectorPartitionSearchAssetMaxBytesV1 int64 = 256 << 20

func vectorPartitionLocalAssetIDV1(partition uint32) string {
	return fmt.Sprintf("hnsw_search_pack_v1/partition/%d", partition)
}

// MaterializeVectorPartitionLocalSearchAssetsV1 uses the M1 column-asset
// authority. Callers install the returned descriptors in the generation M1
// manifest; publication then validates the exact ref, size, CRC and SHA-256.
func (c *Collection) MaterializeVectorPartitionLocalSearchAssetsV1(index string, manifest VectorPartitionManifestV1, fileID uint32, inputs []VectorPartitionSearchAssetV1) ([]VectorPartitionAssetV1, *rootpublication.StableResourceSet, error) {
	generation := manifest.Generation
	if c == nil || c.db == nil || generation == 0 || len(inputs) == 0 {
		return nil, nil, ErrVectorPartitionSearchUnavailable
	}
	cfg := c.meta.Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		return nil, nil, fmt.Errorf("%w: column asset manager", ErrVectorPartitionSearchUnavailable)
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, index)
	if !ok || def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 {
		return nil, nil, fmt.Errorf("%w: native FP32 cosine index", ErrVectorPartitionSearchUnavailable)
	}
	items := make([]StableColumnPhysicalAssetAppend, len(inputs))
	for i, in := range inputs {
		if in.Generation != generation || in.Dimensions != def.Dimensions || in.Source.Generation != manifest.SourceGeneration || in.Source.Checksum != manifest.SourceChecksum || in.Source.SchemaHash != manifest.SourceSchemaHash || in.Source.RowCount != manifest.SourceRowCount {
			return nil, nil, fmt.Errorf("%w: generation mismatch", ErrVectorPartitionSearchUnavailable)
		}
		if err := validateVectorPartitionSearchAssetV1(in); err != nil {
			return nil, nil, err
		}
		rows := make([]columnVectorGraphAssetRow, len(in.IDs))
		for j := range rows {
			rows[j] = columnVectorGraphAssetRow{ID: []byte(in.IDs[j]), Vector: in.Vectors[j], Adjacency: in.Adjacency[j], BaseRowRef: DocumentRowRef{DocumentID: []byte(in.IDs[j]), Generation: manifest.SourceGeneration, PartID: 1, RowIndex: j, AppliedCommandLSN: 1}}
		}
		graph := columnVectorGraphManifestSnapshot{IndexName: def.Name, Field: def.Field, Metric: def.Metric, Encoding: def.Encoding, Dimensions: def.Dimensions, M: def.M, EfConstruction: def.EfConstruction, EfSearch: def.EfSearch, BaseManifestGeneration: manifest.SourceGeneration, BaseManifestChecksum: manifest.SourceChecksum, BaseSchemaHash: manifest.SourceSchemaHash, GraphSchemaHash: cfg.SchemaHash, RowCount: len(rows)}
		pack, err := buildColumnHNSWSearchPackInput(def, graph, rows)
		if err != nil {
			return nil, nil, err
		}
		raw, err := encodeColumnHNSWSearchPack(pack)
		if err != nil {
			return nil, nil, err
		}
		// Column assets reserve physical part ID zero; logical partitions are
		// zero-based, so persist their unambiguous +1 representation.
		items[i] = StableColumnPhysicalAssetAppend{Payload: raw, Kind: ColumnAssetKindTCS1HNSWSearchPack, Generation: generation, PartID: uint64(in.PartitionID) + 1}
	}
	lease, err := c.db.AcquireStableResourceCaptureLease()
	if err != nil {
		return nil, nil, err
	}
	defer lease.Release()
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(c.db.ColumnAssetRootDir(), *cfg, fileID, items, c.db.StableResourceIdentityPinRegistry(), lease)
	if err != nil {
		return nil, nil, err
	}
	out := make([]VectorPartitionAssetV1, len(inputs))
	for i := range inputs {
		sum := sha256.Sum256(items[i].Payload)
		out[i] = VectorPartitionAssetV1{ID: vectorPartitionLocalAssetIDV1(inputs[i].PartitionID), PartitionID: inputs[i].PartitionID, Checksum: hex.EncodeToString(sum[:]), Bytes: uint64(len(items[i].Payload)), Ref: refs[i]}
	}
	return out, resources, nil
}

func encodeVectorPartitionSearchAssetV1(a VectorPartitionSearchAssetV1) ([]byte, error) {
	if len(a.Adjacency) == 0 {
		a.Adjacency = make([][]uint32, len(a.IDs))
	}
	if err := validateVectorPartitionSearchAssetV1(a); err != nil {
		return nil, err
	}
	// Bounded preflight covers all count-derived writes before allocating.
	bytesNeeded := int64(4 + 4 + 8 + 4 + 4 + 4 + 4 + sha256.Size + len(a.ManifestChecksum))
	for i, id := range a.IDs {
		bytesNeeded += int64(4 + len(id) + 1 + 4*len(a.Vectors[i]) + 4 + 4*len(a.Adjacency[i]))
		if bytesNeeded > vectorPartitionSearchAssetMaxBytesV1 {
			return nil, errors.New("partition search asset byte cap")
		}
	}
	var b bytes.Buffer
	b.Grow(int(bytesNeeded))
	b.WriteString(vectorPartitionSearchAssetMagicV1)
	var x [8]byte
	binary.BigEndian.PutUint32(x[:4], vectorPartitionSearchAssetVersionV1)
	b.Write(x[:4])
	binary.BigEndian.PutUint64(x[:], a.Generation)
	b.Write(x[:])
	binary.BigEndian.PutUint32(x[:4], a.PartitionID)
	b.Write(x[:4])
	binary.BigEndian.PutUint32(x[:4], uint32(a.Dimensions))
	b.Write(x[:4])
	binary.BigEndian.PutUint32(x[:4], uint32(len(a.IDs)))
	b.Write(x[:4])
	binary.BigEndian.PutUint32(x[:4], uint32(len(a.ManifestChecksum)))
	b.Write(x[:4])
	b.WriteString(a.ManifestChecksum)
	for i, id := range a.IDs {
		binary.BigEndian.PutUint32(x[:4], uint32(len(id)))
		b.Write(x[:4])
		b.WriteString(id)
		if a.Kinds[i] == VectorPartitionMembershipHomeV1 {
			b.WriteByte(1)
		} else {
			b.WriteByte(2)
		}
		for _, v := range a.Vectors[i] {
			binary.BigEndian.PutUint32(x[:4], math.Float32bits(v))
			b.Write(x[:4])
		}
		binary.BigEndian.PutUint32(x[:4], uint32(len(a.Adjacency[i])))
		b.Write(x[:4])
		for _, n := range a.Adjacency[i] {
			binary.BigEndian.PutUint32(x[:4], n)
			b.Write(x[:4])
		}
	}
	sum := sha256.Sum256(b.Bytes())
	b.Write(sum[:])
	return b.Bytes(), nil
}

func decodeVectorPartitionSearchAssetV1(raw []byte) (VectorPartitionSearchAssetV1, error) {
	if len(raw) < 4+4+8+4+4+4+4+sha256.Size || string(raw[:4]) != vectorPartitionSearchAssetMagicV1 || binary.BigEndian.Uint32(raw[4:8]) != vectorPartitionSearchAssetVersionV1 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset header")
	}
	sum := sha256.Sum256(raw[:len(raw)-sha256.Size])
	if !bytes.Equal(sum[:], raw[len(raw)-sha256.Size:]) {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset checksum")
	}
	off := 8
	take := func(n int) ([]byte, bool) {
		if n < 0 || off > len(raw)-sha256.Size-n {
			return nil, false
		}
		v := raw[off : off+n]
		off += n
		return v, true
	}
	u32 := func() (uint32, bool) {
		v, ok := take(4)
		if !ok {
			return 0, false
		}
		return binary.BigEndian.Uint32(v), true
	}
	u64 := func() (uint64, bool) {
		v, ok := take(8)
		if !ok {
			return 0, false
		}
		return binary.BigEndian.Uint64(v), true
	}
	g, ok := u64()
	if !ok {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
	}
	p, ok := u32()
	if !ok {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
	}
	dims, ok := u32()
	if !ok || dims == 0 || dims > 4096 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset dimensions")
	}
	rows, ok := u32()
	if !ok || rows == 0 || rows > 1_000_000 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset rows")
	}
	n, ok := u32()
	if !ok || n != 64 {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset manifest checksum")
	}
	mb, ok := take(int(n))
	if !ok {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
	}
	a := VectorPartitionSearchAssetV1{ManifestChecksum: string(mb), Generation: g, PartitionID: p, Dimensions: int(dims), IDs: make([]string, rows), Vectors: make([][]float32, rows), Kinds: make([]VectorPartitionMembershipKindV1, rows), Adjacency: make([][]uint32, rows)}
	for i := range a.IDs {
		ln, ok := u32()
		if !ok || ln == 0 || ln > 1<<20 {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset ID")
		}
		id, ok := take(int(ln))
		if !ok {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
		}
		a.IDs[i] = string(id)
		k, ok := take(1)
		if !ok {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset kind")
		}
		switch k[0] {
		case 1:
			a.Kinds[i] = VectorPartitionMembershipHomeV1
		case 2:
			a.Kinds[i] = VectorPartitionMembershipOverlapV1
		default:
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset kind")
		}
		a.Vectors[i] = make([]float32, dims)
		for j := range a.Vectors[i] {
			v, ok := u32()
			if !ok {
				return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
			}
			a.Vectors[i][j] = math.Float32frombits(v)
		}
		count, ok := u32()
		if !ok || count > rows {
			return VectorPartitionSearchAssetV1{}, errors.New("partition search asset adjacency")
		}
		a.Adjacency[i] = make([]uint32, count)
		for j := range a.Adjacency[i] {
			v, ok := u32()
			if !ok {
				return VectorPartitionSearchAssetV1{}, errors.New("partition search asset truncated")
			}
			a.Adjacency[i][j] = v
		}
	}
	if off != len(raw)-sha256.Size {
		return VectorPartitionSearchAssetV1{}, errors.New("partition search asset trailing bytes")
	}
	if err := validateVectorPartitionSearchAssetV1(a); err != nil {
		return VectorPartitionSearchAssetV1{}, err
	}
	return a, nil
}

func (c *Collection) OpenVectorPartitionLocalSearcherForGenerationV1(index string, generation uint64, partition uint32) (*VectorPartitionLocalSearcherV1, error) {
	if c == nil || c.db == nil {
		return nil, ErrVectorPartitionSearchUnavailable
	}
	pin, err := c.AcquireVectorPartitionReaderPinV1(index, generation)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	release := true
	defer func() {
		if release {
			pin.Release()
		}
	}()
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		return nil, err
	}
	m, err := store.Open(c.name, index, generation)
	if err != nil {
		return nil, err
	}
	var asset *VectorPartitionAssetV1
	for i := range m.Assets {
		if m.Assets[i].PartitionID == partition && m.Assets[i].ID == vectorPartitionLocalAssetIDV1(partition) {
			asset = &m.Assets[i]
			break
		}
	}
	if asset == nil || asset.Ref.Kind != ColumnAssetKindTCS1HNSWSearchPack || asset.Ref.Generation != generation || asset.Ref.Length > vectorPartitionSearchAssetMaxBytesV1 || asset.Ref.Length <= 0 {
		return nil, fmt.Errorf("%w: missing or stale partition asset", ErrVectorPartitionSearchUnavailable)
	}
	namespace := c.meta.Options.ColumnStore.AssetManager.Namespace
	if err := verifyVectorPartitionAssetsV1(c.db.ColumnAssetRootDir(), namespace, []VectorPartitionAssetV1{*asset}); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		return nil, err
	}
	if asset.Ref.Length > vectorPartitionSearchAssetMaxBytesV1 {
		return nil, fmt.Errorf("%w: asset byte cap", ErrVectorPartitionSearchUnavailable)
	}
	manager := mappedresource.NewManager()
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: asset.Ref.Namespace, Kind: string(asset.Ref.Kind), Generation: asset.Ref.Generation, PartID: asset.Ref.PartID, FileID: asset.Ref.FileID, Offset: asset.Ref.Offset, Length: asset.Ref.Length, Checksum: uint64(asset.Ref.Checksum), Version: columnHNSWSearchPackVersionV1, Encoding: columnVectorIndexStateEncodingHNSWSearchPackV1, Section: mappedresource.Section{Kind: string(columnVectorIndexStateAssetRoleHNSWSearchPack), Category: string(ColumnAssetKindTCS1HNSWSearchPack), Name: asset.ID}}
	h, err := manager.AcquireFileRange(key, mappedresource.Scope{Kind: mappedresource.ScopePreparedSearch, ID: "vector_partition/" + strconv.FormatUint(generation, 10), Collection: c.name, Namespace: asset.Ref.Namespace, Generation: generation, Reason: "vector partition native HNSW"}, path, mappedresource.AcquireOptions{Reason: "vector partition native HNSW", ValidationMode: mappedresource.ValidationVerify, PreferMapped: true, AllowHeapCopy: true, ResourceRoot: c.db.ColumnAssetRootDir(), ResourcePath: path})
	if err != nil {
		return nil, err
	}
	view, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, h, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: m.SourceGeneration, ManifestChecksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash}})
	if err != nil {
		_ = h.Release()
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionSearchUnavailable, err)
	}
	if view.Header.Dimensions <= 0 {
		_ = view.Close()
		return nil, ErrVectorPartitionSearchUnavailable
	}
	s := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Generation: generation, PartitionID: partition, Dimensions: view.Header.Dimensions}, prepared: view, opened: 1}
	s.partitionPin = pin
	release = false
	return s, nil
}
