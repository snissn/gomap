package collections

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	"github.com/snissn/gomap/TreeDB/node"
)

// VectorPartitionSourceReaderV2 pins immutable source records in an existing
// TreeDB snapshot. Opening it does not admit canonical source authority: the
// caller must obtain expected from the source group's committed descriptor.
// Each read verifies actual local row bytes against that expected root.
type VectorPartitionSourceReaderV2 struct {
	mu         sync.RWMutex
	snap       *backenddb.Snapshot
	root       uint64
	prefix     string
	expected   vectorpartition.SourceSnapshotV2
	commitment VectorPartitionSourceCommitmentV2
}

// VectorPartitionSourceCommitmentV2 identifies a locally committed immutable
// source and the semantic directory root co-published at its completion. It is
// not a quorum capability. Canonical source authority must admit this exact
// commitment before a generation can bind it.
type VectorPartitionSourceCommitmentV2 struct {
	Snapshot            VectorPartitionSourceSnapshotV2
	DirectoryGeneration uint64
	DirectoryDigest     [sha256.Size]byte
}

const sourceImportSealBytesV2 = 4 + 8 + 2*sha256.Size

func encodeSourceImportSealV2(generation uint64, directory, snapshot [sha256.Size]byte) []byte {
	out := make([]byte, 0, sourceImportSealBytesV2)
	out = append(out, "SIS2"...)
	out = binary.BigEndian.AppendUint64(out, generation)
	out = append(out, directory[:]...)
	return append(out, snapshot[:]...)
}

func decodeSourceImportSealV2(raw []byte, expected VectorPartitionSourceSnapshotV2) (VectorPartitionSourceCommitmentV2, error) {
	var out VectorPartitionSourceCommitmentV2
	if len(raw) != sourceImportSealBytesV2 || string(raw[:4]) != "SIS2" {
		return out, errors.New("collections: missing or malformed immutable source seal")
	}
	out.Snapshot = expected
	out.DirectoryGeneration = binary.BigEndian.Uint64(raw[4:12])
	copy(out.DirectoryDigest[:], raw[12:44])
	var snapshot [sha256.Size]byte
	copy(snapshot[:], raw[44:])
	if out.DirectoryGeneration == 0 || out.DirectoryDigest == ([sha256.Size]byte{}) || expected.Digest == ([sha256.Size]byte{}) || snapshot != expected.Digest {
		return VectorPartitionSourceCommitmentV2{}, errors.New("collections: source seal identity mismatch")
	}
	return out, nil
}

// Commitment returns the exact persisted completion binding. It is unchanged
// by later imports, and grants no placement or canonical source authority.
func (r *VectorPartitionSourceReaderV2) Commitment() VectorPartitionSourceCommitmentV2 {
	if r == nil {
		return VectorPartitionSourceCommitmentV2{}
	}
	return r.commitment
}

type VectorPartitionSourceChunkReadV2 struct {
	Chunk         vectorpartition.SourceChunkV2
	Proof         vectorpartition.SourceChunkProofV2
	MetadataReads uint64
	EncodedBytes  uint64
}

// OpenVectorPartitionSourceSnapshotV2 performs a bounded set of point lookups,
// verifies completed durable import progress, and retains no global row map.
func (c *Collection) OpenVectorPartitionSourceSnapshotV2(expected VectorPartitionSourceSnapshotV2, indexName string) (*VectorPartitionSourceReaderV2, error) {
	if c == nil || c.db == nil {
		return nil, errCollectionDBNil
	}
	sealed, err := vectorpartition.SealSourceSnapshotV2(expected, expected.MerkleRoot)
	if err != nil || expected.Digest == ([sha256.Size]byte{}) || sealed != expected {
		return nil, errors.New("collections: unsealed expected source snapshot")
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	fail := func(err error) (*VectorPartitionSourceReaderV2, error) { _ = snap.Close(); return nil, err }
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return fail(err)
	}
	if catalog == nil || catalog.meta.Options.ColumnStore == nil {
		return fail(errCollectionNotFound)
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnSourceDirectoryFormatV2 || cfg.ActiveManifest.Version != 2 {
		return fail(errors.New("collections: source reader requires V2 directory"))
	}
	progressKey, _ := sourceImportKeysV2(catalog.meta.Name, sourceImportBindingV2{Snapshot: expected, IndexName: indexName}, 0)
	bindingRaw, present, err := getSystemValue(snap, progressKey+":binding")
	if err != nil {
		return fail(err)
	}
	if !present || len(bindingRaw) > maxSourceImportBindingBytesV2 {
		return fail(errors.New("collections: source snapshot binding missing or oversized"))
	}
	var record sourceImportBindingRecordV2
	if err := decodeSourceImportJSONV2(bindingRaw, &record); err != nil {
		return fail(err)
	}
	stored, err := vectorpartition.SealSourceSnapshotV2(record.Binding.Snapshot, expected.MerkleRoot)
	if err != nil || stored != expected || record.Version != 2 || record.Binding.IndexName != indexName {
		return fail(errors.New("collections: expected source revision differs from imported identity"))
	}
	progress, present, err := getSystemValue(snap, progressKey)
	if err != nil {
		return fail(err)
	}
	if !present {
		return fail(errors.New("collections: incomplete source import"))
	}
	acc, err := vectorpartition.RestoreSourceSnapshotAccumulatorV2(record.Binding.Snapshot, progress)
	if err != nil {
		return fail(err)
	}
	actual, err := acc.Finish()
	if err != nil || actual != expected {
		return fail(errors.New("collections: source import incomplete or wrong completed root"))
	}
	sealRaw, present, err := getSystemValue(snap, progressKey+":seal")
	if err != nil {
		return fail(err)
	}
	if !present {
		return fail(errors.New("collections: completed source directory seal missing"))
	}
	commitment, err := decodeSourceImportSealV2(sealRaw, expected)
	if err != nil {
		return fail(err)
	}
	if commitment.DirectoryGeneration > cfg.ActiveManifest.Generation {
		return fail(errors.New("collections: source seal exceeds active directory"))
	}
	root := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	if err := validateColumnManifestIdentityAtRoot(snap, root, *cfg.ActiveManifest); err != nil {
		return fail(err)
	}
	entry, err := snap.GetEntryAtRoot(root, columnSourceDirectoryKeyV2(columnSourceDirectoryLeafPrefixV2, commitment.DirectoryGeneration, 0))
	if err != nil {
		return fail(err)
	}
	if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 {
		return fail(errors.New("collections: source directory header missing"))
	}
	header, err := decodeColumnSourceDirectoryHeaderV2(entry.Value)
	if err != nil {
		return fail(err)
	}
	if header.Generation != commitment.DirectoryGeneration || header.Digest != commitment.DirectoryDigest || header.CollectionDigest != sha256.Sum256([]byte(catalog.meta.Name)) || header.SchemaDigest != expected.SchemaDigest || header.SourceMapEpoch != expected.SourceMapEpoch || header.SourceMapDigest != expected.SourceMapDigest {
		return fail(errors.New("collections: source reader directory identity mismatch"))
	}
	return &VectorPartitionSourceReaderV2{snap: snap, root: root, prefix: sourceImportDirectoryPrefixV2(progressKey), expected: expected, commitment: commitment}, nil
}

func (r *VectorPartitionSourceReaderV2) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snap == nil {
		return nil
	}
	snap := r.snap
	r.snap = nil
	return snap.Close()
}

func (r *VectorPartitionSourceReaderV2) ReadChunk(index uint64) (VectorPartitionSourceChunkReadV2, error) {
	var out VectorPartitionSourceChunkReadV2
	if r == nil {
		return out, backenddb.ErrClosed
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.snap == nil {
		return out, backenddb.ErrClosed
	}
	if index >= r.expected.ChunkCount() {
		return out, errors.New("collections: source chunk outside snapshot")
	}
	read := func(key []byte) ([]byte, error) {
		out.MetadataReads++
		entry, err := r.snap.GetEntryAtRoot(r.root, key)
		if err != nil {
			return nil, err
		}
		if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 || len(entry.Value) > 3000 {
			return nil, errors.New("collections: missing or oversized source record")
		}
		return entry.Value, nil
	}
	metadata, err := read(columnSourceDirectoryKeyV2(r.prefix+"chunk/", index, 0))
	if err != nil {
		return VectorPartitionSourceChunkReadV2{}, err
	}
	if len(metadata) != 8+sha256.Size {
		return VectorPartitionSourceChunkReadV2{}, errors.New("collections: source chunk metadata length")
	}
	size := binary.BigEndian.Uint64(metadata)
	if size == 0 || size > vectorpartition.MaxSourceChunkBytesV2 {
		return VectorPartitionSourceChunkReadV2{}, errors.New("collections: source chunk byte bound")
	}
	var leaf [sha256.Size]byte
	copy(leaf[:], metadata[8:])
	raw := make([]byte, 0, int(size))
	for page := uint64(1); uint64(len(raw)) < size; page++ {
		fragment, err := read(columnSourceDirectoryKeyV2(r.prefix+"chunk/", index, page))
		if err != nil {
			return VectorPartitionSourceChunkReadV2{}, err
		}
		want := min(uint64(columnSourceDirectoryPageBytesV2), size-uint64(len(raw)))
		if uint64(len(fragment)) != want {
			return VectorPartitionSourceChunkReadV2{}, errors.New("collections: missing or reordered source fragment")
		}
		raw = append(raw, fragment...)
	}
	chunk, err := vectorpartition.DecodeSourceLeafV2(r.expected, leaf, raw)
	if err != nil || chunk.Index != index {
		return VectorPartitionSourceChunkReadV2{}, errors.Join(err, errors.New("collections: source leaf identity"))
	}
	proof, err := vectorpartition.ReadSourceChunkProofV2(r.expected, index, func(level uint8, nodeIndex uint64) ([sha256.Size]byte, error) {
		var digest [sha256.Size]byte
		v, err := read(columnSourceDirectoryKeyV2(r.prefix+"node/", uint64(level), nodeIndex))
		if err != nil {
			return digest, err
		}
		if len(v) != sha256.Size {
			return digest, errors.New("collections: source proof node length")
		}
		copy(digest[:], v)
		return digest, nil
	})
	if err != nil {
		return VectorPartitionSourceChunkReadV2{}, err
	}
	if err := vectorpartition.VerifySourceChunkV2(r.expected, r.expected.Digest, chunk, proof); err != nil {
		return VectorPartitionSourceChunkReadV2{}, err
	}
	out.Chunk, out.Proof, out.EncodedBytes = chunk, proof, size
	return out, nil
}
