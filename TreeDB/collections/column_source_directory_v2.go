package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/bits"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	"github.com/snissn/gomap/TreeDB/node"
)

const (
	columnSourceDirectoryFormatV2           = "tcd2"
	columnSourceDirectoryActiveSegmentKeyV2 = "\x01column-source-directory/v2/active-segment"
	columnSourceDirectoryHeaderKeyV2        = "\x01column-source-directory/v2/header"
	columnSourceDirectoryNodePrefixV2       = "\x11column-source-directory/v2/node/"
	columnSourceDirectoryLeafPrefixV2       = "\x12column-source-directory/v2/leaf/"
	columnSourceDirectoryPageBytesV2        = 2048
	columnSourceDirectoryHeaderBytesV2      = 4 + 5*8 + 3*sha256.Size + 64*sha256.Size + 2*sha256.Size
)

// This header is a semantic authenticated append directory. A physical B-tree
// root ID is deliberately absent. Its fixed frontier never grows with prior
// rows, parts, source shards or import ranges.
type columnSourceDirectoryHeaderV2 struct {
	Generation        uint64
	Rows              uint64
	Parts             uint64
	SourceMapEpoch    uint64
	AppliedCommandLSN uint64
	CollectionDigest  [sha256.Size]byte
	SchemaDigest      [sha256.Size]byte
	SourceMapDigest   [sha256.Size]byte
	Frontier          [64][sha256.Size]byte
	Digest            [sha256.Size]byte
}

type columnSourceDirectoryAppendV2 struct {
	previous      columnSourceDirectoryHeaderV2
	publication   *sourceImportPublicationV2
	collection    *Collection
	baseRoot      uint64
	activeSegment *columnManifestSegmentOwnership
}

func columnSourceDirectoryHashV2(kind string, values ...[]byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/column-source-directory/" + kind + "/v2\x00"))
	for _, v := range values {
		_, _ = h.Write(v)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

func columnSourceDirectoryNodeHashV2(left, right [sha256.Size]byte) [sha256.Size]byte {
	return columnSourceDirectoryHashV2("node", left[:], right[:])
}

func (h columnSourceDirectoryHeaderV2) treeRoot() [sha256.Size]byte {
	if h.Generation == 0 {
		return columnSourceDirectoryHashV2("empty")
	}
	height := bits.Len64(h.Generation - 1)
	if h.Generation&(h.Generation-1) == 0 {
		return h.Frontier[height]
	}
	empty := columnSourceDirectoryHashV2("empty")
	root := empty
	for level := 0; level < height; level++ {
		if h.Generation&(uint64(1)<<level) != 0 {
			root = columnSourceDirectoryNodeHashV2(h.Frontier[level], root)
		} else {
			root = columnSourceDirectoryNodeHashV2(root, empty)
		}
		empty = columnSourceDirectoryNodeHashV2(empty, empty)
	}
	return root
}

func (h columnSourceDirectoryHeaderV2) encode() ([]byte, error) {
	if h.Generation == 0 || h.SourceMapEpoch == 0 || h.AppliedCommandLSN == 0 || h.Rows == 0 || h.Parts == 0 {
		return nil, errors.New("collections: invalid source directory header")
	}
	out := make([]byte, 0, columnSourceDirectoryHeaderBytesV2)
	out = append(out, "TCD2"...)
	for _, v := range []uint64{h.Generation, h.Rows, h.Parts, h.SourceMapEpoch, h.AppliedCommandLSN} {
		out = binary.BigEndian.AppendUint64(out, v)
	}
	for _, d := range [][sha256.Size]byte{h.CollectionDigest, h.SchemaDigest, h.SourceMapDigest} {
		if d == ([sha256.Size]byte{}) {
			return nil, errors.New("collections: unbound source directory header")
		}
		out = append(out, d[:]...)
	}
	for level, d := range h.Frontier {
		if (h.Generation&(uint64(1)<<level) == 0) != (d == ([sha256.Size]byte{})) {
			return nil, errors.New("collections: source directory frontier occupancy")
		}
		out = append(out, d[:]...)
	}
	root := h.treeRoot()
	// Semantic identity excludes physical part count and local WAL position.
	digest := columnSourceDirectoryHashV2("root", out[:4+2*8], out[4+3*8:4+4*8], out[4+5*8:4+5*8+3*sha256.Size], root[:])
	out = append(out, digest[:]...)
	checksum := columnSourceDirectoryHashV2("header", out)
	return append(out, checksum[:]...), nil
}

func decodeColumnSourceDirectoryHeaderV2(raw []byte) (columnSourceDirectoryHeaderV2, error) {
	var h columnSourceDirectoryHeaderV2
	if len(raw) != columnSourceDirectoryHeaderBytesV2 || string(raw[:4]) != "TCD2" {
		return h, errors.New("collections: invalid source directory encoding")
	}
	p := 4
	for _, v := range []*uint64{&h.Generation, &h.Rows, &h.Parts, &h.SourceMapEpoch, &h.AppliedCommandLSN} {
		*v = binary.BigEndian.Uint64(raw[p:])
		p += 8
	}
	for _, v := range []*[sha256.Size]byte{&h.CollectionDigest, &h.SchemaDigest, &h.SourceMapDigest} {
		copy(v[:], raw[p:p+sha256.Size])
		p += sha256.Size
	}
	for i := range h.Frontier {
		copy(h.Frontier[i][:], raw[p:p+sha256.Size])
		p += sha256.Size
	}
	copy(h.Digest[:], raw[p:p+sha256.Size])
	canonical, err := h.encode()
	if err != nil || !bytes.Equal(canonical, raw) {
		return columnSourceDirectoryHeaderV2{}, errors.New("collections: source directory digest or frontier mismatch")
	}
	return h, nil
}

func columnSourceDirectoryKeyV2(prefix string, ordinal uint64, fragment uint64) []byte {
	key := make([]byte, len(prefix), len(prefix)+16)
	copy(key, prefix)
	key = binary.BigEndian.AppendUint64(key, ordinal)
	return binary.BigEndian.AppendUint64(key, fragment)
}

func (c *Collection) prepareColumnSourceDirectoryAppendV2(input columnWritePublishInput, root uint64) (*columnSourceDirectoryAppendV2, error) {
	p := input.sourceImportV2
	if p == nil || len(p.leafBytes) == 0 || len(p.leafBytes) > vectorpartition.MaxSourceChunkBytesV2 {
		return nil, errors.New("collections: missing bounded immutable source leaf")
	}
	d := &columnSourceDirectoryAppendV2{publication: p, collection: c, baseRoot: root}
	cfg := input.meta.Options.ColumnStore
	if root == 0 {
		if cfg.ActiveManifest != nil {
			return nil, errors.New("collections: source directory missing active root")
		}
		return d, nil
	}
	if cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnSourceDirectoryFormatV2 || cfg.ActiveManifest.Version != 2 {
		return nil, errors.New("collections: source import refuses mixed legacy column manifest")
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer snap.Close()
	if p.stats != nil {
		p.stats.DirectoryRecordsRead += 3
	}
	if err := validateColumnManifestIdentityAtRoot(snap, root, *cfg.ActiveManifest); err != nil {
		return nil, err
	}
	entry, err := snap.GetEntryAtRoot(root, []byte(columnSourceDirectoryHeaderKeyV2))
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, errors.New("collections: missing source directory header")
	}
	d.previous, err = decodeColumnSourceDirectoryHeaderV2(entry.Value)
	if err != nil {
		return nil, err
	}
	if d.previous.Generation != cfg.ActiveManifest.Generation || binary.BigEndian.Uint64(d.previous.Digest[:8]) != cfg.ActiveManifest.Checksum || d.previous.CollectionDigest != sha256.Sum256([]byte(input.meta.Name)) || d.previous.SchemaDigest != p.snapshot.SchemaDigest || d.previous.SourceMapEpoch != p.snapshot.SourceMapEpoch || d.previous.SourceMapDigest != p.snapshot.SourceMapDigest {
		return nil, errors.New("collections: changed source directory identity")
	}
	active, err := snap.GetEntryAtRoot(root, []byte(columnSourceDirectoryActiveSegmentKeyV2))
	if err != nil {
		return nil, err
	}
	if active.Flags&node.FlagTombstone != 0 || len(active.Value) < 4 {
		return nil, errors.New("collections: source directory active segment missing")
	}
	fileID := binary.BigEndian.Uint32(active.Value[:4])
	marker, err := decodeColumnManifestSegmentOwnershipForScan(columnManifestSegmentOwnershipRecordKey(fileID), active.Value[4:], cfg.AssetManager.Namespace, d.previous.Generation)
	if err != nil || marker.Graph {
		return nil, errors.Join(errors.New("collections: source directory active segment invalid"), err)
	}
	d.activeSegment = &marker
	return d, nil
}

func encodeColumnSourceDirectoryManifestV2(input ColumnPublishPlanInput, cfg ColumnStoreConfig, prepared ColumnPublishPreparedAssets) (ColumnPublishManifestEncodeResult, error) {
	d := input.sourceDirectoryV2
	if d == nil || d.publication == nil || input.Operation != ColumnPublishOperationInsert || input.AppliedCommandLSN == 0 || len(input.CurrentManifestRecords) != 0 {
		return ColumnPublishManifestEncodeResult{}, errors.New("collections: invalid incremental source directory input")
	}
	generation := d.previous.Generation + 1
	if generation == 0 {
		return ColumnPublishManifestEncodeResult{}, errors.New("collections: source directory generation overflow")
	}
	// Encoding only fresh asset records reuses the canonical legacy asset
	// codecs. No prior manifest record is loaded, normalized or checksummed.
	fresh, err := encodeColumnManifestAtGeneration(ColumnPublishManifestEncodeInput{Collection: input.Collection, ColumnStore: cfg, Operation: input.Operation, AppliedCommandLSN: input.AppliedCommandLSN, Prepared: prepared}, generation)
	if err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	records := make([]columnManifestRecord, 0, len(fresh.Records)+len(d.publication.leafBytes)/columnSourceDirectoryPageBytesV2+70)
	var activeMarker *columnManifestRecord
	var activeFileID uint32
	for _, r := range fresh.Records {
		if bytes.Equal(r.key, columnManifestHeaderRecordKeyBytes) {
			continue
		}
		records = append(records, r)
		if bytes.HasPrefix(r.key, columnManifestSegmentOwnershipRecordPrefixBytes) {
			marker, err := decodeColumnManifestSegmentOwnership(r.key, r.value)
			if err != nil {
				return ColumnPublishManifestEncodeResult{}, err
			}
			if activeMarker == nil || marker.Ref.FileID > activeFileID {
				copy := r
				activeMarker = &copy
				activeFileID = marker.Ref.FileID
			}
		}
	}
	if activeMarker == nil {
		return ColumnPublishManifestEncodeResult{}, errors.New("collections: source directory publication lacks owned segment")
	}
	activeRaw := binary.BigEndian.AppendUint32(nil, activeFileID)
	activeRaw = append(activeRaw, activeMarker.value...)
	records = append(records, columnManifestRecord{key: []byte(columnSourceDirectoryActiveSegmentKeyV2), value: activeRaw})
	p := d.publication
	if len(prepared.Assets) == 0 || prepared.RowCount <= 0 || d.previous.Rows > ^uint64(0)-uint64(prepared.RowCount) || d.previous.Parts > ^uint64(0)-uint64(len(prepared.Assets)) {
		return ColumnPublishManifestEncodeResult{}, errors.New("collections: source directory row/part count")
	}
	leafHash := sha256.New()
	_, _ = leafHash.Write([]byte("gomap/column-source-directory/leaf/v2\x00"))
	_, _ = leafHash.Write(p.leafDigest[:])
	_, _ = leafHash.Write(p.binding)
	// The receipt hashes canonical command bytes before any local LSN or
	// physical asset address is assigned, binding all retained/typed values.
	_, _ = leafHash.Write(p.receipt[:])
	var digest [sha256.Size]byte
	copy(digest[:], leafHash.Sum(digest[:0]))
	h := d.previous
	h.CollectionDigest = sha256.Sum256([]byte(input.Collection))
	h.SchemaDigest = p.snapshot.SchemaDigest
	h.SourceMapEpoch = p.snapshot.SourceMapEpoch
	h.SourceMapDigest = p.snapshot.SourceMapDigest
	h.Rows += uint64(prepared.RowCount)
	h.Parts += uint64(len(prepared.Assets))
	h.AppliedCommandLSN = input.AppliedCommandLSN
	records = append(records, columnManifestRecord{key: columnSourceDirectoryKeyV2(columnSourceDirectoryNodePrefixV2, 0, h.Generation), value: bytes.Clone(digest[:])})
	level := 0
	for n := h.Generation; n&1 != 0; n >>= 1 {
		digest = columnSourceDirectoryNodeHashV2(h.Frontier[level], digest)
		h.Frontier[level] = [sha256.Size]byte{}
		level++
		records = append(records, columnManifestRecord{key: columnSourceDirectoryKeyV2(columnSourceDirectoryNodePrefixV2, uint64(level), h.Generation>>level), value: bytes.Clone(digest[:])})
	}
	h.Frontier[level] = digest
	h.Generation = generation
	header, err := h.encode()
	if err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	records = append(records, columnManifestRecord{key: []byte(columnSourceDirectoryHeaderKeyV2), value: header})
	// Retain this generation's authenticated frontier for captured source roots.
	records = append(records, columnManifestRecord{key: columnSourceDirectoryKeyV2(columnSourceDirectoryLeafPrefixV2, generation, 0), value: bytes.Clone(header)})
	records = append(records, p.sourceRecordsV2()...)
	sortColumnManifestRecords(records)
	if err := validateSortedUniqueColumnManifestRecords(records, "source directory delta"); err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	h, err = decodeColumnSourceDirectoryHeaderV2(header)
	if err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	if p.completed.Digest != ([sha256.Size]byte{}) {
		p.seal = encodeSourceImportSealV2(generation, h.Digest, p.completed.Digest)
	}
	identity := ColumnManifestIdentity{Generation: generation, Format: columnSourceDirectoryFormatV2, Version: 2, Checksum: binary.BigEndian.Uint64(h.Digest[:8])}
	return ColumnPublishManifestEncodeResult{Identity: identity, ManifestBytes: columnManifestRecordsBytes(records), Records: records}, nil
}

func validateColumnSourceDirectoryDeltaV2(delta ColumnManifestRootDelta) error {
	if !delta.MutationDelta || delta.sourceDirectoryV2 == nil || delta.Identity.Format != columnSourceDirectoryFormatV2 || delta.Identity.Version != 2 || len(delta.Records) == 0 {
		return errors.New("collections: incomplete source directory delta")
	}
	if err := validateSortedUniqueColumnManifestRecords(delta.Records, "source directory delta"); err != nil {
		return err
	}
	var found bool
	for _, r := range delta.Records {
		if len(r.value) > 3000 {
			return errors.New("collections: source directory record exceeds inline bound")
		}
		if bytes.Equal(r.key, []byte(columnSourceDirectoryHeaderKeyV2)) {
			h, err := decodeColumnSourceDirectoryHeaderV2(r.value)
			if err != nil || h.Generation != delta.Identity.Generation || binary.BigEndian.Uint64(h.Digest[:8]) != delta.Identity.Checksum {
				return errors.New("collections: source directory root identity mismatch")
			}
			found = true
		}
	}
	if !found || len(delta.Mutations) != len(delta.Records) {
		return errors.New("collections: source directory header or delta omitted")
	}
	for i, m := range delta.Mutations {
		if m.deleted || !bytes.Equal(m.record.key, delta.Records[i].key) || !bytes.Equal(m.record.value, delta.Records[i].value) {
			return errors.New("collections: source directory delta mismatch")
		}
	}
	return nil
}

func sourceDirectoryDurablePublicationV2(delta ColumnManifestRootDelta, namespace string) (rootpublication.StableLogicalObligationRequirements, rootpublication.StableLogicalObligationMutation, func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error), rootpublication.StableResourceClosureWork, error) {
	var work rootpublication.StableResourceClosureWork
	mutation, err := stableColumnManifestDurableMutationWithWork(nil, delta.Mutations, delta.Identity.Generation, namespace, &work)
	if err != nil {
		return rootpublication.StableLogicalObligationRequirements{}, mutation, nil, work, err
	}
	// Preserve the exact safety fallback. Qualification must expose any use:
	// its full retained scan is not a bounded per-import implementation.
	fallback := func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		var fallbackWork rootpublication.StableResourceClosureWork
		d := delta.sourceDirectoryV2
		var records []columnManifestRecord
		if d.baseRoot != 0 {
			snap := d.collection.db.AcquireSnapshot()
			if snap == nil {
				return rootpublication.StableLogicalObligationRequirements{}, fallbackWork, backenddb.ErrClosed
			}
			var loadErr error
			records, loadErr = loadColumnManifestRecordsFromRoot(snap, d.baseRoot)
			if stats := d.publication.stats; stats != nil {
				stats.DirectoryRecordsRead += uint64(len(records))
				stats.FallbackRecordsRead += uint64(len(records))
			}
			_ = snap.Close()
			if loadErr != nil {
				return rootpublication.StableLogicalObligationRequirements{}, fallbackWork, loadErr
			}
		}
		records = append(records, delta.Records...)
		requirements, err := stableColumnManifestDurableRequirementsWithWork(records, delta.Identity.Generation, namespace, &fallbackWork)
		return requirements, fallbackWork, err
	}
	return rootpublication.StableLogicalObligationRequirements{}, mutation, fallback, work, nil
}

func sourceDirectoryPartRefAtSnapshotV2(snap *backenddb.Snapshot, root uint64, cfg ColumnStoreConfig, generation, partID uint64, kind ColumnAssetKind) (columnManifestAssetRefForScan, error) {
	var zero columnManifestAssetRefForScan
	if snap == nil || root == 0 || cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnSourceDirectoryFormatV2 || cfg.AssetManager == nil || generation == 0 || generation > cfg.ActiveManifest.Generation {
		return zero, errors.New("collections: invalid source directory part request")
	}
	if err := validateColumnManifestIdentityAtRoot(snap, root, *cfg.ActiveManifest); err != nil {
		return zero, err
	}
	entry, err := snap.GetEntryAtRoot(root, columnManifestPartRecordKey(generation, partID))
	if err != nil {
		return zero, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return zero, errors.New("collections: source directory part was deleted")
	}
	ref, rows, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(entry.Value, cfg.AssetManager.Namespace)
	if err != nil {
		return zero, err
	}
	if ref.Kind != kind || ref.Generation != generation || ref.PartID != partID || rows <= 0 || rows > vectorpartition.MaxSourceChunkRowsV2 || ref.Length > vectorpartition.MaxSourceChunkBytesV2 {
		return zero, errors.New("collections: source directory part identity or bounds")
	}
	op, ok := columnPhysicalScanOperationFromBytes(reason)
	if !ok || op != ColumnPublishOperationInsert {
		return zero, errors.New("collections: source directory non-insert part")
	}
	role, err := decodeColumnManifestPartRoleForScan(entry.Value, ref, reason)
	if err != nil {
		return zero, err
	}
	sortKey, err := decodeColumnManifestPartSortKeyForScan(entry.Value)
	if err != nil {
		return zero, err
	}
	return columnManifestAssetRefForScan{Ref: ref, Reason: op, Role: role, Rows: rows, SortKey: sortKey}, nil
}

func (c *Collection) sourceDirectoryVisibleRowAtSnapshotV2(snap *backenddb.Snapshot, catalog *collectionCatalog, id []byte, projected []string) (columnPhysicalVisibleRow, columnPhysicalScanDiagnostics, bool, error) {
	var zero columnPhysicalVisibleRow
	var diag columnPhysicalScanDiagnostics
	cfg := catalog.meta.Options.ColumnStore.copy()
	entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionColumnRowLocatorRootName(catalog.meta.Name), id)
	if err != nil {
		return zero, diag, false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return zero, diag, false, errors.New("collections: deleted source row locator")
	}
	ref, err := decodeColumnPrimaryRowLocator(id, entry.Value)
	if err != nil {
		return zero, diag, false, err
	}
	root := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	part, err := sourceDirectoryPartRefAtSnapshotV2(snap, root, cfg, ref.Generation, ref.PartID, ColumnAssetKindTCS1PartImage)
	if err != nil {
		return zero, diag, false, err
	}
	view := columnPhysicalScanSnapshotView{CollectionName: catalog.meta.Name, Catalog: catalog, Config: columnStoreRowAssetConfig(cfg), FullConfig: cfg, ColumnStoreEnabled: true, AssetRefs: []columnManifestAssetRefForScan{part}, ColumnAssetRootDir: c.db.ColumnAssetRootDir(), AssetNamespace: cfg.AssetManager.Namespace}
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{ProjectedColumns: projected, RequireInsertOnly: true, MaxDecodedBlocks: 1})
	if err != nil {
		return zero, diag, false, err
	}
	defer reader.Close()
	var scratch columnPhysicalRowReaderScratch
	row, err := reader.FetchRow(ref.RowIndex, &scratch)
	if err != nil {
		return zero, diag, false, err
	}
	if err := validateDocumentRowRefMatchesPointRow(ref, row); err != nil {
		return zero, diag, false, err
	}
	if row.Preserved != nil || row.Deleted {
		return zero, diag, false, errors.New("collections: mutable row in immutable source directory")
	}
	out := columnPhysicalVisibleRowFromReaderRow(row)
	out.ID = bytes.Clone(out.ID)
	out.Values = cloneColumnDeclaredValues(out.Values)
	diag.ManifestRoot = root
	diag.ManifestRecords = 2
	diag.AssetRefs = 1
	diag.RowsScanned = 1
	diag.PhysicalBytesScanned = reader.Stats().PhysicalBytesRead
	return out, diag, true, nil
}
