package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

const columnRetainedSemanticStreamV1BlockRows = 4096

var (
	columnRetainedSemanticStreamV1BlockMagic   = []byte("crss1blk\x00")
	columnRetainedSemanticStreamV1LocatorMagic = []byte("crss1loc\x00")
)

type columnRetainedSemanticStreamEntry struct {
	row uint64
	raw []byte
}

type columnRetainedSemanticStreamPath struct {
	segments []string
	entries  []columnRetainedSemanticStreamEntry
}

// ColumnRetainedSemanticStreamV1StorageAccounting reports the retained-payload
// bytes produced by the semantic-stream-v1 InsertBatch encoding.
type ColumnRetainedSemanticStreamV1StorageAccounting struct {
	Rows                int
	PrimaryLocatorBytes int64
	BlockBytes          int64
	BlockCount          int
	TotalBytes          int64
}

// ColumnRetainedSemanticStreamV1BlockLayoutAudit reports exact raw byte
// attribution for semantic-stream-v1 side-root blocks plus payload-only codec
// oracles. It intentionally excludes value-log frame/index overhead.
type ColumnRetainedSemanticStreamV1BlockLayoutAudit struct {
	Rows                 int                                            `json:"rows,omitempty"`
	BlockRows            int                                            `json:"block_rows"`
	BlockCount           int                                            `json:"block_count"`
	PrimaryLocatorBytes  int64                                          `json:"primary_locator_bytes,omitempty"`
	RawBlockBytes        int64                                          `json:"raw_block_bytes"`
	BlockHeaderBytes     int64                                          `json:"block_header_bytes"`
	PathStreamCount      int64                                          `json:"path_stream_count"`
	ValueCount           int64                                          `json:"value_count"`
	PathMetadataBytes    int64                                          `json:"path_metadata_bytes"`
	EntryMetadataBytes   int64                                          `json:"entry_metadata_bytes"`
	ScalarValueBytes     int64                                          `json:"scalar_value_bytes"`
	BlockCodecStats      []ColumnRetainedSemanticStreamV1CodecStat      `json:"block_codec_stats,omitempty"`
	Paths                []ColumnRetainedSemanticStreamV1PathLayoutStat `json:"paths,omitempty"`
	PathsTruncated       bool                                           `json:"paths_truncated,omitempty"`
	PathZSTDInputBytes   int64                                          `json:"path_zstd_input_bytes,omitempty"`
	PathZSTDEncodedBytes int64                                          `json:"path_zstd_encoded_bytes,omitempty"`
}

type ColumnRetainedSemanticStreamV1CodecStat struct {
	Codec             string  `json:"codec"`
	Blocks            int     `json:"blocks"`
	RawBytes          int64   `json:"raw_bytes"`
	EncodedBytes      int64   `json:"encoded_bytes,omitempty"`
	StoredBytes       int64   `json:"stored_bytes"`
	KeptBlocks        int     `json:"kept_blocks,omitempty"`
	RawFallbackBlocks int     `json:"raw_fallback_blocks,omitempty"`
	EncodeErrors      int     `json:"encode_errors,omitempty"`
	EncodedToRawRatio float64 `json:"encoded_to_raw_ratio,omitempty"`
	StoredToRawRatio  float64 `json:"stored_to_raw_ratio,omitempty"`
}

type ColumnRetainedSemanticStreamV1PathLayoutStat struct {
	Path                string  `json:"path"`
	Blocks              int64   `json:"blocks"`
	Occurrences         int64   `json:"occurrences"`
	TotalBytes          int64   `json:"total_bytes"`
	PathMetadataBytes   int64   `json:"path_metadata_bytes"`
	EntryMetadataBytes  int64   `json:"entry_metadata_bytes"`
	ScalarValueBytes    int64   `json:"scalar_value_bytes"`
	MaxScalarValueBytes int     `json:"max_scalar_value_bytes,omitempty"`
	ZSTDBytes           int64   `json:"zstd_bytes,omitempty"`
	ZSTDToTotalRatio    float64 `json:"zstd_to_total_ratio,omitempty"`
}

// ColumnRetainedSemanticStreamV1StorageAccountingFromJSONDocuments applies the
// same retained-payload transform used by InsertBatch and returns the encoded
// primary retained bytes plus side-root block bytes.
func ColumnRetainedSemanticStreamV1StorageAccountingFromJSONDocuments(cfg ColumnStoreConfig, documents [][]byte) (ColumnRetainedSemanticStreamV1StorageAccounting, error) {
	if !columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg) {
		return ColumnRetainedSemanticStreamV1StorageAccounting{}, fmt.Errorf("collections: semantic-stream-v1 accounting requires retained encoding %q", ColumnRetainedPayloadEncodingSemanticStreamV1)
	}
	prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocuments(cfg, documents, nil)
	if err != nil {
		return ColumnRetainedSemanticStreamV1StorageAccounting{}, err
	}
	if prepared.semanticStreamBlocks != nil {
		defer resetCollectionRunTable(prepared.semanticStreamBlocks)
	}

	var out ColumnRetainedSemanticStreamV1StorageAccounting
	out.Rows = len(documents)
	for _, document := range prepared.documents {
		out.PrimaryLocatorBytes += int64(len(document))
	}
	if prepared.semanticStreamBlocks != nil {
		iter := prepared.semanticStreamBlocks.NewIterator(nil, nil)
		defer func() { _ = iter.Close() }()
		for ; iter.Valid(); iter.Next() {
			out.BlockCount++
			out.BlockBytes += int64(len(iter.UnsafeValue()))
		}
		if err := iter.Error(); err != nil {
			return ColumnRetainedSemanticStreamV1StorageAccounting{}, err
		}
	}
	out.TotalBytes = out.PrimaryLocatorBytes + out.BlockBytes
	return out, nil
}

// ColumnRetainedSemanticStreamV1BlockLayoutAuditFromJSONDocuments applies the
// InsertBatch semantic-stream-v1 transform and audits the resulting side-root
// block layout. maxPaths limits emitted path rows; zero emits all paths.
func ColumnRetainedSemanticStreamV1BlockLayoutAuditFromJSONDocuments(cfg ColumnStoreConfig, documents [][]byte, maxPaths int) (ColumnRetainedSemanticStreamV1BlockLayoutAudit, error) {
	if !columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg) {
		return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, fmt.Errorf("collections: semantic-stream-v1 block layout audit requires retained encoding %q", ColumnRetainedPayloadEncodingSemanticStreamV1)
	}
	prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocuments(cfg, documents, nil)
	if err != nil {
		return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, err
	}
	if prepared.semanticStreamBlocks != nil {
		defer resetCollectionRunTable(prepared.semanticStreamBlocks)
	}

	collector, err := newColumnRetainedSemanticStreamV1BlockLayoutCollector()
	if err != nil {
		return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, err
	}
	for _, document := range prepared.documents {
		collector.primaryLocatorBytes += int64(len(document))
	}
	if prepared.semanticStreamBlocks != nil {
		iter := prepared.semanticStreamBlocks.NewIterator(nil, nil)
		defer func() { _ = iter.Close() }()
		for ; iter.Valid(); iter.Next() {
			if err := collector.addBlock(iter.UnsafeValue()); err != nil {
				return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, err
			}
		}
		if err := iter.Error(); err != nil {
			return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, err
		}
	}
	return collector.result(len(documents), maxPaths)
}

func (c *Collection) auditRetainedSemanticStreamV1BlockLayoutAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, maxPaths int) (ColumnRetainedSemanticStreamV1BlockLayoutAudit, map[string]struct{}, error) {
	collector, err := newColumnRetainedSemanticStreamV1BlockLayoutCollector()
	if err != nil {
		return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, nil, err
	}
	rootName := collectionRetainedSemanticStreamRootName(catalog.meta.Name)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil {
		return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, nil, err
	}
	if it != nil {
		defer func() { _ = it.Close() }()
		for ; it.Valid(); it.Next() {
			if it.IsDeleted() {
				continue
			}
			block := it.ValueCopy(nil)
			if err := it.Error(); err != nil {
				return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, nil, fmt.Errorf("collections: semantic-stream-v1 block layout audit read %x: %w", it.UnsafeKey(), err)
			}
			if !it.Valid() {
				return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, nil, fmt.Errorf("collections: semantic-stream-v1 block layout audit iterator invalid after reading %x", it.UnsafeKey())
			}
			if err := collector.addBlock(block); err != nil {
				return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, nil, fmt.Errorf("collections: semantic-stream-v1 block layout audit %x: %w", it.UnsafeKey(), err)
			}
		}
		if err := it.Error(); err != nil {
			return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, nil, err
		}
	}
	paths := make(map[string]struct{}, len(collector.paths))
	for path := range collector.paths {
		paths[path] = struct{}{}
	}
	out, err := collector.result(0, maxPaths)
	if err != nil {
		return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, nil, err
	}
	return out, paths, nil
}

func prepareColumnRetainedPayloadInsertBatchStorageDocuments(cfg ColumnStoreConfig, documents [][]byte, fallback templateV1Resolver) (columnRetainedPayloadStorageDocuments, error) {
	if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn &&
		columnRetainedPayloadEffectiveEncoding(&cfg) == ColumnRetainedPayloadEncodingSemanticStreamV1 &&
		len(documents) > 1 {
		return prepareColumnRetainedSemanticStreamV1StorageDocuments(cfg, documents)
	}
	return prepareColumnRetainedPayloadStorageDocuments(cfg, documents, fallback)
}

func prepareColumnRetainedSemanticStreamV1StorageDocuments(cfg ColumnStoreConfig, documents [][]byte) (columnRetainedPayloadStorageDocuments, error) {
	out := columnRetainedPayloadStorageDocuments{
		documents: make([][]byte, len(documents)),
	}
	if len(documents) == 0 {
		return out, nil
	}
	blockTable := newCollectionRunTable((len(documents) + columnRetainedSemanticStreamV1BlockRows - 1) / columnRetainedSemanticStreamV1BlockRows)
	for start := 0; start < len(documents); start += columnRetainedSemanticStreamV1BlockRows {
		end := start + columnRetainedSemanticStreamV1BlockRows
		if end > len(documents) {
			end = len(documents)
		}
		retainedJSON := make([][]byte, end-start)
		for i := start; i < end; i++ {
			retained, err := columnRetainedPayloadJSONFromJSONDocument(cfg, documents[i])
			if err != nil {
				resetCollectionRunTable(blockTable)
				return columnRetainedPayloadStorageDocuments{}, err
			}
			retainedJSON[i-start] = retained
		}
		block, err := encodeColumnRetainedSemanticStreamV1Block(retainedJSON)
		if err != nil {
			resetCollectionRunTable(blockTable)
			return columnRetainedPayloadStorageDocuments{}, err
		}
		sum := sha256.Sum256(block)
		blockKey := append([]byte(nil), sum[:]...)
		for i := range retainedJSON {
			out.documents[start+i] = encodeColumnRetainedSemanticStreamV1Locator(blockKey, uint64(i))
		}
		setCollectionRunValue(blockTable, blockKey, block)
	}
	blockTable.Freeze()
	out.semanticStreamBlocks = blockTable
	return out, nil
}

func encodeColumnRetainedSemanticStreamV1Locator(blockKey []byte, row uint64) []byte {
	out := make([]byte, 0, len(columnRetainedSemanticStreamV1LocatorMagic)+sha256.Size+binary.MaxVarintLen64)
	out = append(out, columnRetainedSemanticStreamV1LocatorMagic...)
	out = append(out, blockKey...)
	out = binary.AppendUvarint(out, row)
	return out
}

func parseColumnRetainedSemanticStreamV1Locator(raw []byte) ([]byte, uint64, bool, error) {
	if !bytes.HasPrefix(raw, columnRetainedSemanticStreamV1LocatorMagic) {
		return nil, 0, false, nil
	}
	rest := raw[len(columnRetainedSemanticStreamV1LocatorMagic):]
	if len(rest) < sha256.Size+1 {
		return nil, 0, true, errors.New("collections: malformed semantic-stream-v1 retained locator")
	}
	blockKey := append([]byte(nil), rest[:sha256.Size]...)
	row, n := binary.Uvarint(rest[sha256.Size:])
	if n <= 0 || sha256.Size+n != len(rest) {
		return nil, 0, true, errors.New("collections: malformed semantic-stream-v1 retained locator row")
	}
	return blockKey, row, true, nil
}

func appendColumnRetainedSemanticStreamV1ReclaimDeltas(
	db *backenddb.DB,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	meta CollectionMeta,
	removedDocumentIDs [][]byte,
	removedPrimaryValues [][]byte,
	replacementPrimaryValues [][]byte,
	rootNames *[]string,
	baseRootIDs map[string]uint64,
	policies *[]backenddb.OrderedRootStoragePolicy,
	deltaTables *[]memtable.Table,
) error {
	if !columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) ||
		len(removedDocumentIDs) == 0 ||
		len(removedPrimaryValues) == 0 ||
		rootNames == nil ||
		baseRootIDs == nil ||
		policies == nil ||
		deltaTables == nil {
		return nil
	}
	candidates, err := columnRetainedSemanticStreamV1BlockKeysFromValues(removedPrimaryValues)
	if err != nil || len(candidates) == 0 {
		return err
	}
	replacementLive, err := columnRetainedSemanticStreamV1BlockKeysFromValues(replacementPrimaryValues)
	if err != nil {
		return err
	}
	for key := range replacementLive {
		delete(candidates, key)
	}
	if len(candidates) == 0 {
		return nil
	}
	live, err := columnRetainedSemanticStreamV1LiveCandidateBlocks(snap, catalog, meta, candidates, removedDocumentIDs)
	if err != nil {
		return err
	}
	for key := range live {
		delete(candidates, key)
	}
	if len(candidates) == 0 {
		return nil
	}
	rootName := collectionRetainedSemanticStreamRootName(meta.Name)
	baseRootID := catalog.rootID(rootName)
	if baseRootID == 0 && len(catalog.overlayRootIDs(rootName)) == 0 {
		return nil
	}
	policy, err := collectionRootStoragePolicyForDB(db, meta, rootName)
	if err != nil {
		return err
	}
	deleteKeys := make([][]byte, 0, len(candidates))
	for _, key := range candidates {
		deleteKeys = append(deleteKeys, key)
	}
	sort.Slice(deleteKeys, func(i, j int) bool {
		return bytes.Compare(deleteKeys[i], deleteKeys[j]) < 0
	})
	*rootNames = append(*rootNames, rootName)
	baseRootIDs[rootName] = baseRootID
	*policies = append(*policies, policy)
	*deltaTables = append(*deltaTables, buildDeleteRootDeltaTable(deleteKeys))
	return nil
}

func columnRetainedSemanticStreamV1BlockKeysFromValues(values [][]byte) (map[string][]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string][]byte)
	for _, value := range values {
		blockKey, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(value)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out[string(blockKey)] = blockKey
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func columnRetainedSemanticStreamV1LiveCandidateBlocks(snap *backenddb.Snapshot, catalog *collectionCatalog, meta CollectionMeta, candidates map[string][]byte, removedDocumentIDs [][]byte) (map[string]struct{}, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	removed := make(map[string]struct{}, len(removedDocumentIDs))
	for _, id := range removedDocumentIDs {
		removed[string(id)] = struct{}{}
	}
	primaryRootName := collectionPrimaryRootName(meta.Name)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, primaryRootName, nil, nil, false)
	if err != nil || it == nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	live := make(map[string]struct{})
	for ; it.Valid(); it.Next() {
		key := it.UnsafeKey()
		if _, skip := removed[string(key)]; skip {
			continue
		}
		value, _, flags := it.UnsafeEntry()
		if flags&node.FlagTombstone != 0 {
			continue
		}
		if flags&node.FlagPointer != 0 {
			resolved, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, primaryRootName, key, nil)
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}
			value = resolved
		}
		blockKey, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(value)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if _, candidate := candidates[string(blockKey)]; candidate {
			live[string(blockKey)] = struct{}{}
			if len(live) == len(candidates) {
				break
			}
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if len(live) == 0 {
		return nil, nil
	}
	return live, nil
}

func columnRetainedSemanticStreamV1PrimaryValueForReclaim(snap *backenddb.Snapshot, catalog *collectionCatalog, primaryRootName string, documentID []byte, entry node.LeafEntry) ([]byte, bool, error) {
	value := entry.Value
	if entry.Flags&node.FlagPointer != 0 {
		resolved, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, primaryRootName, documentID, nil)
		if err != nil || !found {
			return nil, false, err
		}
		value = resolved
	}
	if _, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(value); err != nil || !ok {
		return nil, ok, err
	}
	return bytes.Clone(value), true, nil
}

func resolveColumnRetainedPayloadAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, cfg ColumnStoreConfig, retained []byte) ([]byte, error) {
	if !columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg) {
		return retained, nil
	}
	blockKey, row, ok, err := parseColumnRetainedSemanticStreamV1Locator(retained)
	if err != nil || !ok {
		return retained, err
	}
	if snap == nil || catalog == nil {
		return nil, errors.New("collections: semantic-stream-v1 retained payload requires snapshot catalog")
	}
	block, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionRetainedSemanticStreamRootName(catalog.meta.Name), blockKey, nil)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("collections: semantic-stream-v1 retained block %x missing", blockKey)
	}
	return decodeColumnRetainedSemanticStreamV1BlockRowJSON(block, row)
}

func encodeColumnRetainedSemanticStreamV1Block(documents [][]byte) ([]byte, error) {
	streams := make(map[string]*columnRetainedSemanticStreamPath)
	for row, document := range documents {
		trimmed := bytes.TrimSpace(document)
		if len(trimmed) == 0 {
			trimmed = []byte("{}")
		}
		var root json.RawMessage
		if err := json.Unmarshal(trimmed, &root); err != nil {
			return nil, fmt.Errorf("collections: semantic-stream-v1 retained row %d: %w", row, err)
		}
		if err := collectColumnRetainedSemanticStreamPaths(root, nil, uint64(row), streams); err != nil {
			return nil, fmt.Errorf("collections: semantic-stream-v1 retained row %d: %w", row, err)
		}
	}
	keys := make([]string, 0, len(streams))
	for key := range streams {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]byte, 0, len(columnRetainedSemanticStreamV1BlockMagic)+len(documents)*16)
	out = append(out, columnRetainedSemanticStreamV1BlockMagic...)
	out = binary.AppendUvarint(out, uint64(len(documents)))
	out = binary.AppendUvarint(out, uint64(len(keys)))
	for _, key := range keys {
		stream := streams[key]
		out = binary.AppendUvarint(out, uint64(len(stream.segments)))
		for _, segment := range stream.segments {
			out = binary.AppendUvarint(out, uint64(len(segment)))
			out = append(out, segment...)
		}
		out = binary.AppendUvarint(out, uint64(len(stream.entries)))
		var last uint64
		for i, entry := range stream.entries {
			if i == 0 {
				out = binary.AppendUvarint(out, entry.row)
			} else {
				out = binary.AppendUvarint(out, entry.row-last)
			}
			last = entry.row
			out = binary.AppendUvarint(out, uint64(len(entry.raw)))
			out = append(out, entry.raw...)
		}
	}
	return out, nil
}

func collectColumnRetainedSemanticStreamPaths(raw json.RawMessage, path []string, row uint64, streams map[string]*columnRetainedSemanticStreamPath) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		trimmed = []byte("null")
	}
	switch trimmed[0] {
	case '{':
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return err
		}
		if len(obj) == 0 && len(path) > 0 {
			appendColumnRetainedSemanticStreamValue(path, row, trimmed, streams)
			return nil
		}
		keys := make([]string, 0, len(obj))
		for key := range obj {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			nextPath := append(append([]string(nil), path...), key)
			if err := collectColumnRetainedSemanticStreamPaths(obj[key], nextPath, row, streams); err != nil {
				return err
			}
		}
	case '[':
		if len(path) == 0 {
			return errors.New("semantic-stream-v1 retained root must be a JSON object")
		}
		appendColumnRetainedSemanticStreamValue(path, row, trimmed, streams)
	default:
		if len(path) == 0 {
			return errors.New("semantic-stream-v1 retained root must be a JSON object")
		}
		appendColumnRetainedSemanticStreamValue(path, row, trimmed, streams)
	}
	return nil
}

func appendColumnRetainedSemanticStreamValue(path []string, row uint64, raw []byte, streams map[string]*columnRetainedSemanticStreamPath) {
	key := columnRetainedSemanticStreamPathKey(path)
	stream := streams[key]
	if stream == nil {
		stream = &columnRetainedSemanticStreamPath{segments: append([]string(nil), path...)}
		streams[key] = stream
	}
	stream.entries = append(stream.entries, columnRetainedSemanticStreamEntry{
		row: row,
		raw: append([]byte(nil), raw...),
	})
}

func columnRetainedSemanticStreamPathKey(path []string) string {
	var out []byte
	for _, segment := range path {
		out = strconv.AppendInt(out, int64(len(segment)), 10)
		out = append(out, ':')
		out = append(out, segment...)
		out = append(out, 0)
	}
	return string(out)
}

func decodeColumnRetainedSemanticStreamV1BlockRowJSON(block []byte, row uint64) ([]byte, error) {
	obj, err := decodeColumnRetainedSemanticStreamV1BlockRowObject(block, row)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("collections: encode semantic-stream-v1 retained row: %w", err)
	}
	return out, nil
}

func decodeColumnRetainedSemanticStreamV1BlockRowObject(block []byte, row uint64) (map[string]any, error) {
	if !bytes.HasPrefix(block, columnRetainedSemanticStreamV1BlockMagic) {
		return nil, errors.New("collections: retained block is not semantic-stream-v1 encoded")
	}
	reader := bytes.NewReader(block[len(columnRetainedSemanticStreamV1BlockMagic):])
	rows, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block row count")
	}
	if row >= rows {
		return nil, fmt.Errorf("collections: semantic-stream-v1 row %d outside block rows %d", row, rows)
	}
	pathCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block path count")
	}
	obj := make(map[string]any)
	for pathOrdinal := uint64(0); pathOrdinal < pathCount; pathOrdinal++ {
		path, err := readColumnRetainedSemanticStreamV1Path(reader)
		if err != nil {
			return nil, err
		}
		entryCount, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, errors.New("collections: malformed semantic-stream-v1 retained block entry count")
		}
		var last uint64
		for entryOrdinal := uint64(0); entryOrdinal < entryCount; entryOrdinal++ {
			delta, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, errors.New("collections: malformed semantic-stream-v1 retained block row delta")
			}
			entryRow := delta
			if entryOrdinal != 0 {
				entryRow = last + delta
			}
			last = entryRow
			valueLen, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, errors.New("collections: malformed semantic-stream-v1 retained block value length")
			}
			if valueLen > uint64(reader.Len()) {
				return nil, errors.New("collections: truncated semantic-stream-v1 retained block value")
			}
			value := make([]byte, int(valueLen))
			if _, err := reader.Read(value); err != nil {
				return nil, err
			}
			if entryRow != row {
				continue
			}
			decoded := json.RawMessage(append([]byte(nil), value...))
			if !json.Valid(decoded) {
				return nil, errors.New("collections: decode semantic-stream-v1 retained value: invalid JSON")
			}
			if err := setColumnRetainedSemanticStreamPathValue(obj, path, decoded); err != nil {
				return nil, err
			}
		}
	}
	if reader.Len() != 0 {
		return nil, errors.New("collections: trailing semantic-stream-v1 retained block bytes")
	}
	return obj, nil
}

func readColumnRetainedSemanticStreamV1Path(reader *bytes.Reader) ([]string, error) {
	segmentCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block path segment count")
	}
	if segmentCount > uint64(int(^uint(0)>>1)) {
		return nil, errors.New("collections: semantic-stream-v1 retained block path too large")
	}
	path := make([]string, int(segmentCount))
	for i := range path {
		segmentLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, errors.New("collections: malformed semantic-stream-v1 retained block path segment length")
		}
		if segmentLen > uint64(reader.Len()) {
			return nil, errors.New("collections: truncated semantic-stream-v1 retained block path segment")
		}
		buf := make([]byte, int(segmentLen))
		if _, err := reader.Read(buf); err != nil {
			return nil, err
		}
		path[i] = string(buf)
	}
	return path, nil
}

func setColumnRetainedSemanticStreamPathValue(root map[string]any, path []string, value any) error {
	if len(path) == 0 {
		return errors.New("collections: semantic-stream-v1 retained value has empty object path")
	}
	current := root
	for _, segment := range path[:len(path)-1] {
		next, ok := current[segment]
		if !ok {
			child := make(map[string]any)
			current[segment] = child
			current = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("collections: semantic-stream-v1 retained path conflict at %q", segment)
		}
		current = child
	}
	current[path[len(path)-1]] = value
	return nil
}

type columnRetainedSemanticStreamV1BlockLayoutCollector struct {
	primaryLocatorBytes int64
	blockHeaderBytes    int64
	rawBlockBytes       int64
	pathMetadataBytes   int64
	entryMetadataBytes  int64
	scalarValueBytes    int64
	pathStreamCount     int64
	valueCount          int64
	blockCount          int
	codecs              map[string]*ColumnRetainedSemanticStreamV1CodecStat
	paths               map[string]*columnRetainedSemanticStreamV1PathLayoutCollector
	zstdEncoder         *zstd.Encoder
}

type columnRetainedSemanticStreamV1PathLayoutCollector struct {
	stat        ColumnRetainedSemanticStreamV1PathLayoutStat
	zstdCounter columnRetainedPayloadCountingWriter
	zstdWriter  *zstd.Encoder
}

func newColumnRetainedSemanticStreamV1BlockLayoutCollector() (*columnRetainedSemanticStreamV1BlockLayoutCollector, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("collections: create semantic-stream-v1 block zstd oracle: %w", err)
	}
	return &columnRetainedSemanticStreamV1BlockLayoutCollector{
		codecs: map[string]*ColumnRetainedSemanticStreamV1CodecStat{
			"snappy": {Codec: "snappy"},
			"lz4":    {Codec: "lz4"},
			"zstd":   {Codec: "zstd"},
		},
		paths:       make(map[string]*columnRetainedSemanticStreamV1PathLayoutCollector),
		zstdEncoder: enc,
	}, nil
}

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) addBlock(block []byte) error {
	if c == nil {
		return nil
	}
	if !bytes.HasPrefix(block, columnRetainedSemanticStreamV1BlockMagic) {
		return errors.New("collections: retained block is not semantic-stream-v1 encoded")
	}
	off := len(columnRetainedSemanticStreamV1BlockMagic)
	rows, n, err := readColumnRetainedSemanticStreamV1Uvarint(block, off, "row count")
	if err != nil {
		return err
	}
	off += n
	if rows == 0 {
		return errors.New("collections: malformed semantic-stream-v1 retained block zero rows")
	}
	pathCount, n, err := readColumnRetainedSemanticStreamV1Uvarint(block, off, "path count")
	if err != nil {
		return err
	}
	off += n
	blockHeaderBytes := off

	c.blockCount++
	c.rawBlockBytes += int64(len(block))
	c.blockHeaderBytes += int64(blockHeaderBytes)
	c.observeBlockCodec("snappy", block)
	c.observeBlockCodec("lz4", block)
	c.observeBlockCodec("zstd", block)

	for pathOrdinal := uint64(0); pathOrdinal < pathCount; pathOrdinal++ {
		pathStart := off
		segmentCount, n, err := readColumnRetainedSemanticStreamV1Uvarint(block, off, "path segment count")
		if err != nil {
			return err
		}
		off += n
		if segmentCount > uint64(int(^uint(0)>>1)) {
			return errors.New("collections: semantic-stream-v1 retained block path too large")
		}
		if segmentCount > uint64(len(block)-off) {
			return errors.New("collections: semantic-stream-v1 retained block path segment count exceeds remaining bytes")
		}
		segments := make([]string, 0, segmentCount)
		for segmentOrdinal := uint64(0); segmentOrdinal < segmentCount; segmentOrdinal++ {
			segmentLen, n, err := readColumnRetainedSemanticStreamV1Uvarint(block, off, "path segment length")
			if err != nil {
				return err
			}
			off += n
			if segmentLen > uint64(len(block)-off) {
				return errors.New("collections: truncated semantic-stream-v1 retained block path segment")
			}
			segment := string(block[off : off+int(segmentLen)])
			segments = append(segments, segment)
			off += int(segmentLen)
		}
		pathMetadataBytes := off - pathStart
		entryCount, n, err := readColumnRetainedSemanticStreamV1Uvarint(block, off, "entry count")
		if err != nil {
			return err
		}
		off += n
		entryMetadataBytes := n
		var scalarValueBytes int64
		var maxScalarValueBytes int
		for entryOrdinal := uint64(0); entryOrdinal < entryCount; entryOrdinal++ {
			if _, n, err = readColumnRetainedSemanticStreamV1Uvarint(block, off, "row delta"); err != nil {
				return err
			}
			off += n
			entryMetadataBytes += n
			valueLen, n, err := readColumnRetainedSemanticStreamV1Uvarint(block, off, "value length")
			if err != nil {
				return err
			}
			off += n
			entryMetadataBytes += n
			if valueLen > uint64(len(block)-off) {
				return errors.New("collections: truncated semantic-stream-v1 retained block value")
			}
			scalarValueBytes += int64(valueLen)
			if int(valueLen) > maxScalarValueBytes {
				maxScalarValueBytes = int(valueLen)
			}
			off += int(valueLen)
		}
		pathRaw := block[pathStart:off]
		path := strings.Join(segments, ".")
		if err := c.observePath(path, int64(pathMetadataBytes), int64(entryMetadataBytes), scalarValueBytes, int64(entryCount), maxScalarValueBytes, pathRaw); err != nil {
			return err
		}
	}
	if off != len(block) {
		return errors.New("collections: trailing semantic-stream-v1 retained block bytes")
	}
	return nil
}

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) observeBlockCodec(codec string, raw []byte) {
	stat := c.codecs[codec]
	if stat == nil {
		stat = &ColumnRetainedSemanticStreamV1CodecStat{Codec: codec}
		c.codecs[codec] = stat
	}
	stat.Blocks++
	stat.RawBytes += int64(len(raw))
	encodedBytes, ok := c.encodeBlockCodec(codec, raw)
	if !ok || encodedBytes <= 0 {
		stat.EncodeErrors++
		stat.StoredBytes += int64(len(raw))
		stat.RawFallbackBlocks++
		return
	}
	stat.EncodedBytes += int64(encodedBytes)
	if encodedBytes < len(raw) {
		stat.StoredBytes += int64(encodedBytes)
		stat.KeptBlocks++
	} else {
		stat.StoredBytes += int64(len(raw))
		stat.RawFallbackBlocks++
	}
}

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) encodeBlockCodec(codec string, raw []byte) (int, bool) {
	switch codec {
	case "snappy":
		return len(snappy.Encode(nil, raw)), true
	case "lz4":
		dst := make([]byte, lz4.CompressBlockBound(len(raw)))
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err != nil || n <= 0 {
			return 0, false
		}
		return n, true
	case "zstd":
		if c.zstdEncoder == nil {
			return 0, false
		}
		return len(c.zstdEncoder.EncodeAll(raw, nil)), true
	default:
		return 0, false
	}
}

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) observePath(path string, pathMetadataBytes, entryMetadataBytes, scalarValueBytes, occurrences int64, maxScalarValueBytes int, raw []byte) error {
	stream := c.paths[path]
	if stream == nil {
		stream = &columnRetainedSemanticStreamV1PathLayoutCollector{
			stat: ColumnRetainedSemanticStreamV1PathLayoutStat{Path: path},
		}
		zstdWriter, err := zstd.NewWriter(&stream.zstdCounter,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderCRC(false),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			return fmt.Errorf("collections: create semantic-stream-v1 path zstd oracle for path %q: %w", path, err)
		}
		stream.zstdWriter = zstdWriter
		c.paths[path] = stream
	}
	stream.stat.Blocks++
	stream.stat.Occurrences += occurrences
	stream.stat.PathMetadataBytes += pathMetadataBytes
	stream.stat.EntryMetadataBytes += entryMetadataBytes
	stream.stat.ScalarValueBytes += scalarValueBytes
	stream.stat.TotalBytes += int64(len(raw))
	if maxScalarValueBytes > stream.stat.MaxScalarValueBytes {
		stream.stat.MaxScalarValueBytes = maxScalarValueBytes
	}
	if _, err := stream.zstdWriter.Write(raw); err != nil {
		return fmt.Errorf("collections: write semantic-stream-v1 path zstd oracle for path %q: %w", path, err)
	}
	c.pathMetadataBytes += pathMetadataBytes
	c.entryMetadataBytes += entryMetadataBytes
	c.scalarValueBytes += scalarValueBytes
	c.pathStreamCount++
	c.valueCount += occurrences
	return nil
}

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) result(rows, maxPaths int) (ColumnRetainedSemanticStreamV1BlockLayoutAudit, error) {
	out := ColumnRetainedSemanticStreamV1BlockLayoutAudit{
		Rows:                rows,
		BlockRows:           columnRetainedSemanticStreamV1BlockRows,
		BlockCount:          c.blockCount,
		PrimaryLocatorBytes: c.primaryLocatorBytes,
		RawBlockBytes:       c.rawBlockBytes,
		BlockHeaderBytes:    c.blockHeaderBytes,
		PathStreamCount:     c.pathStreamCount,
		ValueCount:          c.valueCount,
		PathMetadataBytes:   c.pathMetadataBytes,
		EntryMetadataBytes:  c.entryMetadataBytes,
		ScalarValueBytes:    c.scalarValueBytes,
	}
	codecNames := []string{"snappy", "lz4", "zstd"}
	for _, name := range codecNames {
		stat := c.codecs[name]
		if stat == nil {
			continue
		}
		stat.EncodedToRawRatio = columnRetainedPayloadAuditRatio(stat.EncodedBytes, stat.RawBytes)
		stat.StoredToRawRatio = columnRetainedPayloadAuditRatio(stat.StoredBytes, stat.RawBytes)
		out.BlockCodecStats = append(out.BlockCodecStats, *stat)
	}
	paths := make([]ColumnRetainedSemanticStreamV1PathLayoutStat, 0, len(c.paths))
	for path, stream := range c.paths {
		if stream.zstdWriter != nil {
			if err := stream.zstdWriter.Close(); err != nil {
				return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, fmt.Errorf("collections: close semantic-stream-v1 path zstd oracle for path %q: %w", path, err)
			}
			stream.zstdWriter = nil
		}
		stat := stream.stat
		stat.ZSTDBytes = stream.zstdCounter.n
		stat.ZSTDToTotalRatio = columnRetainedPayloadAuditRatio(stat.ZSTDBytes, stat.TotalBytes)
		out.PathZSTDInputBytes += stat.TotalBytes
		out.PathZSTDEncodedBytes += stat.ZSTDBytes
		paths = append(paths, stat)
	}
	sort.Slice(paths, func(i, j int) bool {
		if paths[i].TotalBytes != paths[j].TotalBytes {
			return paths[i].TotalBytes > paths[j].TotalBytes
		}
		if paths[i].Occurrences != paths[j].Occurrences {
			return paths[i].Occurrences > paths[j].Occurrences
		}
		return paths[i].Path < paths[j].Path
	})
	if maxPaths > 0 && len(paths) > maxPaths {
		out.Paths = paths[:maxPaths]
		out.PathsTruncated = true
	} else {
		out.Paths = paths
	}
	if c.zstdEncoder != nil {
		c.zstdEncoder.Close()
		c.zstdEncoder = nil
	}
	if out.RawBlockBytes != out.BlockHeaderBytes+out.PathMetadataBytes+out.EntryMetadataBytes+out.ScalarValueBytes {
		return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, errors.New("collections: semantic-stream-v1 block layout byte accounting mismatch")
	}
	return out, nil
}

func readColumnRetainedSemanticStreamV1Uvarint(block []byte, off int, context string) (uint64, int, error) {
	if off < 0 || off >= len(block) {
		return 0, 0, fmt.Errorf("collections: malformed semantic-stream-v1 retained block %s", context)
	}
	value, n := binary.Uvarint(block[off:])
	if n <= 0 {
		return 0, 0, fmt.Errorf("collections: malformed semantic-stream-v1 retained block %s", context)
	}
	return value, n, nil
}
