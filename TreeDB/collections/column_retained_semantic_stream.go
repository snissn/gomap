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

	"github.com/buger/jsonparser"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/tidwall/gjson"
)

const columnRetainedSemanticStreamV1BlockRows = 4096
const maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes = 512 << 20
const defaultColumnRetainedSemanticStreamV1DecodeCacheBlocks = 16
const minColumnRetainedSemanticStreamV1DecodeCacheRows = 64
const minColumnRetainedSemanticStreamV1DecodeCacheRowsPerBlock = 512

var (
	// Side-root retained blocks are stored either as a decoded semantic-stream
	// block (`crss1blk\0`) or as a zstd stored-block wrapper (`crss1zst\0`).
	// The wrapper payload is: magic, uvarint decoded crss1blk byte length, and
	// a zstd frame containing that complete raw crss1blk block.
	columnRetainedSemanticStreamV1BlockMagic     = []byte("crss1blk\x00")
	columnRetainedSemanticStreamV1BlockZSTDMagic = []byte("crss1zst\x00")
	columnRetainedSemanticStreamV1LocatorMagic   = []byte("crss1loc\x00")
)

type columnRetainedSemanticStreamEntry struct {
	row uint64
	raw []byte
}

type columnRetainedSemanticStreamPath struct {
	segments []string
	entries  []columnRetainedSemanticStreamEntry
}

type columnRetainedSemanticStreamV1DecodedBlock struct {
	rows [][]byte
}

type columnRetainedSemanticStreamV1StoredBlockEncoder struct {
	enc *zstd.Encoder
}

type columnRetainedSemanticStreamV1RootFastPathPlan struct {
	declaredColumnIndexesByPath map[string][]int
	declaredRowsReady           bool
}

type columnRetainedSemanticStreamV1DecodeCache struct {
	blocks        map[string]*columnRetainedSemanticStreamV1DecodedBlock
	order         []string
	maxBlocks     int
	allowedBlocks map[string]struct{}
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
	StoredBlockBytes     int64                                          `json:"stored_block_bytes,omitempty"`
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
		if _, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(document); err != nil {
			return ColumnRetainedSemanticStreamV1StorageAccounting{}, err
		} else if ok {
			out.PrimaryLocatorBytes += int64(len(document))
		}
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
	defer collector.close()
	for _, document := range prepared.documents {
		if _, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(document); err != nil {
			return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, err
		} else if ok {
			collector.primaryLocatorBytes += int64(len(document))
		}
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
	defer collector.close()
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

func (c *Collection) auditRetainedSemanticStreamV1BlockLayoutPathsAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, blockRows map[string]uint64) (map[string]struct{}, error) {
	collector := newColumnRetainedSemanticStreamV1PathCollector()
	defer collector.close()
	rootName := collectionRetainedSemanticStreamRootName(catalog.meta.Name)
	keys := make([]string, 0, len(blockRows))
	for key := range blockRows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		blockKey := []byte(key)
		block, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, blockKey, nil)
		if err != nil {
			return nil, fmt.Errorf("collections: semantic-stream-v1 sampled block layout audit read %x: %w", blockKey, err)
		}
		if !found {
			return nil, fmt.Errorf("collections: semantic-stream-v1 retained block %x missing", blockKey)
		}
		if err := collector.addBlock(block); err != nil {
			return nil, fmt.Errorf("collections: semantic-stream-v1 sampled block layout audit %x: %w", blockKey, err)
		}
	}
	paths := make(map[string]struct{}, len(collector.paths))
	for pathKey := range collector.paths {
		paths[pathKey] = struct{}{}
	}
	return paths, nil
}

func prepareColumnRetainedPayloadInsertBatchStorageDocuments(cfg ColumnStoreConfig, documents [][]byte, fallback templateV1Resolver) (columnRetainedPayloadStorageDocuments, error) {
	return prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg, nil, documents, fallback)
}

func prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg ColumnStoreConfig, ids, documents [][]byte, fallback templateV1Resolver) (columnRetainedPayloadStorageDocuments, error) {
	if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn &&
		columnRetainedPayloadEffectiveEncoding(&cfg) == ColumnRetainedPayloadEncodingSemanticStreamV1 {
		return prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg, ids, documents)
	}
	return prepareColumnRetainedPayloadStorageDocuments(cfg, documents, fallback)
}

func prepareColumnRetainedSemanticStreamV1StorageDocuments(cfg ColumnStoreConfig, documents [][]byte) (columnRetainedPayloadStorageDocuments, error) {
	return prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg, nil, documents)
}

func prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg ColumnStoreConfig, ids, documents [][]byte) (columnRetainedPayloadStorageDocuments, error) {
	out := columnRetainedPayloadStorageDocuments{
		documents: make([][]byte, len(documents)),
	}
	if len(documents) == 0 {
		return out, nil
	}
	rootPlan, useRootFastPath := columnRetainedSemanticStreamV1RootFastPathPlanForConfig(cfg, ids, len(documents))
	if useRootFastPath && rootPlan.declaredRowsReady {
		out.declaredRows = make([]columnDeclaredRow, len(documents))
		out.declaredRowsReady = true
	}
	blockTable := newCollectionRunTable((len(documents) + columnRetainedSemanticStreamV1BlockRows - 1) / columnRetainedSemanticStreamV1BlockRows)
	storedBlockEncoder, err := newColumnRetainedSemanticStreamV1StoredBlockEncoder()
	if err != nil {
		resetCollectionRunTable(blockTable)
		return columnRetainedPayloadStorageDocuments{}, err
	}
	defer storedBlockEncoder.close()
	for start := 0; start < len(documents); start += columnRetainedSemanticStreamV1BlockRows {
		end := start + columnRetainedSemanticStreamV1BlockRows
		if end > len(documents) {
			end = len(documents)
		}
		rows := end - start
		streams := make(map[string]*columnRetainedSemanticStreamPath)
		for row, i := 0, start; i < end; row, i = row+1, i+1 {
			if useRootFastPath {
				declaredValues, err := collectColumnRetainedSemanticStreamV1RootFastPathDocument(cfg, rootPlan, documents[i], uint64(row), streams)
				if err != nil {
					resetCollectionRunTable(blockTable)
					return columnRetainedPayloadStorageDocuments{}, fmt.Errorf("collections: semantic-stream-v1 retained row %d: %w", row, err)
				}
				if rootPlan.declaredRowsReady {
					out.declaredRows[i] = columnDeclaredRow{
						ID:     bytes.Clone(ids[i]),
						Values: declaredValues,
					}
				}
			} else {
				retained, err := columnRetainedPayloadJSONObjectFromJSONDocument(cfg, documents[i])
				if err != nil {
					resetCollectionRunTable(blockTable)
					return columnRetainedPayloadStorageDocuments{}, err
				}
				if err := collectColumnRetainedSemanticStreamObjectPaths(retained, nil, uint64(row), streams); err != nil {
					resetCollectionRunTable(blockTable)
					return columnRetainedPayloadStorageDocuments{}, fmt.Errorf("collections: semantic-stream-v1 retained row %d: %w", row, err)
				}
			}
		}
		block, err := encodeColumnRetainedSemanticStreamV1BlockFromStreamsWithEncoder(rows, streams, storedBlockEncoder)
		if err != nil {
			resetCollectionRunTable(blockTable)
			return columnRetainedPayloadStorageDocuments{}, err
		}
		sum := sha256.Sum256(block)
		blockKey := append([]byte(nil), sum[:]...)
		for i := 0; i < rows; i++ {
			out.documents[start+i] = encodeColumnRetainedSemanticStreamV1Locator(blockKey, uint64(i))
		}
		setCollectionRunValue(blockTable, blockKey, block)
	}
	blockTable.Freeze()
	out.semanticStreamBlocks = blockTable
	return out, nil
}

func columnRetainedSemanticStreamV1RootFastPathPlanForConfig(cfg ColumnStoreConfig, ids [][]byte, documentCount int) (columnRetainedSemanticStreamV1RootFastPathPlan, bool) {
	plan := columnRetainedSemanticStreamV1RootFastPathPlan{
		declaredColumnIndexesByPath: make(map[string][]int, len(cfg.Columns)),
		declaredRowsReady:           ids != nil && len(ids) == documentCount,
	}
	for colIdx, col := range cfg.Columns {
		if col.Path == "" || strings.Contains(col.Path, ".") {
			return columnRetainedSemanticStreamV1RootFastPathPlan{}, false
		}
		plan.declaredColumnIndexesByPath[col.Path] = append(plan.declaredColumnIndexesByPath[col.Path], colIdx)
		if !columnDeclaredJSONParserValueSupported(col.ValueType) {
			plan.declaredRowsReady = false
		}
	}
	return plan, true
}

func collectColumnRetainedSemanticStreamV1RootFastPathDocument(cfg ColumnStoreConfig, plan columnRetainedSemanticStreamV1RootFastPathPlan, document []byte, row uint64, streams map[string]*columnRetainedSemanticStreamPath) ([]columnDeclaredValue, error) {
	if !gjson.ValidBytes(document) {
		return nil, errors.New("invalid JSON: invalid JSON")
	}
	if !jsonDocumentLooksObject(document) {
		return nil, errors.New("semantic-stream-v1 retained root must be a JSON object")
	}
	var stackValues [8]jsonParserIndexValue
	valuesRaw := stackValues[:]
	if len(cfg.Columns) > len(stackValues) {
		valuesRaw = make([]jsonParserIndexValue, len(cfg.Columns))
	} else {
		valuesRaw = valuesRaw[:len(cfg.Columns)]
	}
	err := jsonparser.ObjectEach(document, func(key, value []byte, dataType jsonparser.ValueType, _ int) error {
		path := string(key)
		if indexes := plan.declaredColumnIndexesByPath[path]; len(indexes) > 0 {
			if plan.declaredRowsReady {
				for _, colIdx := range indexes {
					valuesRaw[colIdx] = jsonParserIndexValue{raw: value, valueType: dataType}
				}
			}
			return nil
		}
		raw, err := columnRetainedSemanticStreamV1JSONParserRawValue(value, dataType)
		if err != nil {
			return err
		}
		return collectColumnRetainedSemanticStreamPaths(json.RawMessage(raw), []string{path}, row, streams)
	})
	if err != nil {
		return nil, err
	}
	if !plan.declaredRowsReady {
		return nil, nil
	}
	values := make([]columnDeclaredValue, len(cfg.Columns))
	var scratch []byte
	for colIdx, col := range cfg.Columns {
		value, err := convertColumnDeclaredJSONParserValue(col, valuesRaw[colIdx], &scratch)
		if err != nil {
			return nil, fmt.Errorf("%w: column[%d] %q: %v", ErrColumnDeclaredValueUnsupported, colIdx, col.Name, err)
		}
		values[colIdx] = value
	}
	return values, nil
}

func columnRetainedSemanticStreamV1JSONParserRawValue(value []byte, dataType jsonparser.ValueType) ([]byte, error) {
	switch dataType {
	case jsonparser.String:
		out := make([]byte, 0, len(value)+2)
		out = append(out, '"')
		out = append(out, value...)
		out = append(out, '"')
		return out, nil
	case jsonparser.Number, jsonparser.Object, jsonparser.Array, jsonparser.Boolean, jsonparser.Null:
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported semantic-stream-v1 retained value type %s", dataType)
	}
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
	deleteKeys, err := columnRetainedSemanticStreamV1ReclaimDeleteKeys(snap, catalog, meta, removedDocumentIDs, removedPrimaryValues, replacementPrimaryValues)
	if err != nil || len(deleteKeys) == 0 {
		return err
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
	*rootNames = append(*rootNames, rootName)
	baseRootIDs[rootName] = baseRootID
	*policies = append(*policies, policy)
	*deltaTables = append(*deltaTables, buildDeleteRootDeltaTable(deleteKeys))
	return nil
}

func appendColumnRetainedSemanticStreamV1BlockDeltas(
	db *backenddb.DB,
	catalog *collectionCatalog,
	meta CollectionMeta,
	blockTable memtable.Table,
	rootNames *[]string,
	baseRootIDs map[string]uint64,
	policies *[]backenddb.OrderedRootStoragePolicy,
	deltaTables *[]memtable.Table,
) error {
	if blockTable == nil ||
		blockTable.Len() == 0 ||
		rootNames == nil ||
		baseRootIDs == nil ||
		policies == nil ||
		deltaTables == nil {
		return nil
	}
	rootName := collectionRetainedSemanticStreamRootName(meta.Name)
	policy, err := collectionRootStoragePolicyForDB(db, meta, rootName)
	if err != nil {
		return err
	}
	*rootNames = append(*rootNames, rootName)
	baseRootIDs[rootName] = catalog.rootID(rootName)
	*policies = append(*policies, policy)
	*deltaTables = append(*deltaTables, blockTable)
	return nil
}

func columnRetainedSemanticStreamV1ReclaimDeleteKeys(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	meta CollectionMeta,
	removedDocumentIDs [][]byte,
	removedPrimaryValues [][]byte,
	replacementPrimaryValues [][]byte,
) ([][]byte, error) {
	if !columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) ||
		len(removedDocumentIDs) == 0 ||
		len(removedPrimaryValues) == 0 {
		return nil, nil
	}
	candidates, err := columnRetainedSemanticStreamV1BlockKeysFromValues(removedPrimaryValues)
	if err != nil || len(candidates) == 0 {
		return nil, err
	}
	replacementLive, err := columnRetainedSemanticStreamV1BlockKeysFromValues(replacementPrimaryValues)
	if err != nil {
		return nil, err
	}
	for key := range replacementLive {
		delete(candidates, key)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	live, err := columnRetainedSemanticStreamV1LiveCandidateBlocks(snap, catalog, meta, candidates, removedDocumentIDs)
	if err != nil {
		return nil, err
	}
	for key := range live {
		delete(candidates, key)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	deleteKeys := make([][]byte, 0, len(candidates))
	for _, key := range candidates {
		deleteKeys = append(deleteKeys, key)
	}
	sort.Slice(deleteKeys, func(i, j int) bool {
		return bytes.Compare(deleteKeys[i], deleteKeys[j]) < 0
	})
	return deleteKeys, nil
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
	return resolveColumnRetainedPayloadAtSnapshotWithCache(snap, catalog, cfg, retained, nil)
}

func resolveColumnRetainedPayloadAtSnapshotWithCache(snap *backenddb.Snapshot, catalog *collectionCatalog, cfg ColumnStoreConfig, retained []byte, cache *columnRetainedSemanticStreamV1DecodeCache) ([]byte, error) {
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
	if cache != nil {
		return cache.rowJSONAtSnapshot(snap, catalog, blockKey, row)
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

func newColumnRetainedSemanticStreamV1DecodeCache() *columnRetainedSemanticStreamV1DecodeCache {
	return newColumnRetainedSemanticStreamV1DecodeCacheWithMaxBlocks(defaultColumnRetainedSemanticStreamV1DecodeCacheBlocks)
}

func newColumnRetainedSemanticStreamV1DecodeCacheWithMaxBlocks(maxBlocks int) *columnRetainedSemanticStreamV1DecodeCache {
	if maxBlocks <= 0 {
		return nil
	}
	return &columnRetainedSemanticStreamV1DecodeCache{
		blocks:    make(map[string]*columnRetainedSemanticStreamV1DecodedBlock, maxBlocks),
		maxBlocks: maxBlocks,
	}
}

func newColumnRetainedSemanticStreamV1DecodeCacheForDocumentRecords(cfg *ColumnStoreConfig, records []DocumentRecord) *columnRetainedSemanticStreamV1DecodeCache {
	if !columnStoreRetainedPayloadUsesSemanticStreamV1(cfg) || len(records) < minColumnRetainedSemanticStreamV1DecodeCacheRows {
		return nil
	}
	counts := make(map[string]int)
	for _, record := range records {
		blockKey, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(record.Document)
		if err != nil || !ok {
			continue
		}
		counts[string(blockKey)]++
	}
	allowed := make(map[string]struct{})
	for blockKey, count := range counts {
		if count >= minColumnRetainedSemanticStreamV1DecodeCacheRowsPerBlock {
			allowed[blockKey] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return nil
	}
	cache := newColumnRetainedSemanticStreamV1DecodeCache()
	cache.allowedBlocks = allowed
	return cache
}

func (c *columnRetainedSemanticStreamV1DecodeCache) rowJSONAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, blockKey []byte, row uint64) ([]byte, error) {
	if c == nil {
		return nil, errors.New("collections: semantic-stream-v1 retained decode cache is nil")
	}
	cacheKey := string(blockKey)
	if !c.shouldCacheBlock(cacheKey) {
		block, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionRetainedSemanticStreamRootName(catalog.meta.Name), blockKey, nil)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("collections: semantic-stream-v1 retained block %x missing", blockKey)
		}
		return decodeColumnRetainedSemanticStreamV1BlockRowJSON(block, row)
	}
	decoded := c.blocks[cacheKey]
	if decoded == nil {
		block, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionRetainedSemanticStreamRootName(catalog.meta.Name), blockKey, nil)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("collections: semantic-stream-v1 retained block %x missing", blockKey)
		}
		rows, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(block)
		if err != nil {
			return nil, err
		}
		decoded = &columnRetainedSemanticStreamV1DecodedBlock{rows: rows}
		c.store(cacheKey, decoded)
	}
	return decoded.rowJSON(row)
}

func (c *columnRetainedSemanticStreamV1DecodeCache) shouldCacheBlock(cacheKey string) bool {
	if c == nil {
		return false
	}
	if len(c.allowedBlocks) == 0 {
		return true
	}
	_, ok := c.allowedBlocks[cacheKey]
	return ok
}

func (c *columnRetainedSemanticStreamV1DecodeCache) store(cacheKey string, decoded *columnRetainedSemanticStreamV1DecodedBlock) {
	if c == nil || c.maxBlocks <= 0 || decoded == nil {
		return
	}
	if _, exists := c.blocks[cacheKey]; !exists {
		c.order = append(c.order, cacheKey)
	}
	c.blocks[cacheKey] = decoded
	for len(c.order) > c.maxBlocks {
		evict := c.order[0]
		copy(c.order, c.order[1:])
		c.order = c.order[:len(c.order)-1]
		delete(c.blocks, evict)
	}
}

func (b *columnRetainedSemanticStreamV1DecodedBlock) rowJSON(row uint64) ([]byte, error) {
	if b == nil {
		return nil, errors.New("collections: semantic-stream-v1 retained decoded block is nil")
	}
	if row >= uint64(len(b.rows)) {
		return nil, fmt.Errorf("collections: semantic-stream-v1 row %d outside block rows %d", row, len(b.rows))
	}
	return bytes.Clone(b.rows[int(row)]), nil
}

func validateColumnRetainedSemanticStreamV1LocatorAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, retained []byte, blockRows map[string]uint64) (bool, error) {
	blockKey, row, ok, err := parseColumnRetainedSemanticStreamV1Locator(retained)
	if err != nil || !ok {
		return ok, err
	}
	if snap == nil || catalog == nil {
		return true, errors.New("collections: semantic-stream-v1 retained payload requires snapshot catalog")
	}
	cacheKey := string(blockKey)
	rows, cached := blockRows[cacheKey]
	if !cached {
		block, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionRetainedSemanticStreamRootName(catalog.meta.Name), blockKey, nil)
		if err != nil {
			return true, err
		}
		if !found {
			return true, fmt.Errorf("collections: semantic-stream-v1 retained block %x missing", blockKey)
		}
		rows, err = columnRetainedSemanticStreamV1BlockRowCount(block)
		if err != nil {
			return true, err
		}
		if blockRows != nil {
			blockRows[cacheKey] = rows
		}
	}
	if row >= rows {
		return true, fmt.Errorf("collections: semantic-stream-v1 row %d outside block rows %d", row, rows)
	}
	return true, nil
}

func columnRetainedSemanticStreamV1BlockRowCount(block []byte) (uint64, error) {
	raw, err := decodeColumnRetainedSemanticStreamV1StoredBlock(block)
	if err != nil {
		return 0, err
	}
	off := len(columnRetainedSemanticStreamV1BlockMagic)
	rows, _, err := readColumnRetainedSemanticStreamV1Uvarint(raw, off, "row count")
	if err != nil {
		return 0, err
	}
	if err := validateColumnRetainedSemanticStreamV1BlockRows(rows); err != nil {
		return 0, err
	}
	return rows, nil
}

func validateColumnRetainedSemanticStreamV1BlockRows(rows uint64) error {
	if rows == 0 {
		return errors.New("collections: malformed semantic-stream-v1 retained block zero rows")
	}
	if rows > columnRetainedSemanticStreamV1BlockRows {
		return fmt.Errorf("collections: semantic-stream-v1 retained block row count %d exceeds max %d", rows, columnRetainedSemanticStreamV1BlockRows)
	}
	return nil
}

func columnRetainedSemanticStreamV1EntryRow(last, delta, entryOrdinal, rows uint64) (uint64, error) {
	entryRow := delta
	if entryOrdinal != 0 {
		if delta > ^uint64(0)-last {
			return 0, errors.New("collections: malformed semantic-stream-v1 retained block row delta overflow")
		}
		entryRow = last + delta
	}
	if entryRow >= rows {
		return 0, fmt.Errorf("collections: semantic-stream-v1 entry row %d outside block rows %d", entryRow, rows)
	}
	return entryRow, nil
}

func encodeColumnRetainedSemanticStreamV1Block(documents [][]byte) ([]byte, error) {
	raw, err := encodeColumnRetainedSemanticStreamV1RawBlock(documents)
	if err != nil {
		return nil, err
	}
	return encodeColumnRetainedSemanticStreamV1StoredBlock(raw)
}

func encodeColumnRetainedSemanticStreamV1RawBlock(documents [][]byte) ([]byte, error) {
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
	return encodeColumnRetainedSemanticStreamV1RawBlockFromStreams(len(documents), streams)
}

func encodeColumnRetainedSemanticStreamV1BlockFromStreams(rows int, streams map[string]*columnRetainedSemanticStreamPath) ([]byte, error) {
	return encodeColumnRetainedSemanticStreamV1BlockFromStreamsWithEncoder(rows, streams, nil)
}

func encodeColumnRetainedSemanticStreamV1BlockFromStreamsWithEncoder(rows int, streams map[string]*columnRetainedSemanticStreamPath, encoder *columnRetainedSemanticStreamV1StoredBlockEncoder) ([]byte, error) {
	raw, err := encodeColumnRetainedSemanticStreamV1RawBlockFromStreams(rows, streams)
	if err != nil {
		return nil, err
	}
	if encoder != nil {
		return encoder.encodeWithRawLimit(raw, maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes)
	}
	return encodeColumnRetainedSemanticStreamV1StoredBlock(raw)
}

func encodeColumnRetainedSemanticStreamV1RawBlockFromStreams(rows int, streams map[string]*columnRetainedSemanticStreamPath) ([]byte, error) {
	if rows <= 0 {
		return nil, errors.New("collections: semantic-stream-v1 retained block requires at least one row")
	}
	if err := validateColumnRetainedSemanticStreamV1BlockRows(uint64(rows)); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(streams))
	for key := range streams {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]byte, 0, len(columnRetainedSemanticStreamV1BlockMagic)+rows*16)
	out = append(out, columnRetainedSemanticStreamV1BlockMagic...)
	out = binary.AppendUvarint(out, uint64(rows))
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

func encodeColumnRetainedSemanticStreamV1StoredBlock(raw []byte) ([]byte, error) {
	return encodeColumnRetainedSemanticStreamV1StoredBlockWithRawLimit(raw, maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes)
}

func encodeColumnRetainedSemanticStreamV1StoredBlockWithRawLimit(raw []byte, compressedRawLimit int) ([]byte, error) {
	encoder, err := newColumnRetainedSemanticStreamV1StoredBlockEncoder()
	if err != nil {
		return nil, err
	}
	defer encoder.close()
	return encoder.encodeWithRawLimit(raw, compressedRawLimit)
}

func newColumnRetainedSemanticStreamV1StoredBlockEncoder() (*columnRetainedSemanticStreamV1StoredBlockEncoder, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("collections: create semantic-stream-v1 retained block zstd encoder: %w", err)
	}
	return &columnRetainedSemanticStreamV1StoredBlockEncoder{enc: enc}, nil
}

func (e *columnRetainedSemanticStreamV1StoredBlockEncoder) close() {
	if e == nil || e.enc == nil {
		return
	}
	e.enc.Close()
	e.enc = nil
}

func (e *columnRetainedSemanticStreamV1StoredBlockEncoder) encodeWithRawLimit(raw []byte, compressedRawLimit int) ([]byte, error) {
	if !bytes.HasPrefix(raw, columnRetainedSemanticStreamV1BlockMagic) {
		return nil, errors.New("collections: retained block is not semantic-stream-v1 encoded")
	}
	if compressedRawLimit > 0 && len(raw) > compressedRawLimit {
		return raw, nil
	}
	if e == nil || e.enc == nil {
		return nil, errors.New("collections: semantic-stream-v1 retained block zstd encoder is closed")
	}
	compressed := e.enc.EncodeAll(raw, nil)
	out := make([]byte, 0, len(columnRetainedSemanticStreamV1BlockZSTDMagic)+binary.MaxVarintLen64+len(compressed))
	out = append(out, columnRetainedSemanticStreamV1BlockZSTDMagic...)
	out = binary.AppendUvarint(out, uint64(len(raw)))
	out = append(out, compressed...)
	if len(out) >= len(raw) {
		return raw, nil
	}
	return out, nil
}

func decodeColumnRetainedSemanticStreamV1StoredBlock(block []byte) ([]byte, error) {
	if bytes.HasPrefix(block, columnRetainedSemanticStreamV1BlockMagic) {
		return block, nil
	}
	if !bytes.HasPrefix(block, columnRetainedSemanticStreamV1BlockZSTDMagic) {
		return nil, errors.New("collections: retained block is not semantic-stream-v1 encoded")
	}
	off := len(columnRetainedSemanticStreamV1BlockZSTDMagic)
	rawBytes, n, err := readColumnRetainedSemanticStreamV1Uvarint(block, off, "zstd raw block length")
	if err != nil {
		return nil, err
	}
	off += n
	if rawBytes == 0 {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block zero zstd raw length")
	}
	if rawBytes > maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes {
		return nil, fmt.Errorf("collections: semantic-stream-v1 retained block zstd raw length %d exceeds max %d", rawBytes, maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes)
	}
	if off >= len(block) {
		return nil, errors.New("collections: truncated semantic-stream-v1 retained block zstd payload")
	}
	maxDecodedBytes := int(rawBytes)
	if maxDecodedBytes < 1<<20 {
		maxDecodedBytes = 1 << 20
	}
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(uint64(maxDecodedBytes)),
		zstd.WithDecodeAllCapLimit(true),
	)
	if err != nil {
		return nil, fmt.Errorf("collections: create semantic-stream-v1 retained block zstd decoder: %w", err)
	}
	raw, err := dec.DecodeAll(block[off:], make([]byte, 0, int(rawBytes)))
	dec.Close()
	if err != nil {
		return nil, fmt.Errorf("collections: decode semantic-stream-v1 retained block zstd payload: %w", err)
	}
	if len(raw) != int(rawBytes) {
		return nil, fmt.Errorf("collections: semantic-stream-v1 retained block zstd decoded length=%d want=%d", len(raw), rawBytes)
	}
	if !bytes.HasPrefix(raw, columnRetainedSemanticStreamV1BlockMagic) {
		return nil, errors.New("collections: semantic-stream-v1 retained block zstd payload decoded to invalid block")
	}
	return raw, nil
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

func collectColumnRetainedSemanticStreamObjectPaths(obj map[string]any, path []string, row uint64, streams map[string]*columnRetainedSemanticStreamPath) error {
	if len(obj) == 0 {
		if len(path) > 0 {
			return appendColumnRetainedSemanticStreamJSONValue(path, row, obj, streams)
		}
		return nil
	}
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		nextPath := append(append([]string(nil), path...), key)
		if err := collectColumnRetainedSemanticStreamAnyPaths(obj[key], nextPath, row, streams); err != nil {
			return err
		}
	}
	return nil
}

func collectColumnRetainedSemanticStreamAnyPaths(value any, path []string, row uint64, streams map[string]*columnRetainedSemanticStreamPath) error {
	switch typed := value.(type) {
	case map[string]any:
		return collectColumnRetainedSemanticStreamObjectPaths(typed, path, row, streams)
	default:
		return appendColumnRetainedSemanticStreamJSONValue(path, row, typed, streams)
	}
}

func appendColumnRetainedSemanticStreamJSONValue(path []string, row uint64, value any, streams map[string]*columnRetainedSemanticStreamPath) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("collections: encode semantic-stream-v1 retained value: %w", err)
	}
	appendColumnRetainedSemanticStreamValue(path, row, raw, streams)
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

func decodeColumnRetainedSemanticStreamV1BlockRowsJSON(block []byte) ([][]byte, error) {
	raw, err := decodeColumnRetainedSemanticStreamV1StoredBlock(block)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(raw[len(columnRetainedSemanticStreamV1BlockMagic):])
	rows, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block row count")
	}
	if rows == 0 {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block zero rows")
	}
	if rows > uint64(int(^uint(0)>>1)) {
		return nil, errors.New("collections: semantic-stream-v1 retained block row count too large")
	}
	if err := validateColumnRetainedSemanticStreamV1BlockRows(rows); err != nil {
		return nil, err
	}
	pathCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block path count")
	}
	objects := make([]map[string]any, int(rows))
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
			entryRow, err := columnRetainedSemanticStreamV1EntryRow(last, delta, entryOrdinal, rows)
			if err != nil {
				return nil, err
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
			decoded := json.RawMessage(value)
			if !json.Valid(decoded) {
				return nil, errors.New("collections: decode semantic-stream-v1 retained value: invalid JSON")
			}
			obj := objects[int(entryRow)]
			if obj == nil {
				obj = make(map[string]any)
				objects[int(entryRow)] = obj
			}
			if err := setColumnRetainedSemanticStreamPathValue(obj, path, decoded); err != nil {
				return nil, err
			}
		}
	}
	if reader.Len() != 0 {
		return nil, errors.New("collections: trailing semantic-stream-v1 retained block bytes")
	}
	out := make([][]byte, len(objects))
	for i, obj := range objects {
		if obj == nil {
			obj = make(map[string]any)
		}
		rowJSON, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("collections: encode semantic-stream-v1 retained row: %w", err)
		}
		out[i] = rowJSON
	}
	return out, nil
}

func decodeColumnRetainedSemanticStreamV1BlockRowObject(block []byte, row uint64) (map[string]any, error) {
	raw, err := decodeColumnRetainedSemanticStreamV1StoredBlock(block)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(raw[len(columnRetainedSemanticStreamV1BlockMagic):])
	rows, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, errors.New("collections: malformed semantic-stream-v1 retained block row count")
	}
	if err := validateColumnRetainedSemanticStreamV1BlockRows(rows); err != nil {
		return nil, err
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
			entryRow, err := columnRetainedSemanticStreamV1EntryRow(last, delta, entryOrdinal, rows)
			if err != nil {
				return nil, err
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
	storedBlockBytes    int64
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
	pathOnly            bool
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

func newColumnRetainedSemanticStreamV1PathCollector() *columnRetainedSemanticStreamV1BlockLayoutCollector {
	return &columnRetainedSemanticStreamV1BlockLayoutCollector{
		paths:    make(map[string]*columnRetainedSemanticStreamV1PathLayoutCollector),
		pathOnly: true,
	}
}

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) close() {
	if c == nil {
		return
	}
	if c.zstdEncoder != nil {
		c.zstdEncoder.Close()
		c.zstdEncoder = nil
	}
	for _, stream := range c.paths {
		if stream.zstdWriter != nil {
			_ = stream.zstdWriter.Close()
			stream.zstdWriter = nil
		}
	}
}

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) addBlock(block []byte) error {
	if c == nil {
		return nil
	}
	raw, err := decodeColumnRetainedSemanticStreamV1StoredBlock(block)
	if err != nil {
		return err
	}
	off := len(columnRetainedSemanticStreamV1BlockMagic)
	rows, n, err := readColumnRetainedSemanticStreamV1Uvarint(raw, off, "row count")
	if err != nil {
		return err
	}
	off += n
	if err := validateColumnRetainedSemanticStreamV1BlockRows(rows); err != nil {
		return err
	}
	pathCount, n, err := readColumnRetainedSemanticStreamV1Uvarint(raw, off, "path count")
	if err != nil {
		return err
	}
	off += n
	blockHeaderBytes := off

	if !c.pathOnly {
		c.blockCount++
		c.storedBlockBytes += int64(len(block))
		c.rawBlockBytes += int64(len(raw))
		c.blockHeaderBytes += int64(blockHeaderBytes)
		c.observeBlockCodec("snappy", raw)
		c.observeBlockCodec("lz4", raw)
		c.observeBlockCodec("zstd", raw)
	}

	for pathOrdinal := uint64(0); pathOrdinal < pathCount; pathOrdinal++ {
		pathStart := off
		segmentCount, n, err := readColumnRetainedSemanticStreamV1Uvarint(raw, off, "path segment count")
		if err != nil {
			return err
		}
		off += n
		if segmentCount > uint64(int(^uint(0)>>1)) {
			return errors.New("collections: semantic-stream-v1 retained block path too large")
		}
		if segmentCount > uint64(len(raw)-off) {
			return errors.New("collections: semantic-stream-v1 retained block path segment count exceeds remaining bytes")
		}
		segments := make([]string, 0, segmentCount)
		for segmentOrdinal := uint64(0); segmentOrdinal < segmentCount; segmentOrdinal++ {
			segmentLen, n, err := readColumnRetainedSemanticStreamV1Uvarint(raw, off, "path segment length")
			if err != nil {
				return err
			}
			off += n
			if segmentLen > uint64(len(raw)-off) {
				return errors.New("collections: truncated semantic-stream-v1 retained block path segment")
			}
			segment := string(raw[off : off+int(segmentLen)])
			segments = append(segments, segment)
			off += int(segmentLen)
		}
		pathMetadataBytes := off - pathStart
		entryCount, n, err := readColumnRetainedSemanticStreamV1Uvarint(raw, off, "entry count")
		if err != nil {
			return err
		}
		off += n
		entryMetadataBytes := n
		var scalarValueBytes int64
		var maxScalarValueBytes int
		var last uint64
		for entryOrdinal := uint64(0); entryOrdinal < entryCount; entryOrdinal++ {
			var delta uint64
			if delta, n, err = readColumnRetainedSemanticStreamV1Uvarint(raw, off, "row delta"); err != nil {
				return err
			}
			off += n
			entryMetadataBytes += n
			entryRow, err := columnRetainedSemanticStreamV1EntryRow(last, delta, entryOrdinal, rows)
			if err != nil {
				return err
			}
			last = entryRow
			valueLen, n, err := readColumnRetainedSemanticStreamV1Uvarint(raw, off, "value length")
			if err != nil {
				return err
			}
			off += n
			entryMetadataBytes += n
			if valueLen > uint64(len(raw)-off) {
				return errors.New("collections: truncated semantic-stream-v1 retained block value")
			}
			scalarValueBytes += int64(valueLen)
			if int(valueLen) > maxScalarValueBytes {
				maxScalarValueBytes = int(valueLen)
			}
			off += int(valueLen)
		}
		pathRaw := raw[pathStart:off]
		path := strings.Join(segments, ".")
		pathKey := columnRetainedSemanticStreamPathKey(segments)
		if err := c.observePath(pathKey, path, int64(pathMetadataBytes), int64(entryMetadataBytes), scalarValueBytes, int64(entryCount), maxScalarValueBytes, pathRaw); err != nil {
			return err
		}
	}
	if off != len(raw) {
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
	if !ok {
		stat.EncodeErrors++
		stat.StoredBytes += int64(len(raw))
		stat.RawFallbackBlocks++
		return
	}
	stat.EncodedBytes += int64(encodedBytes)
	if encodedBytes > 0 && encodedBytes < len(raw) {
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
		dst := make([]byte, len(raw))
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err != nil {
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

func (c *columnRetainedSemanticStreamV1BlockLayoutCollector) observePath(pathKey, path string, pathMetadataBytes, entryMetadataBytes, scalarValueBytes, occurrences int64, maxScalarValueBytes int, raw []byte) error {
	stream := c.paths[pathKey]
	if stream == nil {
		stream = &columnRetainedSemanticStreamV1PathLayoutCollector{
			stat: ColumnRetainedSemanticStreamV1PathLayoutStat{Path: path},
		}
		if !c.pathOnly {
			zstdWriter, err := zstd.NewWriter(&stream.zstdCounter,
				zstd.WithEncoderLevel(zstd.SpeedFastest),
				zstd.WithEncoderCRC(false),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				return fmt.Errorf("collections: create semantic-stream-v1 path zstd oracle for path %q: %w", path, err)
			}
			stream.zstdWriter = zstdWriter
		}
		c.paths[pathKey] = stream
	}
	if c.pathOnly {
		return nil
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
		StoredBlockBytes:    c.storedBlockBytes,
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
	type pathLayoutResult struct {
		key  string
		stat ColumnRetainedSemanticStreamV1PathLayoutStat
	}
	paths := make([]pathLayoutResult, 0, len(c.paths))
	for pathKey, stream := range c.paths {
		if stream.zstdWriter != nil {
			if err := stream.zstdWriter.Close(); err != nil {
				return ColumnRetainedSemanticStreamV1BlockLayoutAudit{}, fmt.Errorf("collections: close semantic-stream-v1 path zstd oracle for path %q: %w", stream.stat.Path, err)
			}
			stream.zstdWriter = nil
		}
		stat := stream.stat
		stat.ZSTDBytes = stream.zstdCounter.n
		stat.ZSTDToTotalRatio = columnRetainedPayloadAuditRatio(stat.ZSTDBytes, stat.TotalBytes)
		out.PathZSTDInputBytes += stat.TotalBytes
		out.PathZSTDEncodedBytes += stat.ZSTDBytes
		paths = append(paths, pathLayoutResult{key: pathKey, stat: stat})
	}
	sort.Slice(paths, func(i, j int) bool {
		if paths[i].stat.TotalBytes != paths[j].stat.TotalBytes {
			return paths[i].stat.TotalBytes > paths[j].stat.TotalBytes
		}
		if paths[i].stat.Occurrences != paths[j].stat.Occurrences {
			return paths[i].stat.Occurrences > paths[j].stat.Occurrences
		}
		if paths[i].stat.Path != paths[j].stat.Path {
			return paths[i].stat.Path < paths[j].stat.Path
		}
		return paths[i].key < paths[j].key
	})
	outPaths := make([]ColumnRetainedSemanticStreamV1PathLayoutStat, 0, len(paths))
	for _, path := range paths {
		outPaths = append(outPaths, path.stat)
	}
	if maxPaths > 0 && len(paths) > maxPaths {
		out.Paths = outPaths[:maxPaths]
		out.PathsTruncated = true
	} else {
		out.Paths = outPaths
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
