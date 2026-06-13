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

	backenddb "github.com/snissn/gomap/TreeDB/db"
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

func resolveColumnRetainedPayloadAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, cfg ColumnStoreConfig, retained []byte) ([]byte, error) {
	if !columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg) {
		return retained, nil
	}
	blockKey, row, ok, err := parseColumnRetainedSemanticStreamV1Locator(bytes.TrimSpace(retained))
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
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return nil, fmt.Errorf("collections: decode semantic-stream-v1 retained value: %w", err)
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
