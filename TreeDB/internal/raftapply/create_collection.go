package raftapply

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func (h *Harness) applyCreateCollectionV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (raftentry.ApplyResultV1, error) {
	collectionMeta, payload, err := lowerCreateCollectionV1(entry)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	alreadyApplied, err := h.preflightCreateCollectionV1(collectionMeta, payload)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	frame, err := commandwalapply.CatalogCreateCollectionFrame(payload)
	if err != nil {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: lower catalog create collection: %v", err)
	}
	var handle commandwalapply.Handle
	handleAppended := false
	handleFinalized := false
	defer func() {
		if handleAppended && !handleFinalized {
			h.walApply.Abort(h.db, handle)
		}
	}()
	manager := h.replayCollectionManager()
	if manager == nil {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil collection manager cannot apply create collection")
	}
	if _, err := manager.CreateCollectionWithPreparedCommandWALIntent(collectionMeta, func() (*backenddb.CommandWALIntent, error) {
		var err error
		handle, _, err = h.walApply.Append(h.db, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL})
		if err != nil {
			return nil, codeCommandWALApplyError(err)
		}
		handleAppended = true
		intent := handle.CommandWALIntent()
		if intent == nil || handle.LSN() == 0 {
			return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: command WAL append did not return a usable intent")
		}
		return intent, nil
	}); err != nil {
		if _, ok := ErrorCodeOf(err); ok {
			return raftentry.ApplyResultV1{}, err
		}
		return raftentry.ApplyResultV1{}, codeCollectionApplyError(err)
	}
	if !handleAppended {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: create collection did not append a command WAL frame")
	}
	logical, err := h.logicalDigestV1(LogicalDigestOptionsV1{
		ScopeRule:     meta.ScopeRule,
		DatabaseScope: meta.DatabaseScope,
		CatalogScope:  meta.CatalogScope,
	})
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if _, err := h.walApply.Finalize(h.db, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL}); err != nil {
		return raftentry.ApplyResultV1{}, codeCommandWALApplyError(err)
	}
	handleFinalized = true
	status := raftentry.ApplyStatusApplied
	affected := int64(1)
	if alreadyApplied {
		status = raftentry.ApplyStatusAlreadyApplied
		affected = 0
	}
	return raftentry.ApplyResultV1{
		Status:                 status,
		CommandDigest:          entry.Digest,
		DeterministicErrorCode: raftentry.ErrorNoneV1,
		AffectedCount:          affected,
		ResultDigest:           raftentry.CommandDigestV1(logical),
	}, nil
}

func lowerCreateCollectionV1(entry raftentry.CommandEntryV1) (collections.CollectionMeta, []byte, error) {
	if entry.Target.CommandID != nativewire.CommandCreateCollection {
		return collections.CollectionMeta{}, nil, codedError(raftentry.ErrorUnsupportedCommandV1, "raftapply: unsupported command %d", entry.Target.CommandID)
	}
	meta, err := decodeCreateCollectionMetaV1(entry.Target.CollectionMeta)
	if err != nil {
		return collections.CollectionMeta{}, nil, err
	}
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		return collections.CollectionMeta{}, nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode catalog create collection payload: %v", err)
	}
	return meta, payload, nil
}

func (h *Harness) preflightCreateCollectionV1(meta collections.CollectionMeta, payload []byte) (bool, error) {
	if h == nil || h.db == nil {
		return false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil DB cannot preflight create collection")
	}
	manager := h.replayCollectionManager()
	if manager == nil {
		return false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil collection manager cannot preflight create collection")
	}
	existing, err := manager.OpenCollection(meta.Name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return false, nil
	}
	if err != nil {
		return false, codeCollectionApplyError(err)
	}
	existingPayload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(existing.Meta())
	if err != nil {
		return false, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode existing catalog metadata: %v", err)
	}
	if !bytes.Equal(existingPayload, payload) {
		return false, codedError(raftentry.ErrorRejectedConflictV1, "raftapply: collection %q already exists with incompatible metadata", meta.Name)
	}
	return true, nil
}

func decodeCreateCollectionMetaV1(raw []byte) (collections.CollectionMeta, error) {
	off := 0
	version, err := readCreateMetaUvarint(raw, &off, "collection_meta.version")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	if version != 1 {
		return collections.CollectionMeta{}, codedError(raftentry.ErrorUnsupportedVersionV1, "raftapply: collection_meta version %d", version)
	}
	name, err := readCreateMetaString(raw, &off, "collection name")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	documentFormat, err := readCreateMetaUvarint(raw, &off, "document_format")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	dataRootStorage, err := readCreateMetaUvarint(raw, &off, "data_root_storage")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	indexStateStorage, err := readCreateMetaUvarint(raw, &off, "index_state_storage")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	allowArray, err := readCreateMetaBool(raw, &off, "allow_array_values_in_index")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	disableIndexedMemtables, err := readCreateMetaBool(raw, &off, "disable_indexed_write_memtables")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	bufferedIndexedWrites, err := readCreateMetaBool(raw, &off, "buffered_indexed_writes")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxDocuments, err := readCreateMetaInt(raw, &off, "buffered_indexed_write_max_documents")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxBytes, err := readCreateMetaInt64(raw, &off, "buffered_indexed_write_max_bytes")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxRootRuns, err := readCreateMetaInt(raw, &off, "buffered_indexed_write_max_root_runs")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	asyncFlush, err := readCreateMetaBool(raw, &off, "buffered_indexed_async_flush")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	overlayRoots, err := readCreateMetaBool(raw, &off, "buffered_indexed_overlay_roots")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	maxQueued, err := readCreateMetaInt(raw, &off, "buffered_indexed_async_flush_max_queued_units")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	indexCount64, err := readCreateMetaUvarint(raw, &off, "index_count")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	if indexCount64 > uint64(maxInt()) {
		return collections.CollectionMeta{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: index count exceeds int capacity")
	}
	indexes := make([]collections.IndexDefinition, 0, int(indexCount64))
	for i := 0; i < int(indexCount64); i++ {
		idx, err := readCreateMetaIndex(raw, &off)
		if err != nil {
			return collections.CollectionMeta{}, err
		}
		indexes = append(indexes, idx)
	}
	if off != len(raw) {
		return collections.CollectionMeta{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: collection_meta has %d trailing bytes", len(raw)-off)
	}
	return collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			AllowArrayValuesInIndex:                 allowArray,
			DocumentFormat:                          decodeCreateDocumentFormat(documentFormat),
			DataRootStoragePolicy:                   decodeCreateRootStoragePolicy(dataRootStorage),
			IndexStateStoragePolicy:                 decodeCreateRootStoragePolicy(indexStateStorage),
			DisableIndexedWriteMemtables:            disableIndexedMemtables,
			BufferedIndexedWrites:                   bufferedIndexedWrites,
			BufferedIndexedWriteMaxDocuments:        maxDocuments,
			BufferedIndexedWriteMaxBytes:            maxBytes,
			BufferedIndexedWriteMaxRootRuns:         maxRootRuns,
			BufferedIndexedAsyncFlush:               asyncFlush,
			BufferedIndexedOverlayRoots:             overlayRoots,
			BufferedIndexedAsyncFlushMaxQueuedUnits: maxQueued,
		},
		Indexes: indexes,
	}, nil
}

func readCreateMetaIndex(raw []byte, off *int) (collections.IndexDefinition, error) {
	name, err := readCreateMetaString(raw, off, "index name")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	field, err := readCreateMetaString(raw, off, "index field")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	valueType, err := readCreateMetaUvarint(raw, off, "index value type")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	unique, err := readCreateMetaBool(raw, off, "unique")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	multiKey, err := readCreateMetaBool(raw, off, "multi_key")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	storagePolicy, err := readCreateMetaUvarint(raw, off, "index storage policy")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	return collections.IndexDefinition{
		Name:          name,
		Field:         field,
		ValueType:     decodeCreateIndexValueType(valueType),
		Unique:        unique,
		MultiKey:      multiKey,
		StoragePolicy: decodeCreateRootStoragePolicy(storagePolicy),
	}, nil
}

func readCreateMetaString(raw []byte, off *int, field string) (string, error) {
	length, err := readCreateMetaUvarint(raw, off, field+".length")
	if err != nil {
		return "", err
	}
	if length > uint64(len(raw)-*off) || length > uint64(maxInt()) {
		return "", codedError(raftentry.ErrorMalformedEntryV1, "raftapply: %s length exceeds remaining payload", field)
	}
	start := *off
	*off += int(length)
	return string(raw[start:*off]), nil
}

func readCreateMetaUvarint(raw []byte, off *int, field string) (uint64, error) {
	if off == nil || *off < 0 || *off > len(raw) {
		return 0, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid %s offset", field)
	}
	value, n := binary.Uvarint(raw[*off:])
	if n <= 0 {
		return 0, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid %s", field)
	}
	*off += n
	return value, nil
}

func readCreateMetaInt64(raw []byte, off *int, field string) (int64, error) {
	if off == nil || *off < 0 || *off > len(raw) {
		return 0, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid %s offset", field)
	}
	value, n := binary.Varint(raw[*off:])
	if n <= 0 {
		return 0, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid %s", field)
	}
	if value < 0 {
		return 0, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: %s cannot be negative", field)
	}
	*off += n
	return value, nil
}

func readCreateMetaInt(raw []byte, off *int, field string) (int, error) {
	value, err := readCreateMetaInt64(raw, off, field)
	if err != nil {
		return 0, err
	}
	if value > int64(maxInt()) {
		return 0, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: %s exceeds int capacity", field)
	}
	return int(value), nil
}

func readCreateMetaBool(raw []byte, off *int, field string) (bool, error) {
	if off == nil || *off < 0 || *off >= len(raw) {
		return false, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: missing %s", field)
	}
	value := raw[*off]
	*off = *off + 1
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid %s bool %d", field, value)
	}
}

func decodeCreateDocumentFormat(value uint64) collections.DocumentFormat {
	switch nativewire.DocumentFormat(value) {
	case nativewire.DocumentFormatJSON:
		return collections.DocumentFormatJSON
	case nativewire.DocumentFormatBSON:
		return collections.DocumentFormatBSON
	case nativewire.DocumentFormatTemplateV1:
		return collections.DocumentFormatTemplateV1
	default:
		return collections.DocumentFormatDefault
	}
}

func decodeCreateRootStoragePolicy(value uint64) collections.RootStoragePolicy {
	switch value {
	case 1:
		return collections.RootStorageFast
	case 2:
		return collections.RootStorageCompressed
	default:
		return collections.RootStorageDefault
	}
}

func decodeCreateIndexValueType(value uint64) collections.IndexValueType {
	switch value {
	case 2:
		return collections.IndexValueBool
	case 3:
		return collections.IndexValueInt64
	case 4:
		return collections.IndexValueDouble
	default:
		return collections.IndexValueString
	}
}

func codeCollectionApplyError(err error) error {
	switch {
	case errors.Is(err, backenddb.ErrReadOnly):
		return codedError(raftentry.ErrorReadOnlyV1, "%v", err)
	case errors.Is(err, backenddb.ErrClosed), errors.Is(err, backenddb.ErrRecoveryRequired), errors.Is(err, backenddb.ErrCommandWALUnsupported), errors.Is(err, backenddb.ErrCommandWALRejected):
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "%v", err)
	default:
		return codedError(raftentry.ErrorRejectedConflictV1, "%v", err)
	}
}

func codeCommandWALApplyError(err error) error {
	switch {
	case errors.Is(err, backenddb.ErrReadOnly):
		return codedError(raftentry.ErrorReadOnlyV1, "%v", err)
	case errors.Is(err, backenddb.ErrCommandWALUnsupported), errors.Is(err, backenddb.ErrCommandWALRejected), errors.Is(err, backenddb.ErrClosed), errors.Is(err, backenddb.ErrRecoveryRequired):
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "%v", err)
	default:
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "%v", err)
	}
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
