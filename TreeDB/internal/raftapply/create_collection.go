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

const (
	createCollectionMetaMaxWireVersion           = 5
	createCollectionMetaMaxIndexDefinitions      = 1 << 16
	createCollectionMetaMinIndexDefinitionLen    = 6
	createCollectionMetaMinVectorDefinitionLen   = 7
	createCollectionMetaMaxQuantizedIndexVersion = uint64(^uint32(0))
)

func (h *Harness) applyCreateCollectionV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (raftentry.ApplyResultV1, error) {
	expectedCatalogVersion, err := decodeExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	collectionMeta, payload, err := lowerCreateCollectionV1(entry)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	alreadyExisting, err := h.preflightCreateCollectionV1(collectionMeta, payload)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if !alreadyExisting {
		if err := checkCatalogVersionGuardV1(meta, expectedCatalogVersion); err != nil {
			return raftentry.ApplyResultV1{}, err
		}
	}
	checkPublishCatalogVersion := func() error {
		return checkCatalogVersionGuardV1(meta, expectedCatalogVersion)
	}
	if alreadyExisting {
		checkPublishCatalogVersion = nil
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
	postAppendFaultRequiresRecovery := false
	_, alreadyApplied, err := manager.CreateCollectionWithPreparedCommandWALIntentStatusAndPreflight(collectionMeta, func() (*backenddb.CommandWALIntent, error) {
		var err error
		handle, _, err = h.walApply.Append(h.db, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL})
		if err != nil {
			return nil, codeCommandWALApplyError(err)
		}
		handleAppended = true
		if err := h.injectFault(FaultAfterLocalWALAppendBeforeVisibleV1, meta.EntryID, entry.Digest); err != nil {
			postAppendFaultRequiresRecovery = true
			return nil, err
		}
		intent := handle.CommandWALIntent()
		if intent == nil || handle.LSN() == 0 {
			return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: command WAL append did not return a usable intent")
		}
		return intent, nil
	}, checkPublishCatalogVersion)
	if err != nil {
		if postAppendFaultRequiresRecovery {
			return commandWALPostAppendRecoveryRequired(entry, err)
		}
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
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	if _, err := h.walApply.Finalize(h.db, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL}); err != nil {
		return commandWALFinalizeRecoveryRequired(entry, err)
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

func decodeExpectedCatalogVersionV1(raw []byte) (uint64, error) {
	if len(raw) == 0 {
		return 0, codedError(raftentry.ErrorMissingGuardV1, "raftapply: missing expected catalog version")
	}
	value, n := binary.Uvarint(raw)
	if n <= 0 {
		return 0, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: malformed expected catalog version")
	}
	if n != len(raw) {
		return 0, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: expected catalog version has %d trailing bytes", len(raw)-n)
	}
	return value, nil
}

func checkCatalogVersionGuardV1(meta ApplyMetadataV1, expected uint64) error {
	if !meta.HasCurrentCatalogVersion {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: missing current catalog version for guard check")
	}
	if meta.CurrentCatalogVersion != expected {
		return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: catalog version %d does not match expected %d", meta.CurrentCatalogVersion, expected)
	}
	return nil
}

func lowerCreateCollectionV1(entry raftentry.CommandEntryV1) (collections.CollectionMeta, []byte, error) {
	if entry.Target.CommandID != nativewire.CommandCreateCollection {
		return collections.CollectionMeta{}, nil, codedError(raftentry.ErrorUnsupportedCommandV1, "raftapply: unsupported command %d", entry.Target.CommandID)
	}
	meta, err := decodeCreateCollectionMetaV1(entry.Target.CollectionMeta)
	if err != nil {
		return collections.CollectionMeta{}, nil, err
	}
	meta = forceNativewireCreateStoragePoliciesV1(meta)
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		return collections.CollectionMeta{}, nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode catalog create collection payload: %v", err)
	}
	return meta, payload, nil
}

func forceNativewireCreateStoragePoliciesV1(meta collections.CollectionMeta) collections.CollectionMeta {
	meta.Options.DataRootStoragePolicy = collections.RootStorageFast
	meta.Options.IndexStateStoragePolicy = collections.RootStorageFast
	for i := range meta.Indexes {
		meta.Indexes[i].StoragePolicy = collections.RootStorageFast
	}
	return meta
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
	if version < 1 || version > createCollectionMetaMaxWireVersion {
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
	if indexCount64 > createCollectionMetaMaxIndexDefinitions {
		return collections.CollectionMeta{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: index count %d exceeds limit %d", indexCount64, createCollectionMetaMaxIndexDefinitions)
	}
	if indexCount64 > uint64((len(raw)-off)/createCollectionMetaMinIndexDefinitionLen) {
		return collections.CollectionMeta{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: index count %d exceeds remaining collection_meta payload", indexCount64)
	}
	documentFormatDecoded, err := decodeCreateDocumentFormat(documentFormat)
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	dataRootStoragePolicy, err := decodeCreateRootStoragePolicy(dataRootStorage, "data_root_storage")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	indexStateStoragePolicy, err := decodeCreateRootStoragePolicy(indexStateStorage, "index_state_storage")
	if err != nil {
		return collections.CollectionMeta{}, err
	}
	indexes := make([]collections.IndexDefinition, 0, int(indexCount64))
	for i := 0; i < int(indexCount64); i++ {
		idx, err := readCreateMetaIndex(raw, &off)
		if err != nil {
			return collections.CollectionMeta{}, err
		}
		indexes = append(indexes, idx)
	}
	var vectorIndexes []collections.VectorIndexDefinition
	if version >= 2 {
		vectorIndexCount64, err := readCreateMetaUvarint(raw, &off, "vector_index_count")
		if err != nil {
			return collections.CollectionMeta{}, err
		}
		if vectorIndexCount64 > uint64(maxInt()) {
			return collections.CollectionMeta{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: vector index count exceeds int capacity")
		}
		if vectorIndexCount64 > createCollectionMetaMaxIndexDefinitions {
			return collections.CollectionMeta{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: vector index count %d exceeds limit %d", vectorIndexCount64, createCollectionMetaMaxIndexDefinitions)
		}
		if vectorIndexCount64 > uint64((len(raw)-off)/createCollectionMetaMinVectorDefinitionLen) {
			return collections.CollectionMeta{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: vector index count %d exceeds remaining collection_meta payload", vectorIndexCount64)
		}
		vectorIndexes = make([]collections.VectorIndexDefinition, 0, int(vectorIndexCount64))
		for i := 0; i < int(vectorIndexCount64); i++ {
			idx, err := readCreateMetaVectorIndex(raw, &off, version)
			if err != nil {
				return collections.CollectionMeta{}, err
			}
			vectorIndexes = append(vectorIndexes, idx)
		}
	}
	if off != len(raw) {
		return collections.CollectionMeta{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: collection_meta has %d trailing bytes", len(raw)-off)
	}
	return collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			AllowArrayValuesInIndex:                 allowArray,
			DocumentFormat:                          documentFormatDecoded,
			DataRootStoragePolicy:                   dataRootStoragePolicy,
			IndexStateStoragePolicy:                 indexStateStoragePolicy,
			DisableIndexedWriteMemtables:            disableIndexedMemtables,
			BufferedIndexedWrites:                   bufferedIndexedWrites,
			BufferedIndexedWriteMaxDocuments:        maxDocuments,
			BufferedIndexedWriteMaxBytes:            maxBytes,
			BufferedIndexedWriteMaxRootRuns:         maxRootRuns,
			BufferedIndexedAsyncFlush:               asyncFlush,
			BufferedIndexedOverlayRoots:             overlayRoots,
			BufferedIndexedAsyncFlushMaxQueuedUnits: maxQueued,
		},
		Indexes:       indexes,
		VectorIndexes: vectorIndexes,
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
	valueTypeDecoded, err := decodeCreateIndexValueType(valueType)
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	storagePolicyDecoded, err := decodeCreateRootStoragePolicy(storagePolicy, "index storage policy")
	if err != nil {
		return collections.IndexDefinition{}, err
	}
	return collections.IndexDefinition{
		Name:          name,
		Field:         field,
		ValueType:     valueTypeDecoded,
		Unique:        unique,
		MultiKey:      multiKey,
		StoragePolicy: storagePolicyDecoded,
	}, nil
}

func readCreateMetaVectorIndex(raw []byte, off *int, version uint64) (collections.VectorIndexDefinition, error) {
	name, err := readCreateMetaString(raw, off, "vector index name")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	field, err := readCreateMetaString(raw, off, "vector index field")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	metric, err := readCreateMetaUvarint(raw, off, "vector index metric")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	dimensions, err := readCreateMetaInt(raw, off, "vector index dimensions")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	m, err := readCreateMetaInt(raw, off, "vector index m")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	efConstruction, err := readCreateMetaInt(raw, off, "vector index ef_construction")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	efSearch, err := readCreateMetaInt(raw, off, "vector index ef_search")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	encoding, err := readCreateMetaUvarint(raw, off, "vector index encoding")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	decodedMetric, err := decodeCreateVectorMetric(metric)
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	decodedEncoding, err := decodeCreateVectorIndexEncoding(encoding)
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	def := collections.VectorIndexDefinition{
		Name:           name,
		Field:          field,
		Metric:         decodedMetric,
		Dimensions:     dimensions,
		M:              m,
		EfConstruction: efConstruction,
		EfSearch:       efSearch,
		Encoding:       decodedEncoding,
	}
	if version < 3 {
		return def, nil
	}
	strategy, err := readCreateMetaUvarint(raw, off, "vector index strategy")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	decodedStrategy, err := decodeCreateVectorIndexStrategy(strategy)
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	quantizedCount64, err := readCreateMetaUvarint(raw, off, "quantized_index_count")
	if err != nil {
		return collections.VectorIndexDefinition{}, err
	}
	if quantizedCount64 > uint64(maxInt()) {
		return collections.VectorIndexDefinition{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: quantized vector index count exceeds int capacity")
	}
	if quantizedCount64 > createCollectionMetaMaxIndexDefinitions {
		return collections.VectorIndexDefinition{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: quantized vector index count %d exceeds limit %d", quantizedCount64, createCollectionMetaMaxIndexDefinitions)
	}
	def.Strategy = decodedStrategy
	def.QuantizedIndexes = make([]collections.QuantizedVectorIndexDefinition, 0, int(quantizedCount64))
	for i := 0; i < int(quantizedCount64); i++ {
		q, err := readCreateMetaQuantizedVectorIndex(raw, off, version, def.Name, i)
		if err != nil {
			return collections.VectorIndexDefinition{}, err
		}
		def.QuantizedIndexes = append(def.QuantizedIndexes, q)
	}
	return def, nil
}

func readCreateMetaQuantizedVectorIndex(raw []byte, off *int, version uint64, vectorIndexName string, index int) (collections.QuantizedVectorIndexDefinition, error) {
	name, err := readCreateMetaString(raw, off, "quantized index name")
	if err != nil {
		return collections.QuantizedVectorIndexDefinition{}, err
	}
	codec, err := readCreateMetaString(raw, off, "quantized index codec")
	if err != nil {
		return collections.QuantizedVectorIndexDefinition{}, err
	}
	codecVersion, err := readCreateMetaUvarint(raw, off, "quantized index version")
	if err != nil {
		return collections.QuantizedVectorIndexDefinition{}, err
	}
	if codecVersion > createCollectionMetaMaxQuantizedIndexVersion {
		return collections.QuantizedVectorIndexDefinition{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: quantized vector index version %d exceeds uint32 capacity", codecVersion)
	}
	var scalarU8Calibration *collections.ScalarU8CalibrationConfig
	if version >= 4 {
		hasScalarU8Calibration, err := readCreateMetaBool(raw, off, "scalar_u8_calibration")
		if err != nil {
			return collections.QuantizedVectorIndexDefinition{}, err
		}
		if hasScalarU8Calibration {
			mode, err := readCreateMetaString(raw, off, "scalar_u8_calibration.mode")
			if err != nil {
				return collections.QuantizedVectorIndexDefinition{}, err
			}
			grouping, err := readCreateMetaString(raw, off, "scalar_u8_calibration.grouping")
			if err != nil {
				return collections.QuantizedVectorIndexDefinition{}, err
			}
			policyName, err := readCreateMetaString(raw, off, "scalar_u8_calibration.alpha_policy.name")
			if err != nil {
				return collections.QuantizedVectorIndexDefinition{}, err
			}
			quantilePPM, err := readCreateMetaUvarint(raw, off, "scalar_u8_calibration.alpha_policy.quantile_ppm")
			if err != nil {
				return collections.QuantizedVectorIndexDefinition{}, err
			}
			if quantilePPM > createCollectionMetaMaxQuantizedIndexVersion {
				return collections.QuantizedVectorIndexDefinition{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: scalar_u8 alpha policy quantile_ppm %d exceeds uint32 capacity", quantilePPM)
			}
			scalarU8Calibration = &collections.ScalarU8CalibrationConfig{
				Mode:     collections.ScalarU8CalibrationMode(mode),
				Grouping: collections.ScalarU8CalibrationGrouping(grouping),
				AlphaPolicy: collections.ScalarU8AlphaPolicy{
					Name:        collections.ScalarU8AlphaPolicyName(policyName),
					QuantilePPM: uint32(quantilePPM),
				},
			}
		}
	}
	q := collections.QuantizedVectorIndexDefinition{
		Name:                name,
		Codec:               codec,
		Version:             uint32(codecVersion),
		ScalarU8Calibration: scalarU8Calibration,
	}
	if q.ScalarU8Calibration != nil {
		normalizedCalibration, err := collections.NormalizeScalarU8CalibrationConfig(vectorIndexName, index, q)
		if err != nil {
			return collections.QuantizedVectorIndexDefinition{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: invalid scalar_u8 calibration: %v", err)
		}
		q.ScalarU8Calibration = normalizedCalibration
	}
	return q, nil
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

func decodeCreateDocumentFormat(value uint64) (collections.DocumentFormat, error) {
	switch nativewire.DocumentFormat(value) {
	case nativewire.DocumentFormatDefault:
		return collections.DocumentFormatDefault, nil
	case nativewire.DocumentFormatJSON:
		return collections.DocumentFormatJSON, nil
	case nativewire.DocumentFormatBSON:
		return collections.DocumentFormatBSON, nil
	case nativewire.DocumentFormatTemplateV1:
		return collections.DocumentFormatTemplateV1, nil
	default:
		return collections.DocumentFormatDefault, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: unsupported document_format enum %d", value)
	}
}

func decodeCreateRootStoragePolicy(value uint64, field string) (collections.RootStoragePolicy, error) {
	switch value {
	case 0:
		return collections.RootStorageDefault, nil
	case 1:
		return collections.RootStorageFast, nil
	case 2:
		return collections.RootStorageCompressed, nil
	default:
		return collections.RootStorageDefault, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: unsupported %s enum %d", field, value)
	}
}

func decodeCreateIndexValueType(value uint64) (collections.IndexValueType, error) {
	switch value {
	case 1:
		return collections.IndexValueString, nil
	case 2:
		return collections.IndexValueBool, nil
	case 3:
		return collections.IndexValueInt64, nil
	case 4:
		return collections.IndexValueDouble, nil
	default:
		return collections.IndexValueString, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: unsupported index value type enum %d", value)
	}
}

func decodeCreateVectorMetric(value uint64) (collections.VectorMetric, error) {
	switch value {
	case 1:
		return collections.VectorMetricCosine, nil
	case 2:
		return collections.VectorMetricL2, nil
	case 3:
		return collections.VectorMetricInnerProduct, nil
	default:
		return collections.VectorMetricCosine, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: unsupported vector metric enum %d", value)
	}
}

func decodeCreateVectorIndexEncoding(value uint64) (collections.VectorIndexEncoding, error) {
	switch value {
	case 1:
		return collections.VectorIndexEncodingFloat32, nil
	case 2:
		return collections.VectorIndexEncodingInt8, nil
	default:
		return collections.VectorIndexEncodingFloat32, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: unsupported vector index encoding enum %d", value)
	}
}

func decodeCreateVectorIndexStrategy(value uint64) (collections.VectorIndexStrategy, error) {
	switch value {
	case 1:
		return collections.VectorIndexStrategyNativeRuntime, nil
	case 2:
		return collections.VectorIndexStrategyColumnGraph, nil
	default:
		return "", codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: unsupported vector index strategy enum %d", value)
	}
}

func codeCollectionApplyError(err error) error {
	switch {
	case errors.Is(err, backenddb.ErrReadOnly):
		return codedError(raftentry.ErrorReadOnlyV1, "%w", err)
	case errors.Is(err, backenddb.ErrClosed), errors.Is(err, backenddb.ErrRecoveryRequired), errors.Is(err, backenddb.ErrCommandWALUnsupported), errors.Is(err, backenddb.ErrCommandWALRejected):
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "%w", err)
	default:
		return codedError(raftentry.ErrorRejectedConflictV1, "%w", err)
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
