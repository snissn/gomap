package collections

import (
	"encoding/json"
	"fmt"
	"strconv"
	"unsafe"

	"github.com/buger/jsonparser"
	"github.com/snissn/gomap/TreeDB/batch"
)

const preparedInsertMaxSystemMetaJSONBytes = 64 << 10

const (
	preparedInsertMaxSystemDeltaRoots    = 8
	preparedInsertMaxSystemDeltaKeyBytes = 8 << 10
	// Three bounded manifest-progress replacements can grow the captured JSON
	// by at most 256 bytes each. The encoder checks this same bound.
	preparedInsertMaxPublishedMetaJSONBytes = preparedInsertMaxSystemMetaJSONBytes + 3*256
)

// preparedInsertSystemDeltaSourceLimit derives the complete key/value size
// before command WAL append. The builder adds one metadata value, one 8-byte
// root value per root, and optionally one 8-byte document-generation value.
// It may hold both map entries and cloned iterator entries until materialized.
func preparedInsertSystemDeltaSourceLimit(collection string, rootNames []string) (int, int64, error) {
	if len(rootNames) > preparedInsertMaxSystemDeltaRoots {
		return 0, 0, fmt.Errorf("%w: prepared system delta has %d roots", ErrPreparedInsertResourceLimit, len(rootNames))
	}
	keyBytes := len(systemCollectionMetaKey(collection))
	entries := 1 + len(rootNames)
	primaryRootName := collectionPrimaryRootName(collection)
	for _, rootName := range rootNames {
		if len(rootName) > preparedInsertMaxSystemDeltaKeyBytes {
			return 0, 0, fmt.Errorf("%w: prepared system root name is oversized", ErrPreparedInsertResourceLimit)
		}
		keyBytes += len(systemCollectionRootKey(rootName))
		if rootName == primaryRootName {
			entries++
			keyBytes += len(systemCollectionDocumentGenerationKey(collection))
		}
	}
	if keyBytes > preparedInsertMaxSystemDeltaKeyBytes || entries > preparedInsertMaxSystemDeltaRoots+2 {
		return 0, 0, fmt.Errorf("%w: prepared system delta keys exceed the source bound", ErrPreparedInsertResourceLimit)
	}
	payloadBytes := int64(keyBytes + preparedInsertMaxPublishedMetaJSONBytes + 8*(entries-1))
	entryBytes := int64(entries) * int64(unsafe.Sizeof(batch.Entry{}))
	return entries, payloadBytes + entryBytes, nil
}

// encodePreparedColumnPublishedMeta patches the bounded, captured catalog
// value. The late system-delta builder cannot use encoding/json.Marshal: its
// process-wide encodeState pool can lend it a buffer whose capacity is
// unrelated to this request's admitted metadata size.
func encodePreparedColumnPublishedMeta(raw []byte, base, updated CollectionMeta) ([]byte, error) {
	if len(raw) == 0 || len(raw) > preparedInsertMaxSystemMetaJSONBytes || !json.Valid(raw) ||
		base.Options.ColumnStore == nil || updated.Options.ColumnStore == nil {
		return nil, fmt.Errorf("%w: prepared system metadata is absent, oversized, or invalid", ErrPreparedInsertResourceLimit)
	}
	columnStore, kind, _, err := jsonparser.Get(raw, "options", "column_store")
	if err != nil || kind != jsonparser.Object || len(columnStore) == 0 {
		return nil, fmt.Errorf("%w: prepared system metadata has no column_store object", ErrPreparedInsertResourceLimit)
	}
	decoded, err := decodeCollectionMeta(raw)
	if err != nil || !sameCollectionMeta(decoded, base) ||
		!sameCollectionMetaIgnoringColumnManifestProgress(base, updated) ||
		base.Options.ColumnStore.PhysicalMutationParts != updated.Options.ColumnStore.PhysicalMutationParts {
		return nil, fmt.Errorf("%w: prepared system metadata differs from the captured schema", ErrPreparedInsertResourceLimit)
	}
	cfg := updated.Options.ColumnStore
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		return nil, fmt.Errorf("%w: prepared system metadata lacks manifest identities", ErrPreparedInsertResourceLimit)
	}
	active, err := preparedManifestIdentityJSON(*cfg.ActiveManifest)
	if err != nil {
		return nil, err
	}
	recovery, err := preparedManifestIdentityJSON(*cfg.RecoveryAuthoritativeManifest)
	if err != nil {
		return nil, err
	}
	lsn := strconv.AppendUint(make([]byte, 0, 20), cfg.RecoveryAuthoritativeAppliedCommandLSN, 10)
	// Set may keep the old value while allocating the next one, so the caller
	// reserves three full bounded JSON images plus these small replacements.
	out, err := jsonparser.Set(raw, active, "options", "column_store", "active_manifest")
	if err == nil {
		out, err = jsonparser.Set(out, recovery, "options", "column_store", "recovery_authoritative_manifest")
	}
	if err == nil {
		out, err = jsonparser.Set(out, lsn, "options", "column_store", "recovery_authoritative_applied_command_lsn")
	}
	if err != nil || len(out) > preparedInsertMaxPublishedMetaJSONBytes {
		return nil, fmt.Errorf("%w: prepared system metadata patch exceeds bound: %v", ErrPreparedInsertResourceLimit, err)
	}
	decoded, err = decodeCollectionMeta(out)
	if err != nil || !sameCollectionMeta(decoded, updated) {
		return nil, fmt.Errorf("%w: prepared system metadata patch changed the schema", ErrPreparedInsertResourceLimit)
	}
	return out, nil
}

func preparedManifestIdentityJSON(identity ColumnManifestIdentity) ([]byte, error) {
	if len(identity.Format) > 32 {
		return nil, fmt.Errorf("%w: prepared manifest identity format is oversized", ErrPreparedInsertResourceLimit)
	}
	result := make([]byte, 0, 160)
	result = append(result, '{')
	comma := false
	if identity.Generation != 0 {
		result = append(result, `"generation":`...)
		result = strconv.AppendUint(result, identity.Generation, 10)
		comma = true
	}
	if identity.Format != "" {
		if comma {
			result = append(result, ',')
		}
		result = append(result, `"format":`...)
		result = strconv.AppendQuote(result, identity.Format)
		comma = true
	}
	if identity.Version != 0 {
		if comma {
			result = append(result, ',')
		}
		result = append(result, `"version":`...)
		result = strconv.AppendUint(result, uint64(identity.Version), 10)
		comma = true
	}
	if identity.Checksum != 0 {
		if comma {
			result = append(result, ',')
		}
		result = append(result, `"checksum":`...)
		result = strconv.AppendUint(result, identity.Checksum, 10)
	}
	return append(result, '}'), nil
}
