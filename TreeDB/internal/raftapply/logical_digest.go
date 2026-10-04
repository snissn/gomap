package raftapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const logicalDigestDomainV1 = "TreeDB/R3a/LogicalDigestV1\x00"

// LogicalDigestV1 is a convergence digest over the supported logical catalog
// state. It intentionally excludes physical root/page IDs, WAL LSNs, value-log
// RIDs, segment names, and filesystem paths.
type LogicalDigestV1 [32]byte

func (d LogicalDigestV1) Hex() string {
	return hex.EncodeToString(d[:])
}

type LogicalDigestOptionsV1 struct {
	ScopeRule     raftentry.ScopeRuleV1
	DatabaseScope string
	CatalogScope  string
}

// LogicalDigestV1ForDB hashes the canonical catalog-create payload and
// materialized collection contents for each listed collection plus stable scope
// identity. The source of truth is the collection catalog API, not local
// storage layout.
func LogicalDigestV1ForDB(db *backenddb.DB, opts LogicalDigestOptionsV1) (LogicalDigestV1, error) {
	if db == nil {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil DB cannot compute logical digest")
	}
	return logicalDigestV1ForCollectionManager(collections.NewCommandWALReplayCollectionManager(db), opts)
}

// LogicalDigestV1ForSnapshotDB hashes an immutable staged snapshot with bounded
// document-ID memory. Its primary-key iterator is ordered; a count pass followed
// by a hash pass preserves the V1 count-before-record encoding. The caller must
// prevent writes for both passes (production uses a staged read-only DB).
func LogicalDigestV1ForSnapshotDB(db *backenddb.DB, opts LogicalDigestOptionsV1) (LogicalDigestV1, error) {
	return LogicalDigestV1ForSnapshotDBContext(context.Background(), db, opts)
}

// LogicalDigestV1ForSnapshotDBContext permits cancellation during both ordered
// document scans. Ordinary live digest calculation retains its existing path.
func LogicalDigestV1ForSnapshotDBContext(ctx context.Context, db *backenddb.DB, opts LogicalDigestOptionsV1) (LogicalDigestV1, error) {
	if ctx == nil {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil snapshot context")
	}
	if db == nil {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil snapshot DB")
	}
	return logicalDigestV1ForCollectionManagerMode(collections.NewCommandWALReplayCollectionManager(db), opts, ctx)
}

func (h *Harness) logicalDigestV1(opts LogicalDigestOptionsV1) (LogicalDigestV1, error) {
	if h != nil && h.logicalDigestV1Fn != nil {
		return h.logicalDigestV1Fn(opts)
	}
	manager := h.replayCollectionManager()
	if manager == nil {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil collection manager cannot compute logical digest")
	}
	return logicalDigestV1ForCollectionManager(manager, opts)
}

func logicalDigestV1ForCollectionManager(manager *collections.CollectionManager, opts LogicalDigestOptionsV1) (LogicalDigestV1, error) {
	return logicalDigestV1ForCollectionManagerMode(manager, opts, nil)
}

func logicalDigestV1ForCollectionManagerMode(manager *collections.CollectionManager, opts LogicalDigestOptionsV1, snapshotContext context.Context) (LogicalDigestV1, error) {
	if manager == nil {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil collection manager cannot compute logical digest")
	}
	orderedSnapshot := snapshotContext != nil
	if orderedSnapshot {
		if err := snapshotContext.Err(); err != nil {
			return LogicalDigestV1{}, err
		}
	}
	scope := opts.ScopeRule
	if scope == "" {
		scope = raftentry.ScopeRuleSingleGroupV1
	}
	if scope != raftentry.ScopeRuleSingleGroupV1 {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsupportedScopeRuleV1, "raftapply: unsupported logical digest scope rule %q", scope)
	}
	database := opts.DatabaseScope
	if database == "" {
		database = raftentry.DatabaseScopeDefaultV1
	}
	catalog := opts.CatalogScope
	if catalog == "" {
		catalog = raftentry.CatalogScopeDefaultV1
	}
	metas, err := manager.ListCollections()
	if err != nil {
		return LogicalDigestV1{}, codeCollectionApplyError(err)
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Name < metas[j].Name
	})
	h := sha256.New()
	writeLogicalDigestField(h, "domain", []byte(logicalDigestDomainV1))
	writeLogicalDigestU64(h, "logical-version", 1)
	writeLogicalDigestField(h, "scope-rule", []byte(scope))
	writeLogicalDigestField(h, "database-scope", []byte(database))
	writeLogicalDigestField(h, "catalog-scope", []byte(catalog))
	writeLogicalDigestU64(h, "collection-count", uint64(len(metas)))
	for _, meta := range metas {
		if orderedSnapshot {
			if err := snapshotContext.Err(); err != nil {
				return LogicalDigestV1{}, err
			}
		}
		payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
		if err != nil {
			return LogicalDigestV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode logical catalog metadata for %q: %v", meta.Name, err)
		}
		writeLogicalDigestField(h, "catalog-create-collection-payload", payload)
		collection, err := manager.OpenCollection(meta.Name)
		if err != nil {
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
		var ids [][]byte
		var count uint64
		// The scan API selects supported column reconstruction and its bounded
		// windows. Hashing the full corpus remains O(N); avoid a visibility scan per ID.
		columnStore := meta.Options.ColumnStore
		scanColumnDocuments := !orderedSnapshot && columnStore != nil && columnStore.Enabled && columnStore.RetainedPayload != collections.ColumnRetainedPayloadFull
		truncated, err := collection.ScanDocumentIDsFunc(maxInt(), func(id []byte) (bool, error) {
			if orderedSnapshot {
				if err := snapshotContext.Err(); err != nil {
					return false, err
				}
			}
			count++
			if !orderedSnapshot && !scanColumnDocuments {
				ids = append(ids, id)
			}
			return true, nil
		})
		if err != nil {
			if orderedSnapshot && snapshotContext.Err() != nil {
				return LogicalDigestV1{}, snapshotContext.Err()
			}
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
		if truncated {
			return LogicalDigestV1{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: logical digest document scan for %q truncated", meta.Name)
		}
		if !orderedSnapshot && !scanColumnDocuments {
			sort.Slice(ids, func(i, j int) bool {
				return bytes.Compare(ids[i], ids[j]) < 0
			})
		}
		materializer, err := collection.NewStoredDocumentJSONMaterializer()
		if err != nil {
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
		metadataContext := snapshotContext
		if metadataContext == nil {
			metadataContext = context.Background()
		}
		splitState, err := collection.VectorPartitionSplitInsertLogicalStateV1(metadataContext)
		if err != nil {
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
		if len(splitState) != 0 {
			writeLogicalDigestU64(h, "collection-split-insert-field-count", uint64(len(splitState)))
			for i, value := range splitState {
				name := "collection-split-insert-key"
				if i%2 == 1 {
					name = "collection-split-insert-state"
				}
				writeLogicalDigestField(h, name, value)
			}
		}
		definitions := append([]collections.VectorIndexDefinition(nil), meta.VectorIndexes...)
		sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
		for _, definition := range definitions {
			completion, present, err := collection.VectorPartitionPrepareCompletionV1(definition.Name)
			if err != nil {
				return LogicalDigestV1{}, codeCollectionApplyError(err)
			}
			if !present {
				continue
			}
			writeLogicalDigestField(h, "collection-vector-prepare-index", []byte(definition.Name))
			// Actual committed identity plus refs-independent output belongs to
			// logical convergence. Local READY/manifest physical digests do not.
			payload, err := commitlog.EncodeVectorPreparePayloadV1(completion.Command)
			if err != nil {
				return LogicalDigestV1{}, codeCollectionApplyError(err)
			}
			writeLogicalDigestField(h, "collection-vector-prepare-command", payload)
			writeLogicalDigestField(h, "collection-vector-prepare-assets", []byte(completion.AssetSetDigest))
		}
		writeLogicalDigestU64(h, "collection-document-count", count)
		var documentScratch []byte
		var hashed uint64
		hashStoredDocument := func(id, document []byte) (bool, error) {
			hashed++
			jsonDoc, err := materializer.StoredDocumentJSON(document)
			if err != nil {
				return false, codeCollectionApplyError(err)
			}
			writeLogicalDigestField(h, "collection-document-id", id)
			writeLogicalDigestField(h, "collection-document-json", jsonDoc)
			return true, nil
		}
		hashDocument := func(id []byte) (bool, error) {
			if orderedSnapshot {
				if err := snapshotContext.Err(); err != nil {
					return false, err
				}
			}
			document, found, err := collection.GetInto(id, documentScratch[:0])
			if err != nil {
				return false, codeCollectionApplyError(err)
			}
			if !found {
				return false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: logical digest document %q disappeared from %q", string(id), meta.Name)
			}
			documentScratch = document
			return hashStoredDocument(id, document)
		}
		if scanColumnDocuments {
			truncated, err = collection.ScanDocumentsFunc(maxInt(), func(record collections.DocumentRecord) (bool, error) {
				return hashStoredDocument(record.ID, record.Document)
			})
		} else if orderedSnapshot {
			truncated, err = collection.ScanDocumentIDsFunc(maxInt(), hashDocument)
		} else {
			for _, id := range ids {
				if _, err = hashDocument(id); err != nil {
					break
				}
			}
		}
		if err != nil || truncated || hashed != count {
			_ = materializer.Close()
			if err != nil {
				if orderedSnapshot && snapshotContext.Err() != nil {
					return LogicalDigestV1{}, snapshotContext.Err()
				}
				return LogicalDigestV1{}, codeCollectionApplyError(err)
			}
			return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: snapshot document count changed or truncated")
		}
		if err := materializer.Close(); err != nil {
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
	}
	var out LogicalDigestV1
	copy(out[:], h.Sum(nil))
	return out, nil
}

type logicalDigestWriter interface {
	Write([]byte) (int, error)
}

func writeLogicalDigestField(w logicalDigestWriter, name string, value []byte) {
	writeLogicalDigestU64(w, "field-name-len", uint64(len(name)))
	_, _ = w.Write([]byte(name))
	writeLogicalDigestU64(w, "field-value-len", uint64(len(value)))
	_, _ = w.Write(value)
}

func writeLogicalDigestU64(w logicalDigestWriter, name string, value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	if name != "" {
		_, _ = w.Write([]byte(name))
	}
	_, _ = w.Write(buf[:])
}
