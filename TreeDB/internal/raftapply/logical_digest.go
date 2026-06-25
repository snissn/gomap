package raftapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	if manager == nil {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: nil collection manager cannot compute logical digest")
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
		payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
		if err != nil {
			return LogicalDigestV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: encode logical catalog metadata for %q: %v", meta.Name, err)
		}
		writeLogicalDigestField(h, "catalog-create-collection-payload", payload)
		collection, err := manager.OpenCollection(meta.Name)
		if err != nil {
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
		records := make([]collections.DocumentRecord, 0)
		truncated, err := collection.ScanDocumentsFunc(maxInt(), func(record collections.DocumentRecord) (bool, error) {
			records = append(records, record)
			return true, nil
		})
		if err != nil {
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
		if truncated {
			return LogicalDigestV1{}, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: logical digest document scan for %q truncated", meta.Name)
		}
		sort.Slice(records, func(i, j int) bool {
			return bytes.Compare(records[i].ID, records[j].ID) < 0
		})
		materializer, err := collection.NewStoredDocumentJSONMaterializer()
		if err != nil {
			return LogicalDigestV1{}, codeCollectionApplyError(err)
		}
		writeLogicalDigestU64(h, "collection-document-count", uint64(len(records)))
		for _, record := range records {
			jsonDoc, err := materializer.StoredDocumentJSON(record.Document)
			if err != nil {
				_ = materializer.Close()
				return LogicalDigestV1{}, codeCollectionApplyError(err)
			}
			writeLogicalDigestField(h, "collection-document-id", record.ID)
			writeLogicalDigestField(h, "collection-document-json", jsonDoc)
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
