package collections

import (
	"fmt"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/node"
)

var registerCommandWALReplayHandlersOnce sync.Once

func init() {
	RegisterCommandWALReplayHandlers()
}

// RegisterCommandWALReplayHandlers installs collection command-WAL replay
// handlers for binaries that want deterministic registration instead of relying
// on a package side-effect import before opening a command_wal_v2 directory.
func RegisterCommandWALReplayHandlers() {
	registerCommandWALReplayHandlersOnce.Do(func() {
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionInsertBatchByID, replayCollectionInsertBatchByIDCommandWAL)
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionDeleteBatchByID, replayCollectionDeleteBatchByIDCommandWAL)
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionUpdateBatchByID, replayCollectionUpdateBatchByIDCommandWAL)
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionReplaceSourceByID, replayCollectionReplaceSourceByIDCommandWAL)
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionRebuildVectorIndex, replayCollectionRebuildVectorIndexCommandWAL)
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCatalogCreateCollection, replayCatalogCreateCollectionCommandWAL)
	})
}

// NewCommandWALReplayCollectionManager returns a collection manager for
// command-WAL replay and deterministic apply paths. It intentionally skips the
// backend publish-barrier and close-hook registrations used by public live
// managers so replay/apply can own those boundaries explicitly.
func NewCommandWALReplayCollectionManager(db *backenddb.DB) *CollectionManager {
	return newCollectionManager(db, collectionManagerOptions{})
}

func newCommandWALReplayCollectionManager(db *backenddb.DB) *CollectionManager {
	return NewCommandWALReplayCollectionManager(db)
}

func (c *Collection) commandWALActive(intent *backenddb.CommandWALIntent) bool {
	return intent != nil || (c != nil && c.db != nil && c.db.CommandWALEnabled())
}

func (c *Collection) newCollectionInsertCommandWALIntent(docs []commitlog.CollectionDocument, replay *backenddb.CommandWALIntent) (*backenddb.CommandWALIntent, error) {
	if replay != nil {
		return replay, nil
	}
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() {
		return nil, nil
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(c.meta.Name, docs)
	if err != nil {
		return nil, err
	}
	return c.db.NewTrustedCommandWALIntent(
		commitlog.CommandKindCollectionInsertBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionInsertBatchByIDV1,
		payload,
	)
}

func (c *Collection) newCollectionDeleteCommandWALIntent(ids [][]byte, replay *backenddb.CommandWALIntent) (*backenddb.CommandWALIntent, error) {
	if replay != nil {
		return replay, nil
	}
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() {
		return nil, nil
	}
	payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload(c.meta.Name, ids)
	if err != nil {
		return nil, err
	}
	return c.db.NewTrustedCommandWALIntent(
		commitlog.CommandKindCollectionDeleteBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionDeleteBatchByIDV1,
		payload,
	)
}

func (c *Collection) newCollectionReplaceSourceCommandWALIntent(deleteIDs [][]byte, docs []commitlog.CollectionDocument, replay *backenddb.CommandWALIntent) (*backenddb.CommandWALIntent, error) {
	if replay != nil {
		return replay, nil
	}
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() {
		return nil, nil
	}
	payload, err := commitlog.EncodeCollectionReplaceSourceByIDPayload(c.meta.Name, deleteIDs, docs)
	if err != nil {
		return nil, err
	}
	return c.db.NewTrustedCommandWALIntent(
		commitlog.CommandKindCollectionReplaceSourceByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionReplaceSourceByIDV1,
		payload,
	)
}

func (c *Collection) newCollectionUpdateCommandWALIntent(docs []commitlog.CollectionDocument, replay *backenddb.CommandWALIntent) (*backenddb.CommandWALIntent, error) {
	if replay != nil {
		return replay, nil
	}
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() {
		return nil, nil
	}
	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload(c.meta.Name, docs)
	if err != nil {
		return nil, err
	}
	return c.db.NewTrustedCommandWALIntent(
		commitlog.CommandKindCollectionUpdateBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionUpdateBatchByIDV1,
		payload,
	)
}

func (c *Collection) newCollectionRebuildVectorIndexCommandWALIntent(indexName string, replay *backenddb.CommandWALIntent) (*backenddb.CommandWALIntent, error) {
	if replay != nil {
		return replay, nil
	}
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() {
		return nil, nil
	}
	payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload(c.meta.Name, indexName)
	if err != nil {
		return nil, err
	}
	return c.db.NewCommandWALIntent(
		commitlog.CommandKindCollectionRebuildVectorIndex,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionRebuildVectorIndexV1,
		payload,
	)
}

func (m *CollectionManager) newCatalogCreateCollectionCommandWALIntent(meta CollectionMeta, replay *backenddb.CommandWALIntent) (*backenddb.CommandWALIntent, error) {
	if replay != nil {
		return replay, nil
	}
	if m == nil || m.db == nil || !m.db.CommandWALEnabled() {
		return nil, nil
	}
	payload, err := EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		return nil, err
	}
	return m.db.NewCommandWALIntent(
		commitlog.CommandKindCatalogCreateCollection,
		commitlog.CommandScopeCatalog,
		commitlog.PayloadFormatCatalogCreateCollectionV1,
		payload,
	)
}

// EncodeCatalogCreateCollectionCommandWALPayload returns the canonical local
// command-WAL payload used for catalog collection creates. R3a apply uses this
// as its lowering boundary before handing the pre-appended intent back to the
// normal catalog executor.
func EncodeCatalogCreateCollectionCommandWALPayload(meta CollectionMeta) ([]byte, error) {
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	encoded, err := encodeNormalizedCollectionMeta(normalized)
	if err != nil {
		return nil, err
	}
	return commitlog.EncodeCatalogCreateCollectionPayload(normalized.Name, encoded)
}

func collectionDocumentsFromNoIndexEntries(entries []noIndexBatchEntry) []commitlog.CollectionDocument {
	docs := make([]commitlog.CollectionDocument, len(entries))
	for i := range entries {
		docs[i] = commitlog.CollectionDocument{
			ID:       entries[i].id,
			Document: entries[i].document,
		}
	}
	return docs
}

func columnWriteDocumentsFromNoIndexEntries(entries []noIndexBatchEntry) []columnWriteDocument {
	docs := make([]columnWriteDocument, len(entries))
	for i := range entries {
		docs[i] = columnWriteDocument{
			ID:       entries[i].id,
			Document: entries[i].document,
		}
	}
	return docs
}

func columnWriteDocumentsFromCommitLog(docs []commitlog.CollectionDocument) []columnWriteDocument {
	if len(docs) == 0 {
		return nil
	}
	out := make([]columnWriteDocument, len(docs))
	for i := range docs {
		out[i] = columnWriteDocument{
			ID:       docs[i].ID,
			Document: docs[i].Document,
		}
	}
	return out
}

func columnWriteDocumentsFromIDs(ids [][]byte) []columnWriteDocument {
	if len(ids) == 0 {
		return nil
	}
	docs := make([]columnWriteDocument, len(ids))
	for i := range ids {
		docs[i] = columnWriteDocument{ID: ids[i]}
	}
	return docs
}

func collectionDocumentsFromBatchInput(ids, documents [][]byte) ([]commitlog.CollectionDocument, error) {
	if len(ids) != len(documents) {
		return nil, fmt.Errorf("collections: command wal batch ids length mismatch: ids=%d documents=%d", len(ids), len(documents))
	}
	docs := make([]commitlog.CollectionDocument, len(ids))
	for i := range ids {
		docs[i] = commitlog.CollectionDocument{
			ID:       ids[i],
			Document: documents[i],
		}
	}
	return docs, nil
}

func collectionDocumentsFromInsertPlan(plan *insertBatchPlan, primaryRootName string) ([]commitlog.CollectionDocument, error) {
	if plan == nil {
		return nil, fmt.Errorf("collections: missing insert plan for command wal")
	}
	if direct := plan.directBufferedInsert; direct != nil && direct.primaryRootName == primaryRootName {
		if len(direct.primaryEntries) != len(plan.resultIDs) {
			return nil, fmt.Errorf("collections: insert plan primary document count mismatch for command wal")
		}
		docs := make([]commitlog.CollectionDocument, 0, len(direct.primaryEntries))
		for _, entry := range direct.primaryEntries {
			if entry.flags&node.FlagTombstone != 0 {
				return nil, fmt.Errorf("collections: insert plan tombstoned primary document for command wal")
			}
			docs = append(docs, commitlog.CollectionDocument{
				ID:       entry.key,
				Document: entry.value,
			})
		}
		return docs, nil
	}
	for _, run := range plan.runs {
		if run.name != primaryRootName {
			continue
		}
		docs := make([]commitlog.CollectionDocument, 0, len(plan.resultIDs))
		for _, id := range plan.resultIDs {
			value, _, flags, ok := run.table.GetEntry(id)
			if !ok || flags&node.FlagTombstone != 0 {
				return nil, fmt.Errorf("collections: insert plan missing primary document for command wal")
			}
			docs = append(docs, commitlog.CollectionDocument{
				ID:       id,
				Document: value,
			})
		}
		return docs, nil
	}
	return nil, fmt.Errorf("collections: insert plan missing primary root for command wal")
}

func collectionDocumentsFromPreparedBatchUpdates(changed []preparedBatchUpdate) []commitlog.CollectionDocument {
	if len(changed) == 0 {
		return nil
	}
	docs := make([]commitlog.CollectionDocument, len(changed))
	for i := range changed {
		docs[i] = commitlog.CollectionDocument{
			ID:       changed[i].documentID,
			Document: changed[i].document,
		}
	}
	return docs
}

func collectionDocumentsFromBatchUpdateDocuments(changed []preparedBatchUpdate, documents [][]byte) []commitlog.CollectionDocument {
	if len(changed) == 0 {
		return nil
	}
	docs := make([]commitlog.CollectionDocument, len(changed))
	for i := range changed {
		docs[i] = commitlog.CollectionDocument{
			ID:       changed[i].documentID,
			Document: documents[i],
		}
	}
	return docs
}

func replayCollectionInsertBatchByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionInsertBatchByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	if len(payload.Documents) == 0 {
		intent, err := db.NewCommandWALReplayIntent(env)
		if err != nil {
			return err
		}
		return db.PublishCommandWALNoop(intent, false)
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	manager := newCommandWALReplayCollectionManager(db)
	collection, err := manager.openCollectionWithCommandWALIntent(payload.Collection, intent)
	if err != nil {
		return err
	}
	ids := make([][]byte, len(payload.Documents))
	docs := make([][]byte, len(payload.Documents))
	for i := range payload.Documents {
		ids[i] = payload.Documents[i].ID
		docs[i] = payload.Documents[i].Document
	}
	_, err = collection.insertBatchWithCommandWALIntent(ids, docs, false, templateV1ReplayStoredDocumentEncoder(collection), intent, insertBatchExecutionOptions{returnResultIDs: true})
	return err
}

func replayCollectionDeleteBatchByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	manager := newCommandWALReplayCollectionManager(db)
	collection, err := manager.openCollectionWithCommandWALIntent(payload.Collection, intent)
	if err != nil {
		return err
	}
	_, err = collection.deleteBatchWithCommandWALIntent(payload.IDs, intent)
	return err
}

func replayCollectionReplaceSourceByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionReplaceSourceByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	collection, err := newCommandWALReplayCollectionManager(db).openCollectionWithCommandWALIntent(payload.Collection, intent)
	if err != nil {
		return err
	}
	ids := make([][]byte, len(payload.Documents))
	documents := make([][]byte, len(payload.Documents))
	for i := range payload.Documents {
		ids[i] = payload.Documents[i].ID
		documents[i] = payload.Documents[i].Document
	}
	_, err = collection.replaceSourceDocumentsWithCommandWALIntent(payload.DeleteIDs, ids, documents, intent, nil)
	return err
}

func replayCollectionUpdateBatchByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	if len(payload.Documents) == 0 {
		intent, err := db.NewCommandWALReplayIntent(env)
		if err != nil {
			return err
		}
		return db.PublishCommandWALNoop(intent, false)
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	manager := newCommandWALReplayCollectionManager(db)
	collection, err := manager.openCollectionWithCommandWALIntent(payload.Collection, intent)
	if err != nil {
		return err
	}
	items := make([]updateBatchItem, len(payload.Documents))
	for i := range payload.Documents {
		doc := payload.Documents[i]
		items[i] = updateBatchItem{
			UpdateBatchItem: UpdateBatchItem{
				DocumentID: doc.ID,
				Update: func(replacement []byte) func([]byte) ([]byte, bool, error) {
					return func([]byte) ([]byte, bool, error) {
						return replacement, true, nil
					}
				}(doc.Document),
			},
			allowTemplateV1StoredDocument: true,
		}
	}
	_, _, err = collection.updateBatchOwnedItemsWithCommandWALIntent(items, updateBatchModeAny, intent)
	return err
}

func replayCollectionRebuildVectorIndexCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionRebuildVectorIndexPayload(env.Payload)
	if err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	manager := newCommandWALReplayCollectionManager(db)
	collection, err := manager.openCollectionWithCommandWALIntent(payload.Collection, intent)
	if err != nil {
		return err
	}
	_, err = collection.rebuildVectorIndexWithCommandWALIntent(payload.IndexName, intent)
	return err
}

func replayCatalogCreateCollectionCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCatalogCreateCollectionPayload(env.Payload)
	if err != nil {
		return err
	}
	meta, err := decodeCollectionMeta(payload.Metadata)
	if err != nil {
		return err
	}
	if meta.Name != payload.Collection {
		return fmt.Errorf("collections: catalog create payload collection %q does not match metadata name %q", payload.Collection, meta.Name)
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	_, err = newCommandWALReplayCollectionManager(db).createCollectionWithCommandWALIntent(meta, intent)
	return err
}

func templateV1ReplayStoredDocumentEncoder(collection *Collection) *TemplateV1Encoder {
	if collection == nil {
		return nil
	}
	if normalizedDocumentFormat(collection.meta.Options.DocumentFormat) != DocumentFormatTemplateV1 {
		return nil
	}
	var learned [32]byte
	return &TemplateV1Encoder{
		ids:      map[[32]byte]uint64{learned: 1},
		scope:    templateV1EncoderScopeForCollection(collection),
		hasScope: true,
	}
}
