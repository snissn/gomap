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
// on a package side-effect import before opening a command_wal_v1 directory.
func RegisterCommandWALReplayHandlers() {
	registerCommandWALReplayHandlersOnce.Do(func() {
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionInsertBatchByID, replayCollectionInsertBatchByIDCommandWAL)
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionDeleteBatchByID, replayCollectionDeleteBatchByIDCommandWAL)
		backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionUpdateBatchByID, replayCollectionUpdateBatchByIDCommandWAL)
		backenddb.RegisterCommandWALReplayHandlerWithOptions(
			commitlog.CommandKindCatalogCreateCollection,
			replayCatalogCreateCollectionCommandWAL,
			backenddb.CommandWALReplayHandlerOptions{NeedsReplayLogSupport: false},
		)
	})
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
	return c.db.NewCommandWALIntent(
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
	return c.db.NewCommandWALIntent(
		commitlog.CommandKindCollectionDeleteBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionDeleteBatchByIDV1,
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
	return c.db.NewCommandWALIntent(
		commitlog.CommandKindCollectionUpdateBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionUpdateBatchByIDV1,
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
	encoded, err := encodeCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	payload, err := commitlog.EncodeCatalogCreateCollectionPayload(meta.Name, encoded)
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
		return db.PublishCommandWALNoop(backenddb.NewCommandWALReplayIntent(env), false)
	}
	intent := backenddb.NewCommandWALReplayIntent(env)
	manager := NewCollectionManager(db)
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
	_, err = collection.insertBatchWithCommandWALIntent(ids, docs, false, templateV1ReplayStoredDocumentEncoder(collection), intent)
	return err
}

func replayCollectionDeleteBatchByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	intent := backenddb.NewCommandWALReplayIntent(env)
	manager := NewCollectionManager(db)
	collection, err := manager.openCollectionWithCommandWALIntent(payload.Collection, intent)
	if err != nil {
		return err
	}
	_, err = collection.deleteBatchWithCommandWALIntent(payload.IDs, intent)
	return err
}

func replayCollectionUpdateBatchByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	if len(payload.Documents) == 0 {
		return db.PublishCommandWALNoop(backenddb.NewCommandWALReplayIntent(env), false)
	}
	intent := backenddb.NewCommandWALReplayIntent(env)
	manager := NewCollectionManager(db)
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
	_, err = NewCollectionManager(db).createCollectionWithCommandWALIntent(meta, backenddb.NewCommandWALReplayIntent(env))
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
