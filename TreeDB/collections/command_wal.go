package collections

import (
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/node"
)

func init() {
	backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionInsertBatchByID, replayCollectionInsertBatchByIDCommandWAL)
	backenddb.RegisterCommandWALReplayHandler(commitlog.CommandKindCollectionDeleteBatchByID, replayCollectionDeleteBatchByIDCommandWAL)
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
		return nil, fmt.Errorf("collections: command wal batch ids length mismatch")
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

func replayCollectionInsertBatchByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionInsertBatchByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	if len(payload.Documents) == 0 {
		return db.PublishCommandWALNoop(backenddb.NewCommandWALReplayIntent(env), false)
	}
	manager := NewCollectionManager(db)
	collection, err := manager.OpenCollection(payload.Collection)
	if err != nil {
		return err
	}
	ids := make([][]byte, len(payload.Documents))
	docs := make([][]byte, len(payload.Documents))
	for i := range payload.Documents {
		ids[i] = payload.Documents[i].ID
		docs[i] = payload.Documents[i].Document
	}
	_, err = collection.insertBatchWithCommandWALIntent(ids, docs, false, templateV1ReplayStoredDocumentEncoder(collection), backenddb.NewCommandWALReplayIntent(env))
	return err
}

func replayCollectionDeleteBatchByIDCommandWAL(db *backenddb.DB, env commitlog.CommandEnvelope) error {
	payload, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(env.Payload)
	if err != nil {
		return err
	}
	manager := NewCollectionManager(db)
	collection, err := manager.OpenCollection(payload.Collection)
	if err != nil {
		return err
	}
	_, err = collection.deleteBatchWithCommandWALIntent(payload.IDs, backenddb.NewCommandWALReplayIntent(env))
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
