package collections

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func bsonSetWALDeltaMeta(meta *CollectionMeta) bool {
	return nativeBSONSetMeta(meta) && len(meta.Indexes) == 0 && len(meta.TextIndexes) == 0 && len(meta.VectorIndexes) == 0
}

// The retained row remains the normal full BSON image. This representation
// only substitutes for the standalone command member after proving that the
// exact transformed image differs at one existing generic binary value.
func (t *NativeJSONTxn) tryAppendBSONSetWALDelta(g *nativeJSONTxnBinding, id []byte, field BSONSetField) (bool, error) {
	if field.Value.Type != bson.TypeBinary || len(field.Value.Value) <= 5 || field.Value.Value[4] != 0 {
		return false, nil
	}
	row := g.rows.find(id)
	if row == nil || len(row.opening) != len(row.current) {
		return false, nil
	}
	start, end, ok := bsonSetWALDeltaValueRange(row.opening, []byte(field.Key))
	if !ok || end-start != len(field.Value.Value) ||
		!bytes.Equal(row.current[start:end], field.Value.Value) ||
		!bytes.Equal(row.opening[:start], row.current[:start]) ||
		!bytes.Equal(row.opening[end:], row.current[end:]) {
		return false, nil
	}
	delta := commitlog.CollectionBSONSetDelta{
		ID: id, Field: []byte(field.Key), Value: field.Value.Value,
		Before: sha256.Sum256(row.opening), After: sha256.Sum256(row.current),
		Incarnation: g.incarnation,
	}
	size, err := commitlog.CollectionBSONSetDeltaPayloadSize(g.name, delta)
	if err != nil || size >= 18+len(g.name)+len(id)+len(row.current) {
		return false, nil
	}
	if err := t.admitStage(g, memtable.COWAllocationCharge(uint64(size))); err != nil {
		return false, err
	}
	payload, err := commitlog.EncodeCollectionBSONSetDelta(g.name, delta)
	if err != nil {
		return false, err
	}
	return true, t.appendNativeMember(g, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)
}

func bsonSetWALDeltaValueRange(doc, key []byte) (start, end int, ok bool) {
	length, rest, valid := bsoncore.ReadLength(doc)
	if !valid || length < 5 || int(length) != len(doc) || doc[len(doc)-1] != 0 {
		return 0, 0, false
	}
	for len(rest) > 1 {
		element, next, valid := bsoncore.ReadElement(rest)
		if !valid {
			return 0, 0, false
		}
		rest = next
		if !bytes.Equal(element.KeyBytes(), key) {
			continue
		}
		value := element.Value()
		if ok || value.Type != bsoncore.Type(bson.TypeBinary) || len(value.Data) <= 5 || value.Data[4] != 0 {
			return 0, 0, false
		}
		end = len(doc) - len(rest)
		start = end - len(value.Data)
		ok = true
	}
	return start, end, ok && len(rest) == 1
}

func replayBSONSetWALDelta(db *backenddb.DB, manager *CollectionManager, env commitlog.CommandEnvelope) error {
	name, delta, err := commitlog.DecodeCollectionBSONSetDelta(env.Payload)
	if err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	if err := db.ValidateCommandWALReplayOperationV1(intent); err != nil {
		return err
	}
	namespace := string(name)
	postimage, err := bsonSetWALDeltaReplayPostimage(db, namespace, delta)
	if err != nil {
		return fmt.Errorf("collections: replay BSON $set WAL delta for %q at LSN %d: %w", namespace, env.LSN, err)
	}
	collection, err := manager.openCollectionWithCommandWALIntent(namespace, intent)
	if err != nil {
		return err
	}
	item := updateBatchItem{UpdateBatchItem: UpdateBatchItem{
		DocumentID: delta.ID,
		Update: func(current []byte) ([]byte, bool, error) {
			if sha256.Sum256(current) != delta.Before {
				return nil, false, backenddb.ErrConcurrentModification
			}
			return postimage, true, nil
		},
	}}
	_, _, err = collection.updateBatchOwnedItemsWithCommandWALIntent([]updateBatchItem{item}, updateBatchModeAny, intent)
	return err
}

func bsonSetWALDeltaReplayPostimage(db *backenddb.DB, name string, delta commitlog.CollectionBSONSetDelta) ([]byte, error) {
	// Startup replays serially. Inspect metadata, incarnation and the row
	// together in the original preceding root, before opening any mutation
	// that could publish a missing-row no-op under this command's LSN.
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snapshot, name)
	if err != nil {
		_ = snapshot.Close()
		return nil, err
	}
	if catalog == nil || !bsonSetWALDeltaMeta(&catalog.meta) {
		_ = snapshot.Close()
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	record, found, err := getSystemValue(snapshot, nativeCatalogIncarnationPrefix+name)
	if err != nil || !found {
		_ = snapshot.Close()
		if err != nil {
			return nil, err
		}
		return nil, errNativeCatalogIdentity
	}
	incarnation, err := nativeCatalogIdentityValue(record, false)
	if err != nil || incarnation != delta.Incarnation {
		_ = snapshot.Close()
		return nil, errNativeCatalogIdentity
	}
	current, found, err := collectionGetAppendAtCatalogRoot(snapshot, catalog, catalog.primaryRootName, delta.ID, nil)
	if closeErr := snapshot.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	if !found || sha256.Sum256(current) != delta.Before {
		return nil, backenddb.ErrConcurrentModification
	}
	start, end, ok := bsonSetWALDeltaValueRange(current, delta.Field)
	if !ok || end-start != len(delta.Value) {
		return nil, commitlog.ErrCorrupt
	}
	spec, err := newBSONSetUpdate([]BSONSetField{{Key: string(delta.Field), Value: bson.RawValue{Type: bson.TypeBinary, Value: delta.Value}}})
	if err != nil {
		return nil, err
	}
	_, postimage, changed, sameShape, err := spec.appendSameShapeReplacement(nil, current)
	if err != nil {
		return nil, err
	}
	if !changed || !sameShape || sha256.Sum256(postimage) != delta.After {
		return nil, commitlog.ErrCorrupt
	}
	return postimage, nil
}
