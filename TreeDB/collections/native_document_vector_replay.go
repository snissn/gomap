package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Replay borrows only the original startup Snapshot; it constructs no native
// engine, capture or allocator. The enclosing original build group owns roots.
func nativeDocumentGraphSnapshotReader(snap *backenddb.Snapshot, root uint64) (func([]byte) ([]byte, error), func(int) (vectorIndexPersistNode, error), func(func([]byte, []byte) error) error) {
	get := func(key []byte) ([]byte, error) {
		if root == 0 {
			return nil, tree.ErrKeyNotFound
		}
		return snap.GetAtRoot(root, key)
	}
	scan := func(id int) (vectorIndexPersistNode, error) {
		var n vectorIndexPersistNode
		raw, err := get(vectorIndexNativeNodeKey(id))
		if err == nil {
			n, err = decodeNativeDocumentGraphNode(raw, json.Unmarshal)
		}
		return n, err
	}
	walk := func(visit func([]byte, []byte) error) (resultErr error) {
		if root == 0 {
			return nil
		}
		it, err := snap.IteratorAtRoot(root, nil, nil)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, it.Close()) }()
		for ; it.Valid(); it.Next() {
			if it.IsDeleted() {
				continue
			}
			value := it.Value()
			if err = it.Error(); err != nil {
				return err
			}
			if err = visit(it.UnsafeKey(), value); err != nil {
				return err
			}
		}
		return it.Error()
	}
	return get, scan, walk
}
func nativeDocumentGraphApplyRows(graph *nativeDocumentGraph, meta CollectionMeta, rows *nativeJSONRows, admit func(uint64) error) error {
	field, err := parseVectorFieldPath(graph.config.field)
	if err != nil {
		return err
	}
	for _, row := range rows.rows {
		if (row.opening == nil) == (row.current == nil) && bytes.Equal(row.opening, row.current) {
			continue
		}
		if row.current == nil {
			if err = graph.tombstone(row.id); err != nil {
				return err
			}
			continue
		}
		amount, err := nativeTypedCheckedMul(uint64(len(row.current)), 64)
		if err != nil {
			return err
		}
		if err = nativeTypedCheckedAdd(&amount, 1024); err != nil {
			return err
		}
		if err = admit(amount); err != nil {
			return err
		}
		var vector []float32
		var present bool
		if normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatBSON {
			vector, present, err = vectorFromBSONField(row.current, field)
		} else {
			vector, present, err = vectorFromJSONField(row.current, field)
		}
		if err != nil {
			return err
		}
		if present {
			err = graph.upsert(row.id, vector)
		} else {
			err = graph.tombstone(row.id)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func nativeDocumentGraphReplayRows(meta CollectionMeta, incarnation uint64, rows *nativeJSONRows, snap *backenddb.Snapshot, systemRoot uint64, rootFor func(string) (uint64, error)) ([]nativeJSONDomainChange, []memtable.COWMutation, error) {
	key, err := nativeDocumentGenerationKey(incarnation)
	if err != nil {
		return nil, nil, err
	}
	raw, err := snap.GetAtRoot(systemRoot, []byte(key))
	if err != nil {
		return nil, nil, err
	}
	old, err := decodeRootID(raw)
	if err != nil {
		return nil, nil, err
	}
	next, err := nativeDocumentGenerationAfter(old, rows)
	if err != nil {
		return nil, nil, err
	}
	if old == next {
		return nil, nil, nil
	}
	var changes []nativeJSONDomainChange
	noAdmission := func(uint64) error { return nil }
	for _, def := range meta.VectorIndexes {
		root, err := rootFor(collectionVectorIndexRootName(meta.Name, def.Name))
		if err != nil {
			return nil, nil, err
		}
		get, scan, walk := nativeDocumentGraphSnapshotReader(snap, root)
		graph, err := newNativeDocumentGraph(def, incarnation, old, get, noAdmission)
		if err != nil {
			return nil, nil, err
		}
		graph.scanNode = scan
		if root != 0 {
			if err = graph.validateCold(scan, walk); err != nil {
				return nil, nil, err
			}
		}
		if err = nativeDocumentGraphApplyRows(graph, meta, rows, noAdmission); err != nil {
			return nil, nil, err
		}
		entries, err := graph.mutations(next)
		if err != nil {
			return nil, nil, err
		}
		changes = append(changes, nativeJSONDomainChange{name: "vector-index/" + def.Name, entries: entries})
	}
	return changes, []memtable.COWMutation{{Key: []byte(key), Value: encodeRootID(next), Flags: node.FlagInline}}, nil
}

// Full rebuild is an explicit operation. Ordinary mutations never call this
// complete source census or serialize unchanged graph records.
func buildNativeDocumentGraph(meta CollectionMeta, incarnation, generation uint64, def VectorIndexDefinition, walk func(func([]byte, []byte) error) error, admit func(uint64) error) ([]memtable.COWMutation, error) {
	graph, err := newNativeDocumentGraph(def, incarnation, 0, func([]byte) ([]byte, error) { return nil, tree.ErrKeyNotFound }, admit)
	if err != nil {
		return nil, err
	}
	field, err := parseVectorFieldPath(def.Field)
	if err != nil {
		return nil, err
	}
	err = walk(func(id, document []byte) error {
		n, err := nativeTypedCheckedMul(uint64(len(document)), 64)
		if err != nil {
			return err
		}
		if err = nativeTypedCheckedAdd(&n, 1024); err != nil {
			return err
		}
		if err = admit(n); err != nil {
			return err
		}
		var vector []float32
		var present bool
		if normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatBSON {
			vector, present, err = vectorFromBSONField(document, field)
		} else {
			vector, present, err = vectorFromJSONField(document, field)
		}
		if err != nil {
			return err
		}
		if !present {
			return nil
		}
		return graph.upsert(id, vector)
	})
	if err != nil {
		return nil, err
	}
	return graph.mutations(generation)
}

func replayNativeDocumentVectorGraph(db *backenddb.DB, intent *backenddb.CommandWALIntent, meta CollectionMeta, name string) (resultErr error) {
	if err := db.ValidateCommandWALReplayOperationV1(intent); err != nil {
		return err
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	defer func() { resultErr = errors.Join(resultErr, snap.Close()) }()
	catalog, err := loadCollectionCatalog(snap, meta.Name)
	if err != nil {
		return err
	}
	if catalog == nil || catalog.meta.nativeDocumentCoverageVersion != nativeDocumentVectorCoverageVersion {
		return backenddb.ErrNativeSemanticUnsupported
	}
	meta = catalog.meta
	token, ok := snap.StateToken()
	if !ok {
		return backenddb.ErrClosed
	}
	raw, err := snap.GetAtRoot(token.SystemRootPageID, []byte(nativeCatalogIncarnationPrefix+meta.Name))
	if err != nil {
		return err
	}
	incarnation, err := nativeCatalogIdentityValue(raw, false)
	if err != nil {
		return err
	}
	key, err := nativeDocumentGenerationKey(incarnation)
	if err != nil {
		return err
	}
	raw, err = snap.GetAtRoot(token.SystemRootPageID, []byte(key))
	if err != nil {
		return err
	}
	generation, err := decodeRootID(raw)
	if err != nil {
		return err
	}
	def, ok := findVectorIndex(meta.VectorIndexes, name)
	if !ok {
		return ErrIndexNotFound
	}
	_, _, walk := nativeDocumentGraphSnapshotReader(snap, catalog.rootID(collectionPrimaryRootName(meta.Name)))
	entries, err := buildNativeDocumentGraph(meta, incarnation, generation, def, walk, func(uint64) error { return nil })
	if err != nil {
		return err
	}
	capture, err := db.AcquireStableResourceCaptureLease()
	if err != nil {
		return err
	}
	defer capture.Release()
	group, err := db.BeginRootPublicationBuildGroupBorrowingLease(snap, capture)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, group.Close()) }()
	delta := batch.New(nil, int(^uint(0)>>1))
	defer delta.Close()
	for _, entry := range entries {
		if entry.Flags&node.FlagTombstone != 0 {
			return fmt.Errorf("collections: complete graph contains tombstone delta")
		}
		if err = delta.Set(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	qualified := collectionVectorIndexRootName(meta.Name, name)
	policy, err := collectionRootStoragePolicyForDB(db, meta, qualified)
	if err != nil {
		return err
	}
	root, err := group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{StoragePolicy: policy, Delta: delta})
	if err != nil {
		return err
	}
	system := batch.New(nil, int(^uint(0)>>1))
	defer system.Close()
	if err = system.Set([]byte(systemCollectionRootKey(qualified)), encodeRootID(root)); err != nil {
		return err
	}
	if err = group.BuildNativeSystemDelta(system); err != nil {
		return err
	}
	physical := db.NewPhysicalBatch().(*backenddb.Batch)
	defer func() { resultErr = errors.Join(resultErr, physical.Close()) }()
	if err = physical.AttachVectorGraphReplayGroup(intent, group); err != nil {
		return err
	}
	return physical.WriteSync()
}
