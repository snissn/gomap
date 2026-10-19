package collections

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A schema transition changes scalar indexes only. In particular a malformed
// catalog frame cannot change storage interpretation, collection identity or
// any typed/vector/text configuration through this command.
func decodeNativeIndexMutation(payload []byte) (commitlog.CatalogIndexMutationPayload, CollectionMeta, CollectionMeta, error) {
	p, err := commitlog.DecodeCatalogIndexMutationPayload(payload)
	if err != nil {
		return p, CollectionMeta{}, CollectionMeta{}, err
	}
	before, err := decodeCollectionMeta(p.BeforeMetadata)
	if err != nil {
		return p, before, CollectionMeta{}, err
	}
	after, err := decodeCollectionMeta(p.Metadata)
	if err != nil {
		return p, before, after, err
	}
	if before.Name != p.Collection || after.Name != p.Collection || before.Options.ColumnStore != nil || after.Options.ColumnStore != nil {
		return p, before, after, backenddb.ErrNativeSemanticUnsupported
	}
	before, err = nativeJSONMeta(before)
	if err != nil {
		return p, before, after, err
	}
	after, err = nativeJSONMeta(after)
	if err != nil {
		return p, before, after, err
	}
	same := before
	same.Indexes = after.Indexes
	same, err = nativeJSONMeta(same)
	if err != nil {
		return p, before, after, err
	}
	if !sameCollectionMeta(same, after) {
		return p, before, after, backenddb.ErrNativeSemanticUnsupported
	}
	return p, before, after, nil
}

// Startup rebuilds index domains from the exact preceding physical cut and
// publishes schema and all roots together through its ORIGINAL replay group.
func replayNativeIndexMutation(db *backenddb.DB, env commitlog.CommandEnvelope) (resultErr error) {
	p, before, after, err := decodeNativeIndexMutation(env.Payload)
	if err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	if err = db.ValidateCommandWALReplayOperationV1(intent); err != nil {
		return err
	}
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		return backenddb.ErrClosed
	}
	defer func() { resultErr = errors.Join(resultErr, snapshot.Close()) }()
	token, ok := snapshot.StateToken()
	if !ok {
		return backenddb.ErrClosed
	}
	encoded, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte(systemCollectionMetaKey(p.Collection)))
	if err != nil {
		return err
	}
	actual, err := decodeCollectionMeta(encoded)
	if err != nil {
		return err
	}
	if !sameCollectionMeta(actual, before) {
		return backenddb.ErrConcurrentModification
	}
	encoded, err = snapshot.GetAtRoot(token.SystemRootPageID, []byte(nativeCatalogIncarnationPrefix+p.Collection))
	if err != nil {
		return err
	}
	identity, err := nativeCatalogIdentityValue(encoded, false)
	if err != nil {
		return err
	}
	if identity != p.Incarnation {
		return errNativeCatalogIdentity
	}
	encoded, err = snapshot.GetAtRoot(token.SystemRootPageID, []byte(nativeCatalogNextIncarnationKey))
	if err != nil {
		return err
	}
	counter, err := nativeCatalogIdentityValue(encoded, true)
	if err != nil {
		return err
	}
	root := uint64(0)
	encoded, err = snapshot.GetAtRoot(token.SystemRootPageID, []byte(systemCollectionRootKey(collectionPrimaryRootName(p.Collection))))
	if err == nil {
		root, err = decodeRootID(encoded)
	} else if errors.Is(err, tree.ErrKeyNotFound) {
		err = nil
	}
	if err != nil {
		return err
	}
	if _, err = snapshot.GetAtRoot(token.SystemRootPageID, []byte(systemCollectionRootOverlayKey(collectionPrimaryRootName(p.Collection)))); err == nil {
		return backenddb.ErrNativeSemanticUnsupported
	} else if !errors.Is(err, tree.ErrKeyNotFound) {
		return err
	}
	// Startup's original transport/construction scope owns these temporary rows,
	// as in aggregate replay; no live COW budget or second allocator is created.
	noAdmission := func(uint64) error { return nil }
	var rows nativeJSONRows
	if root != 0 {
		it, e := snapshot.IteratorAtRoot(root, nil, nil)
		if e != nil {
			return e
		}
		e = func() (ret error) {
			defer func() { ret = errors.Join(ret, it.Close()) }()
			for ; it.Valid(); it.Next() {
				if it.IsDeleted() {
					continue
				}
				id := bytes.Clone(it.UnsafeKey())
				doc := it.ValueCopy(nil)
				if e := it.Error(); e != nil {
					return e
				}
				row, e := rows.touch(id, func([]byte) ([]byte, error) { return nil, nil }, noAdmission)
				if e != nil {
					return e
				}
				row.current = doc
			}
			return it.Error()
		}()
		if e != nil {
			return e
		}
	}
	plan, _, _, err := nativeJSONFinalRows(p.Collection, after, &rows, noAdmission, nil)
	if err != nil {
		return err
	}
	primary := make([]memtable.COWMutation, len(plan.primaryEntries))
	domains, _, err := extractNativeInsertEffects(p.Collection, p.Incarnation, plan, primary, noAdmission)
	if err != nil {
		return err
	}
	effects, err := nativeIndexCatalogEffects(before, after, p.Metadata, p.Incarnation, counter, noAdmission)
	if err != nil {
		return err
	}
	capture, err := db.AcquireStableResourceCaptureLease()
	if err != nil {
		return err
	}
	defer capture.Release()
	group, err := db.BeginRootPublicationBuildGroupBorrowingLease(snapshot, capture)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, group.Close()) }()
	system := batch.New(nil, int(^uint(0)>>1))
	defer system.CloseWithoutPooling()
	for _, effect := range effects {
		if effect.Flags&node.FlagTombstone != 0 {
			err = system.Delete(effect.Key)
		} else {
			err = system.Set(effect.Key, effect.Value)
		}
		if err != nil {
			return err
		}
	}
	for _, domain := range domains {
		if !nativeIndexDomain(domain.name) {
			continue // Preserve original primary/template domains.
		}
		delta := batch.New(nil, int(^uint(0)>>1))
		var built uint64
		err = func() (ret error) {
			defer delta.CloseWithoutPooling()
			for _, entry := range domain.entries {
				if entry.Flags&node.FlagTombstone != 0 {
					ret = delta.Delete(entry.Key)
				} else {
					ret = delta.Set(entry.Key, entry.Value)
				}
				if ret != nil {
					return ret
				}
			}
			built, ret = group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{StoragePolicy: domain.policy, Delta: delta})
			return ret
		}()
		if err != nil {
			return err
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], built)
		if err = system.Set([]byte(systemCollectionRootKey(p.Collection+"/"+domain.name)), value[:]); err != nil {
			return err
		}
	}
	// Explicit zero descriptors distinguish an empty new index from stale roots.
	for _, index := range after.Indexes {
		found := false
		for _, d := range domains {
			found = found || d.name == "index/"+index.Name
		}
		if !found {
			if err = system.Set([]byte(systemCollectionRootKey(collectionSecondaryRootName(p.Collection, index.Name))), make([]byte, 8)); err != nil {
				return err
			}
		}
	}
	if err = group.BuildNativeSystemDelta(system); err != nil {
		return err
	}
	physical := db.NewPhysicalBatch().(*backenddb.Batch)
	defer func() { resultErr = errors.Join(resultErr, physical.Close()) }()
	if err = physical.AttachCatalogIndexReplayGroup(intent, group); err != nil {
		return err
	}
	return physical.WriteSync()
}
