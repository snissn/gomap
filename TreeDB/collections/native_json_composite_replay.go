package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Foreground CRUD and composite commands use the original transaction's
// captured immutable plan. This registration marks only that fixed capability;
// direct registered/assigned preparation cannot create a transaction, fabricate
// a captured plan or invoke startup replay. Registered/assigned CRUD still
// refuses without that captured authority.
func prepareNativeJSONTxnPlanMarker(*backenddb.DB, commitlog.CommandEnvelope, backenddb.NativeSemanticState, *backenddb.Snapshot, *backenddb.StableResourceCaptureLease) (*backenddb.NativePreparedPublication, error) {
	return nil, backenddb.ErrNativeSemanticUnsupported
}

// replayNativeJSONComposite prepares every logical member before building any
// private roots. The original startup Snapshot/build group own physical lifetime;
// no recovery cache, native budget or child singleton publisher is constructed.
func replayNativeJSONComposite(db *backenddb.DB, env commitlog.CommandEnvelope) (resultErr error) {
	if err := commitlog.ValidateSelectedCompositeMutation(env.Payload); err != nil {
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
	type prepared struct {
		name   string
		root   uint64
		policy backenddb.OrderedRootStoragePolicy
		delta  *batch.Batch
	}
	var ordered []prepared
	defer func() {
		for i := range ordered {
			if ordered[i].delta != nil {
				ordered[i].delta.Close()
				ordered[i].delta = nil
			}
		}
		ordered = nil
	}()
	var raw []byte
	read := func(key string) ([]byte, error) { return snapshot.GetAtRoot(token.SystemRootPageID, []byte(key)) }
	type collectionRows struct {
		name        string
		incarnation uint64
		meta        CollectionMeta
		root        uint64
		rows        nativeJSONRows
		planner     insertBatchPlanner
		runtimes    []indexRuntime
	}
	var collections []*collectionRows
	noAdmission := func(uint64) error { return nil }
	rootFor := func(qualified string) (uint64, error) {
		if _, err := read(systemCollectionRootOverlayKey(qualified)); err == nil {
			return 0, backenddb.ErrNativeSemanticUnsupported
		} else if !errors.Is(err, tree.ErrKeyNotFound) {
			return 0, err
		}
		encoded, err := read(systemCollectionRootKey(qualified))
		if errors.Is(err, tree.ErrKeyNotFound) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return decodeRootID(encoded)
	}
	err = commitlog.ScanCompositeMutationPayload(env.Payload, func(member commitlog.CompositeMutationMember) error {
		if member.Kind == commitlog.CommandKindRawKVBatch {
			raw = member.Payload
			return nil
		}
		var name string
		var docs []commitlog.CollectionDocument
		var ids [][]byte
		switch member.Kind {
		case commitlog.CommandKindCollectionInsertBatchByID:
			decoded, err := commitlog.DecodeCollectionInsertBatchByIDPayload(member.Payload)
			if err != nil {
				return err
			}
			name, docs = decoded.Collection, decoded.Documents
		case commitlog.CommandKindCollectionUpdateBatchByID:
			decoded, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(member.Payload)
			if err != nil {
				return err
			}
			if decoded.Colocated != nil {
				return backenddb.ErrNativeSemanticUnsupported
			}
			name, docs = decoded.Collection, decoded.Documents
		case commitlog.CommandKindCollectionDeleteBatchByID:
			decoded, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(member.Payload)
			if err != nil {
				return err
			}
			if decoded.Colocated != nil {
				return backenddb.ErrNativeSemanticUnsupported
			}
			name, ids = decoded.Collection, decoded.IDs
		default:
			return backenddb.ErrNativeSemanticUnsupported
		}
		if err := ValidateCollectionName(name); err != nil {
			return err
		}
		var target *collectionRows
		for _, old := range collections {
			if old.name == name {
				target = old
				break
			}
		}
		if target == nil {
			encoded, err := read(systemCollectionMetaKey(name))
			if err != nil {
				return err
			}
			meta, err := decodeCollectionMeta(encoded)
			if err != nil {
				return err
			}
			meta, err = nativeJSONMeta(meta)
			if err != nil {
				return err
			}
			if meta.Name != name {
				return backenddb.ErrNativeSemanticUnsupported
			}
			identity, err := read(nativeCatalogIncarnationPrefix + name)
			if err != nil {
				return err
			}
			id, err := nativeCatalogIdentityValue(identity, false)
			if err != nil || id != member.Incarnation {
				return errNativeCatalogIdentity
			}
			root, err := rootFor(collectionPrimaryRootName(name))
			if err != nil {
				return err
			}
			planner, err := nativeJSONInsertPlanner(meta)
			if err != nil {
				return err
			}
			runtimes, err := planner.indexRuntimes()
			if err != nil {
				return err
			}
			target = &collectionRows{name: name, incarnation: id, meta: meta, root: root, planner: planner, runtimes: runtimes}
			collections = append(collections, target)
		} else if target.incarnation != member.Incarnation {
			return errNativeCatalogIdentity
		}
		original := func(id []byte) ([]byte, error) {
			if target.root == 0 {
				return nil, tree.ErrKeyNotFound
			}
			return snapshot.GetAtRoot(target.root, id)
		}
		for _, doc := range docs {
			// Validate every canonical intermediate before reducing it. A later
			// delete must not erase a captured scalar-schema error. Use exactly
			// the foreground preparation/extraction contract: unindexed JSON
			// preserves opaque bytes, while indexed documents require valid JSON.
			normalized, _, _, _, err := prepareInsertDocuments([][]byte{doc.Document}, target.planner.options)
			if err != nil {
				return err
			}
			if _, err = orderedIndexStateForDocumentWithArena(normalized[0], target.runtimes, target.planner.options, nil); err != nil {
				return err
			}
			row, err := target.rows.touch(doc.ID, original, noAdmission)
			if err != nil {
				return err
			}
			if member.Kind == commitlog.CommandKindCollectionInsertBatchByID && row.current != nil {
				return ErrDocumentExists
			}
			if member.Kind == commitlog.CommandKindCollectionUpdateBatchByID && row.current == nil {
				return tree.ErrKeyNotFound
			}
			row.current = bytes.Clone(normalized[0])
		}
		for _, id := range ids {
			row, err := target.rows.touch(id, original, noAdmission)
			if err != nil {
				return err
			}
			row.current = nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, target := range collections {
		plan, changed, runtimes, err := nativeJSONFinalRows(target.name, target.meta, &target.rows, noAdmission, nil)
		if err != nil {
			return err
		}
		replacements := batchUniqueReplacementOwners(runtimes, changed)
		for runtimeIdx, runtime := range runtimes {
			if !runtime.def.unique {
				continue
			}
			root, err := rootFor(collectionSecondaryRootName(target.name, runtime.def.name))
			if err != nil {
				return err
			}
			if root == 0 {
				continue
			}
			for _, item := range changed {
				if !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
					continue
				}
				for _, encoded := range item.newState.valuesAt(runtimeIdx) {
					_, prefix, err := appendIndexValuePrefixSlice(make([]byte, 0, 2+len(encoded)), encoded)
					if err != nil {
						return err
					}
					it, err := snapshot.IteratorAtRoot(root, prefix, prefixEnd(prefix))
					if err != nil {
						return err
					}
					err = func() (resultErr error) {
						defer func() { resultErr = errors.Join(resultErr, it.Close()) }()
						for ; it.Valid(); it.Next() {
							if it.IsDeleted() {
								continue
							}
							owner, err := indexKeyDocumentID(runtime.def.valueType, it.UnsafeKey())
							if err != nil {
								return err
							}
							if batchUpdateUniqueOwnerConflicts(runtime.def.name, encoded, owner, item.documentID, replacements) {
								return fmt.Errorf("%w %q", ErrUniqueIndexConflict, runtime.def.name)
							}
						}
						return it.Error()
					}()
					if err != nil {
						return err
					}
				}
			}
		}
		primary := make([]memtable.COWMutation, len(plan.primaryEntries))
		additional, _, err := extractNativeInsertEffects(target.name, target.incarnation, plan, primary, noAdmission)
		if err != nil {
			return err
		}
		for i := 0; i <= len(additional); i++ {
			qualified, entries := collectionPrimaryRootName(target.name), primary
			policy := nativeInsertRootPolicy(plan, qualified)
			if i > 0 {
				input := additional[i-1]
				qualified = target.name + "/" + input.name
				policy, entries = input.policy, input.entries
			}
			root, err := rootFor(qualified)
			if err != nil {
				return err
			}
			delta := batch.New(nil, int(^uint(0)>>1))
			ordered = append(ordered, prepared{name: qualified, root: root, policy: policy, delta: delta})
			for _, row := range entries {
				if row.Flags&node.FlagTombstone != 0 {
					err = delta.Delete(row.Key)
				} else {
					err = delta.Set(row.Key, row.Value)
				}
				if err != nil {
					return err
				}
			}
		}
	}
	physical := db.NewPhysicalBatch().(*backenddb.Batch)
	defer func() { resultErr = errors.Join(resultErr, physical.Close()) }()
	if raw != nil {
		if err = db.StageCompositeRawReplay(intent, physical, raw); err != nil {
			return err
		}
	}
	group, err := db.BeginRootPublicationBuildGroupFromSnapshot(snapshot)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, group.Close()) }()
	system := batch.New(nil, int(^uint(0)>>1))
	defer system.Close()
	for _, input := range ordered {
		root, err := group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: input.root, StoragePolicy: input.policy, Delta: input.delta})
		if err != nil {
			return err
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], root)
		// Collection roots use the same persisted encoding as every other producer.
		if err = system.Set([]byte(systemCollectionRootKey(input.name)), value[:]); err != nil {
			return err
		}
	}
	if err = group.BuildNativeSystemDelta(system); err != nil {
		return err
	}
	if err = physical.AttachCompositeReplayGroup(intent, group); err != nil {
		return err
	}
	return physical.WriteSync()
}
