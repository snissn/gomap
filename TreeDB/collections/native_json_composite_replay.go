package collections

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Foreground uses the original transaction's immutable aggregate plan. This
// registration is only its fixed capability; calling the singleton preparation
// interface cannot create another transaction or invoke startup replay.
func prepareNativeJSONCompositeMarker(*backenddb.DB, commitlog.CommandEnvelope, backenddb.NativeSemanticState, *backenddb.Snapshot, *backenddb.StableResourceCaptureLease) (*backenddb.NativePreparedPublication, error) {
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
	err = commitlog.ScanCompositeMutationPayload(env.Payload, func(member commitlog.CompositeMutationMember) error {
		if member.Kind == commitlog.CommandKindRawKVBatch {
			raw = member.Payload
			return nil
		}
		decoded, err := commitlog.DecodeCollectionInsertBatchByIDPayload(member.Payload)
		if err != nil {
			return err
		}
		if err = ValidateCollectionName(decoded.Collection); err != nil {
			return err
		}
		for _, old := range ordered {
			if strings.HasPrefix(old.name, decoded.Collection+"/") {
				return backenddb.ErrNativeSemanticUnsupported
			}
		}
		encoded, err := read(systemCollectionMetaKey(decoded.Collection))
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
		if meta.Name != decoded.Collection {
			return backenddb.ErrNativeSemanticUnsupported
		}
		identity, err := read(nativeCatalogIncarnationPrefix + decoded.Collection)
		if err != nil {
			return err
		}
		id, err := nativeCatalogIdentityValue(identity, false)
		if err != nil || id != member.Incarnation {
			return errNativeCatalogIdentity
		}
		if _, err = read(systemCollectionRootOverlayKey(collectionPrimaryRootName(decoded.Collection))); err == nil {
			return backenddb.ErrNativeSemanticUnsupported
		} else if !errors.Is(err, tree.ErrKeyNotFound) {
			return err
		}
		root := uint64(0)
		encoded, err = read(systemCollectionRootKey(collectionPrimaryRootName(decoded.Collection)))
		if err == nil {
			root, err = decodeRootID(encoded)
		}
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return err
		}
		policy, err := collectionRootStoragePolicy(meta, collectionPrimaryRootName(decoded.Collection))
		if err != nil {
			return err
		}
		ids, docs := make([][]byte, len(decoded.Documents)), make([][]byte, len(decoded.Documents))
		for i, row := range decoded.Documents {
			ids[i], docs[i] = row.ID, row.Document
		}
		planner, err := nativeJSONInsertPlanner(meta)
		if err != nil {
			return err
		}
		plan, err := planner.planInsertBatch(ids, docs)
		if err != nil {
			return err
		}
		if root != 0 {
			exists, err := snapshot.HasAnySortedAtRoot(root, plan.primaryKeys)
			if err != nil {
				return err
			}
			if exists {
				return ErrDocumentExists
			}
		}
		// Startup owns this ephemeral plan through its existing Snapshot/group;
		// no native allocator or fresh foreground capture is constructed.
		primary := make([]memtable.COWMutation, len(plan.directBufferedInsert.primaryEntries))
		additional, _, err := extractNativeInsertEffects(decoded.Collection, id, plan.directBufferedInsert, primary, func(uint64) error { return nil })
		if err != nil {
			return err
		}
		for _, run := range plan.directBufferedInsert.uniqueValueRootPlans {
			encoded, err := read(systemCollectionRootKey(collectionSecondaryRootName(meta.Name, run.indexName)))
			if errors.Is(err, tree.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			indexRoot, err := decodeRootID(encoded)
			if err != nil {
				return err
			}
			exists, err := snapshot.HasPrefixesAtRoot(indexRoot, run.prefixes)
			if err != nil {
				return err
			}
			for _, conflict := range exists {
				if conflict {
					return ErrUniqueIndexConflict
				}
			}
		}
		for i := 0; i <= len(additional); i++ {
			qualified, baseRoot, storage, entries := collectionPrimaryRootName(decoded.Collection), root, policy, primary
			if i > 0 {
				input := additional[i-1]
				qualified = decoded.Collection + "/" + input.name
				storage = input.policy
				entries = input.entries
				encoded, err := read(systemCollectionRootKey(qualified))
				baseRoot = 0
				if err == nil {
					baseRoot, err = decodeRootID(encoded)
				}
				if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
					return err
				}
			}
			if _, err := read(systemCollectionRootOverlayKey(qualified)); err == nil {
				return backenddb.ErrNativeSemanticUnsupported
			} else if !errors.Is(err, tree.ErrKeyNotFound) {
				return err
			}
			delta := batch.New(nil, int(^uint(0)>>1))
			ordered = append(ordered, prepared{name: qualified, root: baseRoot, policy: storage, delta: delta})
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
		return nil
	})
	if err != nil {
		return err
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
