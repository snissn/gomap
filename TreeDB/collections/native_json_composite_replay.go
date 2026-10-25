package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
		name            string
		incarnation     uint64
		meta            CollectionMeta
		root            uint64
		rows            nativeJSONRows
		planner         insertBatchPlanner
		runtimes        []indexRuntime
		typed           []*nativeTypedDelta
		templateRecords []templateV1Record
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
	var memberOrdinal uint32
	err = commitlog.ScanCompositeMutationPayload(env.Payload, func(member commitlog.CompositeMutationMember) error {
		ordinal := memberOrdinal
		memberOrdinal++
		if member.Kind == commitlog.CommandKindRawKVBatch {
			raw = member.Payload
			return nil
		}
		var name string
		var docs []commitlog.CollectionDocument
		var ids [][]byte
		var typed *commitlog.CollectionTypedBatchPayload
		if member.PayloadFormat == commitlog.PayloadFormatCollectionTypedBatchByIDV1 {
			decoded, err := commitlog.DecodeCollectionTypedBatchPayload(member.Payload)
			if err != nil {
				return err
			}
			typed, name = &decoded, decoded.Collection
		} else if member.PayloadFormat == commitlog.PayloadFormatCollectionTypedSourceByIDV1 {
			decoded, err := commitlog.DecodeCollectionTypedSourcePayload(member.Payload)
			if err != nil {
				return err
			}
			typed, name, ids = &decoded.Inserted, decoded.Inserted.Collection, decoded.DeleteIDs
		} else {
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
			case commitlog.CommandKindCollectionReplaceSourceByID:
				decoded, err := commitlog.DecodeCollectionReplaceSourceByIDPayload(member.Payload)
				if err != nil {
					return err
				}
				if len(decoded.Documents) != 0 {
					return backenddb.ErrNativeSemanticUnsupported
				}
				name, ids = decoded.Collection, decoded.DeleteIDs
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
			if normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatTemplateV1 {
				templateRoot, err := rootFor(collectionTemplateRootName(name))
				if err != nil {
					return err
				}
				planner.options.templateResolver = &templateV1SnapshotResolver{snap: snapshot, rootID: templateRoot}
				planner.options.allowTemplateV1Stored = true // canonical inputs were scoped/validated before append
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
		if nativeTypedMeta(target.meta) {
			var rows []columnDeclaredRow
			var residual [][]byte
			op := ColumnPublishOperationInsert
			switch member.Kind {
			case commitlog.CommandKindCollectionInsertBatchByID:
			case commitlog.CommandKindCollectionUpdateBatchByID, commitlog.CommandKindCollectionReplaceSourceByID:
				op = ColumnPublishOperationUpdate
			case commitlog.CommandKindCollectionDeleteBatchByID:
				op = ColumnPublishOperationDelete
			default:
				return backenddb.ErrNativeSemanticUnsupported
			}
			if typed != nil {
				if columnRetainedPayloadEffectiveEncoding(target.meta.Options.ColumnStore) != ColumnRetainedPayloadEncodingJSON {
					return backenddb.ErrNativeSemanticUnsupported
				}
				projection, typedIDs, retained, err := typedProjectionFromPayload(target.meta, *typed)
				if err != nil {
					return err
				}
				declared := make([]columnWriteDocument, len(typedIDs))
				for i, id := range typedIDs {
					declared[i] = columnWriteDocument{ID: id, Document: retained[i]}
				}
				if err = applyTypedProjection(typedIDs, declared, projection); err != nil {
					return err
				}
				prepared, err := prepareColumnWritePublishInputBeforeCommandWAL(columnWritePublishInput{meta: target.meta, operation: op, documents: declared, rows: len(declared)})
				if err != nil {
					return err
				}
				rows, residual = prepared.declaredRows, retained
			} else if len(docs) != 0 {
				if member.Kind != commitlog.CommandKindCollectionInsertBatchByID && member.Kind != commitlog.CommandKindCollectionUpdateBatchByID {
					return backenddb.ErrNativeSemanticUnsupported
				}
				declared := make([]columnWriteDocument, len(docs))
				for i, doc := range docs {
					declared[i] = columnWriteDocument{ID: doc.ID, Document: doc.Document}
				}
				var err error
				rows, residual, err = nativeTypedDocumentRows(target.meta, declared)
				if err != nil {
					return err
				}
			}
			exists := func(id []byte) (bool, error) {
				for i := len(target.typed) - 1; i >= 0; i-- {
					if row, _, ok := target.typed[i].find(id); ok {
						return row != nil, nil
					}
				}
				_, err := original(id)
				if errors.Is(err, tree.ErrKeyNotFound) {
					return false, nil
				}
				return err == nil, err
			}
			for _, row := range rows {
				found, err := exists(row.ID)
				if err != nil {
					return err
				}
				if op == ColumnPublishOperationInsert && found {
					return ErrDocumentExists
				}
				if member.Kind == commitlog.CommandKindCollectionUpdateBatchByID && !found {
					return tree.ErrKeyNotFound
				}
				if member.Kind == commitlog.CommandKindCollectionReplaceSourceByID && found {
					replacing := false
					for _, id := range ids {
						if bytes.Equal(id, row.ID) {
							replacing = true
							break
						}
					}
					if !replacing {
						return ErrDocumentExists
					}
				}
			}
			sort.Sort(nativeTypedRowsOrder{rows: rows, retained: residual})
			for i := 1; i < len(rows); i++ {
				if bytes.Equal(rows[i-1].ID, rows[i].ID) {
					return ErrDuplicateDocumentID
				}
			}
			if len(rows) == 0 && len(ids) == 0 {
				return nil
			} // maintained empty Replace frame
			if err = nativeTypedValidateResidual(target.meta, rows, residual); err != nil {
				return err
			}
			if err = nativeTypedValidateIndexRows(target.meta, rows, noAdmission); err != nil {
				return err
			}
			amount, err := nativeTypedMutationBuildCapacity(target.meta, rows, residual, ids)
			if err != nil {
				return err
			}
			target.typed = append(target.typed, &nativeTypedDelta{rows: rows, retained: residual, deleted: ids, operation: op, appliedLSN: env.LSN, memberOrdinal: ordinal, buildCapacity: amount})
			return nil
		}
		if typed != nil {
			return backenddb.ErrNativeSemanticUnsupported
		}
		var templateDocuments [][]byte
		if normalizedDocumentFormat(target.planner.options.documentFormat) == DocumentFormatTemplateV1 {
			// The original batch parser discovers ALL envelope records before
			// resolving hash-only documents. Canonical ID order may place a hash
			// reference before the document carrying its envelope in this member.
			inputs := make([][]byte, len(docs))
			for i := range docs {
				inputs[i] = docs[i].Document
			}
			prepared, records, _, resolver, err := prepareInsertDocuments(inputs, target.planner.options)
			if err != nil {
				return err
			}
			templateDocuments = prepared
			target.planner.options.templateResolver = resolver
			target.templateRecords = append(target.templateRecords, records...)
		}
		for docIdx, doc := range docs {
			// Validate every canonical intermediate before reducing it. A later
			// delete must not erase a captured scalar-schema error. Use exactly
			// the foreground preparation/extraction contract: unindexed JSON
			// preserves opaque bytes, indexed JSON requires valid scalar input,
			// and BSON always requires a valid original BSON image.
			var normalized [][]byte
			if templateDocuments != nil {
				normalized = templateDocuments[docIdx : docIdx+1]
			} else {
				normalized, _, _, _, err = prepareInsertDocuments([][]byte{doc.Document}, target.planner.options)
				if err != nil {
					return err
				}
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
			if member.Kind == commitlog.CommandKindCollectionUpdateBatchByID {
				if err := validateBSONReplacementPreservesID(row.current, normalized[0], target.planner.options); err != nil {
					return err
				}
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
		typed := nativeTypedMeta(target.meta)
		if typed && len(target.meta.Indexes) == 0 {
			continue
		}
		var plan *directBufferedInsertPlan
		var changed []preparedBatchUpdate
		var runtimes []indexRuntime
		if typed {
			// These are private startup headers, ordered by original canonical
			// member. Link only the original inputs to reuse final-state reduction.
			var head *nativeTypedDelta
			var mutations []memtable.COWMutation
			for _, delta := range target.typed {
				delta.previous = head
				head = delta
				for _, row := range delta.rows {
					mutations = append(mutations, memtable.COWMutation{Key: row.ID})
				}
				for _, id := range delta.deleted {
					mutations = append(mutations, memtable.COWMutation{Key: id})
				}
			}
			sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })
			n := 0
			for _, mutation := range mutations {
				if n != 0 && bytes.Equal(mutations[n-1].Key, mutation.Key) {
					continue
				}
				mutations[n] = mutation
				n++
			}
			clear(mutations[n:])
			mutations = mutations[:n]
			stateRoot, e := rootFor(collectionIndexStateRootName(target.name))
			if e != nil {
				return e
			}
			readOld := func(id []byte) ([]byte, error) {
				if stateRoot == 0 {
					return nil, nil
				}
				raw, e := snapshot.GetAtRoot(stateRoot, id)
				if errors.Is(e, tree.ErrKeyNotFound) {
					return nil, nil
				}
				return raw, e
			}
			plan, changed, runtimes, err = nativeTypedIndexFinalPlan(target.meta, mutations, head, readOld, noAdmission)
		} else {
			plan, changed, runtimes, err = nativeJSONFinalRowsResolved(target.name, target.meta, &target.rows, noAdmission, nil, updateBatchModeAny, target.planner.options.templateResolver)
		}
		if err != nil {
			return err
		}
		if !typed {
			if err = nativeTemplatePlanEntries(plan, target.name, target.templateRecords, target.planner.options, noAdmission); err != nil {
				return err
			}
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
		additional, _, err := extractNativeInsertEffects(target.name, target.incarnation, plan, primary, noAdmission, target.meta)
		if err != nil {
			return err
		}
		if len(target.meta.TextIndexes) != 0 {
			catalog := &collectionCatalog{meta: target.meta, roots: make(map[string]uint64)}
			for _, name := range collectionRootNames(target.meta) {
				root, e := rootFor(name)
				if e != nil {
					return e
				}
				catalog.roots[name] = root
			}
			text, e := nativeTextMutationDomains(snapshot, catalog, target.rows.rows, noAdmission)
			if e != nil {
				return e
			}
			additional = append(additional, text...)
		}
		for i := 0; i <= len(additional); i++ {
			if typed && i == 0 {
				continue // Original typed materializer alone owns physical primary locators.
			}
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
	var typedResources *rootpublication.StableResourceSetBuilder
	var typedClosure *rootpublication.StableResourceSet
	var typedRequirements rootpublication.StableLogicalObligationRequirements
	defer func() {
		if typedResources != nil {
			resultErr = errors.Join(resultErr, typedResources.AbandonWithError())
		}
		if typedClosure != nil {
			resultErr = errors.Join(resultErr, typedClosure.ReleaseWithError())
		}
	}()
	for _, target := range collections {
		if len(target.typed) == 0 {
			continue
		}
		manifest, err := rootFor(collectionColumnManifestRootName(target.name))
		if err != nil {
			return err
		}
		// Startup uses this original aggregate group's bounded construction scope;
		// it never constructs a live COW budget or calls a child singleton handler.
		work, err := prepareNativeTypedReplayMaterialization(nativeTypedMaterializeInput{meta: target.meta, baseManifestRoot: manifest, deltas: target.typed, snapshot: snapshot, capture: capture, group: group})
		if work != nil {
			defer func() { done, e := work.release(); done.Complete(); resultErr = errors.Join(resultErr, e) }()
		}
		if err != nil {
			return err
		}
		if err = work.materialize(); err != nil {
			return err
		}
		if typedResources == nil {
			typedResources = rootpublication.NewStableResourceSetBuilder()
		}
		requirements, err := rootpublication.MergeStableLogicalObligationRequirements(typedRequirements, work.requirements)
		if err != nil {
			return err
		}
		if err = typedResources.Merge(work.resources); err != nil {
			return err
		}
		work.resources = nil
		typedRequirements = requirements
		for _, root := range work.roots {
			var value [8]byte
			binary.BigEndian.PutUint64(value[:], root.root)
			if err = system.Set([]byte(systemCollectionRootKey(target.name+"/"+root.name)), value[:]); err != nil {
				return err
			}
		}
		encoded, err := encodeCollectionMeta(work.meta)
		if err != nil {
			return err
		}
		if err = system.Set([]byte(systemCollectionMetaKey(target.name)), encoded); err != nil {
			return err
		}
	}
	if typedResources != nil {
		typedClosure, err = typedResources.Freeze()
		if err != nil {
			return err
		}
		typedResources = nil
		if err = group.SetNativeDurableResources(typedClosure, typedRequirements); err != nil {
			return err
		}
		typedClosure = nil
	}
	if err = group.BuildNativeSystemDelta(system); err != nil {
		return err
	}
	if err = physical.AttachCompositeReplayGroup(intent, group); err != nil {
		return err
	}
	return physical.WriteSync()
}
