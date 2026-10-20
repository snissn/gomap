package collections

import (
	"errors"
	"sort"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
)

// NativeJSONManager exposes the native scalar-indexed document capability. It owns
// no legacy manager and never registers a raw publication barrier.
type NativeJSONManager struct {
	db *backenddb.DB
	mu sync.Mutex
}

func NewNativeJSONManager(database *backenddb.DB) (*NativeJSONManager, error) {
	if database == nil {
		return nil, backenddb.ErrClosed
	}
	if err := database.ActivateNativeSemanticOwner(); err != nil {
		return nil, err
	}
	return &NativeJSONManager{db: database}, nil
}

func nativeJSONMeta(meta CollectionMeta) (CollectionMeta, error) {
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		return CollectionMeta{}, err
	}
	format := normalizedDocumentFormat(normalized.Options.DocumentFormat)
	if len(normalized.VectorIndexes)+len(normalized.TextIndexes) != 0 || (normalized.Options.ColumnStore != nil && (normalized.Options.AllowArrayValuesInIndex || !nativeTypedMeta(normalized))) || (format != DocumentFormatJSON && format != DocumentFormatBSON) {
		return CollectionMeta{}, backenddb.ErrNativeSemanticUnsupported
	}
	for _, index := range normalized.Indexes {
		if len(index.Components) != 0 || (index.ValueType != IndexValueString && index.ValueType != IndexValueBool && index.ValueType != IndexValueInt64 && index.ValueType != IndexValueDouble && !(format == DocumentFormatBSON && index.ValueType == IndexValueBSONOrderedV2)) {
			return CollectionMeta{}, backenddb.ErrNativeSemanticUnsupported
		}
	}
	return normalized, nil
}

func finishPrivateNative(work *rootpublication.SemanticCleanup) error {
	if work == nil {
		return nil
	}
	completion, err := work.Finish()
	work = nil
	if err == nil {
		completion.Complete()
	}
	return err
}

// CreateCollection prepares one immutable successor before entering the same
// canonical command-WAL bridge used by raw COW publication.
func (m *NativeJSONManager) CreateCollection(meta CollectionMeta) (*CollectionMeta, error) {
	return m.createCollection(meta, nil)
}

func (m *NativeJSONManager) createCollection(meta CollectionMeta, apply *nativeCommandWALApplyCommit) (created *CollectionMeta, resultErr error) {
	if m == nil || m.db == nil {
		return nil, backenddb.ErrClosed
	}
	if err := nativeTypedProfile(m.db, meta); err != nil {
		return nil, err
	}
	normalized, err := nativeJSONMeta(meta)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	capture, err := m.db.CaptureNativeSemanticState()
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, capture.Close()) }()
	basis := capture.State()
	base, ok := basis.(*nativeEngineVersion)
	if basis != nil && !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		base, err = importNativeJSONCatalog(capture)
		if base != nil {
			defer func() {
				record := base.Release()
				base = nil
				resultErr = errors.Join(resultErr, finishPrivateNative(record))
			}()
		}
		if err != nil {
			return nil, err
		}
	}

	var native *backenddb.NativePreparedPublication
	if existing, found := base.names.Get(normalized.Name); found {
		if !sameCollectionMeta(*existing.schema.meta, normalized) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if apply == nil {
			return existing.schema.meta.copy(), nil
		}
		apply.alreadyApplied = true
		next, prepErr := prepareNativeCommandWALNoop(base)
		if prepErr != nil {
			return nil, prepErr
		}
		native = &backenddb.NativePreparedPublication{State: next}
	} else {
		native, err = prepareNativeJSONCreate(base, normalized)
	}
	if err != nil {
		return nil, err
	}
	native.Basis, native.StartSequence = basis, capture.LogicalSequence()
	payload, err := EncodeCatalogCreateCollectionCommandWALPayload(normalized)
	if err != nil {
		cleanupErr := cancelPrivateNativePublication(native)
		return nil, errors.Join(err, cleanupErr)
	}
	intent, err := m.db.NewTrustedCommandWALIntent(commitlog.CommandKindCatalogCreateCollection, commitlog.CommandScopeCatalog, commitlog.PayloadFormatCatalogCreateCollectionV1, payload)
	if err != nil {
		cleanupErr := cancelPrivateNativePublication(native)
		return nil, errors.Join(err, cleanupErr)
	}
	sync := false
	if apply != nil {
		sync = apply.sync
		native.ObserveAssigned = apply.observe
		if err = intent.PrepareSealedReceiptWithAdmission(capture.NativeAllocator()); err != nil {
			return nil, errors.Join(err, cancelPrivateNativePublication(native))
		}
		defer intent.ReleaseNativeSealedReceipt()
	}
	result, err := m.db.WriteNativePublication(native, intent, sync)
	if apply != nil {
		apply.result = result
	}
	if !result.Consumed {
		err = errors.Join(err, cancelPrivateNativePublication(native))
	}
	if err != nil {
		return nil, err
	}
	return normalized.copy(), nil
}

func nativeCreateIdentityUpdates(name string, next uint64) (map[string][]byte, error) {
	if next == 0 || next == ^uint64(0) {
		return nil, errNativeCatalogIdentity
	}
	identity, err := nativeCatalogIdentityRecord(next, false)
	if err != nil {
		return nil, err
	}
	counter, err := nativeCatalogIdentityRecord(next+1, true)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{nativeCatalogIncarnationPrefix + name: identity[:], nativeCatalogNextIncarnationKey: counter[:]}, nil
}

func prepareNativeJSONCreate(base *nativeEngineVersion, meta CollectionMeta) (publication *backenddb.NativePreparedPublication, resultErr error) {
	// Selected cancellation owns memory-only scratch, never resource inventory.
	if base == nil || base.inventory != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	scratch, err := newNativeEnginePreparation(base, 1, 0, base.allocator)
	if err != nil {
		return nil, err
	}
	p := scratch.preparation
	var collection *nativeCollectionState
	defer func() {
		p.sealPrivateBuilders(scratch)
		p = nil
		collection = nil
		record := scratch.Release()
		scratch = nil
		resultErr = errors.Join(resultErr, finishPrivateNative(record))
	}()
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.allocator.AdmitNativeBytes(n) }
	p.schema, err = newNativeSchemaState(&meta, admit)
	if err != nil {
		return nil, err
	}
	p.domainBuilder, err = cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 0, nativeDomainLifetime{}, admit)
	if err != nil {
		return nil, err
	}
	p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 0, nativeDomainLifetime{}, admit)
	if err != nil {
		return nil, err
	}
	p.sealPrivateBuilders(scratch)
	collection, err = newNativeCollectionState(base.nextIncarnation, p.schema, p.domains, p.pendingDomains, admit)
	if err != nil {
		return nil, err
	}
	p.schema, p.domains, p.pendingDomains = nil, nil, nil
	p.ownedCollections = append(p.ownedCollections, collection)
	encoded, err := encodeCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	updates, err := nativeCreateIdentityUpdates(meta.Name, base.nextIncarnation)
	if err != nil {
		return nil, err
	}
	updates[systemCollectionMetaKey(meta.Name)] = encoded
	effects := make([]memtable.COWMutation, 0, len(updates))
	for key, value := range updates {
		effects = append(effects, memtable.COWMutation{Key: []byte(key), Value: value, Flags: node.FlagInline})
	}
	sort.Slice(effects, func(i, j int) bool { return string(effects[i].Key) < string(effects[j].Key) })
	next, err := prepareNativeCatalogReplacement(base, nil, collection, effects)
	if err != nil {
		if next != nil {
			record := next.Release()
			next = nil
			resultErr = errors.Join(resultErr, finishPrivateNative(record))
		}
		return nil, err
	}
	custody, err := newNativeMutationCustody(0, 1, base.allocator)
	if err != nil {
		record := next.Release()
		next = nil
		resultErr = errors.Join(resultErr, finishPrivateNative(record))
		return nil, err
	}
	(nativeCollectionLifetime{}).Retain(collection)
	custody.adoptCollection(collection)
	changed := []txnhistory.Effect{{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(meta.Name)}, {Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(nativeCatalogNextIncarnationKey)}}
	return &backenddb.NativePreparedPublication{Basis: base, State: next, Custody: custody, Effects: changed, Dependencies: changed}, nil
}

func prepareNativeJSONCreateCommand(_ *backenddb.DB, env commitlog.CommandEnvelope, basis backenddb.NativeSemanticState, _ *backenddb.Snapshot, _ *backenddb.StableResourceCaptureLease) (*backenddb.NativePreparedPublication, error) {
	base, ok := basis.(*nativeEngineVersion)
	if !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	payload, err := commitlog.DecodeCatalogCreateCollectionPayload(env.Payload)
	if err != nil {
		return nil, err
	}
	meta, err := decodeCollectionMeta(payload.Metadata)
	if err != nil {
		return nil, err
	}
	if meta.Name != payload.Collection {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	meta, err = nativeJSONMeta(meta)
	if err != nil {
		return nil, err
	}
	return prepareNativeJSONCreate(base, meta)
}

func cancelPrivateNativePublication(native *backenddb.NativePreparedPublication) error {
	state, custody := native.State, native.Custody
	native.State, native.Custody = nil, nil
	var record *rootpublication.SemanticCleanup
	if state != nil {
		record = state.Release()
	}
	state = nil
	err := finishPrivateNative(record)
	if custody != nil {
		record = custody.ReleaseNativeCustody()
		custody = nil
		err = errors.Join(err, finishPrivateNative(record))
	}
	return err
}
