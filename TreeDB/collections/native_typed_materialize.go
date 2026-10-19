package collections

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// All inputs are borrowed from the SAME captured native prefix and physical
// group. This helper neither captures a newer catalog nor publishes a root.
// The enclosing original build adopts the returned work before checking err.
type nativeTypedMaterializeInput struct {
	meta             CollectionMeta
	baseManifestRoot uint64
	deltas           []*nativeTypedDelta // oldest first, actual assigned identities
	snapshot         *backenddb.Snapshot
	capture          *backenddb.StableResourceCaptureLease
	allocator        backenddb.NativeSemanticAllocator
	group            *backenddb.RootPublicationBuildGroup
}
type nativeTypedMaterializedRoot struct {
	name string
	root uint64
}

type nativeTypedMaterialization struct {
	input             nativeTypedMaterializeInput
	roots             []nativeTypedMaterializedRoot
	meta              CollectionMeta
	resources         *rootpublication.StableResourceSet
	requirements      rootpublication.StableLogicalObligationRequirements
	resourceBuilder   *rootpublication.StableResourceSetBuilder
	records           []columnManifestRecord
	codec             typedcolumn.BuildCodec
	refund            *cowbtree.OwnedMapRefund
	capacity          uint64
	started, complete bool
	failure           error
}

func nativeTypedCheckedAdd(total *uint64, n uint64) error {
	if n > ^uint64(0)-*total {
		return memtable.ErrCOWCapacity
	}
	*total += n
	return nil
}
func nativeTypedCheckedMul(a, b uint64) (uint64, error) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, memtable.ErrCOWCapacity
	}
	return a * b, nil
}

// The delta promise is source dependent. Shared operation scratch is added
// ONCE by nativeTypedCollectionBuildCapacity, not once per accepted command.
func nativeTypedDeltaBuildCapacity(meta CollectionMeta, rows []columnDeclaredRow, retained [][]byte) (uint64, error) {
	if !nativeTypedMeta(meta) || len(rows) != len(retained) || len(rows) == 0 {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	var total uint64
	for i, row := range rows {
		if len(row.Values) != 1 || len(row.ID) == 0 {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
		n, err := nativeTypedDeltaRowBuildCapacity(uint64(len(row.ID)), uint64(len(retained[i])))
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&total, n); err != nil {
			return 0, err
		}
		backing, _, err := nativeTypedValueCapacity(meta.Options.ColumnStore.Columns[0], row.Values[0])
		if err != nil {
			return 0, err
		}
		// The maintained prepared publisher reserves twelve dictionary source
		// copies. The same envelope covers vector batch, granule, section and
		// old/replacement image backing; per-row maps remain in the scalar term.
		backing, err = nativeTypedCheckedMul(backing, 12)
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(backing)); err != nil {
			return 0, err
		}
	}
	manifest, err := nativeTypedManifestAppendCapacity(meta)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, manifest); err != nil {
		return 0, err
	}
	return total, nil
}

// Values retained by an immutable delta own their string/vector backing.
// JSON byte counts are unescaped source bounds for nativeTypedJSONReadCharge.
// encoding/json emits finite float32 values using at most 24 bytes including
// sign, exponent or fixed decimal digits and an array separator.
func nativeTypedValueCapacity(col ColumnStoreColumn, value columnDeclaredValue) (backing, jsonBytes uint64, err error) {
	if value.Type != col.ValueType || value.Null && !col.Nullable {
		return 0, 0, ErrColumnDeclaredValueUnsupported
	}
	if value.Null {
		return 0, 4, nil
	}
	switch col.ValueType {
	case ColumnStoreValueInt64:
		return 0, 20, nil
	case ColumnStoreValueString:
		backing = uint64(len(value.String))
		if err = nativeTypedCheckedAdd(&backing, uint64(cap(value.StringBytes))); err != nil {
			return
		}
		return backing, max(uint64(len(value.String)), uint64(len(value.StringBytes))) + 2, nil
	case ColumnStoreValueFloat32Vector:
		dims := columnStoreFloat32VectorElementsPerRow(col)
		if col.Nullable || dims <= 0 || len(value.Float32Vector) != dims {
			return 0, 0, ErrColumnDeclaredValueUnsupported
		}
		for _, v := range value.Float32Vector {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return 0, 0, ErrColumnDeclaredValueUnsupported
			}
		}
		backing, err = nativeTypedCheckedMul(uint64(cap(value.Float32Vector)), 4)
		if err != nil {
			return
		}
		jsonBytes, err = nativeTypedCheckedMul(uint64(dims), 24)
		if err == nil {
			err = nativeTypedCheckedAdd(&jsonBytes, 2)
		}
		return
	default:
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
}

// This scalar route has no aggregate metadata: its maintained builder adds
// exactly a row part, a TCIM part and the replacement manifest header. Sixteen
// simultaneous record/closure/mutation views each reserve four original 1 KiB
// quota units for headers/maps and separately include all captured metadata
// string backing. This does not assume a maximum namespace or schema name.
func nativeTypedManifestAppendCapacity(meta CollectionMeta) (uint64, error) {
	backing, err := nativeSchemaBackingCharge(reflect.ValueOf(&meta), 0)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&backing, 4*preparedSemanticStreamMapEntryReserveBytes); err != nil {
		return 0, err
	}
	return nativeTypedCheckedMul(backing, 16*3)
}

// Retained source bytes bound structural entries independently of their encoded
// compression ratio. These are the maintained prepared-materializer quota terms.
func nativeTypedDeltaRowBuildCapacity(idBytes, retainedBytes uint64) (uint64, error) {
	structural, err := nativeTypedCheckedMul(retainedBytes, uint64(4*preparedSemanticStreamMapEntryReserveBytes+2*preparedSemanticStreamMaxPathBytes+preparedSemanticStreamMaxCursorDepth*int(unsafe.Sizeof(""))+int(unsafe.Sizeof(columnRetainedSemanticStreamPathNode{}))))
	if err != nil {
		return 0, err
	}
	total := memtable.COWAllocationCharge(structural)
	// C=1: maintained vector expansion, typed image, C+3 sidecars, row image,
	// root descriptors and run records; all independently owned backing rounds.
	const perRow = 4224 + 1024 + 4*1024 + 384 + 1024 + 2048
	if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(perRow)); err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&idBytes, retainedBytes); err != nil {
		return 0, err
	}
	// Raw source/row image, retained wrapper, root delta and old/replacement
	// backing coexist in the maintained append/encoding constructors.
	raw, err := nativeTypedCheckedMul(idBytes, 6)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(raw)); err != nil {
		return 0, err
	}
	return total, nil
}

func nativeTypedCollectionBuildCapacity(meta CollectionMeta, sourceBytes uint64) (uint64, error) {
	if !nativeTypedMeta(meta) {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	// The retained builder's source-derived fixed cursor/raw/output/encoder
	// proof already includes its 8 MiB codec allowance. Native scratch starts
	// empty, so the unrelated maximum pooled raw buffer is absent.
	semantic := uint64(preparedSemanticStreamBlockFixedReserve(nil, nil, 0) - columnRetainedSemanticStreamV1RawBlockScratchMaxRetainedBytes + preparedSemanticStreamBatchReserveBytes)
	// TCIM construction uses the identical pinned fastest codec locally. Its
	// proof includes the full default 4 MiB window's double history, hash table,
	// bounded literal/sequence/FSE/Huffman buffers and their growth coexistence.
	typed := typedcolumn.BuildCodecCapacity() + uint64(1<<20) // original row-image fixed reserve
	decoder, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		return 0, err
	}
	// Validation follows encoder close; ordered-root building follows asset
	// validation. Their operation-local fixed workspaces do not coexist.
	total := max(semantic, typed, decoder+uint64(1<<20), uint64(8<<20))
	if err := nativeTypedCheckedAdd(&total, sourceBytes); err != nil {
		return 0, err
	}
	backing, err := nativeSchemaBackingCharge(reflect.ValueOf(&meta), 0)
	if err != nil {
		return 0, err
	}
	// BOTH original handoffs construct the updated schema. Active/recovery
	// identities may be absent from the opening metadata; charge their exact
	// fixed structures and default format strings before either construction.
	schema := backing
	for _, n := range []uint64{memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeSchemaState{}))), memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{}))), 4 * memtable.COWAllocationCharge(uint64(unsafe.Sizeof(ColumnManifestIdentity{}))+64)} {
		if err = nativeTypedCheckedAdd(&schema, n); err != nil {
			return 0, err
		}
	}
	schema, err = nativeTypedCheckedMul(schema, 2)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, schema); err != nil {
		return 0, err
	}
	for _, n := range []uint64{uint64(unsafe.Sizeof(nativeTypedMaterialization{})), uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})), 4 * uint64(unsafe.Sizeof(nativeTypedMaterializedRoot{}))} {
		if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n)); err != nil {
			return 0, err
		}
	}
	return total, nil
}
func nativeTypedMaterializationCapacity(meta CollectionMeta, deltas []*nativeTypedDelta) (uint64, error) {
	var source uint64
	for _, d := range deltas {
		if d == nil {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
		n, err := nativeTypedDeltaBuildCapacity(meta, d.rows, d.retained)
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&source, n); err != nil {
			return 0, err
		}
	}
	return nativeTypedCollectionBuildCapacity(meta, source)
}

// Count the exact retained manifest basis with fixed iterator traversal scratch
// before loading records or changing a build ledger. The bound covers the
// maintained record clones, checksum/mutation maps and resource requirements.
// This same amount participates in preaccept finite-cohort feasibility.
func nativeTypedManifestCapacity(snapshot *backenddb.Snapshot, root uint64) (uint64, error) {
	if root == 0 {
		return 0, nil
	}
	if snapshot == nil {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	it, err := snapshot.IteratorAtRoot(root, columnManifestHeaderRecordKeyBytes, nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var total uint64
	for it.Valid() {
		key := it.UnsafeKey()
		if !columnManifestRecordKeyKnownForScan(key) {
			break
		}
		if !it.IsDeleted() {
			value, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			// Sixteen maintained logical copies/indices can coexist through manifest
			// encoding, full closure validation and mutation-delta construction. Each
			// has one record header plus separately rounded owned key/value backing.
			record := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(columnManifestRecord{}))) + memtable.COWAllocationCharge(uint64(len(key))) + memtable.COWAllocationCharge(uint64(len(value)))
			n, e := nativeTypedCheckedMul(record+preparedSemanticStreamMapEntryReserveBytes, 16)
			if e != nil {
				return 0, e
			}
			if e = nativeTypedCheckedAdd(&total, n); e != nil {
				return 0, e
			}
		}
		it.Next()
	}
	return total, it.Error()
}

func prepareNativeTypedMaterialization(input nativeTypedMaterializeInput) (*nativeTypedMaterialization, error) {
	if input.allocator == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	return prepareNativeTypedMaterializationCore(input)
}

// Startup uses the registered aggregate replay's original transport and build
// group/capture lifetime. It has no live COW owner or synthetic COW receipt.
// This wrapper does not extend live eligibility or claim live-budget startup
// qualification; it shares the exact physical constructor and failure custody.
func prepareNativeTypedReplayMaterialization(input nativeTypedMaterializeInput) (*nativeTypedMaterialization, error) {
	if input.allocator != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	return prepareNativeTypedMaterializationCore(input)
}

func prepareNativeTypedMaterializationCore(input nativeTypedMaterializeInput) (work *nativeTypedMaterialization, resultErr error) {
	if input.snapshot == nil || input.capture == nil || input.group == nil || len(input.deltas) == 0 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	amount, err := nativeTypedMaterializationCapacity(input.meta, input.deltas)
	if err != nil {
		return nil, err
	}
	basis, err := nativeTypedManifestCapacity(input.snapshot, input.baseManifestRoot)
	if err != nil {
		return nil, err
	}
	if err = nativeTypedCheckedAdd(&amount, basis); err != nil {
		return nil, err
	}
	var refund *cowbtree.OwnedMapRefund
	if input.allocator != nil {
		lease, admitErr := input.allocator.AdmitNativeBytes(amount)
		if admitErr != nil {
			return nil, admitErr
		}
		refund = new(cowbtree.OwnedMapRefund)
		refund.Init(lease)
	}
	w := &nativeTypedMaterialization{input: input, meta: input.meta, refund: refund, capacity: amount}
	defer func() {
		if resultErr != nil {
			// Original caller adopts partial work, but borrowed ancestors must not
			// become part of its terminal failure custody.
			w.input = nativeTypedMaterializeInput{}
			w.failure = resultErr
		}
	}()
	w.roots = []nativeTypedMaterializedRoot{{name: "primary"}, {name: "column/manifest", root: input.baseManifestRoot}, {name: "column/row-locator"}, {name: "retained/semantic-stream-v1"}}
	token, ok := input.snapshot.StateToken()
	if !ok {
		return w, backenddb.ErrNativeSemanticUnsupported
	}
	for i := range w.roots {
		if i == 1 {
			continue
		}
		raw, e := input.snapshot.GetAtRoot(token.SystemRootPageID, []byte(systemCollectionRootPrefix+input.meta.Name+"/"+w.roots[i].name))
		if errors.Is(e, tree.ErrKeyNotFound) {
			continue
		}
		if e != nil {
			return w, e
		}
		root, e := decodeRootID(raw)
		if e != nil {
			return w, e
		}
		w.roots[i].root = root
	}
	if input.baseManifestRoot != 0 {
		w.records, err = loadColumnManifestRecordsFromRoot(input.snapshot, input.baseManifestRoot)
		if err != nil {
			return w, err
		}
	}
	w.resourceBuilder = rootpublication.NewStableResourceSetBuilder()
	return w, nil
}

func (w *nativeTypedMaterialization) materialize() (resultErr error) {
	if w == nil || w.started {
		return backenddb.ErrNativeSemanticUnsupported
	}
	w.started = true
	defer func() {
		w.codec.Close()
		w.input = nativeTypedMaterializeInput{}
		if resultErr != nil {
			w.failure = resultErr
		} else {
			w.complete = true
		}
	}()
	rootDir, registry, err := w.input.group.NativeColumnAssetBuildContext()
	if err != nil {
		return err
	}
	context := columnPhysicalAssetBuildContext{rootDir: rootDir, registry: registry, recovery: w.input.capture, codec: &w.codec, singleOutput: true}
	var previousLSN uint64
	var previousMember uint32
	for _, d := range w.input.deltas {
		if d.appliedLSN == 0 || (previousLSN != 0 && (d.appliedLSN < previousLSN || (d.appliedLSN == previousLSN && d.memberOrdinal <= previousMember))) {
			return backenddb.ErrNativeSemanticUnsupported
		}
		previousLSN, previousMember = d.appliedLSN, d.memberOrdinal
		if err = w.materializeDelta(context, d); err != nil {
			return err
		}
	}
	w.resources, err = w.resourceBuilder.Freeze()
	if err != nil {
		return err
	}
	w.resourceBuilder = nil
	return nil
}

func (w *nativeTypedMaterialization) materializeDelta(context columnPhysicalAssetBuildContext, d *nativeTypedDelta) error {
	// Each command's semantic phase precedes its typed encoder phase. The
	// maintained asset helper also closes after joining its encoder goroutine,
	// before validation decoders; drop again here before the next command.
	_ = w.codec.Close()
	cfg := *w.meta.Options.ColumnStore
	// The input already IS the retained noncolumn carrier. A zero-declaration
	// view preserves every residual field through the maintained semantic codec;
	// no full JSON document or declared-column shadow is reconstructed here.
	residualConfig := cfg
	residualConfig.Columns = nil
	ids := make([][]byte, len(d.rows))
	documents := make([]columnWriteDocument, len(d.rows))
	for i, row := range d.rows {
		ids[i] = row.ID
		documents[i] = columnWriteDocument{ID: row.ID}
	}
	retained, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDsConstruction(residualConfig, ids, d.retained, int64(w.capacity), true)
	if err != nil {
		return err
	}
	defer retained.semanticStreamBlocks.(*memtable.AppendOnly).ReleaseDropEntries()
	for i := range documents {
		documents[i].Document = retained.documents[i]
	}
	generation := uint64(1)
	if cfg.ActiveManifest != nil {
		if cfg.ActiveManifest.Generation == ^uint64(0) {
			return memtable.ErrCOWCapacity
		}
		generation = cfg.ActiveManifest.Generation + 1
	}
	input := columnWritePublishInput{meta: w.meta, operation: ColumnPublishOperationInsert, documents: documents, rows: len(documents), declaredRows: d.rows, declaredRowsReady: true}
	plan, err := BuildColumnPublishPlan(ColumnPublishPlanInput{Collection: w.meta.Name, ColumnStore: &cfg, ColumnStoreNormalized: true, ActiveVectorIndexesKnown: true, Operation: ColumnPublishOperationInsert, CurrentManifest: cfg.ActiveManifest, CurrentManifestRecords: w.records, AppliedCommandLSN: d.appliedLSN, BaseManifestRootID: w.roots[1].root, Hooks: ColumnPublishPlanHooks{
		PrepareAssets: func(hook ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
			prepared, e := context.prepareRows(ColumnPublishPreparedAssets{}, input, hook, d.rows, nil, nil, generation, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
			if e != nil {
				return prepared, e
			}
			if prepared.stableResources == nil {
				return prepared, backenddb.ErrNativeSemanticUnsupported
			}
			// The original Work adopts successful asset custody before any later
			// manifest/closure stage can fail and release the plan's own set.
			custody, cloneErr := rootpublication.CloneStableResourceSetExcludingKinds(prepared.stableResources)
			if cloneErr != nil {
				prepared.stableResources.Release()
				prepared.stableResources = nil
				return prepared, cloneErr
			}
			// Merge consumes its child. Preserve the plan's independent set for
			// its own closure validation and retain the clone in original work.
			if e = w.resourceBuilder.Merge(custody); e != nil {
				custody.Release()
				prepared.stableResources.Release()
				prepared.stableResources = nil
				return prepared, e
			}
			return prepared, nil
		}, EncodeManifest: encodeColumnManifestIdentityForWrite,
	}})
	if err != nil {
		return err
	}
	if plan.stableResources != nil {
		defer plan.stableResources.Release()
	}
	var manifest iterator.UnsafeIterator
	if plan.RootDelta.MutationDelta {
		manifest = columnManifestRootMutationIterator(plan.RootDelta.IdentityRecord, plan.RootDelta.Mutations)
	} else {
		manifest = columnManifestRootRecordIterator(plan.RootDelta.IdentityRecord, plan.RootDelta.Records)
	}
	policy, err := backendRootStoragePolicy(plan.RootDelta.StoragePolicy)
	if err == nil {
		err = w.buildIterator(1, manifest, policy)
	}
	_ = manifest.Close()
	if err != nil {
		return err
	}
	if err = w.buildDocuments(0, documents, plan, false); err != nil {
		return err
	}
	if err = w.buildDocuments(2, documents, plan, true); err != nil {
		return err
	}
	it := retained.semanticStreamBlocks.NewIterator(nil, nil)
	policy, err = collectionRootStoragePolicy(w.meta, w.meta.Name+"/"+w.roots[3].name)
	if err == nil {
		err = w.buildIterator(3, it, policy)
	}
	_ = it.Close()
	if err != nil {
		return err
	}
	updated, err := columnPublishUpdatedMeta(w.meta, plan)
	if err != nil {
		return err
	}
	w.meta = updated
	w.records = plan.RootDelta.Records
	// The latest manifest requirements are the COMPLETE collection closure,
	// including preceding accepted members and the exact retained physical base.
	w.requirements = plan.durableResourceRequirements
	if fallback := plan.durableResourceRequirementsFallback; fallback != nil {
		w.requirements, _, err = fallback()
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *nativeTypedMaterialization) buildDocuments(index int, documents []columnWriteDocument, plan ColumnPublishPlan, locator bool) error {
	policy, err := collectionRootStoragePolicy(w.meta, w.meta.Name+"/"+w.roots[index].name)
	if err != nil {
		return err
	}
	delta := batch.NewBoundedRetainingLargeEntries(nil, int(^uint(0)>>1), nativeBuildBatchEntries)
	defer delta.CloseWithoutPooling()
	flush := func() error {
		if delta.IsEmpty() {
			return nil
		}
		root, e := w.input.group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: w.roots[index].root, Delta: delta, StoragePolicy: policy})
		if e == nil {
			w.roots[index].root = root
		}
		delta.Reset()
		return e
	}
	for i, document := range documents {
		value := document.Document
		if locator {
			value = encodeColumnPrimaryRowLocator(DocumentRowRef{DocumentID: document.ID, Generation: plan.UpdatedActiveManifest.Generation, PartID: columnPhysicalRowAssetPartID, RowIndex: i, AppliedCommandLSN: plan.AppliedCommandLSN})
		}
		if i > 0 && bytes.Compare(documents[i-1].ID, document.ID) >= 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		if err = delta.AppendViewTrustedSortedUnique(document.ID, value); err != nil {
			return err
		}
		if delta.Len() == nativeBuildBatchEntries {
			if err = flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// Stream the existing immutable ordered iterator through the original bounded
// batch and original group. No pooled batch may survive the work's receipt.
func (w *nativeTypedMaterialization) buildIterator(index int, it iterator.UnsafeIterator, policy backenddb.OrderedRootStoragePolicy) error {
	delta := batch.NewBoundedRetainingLargeEntries(nil, int(^uint(0)>>1), nativeBuildBatchEntries)
	defer delta.CloseWithoutPooling()
	flush := func() error {
		if delta.IsEmpty() {
			return nil
		}
		root, err := w.input.group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: w.roots[index].root, Delta: delta, StoragePolicy: policy})
		if err == nil {
			w.roots[index].root = root
		}
		delta.Reset()
		return err
	}
	for it.Valid() {
		var err error
		if it.IsDeleted() {
			err = delta.AppendDeleteViewTrustedSortedUnique(it.UnsafeKey())
		} else {
			value, ptr, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return backenddb.ErrNativeSemanticUnsupported
			}
			_ = ptr
			err = delta.AppendViewTrustedSortedUnique(it.UnsafeKey(), value)
		}
		if err != nil {
			return err
		}
		if delta.Len() == nativeBuildBatchEntries {
			if err = flush(); err != nil {
				return err
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return flush()
}

func (w *nativeTypedMaterialization) release() (cowbtree.OwnedMapCompletion, error) {
	if w == nil {
		return cowbtree.OwnedMapCompletion{}, nil
	}
	var err error
	w.codec.Close()
	w.input = nativeTypedMaterializeInput{}
	if w.resourceBuilder != nil {
		err = w.resourceBuilder.AbandonWithError()
		if err != nil {
			return cowbtree.OwnedMapCompletion{}, err
		}
		w.resourceBuilder = nil
	}
	if w.resources != nil {
		err = w.resources.ReleaseWithError()
		if err != nil {
			return cowbtree.OwnedMapCompletion{}, err
		}
		w.resources = nil
	}
	clear(w.roots)
	w.roots = nil
	w.records = nil
	w.requirements = rootpublication.StableLogicalObligationRequirements{}
	w.meta = CollectionMeta{}
	refund := w.refund
	w.refund = nil
	return refund.Completion(), err
}

// Stage uses the maintained top-level RawMessage residual transform. Its
// temporary map never expands nested values to interface trees: each object
// member needs at least four source bytes, and one 1 KiB quota unit covers a
// Swiss-map group entry, key/header and simultaneous old/replacement group.
// The JSON encoder can escape each source byte to six output bytes; four
// such source-sized buffers cover old/new marshal state and the owned copy.
// Constructor scratch is admitted separately from the actual stripped residual
// after these parser temporaries have been dropped.
func nativeTypedStageCapacity(meta CollectionMeta, ids, docs [][]byte) (retained, scratch uint64, err error) {
	if !nativeTypedMeta(meta) || len(ids) == 0 || len(ids) != len(docs) {
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	if bytes.ContainsRune([]byte(meta.Options.ColumnStore.Columns[0].Path), '.') {
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	for _, n := range []uint64{uint64(unsafe.Sizeof(nativeTypedRows{})), uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})), uint64(len(ids)) * uint64(unsafe.Sizeof(columnDeclaredRow{})), uint64(len(ids)) * uint64(unsafe.Sizeof([]byte{}))} {
		if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(n)); err != nil {
			return
		}
	}
	docHeaders, err := nativeTypedCheckedMul(uint64(len(ids)), uint64(unsafe.Sizeof(columnWriteDocument{})))
	if err != nil {
		return 0, 0, err
	}
	docHeaders = memtable.COWAllocationCharge(docHeaders)
	scratch = docHeaders
	for i, doc := range docs {
		var output uint64
		output, err = nativeTypedCheckedMul(uint64(len(doc)), 6)
		if err != nil {
			return
		}
		if err = nativeTypedCheckedAdd(&output, 2); err != nil {
			return
		}
		var declared uint64
		switch col := meta.Options.ColumnStore.Columns[0]; col.ValueType {
		case ColumnStoreValueString:
			declared = uint64(len(doc)) // unescaping cannot exceed JSON source
		case ColumnStoreValueFloat32Vector:
			// The maintained converter allocates configured capacity before parsing
			// and may grow on invalid overlong arrays before rejecting their length.
			// At least two JSON source bytes are needed per additional element;
			// twice max(configured, source) covers old/new append growth backing.
			declared, err = nativeTypedCheckedMul(max(uint64(columnStoreFloat32VectorElementsPerRow(col)), uint64(len(doc))), 8)
			if err != nil || declared > uint64(maxCollectionInt) {
				return 0, 0, memtable.ErrCOWCapacity
			}
		}
		for _, n := range []uint64{uint64(len(ids[i])), uint64(unsafe.Sizeof(columnDeclaredValue{})), output, declared} {
			if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(n)); err != nil {
				return
			}
		}
		// This is a maximum over serial documents, while retained owned outputs
		// above sum across the batch. RawMessage copies and encoded string buffers
		// coexist only for the current document.
		var maps, codec uint64
		maps, err = nativeTypedCheckedMul(uint64(len(doc)/4+1), preparedSemanticStreamMapEntryReserveBytes)
		if err != nil {
			return
		}
		codec, err = nativeTypedCheckedMul(uint64(len(doc)), 24)
		if err != nil {
			return
		}
		if err = nativeTypedCheckedAdd(&maps, codec); err != nil {
			return
		}
		// The declared extractor keeps one unescape buffer until the batch ends.
		if err = nativeTypedCheckedAdd(&maps, memtable.COWAllocationCharge(uint64(len(doc)))); err != nil {
			return
		}
		maps = memtable.COWAllocationCharge(maps)
		if err = nativeTypedCheckedAdd(&maps, docHeaders); err != nil {
			return
		}
		scratch = max(scratch, maps)
	}
	return
}

// Validate through the same bounded maintained constructor before canonical
// assignment. It creates no asset, manifest identity or physical publication.
// Its operation-local codec and output are gone before the stage receipt ends.
func nativeTypedValidateResidual(meta CollectionMeta, rows []columnDeclaredRow, residual [][]byte) error {
	capacity, err := nativeTypedDeltaBuildCapacity(meta, rows, residual)
	if err != nil {
		return err
	}
	capacity, err = nativeTypedCollectionBuildCapacity(meta, capacity)
	if err != nil {
		return err
	}
	if capacity > uint64(^uint64(0)>>1) {
		return memtable.ErrCOWCapacity
	}
	cfg := *meta.Options.ColumnStore
	cfg.Columns = nil
	ids := make([][]byte, len(rows))
	for i := range rows {
		ids[i] = rows[i].ID
	}
	prepared, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDsConstruction(cfg, ids, residual, int64(capacity), true)
	if prepared.semanticStreamBlocks != nil {
		prepared.semanticStreamBlocks.(*memtable.AppendOnly).ReleaseDropEntries()
	}
	return err
}

// Use the original dictionary-free, concurrency-one decoder's strongest existing
// source envelope. Its fixed terms cover all 64 separately rounded constructor
// allocations, sequence backing and a minimum 1 MiB frame window. Larger actual
// raw/input backing is charged independently from exact asset/row source below.
func nativeTypedDecoderFixedCapacity() (uint64, error) {
	sizes, err := valuelog.COWDecoderRetentionSizes(0, 1, 1, 1<<20)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, n := range []uint64{sizes.OwnerWrapper, sizes.RawBacking, sizes.InputBacking, sizes.SequenceBacking} {
		if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n)); err != nil {
			return 0, err
		}
	}
	for i := 0; i < sizes.FixedAllocationCount; i++ {
		if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(sizes.FixedAllocationBytes)); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// Physical scalar reconstruction is bounded from the exact captured manifest,
// before opening assets. Semantic-stream residual decoding is separately
// admitted from its own stored wrapper's checked raw length by the read owner.
func nativeTypedReadCapacity(snapshot *backenddb.Snapshot, root uint64) (uint64, error) {
	total, err := nativeTypedManifestCapacity(snapshot, root)
	if err != nil {
		return 0, err
	}
	fixed, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, fixed); err != nil {
		return 0, err
	}
	if root == 0 {
		return total, nil
	}
	it, err := snapshot.IteratorAtRoot(root, columnManifestPartRecordPrefixBytes, nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	for it.Valid() && bytes.HasPrefix(it.UnsafeKey(), columnManifestPartRecordPrefixBytes) {
		if !it.IsDeleted() {
			raw, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			// Fixed cursor only: no strings, records, maps or asset handles are
			// constructed before admission. Maintained decoding validates the
			// complete record later; this preflight only extracts finite sizes.
			cur := manifestCursor{raw: raw}
			if cur.u32() != columnManifestPartMagic {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			version := cur.u16()
			if version < columnManifestRecordVersionV2 || !isSupportedColumnManifestRecordVersion(version) {
				return 0, backenddb.ErrNativeSemanticUnsupported
			}
			cur.stringBytes() // kind
			cur.stringBytes() // namespace
			cur.u64()         // generation
			cur.u64()         // part ID
			cur.u64()         // file ID
			cur.u64()         // offset
			length := cur.u64()
			cur.u64() // checksum
			rows := cur.u64()
			if cur.err != nil {
				return 0, cur.err
			}
			// C=1 scalar vector/locator/descriptor containers use the maintained
			// 4224+1024 per-row proof; serialized row IDs and asset input retain
			// eight copies including decoder output and old/replacement backing.
			rowBytes, e := nativeTypedCheckedMul(rows, 4224+1024)
			if e != nil {
				return 0, e
			}
			assetBytes, e := nativeTypedCheckedMul(length, 8)
			if e != nil {
				return 0, e
			}
			for _, n := range []uint64{rowBytes, assetBytes, preparedSemanticStreamMapEntryReserveBytes} {
				if e = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n)); e != nil {
					return 0, e
				}
			}
		}
		it.Next()
	}
	return total, it.Error()
}

// Extend the same physical-read receipt for the two variable-width families.
// String dictionaries may compress much smaller than their decoded backing;
// inspect the maintained TCIM directory under preadmitted operation scratch.
// The original verified asset read and manifest parser remain authoritative.
// No parsed image, file, dictionary or cache escapes this preflight.
func nativeTypedFamilyReadCapacity(meta CollectionMeta, snapshot *backenddb.Snapshot, root uint64, rootDir string, admit func(uint64) error) (extra, typedJSON uint64, err error) {
	if !nativeTypedMeta(meta) {
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	col := meta.Options.ColumnStore.Columns[0]
	typedJSON = 20
	if col.ValueType == ColumnStoreValueInt64 || root == 0 {
		return
	}
	if col.ValueType == ColumnStoreValueFloat32Vector {
		typedJSON, err = nativeTypedCheckedMul(uint64(columnStoreFloat32VectorElementsPerRow(col)), 24)
		if err == nil {
			err = nativeTypedCheckedAdd(&typedJSON, 2)
		}
		return
	}
	metadata, err := nativeTypedManifestCapacity(snapshot, root)
	if err != nil {
		return 0, 0, err
	}
	paths := uint64(len(rootDir))
	for _, n := range []uint64{uint64(len(col.Path)), uint64(len(meta.Options.ColumnStore.AssetManager.Namespace))} {
		if err = nativeTypedCheckedAdd(&paths, n); err != nil {
			return 0, 0, err
		}
	}
	paths, err = nativeTypedCheckedMul(paths, 8)
	if err != nil {
		return 0, 0, err
	}
	if err = nativeTypedCheckedAdd(&metadata, paths); err != nil {
		return 0, 0, err
	}
	if err = nativeTypedCheckedAdd(&metadata, 4*preparedSemanticStreamMapEntryReserveBytes); err != nil {
		return 0, 0, err
	}
	if err = admit(metadata); err != nil {
		return 0, 0, err
	}
	it, err := snapshot.IteratorAtRoot(root, columnManifestPartRecordPrefixBytes, nil)
	if err != nil {
		return 0, 0, err
	}
	defer it.Close()
	for it.Valid() && bytes.HasPrefix(it.UnsafeKey(), columnManifestPartRecordPrefixBytes) {
		if !it.IsDeleted() {
			raw, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return 0, 0, backenddb.ErrNativeSemanticUnsupported
			}
			part, e := decodeColumnManifestPartRecord(raw)
			if e != nil {
				return 0, 0, e
			}
			if part.AssetRef.Kind == ColumnAssetKindTCS1TypedColumnPart {
				if part.AssetRef.Length < 0 {
					return 0, 0, backenddb.ErrNativeSemanticUnsupported
				}
				// The directory has at least 76 serialized bytes per section.
				// Sixteen source copies cover image/section headers, cloned names,
				// validation sorting and original file/path wrappers before parsing.
				scratch, e := nativeTypedCheckedMul(uint64(part.AssetRef.Length), 16)
				if e != nil {
					return 0, 0, e
				}
				if e = nativeTypedCheckedAdd(&scratch, metadata); e != nil {
					return 0, 0, e
				}
				if e = admit(scratch); e != nil {
					return 0, 0, e
				}
				asset, e := readColumnPhysicalAssetFromManager(rootDir, part.AssetRef)
				if e != nil {
					return 0, 0, e
				}
				image, e := typedcolumn.ParseColumnPartImageManifest(asset, len(asset))
				if e != nil {
					return 0, 0, e
				}
				if image.PartID != part.AssetRef.PartID || image.Rows != part.Rows {
					return 0, 0, backenddb.ErrNativeSemanticUnsupported
				}
				var decoded uint64
				for _, section := range image.Sections {
					n := uint64(max(section.RawBytes, section.Length))
					if e = nativeTypedCheckedAdd(&decoded, n); e != nil {
						return 0, 0, e
					}
					if section.Kind == typedcolumn.ColumnPartImageSectionDictionaries {
						typedJSON = max(typedJSON, n)
					}
				}
				// Dictionary keys/codes/reverse maps and reconstructed values borrow
				// or clone decoded sections. Twelve copies include decoder history,
				// output and old/replacement backing; row-map quotas are separate.
				decoded, e = nativeTypedCheckedMul(decoded, 12)
				if e != nil {
					return 0, 0, e
				}
				if e = nativeTypedCheckedAdd(&extra, memtable.COWAllocationCharge(decoded)); e != nil {
					return 0, 0, e
				}
			}
		}
		it.Next()
	}
	return extra, typedJSON, it.Error()
}
