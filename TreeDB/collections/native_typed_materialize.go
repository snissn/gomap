package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
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
	compaction         *nativeColumnCompaction
	capturedCollection *nativeCollectionState
	meta               CollectionMeta
	baseManifestRoot   uint64
	deltas             []*nativeTypedDelta // oldest first, actual assigned identities
	snapshot           *backenddb.Snapshot
	capture            *backenddb.StableResourceCaptureLease
	allocator          backenddb.NativeSemanticAllocator
	group              *backenddb.RootPublicationBuildGroup
	// Fixed operation credit stays in the SAME live build parent's floor.
	// Collections run sequentially; only their retained terms split to children.
	sharedWorkspace uint64 // This work's applicable peak, borrowed from the cohort maximum.
}
type nativeTypedMaterializedRoot struct {
	name string
	root uint64
}

type nativeTypedMaterialization struct {
	compactionRows    []columnDeclaredRow
	input             nativeTypedMaterializeInput
	roots             []nativeTypedMaterializedRoot
	meta              CollectionMeta
	resources         *rootpublication.StableResourceSet
	requirements      rootpublication.StableLogicalObligationRequirements
	appendMutation    rootpublication.StableLogicalObligationMutation
	requirementWork   rootpublication.StableResourceClosureWork
	fallback          func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error)
	sourceDirectory   *columnSourceDirectoryAppendV2
	graphAuthority    *columnVectorGraphStableResourceAccumulator
	graphSource       *columnVectorGraphTypedColumnVectorSource
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
	if !nativeTypedMeta(meta) || len(rows) != len(retained) {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	var total uint64
	for i, row := range rows {
		n, err := nativeTypedDeclaredRowBuildCapacity(meta, row, retained[i])
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&total, n); err != nil {
			return 0, err
		}
	}
	// Each aggregate entry consumes at least one source row. Repeated group
	// strings can be encoded once per spec and granule even when declared
	// backing is interned; reuse the maintained prepared publisher's source
	// total and six-copy encoding/coexistence bound.
	_, groups := preparedInsertRepeatedStringBytes(*meta.Options.ColumnStore, rows)
	groupBytes, err := nativeTypedCheckedMul(uint64(groups), 6)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(groupBytes)); err != nil {
		return 0, err
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

// Keep the identical per-row source promise for member and span construction.
// Values, IDs and retained payloads are immutable input backing, not copies.
func nativeTypedDeclaredRowBuildCapacity(meta CollectionMeta, row columnDeclaredRow, retained []byte) (uint64, error) {
	if len(row.Values) != len(meta.Options.ColumnStore.Columns) || len(row.ID) == 0 {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	total, err := nativeTypedDeltaRowBuildCapacity(meta, uint64(len(row.ID)), uint64(len(retained)))
	if err != nil {
		return 0, err
	}
	for j, col := range meta.Options.ColumnStore.Columns {
		backing, _, e := nativeTypedValueCapacity(col, row.Values[j])
		if e != nil {
			return 0, e
		}
		// Dictionary/vector/granule/section and old/replacement image backing.
		backing, e = nativeTypedCheckedMul(backing, 12)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(backing)); e != nil {
			return 0, e
		}
	}
	return total, nil
}

// Explicit source deletions share the same accepted member and construction
// promise; inserts win for overlapping IDs in both logical and physical views.
func nativeTypedMutationBuildCapacity(meta CollectionMeta, rows []columnDeclaredRow, retained, deleted [][]byte) (uint64, error) {
	total, err := nativeTypedDeltaBuildCapacity(meta, rows, retained)
	if err != nil {
		return 0, err
	}
	if len(rows) != 0 && len(deleted) != 0 {
		n, e := nativeTypedManifestAppendCapacity(meta)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, n); e != nil {
			return 0, e
		}
	}
	for _, id := range deleted {
		n, e := nativeTypedDeltaRowBuildCapacity(meta, uint64(len(id)), 0)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, n); e != nil {
			return 0, e
		}
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

// The original builder emits a row image, a TCIM image only for ColumnPart
// owners, scalar row-owner sidecars, one asset per aggregate spec, a header
// and one owned output-segment marker.
// Sixteen simultaneous record/closure/mutation views each reserve four original
// 1 KiB quota units for headers/maps plus captured schema/string backing.
// The original single-output builder keeps all these assets in one segment.
func nativeTypedManifestAppendCapacity(meta CollectionMeta) (uint64, error) {
	backing, err := nativeSchemaBackingCharge(reflect.ValueOf(&meta), 0)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&backing, 4*preparedSemanticStreamMapEntryReserveBytes); err != nil {
		return 0, err
	}
	cfg := meta.Options.ColumnStore
	records := uint64(3) // header, row image, output-segment marker
	if columnStoreHasTypedColumnPartOwners(*cfg) {
		if err = nativeTypedCheckedAdd(&records, 1); err != nil {
			return 0, err
		}
	}
	if err = nativeTypedCheckedAdd(&records, uint64(len(cfg.AggregateMetadata))); err != nil {
		return 0, err
	}
	for _, col := range cfg.Columns {
		if col.Owner == TypedStorageOwnerRowAsset && (col.ValueType == ColumnStoreValueInt64 || col.ValueType == ColumnStoreValueString && col.Dictionary) {
			if err = nativeTypedCheckedAdd(&records, 1); err != nil {
				return 0, err
			}
		}
	}
	copies, err := nativeTypedCheckedMul(records, 16)
	if err != nil {
		return 0, err
	}
	return nativeTypedCheckedMul(backing, copies)
}

// Retained source bytes bound structural entries independently of their encoded
// compression ratio. These are the maintained prepared-materializer quota terms.
func nativeTypedDeltaRowBuildCapacity(meta CollectionMeta, idBytes, retainedBytes uint64) (uint64, error) {
	var total uint64
	var err error
	// This eligible Full+JSON lane passes already-declared values and the
	// original complete JSON directly to the maintained asset builder. It never
	// constructs the semantic-stream path trie/maps/cursors: validation returns
	// early for JSON and materializeDelta assigns retained.documents=residual.
	// Keep every other existing row/image/TCIM/source/codec term below. All
	// excluded and non-column/semantic carriers retain their original promise.
	if !nativeOrdinaryColumnGraphMeta(meta) {
		structural, err := nativeTypedCheckedMul(retainedBytes, uint64(4*preparedSemanticStreamMapEntryReserveBytes+2*preparedSemanticStreamMaxPathBytes+preparedSemanticStreamMaxCursorDepth*int(unsafe.Sizeof(""))+int(unsafe.Sizeof(columnRetainedSemanticStreamPathNode{}))))
		if err != nil {
			return 0, err
		}
		total = memtable.COWAllocationCharge(structural)
	}
	// Match the maintained prepared publisher: scalar preparation per cell,
	// TCIM containers per typed cell, C+3 sidecars, row image per cell, and
	// original root/run controls per row. Codecs remain one shared workspace.
	columns := uint64(len(meta.Options.ColumnStore.Columns))
	var typedColumns uint64
	for _, col := range meta.Options.ColumnStore.Columns {
		if col.Owner == TypedStorageOwnerColumnPart {
			typedColumns++
		}
	}
	var perRow uint64
	for _, term := range [][2]uint64{{4224 + 384, columns}, {1024, typedColumns}, {1024, columns + 3}, {1024 + 2048, 1}} {
		n, err := nativeTypedCheckedMul(term[0], term[1])
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&perRow, n); err != nil {
			return 0, err
		}
	}
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

func nativeTypedBuildWorkspaceCapacity(meta CollectionMeta) (uint64, error) {
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
	// Ordinary granule statistics/pruning decode none, Snappy or LZ4 payloads;
	// their source-sized backing is in the retained construction bound. Image
	// certification validates stored metadata and hashes without decompressing
	// ZSTD sections. Only the benchmark granule override constructs a ZSTD
	// decoder during building; reads retain their independent decoder bound.
	// JSON and Full retained carriers never enter the semantic-stream encoder.
	// Keep the original codec proof only when this captured schema can use it.
	workspace := max(typed, uint64(8<<20))
	if columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) {
		workspace = max(workspace, semantic)
	}
	if raw, ok := os.LookupEnv(typedColumnBenchmarkCompressionEnv); ok {
		compression, set, err := parseTypedColumnBenchmarkCompression(raw)
		if err != nil {
			return 0, err
		}
		if set && compression == typedcolumn.CompressionZSTD {
			decoder, err := nativeTypedDecoderFixedCapacity()
			if err != nil {
				return 0, err
			}
			workspace = max(workspace, decoder+uint64(1<<20))
		}
	}
	return workspace, nil
}

// The graph-only branch returns before the row/TCIM/semantic encoders. Its
// constructor and replacement-state decoder bounds live in d.buildCapacity.
func nativeTypedGraphOnly(d *nativeTypedDelta) bool {
	return d != nil && d.previous == nil && d.data != nil && d.data.graph != nil && len(d.rows)+len(d.retained)+len(d.deleted) == 0
}

func nativeTypedOperationWorkspaceCapacity(meta CollectionMeta, compaction, graphOnly bool) (uint64, error) {
	if graphOnly {
		if compaction {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
		return 0, nil
	}
	workspace, err := nativeTypedBuildWorkspaceCapacity(meta)
	if err != nil || !compaction {
		return workspace, err
	}
	decoder, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		return 0, err
	}
	// Compaction finishes all physical decoding and closes its operation-local
	// read caches before materialize opens the builder codec. Only this fixed
	// peak is shared; decoded rows and every source-derived term stay charged.
	return max(workspace, decoder), nil
}

func nativeTypedCollectionBuildCapacity(meta CollectionMeta, sourceBytes uint64) (uint64, error) {
	total, err := nativeTypedBuildWorkspaceCapacity(meta)
	if err != nil {
		return 0, err
	}
	retained, err := nativeTypedCollectionRetainedCapacity(meta, sourceBytes)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, retained); err != nil {
		return 0, err
	}
	return total, nil
}

func nativeTypedCollectionRetainedCapacity(meta CollectionMeta, sourceBytes uint64) (uint64, error) {
	if !nativeTypedMeta(meta) {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	total := sourceBytes
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
	// The original typed receipt covers the complete original group control,
	// including its fixed four-root read window, and the borrowed reader array.
	// Generic group factories retain their existing external accounting owner.
	for _, n := range []uint64{uint64(unsafe.Sizeof(backenddb.RootPublicationBuildGroup{})), uint64(unsafe.Sizeof(nativeTypedMaterialization{})), uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})), 4 * uint64(unsafe.Sizeof(nativeTypedMaterializedRoot{})), 4 * uint64(unsafe.Sizeof(iterator.UnsafeIterator(nil)))} {
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
		n, err := nativeTypedMutationBuildCapacity(meta, d.rows, d.retained, d.deleted)
		if d.data != nil && d.data.graph != nil {
			n, err = d.buildCapacity, nil
		}
		if err != nil {
			return 0, err
		}
		if err = nativeTypedCheckedAdd(&source, n); err != nil {
			return 0, err
		}
	}
	if len(deltas) == 1 && nativeTypedGraphOnly(deltas[0]) {
		return nativeTypedCollectionRetainedCapacity(meta, source)
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
	if input.snapshot == nil || input.capture == nil || input.group == nil || (len(input.deltas) == 0 && input.compaction == nil) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	amount, err := nativeTypedMaterializationCapacity(input.meta, input.deltas)
	if err != nil {
		return nil, err
	}
	basis, err := nativeTypedBasisCapacity(input.meta, input.snapshot, input.baseManifestRoot)
	if err != nil {
		return nil, err
	}
	if err = nativeTypedCheckedAdd(&amount, basis); err != nil {
		return nil, err
	}
	if input.compaction != nil {
		read, e := nativeTypedCompactionReadRetainedCapacity(input.meta, input.snapshot, input.baseManifestRoot)
		if e != nil {
			return nil, e
		}
		if e = nativeTypedCheckedAdd(&amount, read); e != nil {
			return nil, e
		}
		build, e := nativeTypedBuildWorkspaceCapacity(input.meta)
		if e != nil {
			return nil, e
		}
		workspace, e := nativeTypedOperationWorkspaceCapacity(input.meta, true, false)
		if e != nil {
			return nil, e
		}
		if e = nativeTypedCheckedAdd(&amount, workspace-build); e != nil {
			return nil, e
		}
	}
	var lease *memtable.COWExternalLease
	var owned uint64
	var refund *cowbtree.OwnedMapRefund
	if input.allocator != nil {
		owned = amount
		if input.sharedWorkspace != 0 {
			fixed, e := nativeTypedOperationWorkspaceCapacity(input.meta, input.compaction != nil, len(input.deltas) == 1 && nativeTypedGraphOnly(input.deltas[0]))
			if e != nil || input.sharedWorkspace != fixed || owned < fixed {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			owned -= fixed
		}
		var admitErr error
		lease, admitErr = input.allocator.AdmitNativeBytes(owned)
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
	// Match collectionRootNames: JSON carriers have three domains. Only the
	// semantic carrier owns a fourth root; the same inventory drives replay and
	// the live build's root-descriptor handoff.
	w.roots = make([]nativeTypedMaterializedRoot, 0, 4)
	w.roots = append(w.roots, nativeTypedMaterializedRoot{name: "primary"}, nativeTypedMaterializedRoot{name: "column/manifest", root: input.baseManifestRoot}, nativeTypedMaterializedRoot{name: "column/row-locator"})
	if columnStoreRetainedPayloadUsesSemanticStreamV1(input.meta.Options.ColumnStore) {
		w.roots = append(w.roots, nativeTypedMaterializedRoot{name: "retained/semantic-stream-v1"})
	}
	token, ok := input.snapshot.StateToken()
	if !ok {
		return w, backenddb.ErrNativeSemanticUnsupported
	}
	for i := range w.roots {
		if i == 1 {
			continue
		}
		if input.compaction != nil {
			domain, exists := input.capturedCollection.domains.Get(w.roots[i].name)
			if !exists {
				return w, backenddb.ErrNativeSemanticUnsupported
			}
			w.roots[i].root = domain.baseRoot
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
	if input.baseManifestRoot != 0 && !nativeSourceImportMetaV2(input.meta) {
		w.records, err = loadColumnManifestRecordsFromRoot(input.snapshot, input.baseManifestRoot)
		if err != nil {
			return w, err
		}
	}
	w.resourceBuilder = rootpublication.NewStableResourceSetBuilder()
	if input.compaction != nil {
		if lease == nil {
			return w, backenddb.ErrNativeSemanticUnsupported
		}
		err = w.prepareCompactionRows(func(n uint64) error {
			next := owned
			if e := nativeTypedCheckedAdd(&next, n); e != nil {
				return e
			}
			if e := lease.Resize(next); e != nil {
				return e
			}
			owned = next
			return nativeTypedCheckedAdd(&w.capacity, n)
		})
		if err != nil {
			return w, err
		}
	}
	return w, nil
}

// This index borrows immutable input bytes. It selects a captured physical cut;
// it never changes the command history or the deltas retained by logical views.
// A negative row denotes deleted[-row-1]; nonnegative rows win a same-member
// source deletion, exactly as nativeTypedDelta.find does.
type nativeTypedMaterializeMutation struct {
	id    []byte
	delta int
	row   int
}

func selectNativeTypedMaterializeMutations(deltas []*nativeTypedDelta) ([]nativeTypedMaterializeMutation, error) {
	var count uint64
	var previousLSN uint64
	var previousMember uint32
	for _, d := range deltas {
		// Validate the complete original order, including shadowed members.
		if d == nil || d.appliedLSN == 0 || len(d.rows) != len(d.retained) ||
			(previousLSN != 0 && (d.appliedLSN < previousLSN || (d.appliedLSN == previousLSN && d.memberOrdinal <= previousMember))) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		previousLSN, previousMember = d.appliedLSN, d.memberOrdinal
		for i, row := range d.rows {
			if len(row.ID) == 0 || (i > 0 && bytes.Compare(d.rows[i-1].ID, row.ID) >= 0) {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
		}
		for _, id := range d.deleted {
			if len(id) == 0 {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
		}
		if err := nativeTypedCheckedAdd(&count, uint64(len(d.rows))); err != nil {
			return nil, err
		}
		if err := nativeTypedCheckedAdd(&count, uint64(len(d.deleted))); err != nil {
			return nil, err
		}
	}
	if count == 0 || count > uint64(int(^uint(0)>>1)) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	// The existing per-mutation 2 KiB run/control allowance covers this index
	// and ALL simultaneous selected row/residual/delete/ID/document headers
	// and physical-input frames (including a possible escaped plan callback).
	// Check their rounded worst case before allocation; ID, value and residual
	// backing is borrowed. Count every header for every input mutation, including
	// the selector's original capacity after winners are compacted. The separate
	// original root/run and codec promises remain charged without any refund.
	var selectionBytes uint64
	for _, width := range []uint64{uint64(unsafe.Sizeof(nativeTypedMaterializeMutation{})), uint64(unsafe.Sizeof(columnDeclaredRow{})), uint64(unsafe.Sizeof([]byte{})), uint64(unsafe.Sizeof([]byte{})), uint64(unsafe.Sizeof([]byte{})), uint64(unsafe.Sizeof(columnWriteDocument{})), uint64(unsafe.Sizeof(columnWriteDocument{})), uint64(unsafe.Sizeof(nativeTypedPhysicalRows{}))} {
		n, err := nativeTypedCheckedMul(count, width)
		if err != nil {
			return nil, err
		}
		if err = nativeTypedCheckedAdd(&selectionBytes, memtable.COWAllocationCharge(n)); err != nil {
			return nil, err
		}
	}
	allowance, err := nativeTypedCheckedMul(count, 2048)
	if err != nil || selectionBytes > allowance {
		return nil, memtable.ErrCOWCapacity
	}
	mutations := make([]nativeTypedMaterializeMutation, 0, int(count))
	for delta, d := range deltas {
		for row, value := range d.rows {
			mutations = append(mutations, nativeTypedMaterializeMutation{id: value.ID, delta: delta, row: row})
		}
		for row, id := range d.deleted {
			mutations = append(mutations, nativeTypedMaterializeMutation{id: id, delta: delta, row: -row - 1})
		}
	}
	// O(N log N), independent of the number of members and distinct IDs. The
	// first occurrence of each ID is its last writer in this exact prefix.
	slices.SortFunc(mutations, func(a, b nativeTypedMaterializeMutation) int {
		if order := bytes.Compare(a.id, b.id); order != 0 {
			return order
		}
		if a.delta != b.delta {
			if a.delta > b.delta {
				return -1
			}
			return 1
		}
		if a.row >= 0 && b.row < 0 {
			return -1
		}
		if a.row < 0 && b.row >= 0 {
			return 1
		}
		return 0
	})
	winners := 0
	for i := 0; i < len(mutations); {
		next := i + 1
		for next < len(mutations) && bytes.Equal(mutations[i].id, mutations[next].id) {
			next++
		}
		mutations[winners] = mutations[i]
		winners++
		i = next
	}
	clear(mutations[winners:])
	mutations = mutations[:winners]
	// Restore original member order and sort its selected rows and deletions by
	// ID. Canonical deletion IDs may retain caller order. The materializer keeps
	// the original member's LSN/ordinal.
	slices.SortFunc(mutations, func(a, b nativeTypedMaterializeMutation) int {
		if a.delta != b.delta {
			if a.delta < b.delta {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.id, b.id)
	})
	return mutations, nil
}

// Eligibility and the source-promise inequality are checked before aggregate
// headers or assets are constructed. A false result leaves the original member
// path untouched. Errors after span construction begins never retry that path.
func (w *nativeTypedMaterialization) canMaterializeInsertSpan(deltas []*nativeTypedDelta, selected []nativeTypedMaterializeMutation) (bool, error) {
	if w.input.compaction != nil || len(deltas) < 2 || !nativeOrdinaryColumnGraphMeta(w.meta) {
		return false, nil
	}
	var original, held uint64
	for _, d := range deltas {
		if d.operation != ColumnPublishOperationInsert || len(d.deleted) != 0 || (d.data != nil && (d.data.graph != nil || d.data.source != nil)) {
			return false, nil
		}
		n, err := nativeTypedDeltaBuildCapacity(w.input.meta, d.rows, d.retained)
		if err != nil {
			return false, err
		}
		if err = nativeTypedCheckedAdd(&original, n); err != nil {
			return false, err
		}
		if err = nativeTypedCheckedAdd(&held, d.buildCapacity); err != nil {
			return false, err
		}
	}
	// Ordinary Full+JSON has one vector column and no aggregates or string
	// groups. Its aggregate source requirement is exactly these winning rows
	// plus one maintained manifest allowance. Do not spend the unused member
	// allowances elsewhere or reduce the original construction receipt.
	aggregate, err := nativeTypedManifestAppendCapacity(w.meta)
	if err != nil {
		return false, err
	}
	for _, mutation := range selected {
		if mutation.row < 0 {
			return false, nil
		}
		d := deltas[mutation.delta]
		n, e := nativeTypedDeclaredRowBuildCapacity(w.meta, d.rows[mutation.row], d.retained[mutation.row])
		if e != nil {
			return false, e
		}
		if e = nativeTypedCheckedAdd(&aggregate, n); e != nil {
			return false, e
		}
	}
	return len(selected) != 0 && aggregate <= min(original, held), nil
}

func (w *nativeTypedMaterialization) materializeInsertSpan(context columnPhysicalAssetBuildContext, deltas []*nativeTypedDelta, selected []nativeTypedMaterializeMutation) error {
	_ = w.codec.Close()
	if cfg := w.meta.Options.ColumnStore; cfg.ActiveManifest != nil && cfg.ActiveManifest.Generation == ^uint64(0) {
		return memtable.ErrCOWCapacity
	}
	// The selector has already validated EVERY original member and selected
	// the last writer. Only its borrowed headers are reordered for one physical
	// row lane; original commands, deltas and captured row backing are untouched.
	slices.SortFunc(selected, func(a, b nativeTypedMaterializeMutation) int {
		return bytes.Compare(a.id, b.id)
	})
	rows := make([]columnDeclaredRow, len(selected))
	residual := make([][]byte, len(selected))
	for i, mutation := range selected {
		d := deltas[mutation.delta]
		rows[i], residual[i] = d.rows[mutation.row], d.retained[mutation.row]
	}
	// Physical coverage ends at this namespace's last ORIGINAL member, not at
	// the enclosing global checkpoint frontier. Even a shadowed final member
	// remains covered; no synthetic command or replacement delta is introduced.
	endpoint := deltas[len(deltas)-1]
	return w.materializeRows(context, nativeTypedPhysicalRows{
		rows: rows, residual: residual, operation: ColumnPublishOperationInsert,
		appliedLSN: endpoint.appliedLSN, memberOrdinal: endpoint.memberOrdinal,
	})
}

func (w *nativeTypedMaterialization) materialize() (resultErr error) {
	if w == nil || w.started {
		return backenddb.ErrNativeSemanticUnsupported
	}
	w.started = true
	defer func() {
		w.codec.Close()
		w.input = nativeTypedMaterializeInput{}
		w.compactionRows = nil
		if resultErr != nil {
			w.failure = resultErr
		} else {
			w.complete = true
		}
	}()
	var previousLSN uint64
	var previousMember uint32
	for _, d := range w.input.deltas {
		if d == nil || d.appliedLSN == 0 || (previousLSN != 0 && (d.appliedLSN < previousLSN || (d.appliedLSN == previousLSN && d.memberOrdinal <= previousMember))) {
			return backenddb.ErrNativeSemanticUnsupported
		}
		previousLSN, previousMember = d.appliedLSN, d.memberOrdinal
	}
	rootDir, registry, err := w.input.group.NativeColumnAssetBuildContext()
	if err != nil {
		return err
	}
	validateOptions, err := w.ordinaryGraphPendingSourceValidator()
	if err != nil {
		return err
	}
	context := columnPhysicalAssetBuildContext{rootDir: rootDir, registry: registry, recovery: w.input.capture, codec: &w.codec, singleOutput: true, typedColumnOptionsValidator: validateOptions}
	// Graph recipes are barriers: later mutations must not shadow the source
	// visible to an earlier recipe. Reuse the original row selector per segment.
	for first := 0; first < len(w.input.deltas); {
		end := first
		for end < len(w.input.deltas) && (w.input.deltas[end] == nil || w.input.deltas[end].data == nil || w.input.deltas[end].data.graph == nil) {
			end++
		}
		if end > first {
			mutations, e := selectNativeTypedMaterializeMutations(w.input.deltas[first:end])
			if e != nil {
				return e
			}
			span, e := w.canMaterializeInsertSpan(w.input.deltas[first:end], mutations)
			if e != nil {
				return e
			}
			if span {
				if err = w.materializeInsertSpan(context, w.input.deltas[first:end], mutations); err != nil {
					return err
				}
			}
			for start := 0; !span && start < len(mutations); {
				stop := start + 1
				for stop < len(mutations) && mutations[stop].delta == mutations[start].delta {
					stop++
				}
				d := w.input.deltas[first+mutations[start].delta]
				if err = w.materializeDelta(context, d, mutations[start:stop]); err != nil {
					return err
				}
				start = stop
			}
		}
		if end == len(w.input.deltas) {
			break
		}
		d := w.input.deltas[end]
		if w.input.compaction != nil || len(d.rows)+len(d.retained)+len(d.deleted) != 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		if !d.data.graph.ordinary {
			if len(w.input.deltas) != 1 {
				return backenddb.ErrNativeSemanticUnsupported
			}
			return w.materializeVectorGraph(d)
		}
		if err = w.materializeOrdinaryVectorGraph(d); err != nil {
			return err
		}
		first = end + 1
	}
	if w.input.compaction != nil {
		if err = w.materializeCompaction(context); err != nil {
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

// A physical row input may describe one original member or an accepted span's
// endpoint. It is not a logical command/delta and never owns input row backing.
type nativeTypedPhysicalRows struct {
	rows          []columnDeclaredRow
	residual      [][]byte
	deletedIDs    [][]byte
	operation     ColumnPublishOperation
	appliedLSN    uint64
	memberOrdinal uint32
	source        *nativeSourceImportDeltaV2
}

func (w *nativeTypedMaterialization) materializeDelta(context columnPhysicalAssetBuildContext, d *nativeTypedDelta, selected []nativeTypedMaterializeMutation) error {
	// Each command's semantic phase precedes its typed encoder phase. The
	// maintained asset helper also closes after joining its encoder goroutine;
	// drop again here before the next command.
	_ = w.codec.Close()
	if cfg := w.meta.Options.ColumnStore; cfg.ActiveManifest != nil && cfg.ActiveManifest.Generation == ^uint64(0) {
		return memtable.ErrCOWCapacity
	}
	rowCount := 0
	for _, mutation := range selected {
		if mutation.row >= 0 {
			rowCount++
		}
	}
	rows := make([]columnDeclaredRow, 0, rowCount)
	residual := make([][]byte, 0, rowCount)
	deletedIDs := make([][]byte, 0, len(selected)-rowCount)
	for _, mutation := range selected {
		if mutation.row >= 0 {
			rows = append(rows, d.rows[mutation.row])
			residual = append(residual, d.retained[mutation.row])
		} else {
			deletedIDs = append(deletedIDs, d.deleted[-mutation.row-1])
		}
	}
	var source *nativeSourceImportDeltaV2
	if d.data != nil {
		source = d.data.source
	}
	return w.materializeRows(context, nativeTypedPhysicalRows{
		rows: rows, residual: residual, deletedIDs: deletedIDs, operation: d.operation,
		appliedLSN: d.appliedLSN, memberOrdinal: d.memberOrdinal, source: source,
	})
}

// One maintained constructor/asset-custody/root suffix serves both inputs.
// Neither successful nor failed construction changes accepted command history.
func (w *nativeTypedMaterialization) materializeRows(context columnPhysicalAssetBuildContext, physical nativeTypedPhysicalRows) error {
	cfg := *w.meta.Options.ColumnStore
	if cfg.ActiveManifest != nil && cfg.ActiveManifest.Generation == ^uint64(0) {
		return memtable.ErrCOWCapacity
	}
	rows, residual, deletedIDs := physical.rows, physical.residual, physical.deletedIDs
	// The input already IS the retained noncolumn carrier. A zero-declaration
	// view preserves every residual field through the maintained semantic codec;
	// no full JSON document or declared-column shadow is reconstructed here.
	residualConfig := cfg
	residualConfig.Columns = nil
	ids := make([][]byte, len(rows))
	documents := make([]columnWriteDocument, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
		documents[i] = columnWriteDocument{ID: row.ID}
	}
	var retained columnRetainedPayloadStorageDocuments
	var err error
	if columnRetainedPayloadEffectiveEncoding(&cfg) == ColumnRetainedPayloadEncodingSemanticStreamV1 && len(rows) != 0 {
		retained, err = prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDsConstruction(residualConfig, ids, residual, int64(w.capacity), true)
		if err != nil {
			return err
		}
		defer retained.semanticStreamBlocks.(*memtable.AppendOnly).ReleaseDropEntries()
	} else {
		retained.documents = residual
	}
	for i := range documents {
		documents[i].Document = retained.documents[i]
	}
	deleted := make([]columnWriteDocument, len(deletedIDs))
	for i, id := range deletedIDs {
		deleted[i] = columnWriteDocument{ID: id}
	}
	op := physical.operation
	if op == "" {
		op = ColumnPublishOperationInsert
	}
	input := columnWritePublishInput{meta: w.meta, operation: op, documents: documents, rows: len(documents), declaredRows: rows, declaredRowsReady: true}
	if len(deleted) != 0 {
		if len(documents) == 0 {
			input.operation, input.documents, input.rows = ColumnPublishOperationDelete, deleted, len(deleted)
		} else {
			input.sourceDeleteDocuments = deleted
		}
	}
	var source *columnSourceDirectoryAppendV2
	if physical.source != nil {
		publication := physical.source.publication // private once-bound seal and diagnostics
		input.sourceImportV2 = &publication
		if w.sourceDirectory == nil {
			var e error
			source, e = prepareColumnSourceDirectoryAppendAtSnapshotV2(nil, input, w.input.baseManifestRoot, w.input.snapshot)
			if e != nil {
				return e
			}
		} else {
			source = &columnSourceDirectoryAppendV2{indexes: w.meta.VectorIndexes, previous: w.sourceDirectory.previous, activeSegment: w.sourceDirectory.activeSegment, publication: &publication, baseRoot: w.input.baseManifestRoot, snapshot: w.input.snapshot}
		}
		source.snapshot = w.input.snapshot
		source.fresh = w.records
		w.sourceDirectory = source
	}
	records := w.records
	if source != nil {
		// cdir2 appends only this delta; retained requirements use the captured
		// root plus the separately owned fresh suffix, never a complete array.
		records = nil
	}
	plan, err := BuildColumnPublishPlan(ColumnPublishPlanInput{Collection: w.meta.Name, ColumnStore: &cfg, ColumnStoreNormalized: true, ActiveVectorIndexesKnown: true, Operation: input.operation, CurrentManifest: cfg.ActiveManifest, CurrentManifestRecords: records, sourceDirectoryV2: source, AppliedCommandLSN: physical.appliedLSN, BaseManifestRootID: w.roots[1].root, Hooks: ColumnPublishPlanHooks{
		PrepareAssets: func(hook ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
			prepared, e := context.prepareAssets(input, hook)
			if e != nil {
				return prepared, fmt.Errorf("collections: native typed assets collection=%q lsn=%d member=%d: %w", w.meta.Name, physical.appliedLSN, physical.memberOrdinal, e)
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
				return prepared, fmt.Errorf("collections: native typed asset custody clone: %w", cloneErr)
			}
			// Merge consumes its child. Preserve the plan's independent set for
			// its own closure validation and retain the clone in original work.
			if e = w.resourceBuilder.Merge(custody); e != nil {
				custody.Release()
				prepared.stableResources.Release()
				prepared.stableResources = nil
				return prepared, fmt.Errorf("collections: native typed asset custody merge: %w", e)
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
	if err = w.buildDeletedDocuments(0, deletedIDs); err != nil {
		return err
	}
	if err = w.buildDeletedDocuments(2, deletedIDs); err != nil {
		return err
	}
	rowPartID := columnPhysicalRowAssetPartID
	if len(input.sourceDeleteDocuments) != 0 {
		rowPartID += 1 << 32
	}
	if err = w.buildDocuments(0, documents, plan, false, rowPartID); err != nil {
		return err
	}
	if err = w.buildDocuments(2, documents, plan, true, rowPartID); err != nil {
		return err
	}
	if retained.semanticStreamBlocks != nil {
		it := retained.semanticStreamBlocks.NewIterator(nil, nil)
		policy, err = collectionRootStoragePolicy(w.meta, w.meta.Name+"/"+w.roots[3].name)
		if err == nil {
			err = w.buildIterator(3, it, policy)
		}
		_ = it.Close()
		if err != nil {
			return err
		}
	}
	updated, err := columnPublishUpdatedMeta(w.meta, plan)
	if err != nil {
		return err
	}
	w.meta = updated
	if source != nil {
		// Every source receipt belongs to the same original private system root.
		if err = w.buildSourceImportSystemDelta(source.publication); err != nil {
			return err
		}
		return w.finishSourceDirectoryDelta(plan, source)
	}
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

func (w *nativeTypedMaterialization) buildDocuments(index int, documents []columnWriteDocument, plan ColumnPublishPlan, locator bool, rowPartID uint64) error {
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
			value = encodeColumnPrimaryRowLocator(DocumentRowRef{DocumentID: document.ID, Generation: plan.UpdatedActiveManifest.Generation, PartID: rowPartID, RowIndex: i, AppliedCommandLSN: plan.AppliedCommandLSN})
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

func (w *nativeTypedMaterialization) buildDeletedDocuments(index int, ids [][]byte) error {
	if len(ids) == 0 {
		return nil
	}
	policy, err := collectionRootStoragePolicy(w.meta, w.meta.Name+"/"+w.roots[index].name)
	if err != nil {
		return err
	}
	delta := batch.NewBoundedRetainingLargeEntries(nil, int(^uint(0)>>1), nativeBuildBatchEntries)
	defer delta.CloseWithoutPooling()
	for i, id := range ids {
		if err = delta.AppendDeleteViewTrustedSortedUnique(id); err != nil {
			return err
		}
		if delta.Len() == nativeBuildBatchEntries || i == len(ids)-1 {
			root, e := w.input.group.BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: w.roots[index].root, Delta: delta, StoragePolicy: policy})
			if e != nil {
				return e
			}
			w.roots[index].root = root
			delta.Reset()
		}
	}
	return nil
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
	w.compactionRows = nil
	w.fallback = nil
	w.sourceDirectory = nil
	w.appendMutation = rootpublication.StableLogicalObligationMutation{}
	if w.graphSource != nil {
		if err = w.graphSource.Close(); err != nil {
			return cowbtree.OwnedMapCompletion{}, err
		}
		w.graphSource = nil
	}
	if w.graphAuthority != nil && w.graphAuthority.builder != nil {
		err = w.graphAuthority.builder.AbandonWithError()
		if err != nil {
			return cowbtree.OwnedMapCompletion{}, err
		}
		w.graphAuthority.builder = nil
	}
	w.graphAuthority = nil
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

// Top-level stage uses the maintained RawMessage transform: each member needs
// at least four source bytes. Nested declared paths use the original UseNumber
// interface extractor and residual walker; admit a full quota unit per source
// byte for their object/array/scalar/header/stack and old/replacement backing.
// These temporary trees are serial per document and never become a delta owner.
// The JSON encoder can escape each source byte to six output bytes; four
// such source-sized buffers cover old/new marshal state and the owned copy.
// Constructor scratch is admitted separately from the actual stripped residual
// after these parser temporaries have been dropped.
func nativeTypedStageCapacity(meta CollectionMeta, ids, docs [][]byte) (retained, scratch uint64, err error) {
	if !nativeTypedMeta(meta) || len(ids) != len(docs) {
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	columns := meta.Options.ColumnStore.Columns
	var nested bool
	var pathScratch uint64
	for _, col := range columns {
		nested = nested || strings.Contains(col.Path, ".")
		// lookup/delete split paths independently. Bound one descriptor per
		// path byte, including missing nullable ancestors in a tiny document.
		n, e := nativeTypedCheckedMul(uint64(len(col.Path)+1), uint64(unsafe.Sizeof("")))
		if e != nil {
			return 0, 0, e
		}
		if e = nativeTypedCheckedAdd(&pathScratch, memtable.COWAllocationCharge(n)); e != nil {
			return 0, 0, e
		}
	}
	for _, n := range []uint64{uint64(unsafe.Sizeof(nativeTypedRows{})), uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{}))} {
		if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(n)); err != nil {
			return
		}
	}
	for _, width := range []uint64{uint64(unsafe.Sizeof(columnDeclaredRow{})), uint64(unsafe.Sizeof([]byte{}))} {
		n, e := nativeTypedCheckedMul(uint64(len(ids)), width)
		if e != nil {
			return 0, 0, e
		}
		if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(n)); err != nil {
			return
		}
	}
	docHeaders, err := nativeTypedCheckedMul(uint64(len(ids)), uint64(unsafe.Sizeof(columnWriteDocument{})))
	if err != nil {
		return 0, 0, err
	}
	docHeaders = memtable.COWAllocationCharge(docHeaders)
	parserSlots, err := nativeTypedCheckedMul(uint64(len(columns)), uint64(unsafe.Sizeof(jsonParserIndexValue{})))
	if err != nil {
		return 0, 0, err
	}
	parserSlots = memtable.COWAllocationCharge(parserSlots)
	scratch = docHeaders
	if err = nativeTypedCheckedAdd(&scratch, parserSlots); err != nil {
		return
	}
	scratchHeaders := scratch
	for i, doc := range docs {
		var output uint64
		output, err = nativeTypedCheckedMul(uint64(len(doc)), 6)
		if err != nil {
			return
		}
		if err = nativeTypedCheckedAdd(&output, 2); err != nil {
			return
		}
		values, e := nativeTypedCheckedMul(uint64(len(columns)), uint64(unsafe.Sizeof(columnDeclaredValue{})))
		if e != nil {
			return 0, 0, e
		}
		for _, n := range []uint64{uint64(len(ids[i])), values, output} {
			if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(n)); err != nil {
				return
			}
		}
		for _, col := range columns {
			var declared uint64
			switch col.ValueType {
			case ColumnStoreValueString:
				declared = uint64(len(doc)) // unescaping cannot exceed source
			case ColumnStoreValueFloat32Vector:
				// Include configured capacity and invalid source-array growth.
				declared, err = nativeTypedCheckedMul(max(uint64(columnStoreFloat32VectorElementsPerRow(col)), uint64(len(doc))), 8)
				if err != nil || declared > uint64(maxCollectionInt) {
					return 0, 0, memtable.ErrCOWCapacity
				}
			}
			if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(declared)); err != nil {
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
		if nested {
			maps, err = nativeTypedJSONReadCharge(uint64(len(doc)))
			if err != nil {
				return
			}
			if err = nativeTypedCheckedAdd(&maps, pathScratch); err != nil {
				return
			}
		}
		if err = nativeTypedCheckedAdd(&maps, scratchHeaders); err != nil {
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
	// JSON retained bytes were validated by the maintained trusted projection.
	// This physical carrier has no semantic-stream constructor.
	if len(rows) == 0 || columnRetainedPayloadEffectiveEncoding(meta.Options.ColumnStore) == ColumnRetainedPayloadEncodingJSON {
		return nil
	}
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

// Keep the entire existing physical-read bound except its fixed decoder peak.
// The original build parent's operation floor covers that peak through the read
// phase; manifest, asset, decoded-row and variable-family backing can outlive it.
func nativeTypedCompactionReadRetainedCapacity(meta CollectionMeta, snapshot *backenddb.Snapshot, root uint64) (uint64, error) {
	read, err := nativeTypedReadCapacity(meta, snapshot, root)
	if err != nil {
		return 0, err
	}
	fixed, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		return 0, err
	}
	if read < fixed {
		return 0, memtable.ErrCOWCapacity
	}
	return read - fixed, nil
}

// Physical scalar reconstruction is bounded from the exact captured manifest,
// before opening assets. Semantic-stream residual decoding is separately
// admitted from its own stored wrapper's checked raw length by the read owner.
func nativeTypedReadCapacity(meta CollectionMeta, snapshot *backenddb.Snapshot, root uint64, selected ...columnManifestRecord) (uint64, error) {
	total, err := nativeTypedReadManifestCapacity(snapshot, root, selected)
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
	err = walkNativeTypedReadRecords(snapshot, root, selected, func(raw []byte, flags byte) error {
		if flags&node.FlagPointer != 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		// Fixed cursor only: no strings, records, maps or asset handles are
		// constructed before admission. Maintained decoding validates the
		// complete record later; this preflight only extracts finite sizes.
		cur := manifestCursor{raw: raw}
		if cur.u32() != columnManifestPartMagic {
			return backenddb.ErrNativeSemanticUnsupported
		}
		version := cur.u16()
		if version < columnManifestRecordVersionV2 || !isSupportedColumnManifestRecordVersion(version) {
			return backenddb.ErrNativeSemanticUnsupported
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
			return cur.err
		}
		// The maintained scalar preparation proof is per schema cell; the
		// locator/descriptor term remains per row. Asset section raw lengths
		// are additionally preflighted below for variable-width families.
		cells, e := nativeTypedCheckedMul(uint64(len(meta.Options.ColumnStore.Columns)), 4224)
		if e != nil {
			return e
		}
		if e = nativeTypedCheckedAdd(&cells, 1024); e != nil {
			return e
		}
		rowBytes, e := nativeTypedCheckedMul(rows, cells)
		if e != nil {
			return e
		}
		assetBytes, e := nativeTypedCheckedMul(length, 8)
		if e != nil {
			return e
		}
		for _, n := range []uint64{rowBytes, assetBytes, preparedSemanticStreamMapEntryReserveBytes} {
			if e = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n)); e != nil {
				return e
			}
		}
		return nil

	})
	return total, err
}

// Extend the same physical-read receipt for the two variable-width families.
// String dictionaries may compress much smaller than their decoded backing;
// inspect the maintained TCIM directory under preadmitted operation scratch.
// The original verified asset read and manifest parser remain authoritative.
// No parsed image, file, dictionary or cache escapes this preflight.
type nativeTypedFamilyReadShape struct {
	decodedBytes    uint64
	dictionarySlots uint64
	// The graph projection uses scan-only typed decoding: metadata sections are
	// validated as stored bytes but never decompressed. A data section can still
	// require the original fixed ZSTD decoder; other read callers ignore this flag.
	columnDataZSTD bool
}

func nativeTypedFamilyReadCapacity(meta CollectionMeta, snapshot *backenddb.Snapshot, root uint64, rootDir string, shape *nativeTypedFamilyReadShape, admit func(uint64) error, selected ...columnManifestRecord) (extra, typedJSON uint64, err error) {
	if !nativeTypedMeta(meta) {
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	var rowStrings, typedStrings uint64
	needsAssets := false
	for _, col := range meta.Options.ColumnStore.Columns {
		var n uint64
		switch col.ValueType {
		case ColumnStoreValueInt64:
			n = 20
		case ColumnStoreValueFloat32Vector:
			n, err = nativeTypedCheckedMul(uint64(columnStoreFloat32VectorElementsPerRow(col)), 24)
			if err == nil {
				err = nativeTypedCheckedAdd(&n, 2)
			}
			needsAssets = true
		case ColumnStoreValueString:
			n = 2
			needsAssets = true
			if col.Owner == TypedStorageOwnerRowAsset {
				rowStrings++
			} else {
				typedStrings++
			}
		}
		if err != nil {
			return 0, 0, err
		}
		if err = nativeTypedCheckedAdd(&typedJSON, n); err != nil {
			return 0, 0, err
		}
	}
	if !needsAssets || root == 0 {
		return
	}
	metadata, err := nativeTypedReadManifestCapacity(snapshot, root, selected)
	if err != nil {
		return 0, 0, err
	}
	paths := uint64(len(rootDir))
	if err = nativeTypedCheckedAdd(&paths, uint64(len(meta.Options.ColumnStore.AssetManager.Namespace))); err != nil {
		return 0, 0, err
	}
	for _, col := range meta.Options.ColumnStore.Columns {
		if err = nativeTypedCheckedAdd(&paths, uint64(len(col.Path))); err != nil {
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
	var rowStringBytes, typedStringBytes uint64
	err = walkNativeTypedReadRecords(snapshot, root, selected, func(raw []byte, flags byte) error {
		if flags&node.FlagPointer != 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		part, e := decodeColumnManifestPartRecord(raw)
		if e != nil {
			return e
		}
		if rowStrings != 0 && part.AssetRef.Kind == ColumnAssetKindTCS1PartImage {
			// The maintained row image stores strings as uncompressed length-
			// prefixed bytes. Its exact captured length bounds one declared
			// output before the reader allocates or decodes that row image.
			if part.AssetRef.Length < 0 {
				return backenddb.ErrNativeSemanticUnsupported
			}
			rowStringBytes = max(rowStringBytes, uint64(part.AssetRef.Length))
		}
		if part.AssetRef.Kind == ColumnAssetKindTCS1TypedColumnPart {
			if part.AssetRef.Length < 0 {
				return backenddb.ErrNativeSemanticUnsupported
			}
			// The directory has at least 76 serialized bytes per section.
			// Sixteen source copies cover image/section headers, cloned names,
			// validation sorting and original file/path wrappers before parsing.
			scratch, e := nativeTypedCheckedMul(uint64(part.AssetRef.Length), 16)
			if e != nil {
				return e
			}
			if e = nativeTypedCheckedAdd(&scratch, metadata); e != nil {
				return e
			}
			if e = admit(scratch); e != nil {
				return e
			}
			asset, e := readColumnPhysicalAssetFromManager(rootDir, part.AssetRef)
			if e != nil {
				return e
			}
			image, e := typedcolumn.ParseColumnPartImageManifest(asset, len(asset))
			if e != nil {
				return e
			}
			if image.PartID != part.AssetRef.PartID || image.Rows != part.Rows {
				return backenddb.ErrNativeSemanticUnsupported
			}
			var decoded uint64
			for _, section := range image.Sections {
				n := uint64(max(section.RawBytes, section.Length))
				if e = nativeTypedCheckedAdd(&decoded, n); e != nil {
					return e
				}
				if shape != nil {
					if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Compression == typedcolumn.CompressionZSTD {
						shape.columnDataZSTD = true
					}
					if e = nativeTypedCheckedAdd(&shape.decodedBytes, n); e != nil {
						return e
					}
				}
				if section.Kind == typedcolumn.ColumnPartImageSectionDictionaries {
					typedStringBytes = max(typedStringBytes, n)
					if shape != nil {
						minimum := uint64(12)
						if section.Encoding == typedcolumn.EncodingDictionaryDense {
							minimum = 4
						}
						if e = nativeTypedCheckedAdd(&shape.dictionarySlots, n/minimum+n/8); e != nil {
							return e
						}
					}
				}
			}
			// Dictionary keys/codes/reverse maps and reconstructed values borrow
			// or clone decoded sections. Twelve copies include decoder history,
			// output and old/replacement backing; row-map quotas are separate.
			decoded, e = nativeTypedCheckedMul(decoded, 12)
			if e != nil {
				return e
			}
			if e = nativeTypedCheckedAdd(&extra, memtable.COWAllocationCharge(decoded)); e != nil {
				return e
			}
			// The next asset may resize this operation's scratch receipt.
			image = typedcolumn.ColumnPartImage{}
			asset = nil
		}
		return nil

	})
	if err != nil {
		return 0, 0, err
	}
	// Each string can use at most the complete row image or dictionary section.
	// Sum the per-column bound, while inspecting each captured asset only once.
	for _, term := range [][2]uint64{{rowStrings, rowStringBytes}, {typedStrings, typedStringBytes}} {
		n, e := nativeTypedCheckedMul(term[0], term[1])
		if e != nil {
			return 0, 0, e
		}
		if e = nativeTypedCheckedAdd(&typedJSON, n); e != nil {
			return 0, 0, e
		}
	}
	return extra, typedJSON, nil
}
