package collections

import (
	"errors"
	"math"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The original queue borrows this exact schema capture until the synchronous
// request returns. Its independent input receipt never owns another publisher.
type nativeTypedUpsertRequest struct {
	read    *NativeJSONRead
	columns []TypedColumnBatch
}

func nativeTypedUpsertCompatible(first, next *typedUpsertGroupRequest) bool {
	if first.native == nil || next.native == nil {
		return false
	}
	a, b := first.native.read, next.native.read
	return a.database == b.database && a.collection.incarnation == b.collection.incarnation &&
		a.collection.schema == b.collection.schema && first.collection.name == next.collection.name
}

func nativeTypedUpsertEligible(database *backenddb.DB, meta CollectionMeta) bool {
	_, err := nativeJSONMeta(meta)
	return err == nil && database.ResolvedProfile() == backenddb.ProfileCommandWALDurable && nativeTypedMeta(meta) &&
		meta.Options.ColumnStore.RetainedPayload == ColumnRetainedPayloadNonColumn &&
		columnRetainedPayloadEffectiveEncoding(meta.Options.ColumnStore) == ColumnRetainedPayloadEncodingJSON &&
		!collectionMetaHasSecondaryUniqueIndex(meta)
}

// A metadata-only existence check does not reconstruct a document or allocate
// caller output. The enclosing original read/transaction keeps its basis live.
func nativeTypedUpsertIDsAbsent(read *NativeJSONRead, ids [][]byte) (bool, error) {
	if read.domain.domain == nil {
		return true, nil
	}
	for _, id := range ids {
		if row, _, found := read.domain.domain.typed.find(id); found {
			if row != nil {
				return false, nil
			}
			continue
		}
		entry, _, err := read.lookupEntry(id, false)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if entry.Flags&node.FlagTombstone == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (c *Collection) nativeTryUpsertTypedBatchGroup(ids, retained [][]byte, columns []TypedColumnBatch) (updated int, handled bool, stats CollectionInsertStats, resultErr error) {
	if c.manager.isClosing() {
		return 0, true, stats, backenddb.ErrClosed
	}
	if len(ids) == 0 {
		return 0, false, stats, nil
	}
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return 0, true, stats, err
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return 0, true, stats, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	if !nativeTypedUpsertEligible(read.database, read.meta) {
		return 0, false, stats, nil
	}
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return 0, true, stats, err
	}
	maxDocs, maxBytes := typedUpsertGroupLimits(read.meta)
	inputBytes := typedUpsertGroupInputBytes(ids, retained, columns)
	if inputBytes < 0 || len(ids) > maxDocs || inputBytes > maxBytes {
		return 0, false, stats, nil
	}
	// Reuse the maintained parser/projection bound while owning the queue's
	// cloned IDs, residuals and declared vectors. Queue controls coexist with
	// those inputs and are admitted before any allocation below.
	owned, work, err := nativeTypedBatchStageCapacity(read.meta, ids, retained, columns, ids)
	if err != nil {
		return 0, true, stats, err
	}
	controls := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTypedUpsertRequest{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(typedUpsertGroupRequest{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(typedUpsertGroup{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(typedUpsertGroupResult{}))+128) +
		memtable.COWAllocationCharge(uint64(len(ids))*preparedSemanticStreamMapEntryReserveBytes) +
		memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof((*typedUpsertGroupRequest)(nil))))
	// The borrowed staging carriers have independent slice backing while the
	// owned projection remains live; include both, rather than counting only
	// the underlying strings/vectors.
	carrierBytes := uint64(len(read.meta.Options.ColumnStore.Columns)) * uint64(unsafe.Sizeof(TypedColumnBatch{}))
	if err = nativeTypedCheckedAdd(&controls, memtable.COWAllocationCharge(carrierBytes)); err != nil {
		return 0, true, stats, err
	}
	for _, col := range read.meta.Options.ColumnStore.Columns {
		width := uint64(unsafe.Sizeof([]float32{}))
		if col.ValueType == ColumnStoreValueString {
			width = uint64(unsafe.Sizeof(""))
		}
		if err = nativeTypedCheckedAdd(&controls, memtable.COWAllocationCharge(uint64(len(ids))*width)); err != nil {
			return 0, true, stats, err
		}
	}
	if err = nativeTypedCheckedAdd(&owned, work); err != nil {
		return 0, true, stats, err
	}
	if err = nativeTypedCheckedAdd(&owned, controls); err != nil {
		return 0, true, stats, err
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(owned)
	if err != nil {
		return 0, true, stats, err
	}
	request := &typedUpsertGroupRequest{collection: c, ownedBytes: inputBytes, done: make(chan typedUpsertGroupResult, 1)}
	request.native = &nativeTypedUpsertRequest{read: read}
	defer func() {
		request.ids, request.retained, request.projection, request.native, request.done = nil, nil, nil, nil, nil
		request = nil
		lease.Close()
	}()
	request.ids, request.retained = cloneTypedUpsertGroupInput(ids, retained)
	request.projection, err = newTrustedTypedProjection(read.meta, request.ids, request.retained, columns)
	if errors.Is(err, ErrDuplicateDocumentID) {
		return 0, false, stats, nil
	}
	if err != nil {
		return 0, true, stats, err
	}
	absent, err := nativeTypedUpsertIDsAbsent(read, request.ids)
	if err != nil || !absent {
		return 0, err != nil, stats, err
	}
	// These are borrowed carriers for the already-owned projection, not a new
	// projection/codec. Execution uses the original typed-batch staging path.
	request.native.columns = make([]TypedColumnBatch, len(request.projection.columns))
	for i, col := range request.projection.columns {
		input := &request.native.columns[i]
		input.Name = col.Name
		if col.ValueType == ColumnStoreValueString {
			input.Strings = make([]string, len(ids))
		} else {
			input.Float32Vectors = make([][]float32, len(ids))
		}
		for row, id := range request.ids {
			value := request.projection.typedRows[string(id)][i]
			if col.ValueType == ColumnStoreValueString {
				input.Strings[row] = value.String
			} else {
				input.Float32Vectors[row] = value.Float32Vector
			}
		}
	}
	coord := collectionCommandWALCoordinatorForDB(read.database)
	if coord == nil {
		return 0, true, stats, backenddb.ErrClosed
	}
	return queueTypedUpsertGroup(coord, request, nil, maxDocs, maxBytes)
}

func (group *typedUpsertGroup) executeNative(requests []*typedUpsertGroupRequest) (results []typedUpsertGroupResult) {
	c := requests[0].collection
	if c.manager.isClosing() {
		return typedUpsertGroupErrorResults(requests, backenddb.ErrClosed)
	}
	tx, err := c.nativeJSON.beginPrepared(c.name)
	if err != nil {
		return typedUpsertGroupErrorResults(requests, err)
	}
	defer func() {
		if err := tx.Close(); err != nil {
			for i := range results {
				results[i].handled = true
				results[i].err = errors.Join(results[i].err, err)
			}
		}
	}()
	g := &tx.bindings[0]
	if !nativeTypedUpsertEligible(tx.db, g.probe.meta) {
		return make([]typedUpsertGroupResult, len(requests))
	}
	maxDocs, maxBytes := typedUpsertGroupLimits(g.probe.meta)
	if group.documents > maxDocs || group.bytes > maxBytes {
		return make([]typedUpsertGroupResult, len(requests))
	}
	for _, request := range requests {
		if request.native == nil || g.probe.collection.schema != request.native.read.collection.schema ||
			g.incarnation != request.native.read.collection.incarnation {
			return make([]typedUpsertGroupResult, len(requests))
		}
		if err := request.collection.requireTypedBatchVectorAdmission(); err != nil {
			return typedUpsertGroupErrorResults(requests, err)
		}
		absent, err := nativeTypedUpsertIDsAbsent(&g.probe, request.ids)
		if err != nil {
			return typedUpsertGroupErrorResults(requests, err)
		}
		if !absent { // late decline has never assigned a participant identity
			return make([]typedUpsertGroupResult, len(requests))
		}
	}
	for _, request := range requests {
		_, matched, err := tx.stageNativeTypedBatch(g, request.ids, request.retained, request.native.columns, request.ids, commitlog.CommandKindCollectionReplaceSourceByID, true)
		if err != nil {
			return typedUpsertGroupErrorResults(requests, err)
		}
		if matched != 0 {
			return make([]typedUpsertGroupResult, len(requests))
		}
	}
	// Preserve the original group's byte limit over both caller images and
	// actual encoded intents. Each singleton envelope is planned by the same
	// canonical codec that the append path uses; no identity is assigned here.
	bytes := group.bytes
	for i := range tx.members {
		n, err := commitlog.PlanCompositeMutationPayload(tx.members[i : i+1])
		if err != nil {
			return typedUpsertGroupErrorResults(requests, err)
		}
		if int64(n) > math.MaxInt64-bytes || int64(n) > maxBytes-bytes {
			return make([]typedUpsertGroupResult, len(requests))
		}
		bytes += int64(n)
	}
	// The original conditional publisher validates the opening's point/catalog
	// dependencies, appends the actual participant prefix, then installs once.
	// An assigned error is returned to every caller without retry or decline.
	if err := tx.commitNativePlan(false, nil, true); err != nil {
		return typedUpsertGroupErrorResults(requests, err)
	}
	if hook := typedUpsertGroupAfterInstallTestHook.Load(); hook != nil {
		(*hook)()
	}
	results = make([]typedUpsertGroupResult, len(requests))
	for i, request := range requests {
		results[i] = typedUpsertGroupResult{handled: true, stats: CollectionInsertStats{Documents: len(request.ids)}}
	}
	return results
}

// Each original staged member receives its own canonical singleton frame.
// Ordinals refer to that frame, never the barrier or a synthetic composite of
// all participants. The returned intents are synchronous finalizer borrows.
func (t *NativeJSONTxn) nativeTypedUpsertIntents(members []commitlog.CompositeMutationMember, admit func(uint64) error) (*backenddb.CommandWALIntent, []*backenddb.CommandWALIntent, error) {
	if t.rawStaged || t.count != 1 || len(members) == 0 || t.bindings[0].typed == nil {
		return nil, nil, backenddb.ErrNativeSemanticUnsupported
	}
	delta := t.bindings[0].typed
	for i := len(members) - 1; i >= 0; i-- {
		member := members[i]
		if delta == nil || delta.memberOrdinal != uint32(i) || member.Incarnation != t.bindings[0].incarnation ||
			member.Kind != commitlog.CommandKindCollectionReplaceSourceByID || member.PayloadFormat != commitlog.PayloadFormatCollectionTypedSourceByIDV1 {
			return nil, nil, backenddb.ErrNativeSemanticUnsupported
		}
		delta = delta.previous
	}
	if delta != nil {
		return nil, nil, backenddb.ErrNativeSemanticUnsupported
	}
	// Intent headers, the participant slice and constructor's owned slice/map
	// coexist. All are charged before either constructor allocates them.
	for _, bytes := range []uint64{
		uint64(len(members)) * uint64(unsafe.Sizeof(backenddb.CommandWALIntent{})),
		2 * uint64(len(members)) * uint64(unsafe.Sizeof((*backenddb.CommandWALIntent)(nil))),
		uint64(len(members)) * preparedSemanticStreamMapEntryReserveBytes,
	} {
		if err := admit(bytes); err != nil {
			return nil, nil, err
		}
	}
	participants := make([]*backenddb.CommandWALIntent, len(members))
	for i := range members {
		planned, err := commitlog.PlanCompositeMutationPayload(members[i : i+1])
		if err != nil {
			return nil, nil, err
		}
		if err = admit(uint64(planned)); err != nil {
			return nil, nil, err
		}
		payload, err := commitlog.EncodeCompositeMutationPayloadTo(make([]byte, 0, planned), members[i:i+1])
		if err != nil {
			return nil, nil, err
		}
		participants[i], err = t.db.NewTrustedCommandWALIntent(commitlog.CommandKindCompositeMutation, commitlog.CommandScopeSystem, commitlog.PayloadFormatCompositeMutationV1, payload)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(participants) == 1 {
		return participants[0], participants, nil
	}
	intent, err := t.db.NewCommandWALDurablePrefixGroupIntent(participants)
	return intent, participants, err
}
