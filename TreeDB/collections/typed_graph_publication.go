package collections

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// Derived publication limits are shared by explicit public serving admission
// and internal lifecycle tests. They are not a lifetime physical disk quota.
type typedGraphPublicationLimits struct {
	Rows, Tombstones, ValueSlots int
	OwnedBytes                   int64
	// EncodedOutputBytes bounds attempted encoded work in a maintenance epoch,
	// not disk/COW or heap. Only explicit successful lifecycle renewal refills it.
	// Zero leaves this additional internal admission term disabled.
	EncodedOutputBytes int64
}

// Scalar receipts follow the existing buffered document ownership. The debt
// lock protects accounting only; it never encloses backend or snapshot calls.
type typedGraphPublicationCost struct {
	rows, tombstones, slots int
	bytes                   int64
}

func (a *typedGraphPublicationCost) add(b typedGraphPublicationCost) {
	a.rows += b.rows
	a.tombstones += b.tombstones
	a.slots += b.slots
	a.bytes += b.bytes
}

func (a *typedGraphPublicationCost) subtract(b typedGraphPublicationCost) {
	a.rows -= b.rows
	a.tombstones -= b.tombstones
	a.slots -= b.slots
	a.bytes -= b.bytes
}

type typedGraphPublicationReceipt struct {
	coord                            *collectionSchemaCoordinator
	cost                             typedGraphPublicationCost
	documents                        []columnWriteDocument // borrowed immutable admitted headers, no payload copy
	consumed                         bool                  // protected by coord.typedPublicationDebtMu
	encoded                          typedGraphEncodedCost
	primaryAttempted, flushAttempted bool // same debt lock; repeated attempts charge again
	immediate                        *typedGraphImmediateReceiptInput
}

func (c *Collection) reserveTypedGraphPublication(cost typedGraphPublicationCost, encoded ...typedGraphEncodedCost) (*typedGraphPublicationReceipt, error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil || coord.typedPublication.Load() == nil {
		if coord != nil && coord.typedGraphServing.Load() != nil {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		return nil, nil
	}
	coord.typedPublicationDebtMu.Lock()
	defer coord.typedPublicationDebtMu.Unlock()
	state := coord.typedPublication.Load()
	if state == nil || state.invalid {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if coord.typedGraphServing.Load() != nil && !state.servingAdmitted {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	l, d := state.limits, coord.typedPublicationDebt
	if cost.rows < 0 || cost.tombstones < 0 || cost.slots < 0 || cost.bytes < 0 || cost.rows > l.Rows-d.rows || cost.tombstones > l.Tombstones-d.tombstones || cost.slots > l.ValueSlots-d.slots || cost.bytes > l.OwnedBytes-d.bytes {
		return nil, errTypedGraphOverlayFoldNeeded
	}
	var physical typedGraphEncodedCost
	if len(encoded) > 1 {
		return nil, ErrHybridSearchUnsupported
	}
	if len(encoded) == 1 {
		physical = encoded[0]
	}
	physicalBytes, err := physical.total()
	if err != nil {
		return nil, err
	}
	if l.EncodedOutputBytes > 0 && cost.rows > 0 && (len(encoded) != 1 || physicalBytes == 0) {
		return nil, ErrHybridSearchUnsupported
	}
	if physicalBytes != 0 && (l.EncodedOutputBytes <= 0 || physicalBytes > l.EncodedOutputBytes-coord.typedPublicationEncodedBytes) {
		return nil, errTypedGraphOverlayFoldNeeded
	}
	coord.typedPublicationDebt.add(cost)
	coord.typedPublicationPending.add(cost)
	coord.typedPublicationEncodedBytes += physicalBytes
	return &typedGraphPublicationReceipt{coord: coord, cost: cost, encoded: physical}, nil
}

func (r *typedGraphPublicationReceipt) rejectBeforeAppend() {
	if r == nil {
		return
	}
	r.coord.typedPublicationDebtMu.Lock()
	defer r.coord.typedPublicationDebtMu.Unlock()
	if !r.consumed {
		r.coord.typedPublicationDebt.subtract(r.cost)
		if !r.primaryAttempted {
			r.coord.typedPublicationEncodedBytes -= r.encoded.primary
		}
		if !r.flushAttempted {
			r.coord.typedPublicationEncodedBytes -= r.encoded.flush
		}
		r.consumePendingLocked()
	}
}

func (coord *collectionSchemaCoordinator) wakeTypedGraphPublicationWaitersLocked() {
	if coord.typedPublicationChanged != nil {
		close(coord.typedPublicationChanged)
		coord.typedPublicationChanged = nil
	}
}

func (r *typedGraphPublicationReceipt) consumePendingLocked() {
	r.coord.typedPublicationPending.subtract(r.cost)
	if len(r.documents) != 0 {
		r.coord.typedPublicationBuffered--
	}
	r.consumed = true
	r.coord.wakeTypedGraphPublicationWaitersLocked()
}

func (r *typedGraphPublicationReceipt) invalidate() {
	if r == nil {
		return
	}
	r.coord.typedPublicationDebtMu.Lock()
	defer r.coord.typedPublicationDebtMu.Unlock()
	defer r.coord.wakeTypedGraphPublicationWaitersLocked()
	if r.consumed {
		return
	}
	for {
		state := r.coord.typedPublication.Load()
		if state == nil || state.invalid {
			return
		}
		invalid := *state
		invalid.invalid = true
		if r.coord.typedPublication.CompareAndSwap(state, &invalid) {
			return
		}
	}
}

func (c *Collection) reserveBufferedTypedGraphPublication(documents []columnWriteDocument, encoded ...typedGraphEncodedCost) (*typedGraphPublicationReceipt, error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil || coord.typedPublication.Load() == nil {
		if coord != nil && coord.typedGraphServing.Load() != nil {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		return nil, nil
	}
	state := coord.typedPublication.Load()
	cost, err := typedGraphPublicationInputCost(columnWritePublishInput{documents: documents, operation: ColumnPublishOperationInsert}, state.limits)
	if err != nil {
		return nil, err
	}
	receipt, err := c.reserveTypedGraphPublication(cost, encoded...)
	if err == nil && receipt != nil {
		coord.typedPublicationDebtMu.Lock()
		receipt.documents = documents
		if len(documents) != 0 {
			coord.typedPublicationBuffered++
		}
		coord.typedPublicationDebtMu.Unlock()
	}
	return receipt, err
}

func typedGraphPublicationInputCost(input columnWritePublishInput, limits typedGraphPublicationLimits) (typedGraphPublicationCost, error) {
	var cost typedGraphPublicationCost
	if len(input.documents) > limits.Rows || len(input.sourceDeleteDocuments) > limits.Rows-len(input.documents) {
		return cost, errTypedGraphOverlayFoldNeeded
	}
	cost.rows = len(input.documents) + len(input.sourceDeleteDocuments)
	cost.tombstones = len(input.sourceDeleteDocuments)
	if input.operation == ColumnPublishOperationDelete {
		cost.tombstones += len(input.documents)
	}
	if cost.tombstones > limits.Tombstones {
		return cost, errTypedGraphOverlayFoldNeeded
	}
	charge := func(n int64) bool {
		if n < 0 || n > limits.OwnedBytes-cost.bytes {
			return false
		}
		cost.bytes += n
		return true
	}
	for _, doc := range input.sourceDeleteDocuments {
		if !charge(int64(len(doc.ID))) {
			return cost, errTypedGraphOverlayFoldNeeded
		}
	}
	for i, doc := range input.documents {
		if !charge(int64(len(doc.ID))) {
			return cost, errTypedGraphOverlayFoldNeeded
		}
		if input.operation == ColumnPublishOperationDelete {
			continue
		}
		values := doc.declaredValues
		if input.declaredRowsReady {
			if len(input.declaredRows) != len(input.documents) {
				return cost, ErrVectorIndexSnapshotMismatch
			}
			values = input.declaredRows[i].Values
		} else if !doc.declaredValuesReady {
			return cost, ErrVectorIndexSnapshotMismatch
		}
		if len(values) > limits.ValueSlots-cost.slots {
			return cost, errTypedGraphOverlayFoldNeeded
		}
		cost.slots += len(values)
		for _, value := range values {
			if value.Type != ColumnStoreValueString && value.Type != ColumnStoreValueFloat32Vector {
				return cost, ErrHybridSearchUnsupported
			}
			if !charge(int64(len(value.String)) + int64(len(value.StringBytes)) + int64(len(value.Float32Vector))*4) {
				return cost, errTypedGraphOverlayFoldNeeded
			}
		}
	}
	return cost, nil
}

type typedGraphPublicationState struct {
	servingBase              *typedGraphServingBaseMetadata
	servingMaterializer      columnPhysicalScanSnapshotView
	servingRefs              []ColumnAssetRef
	servingMetadataBytes     int64
	servingAdmitted          bool
	catalog                  *collectionCatalog
	limits                   typedGraphPublicationLimits
	rows                     []columnPhysicalVisibleRow
	invNorms                 []float32
	physicalRows, tombstones int
	valueSlots               int
	admittedPayloadBytes     int64
	installedAssetBytes      int64
	controlEncodedBytes      int64    // schema-time bound; no metadata encoding on writes
	manifestEncodedBytes     [3]int64 // insert/update/delete including header and identity
	deletePartEncodedBytes   int64    // additional source-removal part
	invalid                  bool
	reconciling              *typedGraphReconcileToken
}

func (c *Collection) typedGraphPublicationSnapshot() *typedGraphPublicationState {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil
	}
	return coord.typedPublication.Load()
}

func (s *typedGraphPublicationState) matches(catalog *collectionCatalog) bool {
	return s != nil && !s.invalid && catalog != nil && s.catalog.pager == catalog.pager && collectionMetaValuesEqual(s.catalog.meta, catalog.meta) && maps.Equal(s.catalog.roots, catalog.roots) && len(catalog.rootOverlays) == 0
}

type typedGraphPublicationCandidate struct {
	coord       *collectionSchemaCoordinator
	before      *typedGraphPublicationState
	next        *typedGraphPublicationState
	receipts    []*typedGraphPublicationReceipt
	ownReceipt  bool
	reconciling bool
}

// The existing typed projection and generic extractor produce owning values;
// physical publication only reads them. Share those immutable value headers and
// payloads. Copy ID bytes and outer row headers only. A borrowed StringBytes
// carrier requires a separate header slice before normalization; owning rows do
// not. No retained JSON enters derived state.
func (c *Collection) prepareTypedGraphPublication(input columnWritePublishInput) (*typedGraphPublicationCandidate, error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, nil
	}
	before := coord.typedPublication.Load()
	if before == nil {
		if coord.typedGraphServing.Load() != nil {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		return nil, nil
	}
	if before.invalid {
		if c.typedGraphReconcile != nil && before.reconciling == c.typedGraphReconcile {
			cost, err := typedGraphPublicationInputCost(input, before.limits)
			if err != nil {
				return nil, err
			}
			if err := validateTypedGraphReceiptInput(coord, input, cost); err != nil {
				return nil, err
			}
			return &typedGraphPublicationCandidate{coord: coord, before: before, receipts: input.typedReceipts, reconciling: true}, nil
		}
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if !before.matches(input.catalog) {
		return nil, fmt.Errorf("%w: typed graph publication frontier mismatch", ErrConcurrentMutation)
	}
	remaining := typedGraphPublicationLimits{Rows: before.limits.Rows - before.physicalRows, Tombstones: before.limits.Tombstones - before.tombstones, ValueSlots: before.limits.ValueSlots - before.valueSlots, OwnedBytes: before.limits.OwnedBytes - before.admittedPayloadBytes}
	cost, err := typedGraphPublicationInputCost(input, remaining)
	if err != nil {
		return nil, err
	}
	count, tombstones, slots, payload := cost.rows, cost.tombstones, cost.slots, cost.bytes
	next := *before
	next.physicalRows += count
	next.tombstones += tombstones
	next.admittedPayloadBytes += payload
	next.valueSlots += slots
	changed := make([]columnPhysicalVisibleRow, 0, count)
	for i, d := range input.sourceDeleteDocuments {
		changed = append(changed, columnPhysicalVisibleRow{ID: bytes.Clone(d.ID), Deleted: true, Operation: ColumnPublishOperationDelete, RowIndex: i, PartID: columnPhysicalRowAssetPartID})
	}
	for i, d := range input.documents {
		partID := columnPhysicalRowAssetPartID + input.partIDOffset
		if len(input.sourceDeleteDocuments) != 0 {
			partID = columnPhysicalRowAssetPartID + 1<<32
		}
		row := columnPhysicalVisibleRow{ID: bytes.Clone(d.ID), Deleted: input.operation == ColumnPublishOperationDelete, Operation: input.operation, RowIndex: i, PartID: partID}
		if len(input.sourceDeleteDocuments) != 0 {
			row.Operation = ColumnPublishOperationInsert
		}
		if !row.Deleted {
			row.Values = input.declaredRows[i].Values
			for _, value := range row.Values {
				if value.StringBytes != nil {
					row.Values = slices.Clone(row.Values)
					break
				}
			}
			for j := range row.Values {
				if row.Values[j].StringBytes != nil {
					row.Values[j].String = string(row.Values[j].StringBytes)
					row.Values[j].StringBytes = nil
				}
			}
		}
		changed = append(changed, row)
	}
	// Stable sort makes a source replacement's inserted row win its same-ID
	// tombstone. All physical versions were charged before this coalescing.
	slices.SortStableFunc(changed, func(a, b columnPhysicalVisibleRow) int { return bytes.Compare(a.ID, b.ID) })
	unique := changed[:0]
	for _, row := range changed {
		if len(unique) > 0 && bytes.Equal(unique[len(unique)-1].ID, row.ID) {
			unique[len(unique)-1] = row
		} else {
			unique = append(unique, row)
		}
	}
	next.rows = make([]columnPhysicalVisibleRow, 0, len(before.rows)+len(unique))
	next.invNorms = make([]float32, 0, cap(next.rows))
	vectorColumn := -1
	if len(input.meta.VectorIndexes) != 1 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	for i, column := range input.meta.Options.ColumnStore.Columns {
		if column.Path == input.meta.VectorIndexes[0].Field {
			vectorColumn = i
			break
		}
	}
	if vectorColumn < 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	old := 0
	for _, row := range unique {
		for old < len(before.rows) && bytes.Compare(before.rows[old].ID, row.ID) < 0 {
			next.rows = append(next.rows, before.rows[old])
			next.invNorms = append(next.invNorms, before.invNorms[old])
			old++
		}
		if old < len(before.rows) && bytes.Equal(before.rows[old].ID, row.ID) {
			old++
		}
		var norm float32
		if !row.Deleted {
			if vectorColumn >= len(row.Values) {
				return nil, ErrVectorIndexSnapshotMismatch
			}
			var err error
			norm, err = columnVectorGraphInvNorm(row.Values[vectorColumn].Float32Vector)
			if err != nil {
				return nil, err
			}
		}
		next.rows = append(next.rows, row)
		next.invNorms = append(next.invNorms, norm)
	}
	next.rows = append(next.rows, before.rows[old:]...)
	next.invNorms = append(next.invNorms, before.invNorms[old:]...)
	receipts := input.typedReceipts
	ownReceipt := len(receipts) == 0
	if ownReceipt {
		receipt, err := c.reserveTypedGraphPublication(cost)
		if err != nil {
			return nil, err
		}
		receipts = []*typedGraphPublicationReceipt{receipt}
	} else {
		if err := validateTypedGraphReceiptInput(coord, input, cost); err != nil {
			return nil, err
		}
	}
	return &typedGraphPublicationCandidate{coord: coord, before: before, next: &next, receipts: receipts, ownReceipt: ownReceipt}, nil
}

func (p *typedGraphPublicationCandidate) preflight() error {
	if p != nil && p.coord.typedPublication.Load() != p.before {
		return fmt.Errorf("%w: typed graph publication predecessor changed", ErrConcurrentMutation)
	}
	return nil
}

// Buffered units preserve admitted document order and shallow header identity.
// Check every slot, not only summed costs, before consuming receipt authority.
func validateTypedGraphReceiptInput(coord *collectionSchemaCoordinator, input columnWritePublishInput, cost typedGraphPublicationCost) error {
	if len(input.typedReceipts) == 1 && input.typedReceipts[0] != nil && input.typedReceipts[0].immediate != nil {
		return validateTypedGraphImmediateReceiptInput(coord, input, cost)
	}
	if len(input.typedReceipts) == 0 || len(input.sourceDeleteDocuments) != 0 || input.operation != ColumnPublishOperationInsert || (input.declaredRowsReady && len(input.declaredRows) != len(input.documents)) {
		return ErrVectorIndexSnapshotMismatch
	}
	coord.typedPublicationDebtMu.Lock()
	defer coord.typedPublicationDebtMu.Unlock()
	var seen map[*typedGraphPublicationReceipt]struct{}
	if len(input.typedReceipts) > 1 {
		seen = make(map[*typedGraphPublicationReceipt]struct{}, len(input.typedReceipts))
	}
	position := 0
	var reserved typedGraphPublicationCost
	for _, receipt := range input.typedReceipts {
		if receipt == nil || receipt.coord != coord || receipt.consumed || len(receipt.documents) > len(input.documents)-position {
			return ErrVectorIndexSnapshotMismatch
		}
		if seen != nil {
			if _, duplicate := seen[receipt]; duplicate {
				return ErrVectorIndexSnapshotMismatch
			}
			seen[receipt] = struct{}{}
		}
		for i, admitted := range receipt.documents {
			d := input.documents[position+i]
			if !bytes.Equal(d.ID, admitted.ID) || len(d.declaredValues) != len(admitted.declaredValues) || len(d.declaredValues) == 0 || &d.declaredValues[0] != &admitted.declaredValues[0] {
				return ErrVectorIndexSnapshotMismatch
			}
			if input.declaredRowsReady {
				values := input.declaredRows[position+i].Values
				if len(values) != len(admitted.declaredValues) || &values[0] != &admitted.declaredValues[0] {
					return ErrVectorIndexSnapshotMismatch
				}
			}
		}
		position += len(receipt.documents)
		reserved.add(receipt.cost)
	}
	state := coord.typedPublication.Load()
	if state == nil || position != len(input.documents) {
		return ErrVectorIndexSnapshotMismatch
	}
	if cost != reserved {
		return ErrVectorIndexSnapshotMismatch
	}
	return nil
}

func (p *typedGraphPublicationCandidate) invalidate() {
	if p == nil {
		return
	}
	// Assigned live LSN can mean append/reservation ambiguity, not proof that
	// roots installed. Replay LSN is already assigned before this attempt.
	// In either uncertain case retain the charge and fail closed.
	p.ownReceipt = false
	invalid := *p.before
	invalid.invalid = true
	p.coord.typedPublication.CompareAndSwap(p.before, &invalid)
	p.coord.typedPublicationDebtMu.Lock()
	p.coord.wakeTypedGraphPublicationWaitersLocked()
	p.coord.typedPublicationDebtMu.Unlock()
}

func (p *typedGraphPublicationCandidate) rejectBeforeAppend() {
	if p != nil && p.ownReceipt {
		for _, r := range p.receipts {
			r.rejectBeforeAppend()
		}
	}
}

func (p *typedGraphPublicationCandidate) install(meta CollectionMeta, rootNames []string, rootIDs []uint64, plan ColumnPublishPlan) {
	if p == nil {
		return
	}
	p.ownReceipt = false
	if p.reconciling {
		p.coord.typedPublicationDebtMu.Lock()
		defer p.coord.typedPublicationDebtMu.Unlock()
		if p.coord.typedPublication.Load() != p.before {
			return
		}
		for _, receipt := range p.receipts {
			if !receipt.consumed {
				receipt.consumePendingLocked()
			}
		}
		return
	}
	p.next.catalog = cloneCatalogWithRootUpdates(p.before.catalog, meta, rootNames, rootIDs)
	for _, asset := range plan.PreparedAssets {
		p.next.installedAssetBytes = saturatingAddNonNegativeInt64(p.next.installedAssetBytes, asset.Bytes)
	}
	for i := range p.next.rows {
		// New rows are the only headers with an unassigned publication frontier.
		if p.next.rows[i].AppliedCommandLSN == 0 {
			p.next.rows[i].AppliedCommandLSN = plan.AppliedCommandLSN
			p.next.rows[i].Generation = plan.UpdatedActiveManifest.Generation
		}
	}
	typedGraphPublicationAfterAcceptedHook.RLock()
	fn := typedGraphPublicationAfterAcceptedHook.fn
	typedGraphPublicationAfterAcceptedHook.RUnlock()
	if fn != nil {
		fn(p)
	}
	p.coord.typedPublicationDebtMu.Lock()
	defer p.coord.typedPublicationDebtMu.Unlock()
	defer p.coord.wakeTypedGraphPublicationWaitersLocked()
	if p.coord.typedPublication.CompareAndSwap(p.before, p.next) {
		for _, r := range p.receipts {
			if !r.consumed {
				r.consumePendingLocked()
			}
		}
	}
}

var typedGraphPublicationAfterAcceptedHook struct {
	sync.RWMutex
	fn                     func(*typedGraphPublicationCandidate)
	foldAfterInstall       func(*Collection)
	foldAfterCapture       func(*Collection)
	foldBeforeStateInstall func(*Collection)
	reconcileBeforeCapture func(*Collection)
}

// Test-only replay instrumentation; unset in production. Bootstrap remains a
// separate lifecycle gate, not a side effect of observing a replay frame.
var typedGraphPublicationReplayOpenHook struct {
	sync.RWMutex
	fn func(*Collection) error
}
