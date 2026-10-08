package caching

import (
	"errors"
	"math"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

var errPreparedFiniteLeafWorkspace = errors.New("cachingdb: finite leaf workspace refused")

// PreparedFiniteLeafWorkspace owns the existing serial cached lane's append
// scratch. It does not certify unimplemented selector/rotation/stable-resource
// backing or enable any public native publication admission.
type PreparedFiniteLeafWorkspace struct {
	installed                          backenddb.LeafPageLog
	db                                 *DB
	lane                               *lane
	reserve                            func(uint64) error
	writerBacking                      *valuelog.FiniteWriterBacking
	preparer                           *valuelog.FiniteFramePreparer
	stableMetadata                     *valuelog.FiniteStableMetadata
	resident                           rootpublication.StableMetadataAccount
	stableCollector                    *rootpublication.StableSegmentFrontierCollector
	suppliedMetadata                   bool
	stableCollectorMaxRegistered       uint64
	compact                            [][]byte
	records                            []valuelog.Record
	ptrs                               []page.ValuePtr
	maxOutput, maxBatch, used, backing uint64
	active, closed, ridsAssigned       bool
	scopedProducer, producerBound      bool
}

// NewPreparedFiniteLeafWorkspace constructs identity-free scratch credit for the
// exact installed cached leaf log. The concrete writer/segment is acquired only
// inside the existing lane.vlogMu append critical section.
func NewPreparedFiniteLeafWorkspace(installed backenddb.LeafPageLog, maxOutput, maxBatch uint64, reserve func(uint64) error) (*PreparedFiniteLeafWorkspace, error) {
	if installed == nil || reserve == nil || maxOutput == 0 || maxBatch == 0 || maxBatch > maxOutput || maxOutput > math.MaxUint64/7 || maxBatch > uint64(math.MaxInt)/uint64(unsafe.Sizeof(valuelog.Record{})) {
		return nil, errPreparedFiniteLeafWorkspace
	}
	var db *DB
	var l *lane
	switch log := installed.(type) {
	case *cachingLeafPageLog:
		if log == nil {
			return nil, errPreparedFiniteLeafWorkspace
		}
		db, l = log.db, log.lane
	case *cachingLeafPageLogGroup:
		if log == nil {
			return nil, errPreparedFiniteLeafWorkspace
		}
		db = log.db
		var ok bool
		l, ok = log.laneForWorkerIndex(0)
		if !ok {
			return nil, errPreparedFiniteLeafWorkspace
		}
	default:
		return nil, errPreparedFiniteLeafWorkspace
	}
	if db == nil || l == nil || !db.isLeafLogAppendLane(l) {
		return nil, errPreparedFiniteLeafWorkspace
	}
	n, classErr := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(PreparedFiniteLeafWorkspace{})), true)
	if classErr != nil {
		return nil, classErr
	}
	if err := reserve(n); err != nil {
		return nil, err
	}
	result := &PreparedFiniteLeafWorkspace{installed: installed, db: db, lane: l, reserve: reserve, maxOutput: maxOutput, maxBatch: maxBatch, backing: n}
	if err := result.chargeAllocation(16, true); err != nil {
		return nil, err
	}
	owner, err := valuelog.NewFiniteWriterBacking(maxOutput+1, result.charge)
	if err != nil {
		return nil, err
	}
	result.writerBacking = owner
	return result, nil
}

// NewScopedPreparedFiniteLeafWorkspace uses the same append scratch engine,
// but keeps the installed producer only on a synchronous Apply call stack.
// The caller must supply its actual installed leaf log while holding the real
// publisher/teardown basis; this scratch object provides no DB admission itself.
func NewScopedPreparedFiniteLeafWorkspace(installed backenddb.LeafPageLog, maxOutput, maxBatch uint64, reserve func(uint64) error, metadata *valuelog.FiniteStableMetadata, resident rootpublication.StableMetadataAccount) (*PreparedFiniteLeafWorkspace, error) {
	w, err := NewPreparedFiniteLeafWorkspaceWithStableMetadata(installed, maxOutput, maxBatch, reserve, metadata, resident)
	if err != nil {
		return nil, err
	}
	w.scopedProducer = true
	w.installed, w.db, w.lane = nil, nil, nil
	return w, nil
}

// BeginOwnedApply binds only the concrete installed serial lane. The trusted
// caller defers EndOwnedApply before returning any owned root/publication result.
func (w *PreparedFiniteLeafWorkspace) BeginOwnedApply(installed backenddb.LeafPageLog) error {
	if w == nil || w.closed || !w.scopedProducer || w.producerBound || w.active || installed == nil {
		return errPreparedFiniteLeafWorkspace
	}
	var db *DB
	var l *lane
	switch log := installed.(type) {
	case *cachingLeafPageLog:
		if log == nil {
			return errPreparedFiniteLeafWorkspace
		}
		db, l = log.db, log.lane
	case *cachingLeafPageLogGroup:
		if log == nil {
			return errPreparedFiniteLeafWorkspace
		}
		db = log.db
		var ok bool
		l, ok = log.laneForWorkerIndex(0)
		if !ok {
			return errPreparedFiniteLeafWorkspace
		}
	default:
		return errPreparedFiniteLeafWorkspace
	}
	if db == nil || l == nil || !db.isLeafLogAppendLane(l) {
		return errPreparedFiniteLeafWorkspace
	}
	w.installed, w.db, w.lane = installed, db, l
	if err := w.validate(db, l); err != nil {
		w.installed, w.db, w.lane = nil, nil, nil
		return err
	}
	w.producerBound = true
	return nil
}
func (w *PreparedFiniteLeafWorkspace) EndOwnedApply() {
	if w == nil || !w.scopedProducer {
		return
	}
	// Append methods discharge their own lane critical section before returning.
	// Clear even an interrupted preparation's borrowed record aliases.
	w.releasePreparedPages()
	w.installed, w.db, w.lane = nil, nil, nil
	w.producerBound = false
}

func (w *PreparedFiniteLeafWorkspace) chargeAllocation(n uint64, scan bool) error {
	class, err := rootpublication.StableBackingClassBytes(n, scan)
	if err != nil {
		return err
	}
	return w.charge(class)
}
func (w *PreparedFiniteLeafWorkspace) charge(n uint64) error {
	if w == nil || w.closed || n > math.MaxUint64-w.backing {
		return errPreparedFiniteLeafWorkspace
	}
	if n == 0 {
		return nil
	}
	if err := w.reserve(n); err != nil {
		return err
	}
	w.backing += n
	return nil
}

// This is the one global metadata budget for the canonical request, not a
// per-capture allowance. Derived conservative counts are R<=3Q, T<=7Q; no
// backing is preallocated for those counts.
func (w *PreparedFiniteLeafWorkspace) stableMetadataOwner() (*valuelog.FiniteStableMetadata, error) {
	if w == nil || w.closed || w.maxOutput == 0 || w.maxOutput > math.MaxUint64/7 {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if w.stableMetadata == nil {
		if err := w.chargeAllocation(16, true); err != nil {
			return nil, err
		}
		owner, err := valuelog.NewFiniteStableMetadata(7*w.maxOutput, 3*w.maxOutput, w.charge)
		if err != nil {
			return nil, err
		}
		w.stableMetadata = owner
	}
	return w.stableMetadata, nil
}

func (w *PreparedFiniteLeafWorkspace) requireStableHooks(count int) error {
	if w == nil || w.closed {
		return errPreparedFiniteLeafWorkspace
	}
	if err := w.validateSelectorInputs(w.db, w.lane, 0, nil, count); err != nil {
		return err
	}
	metadata, err := w.stableMetadataOwner()
	if err != nil {
		return err
	}
	return metadata.RequireRootPublicationHooks()
}

func (w *PreparedFiniteLeafWorkspace) BackingBytes() uint64 {
	if w == nil {
		return 0
	}
	return w.backing
}
func (w *PreparedFiniteLeafWorkspace) validate(db *DB, l *lane) error {
	if w == nil || w.closed || w.db != db || w.lane != l {
		return errPreparedFiniteLeafWorkspace
	}
	switch installed := w.installed.(type) {
	case *cachingLeafPageLog:
		if installed == nil || installed.db != db || installed.lane != l {
			return errPreparedFiniteLeafWorkspace
		}
	case *cachingLeafPageLogGroup:
		if installed == nil || installed.db != db {
			return errPreparedFiniteLeafWorkspace
		}
		actual, ok := installed.laneForWorkerIndex(0)
		if !ok || actual != l {
			return errPreparedFiniteLeafWorkspace
		}
	default:
		return errPreparedFiniteLeafWorkspace
	}
	select {
	case <-db.closeCh:
		return errWALClosed
	default:
	}
	return nil
}

// preparePages is only compaction/owned-record preparation. It reserves no RID,
// opens no file and appends no WAL or physical record. A future admitted caller
// supplies the existing ordered RID reservation after this succeeds.
func (w *PreparedFiniteLeafWorkspace) preparePages(pages [][]byte) ([]valuelog.Record, error) {
	if w == nil || w.closed {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if err := w.validate(w.db, w.lane); err != nil {
		return nil, err
	}
	count := uint64(len(pages))
	if w.active || count == 0 || count > w.maxBatch || count > w.maxOutput-w.used {
		return nil, errPreparedFiniteLeafWorkspace
	}
	for _, p := range pages {
		if len(p) != page.PageSize {
			return nil, errPreparedFiniteLeafWorkspace
		}
	}
	if count > uint64(cap(w.records)) {
		// Both old and replacement backing coexist at growth. Charge whole arrays;
		// copied compact destinations remain owned and are not double allocated.
		recordSize := uint64(unsafe.Sizeof(valuelog.Record{}))
		sliceSize := uint64(unsafe.Sizeof([]byte{}))
		if count > math.MaxUint64/(recordSize+sliceSize) {
			return nil, errPreparedFiniteLeafWorkspace
		}
		if err := w.chargeAllocation(count*recordSize, true); err != nil {
			return nil, err
		}
		if err := w.chargeAllocation(count*sliceSize, true); err != nil {
			return nil, err
		}
		records := make([]valuelog.Record, int(count))
		compact := make([][]byte, int(count))
		copy(compact, w.compact)
		clear(w.records[:cap(w.records)])
		clear(w.compact[:cap(w.compact)])
		w.records, w.compact = records, compact
	}
	w.records = w.records[:int(count)]
	for i, p := range pages {
		if cap(w.compact[i]) < page.PageSize {
			if err := w.chargeAllocation(page.PageSize, false); err != nil {
				w.releasePreparedPages()
				return nil, err
			}
			w.compact[i] = make([]byte, 0, page.PageSize)
		}
		encoded, _, err := valuelog.MaybeCompactLeafLogPayloadTo(w.compact[i][:0], p)
		if err != nil {
			w.releasePreparedPages()
			return nil, err
		}
		w.records[i] = valuelog.Record{Value: encoded}
	}
	w.active = true
	w.ridsAssigned = false
	return w.records, nil
}
func (w *PreparedFiniteLeafWorkspace) reservePreparedRIDs(start uint64) error {
	if w == nil || w.closed || !w.active || w.ridsAssigned || start == 0 || uint64(len(w.records))-1 > math.MaxUint64-start {
		return errPreparedFiniteLeafWorkspace
	}
	w.used += uint64(len(w.records))
	w.ridsAssigned = true
	for i := range w.records {
		w.records[i].RID = start + uint64(i)
	}
	return nil
}
func (w *PreparedFiniteLeafWorkspace) releasePreparedPages() {
	if w == nil {
		return
	}
	clear(w.records[:cap(w.records)])
	w.active = false
	w.ridsAssigned = false
}
func (w *PreparedFiniteLeafWorkspace) Close() error { return w.CloseWithTerminal(nil) }

// CloseWithTerminal keeps failed registrar/source owners on this actual workspace.
// The consumer is synchronous and is never stored. Credits release only after
// the collector's complete checked cleanup succeeds.
func (w *PreparedFiniteLeafWorkspace) CloseWithTerminal(consumer rootpublication.StableSegmentTerminalConsumer) error {
	if w == nil || w.closed {
		return nil
	}
	if w.active {
		return errPreparedFiniteLeafWorkspace
	}
	if w.preparer != nil {
		if err := w.preparer.Close(); err != nil {
			return err
		}
	}
	if w.stableCollector != nil {
		if err := w.writerBacking.ForgetSegmentKeys(); err != nil {
			return err
		}
		if err := w.stableCollector.CloseWithTerminal(consumer); err != nil {
			return err
		}
		w.stableCollector = nil
	}
	if w.stableMetadata != nil && w.suppliedMetadata {
		w.stableMetadata.ReleaseStableMetadata()
		w.stableMetadata = nil
		w.resident = nil
	}
	if w.stableMetadata != nil {
		// No pending finite proof/successor can exist until the internal hooks
		// are implemented. Their retained-lifetime close contract remains a
		// prerequisite to enabling the staged stable route.
		if err := w.stableMetadata.Close(); err != nil {
			return err
		}
		w.stableMetadata = nil
	}
	if err := w.writerBacking.Close(); err != nil {
		return err
	}
	clear(w.records[:cap(w.records)])
	clear(w.compact[:cap(w.compact)])
	w.records = nil
	w.ptrs = nil
	w.compact = nil
	w.writerBacking = nil
	w.preparer = nil
	w.installed = nil
	w.db = nil
	w.lane = nil
	w.reserve = nil
	w.closed = true
	return nil
}

// prepareFrameOne preserves the ordinary scalar block preparation policy but
// replaces its pooled scratch with this request's bounded serial owner.
func (w *PreparedFiniteLeafWorkspace) prepareFrameOne(rid uint64, value []byte, codec valuelog.BlockCodec, ioNs, encodeNs, safety float64) (preparedDictFrame, error) {
	if err := w.validate(w.db, w.lane); err != nil {
		return preparedDictFrame{}, err
	}
	if w.preparer == nil {
		max := int(w.maxBatch)
		if max > valuelog.MaxFrameK {
			max = valuelog.MaxFrameK
		}
		if err := w.chargeAllocation(16, true); err != nil {
			return preparedDictFrame{}, err
		}
		owner, err := valuelog.NewFiniteFramePreparer(max, max*page.PageSize, w.charge)
		if err != nil {
			return preparedDictFrame{}, err
		}
		w.preparer = owner
	}
	var records [1]valuelog.Record
	records[0] = valuelog.Record{RID: rid, Value: value}
	body, stats, err := w.preparer.Prepare(records[:], codec, ioNs, encodeNs, safety, true)
	if err != nil {
		return preparedDictFrame{}, err
	}
	return preparedDictFrame{start: 0, end: 1, body: body, finiteBody: w.preparer, stats: stats}, nil
}
func (w *PreparedFiniteLeafWorkspace) beginWriter(writer valueWriter, records, raw int) (*valuelog.FiniteWriterLoan, error) {
	if w == nil {
		return nil, nil
	}
	if err := w.validate(w.db, w.lane); err != nil {
		return nil, err
	}
	concrete, ok := writer.(*valuelog.Writer)
	if !ok {
		return nil, errPreparedFiniteLeafWorkspace
	}
	// The exact registration/source edge precedes the first writer borrow. It
	// remains live across append frontiers and prevents a retired segment's key
	// address from being recycled within this ledger.
	if w.stableCollector != nil {
		if err := concrete.RecordStableSegmentFrontier(w.stableCollector, w.db.valueLogReader, w.stableCollectorMaxRegistered, w.stableMetadata, w.resident, false); err != nil {
			return nil, err
		}
	}
	return concrete.BeginFiniteWriterLoan(w.writerBacking, records, raw)
}

// validateSelectorInputs refuses sources whose template/training ownership has
// not yet been admitted. It never changes configuration to obtain eligibility.
func (w *PreparedFiniteLeafWorkspace) validateSelectorInputs(db *DB, l *lane, dictID uint64, dict []byte, count int) error {
	if w == nil {
		return nil
	}
	if err := w.validate(db, l); err != nil {
		return err
	}
	if dictID != 0 || len(dict) != 0 || count < 1 || uint64(count) > w.maxBatch {
		return errPreparedFiniteLeafWorkspace
	}
	if db.templateCompressionEnabled() {
		return errPreparedFiniteLeafWorkspace
	}
	trainer := db.valueLogDictTrainerForClass(vlogDictClassOuterLeaf)
	if trainer != nil && trainer.ShouldCollect() {
		return errPreparedFiniteLeafWorkspace
	}
	return nil
}
func (w *PreparedFiniteLeafWorkspace) preparePointers(count int) ([]page.ValuePtr, error) {
	if w == nil || w.closed || count < 1 || uint64(count) > w.maxBatch {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if count > cap(w.ptrs) {
		size := uint64(unsafe.Sizeof(page.ValuePtr{}))
		if uint64(count) > uint64(math.MaxInt)/size {
			return nil, errPreparedFiniteLeafWorkspace
		}
		if err := w.chargeAllocation(uint64(count)*size, false); err != nil {
			return nil, err
		}
		next := make([]page.ValuePtr, count)
		clear(w.ptrs[:cap(w.ptrs)])
		w.ptrs = next
	}
	w.ptrs = w.ptrs[:count]
	clear(w.ptrs)
	return w.ptrs, nil
}

// beginLane/endLane bracket the existing vlogMu critical section. This optional
// scope is not visible while the lane is unlocked and never replaces its lock.
func (w *PreparedFiniteLeafWorkspace) beginLane(l *lane) error {
	if w == nil {
		return nil
	}
	if err := w.validate(w.db, l); err != nil {
		return err
	}
	if l.finiteWorkspace != nil {
		return errPreparedFiniteLeafWorkspace
	}
	l.finiteWorkspace = w
	return nil
}
func (w *PreparedFiniteLeafWorkspace) endLane(l *lane) {
	if w != nil && l != nil && l.finiteWorkspace == w {
		l.finiteWorkspace = nil
	}
}

// These serial interfaces consume the same scalar/batch append algorithms.
// Stable methods are explicitly staged: they refuse before preparation/RIDs
// while rootpublication/registry backing hooks are missing. They cannot be
// mistaken for complete publication admission.
var _ backenddb.LeafPageLog = (*PreparedFiniteLeafWorkspace)(nil)
var _ backenddb.LeafPageBatchLog = (*PreparedFiniteLeafWorkspace)(nil)
var _ backenddb.LeafPageStableLog = (*PreparedFiniteLeafWorkspace)(nil)
var _ backenddb.LeafPageStableBatchLog = (*PreparedFiniteLeafWorkspace)(nil)

func (w *PreparedFiniteLeafWorkspace) ConcurrentLeafPageAppends() bool    { return false }
func (w *PreparedFiniteLeafWorkspace) PreparedLeafPageAppends() bool      { return false }
func (w *PreparedFiniteLeafWorkspace) PreparedLeafPageBatchAppends() bool { return false }
func (w *PreparedFiniteLeafWorkspace) AppendLeafPage(data []byte) (page.LeafLogPtr, error) {
	if w == nil || w.closed {
		return page.LeafLogPtr{}, errPreparedFiniteLeafWorkspace
	}
	if w.stableCollector != nil {
		ptrs, _, err := w.appendLeafPages([][]byte{data}, false)
		if err != nil {
			return page.LeafLogPtr{}, err
		}
		if len(ptrs) != 1 {
			return page.LeafLogPtr{}, errPreparedFiniteLeafWorkspace
		}
		return ptrs[0], nil
	}
	if err := w.validateSelectorInputs(w.db, w.lane, 0, nil, 1); err != nil {
		return page.LeafLogPtr{}, err
	}
	records, err := w.preparePages([][]byte{data})
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	defer w.releasePreparedPages()
	start, err := w.db.ReserveValueLogRIDs(1)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	if err = w.reservePreparedRIDs(start); err != nil {
		return page.LeafLogPtr{}, err
	}
	ptr, retainPath, err := w.db.appendValueLogOneInternal(w.lane, 0, nil, records[0].RID, records[0].Value, journalDurabilityNone, false, w)
	if retainPath != "" {
		w.db.markValueLogRetain(retainPath)
	}
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	leaf, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	w.db.noteLeafGenerationRecordLength(ptr)
	return leaf, nil
}
func (w *PreparedFiniteLeafWorkspace) AppendLeafPageWithStableResources(data []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	// Match the installed stable scalar route, which calls the batch algorithm
	// for one record rather than the ordinary scalar append algorithm.
	ptrs, resources, err := w.appendLeafPages([][]byte{data}, true)
	if err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	if len(ptrs) != 1 {
		resources.Release()
		return page.LeafLogPtr{}, nil, errPreparedFiniteLeafWorkspace
	}
	return ptrs[0], resources, nil
}

func (w *PreparedFiniteLeafWorkspace) AppendLeafPagesWithStableResources(data [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return w.appendLeafPages(data, true)
}

func (w *PreparedFiniteLeafWorkspace) AppendLeafPages(data [][]byte) ([]page.LeafLogPtr, error) {
	result, _, err := w.appendLeafPages(data, false)
	return result, err
}

func (w *PreparedFiniteLeafWorkspace) appendLeafPages(data [][]byte, stable bool) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if w == nil || w.closed {
		return nil, nil, errPreparedFiniteLeafWorkspace
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if stable || w.stableCollector != nil {
		if err := w.requireStableHooks(len(data)); err != nil {
			return nil, nil, err
		}
	}
	if len(data) == 1 && !stable && w.stableCollector == nil {
		n := uint64(unsafe.Sizeof(page.LeafLogPtr{}))
		if err := w.chargeAllocation(n, false); err != nil {
			return nil, nil, err
		}
		result := make([]page.LeafLogPtr, 1)
		ptr, err := w.AppendLeafPage(data[0])
		if err != nil {
			return nil, nil, err
		}
		result[0] = ptr
		return result, nil, nil
	}
	if err := w.validateSelectorInputs(w.db, w.lane, 0, nil, len(data)); err != nil {
		return nil, nil, err
	}
	if uint64(len(data)) > uint64(math.MaxInt)/uint64(unsafe.Sizeof(page.LeafLogPtr{})) {
		return nil, nil, errPreparedFiniteLeafWorkspace
	}
	if err := w.chargeAllocation(uint64(len(data))*uint64(unsafe.Sizeof(page.LeafLogPtr{})), false); err != nil {
		return nil, nil, err
	}
	result := make([]page.LeafLogPtr, len(data))
	records, err := w.preparePages(data)
	if err != nil {
		return nil, nil, err
	}
	defer w.releasePreparedPages()
	start, err := w.db.ReserveValueLogRIDs(len(records))
	if err != nil {
		return nil, nil, err
	}
	if err = w.reservePreparedRIDs(start); err != nil {
		return nil, nil, err
	}
	var ptrs []page.ValuePtr
	var resources *rootpublication.StableResourceSet
	if stable {
		ptrs, resources, err = w.db.appendValueLogWithStableResources(w.lane, 0, nil, records, journalDurabilityNone, w)
	} else {
		ptrs, resources, err = w.db.appendValueLogInternalObserved(w.lane, 0, nil, records, journalDurabilityNone, nil, nil, w)
	}
	releaseResources := true
	defer func() {
		if releaseResources {
			resources.Release()
		}
	}()

	if err != nil {
		return nil, nil, err
	}
	if len(ptrs) != len(result) {
		return nil, nil, errPreparedFiniteLeafWorkspace
	}
	for i, p := range ptrs {
		leaf, err := page.LeafLogPtrFromValuePtr(p)
		if err != nil {
			return nil, nil, err
		}
		result[i] = leaf
		w.db.noteLeafGenerationRecordLength(p)
	}
	releaseResources = false
	return result, resources, nil
}

// Flush and Sync preserve the installed serial lane's durability behavior.
// Their complete publication backing is not yet an admitted composite route.
func (w *PreparedFiniteLeafWorkspace) Flush() error {
	if w == nil || w.closed || w.active {
		return errPreparedFiniteLeafWorkspace
	}
	if err := w.validate(w.db, w.lane); err != nil {
		return err
	}
	return w.installed.Flush()
}
func (w *PreparedFiniteLeafWorkspace) Sync() error {
	if w == nil || w.closed || w.active {
		return errPreparedFiniteLeafWorkspace
	}
	if err := w.validate(w.db, w.lane); err != nil {
		return err
	}
	return w.installed.Sync()
}

// NewPreparedFiniteLeafWorkspaceWithStableMetadata attaches the canonical
// request's existing metadata facet and explicit resident destination. The
// facade stays caller-owned; Close releases this workspace's actual retention.
// It does not enable RequireRootPublicationHooks or any publication guard.
func NewPreparedFiniteLeafWorkspaceWithStableMetadata(installed backenddb.LeafPageLog, maxOutput, maxBatch uint64, scratchReserve func(uint64) error, metadata *valuelog.FiniteStableMetadata, resident rootpublication.StableMetadataAccount) (*PreparedFiniteLeafWorkspace, error) {
	if metadata == nil || resident == nil {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if err := metadata.RetainStableMetadata(); err != nil {
		return nil, err
	}
	w, err := NewPreparedFiniteLeafWorkspace(installed, maxOutput, maxBatch, scratchReserve)
	if err != nil {
		metadata.ReleaseStableMetadata()
		return nil, err
	}
	w.stableMetadata = metadata
	w.resident = resident
	w.suppliedMetadata = true
	return w, nil
}

// PrepareStableResources establishes one collector for the actual profile's
// distinct segment ceiling. The closed production hook is checked before any
// RID, append, file birth or source mutation by the selected append route.
func (w *PreparedFiniteLeafWorkspace) PrepareStableResources(maxDistinctSegments, maxRegisteredFiles uint64) error {
	if w == nil || w.closed || !w.suppliedMetadata || w.resident == nil || w.stableCollector != nil || maxDistinctSegments > w.maxOutput || maxRegisteredFiles == 0 {
		return errPreparedFiniteLeafWorkspace
	}
	c, err := rootpublication.NewStableSegmentFrontierCollector(maxDistinctSegments, w.stableMetadata)
	if err != nil {
		return err
	}
	if err = w.writerBacking.BindSegmentCollector(c); err != nil {
		// No source/registrar edge can exist yet.
		_ = c.Close()
		return err
	}
	w.stableCollector = c
	w.stableCollectorMaxRegistered = maxRegisteredFiles
	return nil
}
func (w *PreparedFiniteLeafWorkspace) FinishStableResources() (*rootpublication.StableResourceSet, error) {
	if w == nil || w.closed || w.active || w.stableCollector == nil {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if err := w.writerBacking.ForgetSegmentKeys(); err != nil {
		return nil, err
	}
	set, err := w.stableCollector.Freeze()
	if err != nil {
		return nil, err
	}
	w.stableCollector = nil
	return set, nil
}

func (w *PreparedFiniteLeafWorkspace) recordStableFrontier(writer valueWriter, fileID uint32, path string, synced bool) error {
	if w == nil || w.stableCollector == nil {
		return nil
	}
	if err := w.requireStableHooks(1); err != nil {
		return err
	}
	concrete, ok := writer.(*valuelog.Writer)
	if !ok {
		return errPreparedFiniteLeafWorkspace
	}
	// The cached owner already retains exact logical/physical/namespace fields.
	// Rebuilding a diagnostic registration here would duplicate backing and
	// substitute NamespaceNone for a constructor-certified creation authority.
	return concrete.RecordStableSegmentFrontier(w.stableCollector, w.db.valueLogReader, w.stableCollectorMaxRegistered, w.stableMetadata, w.resident, synced)
}
