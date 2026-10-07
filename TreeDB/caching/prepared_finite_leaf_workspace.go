package caching

import (
	"errors"
	"math"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	compact                            [][]byte
	records                            []valuelog.Record
	ptrs                               []page.ValuePtr
	maxOutput, maxBatch, used, backing uint64
	active, closed, ridsAssigned       bool
}

// NewPreparedFiniteLeafWorkspace constructs identity-free scratch credit for the
// exact installed cached leaf log. The concrete writer/segment is acquired only
// inside the existing lane.vlogMu append critical section.
func NewPreparedFiniteLeafWorkspace(installed backenddb.LeafPageLog, maxOutput, maxBatch uint64, reserve func(uint64) error) (*PreparedFiniteLeafWorkspace, error) {
	if installed == nil || reserve == nil || maxOutput == 0 || maxBatch == 0 || maxBatch > maxOutput || maxOutput == math.MaxUint64 || maxBatch > uint64(math.MaxInt)/uint64(unsafe.Sizeof(valuelog.Record{})) {
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
	n := uint64(unsafe.Sizeof(PreparedFiniteLeafWorkspace{}))
	if err := reserve(n); err != nil {
		return nil, err
	}
	result := &PreparedFiniteLeafWorkspace{installed: installed, db: db, lane: l, reserve: reserve, maxOutput: maxOutput, maxBatch: maxBatch, backing: n}
	owner, err := valuelog.NewFiniteWriterBacking(maxOutput+1, result.charge)
	if err != nil {
		return nil, err
	}
	result.writerBacking = owner
	return result, nil
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
		if err := w.charge(count * (recordSize + sliceSize)); err != nil {
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
			if err := w.charge(page.PageSize); err != nil {
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
func (w *PreparedFiniteLeafWorkspace) Close() error {
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
		if err := w.charge(uint64(count) * size); err != nil {
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
// Stable-resource interfaces intentionally remain absent until their complete
// token/registry/rotation backing has pre-WAL admission.
var _ backenddb.LeafPageLog = (*PreparedFiniteLeafWorkspace)(nil)
var _ backenddb.LeafPageBatchLog = (*PreparedFiniteLeafWorkspace)(nil)

func (w *PreparedFiniteLeafWorkspace) ConcurrentLeafPageAppends() bool    { return false }
func (w *PreparedFiniteLeafWorkspace) PreparedLeafPageAppends() bool      { return false }
func (w *PreparedFiniteLeafWorkspace) PreparedLeafPageBatchAppends() bool { return false }
func (w *PreparedFiniteLeafWorkspace) AppendLeafPage(data []byte) (page.LeafLogPtr, error) {
	if w == nil || w.closed {
		return page.LeafLogPtr{}, errPreparedFiniteLeafWorkspace
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
func (w *PreparedFiniteLeafWorkspace) AppendLeafPages(data [][]byte) ([]page.LeafLogPtr, error) {
	if w == nil || w.closed {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) == 1 {
		n := uint64(unsafe.Sizeof(page.LeafLogPtr{}))
		if err := w.charge(n); err != nil {
			return nil, err
		}
		result := make([]page.LeafLogPtr, 1)
		ptr, err := w.AppendLeafPage(data[0])
		if err != nil {
			return nil, err
		}
		result[0] = ptr
		return result, nil
	}
	if err := w.validateSelectorInputs(w.db, w.lane, 0, nil, len(data)); err != nil {
		return nil, err
	}
	if uint64(len(data)) > uint64(math.MaxInt)/uint64(unsafe.Sizeof(page.LeafLogPtr{})) {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if err := w.charge(uint64(len(data)) * uint64(unsafe.Sizeof(page.LeafLogPtr{}))); err != nil {
		return nil, err
	}
	result := make([]page.LeafLogPtr, len(data))
	records, err := w.preparePages(data)
	if err != nil {
		return nil, err
	}
	defer w.releasePreparedPages()
	start, err := w.db.ReserveValueLogRIDs(len(records))
	if err != nil {
		return nil, err
	}
	if err = w.reservePreparedRIDs(start); err != nil {
		return nil, err
	}
	ptrs, _, err := w.db.appendValueLogInternalObserved(w.lane, 0, nil, records, journalDurabilityNone, nil, nil, w)
	if err != nil {
		return nil, err
	}
	if len(ptrs) != len(result) {
		return nil, errPreparedFiniteLeafWorkspace
	}
	for i, p := range ptrs {
		leaf, err := page.LeafLogPtrFromValuePtr(p)
		if err != nil {
			return nil, err
		}
		result[i] = leaf
		w.db.noteLeafGenerationRecordLength(p)
	}
	return result, nil
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
