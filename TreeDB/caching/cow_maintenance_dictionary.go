package caching

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// The built-in provider transfers into the preadmitted slot before capture can
// unwind. A return-only provider cannot prove this maintenance lifetime.
type cowMaintenanceDictionaryProvider interface {
	PrepareDictionaryReadDefinitionInto(uint64, valuelog.COWReadLimits, int, func(dictdb.DictionaryReadAllocationSizes) (func(), error), **dictdb.DictionaryReadDefinition) error
}

type cowMaintenanceDictionaryEntry struct {
	id    uint64
	owner *dictdb.DictionaryReadDefinition
	err   error
}

// This is one synchronous paired-basis operation, not a generation/global cache.
// The original flush/admission fence keeps the cache/provider alive. writerMu
// serializes binding and all reads; after it drops only independent definition
// owners survive. Their possibly blocking cleanup never runs under writerMu.
type cowMaintenanceLeafScope struct {
	cache         *cowCache
	before, after *backenddb.Snapshot
	provider      cowMaintenanceDictionaryProvider
	entries       []cowMaintenanceDictionaryEntry
	used          int
	lease         *memtable.COWExternalLease
	cleanup       rootpublication.SemanticCleanup
	failure       error
}

func (db *DB) prepareCOWMaintenanceLeaves() (*cowMaintenanceLeafScope, error) {
	c := db.cow
	n := c.maintenance.Limits().MaxResources
	if n < 1 || uint64(n) > ^uint64(0)/uint64(unsafe.Sizeof(cowMaintenanceDictionaryEntry{})) {
		return nil, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowMaintenanceLeafScope{}))) +
		memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof(cowMaintenanceDictionaryEntry{}))) +
		memtable.COWAllocationCharge(16*uint64(unsafe.Sizeof(uintptr(0))))
	lease, err := c.maintenance.AcquireExternal(charge)
	if err != nil {
		return nil, err
	}
	s := &cowMaintenanceLeafScope{cache: c, entries: make([]cowMaintenanceDictionaryEntry, n), lease: lease}
	s.provider, _ = db.dictStore.(cowMaintenanceDictionaryProvider)
	s.cleanup.Work = s
	return s, nil
}

func (s *cowMaintenanceLeafScope) MatchesNativeMaintenanceSnapshots(before, after *backenddb.Snapshot) bool {
	return s != nil && s.cache != nil && before != nil && after != nil && before == s.before && after == s.after
}

func (s *cowMaintenanceLeafScope) admitDefinition(a dictdb.DictionaryReadAllocationSizes) (func(), error) {
	limits := s.cache.maintenance.Limits()
	if a.Snapshot.ValueLog.MapHint > limits.MaxResources || a.Snapshot.ValueLog.FileCount > limits.MaxResources {
		return nil, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(a.Owner) + memtable.COWAllocationCharge(a.Callback) +
		memtable.COWAllocationCharge(a.Definition) + memtable.COWAllocationCharge(a.Payload) + memtable.COWAllocationCharge(a.Pin) +
		memtable.COWAllocationCharge(3*uint64(unsafe.Sizeof(uintptr(0)))) + cowSnapshotRetentionCharge(a.Snapshot)
	lease, metadata, err := admitCOWSnapshotRetention(s.cache.maintenance, charge, a.Snapshot)
	if err != nil {
		return nil, err
	}
	if metadata != nil && a.CaptureMetadata != nil {
		*a.CaptureMetadata = metadata
		metadata = nil
	}
	return func() {
		m, l := metadata, lease
		metadata, lease = nil, nil
		m.Close()
		l.Close()
	}, nil
}

func (s *cowMaintenanceLeafScope) definition(id uint64, limits valuelog.COWReadLimits) (*dictdb.DictionaryReadDefinition, error) {
	for i := 0; i < s.used; i++ {
		if s.entries[i].id == id {
			return s.entries[i].owner, s.entries[i].err
		}
	}
	if s.used == len(s.entries) {
		return nil, memtable.ErrCOWCapacity
	}
	if s.provider == nil {
		return nil, valuelog.ErrCOWReadUnsupported
	}
	e := &s.entries[s.used]
	e.id = id
	s.used++ // The slot is live before preparation, including panic/Goexit.
	e.err = s.provider.PrepareDictionaryReadDefinitionInto(id, limits, s.cache.maintenance.Limits().MaxResources, s.admitDefinition, &e.owner)
	if e.err == nil && e.owner == nil {
		e.err = rootpublication.ErrPublisherProtocol
	}
	return e.owner, e.err
}

func (s *cowMaintenanceLeafScope) ReadNativeMaintenanceDictionaryLeaf(snapshot *backenddb.Snapshot, ptr page.LeafLogPtr, shape valuelog.COWRecordShape, dst []byte) ([]byte, error) {
	if s == nil || s.cache == nil || snapshot == nil || (snapshot != s.before && snapshot != s.after) || !shape.Compressed || shape.DictID == 0 {
		return nil, backenddb.ErrRootPublicationBasisMismatch
	}
	limit := s.cache.maintenance.Limits().MaxInFlightBytes
	limits := valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit}
	if shape.RawBytes > limit || shape.PayloadBytes() > limit {
		return nil, memtable.ErrCOWCapacity
	}
	// The enclosing helper already inspected this exact Snapshot's record. Pin
	// lookup is non-I/O; ReadCOWLeafPage revalidates source, shape, frame and CRC.
	f, _, err := snapshot.PinnedValueLogFile(ptr.ValuePtr().FileID)
	if err != nil {
		return nil, err
	}
	window := max(shape.RawBytes, uint64(2048))
	sizes, err := valuelog.COWBoundedDecoderRetentionSizes(shape.RawBytes, shape.PayloadBytes(), window)
	if err != nil {
		return nil, err
	}
	owner, err := s.definition(shape.DictID, limits)
	if err != nil {
		return nil, err
	}
	charge := memtable.COWAllocationCharge(sizes.OwnerWrapper) + memtable.COWAllocationCharge(sizes.RawBacking) +
		memtable.COWAllocationCharge(sizes.InputBacking) + memtable.COWAllocationCharge(3*uint64(unsafe.Sizeof(uintptr(0))))
	lease, err := s.cache.maintenance.AcquireExternal(charge)
	if err != nil {
		return nil, err
	}
	var input, raw []byte
	var decoder *valuelog.COWBoundedDecoder
	var decode valuelog.COWDecodeFunc
	defer func() {
		if decoder != nil {
			decoder.Close()
			decoder = nil
		}
		decode = nil
		input, raw = nil, nil
		lease.Close()
	}()
	input, raw = make([]byte, sizes.InputBacking), make([]byte, sizes.RawBacking)
	decoder, err = valuelog.NewCOWBoundedDecoder(shape.DictID, owner.Bytes, owner, sizes.RawBacking, sizes.InputBacking, window, lease, charge)
	if err != nil {
		return nil, err
	}
	decode = decoder.Decode
	out, err := valuelog.ReadCOWLeafPage(f, ptr.ValuePtr(), shape, true, input, raw, dst, decode)
	return out, err
}

func (s *cowMaintenanceLeafScope) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	if s.failure != nil {
		return rootpublication.SemanticCleanupCompletion{}, s.failure
	}
	for i := 0; i < s.used; i++ {
		e := &s.entries[i]
		if err := e.owner.ClosePreparedRead(); err != nil {
			if s.failure == nil {
				s.failure = err
			}
		} else {
			*e = cowMaintenanceDictionaryEntry{}
		}
	}
	if s.failure != nil {
		return rootpublication.SemanticCleanupCompletion{}, s.failure
	}
	s.cache, s.before, s.after, s.provider = nil, nil, nil, nil
	s.entries = nil
	refund := s.lease
	s.lease = nil
	return rootpublication.SemanticCleanupCompletion{Refund: refund}, nil
}

// Transfer only after writerMu drops. Never borrow the worker's cleanup guard
// from another goroutine, wait for it while holding flushMu, or run storage
// callbacks here. The existing worker checks destruction and retains original
// failure/poison/debt; checked Close joins its eligible frontier before dictStore.
func (c *cowCache) retireCOWMaintenanceLeaves(slot **cowMaintenanceLeafScope) {
	s := *slot
	*slot = nil
	s.before, s.after = nil, nil
	if s.used == 0 {
		// No definition/capture exists; only independent accounting remains.
		completion, err := s.cleanup.Finish()
		if err != nil {
			panic("empty maintenance read scope has cleanup debt")
		}
		s = nil
		completion.Complete()
		return
	}
	record := &s.cleanup
	s = nil
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(record)
	c.semanticMu.Unlock()
	c.wakeSemanticCleanup()
}
