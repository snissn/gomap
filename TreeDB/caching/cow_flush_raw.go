package caching

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type cowFrozenMerge struct {
	heap    stableOpMergeHeap
	cursors []stableUnsafeOpIter
	lease   *memtable.COWExternalLease
}

// Admit optional fan-in workspace from the source budget, outside the protected
// maintenance tranche used by the physical build. Each cursor acquires its own
// root view and traversal storage through the existing COW owner.
func openCOWFrozenMerge(budget *memtable.COWBudget, sources []cowTable) (cowFrozenMerge, error) {
	if len(sources) < 2 {
		return cowFrozenMerge{}, nil
	}
	n := uint64(len(sources))
	lease, err := budget.AcquireExternal(
		memtable.COWAllocationCharge(n*uint64(unsafe.Sizeof(stableOpMergeItem{}))) +
			memtable.COWAllocationCharge(n*uint64(unsafe.Sizeof(stableUnsafeOpIter{}))),
	)
	if err == memtable.ErrCOWCapacity {
		return cowFrozenMerge{}, nil
	}
	if err != nil {
		return cowFrozenMerge{}, err
	}
	m := cowFrozenMerge{
		heap:    make(stableOpMergeHeap, 0, len(sources)),
		cursors: make([]stableUnsafeOpIter, len(sources)),
		lease:   lease,
	}
	for i := range sources {
		it := sources[i].newIterator(nil, nil, false)
		if err := it.Error(); err != nil {
			cleanupErr := joinCOWCleanupError(it.Close(), m.close())
			if err == memtable.ErrCOWCapacity && cleanupErr == nil {
				// The one-cursor path can still progress under a small view
				// limit. No build has started and all provisional views are gone.
				return cowFrozenMerge{}, nil
			}
			return cowFrozenMerge{}, joinCOWCleanupError(err, cleanupErr)
		}
		m.cursors[i].UnsafeIterator = it
		if _, ok := it.(iterator.RevisionUnsafeIterator); !ok {
			return cowFrozenMerge{}, joinCOWCleanupError(ErrCOWUnsupported, m.close())
		}
		if it.Valid() {
			// The cut orders its frozen sources oldest to newest. Revisions
			// are copied verbatim and do not determine source precedence.
			m.heap = append(m.heap, stableOpMergeItem{iter: &m.cursors[i], priority: len(sources) - 1 - i, key: it.UnsafeKey()})
		}
	}
	return m, nil
}

func (m *cowFrozenMerge) close() error {
	var err error
	for i := range m.cursors {
		err = joinCOWCleanupError(err, closeCanonicalStableIterator(m.cursors[i].UnsafeIterator))
	}
	lease := m.lease
	*m = cowFrozenMerge{}
	lease.Close()
	return err
}

// Only use entries from cowTable.newIterator: its COWRecord strings are
// immutable and remain owned by the captured cut through backend Write/Close,
// including after the cursor advances or closes. The caller stages the tail
// separately from preceding chunks so it can release the raw buffer before
// final publication.
func writeCOWFrozenRawChunks(sources []cowTable, merge *cowFrozenMerge, chunkCap int, write func([]batch.Entry) error) ([]batch.Entry, error) {
	ops := make([]batch.Entry, 0, chunkCap)
	writeChunk := func() error {
		if err := write(ops); err != nil {
			return err
		}
		ops = ops[:0]
		return nil
	}
	emit := func(entry batch.Entry) error {
		if len(ops) == chunkCap {
			if err := writeChunk(); err != nil {
				return err
			}
		}
		ops = append(ops, entry)
		return nil
	}
	if merge.cursors != nil {
		var stats canonicalFlushRun
		err := mergeCanonicalStableIteratorHeap(merge.heap, &stats, emit)
		if err = joinCOWCleanupError(err, merge.close()); err != nil {
			return nil, err
		}
	} else {
		// Preserve per-source batches and the existing physical precedence
		// on fallback. Hold the prior tail through empty trailing sources.
		for i := range sources {
			it := sources[i].newIterator(nil, nil, false)
			if err := streamCOWFrozenSource(it, emit, func() error {
				if len(ops) == 0 {
					return nil
				}
				return writeChunk()
			}); err != nil {
				return nil, err
			}
		}
	}
	// Even an exact multiple stays pending until every cursor has closed and
	// reported its terminal status. A raw-empty build still publishes coverage.
	return ops, nil
}

func streamCOWFrozenSource(it iterator.UnsafeIterator, emit func(batch.Entry) error, begin func() error) (err error) {
	defer func() { err = joinCOWCleanupError(err, closeCanonicalStableIterator(it)) }()
	if _, ok := it.(iterator.RevisionUnsafeIterator); !ok {
		return ErrCOWUnsupported
	}
	if !it.Valid() {
		return nil
	}
	if err := begin(); err != nil {
		return err
	}
	wrapped := stableUnsafeOpIter{UnsafeIterator: it}
	for it.Valid() {
		if err := emit(wrapped.Entry()); err != nil {
			return err
		}
		it.Next()
	}
	return nil
}
