package caching

import (
	"errors"
	"fmt"
	"runtime"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func nativeRootValueLogAppendWidth(physicalCores, gomaxprocs int) int {
	parallelism := physicalCores
	if gomaxprocs > parallelism {
		parallelism = gomaxprocs
	}
	if parallelism <= 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}
	switch {
	case parallelism >= 8:
		return 8
	case parallelism >= 4:
		return 4
	case parallelism >= 2:
		return 2
	default:
		return 1
	}
}

func (db *DB) initNativeRootValueLogAppendLanes() {
	db.initNativeRootValueLogAppendLanesWithWidth(nativeRootValueLogAppendWidth(db.journalLanesPhysicalCores, db.journalLanesGOMAXPROCS))
}

func (db *DB) initNativeRootValueLogAppendLanesWithWidth(width int) {
	if db == nil || len(db.lanes) == 0 {
		return
	}
	indexes := make([]int, len(db.lanes))
	for i := range indexes {
		indexes[i] = i
	}
	if db.valueLogGenerationPolicy == uint8(backenddb.ValueLogGenerationHotWarmCold) && len(db.valueLogHotLanes) > 0 {
		indexes = db.valueLogHotLanes
	}
	db.nativeRootValueLogAppendLanes = make([]*lane, 0, len(indexes))
	for _, index := range indexes {
		db.nativeRootValueLogAppendLanes = append(db.nativeRootValueLogAppendLanes, &db.lanes[index])
	}
	if db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) || len(indexes) != 1 {
		return
	}
	canonical := db.nativeRootValueLogAppendLanes[0]
	if width < 1 {
		width = 1
	}
	db.nativeRootValueLogAppendShared = width > 1
	db.nativeRootValueLogAppendSeq.Store(uint32(canonical.vlogSeq))
	for len(db.nativeRootValueLogAppendLanes) < width {
		db.nativeRootValueLogAppendLanes = append(db.nativeRootValueLogAppendLanes, &lane{
			id:                      canonical.id,
			vlogGenerationClass:     vlogGenerationClassHot,
			vlogCompressionSelector: db.newValueLogCompressionSelector(),
			vlogSeq:                 canonical.vlogSeq,
		})
	}
}

func (db *DB) nativeRootValueLogAppendLanesSnapshot() []*lane {
	if db == nil || len(db.nativeRootValueLogAppendLanes) == 0 {
		return nil
	}
	return db.nativeRootValueLogAppendLanes
}

func (db *DB) nativeRootValueLogAppendAuxLanesSnapshot() []*lane {
	if db == nil || !db.nativeRootValueLogAppendShared || len(db.nativeRootValueLogAppendLanes) <= 1 {
		return nil
	}
	return db.nativeRootValueLogAppendLanes[1:]
}

func (db *DB) isNativeRootValueLogAppendLane(l *lane) bool {
	if db == nil || l == nil {
		return false
	}
	for _, candidate := range db.nativeRootValueLogAppendLanes {
		if candidate == l {
			return true
		}
	}
	return false
}

func (db *DB) isSharedNativeRootValueLogAppendLane(l *lane) bool {
	return db != nil && db.nativeRootValueLogAppendShared && db.isNativeRootValueLogAppendLane(l)
}

func (db *DB) nativeRootValueLogAppendLaneForFileID(laneID, seq uint32) *lane {
	if db == nil || !db.nativeRootValueLogAppendShared || len(db.nativeRootValueLogAppendLanes) == 0 || uint32(db.nativeRootValueLogAppendLanes[0].id) != laneID {
		return nil
	}
	for _, l := range db.nativeRootValueLogAppendLanes {
		if l == nil {
			continue
		}
		l.vlogMu.Lock()
		matches := l.vlogSeq == int(seq) && (l.vlogPath != "" || l.vlog != nil)
		l.vlogMu.Unlock()
		if matches {
			return l
		}
	}
	return nil
}

func (db *DB) advanceNativeRootValueLogAppendSeqAtLeast(seq int) {
	if db == nil || !db.nativeRootValueLogAppendShared || seq <= 0 {
		return
	}
	need := uint32(seq)
	for {
		cur := db.nativeRootValueLogAppendSeq.Load()
		if cur >= need || db.nativeRootValueLogAppendSeq.CompareAndSwap(cur, need) {
			return
		}
	}
}

func (db *DB) nextNativeRootValueLogAppendSeq() (int, error) {
	if db == nil || !db.nativeRootValueLogAppendShared || len(db.nativeRootValueLogAppendLanes) == 0 {
		return 0, errWALUnavailable
	}
	laneID := db.nativeRootValueLogAppendLanes[0].id
	for {
		cur := db.nativeRootValueLogAppendSeq.Load()
		next := cur + 1
		if next <= cur {
			return 0, fmt.Errorf("cachingdb: native-root value-log sequence space exhausted")
		}
		if _, err := valuelog.EncodeFileID(uint32(laneID), next); err != nil {
			if errors.Is(err, valuelog.ErrSegmentIDRange) {
				return 0, fmt.Errorf("cachingdb: native-root value-log sequence space exhausted")
			}
			return 0, err
		}
		if db.nativeRootValueLogAppendSeq.CompareAndSwap(cur, next) {
			return int(next), nil
		}
	}
}

func (db *DB) allValueLogWriterLanesSnapshot() []*lane {
	if db == nil {
		return nil
	}
	lanes := make([]*lane, 0, len(db.lanes)+len(db.nativeRootValueLogAppendAuxLanesSnapshot()))
	for i := range db.lanes {
		lanes = append(lanes, &db.lanes[i])
	}
	lanes = append(lanes, db.nativeRootValueLogAppendAuxLanesSnapshot()...)
	return lanes
}
