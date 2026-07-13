package caching

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication/osadapter"
	"github.com/snissn/gomap/TreeDB/page"
)

type stableResourceValueWriter interface {
	FileID() uint32
	OpenFile() *os.File
	Flush() error
}

// RegisterStableValueLogSegment captures the exact writer descriptor and the
// complete record end of ptrs while the lane rotation lock is held. It fails
// closed if the producer has already rotated away from the referenced file.
func (a *cachingValueLogAppender) RegisterStableValueLogSegment(
	ptrs []page.ValuePtr,
	spec rootpublication.StableResourceSpec,
) (*rootpublication.StableResourceToken, error) {
	if a == nil || a.db == nil || a.lane == nil {
		return nil, errWALUnavailable
	}
	fileID, frontier, err := exactValuePtrFrontier(ptrs)
	if err != nil {
		return nil, err
	}
	a.lane.vlogMu.Lock()
	defer a.lane.vlogMu.Unlock()
	return a.db.registerStableLaneResourceMuHeld(a.lane, fileID, frontier, rootpublication.ResourceValueLogSegment, spec)
}

// RegisterStableOuterLeafSegment is the raw outer-leaf counterpart of
// RegisterStableValueLogSegment. LogRecordRef.FileID is normalized to its
// physical value-log file ID, but the token kind remains outer_leaf_segment.
func (l *cachingLeafPageLog) RegisterStableOuterLeafSegment(
	refs []page.LogRecordRef,
	spec rootpublication.StableResourceSpec,
) (*rootpublication.StableResourceToken, error) {
	if l == nil || l.db == nil || l.lane == nil {
		return nil, errWALUnavailable
	}
	fileID, frontier, err := exactLogRecordRefFrontier(refs)
	if err != nil {
		return nil, err
	}
	l.lane.vlogMu.Lock()
	defer l.lane.vlogMu.Unlock()
	return l.db.registerStableLaneResourceMuHeld(l.lane, fileID, frontier, rootpublication.ResourceOuterLeafSegment, spec)
}

func exactValuePtrFrontier(ptrs []page.ValuePtr) (uint32, uint64, error) {
	if len(ptrs) == 0 {
		return 0, 0, fmt.Errorf("%w: empty value-log pointer set", rootpublication.ErrInvalidStableResource)
	}
	fileID := ptrs[0].FileID
	if fileID == 0 || !page.IsValueLogFileID(fileID) {
		return 0, 0, fmt.Errorf("%w: invalid value-log file id %d", rootpublication.ErrInvalidStableResource, fileID)
	}
	var frontier uint64
	for i, ptr := range ptrs {
		if ptr.FileID != fileID {
			return 0, 0, fmt.Errorf("%w: value-log pointers span file ids %d and %d", rootpublication.ErrResourceConflict, fileID, ptr.FileID)
		}
		recordLen := uint64(page.ValuePtrRecordLength(ptr))
		if recordLen == 0 || ptr.Offset > math.MaxUint64-recordLen {
			return 0, 0, fmt.Errorf("%w: invalid complete record frontier at pointer %d", rootpublication.ErrInvalidStableResource, i)
		}
		if end := ptr.Offset + recordLen; end > frontier {
			frontier = end
		}
	}
	return fileID, frontier, nil
}

func exactLogRecordRefFrontier(refs []page.LogRecordRef) (uint32, uint64, error) {
	if len(refs) == 0 {
		return 0, 0, fmt.Errorf("%w: empty outer-leaf reference set", rootpublication.ErrInvalidStableResource)
	}
	fileID := refs[0].ValueLogFileID()
	if fileID == 0 || !page.IsValueLogFileID(fileID) {
		return 0, 0, fmt.Errorf("%w: invalid outer-leaf file id %d", rootpublication.ErrInvalidStableResource, refs[0].FileID)
	}
	var frontier uint64
	for i, ref := range refs {
		if ref.ValueLogFileID() != fileID {
			return 0, 0, fmt.Errorf("%w: outer-leaf references span file ids %d and %d", rootpublication.ErrResourceConflict, fileID, ref.ValueLogFileID())
		}
		recordLen := uint64(ref.RecordLength())
		if recordLen == 0 || ref.Offset > math.MaxUint64-recordLen {
			return 0, 0, fmt.Errorf("%w: invalid complete outer-leaf frontier at reference %d", rootpublication.ErrInvalidStableResource, i)
		}
		if end := ref.Offset + recordLen; end > frontier {
			frontier = end
		}
	}
	return fileID, frontier, nil
}

// registerStableLaneResourceMuHeld flushes and duplicates only the exact
// writer descriptor protected by l.vlogMu. Neither token construction nor its
// later durability operations infer a path or whichever writer is then current.
func (db *DB) registerStableLaneResourceMuHeld(
	l *lane,
	fileID uint32,
	frontier uint64,
	kind rootpublication.ResourceKind,
	spec rootpublication.StableResourceSpec,
) (*rootpublication.StableResourceToken, error) {
	if db == nil || l == nil || db.stableResourcePins == nil {
		return nil, fmt.Errorf("cachingdb: stable resource registry unavailable")
	}
	w, ok := l.vlog.(stableResourceValueWriter)
	if !ok || w == nil || w.OpenFile() == nil {
		return nil, fmt.Errorf("%w: value-log writer does not expose an open descriptor", osadapter.ErrInvalidOpenHandle)
	}
	if w.FileID() != fileID {
		return nil, fmt.Errorf("%w: referenced file id %d is no longer current writer %d", rootpublication.ErrResourceConflict, fileID, w.FileID())
	}
	if err := w.Flush(); err != nil {
		return nil, fmt.Errorf("cachingdb: flush stable writer %d: %w", fileID, err)
	}
	file := w.OpenFile()
	identity, err := osadapter.ResourceIdentity(file)
	if err != nil {
		return nil, err
	}
	if err := db.stableResourcePins.Pin(identity); err != nil {
		return nil, fmt.Errorf("cachingdb: reserve stable writer %d: %w", fileID, err)
	}

	var reservationMu sync.Mutex
	reserved := true
	releaseReservation := func() error {
		reservationMu.Lock()
		defer reservationMu.Unlock()
		if !reserved {
			return nil
		}
		reserved = false
		return db.stableResourcePins.Release(identity)
	}
	defer func() { _ = releaseReservation() }()

	if spec.Kind != "" && spec.Kind != kind {
		return nil, fmt.Errorf("%w: producer kind %s conflicts with requested kind %s", rootpublication.ErrResourceConflict, kind, spec.Kind)
	}
	spec.Kind = kind
	spec.RequiredFrontier = frontier
	if spec.LogicalNamespace == "" {
		spec.LogicalNamespace = fmt.Sprintf("value-log/lane-%d", l.id)
		if kind == rootpublication.ResourceOuterLeafSegment {
			spec.LogicalNamespace = fmt.Sprintf("outer-leaf/lane-%d", l.id)
		}
	}
	if spec.ResourceID == "" {
		spec.ResourceID = fmt.Sprintf("segment/%d", fileID)
	}
	if spec.Generation == 0 {
		spec.Generation = uint64(l.vlogSeq)
	}
	diagnosticRoot := db.dir
	if filepath.Base(diagnosticRoot) == "wal" {
		diagnosticRoot = filepath.Dir(diagnosticRoot)
	}
	rel, relErr := filepath.Rel(diagnosticRoot, l.vlogPath)
	if relErr != nil || rel == "." || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("%w: value-log path %q is outside DB root %q", rootpublication.ErrInvalidStableResource, l.vlogPath, diagnosticRoot)
	}
	spec.DiagnosticPath = rel

	hooks := osadapter.ResourceHooks{
		FlushThrough: func(retained *os.File, required uint64) error {
			info, statErr := retained.Stat()
			if statErr != nil {
				return statErr
			}
			if info.Size() < 0 || uint64(info.Size()) < required {
				return fmt.Errorf("%w: required=%d length=%d", osadapter.ErrFrontierNotFlushed, required, info.Size())
			}
			return nil
		},
		Pin: func(got rootpublication.StableIdentity) error {
			reservationMu.Lock()
			defer reservationMu.Unlock()
			if got != identity || !reserved {
				return rootpublication.ErrResourceConflict
			}
			reserved = false
			return nil
		},
		Release: func(got rootpublication.StableIdentity) error {
			if got != identity {
				return rootpublication.ErrResourceConflict
			}
			return db.stableResourcePins.Release(identity)
		},
	}
	token, err := osadapter.RegisterResourceToken(file, hooks, spec)
	if err != nil {
		return nil, err
	}
	return token, nil
}
