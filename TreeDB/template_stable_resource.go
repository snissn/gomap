package treedb

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// acquireStableTemplateSnapshot exposes only the pinned physical view needed
// by templatedb. The adapter deliberately retains the real backend rather than
// reconstructing authority from public lookup bytes.
func acquireStableTemplateSnapshot(backend *db.DB) *templateStableSnapshot {
	if backend == nil {
		return nil
	}
	snapshot := backend.AcquireStableSnapshot()
	if snapshot == nil {
		return nil
	}
	return &templateStableSnapshot{snapshot: snapshot, dir: backend.Dir()}
}

type templateStableSnapshot struct {
	snapshot            *db.Snapshot
	dir                 string
	captureLeaseOnce    sync.Once
	captureLeaseRelease func()
}

func (snapshot *templateStableSnapshot) Get(key []byte) ([]byte, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, fmt.Errorf("%w: templatedb stable snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	return snapshot.snapshot.Get(key)
}

func (snapshot *templateStableSnapshot) GetEntry(key []byte) (templatedb.StablePhysicalEntry, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return templatedb.StablePhysicalEntry{}, fmt.Errorf("%w: templatedb stable snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	entry, err := snapshot.snapshot.GetEntryExact(key)
	if err != nil {
		return templatedb.StablePhysicalEntry{}, err
	}
	recordLength := uint64(page.ValuePtrRecordLength(entry.ValuePtr))
	if entry.Flags&node.FlagPointer != 0 && recordLength == 0 {
		resolved, err := snapshot.snapshot.StableValueLogRecordLength(entry.ValuePtr)
		if err != nil {
			return templatedb.StablePhysicalEntry{}, err
		}
		recordLength = uint64(resolved)
	}
	return templatedb.StablePhysicalEntry{
		Pointer:      entry.Flags&node.FlagPointer != 0,
		FileID:       entry.ValuePtr.FileID,
		Offset:       entry.ValuePtr.Offset,
		RecordLength: recordLength,
	}, nil
}

func (snapshot *templateStableSnapshot) ValueLogDiagnosticPath(fileID uint32) (string, error) {
	if snapshot == nil || snapshot.snapshot == nil || snapshot.dir == "" {
		return "", fmt.Errorf("%w: templatedb value-log namespace unavailable", rootpublication.ErrUnresolvedResource)
	}
	state := snapshot.snapshot.State()
	if state == nil || state.ValueLogSet == nil {
		return "", fmt.Errorf("%w: templatedb value-log set unavailable", rootpublication.ErrUnresolvedResource)
	}
	segment := state.ValueLogSet.Files[fileID]
	if segment == nil || segment.File == nil || segment.Path == "" {
		return "", fmt.Errorf("%w: templatedb value-log file %d unavailable", rootpublication.ErrUnresolvedResource, fileID)
	}
	diagnosticPath, err := filepath.Rel(snapshot.dir, segment.Path)
	if err != nil || diagnosticPath == "." || filepath.IsAbs(diagnosticPath) ||
		diagnosticPath == ".." || strings.HasPrefix(diagnosticPath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: templatedb value-log file %d escapes namespace", rootpublication.ErrUnresolvedResource, fileID)
	}
	return diagnosticPath, nil
}

func (snapshot *templateStableSnapshot) NewStableIndexResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, fmt.Errorf("%w: templatedb stable snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	return snapshot.snapshot.NewStableIndexResourceToken(spec, templatedb.NewStableTemplateResourceToken)
}

func (snapshot *templateStableSnapshot) NewStableValueLogResourceToken(fileID uint32, spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return nil, fmt.Errorf("%w: templatedb stable snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	return snapshot.snapshot.NewStableValueLogPhysicalResourceToken(fileID, spec, templatedb.NewStableTemplateResourceToken)
}

func (snapshot *templateStableSnapshot) ReleaseCaptureLease() {
	if snapshot == nil {
		return
	}
	snapshot.captureLeaseOnce.Do(func() {
		if snapshot.captureLeaseRelease != nil {
			snapshot.captureLeaseRelease()
		}
	})
}

func (snapshot *templateStableSnapshot) Close() error {
	if snapshot == nil || snapshot.snapshot == nil {
		if snapshot != nil {
			snapshot.ReleaseCaptureLease()
		}
		return nil
	}
	err := snapshot.snapshot.Close()
	snapshot.ReleaseCaptureLease()
	return err
}
