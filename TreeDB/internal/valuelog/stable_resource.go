package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var newValueLogStableNamespaceToken = rootpublication.NewStableNamespaceToken

// StableResourceRegistration describes the logical placement and immutable
// metadata attached to the writer's already-open segment. DiagnosticPath is
// informational and must be relative to the DB root; identity is always read
// from the pinned file handle.
type StableResourceRegistration struct {
	LogicalLane        string
	Generation         uint64
	DiagnosticPath     string
	Reachability       rootpublication.ReachabilityField
	Digest             [32]byte
	ExternalRIDs       []uint64
	ParentGeneration   uint64
	NamespaceOperation rootpublication.NamespaceOperation
	OldName            string
}

// StableResourceRotation retains exact handles for the segment closed by a
// rotation and the new active segment. Callers transfer each token into a
// candidate builder or release the unused remainder.
type StableResourceRotation struct {
	Closed *rootpublication.StableResourceToken
	Active *rootpublication.StableResourceToken
}

func (rotation *StableResourceRotation) TakeClosed() *rootpublication.StableResourceToken {
	if rotation == nil {
		return nil
	}
	token := rotation.Closed
	rotation.Closed = nil
	return token
}

func (rotation *StableResourceRotation) TakeActive() *rootpublication.StableResourceToken {
	if rotation == nil {
		return nil
	}
	token := rotation.Active
	rotation.Active = nil
	return token
}

func (rotation *StableResourceRotation) Release() {
	if rotation == nil {
		return
	}
	rotation.Closed.Release()
	rotation.Active.Release()
	rotation.Closed = nil
	rotation.Active = nil
}

// StableResourceToken flushes accepted bytes and captures a duplicate of the
// current file descriptor. It does not fsync the file; publication owns the
// later FlushThrough/SyncThrough boundary on the token.
func (w *Writer) StableResourceToken(registration StableResourceRegistration) (*rootpublication.StableResourceToken, error) {
	if w == nil || w.f == nil {
		return nil, errors.New("valuelog: stable resource requires file-backed writer")
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return w.stableResourceTokenAfterFlush(registration)
}

func (w *Writer) stableResourceTokenAfterFlush(registration StableResourceRegistration) (*rootpublication.StableResourceToken, error) {
	if w == nil || w.f == nil {
		return nil, errors.New("valuelog: stable resource requires file-backed writer")
	}
	namespace, err := stableValueLogNamespaceToken(w.f, registration)
	if err != nil {
		return nil, err
	}
	defer namespace.Release()
	return stableValueLogResourceToken(w.f, w.fileID, registration, namespace)
}

func stableValueLogResourceToken(file *os.File, fileID uint32, registration StableResourceRegistration, namespace *rootpublication.StableNamespaceToken) (*rootpublication.StableResourceToken, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	frontier := rootpublication.NewRIDFrontier(registration.ExternalRIDs)
	frontier.Bytes = uint64(info.Size())
	return rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceValueLog, LogicalLane: registration.LogicalLane,
		ResourceID: strconv.FormatUint(uint64(fileID), 10), Generation: registration.Generation,
		DiagnosticPath: registration.DiagnosticPath, File: file, Frontier: frontier,
		Digest: registration.Digest, Reachability: registration.Reachability, Namespace: namespace,
	})
}

func stableValueLogNamespaceToken(file *os.File, registration StableResourceRegistration) (*rootpublication.StableNamespaceToken, error) {
	if registration.NamespaceOperation == rootpublication.NamespaceNone {
		return nil, nil
	}
	parent, err := os.Open(filepath.Dir(file.Name()))
	if err != nil {
		return nil, err
	}
	namespace, err := newValueLogStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, ParentGeneration: registration.ParentGeneration,
		Operation: registration.NamespaceOperation, OldName: registration.OldName,
		NewName: filepath.Base(file.Name()), DiagnosticPath: filepath.Dir(registration.DiagnosticPath),
	})
	_ = parent.Close()
	if err != nil {
		return nil, err
	}
	if err := namespace.Stabilize(); err != nil {
		namespace.Release()
		return nil, err
	}
	return namespace, nil
}

// RotateToWithStableResources pins the flushed old segment before writer state
// switches and then pins the newly-created active segment after its namespace
// operation has been made persistent.
func (w *Writer) RotateToWithStableResources(path string, fileID uint32, syncCurrent bool, closed, active StableResourceRegistration) (*StableResourceRotation, error) {
	if w == nil || w.f == nil {
		return nil, errors.New("valuelog: stable rotation requires file-backed writer")
	}
	if active.NamespaceOperation == rootpublication.NamespaceNone {
		return nil, errors.New("valuelog: stable rotation requires active namespace operation")
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if syncCurrent {
		if err := w.syncFile(); err != nil {
			return nil, err
		}
	}
	closedToken, err := stableValueLogResourceToken(w.f, w.fileID, closed, nil)
	if err != nil {
		return nil, err
	}
	prepared, err := openLogFile(path)
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	closePrepared := true
	defer func() {
		if closePrepared {
			_ = prepared.Close()
		}
	}()
	preparedInfo, err := prepared.Stat()
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	namespace, err := stableValueLogNamespaceToken(prepared, active)
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	activeToken, err := stableValueLogResourceToken(prepared, fileID, active, namespace)
	namespace.Release()
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	old := w.f
	if err := old.Close(); err != nil {
		activeToken.Release()
		closedToken.Release()
		w.f = nil
		w.bw = nil
		return nil, fmt.Errorf("valuelog: close old writer during stable rotation: %w", err)
	}
	w.f = prepared
	w.bw = nil
	w.size = preparedInfo.Size()
	w.fileID = fileID
	w.appendMax = defaultBufferSize
	w.appendBuf = w.appendBuf[:0]
	w.trimTransientScratchBuffers()
	closePrepared = false
	return &StableResourceRotation{Closed: closedToken, Active: activeToken}, nil
}
