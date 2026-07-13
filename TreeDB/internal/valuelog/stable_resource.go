package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var newValueLogStableNamespaceToken = rootpublication.NewStableNamespaceToken
var openStableValueLogParent = os.Open

type pendingValueLogSuccessor struct {
	parent          *os.File
	file            *os.File
	path            string
	fileID          uint32
	active          StableResourceRegistration
	observerEmitted bool
	failStop        bool
}

func cloneStableResourceRegistration(registration StableResourceRegistration) StableResourceRegistration {
	registration.ExternalRIDs = append([]uint64(nil), registration.ExternalRIDs...)
	return registration
}

func sameStableResourceRegistration(left, right StableResourceRegistration) bool {
	return left.Kind == right.Kind && left.LogicalLane == right.LogicalLane &&
		left.Generation == right.Generation && left.DiagnosticPath == right.DiagnosticPath &&
		left.Reachability == right.Reachability && left.Digest == right.Digest &&
		left.ParentGeneration == right.ParentGeneration && left.NamespaceOperation == right.NamespaceOperation &&
		left.OldName == right.OldName && left.NewName == right.NewName &&
		left.NamespaceParent == right.NamespaceParent && slices.Equal(left.ExternalRIDs, right.ExternalRIDs)
}

func normalizeStableValueLogActiveRegistration(parent *os.File, path string, active StableResourceRegistration) (StableResourceRegistration, error) {
	if active.NamespaceParent != nil && active.NamespaceParent != parent {
		return StableResourceRegistration{}, fmt.Errorf("%w: valuelog active parent differs from retained writer parent", rootpublication.ErrResourceConflict)
	}
	active.NamespaceParent = parent
	name := filepath.Base(path)
	if active.NewName != "" && active.NewName != name {
		return StableResourceRegistration{}, fmt.Errorf("%w: valuelog active namespace name %q differs from %q", rootpublication.ErrResourceConflict, active.NewName, name)
	}
	active.NewName = name
	return cloneStableResourceRegistration(active), nil
}

func captureStableValueLogParent(path string, resource *os.File) (*os.File, error) {
	parent, err := openStableValueLogParent(filepath.Dir(path))
	if err != nil {
		if parent != nil {
			_ = parent.Close()
		}
		return nil, err
	}
	if err := rootpublication.ValidateStableChildLink(parent, resource, filepath.Base(path)); err != nil {
		_ = parent.Close()
		return nil, err
	}
	return parent, nil
}

// replaceStableValueLogParent transfers ownership of parent to w and releases
// the previously retained directory handle. A capture failure is retained so
// ordinary rotation remains usable while every later stable rotation fails
// closed instead of falling back to an older namespace identity.
func (w *Writer) replaceStableValueLogParent(parent *os.File, captureErr error) {
	old := w.stableParent
	w.stableParent = parent
	w.stableParentErr = captureErr
	if old != nil && old != parent {
		_ = old.Close()
	}
}

// NewStableValueLogResourceToken registers an exact already-open persistent
// value-log handle, including the external-RID fence view used by command WAL.
func NewStableValueLogResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	switch spec.Reachability {
	case rootpublication.ReachabilityValueLogPointer:
		return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerValueLog, spec, "authoritative")
	case rootpublication.ReachabilityCommandWALExternalRIDFence:
		return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerValueLog, spec, "authoritative-transitive")
	default:
		return nil, fmt.Errorf("%w: value-log producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
}

// NewStableOuterLeafRawResourceToken registers the exact raw outer-leaf log
// handle before its active lane can rotate.
func NewStableOuterLeafRawResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if spec.Reachability != rootpublication.ReachabilityOuterLeafRawPointer {
		return nil, fmt.Errorf("%w: raw outer-leaf producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
	return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerOuterLeaf, spec, "authoritative")
}

// StableResourceRegistration describes the logical placement and immutable
// metadata attached to the writer's already-open segment. DiagnosticPath is
// informational and must be relative to the DB root; identity is always read
// from the pinned file handle.
type StableResourceRegistration struct {
	Kind               rootpublication.ResourceKind
	LogicalLane        string
	Generation         uint64
	DiagnosticPath     string
	Reachability       rootpublication.ReachabilityField
	Digest             [32]byte
	ExternalRIDs       []uint64
	ParentGeneration   uint64
	NamespaceOperation rootpublication.NamespaceOperation
	OldName            string
	NewName            string
	NamespaceParent    *os.File
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
	return stableValueLogResourceToken(w.f, w.fileID, registration, namespace, false)
}

func stableValueLogResourceToken(file *os.File, fileID uint32, registration StableResourceRegistration, namespace *rootpublication.StableNamespaceToken, contentSynced bool) (*rootpublication.StableResourceToken, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	frontier := rootpublication.NewRIDFrontier(registration.ExternalRIDs)
	frontier.Bytes = uint64(info.Size())
	domain, kind, _, err := stableValueLogProducerPolicy(registration.Reachability)
	if err != nil {
		return nil, err
	}
	if registration.Kind != "" && registration.Kind != kind {
		return nil, fmt.Errorf("%w: valuelog field %q requires kind %q, got %q", rootpublication.ErrResourceConflict, registration.Reachability, kind, registration.Kind)
	}
	spec := rootpublication.StableResourceSpec{
		Kind: kind, LogicalLane: registration.LogicalLane,
		ResourceID: strconv.FormatUint(uint64(fileID), 10), Generation: registration.Generation,
		DiagnosticPath: registration.DiagnosticPath, File: file, Frontier: frontier,
		Digest: registration.Digest, Reachability: registration.Reachability, Namespace: namespace,
		ContentSynced: contentSynced,
	}
	switch domain {
	case rootpublication.StableProducerValueLog:
		return NewStableValueLogResourceToken(spec)
	case rootpublication.StableProducerOuterLeaf:
		return NewStableOuterLeafRawResourceToken(spec)
	default:
		return nil, fmt.Errorf("%w: unsupported valuelog producer domain %q", rootpublication.ErrUnresolvedResource, domain)
	}
}

// stableValueLogProducerPolicy is deliberately independent of the generic
// inventory lookup. This writer family may pin only its three exact-handle
// products; a foreign registerable field must fail instead of echoing that
// field's generic policy back into the checked constructor.
func stableValueLogProducerPolicy(field rootpublication.ReachabilityField) (rootpublication.StableProducerDomain, rootpublication.ResourceKind, string, error) {
	switch field {
	case rootpublication.ReachabilityValueLogPointer:
		return rootpublication.StableProducerValueLog, rootpublication.ResourceValueLog, "authoritative", nil
	case rootpublication.ReachabilityOuterLeafRawPointer:
		return rootpublication.StableProducerOuterLeaf, rootpublication.ResourceOuterLeafLog, "authoritative", nil
	case rootpublication.ReachabilityCommandWALExternalRIDFence:
		return rootpublication.StableProducerValueLog, rootpublication.ResourceCommandWALExternalRID, "authoritative-transitive", nil
	default:
		return "", "", "", fmt.Errorf("%w: valuelog producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, field)
	}
}

func stableValueLogNamespaceToken(file *os.File, registration StableResourceRegistration) (*rootpublication.StableNamespaceToken, error) {
	if registration.NamespaceOperation == rootpublication.NamespaceNone {
		return nil, nil
	}
	if registration.NamespaceParent == nil {
		return nil, fmt.Errorf("%w: valuelog namespace operation requires exact parent handle", rootpublication.ErrUnresolvedResource)
	}
	newName := registration.NewName
	if newName == "" {
		if registration.NamespaceOperation == rootpublication.NamespaceRename {
			return nil, fmt.Errorf("%w: valuelog rename requires exact new name", rootpublication.ErrUnresolvedResource)
		}
		newName = filepath.Base(file.Name())
	}
	if filepath.Base(newName) != newName {
		return nil, fmt.Errorf("%w: valuelog namespace name must be a base name", rootpublication.ErrUnresolvedResource)
	}
	namespace, err := newValueLogStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: registration.NamespaceParent, LinkedResource: file, ParentGeneration: registration.ParentGeneration,
		Operation: registration.NamespaceOperation, OldName: registration.OldName,
		NewName: newName, DiagnosticPath: filepath.Dir(registration.DiagnosticPath),
	})
	if err != nil {
		return nil, err
	}
	if err := namespace.Stabilize(); err != nil {
		namespace.Release()
		return nil, err
	}
	return namespace, nil
}

func openStableValueLogFile(parent *os.File, path string) (*os.File, error) {
	return rootpublication.OpenStableChildFile(parent, filepath.Base(path), os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0o600)
}

func pendingValueLogFailure(parent, file *os.File, path string, err error) error {
	return errors.Join(err, rootpublication.ValidateStableChildLink(parent, file, filepath.Base(path)))
}

// RotateToWithStableResources pins the flushed old segment before writer state
// switches and then pins the newly-created active segment after its namespace
// operation has been made persistent.
func (w *Writer) RotateToWithStableResources(path string, fileID uint32, syncCurrent bool, closed, active StableResourceRegistration) (*StableResourceRotation, error) {
	if w != nil && w.pendingStableSuccessor != nil && w.pendingStableSuccessor.failStop {
		return nil, fmt.Errorf("%w: value-log stable rotation stopped after old-writer close failure", rootpublication.ErrResourceOwnership)
	}
	if w == nil || w.f == nil {
		return nil, errors.New("valuelog: stable rotation requires file-backed writer")
	}
	if active.NamespaceOperation == rootpublication.NamespaceNone {
		return nil, errors.New("valuelog: stable rotation requires active namespace operation")
	}
	if w.stableParentErr != nil || w.stableParent == nil {
		if w.stableParentErr != nil {
			return nil, fmt.Errorf("valuelog: stable parent unavailable: %w", w.stableParentErr)
		}
		return nil, errors.New("valuelog: stable parent unavailable")
	}
	normalizedActive, err := normalizeStableValueLogActiveRegistration(w.stableParent, path, active)
	if err != nil {
		return nil, err
	}
	closed = cloneStableResourceRegistration(closed)
	if pending := w.pendingStableSuccessor; pending != nil {
		if pending.parent != w.stableParent || pending.path != path || pending.fileID != fileID ||
			!sameStableResourceRegistration(pending.active, normalizedActive) {
			return nil, errors.Join(rootpublication.ErrResourceOwnership,
				fmt.Errorf("%w: stable value-log retry does not match the pending exact successor", rootpublication.ErrResourceConflict))
		}
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if syncCurrent {
		if err := w.syncFile(); err != nil {
			return nil, err
		}
	}
	closedToken, err := stableValueLogResourceToken(w.f, w.fileID, closed, nil, syncCurrent)
	if err != nil {
		return nil, err
	}
	pending := w.pendingStableSuccessor
	if pending == nil {
		prepared, openErr := openStableValueLogFile(w.stableParent, path)
		if openErr != nil {
			closedToken.Release()
			return nil, openErr
		}
		pending = &pendingValueLogSuccessor{
			parent: w.stableParent, file: prepared, path: path, fileID: fileID,
			active: normalizedActive,
		}
		w.pendingStableSuccessor = pending
	}
	prepared := pending.file
	if !pending.observerEmitted {
		pending.observerEmitted = true
		if observeErr := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceValueLog, filepath.Dir(path), "", path); observeErr != nil {
			closedToken.Release()
			return nil, pendingValueLogFailure(w.stableParent, prepared, path, observeErr)
		}
	}
	preparedInfo, err := prepared.Stat()
	if err != nil {
		closedToken.Release()
		return nil, pendingValueLogFailure(w.stableParent, prepared, path, err)
	}
	namespace, err := stableValueLogNamespaceToken(prepared, normalizedActive)
	if err != nil {
		closedToken.Release()
		return nil, pendingValueLogFailure(w.stableParent, prepared, path, err)
	}
	activeToken, err := stableValueLogResourceToken(prepared, fileID, normalizedActive, namespace, false)
	namespace.Release()
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	old := w.f
	if err := w.closeRotatedResource(old); err != nil {
		activeToken.Release()
		closedToken.Release()
		pending.failStop = true
		w.f = nil
		w.bw = nil
		return nil, fmt.Errorf("valuelog: close old writer during stable rotation: %w", err)
	}
	pending.file = nil
	w.f = prepared
	w.bw = nil
	w.size = preparedInfo.Size()
	w.fileID = fileID
	w.appendMax = defaultBufferSize
	w.appendBuf = w.appendBuf[:0]
	w.trimTransientScratchBuffers()
	w.pendingStableSuccessor = nil
	return &StableResourceRotation{Closed: closedToken, Active: activeToken}, nil
}
