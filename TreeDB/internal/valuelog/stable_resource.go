package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var newValueLogStableNamespaceToken = rootpublication.NewStableNamespaceToken
var openStableValueLogParent = os.Open

// SetStableResourcePinRegistry installs the DB-scoped physical identity gate
// used by every manager that can delete these segment files.
func (m *Manager) SetStableResourcePinRegistry(registry *rootpublication.IdentityPinRegistry) error {
	if m == nil || registry == nil {
		return errors.New("valuelog: nil stable resource pin registry")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stableResourcePins != nil && m.stableResourcePins != registry {
		return errors.New("valuelog: stable resource pin registry already installed")
	}
	if m.stableResourcePins == registry {
		return nil
	}
	observed := make([]*File, 0, len(m.files))
	for _, file := range m.files {
		if err := m.observeStableFileWithRegistryLocked(file, registry); err != nil {
			for _, prior := range observed {
				_ = registry.Unobserve(prior.stableIdentity)
				prior.stableObserved = false
			}
			return err
		}
		observed = append(observed, file)
	}
	m.stableResourcePins = registry
	return nil
}

// StableResourcePinRegistry returns the deletion gate installed before this
// manager's files became eligible for removal.
func (m *Manager) StableResourcePinRegistry() *rootpublication.IdentityPinRegistry {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stableResourcePins
}

// StableSegmentIdentity returns the immutable physical identity captured when
// the manager opened fileID. Callers can carry it across an intentional evict
// and reject a pathname replacement before fallback cleanup.
func (m *Manager) StableSegmentIdentity(fileID uint32) (rootpublication.StableIdentity, bool) {
	if m == nil {
		return rootpublication.StableIdentity{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	file := m.files[fileID]
	if file == nil || file.stableIdentity == (rootpublication.StableIdentity{}) {
		return rootpublication.StableIdentity{}, false
	}
	return file.stableIdentity, true
}

// StableResourceToken captures a checked token from a manager-owned exact
// handle while holding the manager read lock. The registry is supplied by the
// manager and cannot be omitted or substituted by the caller.
func (m *Manager) StableResourceToken(fileID uint32, registration StableResourceRegistration) (*rootpublication.StableResourceToken, error) {
	if m == nil {
		return nil, errors.New("valuelog: nil manager")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	file := m.files[fileID]
	if file == nil {
		return nil, &fileNotFoundError{id: fileID}
	}
	if m.stableResourcePins == nil {
		return nil, fmt.Errorf("%w: manager has no stable identity registry", rootpublication.ErrUnresolvedResource)
	}
	registration.PinRegistry = m.stableResourcePins
	namespace, err := stableValueLogNamespaceToken(file.File, registration)
	if err != nil {
		return nil, err
	}
	defer namespace.Release()
	return stableValueLogResourceToken(file.File, fileID, registration, namespace, false)
}

func (m *Manager) observeStableFileLocked(file *File) error {
	if m == nil || m.stableResourcePins == nil {
		return nil
	}
	return m.observeStableFileWithRegistryLocked(file, m.stableResourcePins)
}

func (m *Manager) observeStableFileWithRegistryLocked(file *File, registry *rootpublication.IdentityPinRegistry) error {
	if file == nil || registry == nil || file.stableObserved {
		return nil
	}
	identity, err := rootpublication.StableIdentityFromFile(file.File)
	if err != nil {
		return err
	}
	if err := registry.Observe(identity); err != nil {
		return err
	}
	namespace, err := stableDeleteNamespace(file.Path)
	if err != nil {
		_ = registry.Unobserve(identity)
		return err
	}
	file.stableIdentity = identity
	file.stableNamespace = namespace
	file.stableObserved = true
	return nil
}

func (m *Manager) unobserveStableFileLocked(file *File) error {
	if m == nil || m.stableResourcePins == nil || file == nil || !file.stableObserved {
		return nil
	}
	file.stableObserved = false
	return m.stableResourcePins.Unobserve(file.stableIdentity)
}

func (m *Manager) stableDeleteLease(file *File) (*rootpublication.IdentityDeleteLease, error) {
	if m == nil || file == nil || m.stableResourcePins == nil {
		return nil, nil
	}
	identity := file.stableIdentity
	if identity == (rootpublication.StableIdentity{}) {
		var err error
		identity, err = rootpublication.StableIdentityFromFile(file.File)
		if err != nil {
			return nil, err
		}
	}
	namespace := file.stableNamespace
	if namespace == "" {
		var err error
		namespace, err = stableDeleteNamespace(file.Path)
		if err != nil {
			return nil, err
		}
	}
	lease, err := m.stableResourcePins.BeginDeleteAt(identity, namespace)
	if errors.Is(err, rootpublication.ErrResourcePinned) {
		return nil, &filePinnedError{id: file.ID, op: "delete stable resource"}
	}
	return lease, err
}

func stableDeleteNamespace(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("%w: empty value-log path", rootpublication.ErrUnresolvedResource)
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	defer parent.Close()
	identity, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d:%x/%s", identity.Platform, identity.VolumeID, identity.ObjectID, filepath.Base(path)), nil
}

func validateStableDeletePath(file *File) error {
	if file == nil || file.File == nil || file.Path == "" {
		return fmt.Errorf("%w: invalid value-log delete target", rootpublication.ErrUnresolvedResource)
	}
	identity := file.stableIdentity
	if identity == (rootpublication.StableIdentity{}) {
		var err error
		identity, err = rootpublication.StableIdentityFromFile(file.File)
		if err != nil {
			return err
		}
	}
	return validateStableDeletePathIdentity(file.Path, identity)
}

func validateStableDeletePathIdentity(path string, identity rootpublication.StableIdentity) error {
	linked, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer linked.Close()
	linkedIdentity, err := rootpublication.StableIdentityFromFile(linked)
	if err != nil {
		return err
	}
	if !rootpublication.SamePhysicalIdentity(identity, linkedIdentity) {
		return fmt.Errorf("%w: value-log path no longer names captured identity", rootpublication.ErrResourceConflict)
	}
	return nil
}

func abortStableDeleteLease(lease *rootpublication.IdentityDeleteLease) {
	if lease != nil {
		lease.Abort()
	}
}

func commitStableDeleteLease(lease *rootpublication.IdentityDeleteLease) {
	if lease != nil {
		lease.Commit()
	}
}

func finishStableDeleteLease(lease *rootpublication.IdentityDeleteLease, deleted bool) {
	if deleted {
		commitStableDeleteLease(lease)
	} else {
		abortStableDeleteLease(lease)
	}
}

// closeAndRemoveStableSegmentFileResult closes the manager's own handle before
// unlink (required on Windows), then revalidates that the path still names the
// identity captured before close. The registry lease excludes process-local
// pin and delete races across this boundary.
func closeAndRemoveStableSegmentFileResult(file *File, validateIdentity bool) (bool, error) {
	if file == nil {
		return true, nil
	}
	identity := file.stableIdentity
	if validateIdentity && identity == (rootpublication.StableIdentity{}) {
		var err error
		identity, err = rootpublication.StableIdentityFromFile(file.File)
		if err != nil {
			return false, err
		}
	}
	closeErr := file.Close()
	var removed bool
	var removeErr error
	if validateIdentity {
		removed, removeErr = removeSegmentFileWithRetryStable(file.Path, identity)
	} else {
		removed, removeErr = removeSegmentFileWithRetry(file.Path)
	}
	return removed, errors.Join(closeErr, removeErr)
}

// removeStableSegmentFileOnce first moves the linked name into a private
// same-directory quarantine, then validates and unlinks that renamed object.
// A replacement created at the original name after the rename is never passed
// to the unlink syscall.
func removeStableSegmentFileOnce(path string, identity rootpublication.StableIdentity) (bool, error) {
	parent := filepath.Dir(path)
	quarantineDir, err := os.MkdirTemp(parent, "."+filepath.Base(path)+".delete-")
	if err != nil {
		return false, err
	}
	quarantinePath := filepath.Join(quarantineDir, filepath.Base(path))
	if err := os.Rename(path, quarantinePath); err != nil {
		_ = os.Remove(quarantineDir)
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	restore := func() error {
		if err := os.Link(quarantinePath, path); err != nil {
			return err
		}
		return os.Remove(quarantinePath)
	}
	if err := validateStableDeletePathIdentity(quarantinePath, identity); err != nil {
		restoreErr := restore()
		if restoreErr == nil {
			restoreErr = os.Remove(quarantineDir)
		}
		return false, errors.Join(err, restoreErr)
	}
	removeErr := removeSegmentPath(quarantinePath)
	if removeErr != nil && !os.IsNotExist(removeErr) {
		if restoreErr := restore(); restoreErr != nil {
			return false, fmt.Errorf("%w: stable delete failed (%v) and quarantine restore failed: %v", rootpublication.ErrResourceConflict, removeErr, restoreErr)
		}
		_ = os.Remove(quarantineDir)
		return false, removeErr
	}
	cleanupErr := os.Remove(quarantineDir)
	cutErr := durabilitycut.EmitNamespace(durabilitycut.NamespaceUnlink, durabilitycut.ResourceValueLog, parent, path, "")
	return true, errors.Join(cleanupErr, cutErr)
}

func (m *Manager) deleteZombieFile(file *File) error {
	if m == nil || file == nil {
		return nil
	}
	m.mu.Lock()
	current, exists := m.files[file.ID]
	if !exists || current != file || file.RefCount.Load() != 0 || !file.IsZombie.Load() {
		m.mu.Unlock()
		return nil
	}
	lease, err := m.stableDeleteLease(file)
	if errors.Is(err, ErrFilePinned) {
		m.mu.Unlock()
		if file.retryDeletePending.CompareAndSwap(false, true) {
			go m.retryZombieDelete(file)
		}
		return nil
	}
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if lease != nil {
		if err := validateStableDeletePath(file); err != nil {
			abortStableDeleteLease(lease)
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Unlock()

	deleted, unlinkErr := closeAndRemoveStableSegmentFileResult(file, lease != nil)
	if !deleted {
		abortStableDeleteLease(lease)
		if isWindowsSharingViolationError(unlinkErr) && file.retryDeletePending.CompareAndSwap(false, true) {
			go m.retryZombieDelete(file)
			return nil
		}
		return unlinkErr
	}
	commitStableDeleteLease(lease)
	m.mu.Lock()
	if current, exists := m.files[file.ID]; exists && current == file && file.RefCount.Load() == 0 && file.IsZombie.Load() {
		delete(m.files, file.ID)
		_ = m.unobserveStableFileLocked(file)
	}
	m.mu.Unlock()
	return unlinkErr
}

func captureStableValueLogParent(path string, resource *os.File) (*os.File, error) {
	parent, err := openStableValueLogParent(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if err := rootpublication.ValidateStableChildLink(parent, resource, filepath.Base(path)); err != nil {
		_ = parent.Close()
		return nil, err
	}
	return parent, nil
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
	PinRegistry        *rootpublication.IdentityPinRegistry
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
		ContentSynced: contentSynced, PinRegistry: registration.PinRegistry,
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
	created, err := rootpublication.OpenStableChildFile(parent, filepath.Base(path), os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	if observeErr := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceValueLog, filepath.Dir(path), "", path); observeErr != nil {
		_ = created.Close()
		return nil, observeErr
	}
	return created, nil
}

// RotateToWithStableResources pins the flushed old segment before writer state
// switches and then pins the newly-created active segment after its namespace
// operation has been made persistent.
func (w *Writer) RotateToWithStableResources(path string, fileID uint32, syncCurrent bool, closed, active StableResourceRegistration) (_ *StableResourceRotation, retErr error) {
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
	closedToken, err := stableValueLogResourceToken(w.f, w.fileID, closed, nil, syncCurrent)
	if err != nil {
		return nil, err
	}
	if w.stableParentErr != nil || w.stableParent == nil {
		closedToken.Release()
		if w.stableParentErr != nil {
			return nil, fmt.Errorf("valuelog: stable parent unavailable: %w", w.stableParentErr)
		}
		return nil, errors.New("valuelog: stable parent unavailable")
	}
	prepared, err := openStableValueLogFile(w.stableParent, path)
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	installed := false
	defer func() {
		if installed {
			return
		}
		validateErr := rootpublication.ValidateStableChildLink(w.stableParent, prepared, filepath.Base(path))
		closeErr := prepared.Close()
		var removeErr, syncErr error
		if validateErr == nil {
			removeErr = rootpublication.RemoveStableChildFile(w.stableParent, filepath.Base(path))
			syncErr = w.stableParent.Sync()
		}
		retErr = errors.Join(retErr, validateErr, closeErr, removeErr, syncErr)
	}()
	preparedInfo, err := prepared.Stat()
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	if active.NamespaceParent != nil && active.NamespaceParent != w.stableParent {
		closedToken.Release()
		return nil, fmt.Errorf("%w: valuelog active parent differs from retained writer parent", rootpublication.ErrResourceConflict)
	}
	active.NamespaceParent = w.stableParent
	if active.NewName != "" && active.NewName != filepath.Base(path) {
		closedToken.Release()
		return nil, fmt.Errorf("%w: valuelog active namespace name %q differs from %q", rootpublication.ErrResourceConflict, active.NewName, filepath.Base(path))
	}
	active.NewName = filepath.Base(path)
	namespace, err := stableValueLogNamespaceToken(prepared, active)
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	activeToken, err := stableValueLogResourceToken(prepared, fileID, active, namespace, false)
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
	installed = true
	return &StableResourceRotation{Closed: closedToken, Active: activeToken}, nil
}
