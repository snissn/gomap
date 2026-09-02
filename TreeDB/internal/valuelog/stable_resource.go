package valuelog

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

var newValueLogStableNamespaceToken = rootpublication.NewStableNamespaceToken
var newValueLogStableNamespaceCreationProof = rootpublication.NewStableNamespaceCreationProof
var bindValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, parentGeneration uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
	return proof.Bind(parent, parentGeneration, name, diagnosticPath)
}

// Keep retained-current and pending-successor binding as separate seams so
// failure tests can target either ownership phase without perturbing the other.
var bindRetainedValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, parentGeneration uint64, name, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
	return proof.Bind(parent, parentGeneration, name, diagnosticPath)
}
var openStableValueLogParent = rootpublication.OpenStableParent

type pendingValueLogSuccessor struct {
	parent          *os.File
	file            *os.File
	path            string
	fileID          uint32
	active          StableResourceRegistration
	stableIdentity  rootpublication.StableIdentity
	stableObserved  bool
	observerEmitted bool
	creationProof   *rootpublication.StableNamespaceCreationProof
	failStop        bool
}

func (w *Writer) newStableCreationProof(parent, child *os.File, path string) (*rootpublication.StableNamespaceCreationProof, error) {
	resource := segmentNamespaceResource(path)
	dir := filepath.Dir(path)
	if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, resource, "", dir); err != nil {
		return nil, err
	}
	started := time.Now()
	proof, err := newValueLogStableNamespaceCreationProof(parent, child, filepath.Base(path))
	w.directorySyncCalls.Add(1)
	if ns := time.Since(started).Nanoseconds(); ns > 0 {
		w.directorySyncNs.Add(uint64(ns))
	}
	if err != nil {
		w.directorySyncErrors.Add(1)
		return nil, err
	}
	return proof, durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, resource, "", dir)
}

func (w *Writer) prepareStableCreationProof(parent, child *os.File, path string, created, syncDirectory, retainProof bool) (*rootpublication.StableNamespaceCreationProof, bool, bool, error) {
	if created && retainProof && syncDirectory {
		if rootpublication.StableRelativeNamespaceSupported() {
			proof, err := w.newStableCreationProof(parent, child, path)
			return proof, false, false, err
		}
		if err := syncNewFileDirectory(w, path); err != nil {
			return nil, true, false, err
		}
		return nil, true, false, nil
	}
	if created && retainProof && !syncDirectory {
		return nil, false, true, nil
	}
	if syncDirectory {
		return nil, false, false, syncNewFileDirectory(w, path)
	}
	return nil, false, false, nil
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
		left.NamespaceParent == right.NamespaceParent && left.PinRegistry == right.PinRegistry &&
		slices.Equal(left.ExternalRIDs, right.ExternalRIDs)
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

func observeStableWriterFile(file *os.File, registry *rootpublication.IdentityPinRegistry) (rootpublication.StableIdentity, error) {
	if registry == nil {
		return rootpublication.StableIdentity{}, nil
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	if err := registry.Observe(identity); err != nil {
		return rootpublication.StableIdentity{}, err
	}
	return identity, nil
}

func (w *Writer) observeStableResourceFile(file *os.File, registry *rootpublication.IdentityPinRegistry) error {
	if w == nil || registry == nil {
		return nil
	}
	identity, err := observeStableWriterFile(file, registry)
	if err != nil {
		return err
	}
	w.stableResourcePins = registry
	w.stableResourceIdentity = identity
	w.stableResourceObserved = true
	return nil
}

func (w *Writer) releaseStableResourceObservation() error {
	if w == nil || !w.stableResourceObserved || w.stableResourcePins == nil {
		return nil
	}
	err := w.stableResourcePins.Unobserve(w.stableResourceIdentity)
	w.stableResourceIdentity = rootpublication.StableIdentity{}
	w.stableResourceObserved = false
	return err
}

func (w *Writer) bindStableResourcePinRegistry(registration *StableResourceRegistration) error {
	if w == nil || registration == nil {
		return nil
	}
	if registration.PinRegistry != nil {
		if w.stableResourcePins == nil || !w.stableResourceObserved {
			return fmt.Errorf("%w: writer registry must be installed before the file becomes deletable", rootpublication.ErrUnresolvedResource)
		}
		if registration.PinRegistry != w.stableResourcePins {
			return fmt.Errorf("%w: writer stable identity registry differs from registration", rootpublication.ErrResourceConflict)
		}
		return nil
	}
	if w.stableResourcePins != nil {
		registration.PinRegistry = w.stableResourcePins
	}
	return nil
}

// SetStableResourcePinRegistry only accepts the registry already installed by
// the constructor. Installing a registry after the initial directory scan can
// resurrect an identity another manager already committed as deleted.
func (m *Manager) SetStableResourcePinRegistry(registry *rootpublication.IdentityPinRegistry) error {
	if m == nil || registry == nil {
		return errors.New("valuelog: nil stable resource pin registry")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stableResourcePins == nil {
		return fmt.Errorf("%w: stable resource pin registry must be installed at manager construction", rootpublication.ErrUnresolvedResource)
	}
	if m.stableResourcePins != registry {
		return errors.New("valuelog: stable resource pin registry already installed")
	}
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
	return m.stableResourceTokenLocked(fileID, registration)
}

func (m *Manager) stableResourceTokenLocked(fileID uint32, registration StableResourceRegistration) (*rootpublication.StableResourceToken, error) {
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

// StableExternalRIDSegment describes the exact value-log child required by an
// active command-WAL V2 external-RID fence. Pointers are positionally aligned
// with RIDs and are validated through the manager-owned open handle before the
// child is pinned. This producer-side closure is intentionally separate from
// the still-dormant typed CommandEnvelope.ExternalRefs section.
type StableExternalRIDSegment struct {
	FileID   uint32
	RIDs     []uint64
	Pointers []page.ValuePtr
	Digest   [32]byte
}

// StableExternalRIDFence is the independently derived command-payload fence
// that a physical value-log closure must satisfy before V2 command-WAL append.
type StableExternalRIDFence struct {
	Count  uint32
	Digest [32]byte
}

// NewStableExternalRIDFence canonicalizes the command payload's complete RID
// set without accepting any physical segment assignment.
func NewStableExternalRIDFence(rids []uint64) (StableExternalRIDFence, error) {
	canonical, err := canonicalStableExternalRIDs(rids)
	if err != nil {
		return StableExternalRIDFence{}, err
	}
	if uint64(len(canonical)) > uint64(^uint32(0)) {
		return StableExternalRIDFence{}, fmt.Errorf("%w: external-RID fence exceeds uint32 count", rootpublication.ErrResourceConflict)
	}
	h := sha256.New()
	var encoded [8]byte
	for _, rid := range canonical {
		binary.LittleEndian.PutUint64(encoded[:], rid)
		_, _ = h.Write(encoded[:])
	}
	var fence StableExternalRIDFence
	fence.Count = uint32(len(canonical))
	copy(fence.Digest[:], h.Sum(nil))
	return fence, nil
}

func canonicalStableExternalRIDs(rids []uint64) ([]uint64, error) {
	canonical := append([]uint64(nil), rids...)
	for _, rid := range canonical {
		if rid == 0 {
			return nil, fmt.Errorf("%w: external-RID fence contains zero RID", rootpublication.ErrUnresolvedResource)
		}
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i] < canonical[j] })
	unique := canonical[:0]
	for _, rid := range canonical {
		if len(unique) == 0 || unique[len(unique)-1] != rid {
			unique = append(unique, rid)
		}
	}
	if len(unique) == 0 {
		return nil, fmt.Errorf("%w: external-RID fence has no RIDs", rootpublication.ErrUnresolvedResource)
	}
	return unique, nil
}

// CaptureStableExternalRIDFence captures every manager-owned value-log child
// needed by the independently derived fence. The manager fixes the producer,
// reachability field, exact handles, and shared deletion gate; callers cannot
// omit a segment/RID, substitute paths, or provide identity overrides.
func (m *Manager) CaptureStableExternalRIDFence(fence StableExternalRIDFence, segments []StableExternalRIDSegment) (*rootpublication.StableResourceSet, error) {
	if m == nil || fence.Count == 0 || len(segments) == 0 {
		return nil, fmt.Errorf("%w: external-RID fence has no value-log children", rootpublication.ErrUnresolvedResource)
	}
	ordered := append([]StableExternalRIDSegment(nil), segments...)
	for i := range ordered {
		ordered[i].RIDs = append([]uint64(nil), ordered[i].RIDs...)
		ordered[i].Pointers = append([]page.ValuePtr(nil), ordered[i].Pointers...)
		if ordered[i].FileID == 0 || len(ordered[i].RIDs) == 0 || len(ordered[i].Pointers) != len(ordered[i].RIDs) {
			return nil, fmt.Errorf("%w: external-RID child has empty file ID or mismatched RID/pointer set", rootpublication.ErrUnresolvedResource)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].FileID < ordered[j].FileID })
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].FileID == ordered[i].FileID {
			return nil, fmt.Errorf("%w: duplicate external-RID value-log child %d", rootpublication.ErrResourceConflict, ordered[i].FileID)
		}
	}
	physicalRIDs := make([]uint64, 0)
	ridOwners := make(map[uint64]uint32)
	for _, child := range ordered {
		for _, rid := range child.RIDs {
			if owner, exists := ridOwners[rid]; exists && owner != child.FileID {
				return nil, fmt.Errorf("%w: external RID %d is assigned to value-log children %d and %d", rootpublication.ErrResourceConflict, rid, owner, child.FileID)
			}
			ridOwners[rid] = child.FileID
			physicalRIDs = append(physicalRIDs, rid)
		}
	}
	actualFence, err := NewStableExternalRIDFence(physicalRIDs)
	if err != nil {
		return nil, err
	}
	if actualFence != fence {
		return nil, fmt.Errorf("%w: external-RID physical closure count=%d digest=%x does not satisfy expected count=%d digest=%x", rootpublication.ErrUnresolvedResource, actualFence.Count, actualFence.Digest, fence.Count, fence.Digest)
	}

	files := make([]*File, len(ordered))
	m.mu.RLock()
	for i, child := range ordered {
		file := m.files[child.FileID]
		if file == nil {
			m.mu.RUnlock()
			return nil, fmt.Errorf("external-RID value-log child %d: %w", child.FileID, &fileNotFoundError{id: child.FileID})
		}
		files[i] = file
	}
	m.mu.RUnlock()
	for i, child := range ordered {
		if err := validateStableExternalRIDMembership(files[i], child); err != nil {
			return nil, err
		}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	for i, child := range ordered {
		if m.files[child.FileID] != files[i] {
			return nil, fmt.Errorf("%w: external-RID value-log child %d changed during membership validation", rootpublication.ErrResourceConflict, child.FileID)
		}
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityCommandWALExternalRIDFence)
	for _, child := range ordered {
		token, err := m.stableResourceTokenLocked(child.FileID, StableResourceRegistration{
			Kind: rootpublication.ResourceCommandWALExternalRID, LogicalLane: "command-wal-v2/external-rid",
			Generation:     uint64(child.FileID),
			DiagnosticPath: filepath.Join(filepath.Base(m.dir), fmt.Sprintf("%06d.vlog", child.FileID)),
			Reachability:   rootpublication.ReachabilityCommandWALExternalRIDFence,
			ExternalRIDs:   child.RIDs, Digest: child.Digest,
		})
		if err != nil {
			builder.Abandon()
			return nil, fmt.Errorf("external-RID value-log child %d: %w", child.FileID, err)
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			return nil, err
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	return resources, nil
}

func validateStableExternalRIDMembership(file *File, child StableExternalRIDSegment) error {
	for i, rid := range child.RIDs {
		ptr := child.Pointers[i]
		if ptr.FileID != child.FileID {
			return fmt.Errorf("%w: external RID %d pointer names value-log child %d, want %d", rootpublication.ErrResourceConflict, rid, ptr.FileID, child.FileID)
		}
		actualRID, err := file.ReadRIDUnverified(ptr)
		if err != nil {
			return fmt.Errorf("%w: validate external RID %d in value-log child %d: %v", rootpublication.ErrUnresolvedResource, rid, child.FileID, err)
		}
		if actualRID != rid {
			return fmt.Errorf("%w: external RID %d does not belong to value-log child %d at supplied pointer (found %d)", rootpublication.ErrResourceConflict, rid, child.FileID, actualRID)
		}
	}
	return nil
}

// StableExistingPhysicalResourceToken captures an exact manager-owned segment
// for a producer outside the value-log domain. The returned token certifies or
// reuses the segment's exact durable namespace link, then adds the shared
// physical deletion pin and the caller's producer classification.
func (m *Manager) StableExistingPhysicalResourceToken(
	fileID uint32,
	spec rootpublication.StableResourceSpec,
	constructor func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error),
) (*rootpublication.StableResourceToken, error) {
	if m == nil || constructor == nil {
		return nil, errors.New("valuelog: stable physical capture unavailable")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	file := m.files[fileID]
	if file == nil || file.File == nil {
		return nil, &fileNotFoundError{id: fileID}
	}
	if m.stableResourcePins == nil || !file.stableObserved {
		return nil, fmt.Errorf("%w: manager segment has no stable identity observer", rootpublication.ErrUnresolvedResource)
	}
	spec.File = file.File
	spec.PinRegistry = m.stableResourcePins
	if spec.Namespace == nil {
		namespace, err := m.stableExistingPhysicalNamespaceToken(file, spec.DiagnosticPath)
		if err != nil {
			return nil, err
		}
		defer namespace.Release()
		spec.Namespace = namespace
	}
	return constructor(spec)
}

func (m *Manager) stableExistingPhysicalNamespaceToken(file *File, diagnosticPath string) (*rootpublication.StableNamespaceToken, error) {
	if m == nil || file == nil || file.File == nil || m.stableResourcePins == nil {
		return nil, fmt.Errorf("%w: stable physical namespace capture unavailable", rootpublication.ErrUnresolvedResource)
	}
	parent, err := captureStableValueLogParent(file.Path, file.File)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	parentGeneration, err := rootpublication.StableNamespaceParentGeneration(parent)
	if err != nil {
		return nil, err
	}
	namespaceDiagnosticPath := filepath.Dir(diagnosticPath)
	if namespaceDiagnosticPath == "." {
		return nil, fmt.Errorf("%w: stable physical resource diagnostic path has no parent namespace", rootpublication.ErrUnresolvedResource)
	}
	namespaceSpec := rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: file.File, ParentGeneration: parentGeneration,
		Operation: rootpublication.NamespaceCreate, NewName: filepath.Base(file.Path),
		DiagnosticPath: namespaceDiagnosticPath,
	}
	namespace, err := m.stableResourcePins.NewStableNamespaceTokenForKnownLink(namespaceSpec)
	if err == nil {
		return namespace, nil
	}
	if !errors.Is(err, rootpublication.ErrNamespaceUnstable) {
		return nil, err
	}
	namespace, err = newValueLogStableNamespaceToken(namespaceSpec)
	if err != nil {
		return nil, err
	}
	if err := namespace.Stabilize(); err != nil {
		namespace.Release()
		return nil, err
	}
	if err := m.stableResourcePins.RememberStableNamespaceLink(parent, file.File, filepath.Base(file.Path)); err != nil {
		namespace.Release()
		return nil, err
	}
	return namespace, nil
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

const stableDeleteQuarantineMarker = ".delete-"

func stableDeleteQuarantinePaths(path string, identity rootpublication.StableIdentity) (string, string, error) {
	base := filepath.Base(path)
	if path == "" || base == "." || base == string(filepath.Separator) || identity.Platform == "" || identity.ObjectID == [16]byte{} {
		return "", "", fmt.Errorf("%w: invalid stable delete quarantine intent", rootpublication.ErrUnresolvedResource)
	}
	name := fmt.Sprintf(".%s%s%016x-%032x", base, stableDeleteQuarantineMarker, identity.VolumeID, identity.ObjectID)
	dir := filepath.Join(filepath.Dir(path), name)
	return dir, filepath.Join(dir, base), nil
}

func parseStableDeleteQuarantineName(name string) (string, rootpublication.StableIdentity, bool) {
	marker := strings.LastIndex(name, stableDeleteQuarantineMarker)
	if !strings.HasPrefix(name, ".") || marker <= 1 {
		return "", rootpublication.StableIdentity{}, false
	}
	base := name[1:marker]
	encoded := name[marker+len(stableDeleteQuarantineMarker):]
	if filepath.Base(base) != base || len(encoded) != 49 || encoded[16] != '-' {
		return "", rootpublication.StableIdentity{}, false
	}
	volumeID, err := strconv.ParseUint(encoded[:16], 16, 64)
	if err != nil {
		return "", rootpublication.StableIdentity{}, false
	}
	objectBytes, err := hex.DecodeString(encoded[17:])
	if err != nil || len(objectBytes) != 16 {
		return "", rootpublication.StableIdentity{}, false
	}
	var objectID [16]byte
	copy(objectID[:], objectBytes)
	if objectID == [16]byte{} {
		return "", rootpublication.StableIdentity{}, false
	}
	return base, rootpublication.StableIdentity{Platform: runtime.GOOS, VolumeID: volumeID, ObjectID: objectID}, true
}

func stableIdentityAtPath(path string) (rootpublication.StableIdentity, error) {
	file, err := os.Open(path)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	defer file.Close()
	return rootpublication.StableIdentityFromFile(file)
}

// recoverStableDeleteQuarantines reconciles deterministic same-parent delete
// intents before the manager exposes any segment from that directory. A
// matching quarantined inode is the delete target and can be finished. An
// unexpected inode is conservatively restored when the canonical name is
// absent, while ambiguous two-inode states fail closed.
func recoverStableDeleteQuarantines(parent string) error {
	entries, err := os.ReadDir(parent)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		base, expected, ok := parseStableDeleteQuarantineName(entry.Name())
		if !ok || !entry.IsDir() {
			continue
		}
		if _, err := recoverStableDeleteQuarantine(parent, entry.Name(), base, expected); err != nil {
			return err
		}
	}
	return nil
}

func requireNoStableDeleteQuarantines(parent string) error {
	entries, err := os.ReadDir(parent)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if _, _, ok := parseStableDeleteQuarantineName(entry.Name()); ok && entry.IsDir() {
			return fmt.Errorf("%w: %w: pending quarantine %q", ErrStableDeleteRecoveryRequired, rootpublication.ErrRecoveryRequired, filepath.Join(parent, entry.Name()))
		}
	}
	return nil
}

func recoverStableDeleteQuarantine(parent, quarantineName, base string, expected rootpublication.StableIdentity) (bool, error) {
	quarantineDir := filepath.Join(parent, quarantineName)
	entries, err := os.ReadDir(quarantineDir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, os.Remove(quarantineDir)
	}
	if len(entries) != 1 || entries[0].Name() != base || entries[0].IsDir() {
		return false, fmt.Errorf("%w: stable delete quarantine %q contains unexpected entries", rootpublication.ErrResourceConflict, quarantineDir)
	}
	quarantinePath := filepath.Join(quarantineDir, base)
	quarantinedIdentity, err := stableIdentityAtPath(quarantinePath)
	if err != nil {
		return false, err
	}
	originalPath := filepath.Join(parent, base)
	originalIdentity, originalErr := stableIdentityAtPath(originalPath)
	originalExists := originalErr == nil
	if originalErr != nil && !os.IsNotExist(originalErr) {
		return false, originalErr
	}

	if rootpublication.SamePhysicalIdentity(quarantinedIdentity, expected) {
		if err := os.Remove(quarantinePath); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		if err := os.Remove(quarantineDir); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		// If originalExists names the same inode, this merely removes a partial
		// rollback hard link. If it names a replacement, the replacement remains.
		completed := !originalExists || !rootpublication.SamePhysicalIdentity(originalIdentity, quarantinedIdentity)
		return completed, nil
	}

	if originalExists {
		return false, fmt.Errorf("%w: stable delete quarantine %q and canonical path %q name different unexpected identities", rootpublication.ErrResourceConflict, quarantinePath, originalPath)
	}
	if err := os.Rename(quarantinePath, originalPath); err != nil {
		return false, err
	}
	if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, segmentNamespaceResource(originalPath), parent, "", originalPath); err != nil {
		return false, err
	}
	return false, os.Remove(quarantineDir)
}

// removeStableSegmentFileOnce first moves the linked name into a private
// same-directory quarantine and records that completed canonical-name unlink.
// It then validates and unlinks the renamed object. A replacement created at
// the original name after the rename is never passed to the unlink syscall.
func removeStableSegmentFileOnce(path string, identity rootpublication.StableIdentity) (bool, error) {
	parent := filepath.Dir(path)
	resource := segmentNamespaceResource(path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		return false, err
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		if !os.IsExist(err) {
			return false, err
		}
		base, expected, ok := parseStableDeleteQuarantineName(filepath.Base(quarantineDir))
		if !ok {
			return false, fmt.Errorf("%w: invalid existing stable delete quarantine %q", rootpublication.ErrResourceConflict, quarantineDir)
		}
		completed, err := recoverStableDeleteQuarantine(parent, filepath.Base(quarantineDir), base, expected)
		if err != nil {
			return false, err
		}
		if completed {
			return true, nil
		}
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return true, nil
		} else if err != nil {
			return false, err
		}
		if err := os.Mkdir(quarantineDir, 0o700); err != nil {
			return false, err
		}
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		_ = os.Remove(quarantineDir)
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceUnlink, resource, parent, path, ""); err != nil {
		// The canonical name is already gone. Commit the identity deletion even
		// though the injected cut deliberately leaves the private inode behind.
		return true, err
	}
	restore := func() error {
		if err := os.Link(quarantinePath, path); err != nil {
			return err
		}
		if err := os.Remove(quarantinePath); err != nil {
			return err
		}
		return durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, resource, parent, "", path)
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
	return true, cleanupErr
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
	syncErr := m.syncDeferredDeletion(file.Path)
	commitStableDeleteLease(lease)
	m.mu.Lock()
	var unobserveErr error
	if current, exists := m.files[file.ID]; exists && current == file && file.RefCount.Load() == 0 && file.IsZombie.Load() {
		delete(m.files, file.ID)
		unobserveErr = m.unobserveStableFileLocked(file)
	}
	m.mu.Unlock()
	return errors.Join(unlinkErr, syncErr, unobserveErr)
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

// StableCreationNamespacePending reports whether the current segment was
// created by this writer and therefore requires NamespaceCreate evidence when
// it is captured. Existing-file opens return false; the subsequent token
// registration remains responsible for typed unsupported or uncertified
// failures.
func (w *Writer) StableCreationNamespacePending() (bool, error) {
	if w == nil || w.f == nil {
		return false, errors.New("valuelog: stable resource requires file-backed writer")
	}
	return w.creationProof != nil || w.creationUnsupported || w.creationUncertified, nil
}

// StableNamespaceParentGeneration returns the logical epoch derived from the
// exact retained parent directory handle used by stable creation and rotation.
func (w *Writer) StableNamespaceParentGeneration() (uint64, error) {
	if w == nil || w.stableParent == nil {
		if w != nil && w.stableParentErr != nil {
			return 0, w.stableParentErr
		}
		return 0, fmt.Errorf("%w: stable value-log parent unavailable", rootpublication.ErrUnresolvedResource)
	}
	return rootpublication.StableNamespaceParentGeneration(w.stableParent)
}

// CertifyStableCreationNamespace converts an exact relaxed-rotation successor
// into stable creation evidence before a stable caller appends bytes to it.
// The writer and its retained parent remain unchanged on failure, so a caller
// can retry without publishing an unreachable record.
func (w *Writer) CertifyStableCreationNamespace() error {
	if w == nil || w.f == nil {
		return errors.New("valuelog: stable resource requires file-backed writer")
	}
	if !w.creationUncertified {
		return nil
	}
	if w.stableParentErr != nil || w.stableParent == nil {
		if w.stableParentErr != nil {
			return fmt.Errorf("valuelog: stable parent unavailable: %w", w.stableParentErr)
		}
		return errors.New("valuelog: stable parent unavailable")
	}
	if w.creationProof != nil || w.creationUnsupported {
		return fmt.Errorf("%w: inconsistent uncertified value-log creation state", rootpublication.ErrResourceConflict)
	}
	proof, err := w.newStableCreationProof(w.stableParent, w.f, w.f.Name())
	if proof != nil {
		// The parent sync may have completed before an injected after-sync cut.
		// Retain that exact proof so retry does not repeat structural durability.
		w.creationProof = proof
		w.creationUncertified = false
	}
	return err
}

// StableResourceToken flushes accepted bytes and captures a duplicate of the
// current file descriptor. It does not fsync the file; publication owns the
// later FlushThrough/SyncThrough boundary on the token.
func (w *Writer) StableResourceToken(registration StableResourceRegistration) (*rootpublication.StableResourceToken, error) {
	if w == nil || w.f == nil {
		return nil, errors.New("valuelog: stable resource requires file-backed writer")
	}
	if err := w.bindStableResourcePinRegistry(&registration); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return w.stableResourceTokenAfterFlush(registration)
}

func (w *Writer) stableResourceTokenAfterFlush(registration StableResourceRegistration) (*rootpublication.StableResourceToken, error) {
	return w.stableResourceTokenAfterFlushWithContentState(registration, false)
}

func (w *Writer) stableResourceTokenAfterFlushWithContentState(registration StableResourceRegistration, contentSynced bool) (*rootpublication.StableResourceToken, error) {
	if w == nil || w.f == nil {
		return nil, errors.New("valuelog: stable resource requires file-backed writer")
	}
	creationPending := w.creationProof != nil || w.creationUnsupported || w.creationUncertified
	if creationPending && registration.NamespaceOperation != rootpublication.NamespaceCreate {
		return nil, fmt.Errorf("%w: current value-log segment requires NamespaceCreate evidence", rootpublication.ErrNamespaceUnstable)
	}
	if registration.NamespaceOperation == rootpublication.NamespaceCreate && registration.NamespaceParent == nil {
		registration.NamespaceParent = w.stableParent
	}
	if registration.NamespaceOperation == rootpublication.NamespaceCreate && w.creationUnsupported {
		return nil, fmt.Errorf("%w: current value-log segment creation cannot be certified on this platform", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	var namespace *rootpublication.StableNamespaceToken
	var err error
	if registration.NamespaceOperation == rootpublication.NamespaceCreate {
		if w.creationProof == nil {
			if w.stableResourcePins != nil && !w.creationUnsupported {
				if w.creationUncertified {
					return nil, fmt.Errorf("%w: asynchronously created value-log segment has no stable namespace proof", rootpublication.ErrUnresolvedResource)
				}
				return nil, fmt.Errorf("%w: existing value-log segment has no creation namespace proof", rootpublication.ErrUnresolvedResource)
			}
			namespace, err = stableValueLogNamespaceToken(w.f, registration)
		} else {
			parent := registration.NamespaceParent
			if parent == nil {
				parent = w.stableParent
			}
			name := registration.NewName
			if name == "" {
				name = filepath.Base(w.f.Name())
			}
			namespace, err = bindRetainedValueLogStableNamespaceCreationProof(w.creationProof, parent, registration.ParentGeneration, name, filepath.Dir(registration.DiagnosticPath))
		}
	} else {
		namespace, err = stableValueLogNamespaceToken(w.f, registration)
	}
	if err != nil {
		return nil, err
	}
	defer namespace.Release()
	return stableValueLogResourceToken(w.f, w.fileID, registration, namespace, contentSynced)
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
	return rootpublication.OpenStableChildFile(parent, filepath.Base(path), os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0o600)
}

func pendingValueLogFailure(parent, file *os.File, path string, err error) error {
	return errors.Join(err, rootpublication.ValidateStableChildLink(parent, file, filepath.Base(path)))
}

// RotateToWithStableResources pins the flushed old segment before writer state
// switches, attaching any retained proof from its original creation without a
// second directory sync. It then pins the newly-created active segment after
// that segment's namespace operation has been made persistent.
func (w *Writer) RotateToWithStableResources(path string, fileID uint32, syncCurrent bool, closed, active StableResourceRegistration) (*StableResourceRotation, error) {
	if w != nil && w.pendingStableSuccessor != nil && w.pendingStableSuccessor.failStop {
		return nil, fmt.Errorf("%w: value-log stable rotation stopped after old-writer close failure", rootpublication.ErrResourceOwnership)
	}
	if w == nil || w.f == nil {
		return nil, errors.New("valuelog: stable rotation requires file-backed writer")
	}
	if w.creationUncertified {
		return nil, fmt.Errorf("%w: asynchronously created current segment cannot enter a stable rotation", rootpublication.ErrUnresolvedResource)
	}
	if active.NamespaceOperation != rootpublication.NamespaceCreate {
		return nil, fmt.Errorf("%w: stable rotation creates its successor and requires active namespace operation %q, got %q",
			rootpublication.ErrNamespaceUnstable, rootpublication.NamespaceCreate, active.NamespaceOperation)
	}
	if w.stableParentErr != nil || w.stableParent == nil {
		if w.stableParentErr != nil {
			return nil, fmt.Errorf("valuelog: stable parent unavailable: %w", w.stableParentErr)
		}
		return nil, errors.New("valuelog: stable parent unavailable")
	}
	closed = cloneStableResourceRegistration(closed)
	active = cloneStableResourceRegistration(active)
	if err := w.bindStableResourcePinRegistry(&closed); err != nil {
		return nil, err
	}
	if err := w.bindStableResourcePinRegistry(&active); err != nil {
		return nil, err
	}
	normalizedActive, err := normalizeStableValueLogActiveRegistration(w.stableParent, path, active)
	if err != nil {
		return nil, err
	}
	if w.creationProof != nil || w.creationUnsupported || w.creationUncertified {
		switch closed.NamespaceOperation {
		case rootpublication.NamespaceNone:
			closed.NamespaceOperation = rootpublication.NamespaceCreate
		case rootpublication.NamespaceCreate:
		default:
			return nil, fmt.Errorf("%w: current value-log segment creation requires NamespaceCreate evidence", rootpublication.ErrNamespaceUnstable)
		}
		if closed.ParentGeneration == 0 {
			closed.ParentGeneration = normalizedActive.ParentGeneration
		}
	}
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
	closedToken, err := w.stableResourceTokenAfterFlushWithContentState(closed, syncCurrent)
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
		preparedIdentity, observeErr := observeStableWriterFile(prepared, w.stableResourcePins)
		if observeErr != nil {
			validateErr := rootpublication.ValidateStableChildLink(w.stableParent, prepared, filepath.Base(path))
			closeErr := prepared.Close()
			var removeErr, syncErr error
			if validateErr == nil {
				removeErr = rootpublication.RemoveStableChildFile(w.stableParent, filepath.Base(path))
				syncErr = w.stableParent.Sync()
			}
			closedToken.Release()
			return nil, errors.Join(observeErr, validateErr, closeErr, removeErr, syncErr)
		}
		pending = &pendingValueLogSuccessor{
			parent: w.stableParent, file: prepared, path: path, fileID: fileID,
			active: normalizedActive, stableIdentity: preparedIdentity,
			stableObserved: w.stableResourcePins != nil,
		}
		w.pendingStableSuccessor = pending
	}
	prepared := pending.file
	if !pending.observerEmitted {
		pending.observerEmitted = true
		if observeErr := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, segmentNamespaceResource(path), filepath.Dir(path), "", path); observeErr != nil {
			closedToken.Release()
			return nil, pendingValueLogFailure(w.stableParent, prepared, path, observeErr)
		}
	}
	if pending.creationProof == nil {
		proof, proofErr := w.newStableCreationProof(w.stableParent, prepared, path)
		pending.creationProof = proof
		if proofErr != nil {
			closedToken.Release()
			return nil, pendingValueLogFailure(w.stableParent, prepared, path, proofErr)
		}
	}
	preparedInfo, err := prepared.Stat()
	if err != nil {
		closedToken.Release()
		return nil, pendingValueLogFailure(w.stableParent, prepared, path, err)
	}
	namespace, err := bindValueLogStableNamespaceCreationProof(pending.creationProof, normalizedActive.NamespaceParent, normalizedActive.ParentGeneration,
		normalizedActive.NewName, filepath.Dir(normalizedActive.DiagnosticPath))
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
	oldCreationProof := w.creationProof
	closeErr := w.closeRotatedResource(old)
	observeErr := w.releaseStableResourceObservation()
	if err := errors.Join(closeErr, observeErr); err != nil {
		activeToken.Release()
		closedToken.Release()
		pending.failStop = true
		w.f = nil
		w.bw = nil
		return nil, fmt.Errorf("valuelog: retire old writer during stable rotation: %w", err)
	}
	pending.file = nil
	w.f = prepared
	w.creationProof = pending.creationProof
	w.creationUnsupported = false
	w.creationUncertified = false
	pending.creationProof = nil
	w.bw = nil
	w.size = preparedInfo.Size()
	w.fileID = fileID
	w.appendMax = defaultBufferSize
	w.appendBuf = w.appendBuf[:0]
	w.trimTransientScratchBuffers()
	if pending.stableObserved {
		w.stableResourceIdentity = pending.stableIdentity
		w.stableResourceObserved = true
		pending.stableObserved = false
	}
	if oldCreationProof != nil {
		oldCreationProof.Release()
	}
	w.pendingStableSuccessor = nil
	return &StableResourceRotation{Closed: closedToken, Active: activeToken}, nil
}
