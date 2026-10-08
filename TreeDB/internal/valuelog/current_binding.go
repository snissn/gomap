package valuelog

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ValidateCurrentBinding inspects the installed writer without flushing,
// creating a successor, or certifying namespace durability. The producer must
// serialize this call with append, rotation and Close. A successful result is
// only an ownership observation; it is not a durable frontier certificate.
func (w *Writer) ValidateCurrentBinding(path string, fileID uint32, pins *rootpublication.IdentityPinRegistry) (rootpublication.StableIdentity, error) {
	bad := func() (rootpublication.StableIdentity, error) {
		return rootpublication.StableIdentity{}, fmt.Errorf("%w: current writer binding is incomplete", rootpublication.ErrResourceConflict)
	}
	if w == nil || w.f == nil || fileID == 0 || w.fileID != fileID ||
		path == "" || filepath.Clean(w.f.Name()) != filepath.Clean(path) ||
		w.pendingStableSuccessor != nil || w.stableParent == nil || w.stableParentErr != nil ||
		pins == nil || w.stableResourcePins != pins || !w.stableResourceObserved {
		return bad()
	}
	identity, err := rootpublication.StableIdentityFromFile(w.f)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	if !rootpublication.SamePhysicalIdentity(identity, w.stableResourceIdentity) {
		return bad()
	}
	parent, err := rootpublication.OpenStableParent(filepath.Dir(path))
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	defer parent.Close()
	currentParent, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	retainedParent, err := rootpublication.StableIdentityFromFile(w.stableParent)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	if !rootpublication.SamePhysicalIdentity(currentParent, retainedParent) {
		return bad()
	}
	if err := rootpublication.ValidateStableChildLink(parent, w.f, filepath.Base(path)); err != nil {
		return rootpublication.StableIdentity{}, err
	}
	return identity, nil
}

// ValidateCurrentWritableBinding checks one exact registered file. Admission
// shares the Manager's existing Close-joined worker counter; the File reference
// prevents deletion while metadata and namespace checks run outside mu. No
// scan, registration, promotion, flush or durability proof is performed. If a
// concurrent owner retires the file, dropping the last borrowed reference uses
// the Manager's existing zombie cleanup, outside mu and before the Close join.
func (m *Manager) ValidateCurrentWritableBinding(path string, fileID uint32, expected rootpublication.StableIdentity, pins *rootpublication.IdentityPinRegistry) (retErr error) {
	if m == nil || pins == nil || expected == (rootpublication.StableIdentity{}) {
		return fmt.Errorf("%w: current registered binding is incomplete", rootpublication.ErrResourceConflict)
	}
	m.mu.Lock()
	f := m.files[fileID]
	valid := func() bool {
		if m.closing || f == nil || m.files[fileID] != f || f.File == nil ||
			f.closed.Load() || f.IsZombie.Load() || !f.currentWritable.Load() ||
			m.stableResourcePins != pins || !f.stableObserved ||
			filepath.Clean(f.Path) != filepath.Clean(path) ||
			!rootpublication.SamePhysicalIdentity(f.stableIdentity, expected) {
			return false
		}
		lane, _ := DecodeFileID(fileID)
		return m.currentWritableByLane[m.currentWritableKeyLocked(lane, fileID)] == fileID
	}
	if !valid() {
		m.mu.Unlock()
		return fmt.Errorf("%w: registered file %d is not the exact current writer", rootpublication.ErrResourceConflict, fileID)
	}
	f.RefCount.Add(1)
	m.retryWorkers.Add(1)
	namespace := f.stableNamespace
	m.mu.Unlock()
	defer m.retryWorkers.Done()
	defer func() {
		if f.RefCount.Add(-1) == 0 && f.IsZombie.Load() {
			retErr = errors.Join(retErr, m.deleteZombieFile(f))
		}
	}()
	actual, err := rootpublication.StableIdentityFromFile(f.File)
	if err != nil {
		return err
	}
	if !rootpublication.SamePhysicalIdentity(actual, expected) {
		return fmt.Errorf("%w: registered current handle changed", rootpublication.ErrResourceConflict)
	}
	parent, err := rootpublication.OpenStableParent(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parent.Close()
	parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		return err
	}
	observedNamespace := fmt.Sprintf("%s:%d:%x/%s", parentIdentity.Platform, parentIdentity.VolumeID, parentIdentity.ObjectID, filepath.Base(path))
	if namespace == "" || namespace != observedNamespace {
		return fmt.Errorf("%w: current registered namespace changed", rootpublication.ErrResourceConflict)
	}
	if err := rootpublication.ValidateStableChildLink(parent, f.File, filepath.Base(path)); err != nil {
		return err
	}
	m.mu.RLock()
	stillValid := valid() && f.stableNamespace == namespace
	m.mu.RUnlock()
	if !stillValid {
		return fmt.Errorf("%w: current registration changed during inspection", rootpublication.ErrResourceConflict)
	}
	return nil
}
