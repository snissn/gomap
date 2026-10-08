package valuelog

import "github.com/snissn/gomap/TreeDB/internal/rootpublication"

// ReleaseSnapshotSetChecked discharges one actual Snapshot Set reference
// without deletion I/O or retry-worker admission. Open last-set ownership is
// unsupported until the caller has the complete checked terminal group. A
// failed check leaves every Set/File reference unchanged.
func (m *Manager) ReleaseSnapshotSetChecked(set *Set) error {
	if m == nil || set == nil || !set.retentionKnown {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		refs := set.RefCount.Load()
		if refs <= 0 {
			return rootpublication.ErrStableMetadataShapeUnsupported
		}
		if refs > 1 {
			if set.RefCount.CompareAndSwap(refs, refs-1) {
				return nil
			}
			continue
		}
		if m.closeErr != nil || m.registrationIncarnation == nil || !m.registrationIncarnation.closed.Load() {
			return rootpublication.ErrStableMetadataShapeUnsupported
		}
		// A closed incarnation is published only after Manager.Close has joined
		// workers and every tracked File.Close. Exact cells prove these handles.
		for id, f := range set.Files {
			if f == nil || f.ID != id || f.RefCount.cell == nil || f.RefCount.cell.incarnation != m.registrationIncarnation || !f.RefCount.cell.closed.Load() || !f.closed.Load() || f.RefCount.Load() <= 0 {
				return rootpublication.ErrStableMetadataShapeUnsupported
			}
		}
		if !set.RefCount.CompareAndSwap(1, 0) {
			continue
		}
		for _, f := range set.Files {
			f.RefCount.Add(-1)
		}
		return nil
	}
}
