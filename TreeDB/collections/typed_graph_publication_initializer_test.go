package collections

import (
	"bytes"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// Only an exact freshly captured base can seed an empty suffix. Reopen with a
// nonempty suffix will require the separately bounded cold bootstrap.
func (c *Collection) initializeTypedGraphPublication(catalog *collectionCatalog, limits typedGraphPublicationLimits) error {
	if limits.EncodedOutputBytes < 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() || catalog == nil || catalog.pager != c.db.Pager() || catalog.typedGraphBase == nil || limits.Rows <= 0 || limits.Tombstones < 0 || limits.ValueSlots <= 0 || limits.OwnedBytes <= 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	// Initialization is top-level only: exclude admissions and drain every
	// manager's already admitted work before checking the supplied base.
	unlock := c.lockCollectionSchemaWrite()
	defer unlock()
	if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
		return err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	defer snap.Close()
	current, err := loadCollectionCatalog(snap, catalog.meta.Name)
	if err != nil {
		return err
	}
	if !(&typedGraphPublicationState{catalog: catalog}).matches(current) {
		return ErrVectorIndexSnapshotMismatch
	}
	if catalog.meta.Options.ColumnStore == nil || len(catalog.meta.VectorIndexes) != 1 {
		return ErrHybridSearchUnsupported
	}
	if err := validateTypedGraphOverlayVectorOwners(*catalog.meta.Options.ColumnStore); err != nil {
		return err
	}
	base := catalog.typedGraphBase
	if !collectionMetaValuesEqual(base.meta, catalog.meta) {
		return ErrVectorIndexSnapshotMismatch
	}
	if err := assertTypedGraphCapturedRootContents(snap, catalog); err != nil {
		return err
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	next := &typedGraphPublicationState{catalog: catalog, limits: limits}
	if err := next.prepareEncodedBounds(); err != nil {
		return err
	}
	if !coord.typedPublication.CompareAndSwap(nil, next) {
		return ErrVectorIndexSnapshotMismatch
	}
	return nil
}

func assertTypedGraphCapturedRootContents(snap *backenddb.Snapshot, catalog *collectionCatalog) error {
	budget := typedGraphCaptureBudget{records: typedGraphCaptureMaxRecords, bytes: typedGraphCaptureMaxBytes}
	for name, baseRoot := range catalog.typedGraphBase.roots {
		currentRoot := catalog.rootID(name)
		if baseRoot == currentRoot {
			continue
		}
		if _, err := scanTypedGraphCaptureRoot(snap, baseRoot, &budget); err != nil {
			return err
		}
		if _, err := scanTypedGraphCaptureRoot(snap, currentRoot, &budget); err != nil {
			return err
		}
		a, err := snap.IteratorAtRoot(baseRoot, nil, nil)
		if err != nil {
			return err
		}
		b, err := snap.IteratorAtRoot(currentRoot, nil, nil)
		if err != nil {
			a.Close()
			return err
		}
		equal := true
		for a.Valid() && b.Valid() {
			av, ap, af, _ := iterator.UnsafeEntryWithRevision(a)
			bv, bp, bf, _ := iterator.UnsafeEntryWithRevision(b)
			if !bytes.Equal(a.UnsafeKey(), b.UnsafeKey()) || !bytes.Equal(av, bv) || ap != bp || af != bf {
				equal = false
				break
			}
			a.Next()
			b.Next()
		}
		if a.Valid() != b.Valid() {
			equal = false
		}
		ae, be := a.Error(), b.Error()
		a.Close()
		b.Close()
		if ae != nil {
			return ae
		}
		if be != nil {
			return be
		}
		if !equal {
			return fmt.Errorf("captured root %q content differs", name)
		}
	}
	return nil
}
