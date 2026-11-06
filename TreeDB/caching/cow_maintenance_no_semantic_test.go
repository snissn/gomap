package caching

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWMaintenanceNilSemanticBasisNeedsNoDictionaryCapacity(t *testing.T) {
	for _, semantic := range []string{"none", "active", "frozen"} {
		t.Run(semantic, func(t *testing.T) {
			db, backend := cowMaintenanceLifetimeFixture(t)
			if err := db.Set([]byte("a"), []byte("original")); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			held, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := held.Close(); err != nil {
					t.Error(err)
				}
			}()
			db.flushMu.Lock()
			defer db.flushMu.Unlock()
			c := db.cow
			c.writerMu.Lock()
			opening := c.cut
			if opening.semanticRootSet != nil || opening.frozenSemanticRootSet != nil {
				c.writerMu.Unlock()
				t.Fatal("raw fixture unexpectedly has semantic roots")
			}
			if semantic == "active" {
				opening.semanticRootSet = newTestSemanticState(nil)
			} else if semantic == "frozen" {
				opening.frozenSemanticRootSet = newTestSemanticState(nil)
			}
			c.writerMu.Unlock()
			basis, err := c.captureBackendBasis(backend)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := basis.release(); err != nil {
					t.Error(err)
				}
			}()
			next, err := c.prepareCOWBasisInstall()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if next != nil {
					refund := c.releasePrivateCOWBasisInstall(next)
					next = nil
					refund.Close()
				}
			}()
			if err := basis.snapshot.ActivatePrimaryMetadataProducer(); err != nil {
				t.Fatal(err)
			}
			// Capture and the successor cut are already admitted. Fill the SAME
			// finite maintenance lane so even its empty dictionary vector refuses.
			backend.exhaustBudget(c.maintenance)
			defer backend.pressure.Close()
			probe, err := db.prepareCOWMaintenanceLeaves()
			if probe != nil {
				c.retireCOWMaintenanceLeaves(&probe)
			}
			if !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("missing real dictionary capacity refusal: %v", err)
			}
			old, poisoned, err := db.installPreparedCOWBasis(basis, nil, next)
			if semantic == "none" {
				if err != nil || poisoned || c.cut == opening || c.cut.basis != basis {
					t.Fatalf("raw-only physical basis install: poisoned=%t error=%v", poisoned, err)
				}
				next = nil // Published successor now owns its original admission.
			} else if !errors.Is(err, memtable.ErrCOWCapacity) || poisoned || c.cut != opening {
				t.Fatalf("native admission must still refuse without publication: poisoned=%t error=%v", poisoned, err)
			}
			backend.pressure.Close()
			if old != nil {
				if err := old.drain(); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := held.Get([]byte("a")); err != nil || string(got) != "original" {
				t.Fatalf("held original cut: %q %v", got, err)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
