package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
	"unsafe"
)

func TestOriginalCleanupCustodyKeepsSameRegistryUntilActualCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "consumed"
		if fail {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			registry := NewIdentityPinRegistry()
			var action retainedalloc.Owner
			action.Initialize(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(action))))
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			enrollment, err := retainedalloc.EnrollPair(&action, registry.MetadataOwner(), budget)
			if err != nil {
				t.Fatal(err)
			}
			original, err := NewOriginalCleanupCustody(registry, &action)
			if err != nil {
				t.Fatal(err)
			}
			registry.Close()
			if registry.originalCleanupEdges != 1 || registry.MetadataOwner().PhysicalClosed() {
				t.Fatal("preborn original action lost its SAME registry authority")
			}
			fault := errors.New("actual original action refused cleanup")
			payload := &struct{ marker int }{marker: 42}
			var actualErr error
			if fail {
				actualErr = fault
			}
			original.Complete(payload, actualErr)
			if registry.originalCleanupEdges != 0 {
				t.Fatal("original completion guard not consumed")
			}
			original.Complete(nil, nil)
			if registry.originalCleanupEdges != 0 {
				t.Fatal("duplicate original completion consumed another guard")
			}
			enrollment.CloseAfterCleanup(actualErr)
			if fail {
				if registry.MetadataOwner().PhysicalClosed() || !errors.Is(registry.CleanupError(), fault) || registry.failedPinned != (*resourcePinnedBacking)(original) || registry.failedPinned.custody != payload || budget.Stats().ExternalLeases < 2 {
					t.Fatal("exact failed original custody or registry guard refunded")
				}
			} else {
				if !registry.MetadataOwner().PhysicalClosed() {
					t.Fatal("actually consumed original action prevented registry completion")
				}
				if err = action.Close(); err != nil {
					t.Fatal(err)
				}
				if budget.Stats().ExternalBytes != 0 {
					t.Fatal("successful original completion leaked admitted owners")
				}
			}
		})
	}
}

func TestOriginalCleanupCustodyOverflowRefusesBeforeEffects(t *testing.T) {
	registry := NewIdentityPinRegistry()
	owner := registry.MetadataOwner()
	before := owner.Bytes()
	registry.originalCleanupEdges = ^uint64(0)
	if custody, err := NewOriginalCleanupCustody(registry, owner); custody != nil || err == nil {
		t.Fatal("completion guard overflow admitted")
	}
	if owner.Bytes() != before || registry.originalCleanupEdges != ^uint64(0) {
		t.Fatal("overflow refusal changed original owner admission")
	}
	registry.originalCleanupEdges = 0
	registry.Close()
	if custody, err := NewOriginalCleanupCustody(registry, owner); custody != nil || err == nil {
		t.Fatal("terminal authority admitted original action")
	}
}
