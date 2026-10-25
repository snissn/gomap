package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Embedding only the established capture interface deliberately hides optional
// point companions, exercising the unchanged fixed-scratch compatibility path.
type nativeTextPointLegacyCapture struct {
	backenddb.NativeSemanticCapture
}
type nativeTextPointObservedCapture struct {
	backenddb.NativeSemanticCapture
	physical                                backenddb.NativeAdmittedPhysicalReader
	calls, outputAdmissions, refusedOutputs int
}

func (c *nativeTextPointObservedCapture) NativeEntryAtRootAdmitted(root uint64, key []byte, copyValue bool, admit func(uint64) error) (node.LeafEntry, error) {
	c.calls++
	entry, err := c.physical.NativeEntryAtRootAdmitted(root, key, copyValue, func(n uint64) error {
		c.outputAdmissions++
		return admit(n)
	})
	if err != nil && entry.Value == nil {
		c.refusedOutputs++
	}
	return entry, err
}

func TestNativeTextCatalogAdmittedPhysicalPoint(t *testing.T) {
	for _, cacheEntries := range []int{128, -1} {
		t.Run(fmt.Sprintf("cache=%d", cacheEntries), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.LeafPageReadCacheEntries = cacheEntries
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := manager.CreateCollection(nativeTextPublicMeta()); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				if _, err := c.Insert([]byte(fmt.Sprintf("id%d", i)), []byte(`{"content":"refund policy","tenant":"one"}`)); err != nil {
					t.Fatal(err)
				}
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			read, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			original := read.capture
			physical, ok := original.(backenddb.NativeAdmittedPhysicalReader)
			if !ok {
				t.Fatal("missing optional original owner companion")
			}
			observed := nativeTextPointObservedCapture{NativeSemanticCapture: original, physical: physical}
			read.capture = &observed
			defer func() {
				if read.capture != nil {
					read.capture = original
				}
			}()
			lease, err := original.NativeAllocator().AdmitNativeBytes(0)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			budget := original.NativeAllocator().(nativeHNSWAdmissionBudget)
			var charged uint64
			refuse := false
			admit := func(n uint64) error {
				if refuse {
					return memtable.ErrCOWCapacity
				}
				if n > ^uint64(0)-charged {
					return memtable.ErrCOWCapacity
				}
				if err := lease.Resize(charged + n); err != nil {
					return err
				}
				charged += n
				return nil
			}
			catalog, err := newNativeTextCatalog(read, admit)
			if err != nil {
				t.Fatal(err)
			}
			scope := catalog.nativeText
			roots := []string{collectionTextV2DocMapRootName(c.name, "content"), collectionTextV2NormBlocksRootName(c.name, "content")}
			key := encodeTextV2BlockKey(1)
			// Ensure this is the physical captured basis, not merely a memory-only pass.
			for _, root := range roots {
				read.mu.Lock()
				domain, err := read.borrowedNamedDomain(scope.relative(root))
				if err == nil {
					_, resolved, memoryErr := domain.memoryEntry(key)
					if memoryErr != nil || resolved || !domain.hasBase {
						t.Errorf("not a physical base: root=%s resolved=%t err=%v", root, resolved, memoryErr)
					}
				}
				read.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			// Both the general reducer envelope and serving classification must charge
			// exactly the old copy path once, even while alternating physical roots.
			for _, fields := range []uint64{0, 1} {
				scope.singleTermFields = fields
				for i := 0; i < 6; i++ {
					root := roots[i%len(roots)]
					before := charged
					entry, err := scope.entry(root, key)
					if err != nil {
						t.Fatal(err)
					}
					want := bytes.Clone(entry.Value)
					directCharge := charged - before
					if cap(entry.Value) != len(entry.Value) || len(want) == 0 {
						t.Fatalf("independent exact output: %+v", entry)
					}
					entry.Value[0] ^= 0xff
					read.capture = nativeTextPointLegacyCapture{original}
					before = charged
					fallback, err := scope.entry(root, key)
					if err != nil {
						t.Fatal(err)
					}
					fallbackCharge := charged - before
					read.capture = &observed
					// The compatibility reader admits its scratch once, separately from value
					// cohorts. Warm it before comparing subsequent alternating records.
					if i > 0 && directCharge != fallbackCharge {
						t.Fatalf("duplicate/missing charge: direct=%d fallback=%d", directCharge, fallbackCharge)
					}
					if !bytes.Equal(fallback.Value, want) {
						t.Fatal("copied output aliases original physical bytes")
					}
				}
			}
			if observed.calls != 12 || observed.outputAdmissions == 0 {
				t.Fatalf("optional physical path not used: %+v", observed)
			}
			before := budget.Stats()
			beforeAdmissions := observed.outputAdmissions
			refuse = true
			entry, err := scope.entry(roots[0], key)
			refuse = false
			after := budget.Stats()
			if !errors.Is(err, memtable.ErrCOWCapacity) || entry.Value != nil || observed.outputAdmissions != beforeAdmissions+1 || observed.refusedOutputs != 1 || after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
				t.Fatalf("output refusal: entry=%+v err=%v before=%+v after=%+v observer=%+v", entry, err, before, after, observed)
			}
			// Missing physical keys must not admit an inline output at all.
			beforeAdmissions = observed.outputAdmissions
			if _, err := scope.entry(roots[0], []byte("absent")); !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatalf("missing point: %v", err)
			}
			if observed.outputAdmissions != beforeAdmissions {
				t.Fatal("missing point admitted output")
			}
			// Newest memory records and tombstones win without entering the companion.
			if err := c.Delete([]byte("id0")); err != nil {
				t.Fatal(err)
			}
			current, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			currentOriginal := current.capture
			currentObserved := nativeTextPointObservedCapture{NativeSemanticCapture: currentOriginal, physical: currentOriginal.(backenddb.NativeAdmittedPhysicalReader)}
			current.capture = &currentObserved
			defer func() {
				if current.capture != nil {
					current.capture = currentOriginal
				}
			}()
			currentCatalog, err := newNativeTextCatalog(current, admit)
			if err != nil {
				t.Fatal(err)
			}
			deleted, err := currentCatalog.nativeText.entry(c.name+"/primary", []byte("id0"))
			if err != nil || deleted.Flags&node.FlagTombstone == 0 || currentObserved.calls != 0 {
				t.Fatalf("memory tombstone lost: %+v err=%v calls=%d", deleted, err, currentObserved.calls)
			}
			lease.Close()
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("full owner Close retained receipts: %+v", stats)
			}
		})
	}
}
