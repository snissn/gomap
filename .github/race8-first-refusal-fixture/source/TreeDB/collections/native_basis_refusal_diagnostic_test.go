package collections

import (
	"bytes"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestNativeBasisRefusalDescriptorPropagationAndAllocation(t *testing.T) {
	// This control's quiet setup is separate from the UNCHANGED lifecycle witness.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "diagnostic_control"}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("diagnostic_control")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Insert([]byte("one"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if collection.nativeJSON == nil {
		t.Fatal("control did not activate native collection")
	}
	read, err := collection.nativeJSON.Capture("diagnostic_control")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	base := read.capture.State().(*nativeEngineVersion)
	snapshot := read.capture.Snapshot()
	token, ok := snapshot.StateToken()
	if !ok || token.SystemRootPageID == 0 {
		t.Fatal("control requires the original persisted system root")
	}
	lease, err := base.maintenance.AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeBasisRefreshScratch{}))))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	scratch := &nativeBasisRefreshScratch{before: snapshot, after: snapshot, allocator: base.maintenance}
	defer func() { *scratch = nativeBasisRefreshScratch{} }()
	scratch.beforeReader, err = snapshot.NativeReaderAtRoot(token.SystemRootPageID)
	if err != nil {
		t.Fatal(err)
	}
	scratch.afterReader, err = snapshot.NativeReaderAtRoot(token.SystemRootPageID)
	if err != nil {
		t.Fatal(err)
	}
	scratch.beforeRoot, scratch.afterRoot = true, true
	var observer valuelog.COWBasisRefusalObserver
	identity := transactionowner.New()
	if !observer.Bind(identity) {
		t.Fatal("bind scalar control")
	}
	call := observer.Begin(identity, false).Descriptor(valuelog.COWBasisNextIncarnation)
	key := []byte(nativeCatalogNextIncarnationKey)
	before, found, err := scratch.get(key, false, valuelog.COWBasisCall{})
	if err != nil || !found {
		t.Fatalf("original descriptor: found=%t error=%v", found, err)
	}
	copyBefore := append([]byte(nil), before...)
	for _, after := range []bool{false, true} {
		value, found, err := scratch.get(key, after, call)
		if err != nil || !found || !bytes.Equal(value, copyBefore) {
			t.Fatalf("observed descriptor side=%t found=%t error=%v", after, found, err)
		}
	}
	ordinary := testing.AllocsPerRun(16, func() {
		if _, _, err := scratch.get(key, false, valuelog.COWBasisCall{}); err != nil {
			panic(err)
		}
	})
	observed := testing.AllocsPerRun(16, func() {
		if _, _, err := scratch.get(key, false, call); err != nil {
			panic(err)
		}
	})
	if observed != ordinary {
		t.Fatalf("descriptor allocations ordinary=%g observed=%g", ordinary, observed)
	}
	// A heap-escaped view allocated unconditionally would also violate this
	// absolute zero control, independent of physical leaf decode allocations.
	scratch.beforeRoot = false
	if n := testing.AllocsPerRun(16, func() {
		if _, _, err := scratch.get(nil, false, call); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatalf("empty-root diagnostic allocations=%g", n)
	}
	if result := observer.Snapshot(); result.First != 0 || result.Entered[valuelog.COWBasisDescriptor] == 0 || result.Entered[valuelog.COWBasisDescriptor] != result.Finished[valuelog.COWBasisDescriptor] {
		t.Fatalf("descriptor observation=%+v", result)
	}
}
