package collections

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func nativeTestSelectedRecordInventory(t *testing.T, codec valuelog.BlockCodec, length ...int64) (*nativeResourceInventory, rootpublication.StableResourceSelector, page.ValuePtr, *memtable.COWBudget, string, []byte) {
	inventory, selector, ptr, budget, path, want, _ := nativeTestSelectedRecordInventoryWithFailure(t, codec, nil, length...)
	return inventory, selector, ptr, budget, path, want
}

func nativeTestSelectedRecordInventoryWithFailure(t *testing.T, codec valuelog.BlockCodec, failure error, length ...int64) (*nativeResourceInventory, rootpublication.StableResourceSelector, page.ValuePtr, *memtable.COWBudget, string, []byte, *nativeInventoryOwnedOperations) {
	t.Helper()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	owner := new(retainedalloc.Owner)
	owner.Initialize(0)
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	t.Cleanup(registry.Close)
	path := filepath.Join(t.TempDir(), "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(7))
	if err != nil {
		t.Fatal(err)
	}
	writer.SetBlockCompression(codec, codec != valuelog.BlockCodecNone)
	want := bytes.Repeat([]byte("original-selected-record"), 200)
	var ptr page.ValuePtr
	if codec == valuelog.BlockCodecNone {
		ptr, err = writer.Append(0, nil, 1, want)
	} else {
		var ptrs []page.ValuePtr
		ptrs, err = writer.AppendFrame(0, nil, []valuelog.Record{{RID: 1, Value: want}})
		if err == nil {
			ptr = ptrs[0]
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registry.Unobserve(identity); err != nil {
			t.Error(err)
		}
	})
	obligation := rootpublication.StableLogicalObligation{Class: "value-record", Kind: "record", Namespace: "captured", Generation: 1, FileID: uint64(ptr.FileID), Offset: int64(ptr.Offset - 4), Length: info.Size() - int64(ptr.Offset-4), Reachability: rootpublication.ReachabilityValueLogPointer, Digest: sha256.Sum256(want)}
	if len(length) != 0 && length[0] > 0 {
		obligation.Length = length[0]
	}
	var operations *nativeInventoryOwnedOperations
	if failure != nil {
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(nativeInventoryOwnedOperations{})))
		if err := owner.AddPending(charge); err != nil {
			t.Fatal(err)
		}
		operations = &nativeInventoryOwnedOperations{owner: owner, failure: failure, charge: charge}
	}
	spec := rootpublication.StableResourceSpec{MetadataOwner: owner, PinRegistry: registry, Kind: rootpublication.ResourceValueLog, LogicalLane: "main", ResourceID: "7", Generation: 1, DiagnosticPath: "value.log", File: file, Frontier: rootpublication.DurableFrontier{Bytes: uint64(info.Size())}, Reachability: rootpublication.ReachabilityValueLogPointer, LogicalObligations: []rootpublication.StableLogicalObligation{obligation}}
	if operations != nil {
		spec.OwnedOperations = operations
	}
	token, err := rootpublication.NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.AbandonWithError(); err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeResourceInventory{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{}))))
	if err != nil {
		t.Fatal(err)
	}
	inventory := &nativeResourceInventory{resources: resources, metadata: owner, enrollment: enrollment, refund: &nativeEngineRefund{lease: lease}}
	inventory.refs.Store(1)
	inventory.cleanup.Work = inventory
	selector := rootpublication.StableResourceSelector{Kind: rootpublication.ResourceValueLog, LogicalLane: "main", ResourceID: "7", PhysicalGeneration: 1, Obligation: obligation}
	return inventory, selector, ptr, budget, path, want, operations
}

func TestNativeSelectedRecordReadOriginalCleanupFailureRemainsOnReadWork(t *testing.T) {
	failure := errors.New("original selected record resource refused cleanup")
	inventory, selector, ptr, budget, _, want, operations := nativeTestSelectedRecordInventoryWithFailure(t, valuelog.BlockCodecSnappy, failure)
	read, err := prepareNativeSelectedRecordRead(inventory, selector, ptr, nativeSelectedRecordTestLimits, budget)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := read.read(); err != nil || !bytes.Equal(got, want) {
		t.Fatal("original record unavailable before cleanup", err)
	}
	if work := inventory.release(); work != nil {
		t.Fatal("read did not own the original inventory edge")
	}
	inventory = nil
	budget.Close()
	work := read.release()
	beforeCalls := operations.calls.Load()
	done, err := work.Finish()
	if !errors.Is(err, failure) || done.Refund != nil || work.Work != read || read.failure != err || read.refund == nil ||
		read.inventory == nil || read.inventory.resources == nil || read.inventory.enrollment == nil || read.inventory.failure != err {
		t.Fatal("failed cleanup lost original read/inventory custody", err)
	}
	if read.ready || read.decoder != nil || read.input != nil || read.raw != nil || read.output != nil || read.buffers != nil ||
		read.selector != (rootpublication.StableResourceSelector{}) || read.ptr != (page.ValuePtr{}) || operations.calls.Load() != beforeCalls+1 {
		t.Fatal("read scratch survived or physical cleanup repeated")
	}
	before := budget.Stats()
	if before.TotalBytes == 0 {
		t.Fatal("original failed owners refunded")
	}
	original := err
	done, err = work.Finish()
	if err != original || done.Refund != nil || operations.calls.Load() != beforeCalls+1 || budget.Stats() != before {
		t.Fatal("original failed read work retried or changed custody", err)
	}
}

func nativeTestFinishSelectedRecord(t *testing.T, read **nativeSelectedRecordRead) {
	t.Helper()
	if *read == nil {
		return
	}
	work := (*read).release()
	*read = nil
	if work != nil {
		done, err := work.Finish()
		if err != nil {
			t.Fatal(err)
		}
		work = nil
		done.Complete()
	}
}

var nativeSelectedRecordTestLimits = valuelog.COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20}

func TestNativeSelectedRecordReadRetainsOriginalInventory(t *testing.T) {
	for _, codec := range []valuelog.BlockCodec{valuelog.BlockCodecNone, valuelog.BlockCodecSnappy, valuelog.BlockCodecLZ4} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			inventory, selector, ptr, budget, path, want := nativeTestSelectedRecordInventory(t, codec)
			read, err := prepareNativeSelectedRecordRead(inventory, selector, ptr, nativeSelectedRecordTestLimits, budget)
			if err != nil || !read.ready || (codec != valuelog.BlockCodecNone && !read.shape.Compressed) {
				t.Fatalf("read preparation: %v", err)
			}
			if work := inventory.release(); work != nil {
				t.Fatal("read lost its independent inventory edge")
			}
			inventory = nil
			if err = os.Rename(path, path+".original"); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, bytes.Repeat([]byte("future"), 1000), 0o600); err != nil {
				t.Fatal(err)
			}
			budget.Close()
			before := budget.Stats()
			if allocations := testing.AllocsPerRun(100, func() {
				got, err := read.read()
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("original record changed: %v", err)
				}
			}); allocations != 0 {
				t.Fatalf("prepared read allocated: %g", allocations)
			}
			if budget.Stats() != before {
				t.Fatal("prepared read admitted or recaptured resources")
			}
			nativeTestFinishSelectedRecord(t, &read)
			if budget.Stats().TotalBytes != 0 {
				t.Fatal(budget.Stats())
			}
		})
	}
}

func TestNativeSelectedRecordReadPreparationRefusalKeepsOriginalScope(t *testing.T) {
	inventory, selector, ptr, budget, _, _ := nativeTestSelectedRecordInventory(t, valuelog.BlockCodecSnappy)
	baseline := budget.Stats().TotalBytes
	wrong := selector
	wrong.Obligation.FileID++
	if read, err := prepareNativeSelectedRecordRead(inventory, wrong, ptr, nativeSelectedRecordTestLimits, budget); read != nil || err == nil || budget.Stats().TotalBytes != baseline {
		t.Fatal("wrong pointer identity allocated")
	}
	wrong = selector
	wrong.Obligation.Length = 1
	read, err := prepareNativeSelectedRecordRead(inventory, wrong, ptr, nativeSelectedRecordTestLimits, budget)
	if read != nil || !errors.Is(err, rootpublication.ErrUnresolvedResource) || budget.Stats().TotalBytes != baseline {
		t.Fatal("out-of-obligation header inspection allocated")
	}
	outside := ptr
	outside.Offset += uint64(selector.Obligation.Length) + 4
	if read, err = prepareNativeSelectedRecordRead(inventory, selector, outside, nativeSelectedRecordTestLimits, budget); read != nil || !errors.Is(err, rootpublication.ErrUnresolvedResource) || budget.Stats().TotalBytes != baseline {
		t.Fatal("same-file pointer outside selected obligation inspected or allocated")
	}
	control := &nativeRolloverAdmissionControl{COWBudget: budget, remaining: 1, err: memtable.ErrCOWCapacity}
	read, err = prepareNativeSelectedRecordRead(inventory, selector, ptr, nativeSelectedRecordTestLimits, control)
	if read == nil || !errors.Is(err, memtable.ErrCOWCapacity) || read.ready || read.buffers != nil || read.inventory != inventory {
		t.Fatalf("buffer admission lost original partial: %v", err)
	}
	nativeTestFinishSelectedRecord(t, &read)
	limits := nativeSelectedRecordTestLimits
	limits.MaxRecordBytes = 1
	read, err = prepareNativeSelectedRecordRead(inventory, selector, ptr, limits, budget)
	if read == nil || !errors.Is(err, valuelog.ErrCOWReadCapacity) || read.ready {
		t.Fatalf("record limit refusal: %v", err)
	}
	nativeTestFinishSelectedRecord(t, &read)
	if budget.Stats().TotalBytes != baseline || inventory.refs.Load() != 1 {
		t.Fatal("read refusal dropped original inventory or leaked partial")
	}
	budget.Close()
	if read, err = prepareNativeSelectedRecordRead(inventory, selector, ptr, nativeSelectedRecordTestLimits, budget); read != nil || !errors.Is(err, memtable.ErrCOWClosed) {
		t.Fatalf("closed admission allocated: %v", err)
	}
	work := inventory.release()
	inventory = nil
	done, err := work.Finish()
	if err != nil {
		t.Fatal(err)
	}
	work = nil
	done.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeSelectedRecordReadRejectsUnrepresentedRecordAndCodec(t *testing.T) {
	for _, test := range []struct {
		name   string
		codec  valuelog.BlockCodec
		length int64
		err    error
	}{
		{"outside-exact-obligation", valuelog.BlockCodecNone, 1, rootpublication.ErrUnresolvedResource},
		{"unrepresented-zstd", valuelog.BlockCodecZSTD, 0, valuelog.ErrCOWReadUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			inventory, selector, ptr, budget, _, _ := nativeTestSelectedRecordInventory(t, test.codec, test.length)
			before := budget.Stats().TotalBytes
			read, err := prepareNativeSelectedRecordRead(inventory, selector, ptr, nativeSelectedRecordTestLimits, budget)
			if !errors.Is(err, test.err) || (read != nil && (read.ready || read.buffers != nil || read.decoder != nil)) {
				t.Fatalf("unrepresented read shape accepted: %v", err)
			}
			nativeTestFinishSelectedRecord(t, &read)
			if budget.Stats().TotalBytes != before || inventory.refs.Load() != 1 {
				t.Fatal("refused shape dropped original inventory or leaked scratch")
			}
			budget.Close()
			work := inventory.release()
			inventory = nil
			done, err := work.Finish()
			if err != nil {
				t.Fatal(err)
			}
			work = nil
			done.Complete()
			if budget.Stats().TotalBytes != 0 {
				t.Fatal(budget.Stats())
			}
		})
	}
}

func TestNativeSelectedRecordReadRevalidatesOriginalCRC(t *testing.T) {
	inventory, selector, ptr, budget, path, _ := nativeTestSelectedRecordInventory(t, valuelog.BlockCodecNone)
	read, err := prepareNativeSelectedRecordRead(inventory, selector, ptr, nativeSelectedRecordTestLimits, budget)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(ptr.Offset-4) + int64(read.shape.RecordBytes) - 1
	if _, err = file.WriteAt([]byte{0xff}, offset); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	budget.Close()
	before := budget.Stats()
	if got, err := read.read(); got != nil || !errors.Is(err, valuelog.ErrCorrupt) || budget.Stats() != before {
		t.Fatalf("corrupt original record escaped: %v", err)
	}
	nativeTestFinishSelectedRecord(t, &read)
	work := inventory.release()
	inventory = nil
	done, err := work.Finish()
	if err != nil {
		t.Fatal(err)
	}
	work = nil
	done.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}
