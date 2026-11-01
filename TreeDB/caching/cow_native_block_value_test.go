package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Use the real writer in an original cache's persistent segment. The ordinary
// seed write registers that segment; frames are appended before the checkpoint
// that pins it. No workspace or decoder is manually prepared by this fixture.
func nativeBlockValueFixture(t *testing.T, codec int, members int) (*DB, *backenddb.DB, *NativeSemanticCapture, []page.ValuePtr, [][]byte) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	compression := vlogCompressionBlock
	if codec < 0 {
		compression = vlogCompressionOff
	}
	db, err := Open(dir, backend, Options{TransactionOwner: owner, DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", FlushThreshold: 1 << 30, JournalLanes: 1, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(compression), ValueLogBlockCodec: uint8(max(codec, 0))})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
		deadline := time.Now().Add(5 * time.Second)
		for db.cow.budget.Stats().TotalBytes != 0 {
			if time.Now().After(deadline) {
				t.Error("fixture final original budget", db.cow.budget.Stats())
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("seed"), bytes.Repeat([]byte("registered-segment"), 256)); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	records := make([]valuelog.Record, members)
	want := make([][]byte, members)
	for i := range records {
		want[i] = bytes.Repeat([]byte(fmt.Sprintf("block-value-member-%d|", i)), 256*(i+1))
		records[i] = valuelog.Record{RID: uint64(100 + i), Value: want[i]}
	}
	lane := &db.lanes[0]
	lane.vlogMu.Lock()
	ptrs, err := lane.vlog.AppendFrame(0, nil, records)
	if err == nil {
		err = lane.vlog.Sync()
	}
	lane.vlogMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	captured, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	capture := captured.(*NativeSemanticCapture)
	t.Cleanup(func() {
		if err := capture.Close(); err != nil {
			t.Error(err)
		}
	})
	f, _, err := capture.Snapshot().PinnedValueLogFile(ptrs[0].FileID)
	if err != nil {
		t.Fatal(err)
	}
	for i, ptr := range ptrs {
		shape, err := valuelog.InspectCOWRecord(f, ptr, db.cow.readLimits())
		if err != nil || shape.Compressed != (codec >= 0) || shape.DictID != 0 || shape.ValueBytes != uint64(len(want[i])) {
			t.Fatalf("actual frame[%d]: %+v/%v", i, shape, err)
		}
		if codec >= 0 && shape.Codec != []valuelog.BlockCodec{valuelog.BlockCodecSnappy, valuelog.BlockCodecLZ4, valuelog.BlockCodecZSTD}[codec] {
			t.Fatalf("actual codec: %+v", shape)
		}
		if !page.ValuePtrIsGrouped(ptr) || page.ValuePtrSubIndex(ptr) != uint8(i) || ptr.Offset != ptrs[0].Offset {
			t.Fatalf("actual grouped K=%d ptrs=%+v", members, ptrs)
		}
	}
	return db, backend, capture, ptrs, want
}

// Releases precede every bounded join, including an adverse baseline stall.
func nativeValueWithHeldPhysical(t *testing.T, cache *cowCache, read func() error) {
	t.Helper()
	done := make(chan error, 1)
	cache.physicalMu.Lock()
	held := true
	defer func() {
		if held {
			cache.physicalMu.Unlock()
		}
	}()
	go func() { done <- read() }()
	select {
	case err := <-done:
		cache.physicalMu.Unlock()
		held = false
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		cache.physicalMu.Unlock()
		held = false
		select {
		case err := <-done:
			t.Fatalf("native value waited for physicalMu: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("native value failed to join after physicalMu release")
		}
	}
}

func TestNativeValueBlockWorkspaceHeldPhysicalMutex(t *testing.T) {
	for codec := -1; codec <= 1; codec++ {
		for _, members := range []int{1, 2} {
			t.Run(fmt.Sprintf("codec=%d/K=%d", codec, members), func(t *testing.T) {
				db, _, capture, ptrs, want := nativeBlockValueFixture(t, codec, members)
				basis, budget := capture.cut.basis, db.cow.budget
				before := budget.Stats()
				read := func() error {
					for i, ptr := range ptrs {
						got, err := capture.NativeValueAtPointer(ptr, nil)
						if err != nil {
							return err
						}
						if !bytes.Equal(got, want[i]) || len(got) != cap(got) {
							return fmt.Errorf("owned member%d differs", i)
						}
						got[0] ^= 255
					}
					return nil
				}
				// The first ordinary read must cold-bootstrap; no grow/codec call.
				if err := read(); err != nil {
					t.Fatal(err)
				}
				nativeValueWithHeldPhysical(t, db.cow, read)
				if basis.reader == nil || len(basis.reader.output) == 0 || budget.Stats().ReservedBytes <= before.ReservedBytes {
					t.Fatal("first ordinary read did not retain charged basis value storage")
				}
				if codec >= 0 && len(basis.reader.codecs) == 0 {
					t.Fatal("missing admitted basis block codec")
				}
				if db.cow.physicalReader != nil {
					t.Fatal("eligible cold read warmed shared owner")
				}
			})
		}
	}
}

func TestNativeValueBlockWorkspaceIntegrityAndOwnership(t *testing.T) {
	db, _, capture, ptrs, want := nativeBlockValueFixture(t, 0, 2)
	calls := 0
	refusal := memtable.ErrCOWCapacity
	nativeValueWithHeldPhysical(t, db.cow, func() error {
		got, err := capture.NativeValueAtPointer(ptrs[0], func(n uint64) error {
			calls++
			if n != memtable.COWAllocationCharge(uint64(len(want[0]))) {
				return fmt.Errorf("output charge=%d", n)
			}
			return refusal
		})
		if got != nil || !errors.Is(err, refusal) || calls != 1 {
			return fmt.Errorf("callback replay/output: calls=%d err=%v", calls, err)
		}
		return nil
	})
	f, _, err := capture.Snapshot().PinnedValueLogFile(ptrs[0].FileID)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	var crc [1]byte
	off := int64(ptrs[0].Offset - 4)
	if _, err := f.ReadAt(crc[:], off); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := writer.WriteAt(crc[:], off); err != nil {
			t.Error(err)
		}
	}()
	if _, err := writer.WriteAt([]byte{crc[0] ^ 1}, off); err != nil {
		t.Fatal(err)
	}
	nativeValueWithHeldPhysical(t, db.cow, func() error {
		for _, ptr := range ptrs {
			got, err := capture.NativeValueAtPointer(ptr, func(uint64) error { calls++; return nil })
			if got != nil || !errors.Is(err, valuelog.ErrCorrupt) {
				return fmt.Errorf("CRC read=%x/%v", got, err)
			}
		}
		if calls != 1 {
			return fmt.Errorf("unverified output callback count=%d", calls)
		}
		return nil
	})
}

// This test adapter changes only the actual decode result after preparation.
// A capacity-like error here must never be mistaken for pre-read refusal.
type nativeBlockRefusingDecoder struct {
	cowReadDecoder
	calls int
}

func (d *nativeBlockRefusingDecoder) Decode(_ valuelog.FrameHeader, _, _ []byte) ([]byte, error) {
	d.calls++
	return nil, memtable.ErrCOWCapacity
}
func TestNativeValueBlockWorkspaceActualReadRefusalIsTerminal(t *testing.T) {
	db, _, capture, ptrs, _ := nativeBlockValueFixture(t, 0, 1)
	if _, err := capture.NativeValueAtPointer(ptrs[0], nil); err != nil {
		t.Fatal(err)
	}
	basis := capture.cut.basis
	basis.readMu.Lock()
	codec := basis.reader.codecs[0]
	decoder := &nativeBlockRefusingDecoder{cowReadDecoder: codec.decoder}
	codec.decoder = decoder
	basis.readMu.Unlock()
	callbacks := 0
	nativeValueWithHeldPhysical(t, db.cow, func() error {
		got, err := capture.NativeValueAtPointer(ptrs[0], func(uint64) error { callbacks++; return nil })
		if got != nil || !errors.Is(err, memtable.ErrCOWCapacity) || decoder.calls != 1 || callbacks != 0 {
			return fmt.Errorf("actual decode refusal replay/output: calls=%d callbacks=%d err=%v", decoder.calls, callbacks, err)
		}
		return nil
	})
}

func TestNativeValueBlockWorkspaceAllSlotsBusy(t *testing.T) {
	db, _, capture, ptrs, want := nativeBlockValueFixture(t, 0, 1)
	basis := capture.cut.basis
	basis.readMu.Lock()
	for i := range basis.readSlots {
		basis.readSlots[i].mu.Lock()
	}
	defer func() {
		for i := range basis.readSlots {
			basis.readSlots[i].mu.Unlock()
		}
		basis.readMu.Unlock()
	}()
	got, err := capture.NativeValueAtPointer(ptrs[0], nil)
	if err != nil || !bytes.Equal(got, want[0]) || db.cow.physicalReader == nil {
		t.Fatalf("busy shared fallback: %x/%v", got, err)
	}
}

func TestNativeValueBlockWorkspacePreparationRefusal(t *testing.T) {
	for _, stage := range []string{"metadata", "codec-slot", "codec", "output"} {
		for _, extra := range []bool{false, true} {
			for _, existing := range []bool{false, true} {
				if existing && stage == "metadata" {
					continue
				}
				t.Run(fmt.Sprintf("stage=%s/extra=%t/existing=%t", stage, extra, existing), func(t *testing.T) {
					db, _, capture, ptrs, want := nativeBlockValueFixture(t, 0, 1)
					basis, budget := capture.cut.basis, db.cow.budget
					// Warm the authoritative shared fallback while all local owners are busy.
					basis.readMu.Lock()
					for i := range basis.readSlots {
						basis.readSlots[i].mu.Lock()
					}
					_, err := capture.NativeValueAtPointer(ptrs[0], nil)
					for i := range basis.readSlots {
						basis.readSlots[i].mu.Unlock()
					}
					basis.readMu.Unlock()
					if err != nil {
						t.Fatal(err)
					}
					mu, owner := &basis.readMu, &basis.reader
					if extra {
						basis.readMu.Lock()
						defer basis.readMu.Unlock()
						mu, owner = &basis.readSlots[0].mu, &basis.readSlots[0].reader
					}
					var retired *cowReadWorkspace
					var stale cowFreshReadOwner
					if existing {
						mu.Lock()
						retired, err = basis.workspaceLocked(owner, budget, db.cow.readLimits())
						if err == nil {
							_, err = retired.pointScratch()
						}
						stale = cowReadHandoffLocked(mu, owner, true, true, nil)
						mu.Unlock()
						if err != nil {
							t.Fatal(err)
						}
					}
					// Price actual admitted constituents through existing operations. The
					// real route below is cold and has none of these probe allocations.
					beforeProbe := budget.Stats().ReservedBytes
					probe, err := admitCOWReadWorkspace(budget, db.cow.readLimits())
					if err != nil {
						t.Fatal(err)
					}
					metadata := budget.Stats().ReservedBytes - beforeProbe
					prefix := uint64(0)
					if stage != "metadata" {
						if !existing {
							prefix = metadata
						}
						if stage == "codec" || stage == "output" {
							before := budget.Stats().ReservedBytes
							if _, err := probe.addCodecSlot(); err != nil {
								probe.close()
								t.Fatal(err)
							}
							prefix += budget.Stats().ReservedBytes - before
						}
						if stage == "output" {
							f, _, err := capture.Snapshot().PinnedValueLogFile(ptrs[0].FileID)
							if err != nil {
								probe.close()
								t.Fatal(err)
							}
							shape, err := valuelog.InspectCOWRecord(f, ptrs[0], db.cow.readLimits())
							if err != nil {
								probe.close()
								t.Fatal(err)
							}
							before := budget.Stats().ReservedBytes
							if _, err := probe.codec(shape); err != nil {
								probe.close()
								t.Fatal(err)
							}
							prefix += budget.Stats().ReservedBytes - before
						}
					}
					probe.close()
					probe = nil
					before := budget.Stats()
					overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
					pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead - prefix)
					if err != nil {
						t.Fatal(err)
					}
					defer pressure.Close()
					calls := 0
					got, err := capture.NativeValueAtPointer(ptrs[0], func(uint64) error { calls++; return nil })
					if err != nil || !bytes.Equal(got, want[0]) || calls != 1 || *owner != nil {
						t.Fatalf("prepare refusal/fallback: len=%d err=%v callbacks=%d owner=%p", len(got), err, calls, *owner)
					}
					if retired != nil && (retired.point != nil || retired.budget != nil || retired.lease != nil || retired.codecs != nil || retired.input != nil || retired.output != nil || retired.basis != nil) {
						t.Fatal("retired owner retains refunded references")
					}
					pressure.Close()
					got, err = capture.NativeValueAtPointer(ptrs[0], nil)
					if err != nil || !bytes.Equal(got, want[0]) || *owner == nil {
						t.Fatal("replacement admission", err)
					}
					replacement := *owner
					if stale.refund() || *owner != replacement {
						t.Fatal("pending token closed replacement identity")
					}
				})
			}
		}
	}
}

func TestNativeValueBlockWorkspaceUnsupportedAndClosedBudget(t *testing.T) {
	t.Run("valid-ZSTD", func(t *testing.T) {
		db, _, capture, ptrs, want := nativeBlockValueFixture(t, 2, 1)
		got, err := capture.NativeValueAtPointer(ptrs[0], nil)
		if err != nil || !bytes.Equal(got, want[0]) || capture.cut.basis.reader != nil || db.cow.physicalReader == nil {
			t.Fatalf("valid unsupported shared fallback: %x/%v", got, err)
		}
	})
	t.Run("fitting-live-capture", func(t *testing.T) {
		db, _, capture, ptrs, want := nativeBlockValueFixture(t, 0, 2)
		if _, err := capture.NativeValueAtPointer(ptrs[0], nil); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		nativeValueWithHeldPhysical(t, db.cow, func() error {
			got, err := capture.NativeValueAtPointer(ptrs[0], nil)
			if err != nil || !bytes.Equal(got, want[0]) {
				return fmt.Errorf("closed budget fit: %v", err)
			}
			return nil
		})
		calls := 0
		got, err := capture.NativeValueAtPointer(ptrs[1], func(uint64) error { calls++; return nil })
		if got != nil || !errors.Is(err, memtable.ErrCOWClosed) || calls != 0 || capture.cut.basis.reader != nil {
			t.Fatalf("closed growth retirement: len=%d err=%v callbacks=%d", len(got), err, calls)
		}

	})
}

func TestNativeValueBlockWorkspaceConcurrentClose(t *testing.T) {
	db, backend, capture, ptrs, want := nativeBlockValueFixture(t, 1, 2)
	budget := db.cow.budget
	var scopes []*NativeSemanticCapture
	for range 4 {
		c, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		scopes = append(scopes, c.(*NativeSemanticCapture))
	}
	defer func() {
		for _, c := range scopes {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	basis := capture.cut.basis
	var held []*sync.Mutex
	defer func() {
		for _, mu := range held {
			mu.Unlock()
		}
	}()
	for i, c := range scopes {
		for j, ptr := range ptrs {
			got, err := c.NativeValueAtPointer(ptr, nil)
			if err != nil || !bytes.Equal(got, want[j]) {
				t.Fatal("warm retained owner", err)
			}
		}
		mu := &basis.readMu
		if i > 0 {
			mu = &basis.readSlots[i-1].mu
		}
		mu.Lock()
		held = append(held, mu)
	}
	for j, ptr := range ptrs {
		got, err := capture.NativeValueAtPointer(ptr, nil)
		if err != nil || !bytes.Equal(got, want[j]) {
			t.Fatal("warm shared fallback", err)
		}
	}
	for _, mu := range held {
		mu.Unlock()
	}
	held = nil
	var wg sync.WaitGroup
	errs := make(chan error, len(scopes))
	checkpointed := make(chan struct{})
	defer func() {
		select {
		case <-checkpointed:
		default:
			close(checkpointed)
		}
	}()
	for _, c := range scopes {
		wg.Add(1)
		go func(c *NativeSemanticCapture) {
			defer wg.Done()
			for n := range 16 {
				if n == 8 {
					<-checkpointed
				}
				for i, ptr := range ptrs {
					got, err := c.NativeValueAtPointer(ptr, nil)
					if err != nil || !bytes.Equal(got, want[i]) {
						errs <- fmt.Errorf("retained read: %v", err)
						return
					}
				}
			}
		}(c)
	}
	if err := db.Set([]byte("after"), bytes.Repeat([]byte("after"), 1024)); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	closing := make(chan struct{})
	go func() { close(closing); closed <- db.Close() }()
	<-closing
	close(checkpointed)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent original Close did not join")
	}
	for _, c := range scopes {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for budget.Stats().TotalBytes != 0 {
		if time.Now().After(deadline) {
			t.Fatal("final budget not zero", budget.Stats())
		}
		time.Sleep(time.Millisecond)
	}
}
