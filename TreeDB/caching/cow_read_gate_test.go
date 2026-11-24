package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCOWBasisReadSlotPadding(t *testing.T) {
	if sz := unsafe.Sizeof(cowBasisReadSlot{}); sz != 64 {
		t.Fatalf("cowBasisReadSlot size = %d, want 64", sz)
	}
	var basis cowBackendBasis
	if off := unsafe.Offsetof(basis.readSlots); off%64 != 0 {
		t.Fatalf("cowBackendBasis.readSlots offset = %d, want multiple of 64", off)
	}
	total := len(basis.readSlots) + 1
	locked := make([]*sync.Mutex, 0, total)
	seen := make(map[*sync.Mutex]bool, total)
	for seed := uint32(0); seed < uint32(total); seed++ {
		mu, _, ok := basis.tryLockReadLane(seed)
		if !ok {
			for _, m := range locked {
				m.Unlock()
			}
			t.Fatalf("tryLockReadLane(%d) failed with %d/%d lanes held", seed, len(locked), total)
		}
		locked = append(locked, mu)
		seen[mu] = true
	}
	if _, _, ok := basis.tryLockReadLane(0); ok {
		t.Fatal("tryLockReadLane succeeded with all 8 lanes held")
	}
	for _, m := range locked {
		m.Unlock()
	}
	if len(seen) != total {
		t.Fatalf("distinct locked read lanes = %d, want %d", len(seen), total)
	}
}

func TestCOWReadGateStripeDistributionAndLocking(t *testing.T) {
	if sz := unsafe.Sizeof(cowReadGateShard{}); sz != 64 {
		t.Fatalf("cowReadGateShard size = %d, want 64", sz)
	}
	if sz := unsafe.Sizeof(cowBasisReadSlot{}); sz != 64 {
		t.Fatalf("cowBasisReadSlot size = %d, want 64", sz)
	}
	if cowReadStripe(nil) != 0 || cowReadStripe([]byte{}) != 0 {
		t.Fatalf("empty key must hash to stripe 0")
	}

	// Verify uniform distribution for 8-byte big-endian keys (be8).
	const totalKeys = 16384
	var be8Counts [cowReadGateShards]int
	var key8 [8]byte
	for i := uint64(0); i < totalKeys; i++ {
		binary.BigEndian.PutUint64(key8[:], i)
		be8Counts[cowReadStripe(key8[:])]++
	}
	const expected = totalKeys / cowReadGateShards
	for i, c := range be8Counts {
		if c < expected/2 || c > expected*3/2 {
			t.Fatalf("be8 stripe distribution skewed at stripe %d: count=%d expected~%d all=%v", i, c, expected, be8Counts)
		}
	}

	// Verify uniform distribution for variable-length keys.
	var varCounts [cowReadGateShards]int
	for i := 0; i < totalKeys; i++ {
		k := fmt.Sprintf("key-%06d", i)
		varCounts[cowReadStripe([]byte(k))]++
	}
	for i, c := range varCounts {
		if c < expected/2 || c > expected*3/2 {
			t.Fatalf("var-len stripe distribution skewed at stripe %d: count=%d expected~%d all=%v", i, c, expected, varCounts)
		}
	}

	// Zero-allocation check for both 8-byte and non-8-byte keys.
	var sink uint32
	if allocs := testing.AllocsPerRun(100, func() {
		sink ^= cowReadStripe(key8[:])
		sink ^= cowReadStripe([]byte("variable-length-key"))
	}); allocs != 0 {
		t.Fatalf("cowReadStripe allocated %.0f objects, want 0", allocs)
	}
	_ = sink

	// Verify tryLockAll / unlockAll / tryRLockStripe / rUnlockStripe.
	var gate cowReadGate
	if !gate.tryLockAll() {
		t.Fatal("tryLockAll failed on unlocked gate")
	}
	if gate.tryRLockStripe(7) {
		gate.rUnlockStripe(7)
		gate.unlockAll()
		t.Fatal("tryRLockStripe succeeded while gate exclusively locked")
	}
	gate.unlockAll()

	gate.rLockStripe(11)
	if gate.tryLockAll() {
		gate.unlockAll()
		gate.rUnlockStripe(11)
		t.Fatal("tryLockAll succeeded while stripe 11 was RLocked")
	}
	// Earlier stripes (0..10) must have been unlocked on failure.
	if !gate.tryRLockStripe(0) {
		gate.rUnlockStripe(11)
		t.Fatal("stripe 0 remained locked after failed tryLockAll")
	}
	gate.rUnlockStripe(0)
	gate.rUnlockStripe(11)

	if !gate.tryLockAll() {
		t.Fatal("tryLockAll failed after releasing all RLock holders")
	}
	gate.unlockAll()
}

func TestCOWReadGateConcurrentSnapshotAndPointReadsClose(t *testing.T) {
	db := cowPointReadFixture(t)
	const keyCount = 64
	keys := make([][]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], uint64(i))
		keys[i] = append([]byte(nil), k[:]...)
		val := bytes.Repeat([]byte{byte('a' + (i % 26))}, 32)
		if err := db.Set(keys[i], val); err != nil {
			t.Fatal(err)
		}
	}

	// Shared long-lived snapshot pinned to the initial cut.
	pinned := db.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}

	const readers = 8
	var stop atomic.Bool
	errCh := make(chan error, readers+4)
	var wg sync.WaitGroup

	// Parallel Snapshot.GetAppend + DB.GetAppend readers across many keys/stripes.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			dst := make([]byte, 0, 256)
			for iter := 0; !stop.Load() && iter < 4000; iter++ {
				idx := (id*17 + iter) % keyCount
				key := keys[idx]
				wantByte := byte('a' + (idx % 26))

				// Read through shared Snapshot (may race with pinned.Close() and db.Close()).
				got, err := pinned.GetAppend(key, dst[:0])
				if err != nil {
					if !errors.Is(err, backenddb.ErrClosed) {
						errCh <- fmt.Errorf("pinned.GetAppend(%d): %v", idx, err)
						return
					}
				} else if len(got) != 32 || !bytes.Equal(got, bytes.Repeat([]byte{wantByte}, 32)) {
					errCh <- fmt.Errorf("pinned.GetAppend(%d): unexpected value len=%d", idx, len(got))
					return
				}

				// Direct DB.GetAppend racing concurrent writes, rollovers, and Close.
				got, err = db.GetAppend(key, dst[:0])
				if err != nil {
					if !errors.Is(err, backenddb.ErrClosed) && !errors.Is(err, tree.ErrKeyNotFound) {
						errCh <- fmt.Errorf("db.GetAppend(%d): %v", idx, err)
						return
					}
				} else if len(got) != 32 || !bytes.Equal(got, bytes.Repeat(got[:1], 32)) {
					errCh <- fmt.Errorf("db.GetAppend(%d): torn value len=%d", idx, len(got))
					return
				}

				// Ephemeral Snapshot acquire/read/close.
				if iter%32 == 0 {
					s := db.AcquireSnapshot()
					if s != nil {
						out, sErr := s.GetAppend(key, dst[:0])
						if sErr != nil && !errors.Is(sErr, backenddb.ErrClosed) && !errors.Is(sErr, tree.ErrKeyNotFound) {
							_ = s.Close()
							errCh <- fmt.Errorf("ephemeral snap.GetAppend(%d): %v", idx, sErr)
							return
						}
						if sErr == nil && (len(out) != 32 || !bytes.Equal(out, bytes.Repeat(out[:1], 32))) {
							_ = s.Close()
							errCh <- fmt.Errorf("ephemeral snap.GetAppend(%d): torn value len=%d", idx, len(out))
							return
						}
						if cErr := s.Close(); cErr != nil {
							errCh <- fmt.Errorf("ephemeral snap.Close: %v", cErr)
							return
						}
					}
				}
			}
		}(r)
	}

	// Concurrent writer performing updates, rollovers, and checkpoints.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 32; round++ {
			for i := 0; i < 8; i++ {
				idx := (round*8 + i) % keyCount
				val := bytes.Repeat([]byte{byte('A' + ((round + idx) % 26))}, 32)
				if err := db.Set(keys[idx], val); err != nil {
					if !errors.Is(err, backenddb.ErrClosed) {
						errCh <- fmt.Errorf("Set round=%d: %v", round, err)
					}
					return
				}
			}
			if round%4 == 2 {
				if err := db.rolloverCOWForFlush(); err != nil && !errors.Is(err, backenddb.ErrClosed) {
					errCh <- fmt.Errorf("rollover round=%d: %v", round, err)
					return
				}
			}
			if round%8 == 7 {
				if err := db.Checkpoint(); err != nil && !errors.Is(err, backenddb.ErrClosed) {
					errCh <- fmt.Errorf("Checkpoint round=%d: %v", round, err)
					return
				}
			}
			if round == 16 {
				if err := pinned.Close(); err != nil {
					errCh <- fmt.Errorf("pinned.Close: %v", err)
					return
				}
			}
		}
		stop.Store(true)
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Ensure pinned snapshot stayed finalized with closed bit intact and no underflow.
	if !pinned.finalized.Load() {
		t.Fatal("pinned snapshot was not finalized")
	}
	if state := pinned.readState.Load(); state != snapshotReadClosedBit {
		t.Fatalf("pinned readState = 0x%x, want 0x%x (snapshotReadClosedBit)", state, snapshotReadClosedBit)
	}

	// Hammer stale post-Close reads on pinned to confirm no underflow or re-finalization.
	var staleWG sync.WaitGroup
	for g := 0; g < 8; g++ {
		staleWG.Add(1)
		go func(id int) {
			defer staleWG.Done()
			for i := 0; i < 500; i++ {
				if _, err := pinned.GetAppend(keys[(id+i)%keyCount], nil); !errors.Is(err, backenddb.ErrClosed) {
					t.Errorf("stale GetAppend = %v, want ErrClosed", err)
					return
				}
			}
		}(g)
	}
	staleWG.Wait()
	if !pinned.finalized.Load() {
		t.Fatal("stale reads cleared finalized bit")
	}
	if state := pinned.readState.Load(); state != snapshotReadClosedBit {
		t.Fatalf("stale reads corrupted readState = 0x%x, want 0x%x", state, snapshotReadClosedBit)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.cow.activeCuts.Load() != 0 {
		t.Fatalf("activeCuts = %d after Close, want 0", db.cow.activeCuts.Load())
	}
	if stats := db.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 || stats.Views != 0 {
		t.Fatalf("leaked COW memory stats after Close: %+v", stats)
	}
}

func TestCOWSnapshotCloseConcurrentStaleReadNoUnderflow(t *testing.T) {
	db := cowPointReadFixture(t)
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 64; round++ {
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot returned nil")
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 64; i++ {
					got, err := snap.GetAppend([]byte("k"), nil)
					if err != nil {
						if !errors.Is(err, backenddb.ErrClosed) {
							t.Errorf("unexpected error: %v", err)
							return
						}
					} else if string(got) != "v" {
						t.Errorf("unexpected value: %q", got)
						return
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			runtime.Gosched()
			if err := snap.Close(); err != nil {
				t.Errorf("snap.Close: %v", err)
			}
		}()
		close(start)
		wg.Wait()
		if !snap.finalized.Load() {
			t.Fatalf("round %d: snapshot not finalized after all readers and Close finished", round)
		}
		if state := snap.readState.Load(); state != snapshotReadClosedBit {
			t.Fatalf("round %d: readState = 0x%x, want 0x%x", round, state, snapshotReadClosedBit)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := db.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 || stats.Views != 0 {
		t.Fatalf("leaked COW memory stats: %+v", stats)
	}
}

func TestCOWActivateNativeSemanticOwnerRacesConcurrentReaders(t *testing.T) {
	cache, backend := nativeActivationFixture(t, false)
	c := cache.cow

	// Verify quiescent activation works cleanly after concurrent readers stop,
	// and racing readers with ActivateNativeSemanticOwner is race-free.
	const workers = 8
	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan error, workers+2)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(id))
			for !stop.Load() {
				if id%2 == 0 {
					cut := c.retainCut()
					if cut != nil {
						if err := c.releaseCut(cut); err != nil {
							errCh <- err
							return
						}
					}
				} else {
					_, err := cache.GetAppend(key[:], nil)
					if err != nil && !errors.Is(err, tree.ErrKeyNotFound) && !errors.Is(err, backenddb.ErrClosed) {
						errCh <- err
						return
					}
				}
			}
		}(w)
	}

	activated := false
	for i := 0; i < 200; i++ {
		err := backend.ActivateNativeSemanticOwner()
		if err == nil {
			activated = true
			break
		}
		if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("ActivateNativeSemanticOwner unexpected error: %v", err)
		}
		runtime.Gosched()
	}

	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Once readers are quiescent, refs must be restored to 1 and activation must succeed if not already activated.
	if refs := c.cut.refs.Load(); refs != 1 {
		t.Fatalf("quiescent cut refs = %d, want 1", refs)
	}
	if !activated {
		if err := backend.ActivateNativeSemanticOwner(); err != nil {
			t.Fatalf("quiescent ActivateNativeSemanticOwner failed: %v", err)
		}
		if refs := c.cut.refs.Load(); refs != 1 {
			t.Fatalf("post-activation cut refs = %d, want 1", refs)
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 || stats.Views != 0 {
		t.Fatalf("leaked COW memory stats after Close: %+v", stats)
	}
}
