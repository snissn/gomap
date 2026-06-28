package valuelog

import "testing"

func drainDecodeScratchPoolsForTest() {
	for {
		select {
		case buf := <-smallDecodeScratchPool:
			noteDecodeScratchSmallPoolTake(cap(buf))
		default:
			goto drainLarge
		}
	}

drainLarge:
	for {
		select {
		case buf := <-largeDecodeScratchPool:
			noteDecodeScratchLargePoolTake(cap(buf))
		default:
			return
		}
	}
}

func TestDecodeScratchPool_ReusesSmallBuffer_NoAllocsAfterWarmup(t *testing.T) {
	minCap := 1024

	buf := getDecodeScratch(minCap)
	if cap(buf) < minCap {
		t.Fatalf("cap(buf)=%d < minCap=%d", cap(buf), minCap)
	}
	putDecodeScratch(buf)

	allocs := testing.AllocsPerRun(1000, func() {
		b := getDecodeScratch(minCap)
		if cap(b) < minCap {
			t.Fatalf("cap(b)=%d < minCap=%d", cap(b), minCap)
		}
		putDecodeScratch(b)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs after warm-up, got %f", allocs)
	}
}

func TestDecodeScratchPool_BoundsSmallRetainedBytes(t *testing.T) {
	drainDecodeScratchPoolsForTest()
	t.Cleanup(drainDecodeScratchPoolsForTest)

	before := DecodeScratchStatsSnapshot()
	for i := 0; i < smallDecodeScratchPoolEntries+7; i++ {
		putDecodeScratch(make([]byte, 0, maxDecodeScratchKeep))
	}
	after := DecodeScratchStatsSnapshot()

	if got := after.SmallPoolEntries; got != smallDecodeScratchPoolEntries {
		t.Fatalf("small pool entries=%d want %d", got, smallDecodeScratchPoolEntries)
	}
	if got := after.SmallPoolRetainedBuffers; got != uint64(smallDecodeScratchPoolEntries) {
		t.Fatalf("small retained buffers=%d want %d", got, smallDecodeScratchPoolEntries)
	}
	wantBytes := uint64(smallDecodeScratchPoolEntries * maxDecodeScratchKeep)
	if got := after.SmallPoolRetainedBytes; got != wantBytes {
		t.Fatalf("small retained bytes=%d want %d", got, wantBytes)
	}
	if got := after.SmallPoolDropsTotal - before.SmallPoolDropsTotal; got != 7 {
		t.Fatalf("small pool drops delta=%d want 7", got)
	}
}

func TestDecodeScratchPool_DropRollsBackRetainedGauge(t *testing.T) {
	drainDecodeScratchPoolsForTest()
	t.Cleanup(drainDecodeScratchPoolsForTest)

	for i := 0; i < smallDecodeScratchPoolEntries; i++ {
		putDecodeScratch(make([]byte, 0, maxDecodeScratchKeep))
	}
	before := DecodeScratchStatsSnapshot()
	putDecodeScratch(make([]byte, 0, maxDecodeScratchKeep))
	after := DecodeScratchStatsSnapshot()

	if got := after.SmallPoolRetainedBuffers; got != before.SmallPoolRetainedBuffers {
		t.Fatalf("small retained buffers changed after drop: got %d want %d", got, before.SmallPoolRetainedBuffers)
	}
	if got := after.SmallPoolRetainedBytes; got != before.SmallPoolRetainedBytes {
		t.Fatalf("small retained bytes changed after drop: got %d want %d", got, before.SmallPoolRetainedBytes)
	}
	if got := after.SmallPoolDropsTotal - before.SmallPoolDropsTotal; got != 1 {
		t.Fatalf("small pool drops delta=%d want 1", got)
	}

	drainDecodeScratchPoolsForTest()
	for i := 0; i < largeDecodeScratchPoolEntries; i++ {
		putDecodeScratch(make([]byte, 0, maxDecodeScratchKeep+1))
	}
	before = DecodeScratchStatsSnapshot()
	putDecodeScratch(make([]byte, 0, maxDecodeScratchKeep+1))
	after = DecodeScratchStatsSnapshot()

	if got := after.LargePoolRetainedBuffers; got != before.LargePoolRetainedBuffers {
		t.Fatalf("large retained buffers changed after drop: got %d want %d", got, before.LargePoolRetainedBuffers)
	}
	if got := after.LargePoolRetainedBytes; got != before.LargePoolRetainedBytes {
		t.Fatalf("large retained bytes changed after drop: got %d want %d", got, before.LargePoolRetainedBytes)
	}
	if got := after.LargePoolDropsTotal - before.LargePoolDropsTotal; got != 1 {
		t.Fatalf("large pool drops delta=%d want 1", got)
	}
}

func TestDecodeScratchPool_ReusesLargeBuffer(t *testing.T) {
	// Ensure we exercise the large-scratch pool path (>maxDecodeScratchKeep).
	minCap := maxDecodeScratchKeep + (64 << 10) // 320KiB
	if minCap > maxLargeDecodeScratchKeep {
		t.Fatalf("test expects minCap <= maxLargeDecodeScratchKeep")
	}

	buf := getDecodeScratch(minCap)
	if cap(buf) < minCap {
		t.Fatalf("cap(buf)=%d < minCap=%d", cap(buf), minCap)
	}
	putDecodeScratch(buf)

	allocs := testing.AllocsPerRun(1000, func() {
		b := getDecodeScratch(minCap)
		if cap(b) < minCap {
			t.Fatalf("cap(b)=%d < minCap=%d", cap(b), minCap)
		}
		putDecodeScratch(b)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs after warm-up, got %f", allocs)
	}
}

func TestFileDecodeScratch_RetainsParallelSmallBuffers(t *testing.T) {
	f := &File{}
	first := make([]byte, 0, 32<<10)
	second := make([]byte, 0, 64<<10)
	f.releaseDecodeScratch(first)
	f.releaseDecodeScratch(second)

	gotA := f.takeDecodeScratch(64 << 10)
	if cap(gotA) < 64<<10 {
		t.Fatalf("first take cap=%d want >=64KiB", cap(gotA))
	}
	gotB := f.takeDecodeScratch(32 << 10)
	if cap(gotB) < 32<<10 {
		t.Fatalf("second take cap=%d want >=32KiB", cap(gotB))
	}
	f.releaseDecodeScratch(gotA)
	f.releaseDecodeScratch(gotB)
}

func TestFileDecodeScratch_BoundsPerFileSmallSpares(t *testing.T) {
	f := &File{}
	for i := 0; i < fileDecodeScratchSpareKeep+4; i++ {
		f.releaseDecodeScratch(make([]byte, 0, fileDecodeScratchRetainMaxBytes/2))
	}
	if cap(f.decodeScratch) == 0 {
		t.Fatalf("expected primary decode scratch")
	}
	if got := len(f.decodeScratchSpare); got > fileDecodeScratchSpareKeep {
		t.Fatalf("decodeScratchSpare len=%d want <=%d", got, fileDecodeScratchSpareKeep)
	}
	stats := DecodeScratchStats{}
	f.addDecodeScratchStats(&stats)
	if got := stats.FileRetainedBytes; got > fileDecodeScratchRetainMaxBytes {
		t.Fatalf("file retained bytes=%d want <=%d", got, fileDecodeScratchRetainMaxBytes)
	}
}

func TestFileDecodeScratch_TrimsAfterPrimaryRefill(t *testing.T) {
	t.Cleanup(drainDecodeScratchPoolsForTest)

	f := &File{}
	f.releaseDecodeScratch(make([]byte, 0, fileDecodeScratchRetainMaxBytes/2))
	f.releaseDecodeScratch(make([]byte, 0, fileDecodeScratchRetainMaxBytes/2))
	checkedOut := f.takeDecodeScratch(fileDecodeScratchRetainMaxBytes / 2)
	if cap(checkedOut) < fileDecodeScratchRetainMaxBytes/2 {
		t.Fatalf("checkedOut cap=%d want >=%d", cap(checkedOut), fileDecodeScratchRetainMaxBytes/2)
	}
	if cap(f.decodeScratch) != 0 {
		t.Fatalf("expected empty primary after take, got cap=%d", cap(f.decodeScratch))
	}
	if got := len(f.decodeScratchSpare); got == 0 {
		t.Fatalf("expected retained spare after primary take")
	}

	f.releaseDecodeScratch(make([]byte, 0, fileDecodeScratchRetainMaxBytes))

	stats := DecodeScratchStats{}
	f.addDecodeScratchStats(&stats)
	if got := stats.FileRetainedBytes; got > fileDecodeScratchRetainMaxBytes {
		t.Fatalf("file retained bytes=%d want <=%d", got, fileDecodeScratchRetainMaxBytes)
	}
}

func TestFileDecodeScratch_ReusesLargeBuffer(t *testing.T) {
	minCap := maxDecodeScratchKeep + (64 << 10) // 320KiB
	if minCap > maxLargeDecodeScratchKeep {
		t.Fatalf("test expects minCap <= maxLargeDecodeScratchKeep")
	}

	f := &File{}
	buf := f.takeDecodeScratch(minCap)
	if cap(buf) < minCap {
		t.Fatalf("cap(buf)=%d < minCap=%d", cap(buf), minCap)
	}
	f.releaseDecodeScratch(buf)

	allocs := testing.AllocsPerRun(1000, func() {
		b := f.takeDecodeScratch(minCap)
		if cap(b) < minCap {
			t.Fatalf("cap(b)=%d < minCap=%d", cap(b), minCap)
		}
		f.releaseDecodeScratch(b)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs after warm-up, got %f", allocs)
	}
}
