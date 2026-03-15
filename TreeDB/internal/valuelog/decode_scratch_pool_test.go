package valuelog

import "testing"

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
