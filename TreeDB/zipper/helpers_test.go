package zipper

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestCloneKeyIntoArena(t *testing.T) {
	arena := make([]byte, 0, 16)
	src := []byte("abc")
	got := cloneKeyIntoArena(src, &arena)
	if !bytes.Equal(got, src) {
		t.Fatalf("clone mismatch: got=%q want=%q", got, src)
	}
	src[0] = 'z'
	if string(got) != "abc" {
		t.Fatalf("clone should be independent of src mutation, got=%q", got)
	}
}

func TestShortestSeparatorIntoArenaMatchesStandalone(t *testing.T) {
	cases := []struct {
		left  []byte
		right []byte
	}{
		{[]byte("a"), []byte("c")},
		{[]byte("abc"), []byte("abz")},
		{[]byte{0, 0, 0, 1}, []byte{0, 0, 0, 2}},
		{bytes.Repeat([]byte{0x00}, 8), bytes.Repeat([]byte{0xFF}, 8)},
	}

	for _, tc := range cases {
		arena := make([]byte, 0, 64)
		got := shortestSeparatorIntoArena(tc.left, tc.right, &arena)
		want := shortestSeparator(tc.left, tc.right)
		if !bytes.Equal(got, want) {
			t.Fatalf("separator mismatch left=%x right=%x got=%x want=%x", tc.left, tc.right, got, want)
		}
	}
}

func TestReserveBytesFromPPM(t *testing.T) {
	if got := reserveBytesFromPPM(1_000_000); got != 0 {
		t.Fatalf("reserveBytesFromPPM(1_000_000)=%d, want 0", got)
	}
	if got := reserveBytesFromPPM(0); got != page.PageSize {
		t.Fatalf("reserveBytesFromPPM(0)=%d, want %d", got, page.PageSize)
	}
	if got := reserveBytesFromPPM(500_000); got != page.PageSize/2 {
		t.Fatalf("reserveBytesFromPPM(500_000)=%d, want %d", got, page.PageSize/2)
	}
}

func TestMaintenanceBudget(t *testing.T) {
	if b := newMaintenanceBudget(0, 0, 10); b != nil {
		t.Fatalf("expected nil budget for zero ops")
	}

	b := newMaintenanceBudget(1000, 10, 200)
	if b == nil {
		t.Fatalf("expected non-nil budget")
	}
	if !b.allow() {
		t.Fatalf("budget should allow initially")
	}
	if !b.take(1) {
		t.Fatalf("take(1) should succeed")
	}
	if b.take(1 << 20) {
		t.Fatalf("take with very large n should fail")
	}
}

func TestSetFillTargets(t *testing.T) {
	z := &Zipper{}
	z.SetFillTargets(900_000, 800_000)
	if z.leafReserveBytes <= 0 || z.internalReserveBytes <= 0 {
		t.Fatalf("expected positive reserve bytes, leaf=%d internal=%d", z.leafReserveBytes, z.internalReserveBytes)
	}
	z.SetFillTargets(1_000_000, 1_000_000)
	if z.leafReserveBytes != 0 || z.internalReserveBytes != 0 {
		t.Fatalf("expected zero reserve at full ppm, leaf=%d internal=%d", z.leafReserveBytes, z.internalReserveBytes)
	}
}
