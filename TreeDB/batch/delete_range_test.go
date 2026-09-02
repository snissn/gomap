package batch

import (
	"bytes"
	"testing"
)

func TestBatchDeleteRangeReplayPreservesOrderAndNilBounds(t *testing.T) {
	b := New(nil, -1)
	defer b.Close()
	if err := b.Set([]byte("b"), []byte("before")); err != nil {
		t.Fatalf("Set before: %v", err)
	}
	if err := b.DeleteRange(nil, []byte("c")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Set([]byte("b"), []byte("after")); err != nil {
		t.Fatalf("Set after: %v", err)
	}
	if err := b.DeleteRange([]byte("z"), []byte("a")); err != nil {
		t.Fatalf("reversed DeleteRange: %v", err)
	}

	var got []Entry
	if err := b.Replay(func(e Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Replay entries=%d want 3: %+v", len(got), got)
	}
	if got[1].Type != OpDeleteRange || got[1].Key != nil || !bytes.Equal(got[1].Value, []byte("c")) {
		t.Fatalf("range entry mismatch: %+v", got[1])
	}
}

func TestBatchDeleteRangeApplyPlanOverlappingRangesKeepOnlyLaterShadowedPoints(t *testing.T) {
	b := New(nil, -1)
	defer b.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(b.DeleteRange([]byte("a"), []byte("d")))
	must(b.Set([]byte("b"), []byte("after-first-range")))
	must(b.Set([]byte("e"), []byte("shadowed-by-second-range")))
	must(b.DeleteRange([]byte("c"), []byte("f")))
	must(b.Set([]byte("g"), []byte("after-ranges")))

	points, ranges := b.ApplyPlan()
	if len(ranges) != 1 || !bytes.Equal(ranges[0].Start, []byte("a")) || !bytes.Equal(ranges[0].End, []byte("f")) {
		t.Fatalf("ranges mismatch: %+v", ranges)
	}
	if len(points) != 2 {
		t.Fatalf("points=%d want 2: %+v", len(points), points)
	}
	if points[0].Type != OpPut || !bytes.Equal(points[0].Key, []byte("b")) || !bytes.Equal(points[0].Value, []byte("after-first-range")) {
		t.Fatalf("point[0] mismatch: %+v", points[0])
	}
	if points[1].Type != OpPut || !bytes.Equal(points[1].Key, []byte("g")) || !bytes.Equal(points[1].Value, []byte("after-ranges")) {
		t.Fatalf("point[1] mismatch: %+v", points[1])
	}
}

func TestBatchDeleteRangeApplyPlanHonorsOrderedPointInteractions(t *testing.T) {
	b := New(nil, -1)
	defer b.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(b.Set([]byte("b"), []byte("shadowed")))
	must(b.DeleteRange([]byte("a"), []byte("d")))
	must(b.Set([]byte("c"), []byte("survives")))
	must(b.Delete([]byte("e")))

	points, ranges := b.ApplyPlan()
	if len(ranges) != 1 || !bytes.Equal(ranges[0].Start, []byte("a")) || !bytes.Equal(ranges[0].End, []byte("d")) {
		t.Fatalf("ranges mismatch: %+v", ranges)
	}
	if len(points) != 2 {
		t.Fatalf("points=%d want 2: %+v", len(points), points)
	}
	if points[0].Type != OpPut || !bytes.Equal(points[0].Key, []byte("c")) || !bytes.Equal(points[0].Value, []byte("survives")) {
		t.Fatalf("point[0] mismatch: %+v", points[0])
	}
	if points[1].Type != OpDelete || !bytes.Equal(points[1].Key, []byte("e")) {
		t.Fatalf("point[1] mismatch: %+v", points[1])
	}
}
