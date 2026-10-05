package valuelog

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestGroupedFrameCache_OffsetBackingLifecycle(t *testing.T) {
	// Characterizes the retention regression without requiring a different test
	// source for the previous inline-array representation.
	field, _ := reflect.TypeOf(groupedFrameCacheSlot{}).FieldByName("offsets")
	if field.Type.Kind() != reflect.Slice {
		t.Fatalf("offset metadata is retained in every allocated slot: %v", field.Type)
	}
	c := newGroupedFrameCache(nil, 2048, 1024, 0, nil)
	offsets := groupedCacheOffsets(1)
	if !c.store(100, false, 1, offsets, []byte("a"), false) {
		t.Fatal("initial admission")
	}
	s := c.shardFor(100, false)
	var slot *groupedFrameCacheSlot
	for i := range s.slots {
		if s.slots[i].valid {
			slot = &s.slots[i]
			if len(slot.offsets) != 2 || cap(slot.offsets) != 2 {
				t.Fatalf("small group offsets len/cap=%d/%d, want 2/2", len(slot.offsets), cap(slot.offsets))
			}
		} else if cap(s.slots[i].offsets) != 0 {
			t.Fatal("unused slot retained offset backing")
		}
	}
	if slot == nil {
		t.Fatal("missing admitted slot")
	}
	backing := &slot.offsets[0]
	if !c.store(100, false, 1, offsets, []byte("b"), false) || &slot.offsets[0] != backing {
		t.Fatal("same-sized replacement did not reuse offsets")
	}
	large := groupedCacheOffsets(1, 1, 1, 1)
	if !c.store(100, false, 4, large, []byte("cdef"), false) {
		t.Fatal("larger replacement")
	}
	if len(slot.offsets) != 5 || cap(slot.offsets) != 5 {
		t.Fatalf("grown offsets len/cap=%d/%d, want 5/5", len(slot.offsets), cap(slot.offsets))
	}
	backing = &slot.offsets[0]
	if !c.store(100, false, 1, offsets, []byte("g"), false) || &slot.offsets[0] != backing || len(slot.offsets) != 2 {
		t.Fatal("smaller replacement did not reuse and resize offsets")
	}
	got, _, err, hit := c.readTo(100, false, 1, &offsets, 1, 0, nil, nil)
	if err != nil || !hit || !bytes.Equal(got, []byte("g")) {
		t.Fatalf("changed-K replacement: hit=%v got=%q err=%v", hit, got, err)
	}
	s.mu.Lock()
	s.evictSlotLocked(c, 0)
	s.mu.Unlock()
	if len(slot.offsets) != 0 || cap(slot.offsets) != 5 {
		t.Fatalf("eviction offsets len/cap=%d/%d, want 0/5", len(slot.offsets), cap(slot.offsets))
	}
	c.clear()
	if cap(slot.offsets) != 0 {
		t.Fatal("clear retained evicted offset backing")
	}
	if !c.store(100, false, 1, offsets, []byte("h"), false) {
		t.Fatal("admission after clear")
	}
	c.clear()
	if cap(slot.offsets) != 0 {
		t.Fatal("clear retained live offset backing")
	}
}

func TestGroupedFrameCache_OffsetBackingCloseAndMaxK(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "vlog")
	if err != nil {
		t.Fatal(err)
	}
	f := newTestGroupedCacheFile(1, 1024, 0)
	f.File = file
	var offsets [MaxFrameK + 1]uint32
	for i := range offsets {
		offsets[i] = uint32(i)
	}
	raw := bytes.Repeat([]byte{'x'}, MaxFrameK)
	if !f.groupedFrameCacheStore(100, true, MaxFrameK, offsets, raw, false) {
		t.Fatal("maximum-K admission")
	}
	c := f.groupedFrameCache.Load()
	slot := &c.shardFor(100, true).slots[0]
	if len(slot.offsets) != MaxFrameK+1 || cap(slot.offsets) != MaxFrameK+1 {
		t.Fatalf("maximum-K offsets len/cap=%d/%d", len(slot.offsets), cap(slot.offsets))
	}
	got, _, err, hit := f.groupedFrameCacheReadTo(100, true, MaxFrameK, &offsets, MaxFrameK, MaxFrameK-1, nil)
	if err != nil || !hit || !bytes.Equal(got, []byte{'x'}) {
		t.Fatalf("last maximum-K value: hit=%v got=%q err=%v", hit, got, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if cap(slot.offsets) != 0 || c.stats().RetainedBytes != 0 {
		t.Fatal("close retained offset backing or raw bytes")
	}
}

// metadata-B counts slot structures plus offset backing capacity. It excludes
// allocator size-class rounding, shard structures and raw payloads, and is not
// a process RSS or live-heap measurement. Cold admission includes cache/shard
// construction; warm replacement exposes whether offset backing is reused.
func BenchmarkGroupedFrameCacheOffsets(b *testing.B) {
	for _, k := range []int{1, 4, 32, MaxFrameK} {
		var offsets [MaxFrameK + 1]uint32
		for i := 0; i <= k; i++ {
			offsets[i] = uint32(i)
		}
		raw := bytes.Repeat([]byte{'a'}, k)
		for _, op := range []string{"hit", "miss", "store"} {
			b.Run(fmt.Sprintf("K%d/%s", k, op), func(b *testing.B) {
				c := newGroupedFrameCache(nil, 2048, 1024, 0, nil)
				for start := int64(0); start < 8192; start++ {
					if !c.store(start, false, k, offsets, raw, false) {
						b.Fatal("warm admission")
					}
				}
				var metadata int
				for i := range c.shards {
					metadata += len(c.shards[i].slots) * int(reflect.TypeOf(groupedFrameCacheSlot{}).Size())
					for j := range c.shards[i].slots {
						field := reflect.ValueOf(&c.shards[i].slots[j]).Elem().FieldByName("offsets")
						if field.Kind() == reflect.Slice {
							metadata += field.Cap() * 4
						}
					}
				}
				b.ReportAllocs()
				dst := make([]byte, 1)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					switch op {
					case "hit", "miss":
						start := int64(8191)
						if op == "miss" {
							start = 9000
						}
						_, _, err, hit := c.readTo(start, false, k, &offsets, uint32(k), 0, dst, nil)
						if err != nil || hit != (op == "hit") {
							b.Fatalf("%s hit=%v err=%v", op, hit, err)
						}
					case "store":
						if !c.store(8191, false, k, offsets, raw, false) {
							b.Fatal("replacement")
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(metadata), "metadata-B")
				b.ReportMetric(float64(metadata)/float64(c.stats().AllocatedSlots), "metadata-B/slot")
			})
		}
		b.Run(fmt.Sprintf("K%d/cold", k), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c := newGroupedFrameCache(nil, 2048, 1024, 0, nil)
				if !c.store(100, false, k, offsets, raw, false) {
					b.Fatal("cold admission")
				}
			}
		})
	}
}
