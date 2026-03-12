package batch

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBatchSetCopiesKeyAndValue(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	key := []byte("k1")
	val := []byte("v1")
	if err := b.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Mutate inputs after Set to ensure the batch stored copies.
	key[0] = 'K'
	val[0] = 'V'

	if got := b.entries; len(got) != 1 {
		t.Fatalf("entries len=%d want=1", len(got))
	}
	if got := b.entries[0].Key; !bytes.Equal(got, []byte("k1")) {
		t.Fatalf("key=%q want=%q", got, "k1")
	}
	if got := b.entries[0].Value; !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("value=%q want=%q", got, "v1")
	}
}

func TestBatchSet_AllocFreeAfterWarm(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	key := []byte("key")
	val := []byte("value")
	if err := b.Set(key, val); err != nil {
		t.Fatalf("warm Set: %v", err)
	}
	b.Reset()

	allocs := testing.AllocsPerRun(1000, func() {
		if err := b.Set(key, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
		b.Reset()
	})
	if allocs != 0 {
		t.Fatalf("allocs/run=%f want=0", allocs)
	}
}
