package tree

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestNegativeFilterPointEntrancesNeverDescend(t *testing.T) {
	f := NewNegativeFilter(1024)
	tr := New(nil, nil, 99) // Any actual descent fails on the missing pager.
	tr.SetNegativeFilter(f)
	k := []byte("definitely absent")
	if _, err := tr.GetEntry(k); !errors.Is(err, ErrKeyNotFound) {
		t.Fatal(err)
	}
	if _, err := tr.GetUnsafe(k); !errors.Is(err, ErrKeyNotFound) {
		t.Fatal(err)
	}
	if _, err := tr.GetAppend(k, nil); !errors.Is(err, ErrKeyNotFound) {
		t.Fatal(err)
	}
	if _, rev, err := tr.GetVersionedAppend(k, nil); !errors.Is(err, ErrKeyNotFound) || rev != page.LegacyEntryRevision {
		t.Fatal(rev, err)
	}
	if found, err := tr.Has(k); found || err != nil {
		t.Fatal(found, err)
	}
	if _, _, err := tr.findLeafRefForGetMany(k, false); !errors.Is(err, ErrKeyNotFound) {
		t.Fatal(err)
	}
	if err := tr.GetManyView([][]byte{k}, func(_ int, _, v []byte, found bool) error {
		if found || v != nil {
			t.Fatal(found, v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	out := make([][]byte, 1)
	if _, err := tr.GetManyAppend([][]byte{k}, out, nil); err != nil || out[0] != nil {
		t.Fatal(out, err)
	}
	tr.Reset(nil, nil, 99)
	if tr.negativeFilter != nil {
		t.Fatal("Reset retained coverage")
	}
}

func TestNegativeFilterMonotonicConcurrentAndBounded(t *testing.T) {
	f := NewNegativeFilter(2048)
	keys := [][]byte{nil, {}, {0, 255, 0}, bytes.Repeat([]byte("x"), NegativeFilterMaxKeyBytes)}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				for _, k := range keys {
					f.Add(k)
					if f.DefinitelyAbsent(k) {
						t.Error("false negative")
					}
				}
			}
		}()
	}
	wg.Wait()
	if f.DefinitelyAbsent(bytes.Repeat([]byte("x"), NegativeFilterMaxKeyBytes+1)) {
		t.Fatal("oversized key rejected")
	}
	if got := testing.AllocsPerRun(100, func() { f.DefinitelyAbsent(keys[2]) }); got != 0 {
		t.Fatal(got)
	}
	for _, size := range []int{-1, 0, 7, 64<<20 + 1} {
		if NewNegativeFilter(size) != nil {
			t.Fatal(size)
		}
	}
	saturated := NewNegativeFilter(8)
	for i := 0; i < 10000; i++ {
		saturated.Add([]byte{byte(i), byte(i >> 8)})
	}
	if saturated.DefinitelyAbsent([]byte("anything")) {
		t.Fatal("saturation should degrade to exact")
	}
}
