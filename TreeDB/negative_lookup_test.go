package treedb

import (
	"bytes"
	"testing"
)

func TestNegativeLookupCachedMutableQueuedAndCheckpoint(t *testing.T) {
	opts := Options{Dir: t.TempDir(), NegativeLookupFilterBytes: 1024, BackgroundCheckpointInterval: -1, BackgroundCheckpointIdleDuration: -1, BackgroundIndexVacuumInterval: -1, DisableBackgroundPrune: true}
	d, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if got := d.backend.Stats()["treedb.negative_lookup_filter.active_bytes"]; got != "1024" {
		t.Fatal("main filter bytes", got)
	}
	if d.dictdb == nil {
		t.Fatal("side store not opened")
	}
	if got := d.dictdb.Stats()["treedb.negative_lookup_filter.active_bytes"]; got != "0" {
		t.Fatal("side store inherited main budget", got)
	}
	keys := [][]byte{nil, {}, []byte("empty"), {0, 255, 0}, []byte("large")}
	values := [][]byte{{}, {}, {}, {1, 2, 3}, bytes.Repeat([]byte("v"), 4096)}
	for phase := 0; phase < 3; phase++ {
		for i, k := range keys {
			if e := d.Set(k, values[i]); e != nil {
				t.Fatal(e)
			}
		}
		if phase == 0 {
			if value, err := d.backend.Get([]byte("empty")); err != nil || value != nil {
				t.Fatal("fixture already persisted", value, err)
			}
		}
		if phase == 1 {
			it, err := d.Iterator(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := d.Stats()["treedb.cache.queue_len"]; got == "0" || got == "" {
				t.Fatal("fixture did not queue mutable writes", got)
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for i, k := range keys {
			v, e := d.Get(k)
			if e != nil || !bytes.Equal(v, values[i]) || v == nil {
				t.Fatal(i, v, e)
			}
		}
		got, e := d.GetMany(keys)
		if e != nil {
			t.Fatal(e)
		}
		for i, v := range got {
			if !bytes.Equal(v, values[i]) || v == nil {
				t.Fatal(i, v)
			}
		}
		if e := d.GetManyView(keys, func(i int, _, v []byte, found bool) error {
			if !found || !bytes.Equal(v, values[i]) {
				t.Fatal(i, v, found)
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		if e := d.Delete([]byte("empty")); e != nil {
			t.Fatal(e)
		}
		if v, e := d.Get([]byte("empty")); e != nil || v != nil {
			t.Fatal(v, e)
		}
		if e := d.Checkpoint(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestNegativeLookupOpenBackendAndReadOnly(t *testing.T) {
	opts := Options{Dir: t.TempDir(), NegativeLookupFilterBytes: 1024, DisableBackgroundPrune: true}
	backend, cleanup, err := OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := backend.Stats()["treedb.negative_lookup_filter.active_bytes"]; got != "1024" {
		t.Fatal(got)
	}
	if err := backend.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	opts.ReadOnly = true
	backend, cleanup, err = OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if got := backend.Stats()["treedb.negative_lookup_filter.active_bytes"]; got != "1024" {
		t.Fatal(got)
	}
	if value, err := backend.Get([]byte("key")); err != nil || string(value) != "value" {
		t.Fatal(value, err)
	}
}

func TestNegativeLookupQualificationFixture(t *testing.T) {
	for _, payload := range []string{"compressible256", "random4096"} {
		for _, enabled := range []bool{false, true} {
			d, keys, misses := openNegativeLookupBench(t, payload, enabled)
			old := d.AcquireSnapshot()
			update := d.NewBatch()
			if err := update.Set(keys[0], []byte("updated")); err != nil {
				t.Fatal(err)
			}
			if err := update.WriteSync(); err != nil {
				t.Fatal(err)
			}
			update.Close()
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if value, err := old.Get(keys[0]); err != nil || len(value) < 256 {
				t.Fatal("retained snapshot lost original payload", value, err)
			}
			batchKeys := append(append([][]byte{}, keys[:32]...), misses[:32]...)
			values, err := d.GetMany(batchKeys)
			if err != nil {
				t.Fatal(err)
			}
			for i, value := range values {
				if (value != nil) != (i < 32) {
					t.Fatal("fixture hit/miss mismatch", i, value)
				}
			}
			calls := 0
			if err := d.GetManyView(batchKeys, func(i int, _, value []byte, found bool) error {
				calls++
				if found != (i < 32) || (value != nil) != (i < 32) {
					t.Fatal("fixture callback mismatch", i, found, value)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if calls != 64 {
				t.Fatal("callback count", calls)
			}
			old.Close()
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
