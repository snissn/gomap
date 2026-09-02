package treedb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"
)

type modelKV struct {
	live map[uint64][]byte
}

func newModelKV() *modelKV {
	return &modelKV{live: make(map[uint64][]byte)}
}

func (m *modelKV) set(k uint64, v []byte) {
	if v == nil {
		// Treat nil as an invalid value; TreeDB rejects nil writes.
		v = []byte{}
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	m.live[k] = cp
}

func (m *modelKV) del(k uint64) {
	delete(m.live, k)
}

func (m *modelKV) get(k uint64) ([]byte, bool) {
	v, ok := m.live[k]
	if !ok {
		return nil, false
	}
	return v, true
}

func (m *modelKV) keysInRange(start, end *uint64) []uint64 {
	out := make([]uint64, 0, len(m.live))
	for k := range m.live {
		if start != nil && k < *start {
			continue
		}
		if end != nil && k >= *end {
			continue
		}
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func u64Key(k uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], k)
	return buf[:]
}

func assertPointReadMatchesModel(t *testing.T, db *DB, model *modelKV, k uint64) {
	t.Helper()
	got, err := db.Get(u64Key(k))
	if err != nil {
		t.Fatalf("get k=%d: %v", k, err)
	}
	want, ok := model.get(k)
	if !ok {
		if got != nil {
			t.Fatalf("get k=%d: got=%dB want missing", k, len(got))
		}
		return
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("get k=%d mismatch got_len=%d want_len=%d", k, len(got), len(want))
	}
}

func assertIteratorRangeMatchesModel(t *testing.T, db *DB, model *modelKV, start, end *uint64, reverse bool) {
	t.Helper()
	var (
		it  Iterator
		err error
	)
	if reverse {
		it, err = db.ReverseIterator(u64KeyOrNil(start), u64KeyOrNil(end))
	} else {
		it, err = db.Iterator(u64KeyOrNil(start), u64KeyOrNil(end))
	}
	if err != nil {
		t.Fatalf("iterator reverse=%t start=%v end=%v: %v", reverse, start, end, err)
	}
	defer it.Close()

	wantKeys := model.keysInRange(start, end)
	if reverse {
		slices.Reverse(wantKeys)
	}

	gotKeys := make([]uint64, 0, len(wantKeys))
	for it.Valid() {
		kb := it.KeyCopy(nil)
		if len(kb) != 8 {
			t.Fatalf("iterator key len=%d want 8", len(kb))
		}
		k := binary.BigEndian.Uint64(kb)
		gotKeys = append(gotKeys, k)
		wantVal, ok := model.get(k)
		if !ok {
			t.Fatalf("iterator returned phantom key=%d", k)
		}
		if gotVal := it.Value(); !bytes.Equal(gotVal, wantVal) {
			t.Fatalf("iterator value mismatch key=%d got_len=%d want_len=%d", k, len(gotVal), len(wantVal))
		}
		it.Next()
	}
	if ierr := it.Error(); ierr != nil {
		t.Fatalf("iterator error: %v", ierr)
	}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("iterator key count mismatch got=%d want=%d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("iterator key mismatch at %d got=%d want=%d", i, gotKeys[i], wantKeys[i])
		}
	}
}

func u64KeyOrNil(v *uint64) []byte {
	if v == nil {
		return nil
	}
	return u64Key(*v)
}

func TestProfileFast_ModelCorrectness_WithFlushAndIterators(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}

	opts := OptionsFor(ProfileFast, t.TempDir())
	opts.FlushThreshold = 1 << 16 // 64KiB (force frequent rotations/flushes)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	model := newModelKV()
	r := rand.New(rand.NewSource(1))

	const (
		steps    = 4000
		keySpace = 500
	)

	for step := 0; step < steps; step++ {
		k := uint64(r.Intn(keySpace))
		switch p := r.Intn(100); {
		case p < 55:
			// Set
			n := 1 + r.Intn(200)
			val := make([]byte, n)
			_, _ = r.Read(val)
			if err := db.Set(u64Key(k), val); err != nil {
				t.Fatalf("step=%d set k=%d: %v", step, k, err)
			}
			model.set(k, val)
			assertPointReadMatchesModel(t, db, model, k)

		case p < 75:
			// Delete
			if err := db.Delete(u64Key(k)); err != nil {
				t.Fatalf("step=%d delete k=%d: %v", step, k, err)
			}
			model.del(k)
			assertPointReadMatchesModel(t, db, model, k)

		case p < 85:
			// Get probe
			assertPointReadMatchesModel(t, db, model, k)

		case p < 92:
			// Forward iterator probe
			start := uint64(r.Intn(keySpace))
			end := uint64(r.Intn(keySpace))
			var startPtr *uint64
			var endPtr *uint64
			if r.Intn(3) != 0 {
				startPtr = &start
			}
			if r.Intn(3) != 0 {
				endPtr = &end
			}
			assertIteratorRangeMatchesModel(t, db, model, startPtr, endPtr, false)

		case p < 97:
			// Reverse iterator probe
			start := uint64(r.Intn(keySpace))
			end := uint64(r.Intn(keySpace))
			var startPtr *uint64
			var endPtr *uint64
			if r.Intn(3) != 0 {
				startPtr = &start
			}
			if r.Intn(3) != 0 {
				endPtr = &end
			}
			assertIteratorRangeMatchesModel(t, db, model, startPtr, endPtr, true)

		default:
			// Checkpoint boundary
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("step=%d checkpoint: %v", step, err)
			}
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("final checkpoint: %v", err)
	}

	// Final scan parity.
	assertIteratorRangeMatchesModel(t, db, model, nil, nil, false)
	assertIteratorRangeMatchesModel(t, db, model, nil, nil, true)
}
