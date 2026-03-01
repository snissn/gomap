package caching_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/tree"
)

func bankBalanceFastKey(addr []byte) []byte {
	const prefix = "s/k:bank/f\x02\x14"
	k := make([]byte, 0, len(prefix)+len(addr)+len("utia"))
	k = append(k, prefix...)
	k = append(k, addr...)
	k = append(k, "utia"...)
	return k
}

func Test_TMP_RouteBankBalanceStressParity(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 512

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	rng := rand.New(rand.NewSource(881931))
	expected := make(map[string][]byte, 400000)
	pool := make([][20]byte, 0, 200000)

	const (
		rounds    = 60
		batchSize = 12000
	)

	for r := 0; r < rounds; r++ {
		b := db.NewBatch()
		for i := 0; i < batchSize; i++ {
			var addr [20]byte
			if len(pool) > 0 && rng.Intn(100) < 75 {
				addr = pool[rng.Intn(len(pool))]
			} else {
				if _, err := rng.Read(addr[:]); err != nil {
					_ = b.Close()
					t.Fatalf("rng read: %v", err)
				}
				pool = append(pool, addr)
			}
			k := bankBalanceFastKey(addr[:])
			v := []byte(strconv.Itoa(1 + rng.Intn(1_000_000_000)))
			if err := b.Set(k, v); err != nil {
				_ = b.Close()
				t.Fatalf("set round=%d idx=%d: %v", r, i, err)
			}
			expected[string(k)] = append([]byte(nil), v...)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("writesync round=%d: %v", r, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("batch close round=%d: %v", r, err)
		}
		if (r+1)%10 == 0 {
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint round=%d: %v", r, err)
			}
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("final checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	checked := 0
	for k, want := range expected {
		got, err := reopened.Get([]byte(k))
		if errors.Is(err, tree.ErrKeyNotFound) {
			t.Fatalf("missing key=%x len(expected)=%d checked=%d", []byte(k), len(expected), checked)
		}
		if err != nil {
			t.Fatalf("get err key=%x: %v", []byte(k), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch key=%x got=%q want=%q", []byte(k), got, want)
		}
		checked++
	}

	if testing.Verbose() {
		fmt.Printf("checked %d keys\n", checked)
	}
}
