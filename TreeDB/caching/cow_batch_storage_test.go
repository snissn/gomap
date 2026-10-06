package caching

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWBatchStorageAdmissionAndReset(t *testing.T) {
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats().ExternalBytes
	b := db.NewBatchWithSize(1)
	for i := 0; i < 65; i++ {
		if err := b.Set([]byte{byte(i)}, make([]byte, 1025)); err != nil {
			t.Fatal(err)
		}
	}
	if b.cowState.storage == nil || cap(b.entries) < 65 {
		t.Fatal("missing admitted growth")
	}
	if db.cow.budget.Stats().ExternalBytes <= baseline {
		t.Fatal("batch capacity uncharged")
	}
	b.Reset()
	if len(b.entries) != 0 || b.copyArena != nil || b.cowState.storage != nil {
		t.Fatal("reset retained batch storage")
	}
	if err := b.Set([]byte("again"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if got := db.cow.budget.Stats().ExternalBytes; got != baseline {
		t.Fatalf("external charge leaked: %d want %d", got, baseline)
	}
}

func TestCOWBatchRefusalBeforeStagingAndCallback(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.NewBatch()
	defer b.Close()
	if err := b.SetOps([]batch.Entry{{Type: batch.OpPut, Key: []byte("good"), Value: []byte("v")}, {Type: batch.OpDeleteRange, Key: []byte("a"), Value: []byte("z")}}); !errors.Is(err, ErrCOWUnsupported) {
		t.Fatal(err)
	}
	if len(b.entries) != 0 {
		t.Fatal("unsupported group partially staged")
	}
	b.Reserve(1 << 30)
	if !errors.Is(b.Set([]byte("key"), []byte("v")), memtable.ErrCOWCapacity) {
		t.Fatal("reserve refusal lost")
	}
	called := false
	if err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error { called = true; return nil }); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal(err)
	}
	if called {
		t.Fatal("refused batch invoked append")
	}
	db.cow.budget.Close()
	denied := db.NewBatch()
	if denied != &cowDeniedBatch {
		t.Fatal("refused constructor allocated a wrapper")
	}
	denied.Reserve(8)
	denied.Reset()
	_ = denied.Close()
	if !errors.Is(denied.Set([]byte("x"), []byte("v")), memtable.ErrCOWCapacity) {
		t.Fatal("denied carrier changed")
	}
}

func TestCOWHugeFiniteInputsRefuseBeforeAllocation(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if _, _, err := validateCOWOpenOptions(Options{DisableWAL: true, MemtableShards: maxInt}); err == nil {
		t.Fatal("overflowing shard input accepted")
	}
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats().ExternalBytes
	for _, n := range []int{maxInt, maxInt / int(unsafe.Sizeof(batch.Entry{})), 1 << 30} {
		b := db.NewBatchWithSize(n)
		// The public hint normalizer may cap a constructor hint. An explicit
		// Reserve must still refuse the original finite input before make.
		b.Reserve(n)
		if !errors.Is(b.Set([]byte("x"), []byte("v")), memtable.ErrCOWCapacity) {
			t.Fatalf("Reserve(%d) accepted", n)
		}
		_ = b.Close()
	}
	if got := db.cow.budget.Stats().ExternalBytes; got != baseline {
		t.Fatalf("refusal retained charge: %d want %d", got, baseline)
	}
	b := db.NewBatch()
	defer b.Close()
	if !errors.Is(b.cowAdmitStorage(^uint64(0)), memtable.ErrCOWCapacity) {
		t.Fatal("sentinel charge wrapped")
	}
}
