package db

import (
	"context"
	"testing"
)

func newClosedDBForPublicMethodTest(t *testing.T) *DB {
	t.Helper()
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return d
}

func assertNoPanicForClosedDBMethod(t *testing.T, method string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s panicked on closed DB: %v", method, r)
		}
	}()
	fn()
}

func runClosedDBMethod(t *testing.T, method string, fn func(*DB)) {
	t.Helper()
	d := newClosedDBForPublicMethodTest(t)
	assertNoPanicForClosedDBMethod(t, method, func() {
		fn(d)
	})
}

func TestClosedDB_AcquireSnapshot(t *testing.T) {
	runClosedDBMethod(t, "AcquireSnapshot", func(d *DB) {
		_ = d.AcquireSnapshot()
	})
}

func TestClosedDB_Close(t *testing.T) {
	runClosedDBMethod(t, "Close", func(d *DB) {
		_ = d.Close()
	})
}

func TestClosedDB_Commit(t *testing.T) {
	runClosedDBMethod(t, "Commit", func(d *DB) {
		_ = d.Commit(0)
	})
}

func TestClosedDB_Prune(t *testing.T) {
	runClosedDBMethod(t, "Prune", func(d *DB) {
		d.Prune()
	})
}

func TestClosedDB_Pager(t *testing.T) {
	runClosedDBMethod(t, "Pager", func(d *DB) {
		_ = d.Pager()
	})
}

func TestClosedDB_Zipper(t *testing.T) {
	runClosedDBMethod(t, "Zipper", func(d *DB) {
		_ = d.Zipper()
	})
}

func TestClosedDB_InlineThreshold(t *testing.T) {
	runClosedDBMethod(t, "InlineThreshold", func(d *DB) {
		_ = d.InlineThreshold()
	})
}

func TestClosedDB_InlineThresholdForKey(t *testing.T) {
	runClosedDBMethod(t, "InlineThresholdForKey", func(d *DB) {
		_ = d.InlineThresholdForKey([]byte("k"))
	})
}

func TestClosedDB_State(t *testing.T) {
	runClosedDBMethod(t, "State", func(d *DB) {
		_ = d.State()
	})
}

func TestClosedDB_RefreshValueLogSet(t *testing.T) {
	runClosedDBMethod(t, "RefreshValueLogSet", func(d *DB) {
		_ = d.RefreshValueLogSet()
	})
}

func TestClosedDB_MarkValueLogZombie(t *testing.T) {
	runClosedDBMethod(t, "MarkValueLogZombie", func(d *DB) {
		_ = d.MarkValueLogZombie(1)
	})
}

func TestClosedDB_CompactIndex(t *testing.T) {
	runClosedDBMethod(t, "CompactIndex", func(d *DB) {
		_ = d.CompactIndex()
	})
}

func TestClosedDB_VacuumIndexOnline(t *testing.T) {
	runClosedDBMethod(t, "VacuumIndexOnline", func(d *DB) {
		d.dir = ""
		_ = d.VacuumIndexOnline(context.Background())
	})
}

func TestClosedDB_ValueLogRewriteOnline(t *testing.T) {
	runClosedDBMethod(t, "ValueLogRewriteOnline", func(d *DB) {
		_, _ = d.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{})
	})
}

func TestClosedDB_NewBatch(t *testing.T) {
	runClosedDBMethod(t, "NewBatch", func(d *DB) {
		b := d.NewBatch()
		if b != nil {
			_ = b.Close()
		}
	})
}

func TestClosedDB_NewBatchWithSize(t *testing.T) {
	runClosedDBMethod(t, "NewBatchWithSize", func(d *DB) {
		b := d.NewBatchWithSize(16)
		if b != nil {
			_ = b.Close()
		}
	})
}

func TestClosedDB_ValueLogGC(t *testing.T) {
	runClosedDBMethod(t, "ValueLogGC", func(d *DB) {
		_, _ = d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	})
}

func TestClosedDB_FragmentationReport(t *testing.T) {
	runClosedDBMethod(t, "FragmentationReport", func(d *DB) {
		_, _ = d.FragmentationReport()
	})
}

func TestClosedDB_Get(t *testing.T) {
	runClosedDBMethod(t, "Get", func(d *DB) {
		_, _ = d.Get([]byte("k"))
	})
}

func TestClosedDB_GetMany(t *testing.T) {
	runClosedDBMethod(t, "GetMany", func(d *DB) {
		_, _ = d.GetMany([][]byte{[]byte("k")})
	})
}

func TestClosedDB_GetUnsafe(t *testing.T) {
	runClosedDBMethod(t, "GetUnsafe", func(d *DB) {
		_, _ = d.GetUnsafe([]byte("k"))
	})
}

func TestClosedDB_Dir(t *testing.T) {
	runClosedDBMethod(t, "Dir", func(d *DB) {
		_ = d.Dir()
	})
}

func TestClosedDB_GetAppend(t *testing.T) {
	runClosedDBMethod(t, "GetAppend", func(d *DB) {
		_, _ = d.GetAppend([]byte("k"), nil)
	})
}

func TestClosedDB_Has(t *testing.T) {
	runClosedDBMethod(t, "Has", func(d *DB) {
		_, _ = d.Has([]byte("k"))
	})
}

func TestClosedDB_Set(t *testing.T) {
	runClosedDBMethod(t, "Set", func(d *DB) {
		_ = d.Set([]byte("k"), []byte("v"))
	})
}

func TestClosedDB_SetSync(t *testing.T) {
	runClosedDBMethod(t, "SetSync", func(d *DB) {
		_ = d.SetSync([]byte("k"), []byte("v"))
	})
}

func TestClosedDB_Delete(t *testing.T) {
	runClosedDBMethod(t, "Delete", func(d *DB) {
		_ = d.Delete([]byte("k"))
	})
}

func TestClosedDB_DeleteSync(t *testing.T) {
	runClosedDBMethod(t, "DeleteSync", func(d *DB) {
		_ = d.DeleteSync([]byte("k"))
	})
}

func TestClosedDB_Iterator(t *testing.T) {
	runClosedDBMethod(t, "Iterator", func(d *DB) {
		it, _ := d.Iterator(nil, nil)
		if it != nil {
			_ = it.Close()
		}
	})
}

func TestClosedDB_IteratorWithOptions(t *testing.T) {
	runClosedDBMethod(t, "IteratorWithOptions", func(d *DB) {
		it, _ := d.IteratorWithOptions(nil, nil, IteratorOptions{})
		if it != nil {
			_ = it.Close()
		}
	})
}

func TestClosedDB_ReverseIterator(t *testing.T) {
	runClosedDBMethod(t, "ReverseIterator", func(d *DB) {
		it, _ := d.ReverseIterator(nil, nil)
		if it != nil {
			_ = it.Close()
		}
	})
}

func TestClosedDB_ReverseIteratorWithOptions(t *testing.T) {
	runClosedDBMethod(t, "ReverseIteratorWithOptions", func(d *DB) {
		it, _ := d.ReverseIteratorWithOptions(nil, nil, IteratorOptions{})
		if it != nil {
			_ = it.Close()
		}
	})
}

func TestClosedDB_Stats(t *testing.T) {
	runClosedDBMethod(t, "Stats", func(d *DB) {
		_ = d.Stats()
	})
}

func TestClosedDB_Print(t *testing.T) {
	runClosedDBMethod(t, "Print", func(d *DB) {
		_ = d.Print()
	})
}
