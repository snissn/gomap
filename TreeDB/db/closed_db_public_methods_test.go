package db

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
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
		if snap := d.AcquireSnapshot(); snap != nil {
			t.Fatalf("AcquireSnapshot()=%v want nil on closed DB", snap)
		}
	})
}

func TestClosedDB_Close(t *testing.T) {
	runClosedDBMethod(t, "Close", func(d *DB) {
		_ = d.Close()
	})
}

func TestClosedDB_Commit(t *testing.T) {
	runClosedDBMethod(t, "Commit", func(d *DB) {
		_ = d.ForceCommit(0)
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
		_, err := d.Get([]byte("k"))
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Get err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_GetMany(t *testing.T) {
	runClosedDBMethod(t, "GetMany", func(d *DB) {
		_, err := d.GetMany([][]byte{[]byte("k")})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("GetMany err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_GetManyView(t *testing.T) {
	runClosedDBMethod(t, "GetManyView", func(d *DB) {
		err := d.GetManyView([][]byte{[]byte("k")}, func(int, []byte, []byte, bool) error { return nil })
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("GetManyView err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_GetUnsafe(t *testing.T) {
	runClosedDBMethod(t, "GetUnsafe", func(d *DB) {
		_, err := d.GetUnsafe([]byte("k"))
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("GetUnsafe err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_Dir(t *testing.T) {
	runClosedDBMethod(t, "Dir", func(d *DB) {
		_ = d.Dir()
	})
}

func TestClosedDB_GetAppend(t *testing.T) {
	runClosedDBMethod(t, "GetAppend", func(d *DB) {
		_, err := d.GetAppend([]byte("k"), nil)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("GetAppend err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_Has(t *testing.T) {
	runClosedDBMethod(t, "Has", func(d *DB) {
		_, err := d.Has([]byte("k"))
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Has err=%v want %v", err, ErrClosed)
		}
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

func TestClosedDB_Update(t *testing.T) {
	runClosedDBMethod(t, "Update", func(d *DB) {
		err := d.Update([]byte("k"), func([]byte) (UpdateResult, error) {
			return SetUpdate([]byte("v")), nil
		})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Update err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_UpdateSync(t *testing.T) {
	runClosedDBMethod(t, "UpdateSync", func(d *DB) {
		err := d.UpdateSync([]byte("k"), func([]byte) (UpdateResult, error) {
			return SetUpdate([]byte("v")), nil
		})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("UpdateSync err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_Iterator(t *testing.T) {
	runClosedDBMethod(t, "Iterator", func(d *DB) {
		it, err := d.Iterator(nil, nil)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Iterator err=%v want %v", err, ErrClosed)
		}
		if it != nil {
			t.Fatalf("Iterator() returned iterator on closed DB")
		}
	})
}

func TestClosedDB_IteratorWithOptions(t *testing.T) {
	runClosedDBMethod(t, "IteratorWithOptions", func(d *DB) {
		it, err := d.IteratorWithOptions(nil, nil, IteratorOptions{})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("IteratorWithOptions err=%v want %v", err, ErrClosed)
		}
		if it != nil {
			t.Fatalf("IteratorWithOptions() returned iterator on closed DB")
		}
	})
}

func TestClosedDB_ReverseIterator(t *testing.T) {
	runClosedDBMethod(t, "ReverseIterator", func(d *DB) {
		it, err := d.ReverseIterator(nil, nil)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ReverseIterator err=%v want %v", err, ErrClosed)
		}
		if it != nil {
			t.Fatalf("ReverseIterator() returned iterator on closed DB")
		}
	})
}

func TestClosedDB_ReverseIteratorWithOptions(t *testing.T) {
	runClosedDBMethod(t, "ReverseIteratorWithOptions", func(d *DB) {
		it, err := d.ReverseIteratorWithOptions(nil, nil, IteratorOptions{})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ReverseIteratorWithOptions err=%v want %v", err, ErrClosed)
		}
		if it != nil {
			t.Fatalf("ReverseIteratorWithOptions() returned iterator on closed DB")
		}
	})
}

func TestClosedDB_PublishSystemRootIterator(t *testing.T) {
	runClosedDBMethod(t, "PublishSystemRootIterator", func(d *DB) {
		table := mustFrozenSystemMemtable(t, "sys/k", "v")
		iter := table.NewIterator(nil, nil)
		defer iter.Close()
		_, err := d.PublishSystemRootIterator(iter)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("PublishSystemRootIterator err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_PublishOrderedRootIterator(t *testing.T) {
	runClosedDBMethod(t, "PublishOrderedRootIterator", func(d *DB) {
		table := mustFrozenSystemMemtable(t, "root/k", "v")
		iter := table.NewIterator(nil, nil)
		defer iter.Close()
		_, err := d.PublishOrderedRootIterator(0, iter)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("PublishOrderedRootIterator err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_PublishOrderedRootGroup(t *testing.T) {
	runClosedDBMethod(t, "PublishOrderedRootGroup", func(d *DB) {
		_, _, err := d.PublishOrderedRootGroup(nil, nil)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("PublishOrderedRootGroup err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_PublishOrderedRootDeltaGroupWithSystemBuilder(t *testing.T) {
	runClosedDBMethod(t, "PublishOrderedRootDeltaGroupWithSystemBuilder", func(d *DB) {
		table := mustFrozenSystemMemtable(t, "root/k", "v")
		iter := table.NewIterator(nil, nil)
		defer iter.Close()
		_, _, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
			Iter: iter,
		}}, func([]uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/k", "v").NewIterator(nil, nil), nil
		})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("PublishOrderedRootDeltaGroupWithSystemBuilder err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(t *testing.T) {
	runClosedDBMethod(t, "PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder", func(d *DB) {
		table := mustFrozenSystemMemtable(t, "root/k", "v")
		iter := table.NewIterator(nil, nil)
		defer iter.Close()
		_, _, err := d.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(storagemaintenance.ColumnAssetRewritePlan(), []StorageMaintenanceRootDeltaPublishInput{{
			Iter: iter,
		}}, nil, func([]uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run on a closed DB")
			return nil, nil
		})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder err=%v want %v", err, ErrClosed)
		}
	})
}

func TestClosedDB_PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(t *testing.T) {
	runClosedDBMethod(t, "PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder", func(d *DB) {
		table := mustFrozenSystemMemtable(t, "root/k", "v")
		iter := table.NewIterator(nil, nil)
		delta, err := OrderedRootDeltaBatchFromIterator(iter)
		_ = iter.Close()
		if err != nil {
			t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
		}
		defer func() { _ = delta.Close() }()
		_, _, err = d.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			Delta: delta,
		}}, func([]uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/k", "v").NewIterator(nil, nil), nil
		})
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder err=%v want %v", err, ErrClosed)
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
