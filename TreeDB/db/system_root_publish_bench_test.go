package db

import "testing"

func BenchmarkPublishSystemRootIterator_WarmSparseDelta(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	base := mustFrozenSystemMemtable(b, systemRangeKVs(2048, nil)...)
	if _, err := db.PublishSystemRootIterator(base.NewIterator(nil, nil)); err != nil {
		b.Fatalf("initial publish system root: %v", err)
	}
	left := mustFrozenSystemMemtable(b, systemRangeKVs(2048, map[int]string{17: "value-0017-left"})...)
	right := mustFrozenSystemMemtable(b, systemRangeKVs(2048, map[int]string{17: "value-0017-right"})...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table := left
		if i&1 == 1 {
			table = right
		}
		if _, err := db.PublishSystemRootIterator(table.NewIterator(nil, nil)); err != nil {
			b.Fatalf("warm sparse publish: %v", err)
		}
	}
}

func BenchmarkPublishSystemRootIterator_WarmDenseDelta(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	base := mustFrozenSystemMemtable(b, systemRangeKVs(2048, nil)...)
	if _, err := db.PublishSystemRootIterator(base.NewIterator(nil, nil)); err != nil {
		b.Fatalf("initial publish system root: %v", err)
	}
	leftOverrides := make(map[int]string, 1024)
	rightOverrides := make(map[int]string, 1024)
	for i := 0; i < 1024; i++ {
		leftOverrides[i] = "dense-left"
		rightOverrides[i] = "dense-right"
	}
	left := mustFrozenSystemMemtable(b, systemRangeKVs(2048, leftOverrides)...)
	right := mustFrozenSystemMemtable(b, systemRangeKVs(2048, rightOverrides)...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table := left
		if i&1 == 1 {
			table = right
		}
		if _, err := db.PublishSystemRootIterator(table.NewIterator(nil, nil)); err != nil {
			b.Fatalf("warm dense publish: %v", err)
		}
	}
}
