package treedb

import (
	"fmt"
	"testing"
)

func BenchmarkSnapshotHasMany(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("acct/%02d/doc", i))
		if err := db.Set(key, []byte("v")); err != nil {
			b.Fatalf("set %q: %v", string(key), err)
		}
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		b.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	cases := []struct {
		name string
		keys [][]byte
	}{
		{
			name: "distinct8",
			keys: [][]byte{
				[]byte("acct/00/doc"), []byte("acct/01/doc"), []byte("acct/02/doc"), []byte("acct/03/doc"),
				[]byte("acct/04/doc"), []byte("acct/05/doc"), []byte("acct/06/doc"), []byte("acct/07/doc"),
			},
		},
		{
			name: "duplicate64",
			keys: func() [][]byte {
				keys := make([][]byte, 64)
				for i := range keys {
					keys[i] = []byte("acct/00/doc")
				}
				return keys
			}(),
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := snap.HasMany(tc.keys)
				if err != nil {
					b.Fatalf("HasMany: %v", err)
				}
				if len(got) != len(tc.keys) {
					b.Fatalf("len(HasMany)=%d want %d", len(got), len(tc.keys))
				}
			}
		})
	}
}

func BenchmarkSnapshotHasPrefixes(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("uniq/%02d/doc", i))
		if err := db.Set(key, []byte("v")); err != nil {
			b.Fatalf("set %q: %v", string(key), err)
		}
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		b.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	prefixes := make([][]byte, 8)
	for i := range prefixes {
		prefixes[i] = []byte(fmt.Sprintf("uniq/%02d/", i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := snap.HasPrefixes(prefixes)
		if err != nil {
			b.Fatalf("HasPrefixes: %v", err)
		}
		if len(got) != len(prefixes) {
			b.Fatalf("len(HasPrefixes)=%d want %d", len(got), len(prefixes))
		}
	}
}
