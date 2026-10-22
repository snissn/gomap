package caching

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCOWOpenRejectsAdaptiveSelectorBeforeBackendOrStorage(t *testing.T) {
	for _, mode := range []string{"adaptive:cow_btree", "adaptive:cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "absent-cache")
			// Otherwise valid COW options and a nil backend prove rejection
			// happens before backend capability lookup or storage creation.
			database, err := Open(directory, nil, Options{
				MemtableMode: mode, MemtableShards: 2,
				DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30,
			})
			if database != nil {
				_ = database.Close()
			}
			if !errors.Is(err, ErrCOWUnsupported) || database != nil {
				t.Errorf("Open(%q) = %v, %v; want nil, ErrCOWUnsupported", mode, database, err)
			}
			if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("unsupported selector mutated storage: Stat = %v", err)
			}
		})
	}
}
