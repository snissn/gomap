package contracttest

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/HashDB"
	"github.com/snissn/gomap/TreeDB"
)

func TestContract_ExclusiveOpenLock(t *testing.T) {
	cases := []struct {
		name      string
		engine    string
		wantIsErr func(error) bool
	}{
		{name: "hashdb-single", engine: "hashdb-single", wantIsErr: func(err error) bool { return errors.Is(err, hashdb.ErrLocked) }},
		{name: "hashdb-sharded", engine: "hashdb-sharded", wantIsErr: func(err error) bool { return errors.Is(err, hashdb.ErrLocked) }},
		{name: "treedb-cached", engine: "treedb-cached", wantIsErr: func(err error) bool { return errors.Is(err, treedb.ErrLocked) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			first, err := openEngine(tc.engine, dir)
			if err != nil {
				t.Fatalf("open first: %v", err)
			}

			_, err = openEngine(tc.engine, dir)
			if err == nil || !tc.wantIsErr(err) {
				_ = first.Close()
				t.Fatalf("open second: expected ErrLocked, got %v", err)
			}

			if err := first.Close(); err != nil {
				t.Fatalf("close first: %v", err)
			}

			again, err := openEngine(tc.engine, dir)
			if err != nil {
				t.Fatalf("open again: %v", err)
			}
			_ = again.Close()
		})
	}
}
