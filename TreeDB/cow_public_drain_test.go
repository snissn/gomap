package treedb

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCOWPublicCompactIndexIncludesDirtyActiveCut(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			prior := bytes.Repeat([]byte("prior"), 1024)
			value := bytes.Repeat([]byte("current"), 1024)
			if err := db.Set([]byte("a"), prior); err != nil {
				t.Fatal(err)
			}
			old := db.AcquireSnapshot()
			if old == nil {
				t.Fatal("missing old cut")
			}
			defer old.Close()
			if err := db.Set([]byte("a"), value); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("gone"), prior); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete([]byte("gone")); err != nil {
				t.Fatal(err)
			}
			_, revision, err := db.GetVersioned([]byte("a"))
			if err != nil {
				t.Fatal(err)
			}
			if profile != ProfileNoWALFast {
				// Native CompactIndex deliberately refuses unlogged root rebuilds
				// in command-WAL mode. Preserve that capability boundary.
				if err = db.CompactIndex(); !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
					t.Fatalf("command-WAL compact refusal: %v", err)
				}
				if got, err := db.Get([]byte("a")); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("refused compact current value: %q %v", got, err)
				}
				if got, err := old.Get([]byte("a")); err != nil || !bytes.Equal(got, prior) {
					t.Fatalf("refused compact old cut: %q %v", got, err)
				}
				return
			}
			if err = db.CompactIndex(); err != nil {
				t.Fatal(err)
			}
			got, backendRevision, err := db.backend.GetVersioned([]byte("a"))
			if err != nil || !bytes.Equal(got, value) || backendRevision != revision {
				t.Fatalf("compacted backend: %q rev=%d want=%d err=%v", got, backendRevision, revision, err)
			}
			if found, err := db.backend.Has([]byte("gone")); err != nil || found {
				t.Fatalf("compacted delete: %v %v", found, err)
			}
			if got, err := old.Get([]byte("a")); err != nil || !bytes.Equal(got, prior) {
				t.Fatalf("old cut after compact: %q %v", got, err)
			}
			if err = db.Set([]byte("late"), value); err != nil {
				t.Fatal(err)
			}
			if err = db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if got, err := db.backend.Get([]byte("late")); err != nil || !bytes.Equal(got, value) {
				t.Fatalf("late materialization: %q %v", got, err)
			}
		})
	}
}
