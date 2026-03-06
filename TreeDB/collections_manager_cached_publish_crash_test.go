package treedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

var errSyntheticNamedRootPublishFailure = errors.New("synthetic named-root publish failure")

func runCachedCollectionsCrashWriter(t *testing.T, dir, mode string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperCachedCollectionsCrashWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_CRASH_HELPER=1",
		"TREEDB_CRASH_DIR="+dir,
		"TREEDB_COLLECTION_CRASH_MODE="+mode,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cached collections crash helper failed: %v\n%s", err, string(out))
	}
}

func TestCachedCollectionsCrashReopen_InsertPublishEquivalence(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		wantDoc   []byte
		wantIndex [][]byte
	}{
		{
			name:      "buffered_insert_lost_on_crash_before_checkpoint",
			mode:      "insert_uncheckpointed",
			wantIndex: nil,
		},
		{
			name:      "buffered_insert_not_partially_published_after_checkpoint_failure_then_crash",
			mode:      "insert_failed_checkpoint",
			wantIndex: nil,
		},
		{
			name:      "checkpointed_insert_survives_crash_reopen",
			mode:      "insert_checkpointed",
			wantDoc:   []byte(`{"email":"ada@example.com"}`),
			wantIndex: [][]byte{[]byte("u1")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			runCachedCollectionsCrashWriter(t, dir, tc.mode)

			d, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("reopen cached: %v", err)
			}
			defer d.Close()

			mgr := NewCollectionManager(d)
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("open collection after reopen: %v", err)
			}

			gotDoc, err := col.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("get after reopen: %v", err)
			}
			if !bytes.Equal(gotDoc, tc.wantDoc) {
				t.Fatalf("doc after reopen = %q, want %q", gotDoc, tc.wantDoc)
			}

			gotIDs, err := col.FindByIndex("email_idx", "ada@example.com")
			if err != nil {
				t.Fatalf("find by index after reopen: %v", err)
			}
			if len(gotIDs) != len(tc.wantIndex) {
				t.Fatalf("ids after reopen = %#v, want %#v", gotIDs, tc.wantIndex)
			}
			for i := range gotIDs {
				if !bytes.Equal(gotIDs[i], tc.wantIndex[i]) {
					t.Fatalf("ids after reopen = %#v, want %#v", gotIDs, tc.wantIndex)
				}
			}
		})
	}
}

func TestCachedCollectionsCrashReopen_DeletePublishEquivalence(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		wantDoc   []byte
		wantIndex [][]byte
	}{
		{
			name:      "buffered_delete_lost_on_crash_before_checkpoint",
			mode:      "delete_uncheckpointed",
			wantDoc:   []byte(`{"email":"ada@example.com"}`),
			wantIndex: [][]byte{[]byte("u1")},
		},
		{
			name:      "buffered_delete_not_partially_published_after_checkpoint_failure_then_crash",
			mode:      "delete_failed_checkpoint",
			wantDoc:   []byte(`{"email":"ada@example.com"}`),
			wantIndex: [][]byte{[]byte("u1")},
		},
		{
			name:      "checkpointed_delete_survives_crash_reopen",
			mode:      "delete_checkpointed",
			wantIndex: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			runCachedCollectionsCrashWriter(t, dir, tc.mode)

			d, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("reopen cached: %v", err)
			}
			defer d.Close()

			mgr := NewCollectionManager(d)
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("open collection after reopen: %v", err)
			}

			gotDoc, err := col.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("get after reopen: %v", err)
			}
			if !bytes.Equal(gotDoc, tc.wantDoc) {
				t.Fatalf("doc after reopen = %q, want %q", gotDoc, tc.wantDoc)
			}

			gotIDs, err := col.FindByIndex("email_idx", "ada@example.com")
			if err != nil {
				t.Fatalf("find by index after reopen: %v", err)
			}
			if len(gotIDs) != len(tc.wantIndex) {
				t.Fatalf("ids after reopen = %#v, want %#v", gotIDs, tc.wantIndex)
			}
			for i := range gotIDs {
				if !bytes.Equal(gotIDs[i], tc.wantIndex[i]) {
					t.Fatalf("ids after reopen = %#v, want %#v", gotIDs, tc.wantIndex)
				}
			}
		})
	}
}

func TestHelperCachedCollectionsCrashWriter(t *testing.T) {
	if os.Getenv("TREEDB_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing TREEDB_CRASH_DIR")
	}
	mode := os.Getenv("TREEDB_COLLECTION_CRASH_MODE")
	if mode == "" {
		t.Fatalf("missing TREEDB_COLLECTION_CRASH_MODE")
	}

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	switch mode {
	case "insert_uncheckpointed":
		if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	case "insert_checkpointed":
		if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint insert: %v", err)
		}
	case "insert_failed_checkpoint":
		if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
			t.Fatalf("insert: %v", err)
		}
		setNamedRootPublishTestHook(func(stage string) error {
			if stage == "before_publish" {
				return errSyntheticNamedRootPublishFailure
			}
			return nil
		})
		if err := d.Checkpoint(); err == nil {
			t.Fatalf("expected checkpoint failure")
		}
	case "delete_uncheckpointed":
		if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint seed insert: %v", err)
		}
		if err := col.Delete([]byte("u1")); err != nil {
			t.Fatalf("delete: %v", err)
		}
	case "delete_checkpointed":
		if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint seed insert: %v", err)
		}
		if err := col.Delete([]byte("u1")); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint delete: %v", err)
		}
	case "delete_failed_checkpoint":
		if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint seed insert: %v", err)
		}
		if err := col.Delete([]byte("u1")); err != nil {
			t.Fatalf("delete: %v", err)
		}
		setNamedRootPublishTestHook(func(stage string) error {
			if stage == "before_publish" {
				return errSyntheticNamedRootPublishFailure
			}
			return nil
		})
		if err := d.Checkpoint(); err == nil {
			t.Fatalf("expected checkpoint failure")
		}
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}

	os.Exit(0)
}
