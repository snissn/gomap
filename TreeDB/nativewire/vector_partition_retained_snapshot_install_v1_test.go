package nativewire

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestVectorPartitionRetainedSnapshotInstallV1(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("snapshot rebind requires durable rename and removal namespaces")
	}
	source := filepath.Join(t.TempDir(), "source")
	opts := forcedPointerYCSBNativewireOptions(source)
	opts.DisableSideStores = true
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{"first": strings.Repeat("first-pointer-", 32), "second": strings.Repeat("second-pointer-", 32)}
	for _, key := range []string{"first", "second"} {
		if err := database.SetSync([]byte(key), []byte(values[key])); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	if err := database.Checkpoint(); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requireValueLogContains(t, source, []byte(values["first"]), []byte(values["second"]))
	original := ownerRelocationDiagnosticTreeSealV1(t, source)
	unbound := filepath.Join(t.TempDir(), "unbound")
	if err := os.CopyFS(unbound, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	// Keep the constructor's canonical durability contract at the backend boundary.
	reopenOpts := opts
	reopenOpts.Dir = unbound
	reopenOpts.ReadOnly = true
	if copied, err := backenddb.Open(reopenOpts); err == nil {
		_ = copied.Close()
		t.Fatal("ordinary recovery accepted copied forced-pointer dependencies")
	} else if !errors.Is(err, backenddb.ErrNoRecoverableMeta) {
		t.Fatalf("copied open error=%v, want ErrNoRecoverableMeta", err)
	}
	copyDir := filepath.Join(t.TempDir(), "mutable-lifecycle-copy")
	if err := os.CopyFS(copyDir, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	files := filepath.Join(t.TempDir(), "snapshot-files.json")
	if err := os.WriteFile(files, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	in := liveLifecycleRetainedInputV1{DB: copyDir, SnapshotFiles: files, SnapshotFilesSHA256: fmt.Sprintf("%x", sha256.Sum256(raw))}
	for _, tc := range []struct {
		name  string
		input liveLifecycleRetainedInputV1
	}{
		{"wrong-pin", liveLifecycleRetainedInputV1{DB: copyDir, SnapshotFiles: files, SnapshotFilesSHA256: fmt.Sprintf("%064x", 1)}},
		{"missing-pin", liveLifecycleRetainedInputV1{DB: copyDir, SnapshotFiles: files}},
		{"source-path", liveLifecycleRetainedInputV1{DB: source, SnapshotFiles: files, SnapshotFilesSHA256: in.SnapshotFilesSHA256}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := liveLifecycleInstallRetainedCopyV1(t.Context(), tc.input); err == nil {
				t.Fatal("unsafe snapshot install accepted")
			}
		})
	}
	t.Run("changed-copy", func(t *testing.T) {
		extra := filepath.Join(copyDir, "unexpected")
		if err := os.WriteFile(extra, []byte("not pinned"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := liveLifecycleInstallRetainedCopyV1(t.Context(), in); err == nil {
			t.Fatal("changed snapshot pathset accepted")
		}
		if err := os.Remove(extra); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("changed-bytes", func(t *testing.T) {
		path := filepath.Join(copyDir, "index.db")
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		var originalByte [1]byte
		if _, err := file.ReadAt(originalByte[:], 0); err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt([]byte{originalByte[0] ^ 0xff}, 0); err != nil {
			t.Fatal(err)
		}
		if err := liveLifecycleInstallRetainedCopyV1(t.Context(), in); err == nil {
			t.Fatal("changed snapshot bytes accepted")
		}
		if _, err := file.WriteAt(originalByte[:], 0); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("symlink-copy", func(t *testing.T) {
		link := filepath.Join(copyDir, "unexpected-link")
		if err := os.Symlink(filepath.Join(source, "index.db"), link); err != nil {
			t.Fatal(err)
		}
		if err := liveLifecycleInstallRetainedCopyV1(t.Context(), in); err == nil {
			t.Fatal("snapshot symlink accepted")
		}
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("unsafe-file-list", func(t *testing.T) {
		unsafe := maps.Clone(original)
		unsafe["../index.db"] = original["index.db"]
		bad, err := json.Marshal(unsafe)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(files, bad, 0o600); err != nil {
			t.Fatal(err)
		}
		badInput := in
		badInput.SnapshotFilesSHA256 = fmt.Sprintf("%x", sha256.Sum256(bad))
		if err := liveLifecycleInstallRetainedCopyV1(t.Context(), badInput); err == nil {
			t.Fatal("unsafe snapshot file-list path accepted")
		}
		if err := os.WriteFile(files, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	})
	for _, side := range []string{"dictdb", "templatedb"} {
		t.Run("unsupported-"+side, func(t *testing.T) {
			sideDir := filepath.Join(copyDir, side)
			sideDB, err := backenddb.Open(backenddb.Options{Dir: sideDir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := sideDB.SetSync([]byte("side-store"), []byte(side)); err != nil {
				_ = sideDB.Close()
				t.Fatal(err)
			}
			if err := sideDB.Close(); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(sideDir)
			before := ownerRelocationDiagnosticTreeSealV1(t, copyDir)
			pinned, err := json.Marshal(before)
			if err != nil {
				t.Fatal(err)
			}
			pinPath := filepath.Join(t.TempDir(), "snapshot-files.json")
			if err := os.WriteFile(pinPath, pinned, 0o600); err != nil {
				t.Fatal(err)
			}
			unsupported := liveLifecycleRetainedInputV1{DB: copyDir, SnapshotFiles: pinPath, SnapshotFilesSHA256: fmt.Sprintf("%x", sha256.Sum256(pinned))}
			if err := liveLifecycleInstallRetainedCopyV1(t.Context(), unsupported); err == nil || !strings.Contains(err.Error(), "does not support "+side) {
				t.Fatalf("side-store install error=%v, want flat-layout refusal", err)
			}
			if got := ownerRelocationDiagnosticTreeSealV1(t, copyDir); !maps.Equal(got, before) {
				t.Fatal("refused side-store install changed the staged copy")
			}
		})
	}
	if err := liveLifecycleInstallRetainedCopyV1(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	reopenOpts.Dir = copyDir
	for phase := 0; phase < 2; phase++ {
		installed, err := backenddb.Open(reopenOpts)
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range values {
			got, err := installed.Get([]byte(key))
			if err != nil || string(got) != want {
				_ = installed.Close()
				t.Fatalf("phase=%d Get(%s)=(%q,%v), want %q", phase, key, got, err, want)
			}
		}
		if err := installed.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := ownerRelocationDiagnosticTreeSealV1(t, source); !maps.Equal(got, original) {
		t.Fatal("snapshot installation changed the original source")
	}
}
