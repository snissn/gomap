package commitlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func fixedWALRepairFixture(t *testing.T) (string, string, int64, []byte) {
	t.Helper()
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication is unsupported on this platform")
	}
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	b := fixedWALLifecycleFrame(t, 128<<10)
	b.DurabilityClass = CommandDurabilityRelaxed
	if _, err = j.AppendCommand(b); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(j.path)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := loadFixedWAL(file)
	_ = file.Close()
	if err != nil || len(state.extents) != 2 {
		t.Fatalf("fixture extents: state=%v err=%v", state, err)
	}
	image, err := os.ReadFile(j.path)
	if err != nil {
		t.Fatal(err)
	}
	last := state.extents[1]
	// A positive start, intact V2 identity, incomplete relaxed body, and zero
	// footer retain exactly the evidence that ordinary horizon recovery uses.
	image[last.physical+fixedWALWitnessSize+last.length-1] ^= 0xff
	clear(image[last.end-fixedWALWitnessSize : last.end])
	if err = os.WriteFile(j.path, image, 0600); err != nil {
		t.Fatal(err)
	}
	return dir, j.path, state.extents[0].length, image
}

func TestFixedWALRepairInterruptedWriteCanRetry(t *testing.T) {
	for _, size := range []int{97, 64 << 10} {
		t.Run(fmt.Sprintf("partial_%d", size), func(t *testing.T) {
			dir, path, offset, original := fixedWALRepairFixture(t)
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			oldWrite := fixedWALRepairWrite
			defer func() { fixedWALRepairWrite = oldWrite }()
			fixedWALRepairWrite = func(w io.WriterAt, p []byte, off int64) error {
				if _, e := w.WriteAt(p[:min(len(p), size)], off); e != nil {
					return e
				}
				return syscall.EIO
			}
			err = TruncateLogicalFile(&file, offset)
			fixedWALRepairWrite = oldWrite
			_ = file.Close()
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("injected repair write=%v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, original) {
				t.Errorf("interrupted repair changed canonical image: %v", err)
			}
			file, err = os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = file.Close() }()
			if err = TruncateLogicalFile(&file, offset); err != nil {
				t.Fatalf("retry cannot recover intact acknowledged prefix: %v", err)
			}
			if err = file.Sync(); err != nil {
				t.Fatal(err)
			}
			j, err := OpenCommandJournal(dir, CommandJournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if j.NextLSN() != 2 {
				t.Fatalf("ACK prefix LSN=%d want2", j.NextLSN())
			}
		})
	}
}

func verifyFixedWALRepairRetry(t *testing.T, dir, path string, offset int64, original []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	state, _, err := loadFixedWAL(file)
	if err != nil {
		t.Fatalf("interrupted repair is not classifiable: %v", err)
	}
	keptEnd := state.extents[0].end
	if err = TruncateLogicalFile(&file, offset); err != nil {
		t.Fatalf("repair retry: %v", err)
	}
	if err = file.Sync(); err != nil {
		t.Fatal(err)
	}
	image, err := os.ReadFile(path)
	if err != nil || len(image) != len(original) || !bytes.Equal(image[:keptEnd], original[:keptEnd]) {
		t.Fatalf("retained physical prefix/capacity changed: %v", err)
	}
	state, _, err = loadFixedWAL(file)
	if err != nil || len(state.extents) != 1 || len(state.incomplete) != 0 || state.lastLSN != 1 {
		t.Fatalf("repaired container=%v err=%v", state, err)
	}
	j, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if j.NextLSN() != 2 {
		t.Fatalf("reopened frontier=%d want2", j.NextLSN())
	}
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.NextLSN() != 3 {
		t.Fatalf("post-repair ACK frontier=%d want3", j.NextLSN())
	}
}

func TestFixedWALRepairPrivateIOFailuresPreserveCanonical(t *testing.T) {
	for _, fault := range []string{"short_write", "enospc", "full_sync"} {
		t.Run(fault, func(t *testing.T) {
			dir, path, offset, original := fixedWALRepairFixture(t)
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			old := file
			oldWrite, oldSync := fixedWALRepairWrite, fixedWALRepairSync
			defer func() { fixedWALRepairWrite, fixedWALRepairSync = oldWrite, oldSync }()
			var modified *os.File
			if fault == "full_sync" {
				fixedWALRepairSync = func(f *os.File) error {
					modified = f
					if e := oldSync(f); e != nil {
						return e
					}
					return syscall.EIO
				}
			} else {
				fixedWALRepairWrite = func(w io.WriterAt, p []byte, off int64) error {
					modified = w.(*os.File)
					if _, e := w.WriteAt(p[:97], off); e != nil {
						return e
					}
					if fault == "enospc" {
						return syscall.ENOSPC
					}
					return io.ErrShortWrite
				}
			}
			err = TruncateLogicalFile(&file, offset)
			fixedWALRepairWrite, fixedWALRepairSync = oldWrite, oldSync
			if err == nil || modified == nil || file != old {
				t.Fatalf("failed private custody: modified=%v swapped=%t err=%v", modified != nil, file != old, err)
			}
			if _, e := modified.ReadAt(make([]byte, 1), 0); !errors.Is(e, os.ErrClosed) {
				t.Fatalf("private descriptor leaked: %v", e)
			}
			if _, e := file.ReadAt(make([]byte, 1), 0); e != nil {
				t.Fatalf("old descriptor closed before caller cleanup: %v", e)
			}
			_ = file.Close()
			image, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(image, original) {
				t.Fatalf("private failure changed canonical bytes: %v", e)
			}
			verifyFixedWALRepairRetry(t, dir, path, offset, original)
		})
	}
}

func TestFixedWALRepairPostPublicationErrorsKeepCustody(t *testing.T) {
	for _, fault := range []string{"after_rename", "directory_sync", "reopen"} {
		t.Run(fault, func(t *testing.T) {
			dir, path, offset, original := fixedWALRepairFixture(t)
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			old := file
			retained, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer retained.Close()
			oldInfo, err := old.Stat()
			if err != nil {
				t.Fatal(err)
			}
			oldOpen := fixedWALRepairOpen
			if fault == "reopen" {
				fixedWALRepairOpen = func(*os.File, string, int, os.FileMode) (*os.File, error) { return nil, syscall.EIO }
			}
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL &&
					((fault == "after_rename" && event.Namespace == durabilitycut.NamespaceRename) ||
						(fault == "directory_sync" && event.Point == durabilitycut.BeforeNewFileDirectorySync)) {
					return syscall.EIO
				}
				return nil
			})
			err = TruncateLogicalFile(&file, offset)
			restore()
			fixedWALRepairOpen = oldOpen
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("post-publication error=%v", err)
			}
			if fault == "reopen" {
				if file != old {
					t.Fatal("failed reopen lost old handle custody")
				}
				if _, e := old.ReadAt(make([]byte, 1), 0); e != nil {
					t.Fatalf("failed reopen closed owned old handle: %v", e)
				}
			} else {
				if _, e := old.ReadAt(make([]byte, 1), 0); !errors.Is(e, os.ErrClosed) {
					t.Fatalf("transferred owner descriptor leaked: %v", e)
				}
				active, e := file.Stat()
				canonical, statErr := os.Stat(path)
				if e != nil || statErr != nil || !os.SameFile(active, canonical) || os.SameFile(active, oldInfo) {
					t.Fatalf("ambiguous sync lost canonical identity: %v %v", e, statErr)
				}
			}
			_ = file.Close()
			retainedImage := make([]byte, len(original))
			if _, e := retained.ReadAt(retainedImage, 0); e != nil || !bytes.Equal(retainedImage, original) {
				t.Fatalf("retained original object changed: %v", e)
			}
			verifyFixedWALRepairRetry(t, dir, path, offset, original)
		})
	}
}

// Stops without running deferred cleanup, including at each private write.
func TestFixedWALRepairCrashHelper(t *testing.T) {
	path := os.Getenv("GOMAP_FIXED_WAL_REPAIR_PATH")
	if path == "" {
		return
	}
	offset, err := strconv.ParseInt(os.Getenv("GOMAP_FIXED_WAL_REPAIR_OFFSET"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	cut := os.Getenv("GOMAP_FIXED_WAL_REPAIR_CUT")
	oldWrite := fixedWALRepairWrite
	writes := 0
	fixedWALRepairWrite = func(w io.WriterAt, p []byte, off int64) error {
		writes++
		if cut == "partial-write" {
			if _, e := w.WriteAt(p[:97], off); e != nil {
				t.Fatal(e)
			}
			os.Exit(73)
		}
		if e := oldWrite(w, p, off); e != nil {
			return e
		}
		if cut == fmt.Sprintf("write-%d", writes) {
			os.Exit(73)
		}
		return nil
	}
	durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		switch {
		case cut == "private-create" && event.Namespace == durabilitycut.NamespaceCreate,
			cut == "before-private-sync" && event.Point == durabilitycut.BeforeDependencyFileSync,
			cut == "after-private-sync" && event.Point == durabilitycut.AfterDependencyFileSync,
			cut == "after-rename" && event.Namespace == durabilitycut.NamespaceRename,
			cut == "before-directory-sync" && event.Point == durabilitycut.BeforeNewFileDirectorySync,
			cut == "after-directory-sync" && event.Point == durabilitycut.AfterNewFileDirectorySync:
			os.Exit(73)
		}
		return nil
	})
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = TruncateLogicalFile(&file, offset); err != nil {
		t.Fatal(err)
	}
	t.Fatal("did not reach requested repair crash")
}

func TestFixedWALRepairCrashCutsRetry(t *testing.T) {
	_, _, _, template := fixedWALRepairFixture(t)
	cuts := []string{"private-create", "partial-write", "before-private-sync", "after-private-sync", "after-rename", "before-directory-sync", "after-directory-sync"}
	for write := 1; write <= (len(template)+(64<<10)-1)/(64<<10); write++ {
		cuts = append(cuts, fmt.Sprintf("write-%d", write))
	}
	for _, cut := range cuts {
		t.Run(cut, func(t *testing.T) {
			dir, path, offset, original := fixedWALRepairFixture(t)
			cmd := exec.Command(os.Args[0], "-test.run=^TestFixedWALRepairCrashHelper$")
			cmd.Env = append(os.Environ(), "GOMAP_FIXED_WAL_REPAIR_PATH="+path,
				"GOMAP_FIXED_WAL_REPAIR_OFFSET="+strconv.FormatInt(offset, 10), "GOMAP_FIXED_WAL_REPAIR_CUT="+cut)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
			if cut == "private-create" || cut == "partial-write" || strings.HasPrefix(cut, "write-") ||
				cut == "before-private-sync" || cut == "after-private-sync" {
				image, e := os.ReadFile(path)
				if e != nil || !bytes.Equal(image, original) {
					t.Fatalf("prepublication crash changed canonical: %v", e)
				}
			}
			verifyFixedWALRepairRetry(t, dir, path, offset, original)
			// Interrupted private names are ignored; no sidecar is recovery authority.
			entries, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".wal-repair-*"))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("inert interrupted private files=%d", len(entries))
		})
	}
}

func TestFixedWALRepairNoOpAndInvalidBoundary(t *testing.T) {
	dir, path, offset, original := fixedWALRepairFixture(t)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	for _, invalid := range []int64{-1, offset - 1, int64(len(original))} {
		if err = TruncateLogicalFile(&file, invalid); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("invalid boundary%d=%v", invalid, err)
		}
	}
	verifyFixedWALRepairRetry(t, dir, path, offset, original)
	_ = file.Close()
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	size, err := LogicalFileSize(file)
	if err != nil {
		t.Fatal(err)
	}
	old := file
	restore := durabilitycut.Install(func(durabilitycut.Event) error { return syscall.EIO })
	err = TruncateLogicalFile(&file, size)
	restore()
	if err != nil || file != old {
		t.Fatalf("validated no-op published replacement: %v", err)
	}
}

func TestFixedWALRepairRejectsReboundCanonical(t *testing.T) {
	_, path, offset, original := fixedWALRepairFixture(t)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err = os.Rename(path, path+".retained"); err != nil {
		t.Fatal(err)
	}
	foreign := []byte("foreign canonical object")
	if err = os.WriteFile(path, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	if err = TruncateLogicalFile(&file, offset); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("rebound canonical=%v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, foreign) {
		t.Fatalf("foreign name changed: %v", err)
	}
	got, err = os.ReadFile(path + ".retained")
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("retained source changed: %v", err)
	}
}
