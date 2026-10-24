package atomicfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

func TestWriteClassifiesOnlyPostRenameFailureAsPossiblyCommitted(t *testing.T) {
	for _, test := range []struct {
		name        string
		failAt      durabilitycut.NamespaceOperation
		mayCommit   bool
		wantContent string
	}{
		{name: "create", failAt: durabilitycut.NamespaceCreate, wantContent: "old"},
		{name: "rename", failAt: durabilitycut.NamespaceRename, mayCommit: true, wantContent: "new"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest")
			if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			cut := errors.New("injected namespace cut")
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Namespace == test.failAt {
					return cut
				}
				return nil
			})
			err := Write(path, []byte("new"), 0o600)
			restore()
			if !errors.Is(err, cut) {
				t.Fatalf("Write error=%v, want injected cut", err)
			}
			if got := ReplacementMayHaveCommitted(err); got != test.mayCommit {
				t.Fatalf("ReplacementMayHaveCommitted=%v, want %v", got, test.mayCommit)
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatalf("ReadFile: %v", readErr)
			}
			if got := string(data); got != test.wantContent {
				t.Fatalf("content=%q, want %q", got, test.wantContent)
			}
		})
	}
}

func TestWriteDurableNamespaceBarrierAndAmbiguousFailure(t *testing.T) {
	probeDir := t.TempDir()
	probe, err := os.CreateTemp(probeDir, "sync-probe")
	if err != nil {
		t.Fatal(err)
	}
	fileErr := stableio.SyncFile(probe)
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	dirErr := SyncDirectory(probeDir)
	for _, err := range []error{fileErr, dirErr} {
		if err != nil && !errors.Is(err, stableio.ErrFilePersistenceUnsupported) {
			t.Fatalf("persistence probe: %v", err)
		}
	}
	for _, failAt := range []durabilitycut.Point{"", durabilitycut.BeforeDependencyFileSync, durabilitycut.BeforeNewFileDirectorySync, durabilitycut.AfterNewFileDirectorySync} {
		t.Run(string(failAt), func(t *testing.T) {
			if (fileErr != nil && failAt != "" && failAt != durabilitycut.BeforeDependencyFileSync) || (dirErr != nil && failAt == durabilitycut.AfterNewFileDirectorySync) {
				t.Skip("platform cannot reach this successful persistence boundary")
			}
			path := filepath.Join(t.TempDir(), "CURRENT")
			if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("durable boundary failure")
			var points []durabilitycut.Point
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point != "" {
					points = append(points, event.Point)
				}
				if failAt != "" && event.Point == failAt {
					return injected
				}
				return nil
			})
			err := WriteDurable(path, []byte("new"), 0600)
			restore()
			if failAt == "" && (fileErr != nil || dirErr != nil) {
				if !errors.Is(err, stableio.ErrFilePersistenceUnsupported) || ReplacementMayHaveCommitted(err) != (fileErr == nil) {
					t.Fatalf("unsupported persistence must fail closed: err=%v ambiguous=%v", err, ReplacementMayHaveCommitted(err))
				}
				want := "old"
				if fileErr == nil {
					want = "new"
				}
				data, readErr := os.ReadFile(path)
				if readErr != nil || string(data) != want {
					t.Fatalf("unsupported persistence content=%q err=%v want=%q", data, readErr, want)
				}
				return
			}
			if failAt == "" {
				if err != nil || len(points) != 4 || points[0] != durabilitycut.BeforeDependencyFileSync || points[1] != durabilitycut.AfterDependencyFileSync || points[2] != durabilitycut.BeforeNewFileDirectorySync || points[3] != durabilitycut.AfterNewFileDirectorySync {
					t.Fatalf("durable write err=%v points=%v", err, points)
				}
			} else if !errors.Is(err, injected) {
				t.Fatalf("err=%v, want injected boundary failure", err)
			}
			afterRename := failAt == durabilitycut.BeforeNewFileDirectorySync || failAt == durabilitycut.AfterNewFileDirectorySync
			if ReplacementMayHaveCommitted(err) != afterRename {
				t.Fatalf("ambiguous=%v, want %v", ReplacementMayHaveCommitted(err), afterRename)
			}
			data, readErr := os.ReadFile(path)
			want := "new"
			if failAt == durabilitycut.BeforeDependencyFileSync {
				want = "old"
			}
			if readErr != nil || string(data) != want {
				t.Fatalf("content=%q err=%v want=%q", data, readErr, want)
			}
		})
	}
}
