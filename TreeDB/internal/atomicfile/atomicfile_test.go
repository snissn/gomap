package atomicfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
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
