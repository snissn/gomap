package valuelog

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This entire file uses the pre-repair API: it can be copied unchanged onto
// 4dc2f422 to establish a behavioral RED, independently of candidate helpers.
func TestManagerRetirementQuarantinePartialRestore(t *testing.T) {
	for _, expected := range []bool{true, false} {
		for _, cut := range []bool{false, true} {
			name := "unexpected"
			if expected {
				name = "expected"
			}
			if cut {
				name += "/create-cut"
			} else {
				name += "/complete"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				_, path := writeIdentityPinTestSegment(t, dir)
				encoded := identityPinTestIdentity(t, path)
				qdir, qpath, err := stableDeleteQuarantinePaths(path, encoded)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(qdir, 0700); err != nil {
					t.Fatal(err)
				}
				if expected {
					if err := os.Link(path, qpath); err != nil {
						t.Fatal(err)
					}
				} else {
					// Allocate the unexpected inode while the encoded original is still live.
					if err := os.WriteFile(qpath, []byte("unexpected partial restoration"), 0600); err != nil {
						t.Fatal(err)
					}
					if rootpublication.SamePhysicalIdentity(encoded, identityPinTestIdentity(t, qpath)) {
						t.Fatal("fixture reused encoded inode")
					}
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					if err := os.Link(qpath, path); err != nil {
						t.Fatal(err)
					}
				}
				identity := identityPinTestIdentity(t, path)
				want, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				creates := 0
				wantCut := errors.New("partial restore compensation cut")
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Namespace != durabilitycut.NamespaceCreate || event.NewPath != path {
						return nil
					}
					creates++
					if _, err := os.Stat(qpath); !errors.Is(err, os.ErrNotExist) {
						t.Errorf("compensation preceded quarantine unlink: %v", err)
					}
					got, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(got, want) {
						t.Errorf("compensation lost canonical bytes: %v", err)
					}
					if cut {
						return wantCut
					}
					return nil
				})
				manager, err := NewManager(dir)
				restore()
				if manager != nil {
					if closeErr := manager.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
				if cut {
					if !errors.Is(err, wantCut) {
						t.Fatalf("partial restoration was not reconciled through compensation cut: %v", err)
					}
				} else if err != nil {
					t.Fatalf("partial restoration was not reconciled: %v", err)
				}
				if creates != 1 {
					t.Fatalf("partial restoration omitted compensation create: got %d", creates)
				}
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(identity, identityPinTestIdentity(t, path)) {
					t.Fatalf("partial restoration changed canonical identity/bytes: %v", err)
				}
				// A cut after unlink leaves an empty quarantine; retry only cleans it.
				retry, err := NewManager(dir)
				if err != nil {
					t.Fatalf("partial restoration retry: %v", err)
				}
				if err := retry.Refresh(); err != nil {
					t.Fatal(err)
				}
				if err := retry.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(qdir); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partial restore left quarantine: %v", err)
				}
				got, err = os.ReadFile(path)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("retry changed canonical bytes: %v", err)
				}
			})
		}
	}
}
