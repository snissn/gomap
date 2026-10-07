package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This portable case does not need symlink privileges. Both cross-parent
// operations must keep using captured directory handles after their names move.
func TestManagerSymlinkParentRetirementCrossParentHandles(t *testing.T) {
	dir := t.TempDir()
	_, path := writeIdentityPinTestSegment(t, dir)
	wantBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	wantIdentity := retirementIdentityAtPath(t, path)
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	parent, err := root.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	const qname = "captured-quarantine"
	if err := root.Mkdir(qname, 0o700); err != nil {
		t.Fatal(err)
	}
	q, err := openRetirementQuarantine(root, qname)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	saved := filepath.Join(dir, "saved-quarantine")
	if err := os.Rename(filepath.Join(dir, qname), saved); err != nil {
		t.Fatal(err)
	}
	if err := root.Mkdir(qname, 0o700); err != nil {
		t.Fatal(err)
	}
	base := filepath.Base(path)
	foreignPath := filepath.Join(dir, qname, base)
	foreignBytes := []byte("same basename in unrelated directory")
	if err := os.WriteFile(foreignPath, foreignBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	foreignIdentity := retirementIdentityAtPath(t, foreignPath)
	if err := retirementRenameChild(parent, base, q.parent, base); err != nil {
		t.Fatalf("retained cross-parent rename: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("rename left original canonical child: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(saved, base))
	if err != nil || !bytes.Equal(got, wantBytes) {
		t.Fatalf("rename missed retained quarantine: %v", err)
	}
	if err := os.WriteFile(path, []byte("rollback collision"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := retirementLinkChild(q.parent, base, parent, base); !os.IsExist(err) {
		t.Fatalf("cross-parent link replaced collision: %v", err)
	}
	got, err = os.ReadFile(path)
	if err != nil || string(got) != "rollback collision" {
		t.Fatalf("link collision changed canonical bytes: %v", err)
	}
	if err := root.Remove(base); err != nil {
		t.Fatal(err)
	}
	if err := retirementLinkChild(q.parent, base, parent, base); err != nil {
		t.Fatalf("retained cross-parent link: %v", err)
	}
	if err := q.root.Remove(base); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(got, wantBytes) || !rootpublication.SamePhysicalIdentity(retirementIdentityAtPath(t, path), wantIdentity) {
		t.Fatalf("link did not restore captured original: %v", err)
	}
	if err := q.removeDirectory(root, parent, qname); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("rebound real directory cleanup not refused: %v", err)
	}
	got, err = os.ReadFile(foreignPath)
	if err != nil || !bytes.Equal(got, foreignBytes) || !rootpublication.SamePhysicalIdentity(retirementIdentityAtPath(t, foreignPath), foreignIdentity) {
		t.Fatalf("cross-parent operation changed foreign sentinel: %v", err)
	}
	if _, err := os.Stat(filepath.Join(saved, base)); !os.IsNotExist(err) {
		t.Fatalf("captured original survived quarantine unlink: %v", err)
	}
}
