package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Physical-parent renaming is portable and must not become successful absence.
// This fixture is additive to the original source, with no new field accesses.
func TestManagerSymlinkParentRetirementPhysicalParentRenamed(t *testing.T) {
	for _, pins := range []string{"nil", "registry"} {
		t.Run(pins, func(t *testing.T) {
			manager, file := directRetirementManager(t, pins == "registry")
			identity := retirementIdentityAtPath(t, file.Path)
			if err := manager.MarkZombie(file.ID); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			parent := filepath.Dir(file.Path)
			moved := parent + "-retained"
			if err := os.Rename(parent, moved); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Rename(moved, parent) })
			if err := manager.deleteZombieFile(file); !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("renamed physical parent lost retirement owner: %v", err)
			}
			retirementIdentityAssertOwner(t, manager, file)
			// A new empty directory at the old name is not the captured parent.
			if err := os.Mkdir(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := manager.deleteZombieFile(file); !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("replacement physical parent accepted: %v", err)
			}
			if err := os.Remove(parent); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(moved, parent); err != nil {
				t.Fatal(err)
			}
			if !rootpublication.SamePhysicalIdentity(identity, retirementIdentityAtPath(t, file.Path)) {
				t.Fatal("renamed target changed identity")
			}
			if err := manager.deleteZombieFile(file); err != nil {
				t.Fatal(err)
			}
			if manager.HasSegment(file.ID) {
				t.Fatal("restored physical-parent retry retained owner")
			}
		})
	}
}

func TestManagerSymlinkParentRetirementPhysicalAbsence(t *testing.T) {
	manager, file := directRetirementManager(t, false)
	if err := manager.MarkZombie(file.ID); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file.Path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Dir(file.Path)); err != nil {
		t.Fatal(err)
	}
	if err := manager.deleteZombieFile(file); err != nil {
		t.Fatalf("proven physical-parent child absence refused: %v", err)
	}
	if manager.HasSegment(file.ID) {
		t.Fatal("proven physical absence retained owner")
	}
}
