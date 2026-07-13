//go:build darwin || freebsd || netbsd || openbsd

package rootpublication

import (
	"errors"
	"os"
	"testing"
)

func TestMoveStableChildFileNoReplaceFailsClosedWithoutAtomicPrimitive(t *testing.T) {
	source, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if StableCrossParentMoveNoReplaceSupported() {
		t.Fatal("platform unexpectedly advertises atomic cross-parent no-replace")
	}
	if installed, err := MoveStableChildFileNoReplace(source, source, "old", destination, "new"); installed || !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("installed=%v err=%v want typed unsupported", installed, err)
	}
}
