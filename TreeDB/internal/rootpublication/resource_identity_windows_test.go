//go:build windows

package rootpublication

import (
	"errors"
	"os"
	"testing"
)

func TestOpenStableChildFileFailsTypedWhenRelativePrimitiveUnavailable(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	_, err = OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("OpenStableChildFile error=%v want ErrNamespacePersistenceUnsupported", err)
	}
}
