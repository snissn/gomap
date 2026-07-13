//go:build !linux

package osadapter

import (
	"errors"
	"os"
	"testing"
)

func TestUnsupportedPlatformFailsClosed(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "resource")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if _, err := NewResourceHandle(file, ResourceHooks{}); !errors.Is(err, ErrUnsupportedPlatform) {
		t.Fatalf("resource constructor error = %v, want ErrUnsupportedPlatform", err)
	}
	if _, err := NewNamespaceHandle(file, 1, NamespaceHooks{}); !errors.Is(err, ErrUnsupportedPlatform) {
		t.Fatalf("namespace constructor error = %v, want ErrUnsupportedPlatform", err)
	}
}
