//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func stableChildProbeFixture(t testing.TB) (*os.File, string, StableIdentity) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "child")
	if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	parent, err := OpenStableParent(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	child, err := OpenStableChildFile(parent, "child", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Close() })
	identity, err := StableIdentityFromFile(child)
	if err != nil {
		t.Fatal(err)
	}
	return parent, path, identity
}

func TestStableChildIdentityProbeValidationAndReplacement(t *testing.T) {
	parent, path, identity := stableChildProbeFixture(t)
	if err := validateStableChildIdentity(parent, identity, "child"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "../child", "nested/child"} {
		if err := validateStableChildIdentity(parent, identity, name); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("invalid name %q: %v", name, err)
		}
	}
	if err := validateStableChildIdentity(nil, identity, "child"); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("nil parent: %v", err)
	}
	if err := validateStableChildIdentity(parent, identity, "missing"); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("missing child: %v", err)
	}
	if err := os.Symlink(path, filepath.Join(filepath.Dir(path), "link")); err != nil {
		t.Fatal(err)
	}
	if err := validateStableChildIdentity(parent, identity, "link"); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("followed symlink: %v", err)
	}
	if err := os.Rename(filepath.Dir(path), filepath.Dir(path)+"-retained"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(path) + "-retained") })
	if err := os.Mkdir(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("wrong parent"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateStableChildIdentity(parent, identity, "child"); err != nil {
		t.Fatalf("reopened parent pathname: %v", err)
	}
	retainedPath := filepath.Join(filepath.Dir(path)+"-retained", "child")
	if err := os.Rename(retainedPath, retainedPath+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(retainedPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateStableChildIdentity(parent, identity, "child"); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("accepted replacement under retained parent: %v", err)
	}
}

func TestStableChildIdentityProbePreservesScopedOverrides(t *testing.T) {
	parent, path, identity := stableChildProbeFixture(t)
	identity.ObjectID[0] ^= 1
	release, err := InstallStableIdentityOverridesForTesting(map[string]StableIdentity{path: identity})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := validateStableChildIdentity(parent, identity, "child"); err != nil {
		t.Fatalf("lost deterministic recovery-image identity: %v", err)
	}
	release()
	if err := validateStableChildIdentity(parent, identity, "child"); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("override survived release: %v", err)
	}
}

func stableProbeDescriptorCount(t *testing.T) int {
	t.Helper()
	for _, path := range []string{"/proc/self/fd", "/dev/fd"} {
		entries, err := os.ReadDir(path)
		if err == nil {
			return len(entries)
		}
	}
	t.Skip("descriptor enumeration unavailable on this Unix platform")
	return 0
}

func TestStableChildIdentityProbeClosesDescriptorsAndDoesNotBlock(t *testing.T) {
	parent, path, identity := stableChildProbeFixture(t)
	if err := syscall.Mkfifo(filepath.Join(filepath.Dir(path), "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- validateStableChildIdentity(parent, identity, "pipe") }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("FIFO identity: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("identity probe blocked opening a FIFO")
	}
	wrong := identity
	wrong.ObjectID[0] ^= 1
	runtime.GC()
	before := stableProbeDescriptorCount(t)
	for i := 0; i < 512; i++ {
		if err := validateStableChildIdentity(parent, identity, "child"); err != nil {
			t.Fatal(err)
		}
		if err := validateStableChildIdentity(parent, wrong, "child"); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("mismatch: %v", err)
		}
		if err := validateStableChildIdentity(parent, identity, "pipe"); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("FIFO: %v", err)
		}
		if err := validateStableChildIdentity(parent, identity, "missing"); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("missing: %v", err)
		}
	}
	// Inspect before GC: finalizers must not conceal leaked probe descriptors.
	if after := stableProbeDescriptorCount(t); after != before {
		t.Fatalf("probe descriptor count grew: before=%d after=%d", before, after)
	}
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	if err := validateStableChildIdentity(parent, identity, "child"); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("closed parent: %v", err)
	}
}

func BenchmarkStableChildIdentityProbe(b *testing.B) {
	for _, mismatch := range []bool{false, true} {
		name := "linked"
		if mismatch {
			name = "mismatch"
		}
		b.Run(name, func(b *testing.B) {
			parent, _, identity := stableChildProbeFixture(b)
			if mismatch {
				identity.ObjectID[0] ^= 1
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := validateStableChildIdentity(parent, identity, "child")
				if mismatch {
					if !errors.Is(err, ErrResourceConflict) {
						b.Fatal(err)
					}
				} else if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
