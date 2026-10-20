package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Refuse before startup cleanup can remove staging names or acquire a writer
// lock. The persisted witness in the public package covers complete old stores.
func TestDATAStartupRefusesRetiredPRIMARYBeforeMutation(t *testing.T) {
	for _, name := range []string{retiredPrimaryIndexFileName, retiredPrimaryNewFileName, indexReadyFileName} {
		for _, readOnly := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/write", true: "/read"}[readOnly], func(t *testing.T) {
				dir := t.TempDir()
				payload := []byte("preserve retired companion")
				if name == indexReadyFileName {
					payload = []byte(retiredJointSwapMagic + "interrupted committed pair")
				}
				path := filepath.Join(dir, name)
				if err := os.WriteFile(path, payload, 0600); err != nil {
					t.Fatal(err)
				}
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				d, err := Open(Options{Dir: dir, ReadOnly: readOnly})
				if d != nil {
					_ = d.Close()
				}
				if d != nil || !errors.Is(err, ErrLegacyFormatRebuildRequired) {
					t.Fatalf("retired format opened: DB=%v err=%v", d != nil, err)
				}
				after, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !os.SameFile(before, after) || !bytes.Equal(got, payload) {
					t.Fatal("refusal changed original identity/content")
				}
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != 1 || entries[0].Name() != name {
					t.Fatalf("refusal changed namespace: %v", entries)
				}
			})
		}
	}
}

// Direct recovery callers share the same refusal even when they already own a
// registry. A retired joint decision must neither clean staging nor rename it
// into a missing canonical index.
func TestDATAIndexSwapRefusesRetiredPRIMARYBeforeMutation(t *testing.T) {
	for _, canonical := range []bool{false, true} {
		for _, name := range []string{retiredPrimaryNewFileName, indexReadyFileName} {
			t.Run(name+map[bool]string{false: "/missing-index", true: "/existing-index"}[canonical], func(t *testing.T) {
				dir := t.TempDir()
				files := map[string][]byte{indexNewFileName: []byte("replacement DATA")}
				files[name] = []byte("replacement PRIMARY")
				if name == indexReadyFileName {
					files[name] = []byte(retiredJointSwapMagic)
				}
				if canonical {
					files[indexFileName] = []byte("canonical DATA")
				}
				identities := make(map[string]os.FileInfo)
				for path, data := range files {
					if err := os.WriteFile(filepath.Join(dir, path), data, 0600); err != nil {
						t.Fatal(err)
					}
					info, err := os.Stat(filepath.Join(dir, path))
					if err != nil {
						t.Fatal(err)
					}
					identities[path] = info
				}
				registry := rootpublication.NewIdentityPinRegistry()
				defer registry.Close()
				if err := recoverIndexSwapWithRegistryV6(dir, registry, nil); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
					t.Fatalf("direct recovery error=%v, want rebuild refusal", err)
				}
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != len(files) {
					t.Fatalf("recovery refusal changed namespace: entries=%v error=%v", entries, err)
				}
				for path, data := range files {
					info, err := os.Stat(filepath.Join(dir, path))
					if err != nil || !os.SameFile(identities[path], info) {
						t.Fatalf("recovery refusal changed identity %s: %v", path, err)
					}
					got, err := os.ReadFile(filepath.Join(dir, path))
					if err != nil || !bytes.Equal(got, data) {
						t.Fatalf("recovery refusal changed contents %s: %v", path, err)
					}
				}
			})
		}
	}
}
