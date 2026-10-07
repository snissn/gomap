package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	templ "github.com/snissn/gomap/TreeDB/template"
)

// These additive causal fixtures use only fields and APIs available at 4dc2f422.
func TestManagerRegisteredParentReboundBeforeRetirement(t *testing.T) {
	for _, registry := range []bool{false, true} {
		for _, additional := range []bool{false, true} {
			for _, mode := range []string{"MarkZombie", "MarkZombieIfTracked", "RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
				pinsName, scanName := "nil", "primary"
				if registry {
					pinsName = "registry"
				}
				if additional {
					scanName = "additional"
				}
				t.Run(pinsName+"/"+scanName+"/"+mode, func(t *testing.T) {
					root := t.TempDir()
					dir := filepath.Join(root, "segments")
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
					id, path := writeIdentityPinTestSegment(t, dir)
					managerDir := dir
					if additional {
						managerDir = root
					}
					var pins *rootpublication.IdentityPinRegistry
					if registry {
						pins = rootpublication.NewIdentityPinRegistry()
					}
					manager, err := NewManagerWithStableResourcePinRegistry(managerDir, pins)
					if err != nil {
						t.Fatal(err)
					}
					defer manager.Close()
					if additional {
						if err := manager.AddScanDir(dir); err != nil {
							t.Fatal(err)
						}
					}
					file := manager.files[id]
					if file == nil {
						t.Fatal("fixture lacks registered owner")
					}
					identity := identityPinTestIdentity(t, path)
					want, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					saved := dir + "-original"
					if err := os.Rename(dir, saved); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
					original := filepath.Join(saved, filepath.Base(path))
					if err := os.Link(original, path); err != nil {
						t.Fatal(err)
					}
					call := func() error {
						switch mode {
						case "MarkZombie":
							return manager.MarkZombie(id)
						case "MarkZombieIfTracked":
							_, _, err := manager.MarkZombieIfTracked(id)
							return err
						default:
							return retirementIdentityRemove(manager, file, identity, mode)
						}
					}
					if err := call(); !errors.Is(err, rootpublication.ErrResourceConflict) {
						t.Fatalf("accepted pre-retirement parent swap via %s: %v", mode, err)
					}
					if file.closed.Load() || file.IsZombie.Load() || manager.files[id] != file || manager.retiredCount != 0 || file.stableObserved != registry {
						t.Fatal("parent refusal changed live owner/observation")
					}
					for _, name := range []string{path, original} {
						got, err := os.ReadFile(name)
						if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(identity, identityPinTestIdentity(t, name)) {
							t.Fatalf("parent refusal changed bytes/identity: %v", err)
						}
					}
					entries, err := os.ReadDir(dir)
					if err != nil || len(entries) != 1 {
						t.Fatalf("parent refusal mutated replacement namespace: %v", err)
					}
					set := manager.CurrentSetNoRefresh()
					if set.Files[id] != file {
						t.Fatal("parent refusal removed live set owner")
					}
					if err := manager.Release(set); err != nil {
						t.Fatal(err)
					}
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					if err := os.Remove(dir); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(saved, dir); err != nil {
						t.Fatal(err)
					}
					if err := call(); err != nil {
						t.Fatalf("restored original parent refused retry: %v", err)
					}
					if mode == "MarkZombie" || mode == "MarkZombieIfTracked" {
						if err := manager.RemoveSegment(id); err != nil {
							t.Fatal(err)
						}
					}
					if manager.files[id] != nil || manager.retiredCount != 0 || file.stableObserved {
						t.Fatal("successful retry retained owner/observation")
					}
					if pins != nil && pins.ActiveIdentities() != 0 {
						t.Fatal("successful retry retained registry identity")
					}
					if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("successful retry retained segment: %v", err)
					}
				})
			}
		}
	}
}

func TestManagerRegisteredParentCaptureRebound(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "segments")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			_, path := writeIdentityPinTestSegment(t, dir)
			saved := dir + "-original"
			var opened *File
			previous := swapOpenSegmentFileForTest(func(path string, id uint32, dict DictLookup, templates TemplateLookup, opts templ.DecodeOptions, cache *templateDefCache) (*File, error) {
				file, err := openFile(path, id, dict, templates, opts, cache)
				if err != nil {
					return nil, err
				}
				opened = file
				if err := os.Rename(dir, saved); err != nil {
					return nil, errors.Join(err, file.Close())
				}
				if err := os.Mkdir(dir, 0700); err != nil {
					return nil, errors.Join(err, file.Close())
				}
				if err := os.Link(filepath.Join(saved, filepath.Base(path)), path); err != nil {
					return nil, errors.Join(err, file.Close())
				}
				return file, nil
			})
			var pins *rootpublication.IdentityPinRegistry
			if registry {
				pins = rootpublication.NewIdentityPinRegistry()
			}
			manager, err := NewManagerWithStableResourcePinRegistry(dir, pins)
			swapOpenSegmentFileForTest(previous)
			if manager != nil {
				defer manager.Close()
			}
			if !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("registration published rebound parent: %v", err)
			}
			if opened == nil || !opened.closed.Load() {
				t.Fatal("registration refusal leaked opened child")
			}
			if pins != nil && pins.ActiveIdentities() != 0 {
				t.Fatal("registration refusal leaked registry observation")
			}
			if !rootpublication.SamePhysicalIdentity(identityPinTestIdentity(t, path), identityPinTestIdentity(t, filepath.Join(saved, filepath.Base(path)))) {
				t.Fatal("registration refusal changed original hard links")
			}
		})
	}
}
