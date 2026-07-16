package powerlosscert

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type Bundle struct {
	Root      string
	Inventory RiskInventory
	Manifests []ChildManifest
}

func LoadBundle(root string) (Bundle, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return Bundle{}, fmt.Errorf("powerlosscert: resolve bundle root: %w", err)
	}
	inventoryData, err := os.ReadFile(filepath.Join(root, "risk_inventory.json"))
	if err != nil {
		return Bundle{}, fmt.Errorf("powerlosscert: read risk inventory: %w", err)
	}
	inventory, err := ParseRiskInventory(inventoryData)
	if err != nil {
		return Bundle{}, err
	}
	paths, err := filepath.Glob(filepath.Join(root, "manifests", "*.json"))
	if err != nil {
		return Bundle{}, fmt.Errorf("powerlosscert: enumerate child manifests: %w", err)
	}
	if len(paths) == 0 {
		return Bundle{}, fmt.Errorf("powerlosscert: bundle %q contains no child manifests", root)
	}
	manifests := make([]ChildManifest, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return Bundle{}, fmt.Errorf("powerlosscert: read child manifest %q: %w", path, err)
		}
		manifest, err := ParseChildManifest(data)
		if err != nil {
			return Bundle{}, fmt.Errorf("powerlosscert: child manifest %q: %w", path, err)
		}
		manifests = append(manifests, manifest)
	}
	sort.Slice(manifests, func(i, j int) bool {
		if manifests[i].ManifestID == manifests[j].ManifestID {
			return manifests[i].Issue < manifests[j].Issue
		}
		return manifests[i].ManifestID < manifests[j].ManifestID
	})
	return Bundle{Root: root, Inventory: inventory, Manifests: manifests}, nil
}

func VerifyArtifacts(root string, manifests []ChildManifest) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("powerlosscert: resolve artifact root: %w", err)
	}
	seen := make(map[string]string)
	for _, manifest := range manifests {
		artifacts := append([]Artifact(nil), manifest.TestBinaries...)
		for _, witness := range manifest.Witnesses {
			artifacts = append(artifacts, witness.Artifacts...)
		}
		for _, artifact := range artifacts {
			prefix := fmt.Sprintf("powerlosscert: manifest %q artifact", manifest.ManifestID)
			if err := validateArtifact(prefix, artifact); err != nil {
				return err
			}
			if prior, ok := seen[artifact.Path]; ok {
				if prior != artifact.SHA256 {
					return fmt.Errorf("%s %q has conflicting sha256 values %s and %s", prefix, artifact.Path, prior, artifact.SHA256)
				}
				continue
			}
			seen[artifact.Path] = artifact.SHA256
			fullPath := filepath.Join(root, filepath.FromSlash(artifact.Path))
			if !pathWithin(root, fullPath) {
				return fmt.Errorf("%s has unsafe path %q", prefix, artifact.Path)
			}
			info, err := os.Stat(fullPath)
			if err != nil {
				return fmt.Errorf("%s %q: %w", prefix, artifact.Path, err)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("%s %q is not a regular file", prefix, artifact.Path)
			}
			digest, err := fileSHA256(fullPath)
			if err != nil {
				return fmt.Errorf("%s hash %q: %w", prefix, artifact.Path, err)
			}
			if digest != artifact.SHA256 {
				return fmt.Errorf("%s %q sha256=%s want=%s", prefix, artifact.Path, digest, artifact.SHA256)
			}
		}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func pathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !filepath.IsAbs(rel) && (rel == "." || len(rel) < 3 || rel[:3] != ".."+string(filepath.Separator))
}
