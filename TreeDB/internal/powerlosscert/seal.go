package powerlosscert

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

const (
	BundleSealSchemaVersion = "treedb-power-loss-bundle-seal/v1"
	BundleSealFileName      = "bundle_seal.json"
)

type BundleSeal struct {
	SchemaVersion string            `json:"schema_version"`
	RepositorySHA string            `json:"repository_sha"`
	Files         []BundleSealEntry `json:"files"`
}

type BundleSealEntry struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// WriteBundleSeal hashes every regular file in the completed bundle except the
// seal itself. The seal's own digest is returned for publication outside the
// artifact location (for example, in the issue closeout comment).
func WriteBundleSeal(root, repositorySHA string) (string, error) {
	if !validHex(repositorySHA, 40) {
		return "", fmt.Errorf("powerlosscert: bundle seal repository SHA is not a full SHA")
	}
	entries, err := collectBundleSealEntries(root)
	if err != nil {
		return "", err
	}
	sealPath := filepath.Join(root, BundleSealFileName)
	if err := writeJSONExclusive(sealPath, BundleSeal{
		SchemaVersion: BundleSealSchemaVersion,
		RepositorySHA: repositorySHA,
		Files:         entries,
	}); err != nil {
		return "", err
	}
	digest, err := fileSHA256(sealPath)
	if err != nil {
		return "", fmt.Errorf("powerlosscert: hash bundle seal: %w", err)
	}
	return digest, nil
}

// VerifyBundleSeal rejects any changed, missing, extra, symlinked, or
// non-regular file in a retained bundle.
func VerifyBundleSeal(root, repositorySHA string) error {
	sealPath := filepath.Join(root, BundleSealFileName)
	info, err := os.Lstat(sealPath)
	if err != nil {
		return fmt.Errorf("powerlosscert: inspect bundle seal: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("powerlosscert: bundle seal is not a regular non-symlink file")
	}
	data, err := os.ReadFile(sealPath)
	if err != nil {
		return fmt.Errorf("powerlosscert: read bundle seal: %w", err)
	}
	var seal BundleSeal
	if err := decodeStrict(data, &seal); err != nil {
		return fmt.Errorf("powerlosscert: decode bundle seal: %w", err)
	}
	if seal.SchemaVersion != BundleSealSchemaVersion {
		return fmt.Errorf("powerlosscert: bundle seal schema_version=%q want=%q", seal.SchemaVersion, BundleSealSchemaVersion)
	}
	if seal.RepositorySHA != repositorySHA {
		return fmt.Errorf("powerlosscert: bundle seal repository_sha=%s want=%s", seal.RepositorySHA, repositorySHA)
	}
	prior := ""
	for index, entry := range seal.Files {
		if entry.Path == "" || filepath.IsAbs(entry.Path) || normalizedArtifactPath(entry.Path) != entry.Path || entry.Path == BundleSealFileName || entry.Bytes < 0 || !validHex(entry.SHA256, 64) {
			return fmt.Errorf("powerlosscert: bundle seal entry %d is invalid: %+v", index, entry)
		}
		if prior != "" && entry.Path <= prior {
			return fmt.Errorf("powerlosscert: bundle seal file entries are not strictly sorted at %q", entry.Path)
		}
		prior = entry.Path
	}
	actual, err := collectBundleSealEntries(root)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, seal.Files) {
		return fmt.Errorf("powerlosscert: bundle contents do not match the retained whole-bundle seal")
	}
	return nil
}

func collectBundleSealEntries(root string) ([]BundleSealEntry, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("powerlosscert: resolve bundle seal root: %w", err)
	}
	entries := make([]BundleSealEntry, 0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root || entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == BundleSealFileName {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("powerlosscert: bundle seal encountered non-regular path %q", relative)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return err
		}
		entries = append(entries, BundleSealEntry{Path: relative, Bytes: info.Size(), SHA256: digest})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("powerlosscert: collect bundle seal entries: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}
