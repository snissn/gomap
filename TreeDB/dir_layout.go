package treedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type openDirLayout struct {
	rootDir           string
	mainDir           string
	templatedbDir     string
	disableSideStores bool
}

func resolveOpenDirLayout(dir string, disableSideStores bool) (openDirLayout, error) {
	if dir == "" {
		return openDirLayout{}, errors.New("treedb: db dir required")
	}
	clean := filepath.Clean(dir)

	if disableSideStores {
		// Refuse obviously-root layouts to avoid silently creating a second DB at
		// <root>/index.db while the real DB lives in <root>/maindb.
		if info, err := os.Stat(filepath.Join(clean, "maindb")); err == nil && info.IsDir() {
			return openDirLayout{}, fmt.Errorf("treedb: DisableSideStores=true but dir looks like a TreeDB root (contains maindb/): %s", clean)
		}
		return openDirLayout{
			rootDir:           clean,
			mainDir:           clean,
			disableSideStores: true,
		}, nil
	}

	// Root layout: <root>/maindb, optional <root>/templatedb.
	if info, err := os.Stat(filepath.Join(clean, "maindb")); err == nil && info.IsDir() {
		return openDirLayout{
			rootDir:       clean,
			mainDir:       filepath.Join(clean, "maindb"),
			templatedbDir: filepath.Join(clean, "templatedb"),
		}, nil
	}

	// Main dir: <root>/maindb (caller may have passed maindb directly).
	if info, err := os.Stat(filepath.Join(clean, "index.db")); err == nil {
		if info.IsDir() {
			return openDirLayout{}, fmt.Errorf("treedb: index.db exists but is a directory: %s", filepath.Join(clean, "index.db"))
		}
		if filepath.Base(clean) == "maindb" {
			parent := filepath.Dir(clean)
			// Heuristic: treat <root>/maindb as a root-layout main DB dir if the
			// parent has templatedb/.
			if info, err := os.Stat(filepath.Join(parent, "templatedb")); err == nil && info.IsDir() {
				return openDirLayout{
					rootDir:       parent,
					mainDir:       clean,
					templatedbDir: filepath.Join(parent, "templatedb"),
				}, nil
			}
		}
		// Existing flat layout (index.db directly under the provided dir).
		return openDirLayout{
			rootDir:           clean,
			mainDir:           clean,
			disableSideStores: true,
		}, nil
	}

	// New DB: accept either <root> or <root>/maindb as the provided dir.
	if filepath.Base(clean) == "maindb" {
		parent := filepath.Dir(clean)
		return openDirLayout{
			rootDir:       parent,
			mainDir:       clean,
			templatedbDir: filepath.Join(parent, "templatedb"),
		}, nil
	}
	return openDirLayout{
		rootDir:       clean,
		mainDir:       filepath.Join(clean, "maindb"),
		templatedbDir: filepath.Join(clean, "templatedb"),
	}, nil
}

func resolveMainDBDir(dir string) (string, error) {
	if dir == "" {
		return "", errors.New("treedb: db dir required")
	}

	maindb := filepath.Join(dir, "maindb")
	if info, err := os.Stat(maindb); err == nil && info.IsDir() {
		return maindb, nil
	}

	if _, err := os.Stat(filepath.Join(dir, "index.db")); err == nil {
		return dir, nil
	}

	return "", fmt.Errorf("treedb: expected a TreeDB root dir (containing maindb/) or a main DB dir (containing index.db): %s", dir)
}
