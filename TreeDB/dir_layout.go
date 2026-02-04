package treedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

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
