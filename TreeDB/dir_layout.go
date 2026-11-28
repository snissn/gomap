package treedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func resolveOpenDir(dir string) (string, error) {
	if dir == "" {
		return "", errors.New("treedb: db dir required")
	}
	clean := filepath.Clean(dir)
	if info, err := os.Stat(filepath.Join(clean, "index.db")); err == nil && info.IsDir() {
		return "", fmt.Errorf("treedb: index.db exists but is a directory: %s", filepath.Join(clean, "index.db"))
	}
	return clean, nil
}
