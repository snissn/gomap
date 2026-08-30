package pebblecompat

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const checkpointTempSuffix = ".tmp-pebblecompat-checkpoint"

func copyTreeDBCheckpoint(srcDir, destDir string) error {
	if srcDir == "" {
		return fmt.Errorf("pebblecompat: checkpoint source dir is empty")
	}
	if info, err := os.Stat(destDir); err == nil {
		return fmt.Errorf("pebblecompat: checkpoint destination already exists: %s (mode=%s)", destDir, info.Mode())
	} else if !os.IsNotExist(err) {
		return err
	}

	tmpDir := destDir + checkpointTempSuffix
	_ = os.RemoveAll(tmpDir)
	if err := os.MkdirAll(filepath.Dir(tmpDir), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	if err := copyDirRecursive(srcDir, tmpDir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}
	if err := os.Rename(tmpDir, destDir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}
	return nil
}

func shouldSkipCheckpointEntry(name string) bool {
	return name == "LOCK"
}

func copyDirRecursive(srcDir, dstDir string) error {
	return filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == srcDir {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dstDir, rel)
		if d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.MkdirAll(dstPath, info.Mode().Perm())
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("pebblecompat: symlink not supported in checkpoint copy: %s", path)
		}
		if shouldSkipCheckpointEntry(d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return copyRegularFile(path, dstPath, info.Mode().Perm())
	})
}

func copyRegularFile(src, dst string, perm fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
