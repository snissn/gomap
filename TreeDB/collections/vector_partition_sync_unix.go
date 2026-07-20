//go:build !windows

package collections

import "os"

// syncDirVPM makes namespace changes durable on platforms with directory fsync.
func syncDirVPM(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func renameVPM(old, new string) error { return os.Rename(old, new) }
