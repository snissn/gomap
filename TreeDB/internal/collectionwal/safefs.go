package collectionwal

import (
	"fmt"
	"os"
	"runtime"
)

// ValidateClassRootDir performs the portable subset of the collection WAL class
// root safety checks, including POSIX permission bits on platforms where those
// bits are authoritative. OS-specific openat/no-follow/owner/inode validation
// must run in the actual file opener before collection WAL can use a class root.
func ValidateClassRootDir(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("%w: stat class root: %w", ErrCollectionWALUnsafePath, err)
	}
	mode := info.Mode()
	if mode&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: class root is symlink", ErrCollectionWALUnsafePath)
	}
	if !mode.IsDir() {
		return fmt.Errorf("%w: class root is not a directory", ErrCollectionWALUnsafePath)
	}
	if runtime.GOOS != "windows" && mode.Perm()&0o022 != 0 {
		return fmt.Errorf("%w: class root is group/world writable", ErrCollectionWALUnsafePath)
	}
	return nil
}
