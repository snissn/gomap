package db

import (
	"fmt"
	"os"
	"runtime"
)

func warnInsecureDir(dir string, notify func(error)) {
	if dir == "" || notify == nil || runtime.GOOS == "windows" {
		return
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		notify(fmt.Errorf("treedb: failed to stat dir %q: %w", dir, err))
		return
	}
	if info.Mode()&os.ModeSymlink != 0 {
		notify(fmt.Errorf("treedb: dir %q is a symlink; verify target permissions", dir))
		info, err = os.Stat(dir)
		if err != nil {
			notify(fmt.Errorf("treedb: failed to stat symlink target %q: %w", dir, err))
			return
		}
	}
	if !info.IsDir() {
		notify(fmt.Errorf("treedb: path %q is not a directory", dir))
		return
	}
	perms := info.Mode().Perm()
	if perms&0o002 != 0 {
		notify(fmt.Errorf("treedb: dir %q is world-writable (mode %o)", dir, perms))
	} else if perms&0o020 != 0 {
		notify(fmt.Errorf("treedb: dir %q is group-writable (mode %o)", dir, perms))
	}
}
