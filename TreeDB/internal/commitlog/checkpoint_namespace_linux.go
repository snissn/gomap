//go:build linux

package commitlog

import (
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"golang.org/x/sys/unix"
)

// Compare the existing descriptor with the retained parent's actual directory
// entry. Neither lookup opens another FD during the outstanding obligation.
func checkpointChildLink(parent, child *os.File) error {
	if parent == nil || child == nil {
		return os.ErrInvalid
	}
	var open, linked unix.Stat_t
	var err error
	for {
		err = unix.Fstat(int(child.Fd()), &open)
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		return err
	}
	for {
		err = unix.Fstatat(int(parent.Fd()), filepath.Base(child.Name()), &linked, unix.AT_SYMLINK_NOFOLLOW)
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		return err
	}
	if open.Mode&unix.S_IFMT != unix.S_IFREG || open.Ino != linked.Ino || open.Dev != linked.Dev {
		return rootpublication.ErrResourceConflict
	}
	return nil
}
