//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// These private visibility operations do not certify namespace durability.
func retirementRenameChild(source *os.File, old string, destination *os.File, name string) error {
	defer runtime.KeepAlive(source)
	defer runtime.KeepAlive(destination)
	for {
		err := unix.Renameat(int(source.Fd()), old, int(destination.Fd()), name)
		if err != unix.EINTR {
			return err
		}
	}
}

func retirementLinkChild(source *os.File, old string, destination *os.File, name string) error {
	defer runtime.KeepAlive(source)
	defer runtime.KeepAlive(destination)
	for {
		err := unix.Linkat(int(source.Fd()), old, int(destination.Fd()), name, 0)
		if err != unix.EINTR {
			return err
		}
	}
}

func retirementRemoveDirectory(parent, expected *os.File, name string) error {
	defer runtime.KeepAlive(parent)
	defer runtime.KeepAlive(expected)
	for {
		err := unix.Unlinkat(int(parent.Fd()), name, unix.AT_REMOVEDIR)
		if err != unix.EINTR {
			return err
		}
	}
}
