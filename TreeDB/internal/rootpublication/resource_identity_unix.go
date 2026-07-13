//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func openStableChildFile(parent *os.File, name string, flags int, perm os.FileMode) (*os.File, error) {
	if parent == nil {
		return nil, os.ErrInvalid
	}
	fd, err := unix.Openat(int(parent.Fd()), name, flags|unix.O_CLOEXEC, uint32(perm.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), parent.Name()+string(os.PathSeparator)+name), nil
}

func removeStableChildFile(parent *os.File, name string) error {
	if parent == nil {
		return os.ErrInvalid
	}
	for {
		err := unix.Unlinkat(int(parent.Fd()), name, 0)
		if err == nil || err == unix.ENOENT {
			return nil
		}
		if err != unix.EINTR {
			return err
		}
	}
}

func duplicateStableFile(file *os.File) (*os.File, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	fd, err := unix.Dup(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	return os.NewFile(uintptr(fd), file.Name()+"#stable-pin"), nil
}

func stableIdentityFromFile(file *os.File) (StableIdentity, error) {
	if file == nil {
		return StableIdentity{}, os.ErrInvalid
	}
	var stat unix.Stat_t
	for {
		err := unix.Fstat(int(file.Fd()), &stat)
		if err == nil {
			var objectID [16]byte
			binary.LittleEndian.PutUint64(objectID[:8], uint64(stat.Dev))
			binary.LittleEndian.PutUint64(objectID[8:], uint64(stat.Ino))
			return StableIdentity{Platform: runtime.GOOS, VolumeID: uint64(stat.Dev), ObjectID: objectID}, nil
		}
		if err != unix.EINTR {
			return StableIdentity{}, fmt.Errorf("fstat stable resource: %w", err)
		}
	}
}

func syncStableNamespace(parent *os.File) error {
	if parent == nil {
		return os.ErrInvalid
	}
	for {
		err := unix.Fsync(int(parent.Fd()))
		if err == nil {
			return nil
		}
		if err == unix.EINTR {
			continue
		}
		if err == unix.EINVAL || err == unix.ENOTSUP || err == unix.EPERM {
			return fmt.Errorf("%w: %v", ErrNamespacePersistenceUnsupported, err)
		}
		return err
	}
}
