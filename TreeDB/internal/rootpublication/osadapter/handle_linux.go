//go:build linux

package osadapter

import (
	"errors"
	"fmt"
	"os"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"golang.org/x/sys/unix"
)

func stableOSHandlesSupported() bool { return true }

func duplicateOpenHandle(file *os.File) (*os.File, error) {
	if file == nil {
		return nil, ErrInvalidOpenHandle
	}
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	runtime.KeepAlive(file)
	if err != nil {
		return nil, err
	}
	// The name is diagnostic metadata on os.File only. No operation resolves
	// it, and the descriptor already names the retained open-file description.
	return os.NewFile(uintptr(fd), file.Name()+" (stable-retained)"), nil
}

func inspectOpenHandle(file *os.File) (openSnapshot, error) {
	if file == nil {
		return openSnapshot{}, ErrInvalidOpenHandle
	}
	var stat unix.Stat_t
	err := unix.Fstat(int(file.Fd()), &stat)
	runtime.KeepAlive(file)
	if err != nil {
		return openSnapshot{}, err
	}
	if stat.Size < 0 {
		return openSnapshot{}, fmt.Errorf("negative file length %d", stat.Size)
	}
	fileType := stat.Mode & unix.S_IFMT
	return openSnapshot{
		identity: rootpublication.StableIdentity{
			Device: uint64(stat.Dev),
			File:   stat.Ino,
		},
		length:      uint64(stat.Size),
		regularFile: fileType == unix.S_IFREG,
		directory:   fileType == unix.S_IFDIR,
	}, nil
}

func syncOpenResource(file *os.File) error {
	return fsyncOpenFile(file, false)
}

func validateNamespacePersistence(parent *os.File) error {
	snapshot, err := inspectOpenHandle(parent)
	if err != nil {
		return fmt.Errorf("validate namespace descriptor: %w", err)
	}
	if !snapshot.directory {
		return fmt.Errorf("%w: namespace descriptor is not a directory", ErrInvalidOpenHandle)
	}
	// Linux supports fsync on directory descriptors for persistent local
	// filesystems. Some filesystems reject it; SyncNamespace maps that result
	// to ErrNamespacePersistenceUnsupported instead of silently succeeding.
	return nil
}

func syncOpenNamespace(parent *os.File) error {
	return fsyncOpenFile(parent, true)
}

func fsyncOpenFile(file *os.File, namespace bool) error {
	if file == nil {
		return ErrInvalidOpenHandle
	}
	fd := int(file.Fd())
	for {
		err := unix.Fsync(fd)
		if err == unix.EINTR {
			continue
		}
		runtime.KeepAlive(file)
		if err == nil {
			return nil
		}
		if namespace && (errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP)) {
			return errors.Join(rootpublication.ErrNamespacePersistenceUnsupported, err)
		}
		return err
	}
}
