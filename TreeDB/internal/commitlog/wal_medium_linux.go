//go:build linux

package commitlog

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func walFilesystemType(file *os.File) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	// Linux filesystem magic values are unsigned 32-bit identifiers, including
	// RAMFS_MAGIC whose high bit is set on 32-bit platforms.
	return uint64(uint32(stat.Type)), nil
}

func walIsExt4Mount(file *os.File, kind uint64) bool {
	// ext2 and ext3 share ext4's magic. Bind the actual descriptor to its
	// mount ID so neither a pathname replacement nor the shared magic can
	// enable this policy on an unqualified mount.
	if kind != 0xef53 {
		return false
	}
	var stat unix.Statx_t
	if err := unix.Statx(int(file.Fd()), "", unix.AT_EMPTY_PATH|unix.AT_STATX_DONT_SYNC, unix.STATX_MNT_ID, &stat); err != nil ||
		stat.Mask&unix.STATX_MNT_ID == 0 {
		return false
	}
	mounts, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer mounts.Close()
	return walMountIsExt4(bufio.NewScanner(mounts), strconv.FormatUint(stat.Mnt_id, 10))
}

func walMountIsExt4(mounts *bufio.Scanner, mountID string) bool {
	for mounts.Scan() {
		id, _, _ := strings.Cut(mounts.Text(), " ")
		if id != mountID {
			continue
		}
		_, description, ok := strings.Cut(mounts.Text(), " - ")
		fs, _, _ := strings.Cut(description, " ")
		return ok && fs == "ext4"
	}
	return false
}

// Keep the format probe on this exact descriptor without dirtying access
// times. Its stable pins share the open file description. If setting the
// flag fails, reads and fences still run with their original behavior.
func avoidWALReadAtime(file *os.File) {
	raw, err := file.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		flags, err := unix.FcntlInt(fd, unix.F_GETFL, 0)
		if err == nil && flags&unix.O_NOATIME == 0 {
			_, _ = unix.FcntlInt(fd, unix.F_SETFL, flags|unix.O_NOATIME)
		}
	})
}
