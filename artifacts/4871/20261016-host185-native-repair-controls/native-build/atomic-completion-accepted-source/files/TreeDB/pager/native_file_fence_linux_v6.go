//go:build linux

package pager

import (
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

func nativeFileFenceSupportedV6() bool { return true }

// One descriptor extraction, fdatasync, KeepAlive, fstat, KeepAlive, extent
// result, error branch and return. Both actual stack stat operands and fixed
// descriptor/error traffic are admitted; no FileInfo is allocated. This is
// source work, not a bound on syscall or runtime scheduling latency.
func nativeFileFenceAdmissionV6() (uint64, uint64) {
	return 8, 2*uint64(unsafe.Sizeof(unix.Stat_t{})) + 2*uint64(unsafe.Sizeof(uintptr(0))) + 2*uint64(unsafe.Sizeof(error(nil))) + 2*uint64(unsafe.Sizeof((*os.File)(nil))) + 16
}

// There is intentionally one attempt per syscall, including EINTR, with no
// fsync fallback. The SAME retained file remains alive through both calls.
func nativeFileFenceOnceV6(file *os.File) (int64, error) {
	fd := int(file.Fd())
	err := unix.Fdatasync(fd)
	runtime.KeepAlive(file)
	if err != nil {
		return 0, err
	}
	var stat unix.Stat_t
	err = unix.Fstat(fd, &stat)
	runtime.KeepAlive(file)
	if err != nil {
		return 0, err
	}
	return stat.Size, nil
}
