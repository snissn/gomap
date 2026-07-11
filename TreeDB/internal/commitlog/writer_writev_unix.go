//go:build unix

package commitlog

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type writevStats struct {
	syscalls uint64
	bytes    uint64
}

func writevFull(f *os.File, parts [][]byte) (writevStats, error) {
	var stats writevStats
	if f == nil {
		return stats, errors.New("commitlog: nil file")
	}
	for len(parts) > 0 {
		for len(parts) > 0 && len(parts[0]) == 0 {
			parts = parts[1:]
		}
		if len(parts) == 0 {
			return stats, nil
		}
		stats.syscalls++
		n, err := unix.Writev(int(f.Fd()), parts)
		if err != nil {
			if err == unix.EINTR || err == unix.EAGAIN {
				continue
			}
			return stats, err
		}
		if n <= 0 {
			return stats, errors.New("commitlog: short writev")
		}
		stats.bytes += uint64(n)
		written := n
		for written > 0 && len(parts) > 0 {
			if written >= len(parts[0]) {
				written -= len(parts[0])
				parts = parts[1:]
				continue
			}
			parts[0] = parts[0][written:]
			written = 0
		}
	}
	return stats, nil
}
