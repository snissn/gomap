//go:build !unix

package commitlog

import "os"

type writevStats struct {
	syscalls uint64
	bytes    uint64
}

func writevFull(f *os.File, parts [][]byte) (writevStats, error) {
	var stats writevStats
	for _, p := range parts {
		for len(p) > 0 {
			stats.syscalls++
			n, err := f.Write(p)
			if n > 0 {
				stats.bytes += uint64(n)
				p = p[n:]
			}
			if err != nil {
				return stats, err
			}
			if n <= 0 {
				return stats, os.ErrInvalid
			}
		}
	}
	return stats, nil
}
