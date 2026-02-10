//go:build linux

package pager

import "golang.org/x/sys/unix"

func mmapPopulateFlag() int {
	if disableMmapPopulate() {
		return 0
	}
	return unix.MAP_POPULATE
}
