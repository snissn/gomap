//go:build linux

package pager

import "golang.org/x/sys/unix"

func mmapPopulateFlag() int {
	return unix.MAP_POPULATE
}
