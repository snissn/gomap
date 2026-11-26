//go:build !unix

package treedb

import "testing"

type segmentInode struct {
	dev uint64
	ino uint64
}

func statSegmentInode(string) (segmentInode, bool) {
	return segmentInode{}, false
}

func countOpenDeletedSegmentFDs(*testing.T, string, map[segmentInode]string) (int, int) {
	return 0, 0
}
