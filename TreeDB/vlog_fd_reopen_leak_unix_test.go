//go:build unix

package treedb

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

type segmentInode struct {
	dev uint64
	ino uint64
}

func statSegmentInode(path string) (segmentInode, bool) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return segmentInode{}, false
	}
	return segmentInode{dev: uint64(st.Dev), ino: uint64(st.Ino)}, true
}

func countOpenDeletedSegmentFDs(t *testing.T, valueLogDir string, deletedInodes map[segmentInode]string) (matchedInodeFDs int, procDeletedFDs int) {
	t.Helper()
	fdDir := "/dev/fd"
	if runtime.GOOS == "linux" {
		fdDir = "/proc/self/fd"
	}
	f, err := os.Open(fdDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0
		}
		t.Fatalf("Open(%s): %v", fdDir, err)
	}
	names, err := f.Readdirnames(-1)
	_ = f.Close()
	if err != nil {
		t.Fatalf("Readdirnames(%s): %v", fdDir, err)
	}
	cleanDir := filepath.Clean(valueLogDir)
	for _, name := range names {
		fd, err := strconv.Atoi(name)
		if err != nil {
			continue
		}
		if runtime.GOOS == "linux" {
			if target, err := os.Readlink(filepath.Join(fdDir, name)); err == nil {
				if strings.HasSuffix(target, " (deleted)") && strings.Contains(target, cleanDir) {
					procDeletedFDs++
				}
			}
		}
		var st syscall.Stat_t
		if err := syscall.Fstat(fd, &st); err != nil {
			continue
		}
		key := segmentInode{dev: uint64(st.Dev), ino: uint64(st.Ino)}
		if _, ok := deletedInodes[key]; ok && st.Nlink == 0 {
			matchedInodeFDs++
		}
	}
	return matchedInodeFDs, procDeletedFDs
}
