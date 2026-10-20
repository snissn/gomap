package mvcc

import "io/fs"

func managedHistoryAllocatedBytes(info fs.FileInfo) (int64, bool) {
	return 0, false
}
