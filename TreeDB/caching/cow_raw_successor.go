package caching

import "github.com/snissn/gomap/TreeDB/internal/memtable"

// The cache, never a read cut, owns the actual next empty writer and root.
// A preparation owns newly constructed entries until its whole-cut install.
// sourceReserved records only preaccept cancellation ownership; the source slot
// itself belongs to the dirty generation, not this successor or its empty root.
type cowRawSuccessor struct {
	writer         *memtable.COWWriter
	root           *memtable.COWRoot
	resources      *cowGenerationResources
	sourceReserved bool
}

// All actual owner cleanup runs after publication/writer gates are released.
func (s *cowRawSuccessor) drain() {
	writer, root := s.writer, s.root
	*s = cowRawSuccessor{}
	if root != nil {
		retired := root.Release()
		root = nil
		retired.Drain()
	}
	if writer != nil {
		retired := writer.Close()
		writer = nil
		retired.Drain()
	}
}
