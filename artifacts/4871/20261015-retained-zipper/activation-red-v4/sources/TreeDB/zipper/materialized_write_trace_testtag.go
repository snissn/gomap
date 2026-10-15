//go:build treedb_test

package zipper

// MaterializedInstalledPageForTest borrows only an already written original
// allocator ID. It creates no retained page alias or runtime counter.
func (s *MaterializedWriteWork) MaterializedInstalledPageForTest() uint64 {
 if s == nil { return 0 }; for f := s.head; f != nil; f = f.next { if f.phase == 11 { return f.result.Page } }; return 0
}
