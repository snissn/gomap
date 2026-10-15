//go:build treedb_test

package zipper

// MaterializedInstalledPageForTest exposes only the smallest already written
// original allocator ID, which stays stable while later split pages are built.
func (s *MaterializedWriteWork) MaterializedInstalledPageForTest() uint64 {
	if s == nil {
		return 0
	}
	var id uint64
	for f := s.head; f != nil; f = f.next {
		if f.written && f.result.IsPage() && (id == 0 || f.result.Page < id) {
			id = f.result.Page
		}
	}
	return id
}
