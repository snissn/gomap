package node

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func FuzzNodeDecode(f *testing.F) {
	f.Add(make([]byte, page.PageSize))
	f.Fuzz(func(t *testing.T, data []byte) {
		buf := make([]byte, page.PageSize)
		copy(buf, data)
		n := NewNode(buf)
		_ = n.Type()
		_ = n.Count()
		_ = n.VerifyChecksum()
	})
}
