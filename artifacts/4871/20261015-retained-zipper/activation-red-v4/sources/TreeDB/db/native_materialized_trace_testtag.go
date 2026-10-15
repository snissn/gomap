//go:build treedb_test

package db

func (c *NativeMVCCCurrentDeleteV6) MaterializedInstalledPageForTest() uint64 {
 if c == nil || c.output == nil { return 0 }; return c.output.MaterializedInstalledPageForTest()
}
