//go:build !linux

package stableio

import "os"

func writebackRange(*os.File, int64) error { return nil }
