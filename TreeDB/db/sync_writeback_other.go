//go:build !linux

package db

import "os"

func hintStableFileWriteback(_ *os.File) {}
