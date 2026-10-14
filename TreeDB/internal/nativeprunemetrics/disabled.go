//go:build !mvcc_native_prune

package nativeprunemetrics

import "os"

const Enabled = false

func Start() Stamp                             { return Stamp{} }
func Foreground(Fence, Stamp)                  {}
func Hold(Fence, Stamp)                        {}
func Barrier(SyncKind, error)                  {}
func SyncFile(file *os.File, _ SyncKind) error { return file.Sync() }

func Writer(Fence, Stamp) {}
