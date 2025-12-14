//go:build !linux
// +build !linux

package hashdb

func applyFadvise(fd int, size int64) {
	// no-op on non-Linux systems
}

func applyMadvise(data []byte) {
	// no-op on non-Linux systems
}
