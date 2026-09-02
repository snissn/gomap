//go:build unix

package hashdb

import "golang.org/x/sys/unix"

func lockBytes(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Mlock(b)
}

func unlockBytes(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Munlock(b)
}

func adviseWillNeed(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Madvise(b, unix.MADV_WILLNEED)
}

func adviseRandom(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Madvise(b, unix.MADV_RANDOM)
}
