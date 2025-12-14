//go:build !unix && !windows

package hashdb

import "errors"

func lockBytes(b []byte) error {
	return errors.New("memory locking unsupported on this platform")
}

func unlockBytes(b []byte) error {
	return nil
}

func adviseWillNeed(b []byte) error {
	return nil
}

func adviseRandom(b []byte) error {
	return nil
}
