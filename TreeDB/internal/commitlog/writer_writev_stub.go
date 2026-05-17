//go:build !unix

package commitlog

import "os"

func writevFull(f *os.File, parts [][]byte) error {
	for _, p := range parts {
		if err := writeFull(f, p); err != nil {
			return err
		}
	}
	return nil
}
