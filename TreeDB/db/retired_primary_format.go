package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const retiredPrimaryIndexFileName = "index.db.primary"
const retiredPrimaryNewFileName = "index.db.primary.new"
const retiredJointSwapMagic = "TDJCOM06"

// ValidateDATAIndexOpen inspects the namespace before any recovery or side-store
// writes. Pre-alpha PRIMARY directories require rebuilding on DATA.
func ValidateDATAIndexOpen(dir string, primaryRequested bool) error {
	if primaryRequested {
		return fmt.Errorf("%w: IndexPrimaryDirectory is retired; rebuild on DATA", ErrLegacyFormatRebuildRequired)
	}
	for _, name := range []string{retiredPrimaryIndexFileName, retiredPrimaryNewFileName} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return fmt.Errorf("%w: retired PRIMARY file %s; rebuild on DATA", ErrLegacyFormatRebuildRequired, name)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	f, err := os.Open(filepath.Join(dir, indexReadyFileName))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var magic [8]byte
	n, readErr := io.ReadFull(f, magic[:])
	closeErr := f.Close()
	if n == len(magic) && string(magic[:]) == retiredJointSwapMagic {
		return fmt.Errorf("%w: retired joint DATA/PRIMARY swap; rebuild on DATA", ErrLegacyFormatRebuildRequired)
	}
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return readErr
	}
	// Only the complete marker emitted by DATA vacuum authorizes recovery or
	// cleanup. A truncated or unknown marker cannot authorize namespace changes.
	if n != len("ready\n") || string(magic[:n]) != "ready\n" {
		return fmt.Errorf("%w: malformed DATA vacuum ready marker", ErrRecoveryRequired)
	}
	return closeErr
}
