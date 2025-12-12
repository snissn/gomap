package crc

import (
	"errors"
	"hash/crc32"
)

var (
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

	// ErrChecksumMismatch is returned when Verify detects a mismatch.
	ErrChecksumMismatch = errors.New("crc32c checksum mismatch")
)

// Checksum returns the CRC32C Castagnoli checksum of data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

// Verify checks that data's CRC32C Castagnoli checksum equals want.
func Verify(data []byte, want uint32) error {
	if Checksum(data) != want {
		return ErrChecksumMismatch
	}
	return nil
}

