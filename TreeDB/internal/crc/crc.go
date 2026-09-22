// Package crc is TreeDB's audited CRC-32 facade.
//
// All functions use the CRC-32/IEEE polynomial and preserve the byte-for-byte
// values produced by hash/crc32.ChecksumIEEE and hash/crc32.Update. TreeDB
// stores these checksum bytes on disk, so this package centralizes routing
// through github.com/snissn/go-crc32-asm without changing the polynomial or
// verification semantics.
//
// go-crc32-asm selects architecture-specific acceleration where available and
// uses its bit-compatible fallback on unsupported architectures; callers must
// treat either path as the same fail-closed verification primitive.
package crc

import crc32 "github.com/snissn/go-crc32-asm"

var table = crc32.MakeTable(crc32.IEEE)

// Checksum returns the CRC-32/IEEE checksum of data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, table)
}

// ChecksumParts returns the CRC-32/IEEE checksum over the concatenation of parts.
func ChecksumParts(parts ...[]byte) uint32 {
	sum := uint32(0)
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		sum = crc32.Update(sum, table, p)
	}
	return sum
}

// Update extends sum with data using the CRC-32/IEEE polynomial.
func Update(sum uint32, data []byte) uint32 {
	if len(data) == 0 {
		return sum
	}
	return crc32.Update(sum, table, data)
}
