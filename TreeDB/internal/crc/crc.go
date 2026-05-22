package crc

import crc32 "github.com/snissn/go-crc32-asm"

var table = crc32.MakeTable(crc32.IEEE)

func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, table)
}

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

func Update(sum uint32, data []byte) uint32 {
	if len(data) == 0 {
		return sum
	}
	return crc32.Update(sum, table, data)
}
