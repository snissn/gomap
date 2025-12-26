package crc

import "hash/crc32"

var table = crc32.MakeTable(crc32.Castagnoli)

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
