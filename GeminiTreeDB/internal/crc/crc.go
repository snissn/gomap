package crc

import "hash/crc32"

var table = crc32.MakeTable(crc32.Castagnoli)

func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, table)
}
