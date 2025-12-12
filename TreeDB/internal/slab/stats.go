package slab

import (
	"encoding/binary"
	"errors"
)

var ErrStatsValueSize = errors.New("slab: stats value size")

type SlabStats struct {
	DeadBytes  uint64
	TotalBytes uint64
}

func StatsKey(fileID uint32) []byte {
	key := make([]byte, 9)
	key[0] = 0x00
	copy(key[1:5], []byte("slab"))
	binary.LittleEndian.PutUint32(key[5:9], fileID)
	return key
}

func ParseStatsKey(key []byte) (uint32, bool) {
	if len(key) != 9 {
		return 0, false
	}
	if key[0] != 0x00 || string(key[1:5]) != "slab" {
		return 0, false
	}
	return binary.LittleEndian.Uint32(key[5:9]), true
}

func EncodeStatsValue(stats SlabStats) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], stats.DeadBytes)
	binary.LittleEndian.PutUint64(buf[8:16], stats.TotalBytes)
	return buf
}

func DecodeStatsValue(buf []byte) (SlabStats, error) {
	if len(buf) != 16 {
		return SlabStats{}, ErrStatsValueSize
	}
	return SlabStats{
		DeadBytes:  binary.LittleEndian.Uint64(buf[0:8]),
		TotalBytes: binary.LittleEndian.Uint64(buf[8:16]),
	}, nil
}

