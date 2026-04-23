package valuelog

import (
	"sync/atomic"
	"time"
)

type ReadInstrumentationStats struct {
	CompactLeafDecodeCallsTotal            uint64
	CompactLeafDecodeBytesTotal            uint64
	CompactLeafAppendDirectCallsTotal      uint64
	CompactLeafAppendDirectBytesTotal      uint64
	CompactLeafAppendScratchCallsTotal     uint64
	CompactLeafAppendScratchBytesTotal     uint64
	DecodeScratchGlobalSmallHitsTotal      uint64
	DecodeScratchGlobalSmallMissesTotal    uint64
	DecodeScratchGlobalLargeHitsTotal      uint64
	DecodeScratchGlobalLargeMissesTotal    uint64
	DecodeScratchGlobalOversizeMissesTotal uint64
	DecodeScratchFileHitsTotal             uint64
	DecodeScratchFileMissesTotal           uint64
	DecodeScratchSmallPutsTotal            uint64
	DecodeScratchLargePutsTotal            uint64
	DecodeScratchFilePutsTotal             uint64
	DecodeScratchDropsTotal                uint64
	ReadUnsafeAppendCallsTotal             uint64
	ReadAppendCallsTotal                   uint64
	ReadAppendBytesTotal                   uint64
	ReadAppendLatencyNsTotal               uint64
	ReadAppendMmapCallsTotal               uint64
	ReadAppendMmapBytesTotal               uint64
	ReadAppendMmapLatencyNsTotal           uint64
	ReadAppendFileCallsTotal               uint64
	ReadAppendFileBytesTotal               uint64
	ReadAppendFileLatencyNsTotal           uint64
}

type readAppendPath uint8

const (
	readAppendPathUnknown readAppendPath = iota
	readAppendPathMmap
	readAppendPathFile
)

var compactLeafDecodeCallsTotal atomic.Uint64
var compactLeafDecodeBytesTotal atomic.Uint64
var compactLeafAppendDirectCallsTotal atomic.Uint64
var compactLeafAppendDirectBytesTotal atomic.Uint64
var compactLeafAppendScratchCallsTotal atomic.Uint64
var compactLeafAppendScratchBytesTotal atomic.Uint64
var decodeScratchGlobalSmallHitsTotal atomic.Uint64
var decodeScratchGlobalSmallMissesTotal atomic.Uint64
var decodeScratchGlobalLargeHitsTotal atomic.Uint64
var decodeScratchGlobalLargeMissesTotal atomic.Uint64
var decodeScratchGlobalOversizeMissesTotal atomic.Uint64
var decodeScratchFileHitsTotal atomic.Uint64
var decodeScratchFileMissesTotal atomic.Uint64
var decodeScratchSmallPutsTotal atomic.Uint64
var decodeScratchLargePutsTotal atomic.Uint64
var decodeScratchFilePutsTotal atomic.Uint64
var decodeScratchDropsTotal atomic.Uint64
var readUnsafeAppendCallsTotal atomic.Uint64
var readAppendCallsTotal atomic.Uint64
var readAppendBytesTotal atomic.Uint64
var readAppendLatencyNsTotal atomic.Uint64
var readAppendMmapCallsTotal atomic.Uint64
var readAppendMmapBytesTotal atomic.Uint64
var readAppendMmapLatencyNsTotal atomic.Uint64
var readAppendFileCallsTotal atomic.Uint64
var readAppendFileBytesTotal atomic.Uint64
var readAppendFileLatencyNsTotal atomic.Uint64

func ReadInstrumentationStatsSnapshot() ReadInstrumentationStats {
	return ReadInstrumentationStats{
		CompactLeafDecodeCallsTotal:            compactLeafDecodeCallsTotal.Load(),
		CompactLeafDecodeBytesTotal:            compactLeafDecodeBytesTotal.Load(),
		CompactLeafAppendDirectCallsTotal:      compactLeafAppendDirectCallsTotal.Load(),
		CompactLeafAppendDirectBytesTotal:      compactLeafAppendDirectBytesTotal.Load(),
		CompactLeafAppendScratchCallsTotal:     compactLeafAppendScratchCallsTotal.Load(),
		CompactLeafAppendScratchBytesTotal:     compactLeafAppendScratchBytesTotal.Load(),
		DecodeScratchGlobalSmallHitsTotal:      decodeScratchGlobalSmallHitsTotal.Load(),
		DecodeScratchGlobalSmallMissesTotal:    decodeScratchGlobalSmallMissesTotal.Load(),
		DecodeScratchGlobalLargeHitsTotal:      decodeScratchGlobalLargeHitsTotal.Load(),
		DecodeScratchGlobalLargeMissesTotal:    decodeScratchGlobalLargeMissesTotal.Load(),
		DecodeScratchGlobalOversizeMissesTotal: decodeScratchGlobalOversizeMissesTotal.Load(),
		DecodeScratchFileHitsTotal:             decodeScratchFileHitsTotal.Load(),
		DecodeScratchFileMissesTotal:           decodeScratchFileMissesTotal.Load(),
		DecodeScratchSmallPutsTotal:            decodeScratchSmallPutsTotal.Load(),
		DecodeScratchLargePutsTotal:            decodeScratchLargePutsTotal.Load(),
		DecodeScratchFilePutsTotal:             decodeScratchFilePutsTotal.Load(),
		DecodeScratchDropsTotal:                decodeScratchDropsTotal.Load(),
		ReadUnsafeAppendCallsTotal:             readUnsafeAppendCallsTotal.Load(),
		ReadAppendCallsTotal:                   readAppendCallsTotal.Load(),
		ReadAppendBytesTotal:                   readAppendBytesTotal.Load(),
		ReadAppendLatencyNsTotal:               readAppendLatencyNsTotal.Load(),
		ReadAppendMmapCallsTotal:               readAppendMmapCallsTotal.Load(),
		ReadAppendMmapBytesTotal:               readAppendMmapBytesTotal.Load(),
		ReadAppendMmapLatencyNsTotal:           readAppendMmapLatencyNsTotal.Load(),
		ReadAppendFileCallsTotal:               readAppendFileCallsTotal.Load(),
		ReadAppendFileBytesTotal:               readAppendFileBytesTotal.Load(),
		ReadAppendFileLatencyNsTotal:           readAppendFileLatencyNsTotal.Load(),
	}
}

func noteCompactLeafDecode(decodedBytes int) {
	if decodedBytes <= 0 {
		return
	}
	compactLeafDecodeCallsTotal.Add(1)
	compactLeafDecodeBytesTotal.Add(uint64(decodedBytes))
}

func noteCompactLeafAppendDirectDecode(decodedBytes int) {
	if decodedBytes <= 0 {
		return
	}
	compactLeafAppendDirectCallsTotal.Add(1)
	compactLeafAppendDirectBytesTotal.Add(uint64(decodedBytes))
}

func noteCompactLeafAppendScratchDecode(decodedBytes int) {
	if decodedBytes <= 0 {
		return
	}
	compactLeafAppendScratchCallsTotal.Add(1)
	compactLeafAppendScratchBytesTotal.Add(uint64(decodedBytes))
}

func noteDecodeScratchGlobalSmallHit()     { decodeScratchGlobalSmallHitsTotal.Add(1) }
func noteDecodeScratchGlobalSmallMiss()    { decodeScratchGlobalSmallMissesTotal.Add(1) }
func noteDecodeScratchGlobalLargeHit()     { decodeScratchGlobalLargeHitsTotal.Add(1) }
func noteDecodeScratchGlobalLargeMiss()    { decodeScratchGlobalLargeMissesTotal.Add(1) }
func noteDecodeScratchGlobalOversizeMiss() { decodeScratchGlobalOversizeMissesTotal.Add(1) }
func noteDecodeScratchFileHit()            { decodeScratchFileHitsTotal.Add(1) }
func noteDecodeScratchFileMiss()           { decodeScratchFileMissesTotal.Add(1) }
func noteDecodeScratchSmallPut()           { decodeScratchSmallPutsTotal.Add(1) }
func noteDecodeScratchLargePut()           { decodeScratchLargePutsTotal.Add(1) }
func noteDecodeScratchFilePut()            { decodeScratchFilePutsTotal.Add(1) }
func noteDecodeScratchDrop()               { decodeScratchDropsTotal.Add(1) }
func noteReadUnsafeAppendCall()            { readUnsafeAppendCallsTotal.Add(1) }

func noteReadAppend(path readAppendPath, bytesReturned int, dur time.Duration) {
	readAppendCallsTotal.Add(1)
	if bytesReturned > 0 {
		readAppendBytesTotal.Add(uint64(bytesReturned))
	}
	if dur > 0 {
		readAppendLatencyNsTotal.Add(uint64(dur))
	}
	switch path {
	case readAppendPathMmap:
		readAppendMmapCallsTotal.Add(1)
		if bytesReturned > 0 {
			readAppendMmapBytesTotal.Add(uint64(bytesReturned))
		}
		if dur > 0 {
			readAppendMmapLatencyNsTotal.Add(uint64(dur))
		}
	case readAppendPathFile:
		readAppendFileCallsTotal.Add(1)
		if bytesReturned > 0 {
			readAppendFileBytesTotal.Add(uint64(bytesReturned))
		}
		if dur > 0 {
			readAppendFileLatencyNsTotal.Add(uint64(dur))
		}
	}
}
