package slab

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// MagicV2 is "TRDB-SLB" (8 bytes)
	MagicV2 = "TRDB-SLB"

	// Version2 identifies the V2 format
	Version2 = 0x02

	// FileHeaderSizeV2 is 32KB (Magic + Version + Metadata + Padding)
	FileHeaderSizeV2 = 32 * 1024

	// GlobalDictSize is 40KB (increased from 32KB to accommodate ZSTD headers)
	GlobalDictSize = 40 * 1024

	// SlabV2DataStart is where Zone 0 data begins (64KB)
	SlabV2DataStart = FileHeaderSizeV2 + GlobalDictSize

	// ZoneSize is 2MB
	ZoneSize = 2 * 1024 * 1024

	// ZoneHeaderSize is 64 bytes
	ZoneHeaderSize = 64

	// maxV2RecordSize is the maximum record size that fits within a V2 zone.
	maxV2RecordSize = ZoneSize - ZoneHeaderSize

	// ZoneHeaderMagic is "ZNHD"
	ZoneHeaderMagic uint32 = 0x5A4E4844
)

// Zone Dictionary Flags
const (
	ZoneDictGlobal = 0x00 // Use Global Dictionary
	ZoneDictLocal  = 0x01 // Use Local Dictionary (stored after header)
	ZoneDictRef    = 0x02 // Use Referenced Dictionary (index in header)
)

var (
	ErrInvalidZoneHeader = errors.New("slab: invalid zone header")
)

// SlabHeaderV2 represents the fixed 32KB start of a V2 slab.
// On disk:
// [0..8]: Magic ("TRDB-SLB")
// [8]: Version (0x02)
// [9..32768]: Reserved/Padding
type SlabHeaderV2 struct {
	Magic   [8]byte
	Version uint8
	// Padding to 32KB
}

// ZoneHeader represents the 64-byte header at the start of each zone (except Zone 0? No, Spec says "at each zone boundary").
// Wait, spec says:
// 0..32KB: File Header
// 32KB..64KB: Global Dict
// 64KB..2MB: Zone 0 Data
// 2MB: Zone 1 Header
// So Zone 0 DOES NOT have a Zone Header? It implicitly uses Global?
// "Zone 0 Data: Records compressed against Global Dict." -> Yes.
// "2,097,152: Zone 1 Header" -> Yes.
type ZoneHeader struct {
	Magic      uint32 // Safety check
	DictType   uint8  // ZoneDictGlobal, ZoneDictLocal, ZoneDictRef
	DictCRC    uint32 // CRC32C of the dictionary to be used
	DictLength uint32 // Length of dictionary (if local) or ZoneID (if ref)
	Padding    [51]byte
}

func (zh *ZoneHeader) Marshal() []byte {
	var buf [ZoneHeaderSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], zh.Magic)
	buf[4] = zh.DictType
	binary.LittleEndian.PutUint32(buf[5:9], zh.DictCRC)
	binary.LittleEndian.PutUint32(buf[9:13], zh.DictLength)
	// Padding is zeros
	return buf[:]
}

func (zh *ZoneHeader) Unmarshal(buf []byte) error {
	if len(buf) < ZoneHeaderSize {
		return errors.New("buffer too small for zone header")
	}
	zh.Magic = binary.LittleEndian.Uint32(buf[0:4])
	zh.DictType = buf[4]
	zh.DictCRC = binary.LittleEndian.Uint32(buf[5:9])
	zh.DictLength = binary.LittleEndian.Uint32(buf[9:13])
	return nil
}

// verifyZoneCRC checks the CRC of the dictionary payload against the header.
func (zh *ZoneHeader) VerifyDict(dict []byte) bool {
	sum := crc32.Checksum(dict, crc32cTable)
	return sum == zh.DictCRC
}
