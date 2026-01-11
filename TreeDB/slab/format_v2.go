package slab

import (
	"encoding/binary"
	"errors"
	"os"
)

const (
	slabMagic         = "TRDB-SLB"
	slabVersionV1     = 1
	slabVersionV2     = 2
	slabV2HeaderSize  = 32 << 10
	slabV2DictSize    = 32 << 10
	slabV2DataStart   = slabV2HeaderSize + slabV2DictSize
	slabV2ZoneSize    = 2 << 20
	slabV2HeaderBytes = 24
)

// SlabV2DataStart is the first byte offset for records in a v2 slab.
// Exported for tests that tune MaxSlabSize.
const SlabV2DataStart = slabV2DataStart

const (
	slabFlagDictReady = 1 << 0
	slabFlagDictRaw   = 1 << 1
)

var errSlabHeaderCorrupt = errors.New("slab: corrupt v2 header")

type slabHeader struct {
	Version  uint8
	Flags    uint8
	ZoneSize uint32
	DictSize uint32
	DictID   uint32
}

func defaultSlabHeaderV2(id uint32) slabHeader {
	return slabHeader{
		Version:  slabVersionV2,
		Flags:    slabFlagDictRaw,
		ZoneSize: slabV2ZoneSize,
		DictSize: slabV2DictSize,
		DictID:   id,
	}
}

func readSlabHeader(f *os.File, size int64) (slabHeader, bool, error) {
	if size < slabV2HeaderBytes {
		return slabHeader{}, false, nil
	}
	var buf [slabV2HeaderBytes]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return slabHeader{}, false, err
	}
	if string(buf[:len(slabMagic)]) != slabMagic {
		return slabHeader{}, false, nil
	}
	h := slabHeader{
		Version:  buf[8],
		Flags:    buf[9],
		ZoneSize: binary.LittleEndian.Uint32(buf[12:16]),
		DictSize: binary.LittleEndian.Uint32(buf[16:20]),
		DictID:   binary.LittleEndian.Uint32(buf[20:24]),
	}
	if h.Version != slabVersionV2 {
		return slabHeader{}, false, nil
	}
	if h.DictSize == 0 || h.ZoneSize == 0 {
		return slabHeader{}, false, errSlabHeaderCorrupt
	}
	return h, true, nil
}

func writeSlabHeader(f *os.File, h slabHeader) error {
	buf := make([]byte, slabV2HeaderSize)
	copy(buf[:len(slabMagic)], slabMagic)
	buf[8] = h.Version
	buf[9] = h.Flags
	binary.LittleEndian.PutUint32(buf[12:16], h.ZoneSize)
	binary.LittleEndian.PutUint32(buf[16:20], h.DictSize)
	binary.LittleEndian.PutUint32(buf[20:24], h.DictID)
	_, err := f.WriteAt(buf, 0)
	return err
}

func initSlabV2(f *os.File, id uint32) (slabHeader, error) {
	h := defaultSlabHeaderV2(id)
	if err := f.Truncate(slabV2DataStart); err != nil {
		return slabHeader{}, err
	}
	if err := writeSlabHeader(f, h); err != nil {
		return slabHeader{}, err
	}
	return h, nil
}
