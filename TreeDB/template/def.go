package template

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/zeebo/xxh3"
)

const (
	templateDefVer = 1
)

var (
	ErrCorruptTemplateDef = errors.New("template: corrupt template def")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeTemplateDef serializes anchors into the TemplateDefBytes format.
func EncodeTemplateDef(def TemplateDef, cfg Config) ([]byte, error) {
	cfg = NormalizeConfig(cfg)
	if len(def.Anchors) == 0 {
		return nil, ErrCorruptTemplateDef
	}
	if len(def.Anchors) > cfg.MaxAnchorsPerTemplate {
		return nil, ErrCorruptTemplateDef
	}
	total := 0
	for _, a := range def.Anchors {
		if len(a) < cfg.MinAnchorLen || len(a) > cfg.MaxAnchorLen {
			return nil, ErrCorruptTemplateDef
		}
		total += len(a)
		if total > cfg.MaxAnchorBytesTotal {
			return nil, ErrCorruptTemplateDef
		}
	}
	size := 1 + uvarintLen(uint64(len(def.Anchors)))
	for _, a := range def.Anchors {
		size += uvarintLen(uint64(len(a))) + len(a)
	}
	size += 4
	buf := make([]byte, size)
	buf[0] = templateDefVer
	off := 1
	off += binary.PutUvarint(buf[off:], uint64(len(def.Anchors)))
	for _, a := range def.Anchors {
		off += binary.PutUvarint(buf[off:], uint64(len(a)))
		copy(buf[off:], a)
		off += len(a)
	}
	crc := crc32.Checksum(buf[:off], crcTable)
	binary.LittleEndian.PutUint32(buf[off:], crc)
	off += 4
	if off != len(buf) {
		return nil, ErrCorruptTemplateDef
	}
	return buf, nil
}

// DecodeTemplateDef parses TemplateDefBytes.
func DecodeTemplateDef(buf []byte) (TemplateDef, error) {
	if len(buf) < 1+1+4 {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	if buf[0] != templateDefVer {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	if len(buf) < 5 {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	payloadLen := len(buf) - 4
	crcWant := binary.LittleEndian.Uint32(buf[payloadLen:])
	crcGot := crc32.Checksum(buf[:payloadLen], crcTable)
	if crcGot != crcWant {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	off := 1
	anchorCount64, n := binary.Uvarint(buf[off:payloadLen])
	if n <= 0 {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	off += n
	if anchorCount64 == 0 || anchorCount64 > 1<<20 {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	anchorCount := int(anchorCount64)
	anchors := make([][]byte, anchorCount)
	for i := 0; i < anchorCount; i++ {
		if off >= payloadLen {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		anchorLen64, n := binary.Uvarint(buf[off:payloadLen])
		if n <= 0 {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		off += n
		if anchorLen64 == 0 || anchorLen64 > uint64(payloadLen-off) {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		anchorLen := int(anchorLen64)
		anchors[i] = buf[off : off+anchorLen]
		off += anchorLen
	}
	if off != payloadLen {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	return TemplateDef{Anchors: anchors}, nil
}

// TemplateID computes a deterministic ID for TemplateDefBytes.
func TemplateID(defBytes []byte, salt byte) uint64 {
	if salt == 0 {
		return xxh3.Hash(defBytes)
	}
	buf := make([]byte, len(defBytes)+1)
	copy(buf, defBytes)
	buf[len(defBytes)] = salt
	return xxh3.Hash(buf)
}
