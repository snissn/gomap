package largevalue

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	manifestVersion = 1
	manifestMagic   = "TDBLVM01"
	manifestHdrSize = len(manifestMagic) + 1 + 1 + 2 + 8 + 4 + 4
)

type Manifest struct {
	Version  uint8
	Flags    uint8
	TotalLen uint64
	Checksum uint32
	Chunks   []page.ValuePtr
}

var (
	ErrManifestCorrupt = errors.New("largevalue: corrupt manifest")
)

func maxChunks() int {
	return int(^uint32(0))
}

func IsManifest(data []byte) bool {
	if len(data) < manifestHdrSize {
		return false
	}
	return string(data[:len(manifestMagic)]) == manifestMagic
}

func EncodeManifest(dst []byte, totalLen uint64, chunks []page.ValuePtr, flags uint8, checksum uint32) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, fmt.Errorf("%w: no chunks", ErrManifestCorrupt)
	}
	if len(chunks) > maxChunks() {
		return nil, fmt.Errorf("%w: chunk count overflow", ErrManifestCorrupt)
	}
	for i := range chunks {
		if !page.IsValueLogFileID(chunks[i].FileID) {
			return nil, fmt.Errorf("%w: invalid chunk file id %d", ErrManifestCorrupt, chunks[i].FileID)
		}
	}

	required := manifestHdrSize + len(chunks)*page.ValuePtrSize
	if cap(dst)-len(dst) < required {
		newDst := make([]byte, len(dst), len(dst)+required)
		copy(newDst, dst)
		dst = newDst
	}

	start := len(dst)
	dst = dst[:start+required]
	out := dst[start:]
	copy(out[:len(manifestMagic)], manifestMagic)
	off := len(manifestMagic)
	out[off] = manifestVersion
	off++
	out[off] = flags
	off++
	binary.LittleEndian.PutUint16(out[off:off+2], 0)
	off += 2
	binary.LittleEndian.PutUint64(out[off:off+8], totalLen)
	off += 8
	binary.LittleEndian.PutUint32(out[off:off+4], uint32(len(chunks)))
	off += 4
	binary.LittleEndian.PutUint32(out[off:off+4], checksum)
	off += 4

	for i := range chunks {
		chunks[i].Encode(out[off : off+page.ValuePtrSize])
		off += page.ValuePtrSize
	}
	return dst, nil
}

func DecodeManifest(data []byte) (Manifest, bool, error) {
	if !IsManifest(data) {
		return Manifest{}, false, nil
	}
	if len(data) < manifestHdrSize {
		return Manifest{}, true, ErrManifestCorrupt
	}
	off := len(manifestMagic)
	version := data[off]
	off++
	flags := data[off]
	off++
	off += 2 // reserved
	totalLen := binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	chunkCount := binary.LittleEndian.Uint32(data[off : off+4])
	off += 4
	checksum := binary.LittleEndian.Uint32(data[off : off+4])
	off += 4

	if version != manifestVersion {
		return Manifest{}, true, fmt.Errorf("%w: unsupported version %d", ErrManifestCorrupt, version)
	}
	if chunkCount == 0 {
		return Manifest{}, true, fmt.Errorf("%w: empty chunk list", ErrManifestCorrupt)
	}
	if chunkCount > uint32(maxChunks()) {
		return Manifest{}, true, fmt.Errorf("%w: chunk count overflow", ErrManifestCorrupt)
	}
	needed := manifestHdrSize + int(chunkCount)*page.ValuePtrSize
	if len(data) != needed {
		return Manifest{}, true, fmt.Errorf("%w: size mismatch", ErrManifestCorrupt)
	}

	chunks := make([]page.ValuePtr, int(chunkCount))
	for i := 0; i < int(chunkCount); i++ {
		ptr := page.DecodeValuePtr(data[off : off+page.ValuePtrSize])
		if !page.IsValueLogFileID(ptr.FileID) {
			return Manifest{}, true, fmt.Errorf("%w: invalid chunk file id %d", ErrManifestCorrupt, ptr.FileID)
		}
		chunks[i] = ptr
		off += page.ValuePtrSize
	}
	return Manifest{
		Version:  version,
		Flags:    flags,
		TotalLen: totalLen,
		Checksum: checksum,
		Chunks:   chunks,
	}, true, nil
}
