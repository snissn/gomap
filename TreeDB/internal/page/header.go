package page

import (
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

// PageID is the logical page number within index.db.
type PageID uint64

// PageFlags encodes the page type.
type PageFlags uint16

const (
	PageTypeMeta     PageFlags = 0x01
	PageTypeFreelist PageFlags = 0x02
	PageTypeInternal PageFlags = 0x03
	PageTypeLeaf     PageFlags = 0x04
)

// Header is the common 16-byte page header stored at the start of every page.
// It matches the on-disk layout and is intended for unsafe zero-copy casting.
type Header struct {
	PageID PageID   // bytes 0-7
	CRC    uint32   // bytes 8-11 (CRC32C of body)
	Flags  PageFlags // bytes 12-13
	Count  uint16   // bytes 14-15
}

var ErrPageTooSmall = errors.New("page buffer too small")

// UnsafeHeader returns a zero-copy view of the page header.
func UnsafeHeader(page []byte) (*Header, error) {
	if len(page) < HeaderSize {
		return nil, ErrPageTooSmall
	}
	return (*Header)(unsafe.Pointer(&page[0])), nil
}

// UnsafeBody returns a slice over the page body (excluding the header).
func UnsafeBody(page []byte) ([]byte, error) {
	if len(page) < PageSize {
		return nil, ErrPageTooSmall
	}
	return page[HeaderSize:PageSize], nil
}

// SplitPage returns zero-copy views of the header and body.
func SplitPage(page []byte) (*Header, []byte, error) {
	h, err := UnsafeHeader(page)
	if err != nil {
		return nil, nil, err
	}
	b, err := UnsafeBody(page)
	if err != nil {
		return nil, nil, err
	}
	return h, b, nil
}

// SetBodyCRC computes and sets the checksum for body.
func (h *Header) SetBodyCRC(body []byte) {
	h.CRC = crc.Checksum(body)
}

// VerifyBodyCRC verifies that body matches the checksum in the header.
func (h *Header) VerifyBodyCRC(body []byte) error {
	return crc.Verify(body, h.CRC)
}

