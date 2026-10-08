package collections

import (
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"math"
)

// This concrete count/write sink owns no control allocation or operational
// graph. Its exact byte array is debited before birth, then cannot grow.
type columnManifestEncodingSink struct {
	raw      []byte
	offset   uint64
	overflow bool
}

func (s *columnManifestEncodingSink) take(n uint64) (uint64, bool) {
	if s.overflow || n > math.MaxUint64-s.offset {
		s.overflow = true
		return 0, false
	}
	start := s.offset
	s.offset += n
	if s.raw != nil && s.offset > uint64(len(s.raw)) {
		s.overflow = true
		return 0, false
	}
	return start, true
}
func (s *columnManifestEncodingSink) u16(v uint16) {
	if p, ok := s.take(2); ok && s.raw != nil {
		binary.BigEndian.PutUint16(s.raw[p:p+2], v)
	}
}
func (s *columnManifestEncodingSink) u32(v uint32) {
	if p, ok := s.take(4); ok && s.raw != nil {
		binary.BigEndian.PutUint32(s.raw[p:p+4], v)
	}
}
func (s *columnManifestEncodingSink) u64(v uint64) {
	if p, ok := s.take(8); ok && s.raw != nil {
		binary.BigEndian.PutUint64(s.raw[p:p+8], v)
	}
}
func (s *columnManifestEncodingSink) text(v string) {
	s.u64(uint64(len(v)))
	if p, ok := s.take(uint64(len(v))); ok && s.raw != nil {
		copy(s.raw[p:s.offset], v)
	}
}
func (s *columnManifestEncodingSink) allocate(account rootpublication.StableMetadataAccount) ([]byte, error) {
	if s.overflow || s.offset > uint64(math.MaxInt) {
		return nil, ErrPreparedInsertResourceLimit
	}
	if account != nil {
		class, err := rootpublication.StableBackingClassBytes(s.offset, false)
		if err != nil {
			return nil, err
		}
		if err := account.ReserveStableMetadata(class); err != nil {
			return nil, err
		}
	}
	return make([]byte, int(s.offset)), nil
}
func (s *columnManifestEncodingSink) result() ([]byte, error) {
	if s.overflow || s.offset != uint64(len(s.raw)) {
		return nil, ErrPreparedInsertResourceLimit
	}
	return s.raw, nil
}

// Each raw capacity below is one actual allocation, not a rounded sum or a
// logical element count. The enclosing operation retains the creator facet;
// these helpers neither create credit nor release cumulative attempt debit.
func reserveColumnManifestBacking(account rootpublication.StableMetadataAccount, raw uint64, scan bool) error {
	if account == nil || raw == 0 {
		return nil
	}
	class, err := rootpublication.StableBackingClassBytes(raw, scan)
	if err != nil {
		return err
	}
	return account.ReserveStableMetadata(class)
}
func reserveColumnManifestArray(account rootpublication.StableMetadataAccount, count int, element uint64) error {
	if count < 0 || element != 0 && uint64(count) > math.MaxUint64/element {
		return ErrPreparedInsertResourceLimit
	}
	return reserveColumnManifestBacking(account, uint64(count)*element, true)
}
