package hashdb

import (
	"fmt"
	"unsafe"
)

const (
	metadataOffsetVersion = 16
	metadataOffsetState   = 24

	metadataVersionCurrent uint64 = 1

	metadataStateUnknown uint64 = 0
	metadataStateClean   uint64 = 1
	metadataStateDirty   uint64 = 2
)

func (h *DB) metadataVersionPtr() (*uint64, error) {
	if h.metadataMap == nil || len(h.metadataMap) < metadataOffsetVersion+8 {
		return nil, fmt.Errorf("metadata version: metadata map not initialized")
	}
	return (*uint64)(unsafe.Pointer(&h.metadataMap[metadataOffsetVersion])), nil
}

func (h *DB) metadataStatePtr() (*uint64, error) {
	if h.metadataMap == nil || len(h.metadataMap) < metadataOffsetState+8 {
		return nil, fmt.Errorf("metadata state: metadata map not initialized")
	}
	return (*uint64)(unsafe.Pointer(&h.metadataMap[metadataOffsetState])), nil
}
