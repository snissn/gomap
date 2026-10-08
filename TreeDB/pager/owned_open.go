package pager

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"unsafe"
)

func openConstructorPager(path string, chunkSize int64, opts OpenOptions, readOnly bool) (*Pager, error) {
	if chunkSize <= 0 || chunkSize%page.PageSize != 0 {
		return nil, fmt.Errorf("chunk size must be a positive multiple of page size (%d)", page.PageSize)
	}
	if chunkSize%int64(os.Getpagesize()) != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of OS page size (%d)", os.Getpagesize())
	}
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of mmap allocation granularity (%d)", gran)
	}
	p, err := newConstructorPager(path, chunkSize, opts, readOnly)
	if err != nil {
		return nil, err
	}
	if readOnly {
		p.file, err = os.Open(path)
	} else {
		p.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		return p.failedOpen(err)
	}
	if testOwnedOpenAfterFile != nil {
		if err := testOwnedOpenAfterFile(p); err != nil {
			return p.failedOpen(err)
		}
	}
	if err := mmapAvailable(); err != nil {
		return p.failedOpen(err)
	}
	info, err := p.file.Stat()
	if err != nil {
		return p.failedOpen(err)
	}
	size := info.Size()
	p.durableFileSize.Store(size)
	if readOnly && size%int64(page.PageSize) != 0 {
		return p.failedOpen(ErrFileSize)
	}
	if size > 0 {
		if !readOnly && size%chunkSize != 0 {
			size = ((size / chunkSize) + 1) * chunkSize
			if err := p.file.Truncate(size); err != nil {
				return p.failedOpen(err)
			}
		}
		numChunks := (size + chunkSize - 1) / chunkSize
		if err := p.reserveKnown(uint64(numChunks)*uint64(unsafe.Sizeof([]byte{})), true); err != nil {
			return p.failedOpen(err)
		}
		p.chunks = make([][]byte, numChunks)
		if err := p.ensurePrefetchCapacityLocked(int(numChunks)); err != nil {
			return p.failedOpen(err)
		}
		for i := int64(0); i < numChunks; i++ {
			offset := i * chunkSize
			length := int(chunkSize)
			if readOnly && size-offset < int64(length) {
				length = int(size - offset)
			}
			var data []byte
			if readOnly {
				data, err = mmapFileReadOnly(p.file.Fd(), offset, length, opts.MmapPopulate)
			} else {
				data, err = mmapFile(p.file.Fd(), offset, length, opts.MmapPopulate)
			}
			if err != nil {
				return p.failedOpen(err)
			}
			p.chunks[i] = data
			madviseChunk(data)
		}
		if err := p.reserveKnown(uint64(unsafe.Sizeof(chunkList{})), true); err != nil {
			return p.failedOpen(err)
		}
		p.atomicChunks.Store(&chunkList{data: p.chunks})
		p.numPages.Store(uint64(size / int64(page.PageSize)))
		if err := p.ensureVerifiedCapacityLocked(p.numPages.Load()); err != nil {
			return p.failedOpen(err)
		}
	} else {
		if err := p.ensurePrefetchCapacityLocked(0); err != nil {
			return p.failedOpen(err)
		}
		if err := p.ensureVerifiedCapacityLocked(0); err != nil {
			return p.failedOpen(err)
		}
		if err := p.reserveKnown(uint64(unsafe.Sizeof(chunkList{})), true); err != nil {
			return p.failedOpen(err)
		}
		p.atomicChunks.Store(&chunkList{})
	}
	if !readOnly {
		p.startGrower()
	}
	return p, nil
}
