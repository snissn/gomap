package commitlog

import (
	"bufio"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

const cleanupReadWindowBytes = 64 << 10

// CleanupReadBuffer reuses one read-ahead window across an invocation's closed
// stream segments. Only one Reader may use the buffer at a time. The caller
// retains the exact file and must ensure its extent is immutable.
type CleanupReadBuffer struct {
	input    *bufio.Reader
	file     *os.File
	offset   int64
	fileIO   *sync.Mutex
	released *atomic.Bool
}

// NewDetachedStreamCleanupReadBuffer serves files the caller has verified
// to be immutable streams with their cursor at zero. It holds fileIO only
// during physical reads and seeks so an owner can release the actual file
// descriptors before starting rotation. The owner must use the same mutex.
func NewDetachedStreamCleanupReadBuffer(fileIO *sync.Mutex, released *atomic.Bool) *CleanupReadBuffer {
	return &CleanupReadBuffer{fileIO: fileIO, released: released}
}

// Open preserves the normal format detection and reader. Fixed containers
// keep their extent reader and complete reserve validation.
func (b *CleanupReadBuffer) Open(file *os.File, opts Options) (*Reader, error) {
	if b != nil && b.fileIO != nil {
		b.file = file
		b.reset((*cleanupPhysicalReader)(b), 0)
		return &Reader{input: b, f: file, maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize)}, nil
	}
	r, err := NewReaderFromFileWithOptions(file, opts)
	if err != nil || b == nil || r.fixed != nil {
		return r, err
	}
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	b.file = file
	b.reset(file, offset)
	r.input = b
	return r, nil
}

func (b *CleanupReadBuffer) reset(input io.Reader, offset int64) {
	if b.input == nil {
		b.input = bufio.NewReaderSize(input, cleanupReadWindowBytes)
	} else {
		b.input.Reset(input)
	}
	b.offset = offset
}

type cleanupPhysicalReader CleanupReadBuffer

func (b *cleanupPhysicalReader) Read(p []byte) (int, error) {
	b.fileIO.Lock()
	defer b.fileIO.Unlock()
	if b.released.Load() {
		return 0, os.ErrClosed
	}
	// bufio.Reader bypasses its window for a larger destination. Limit
	// those direct reads too so rotation never waits for an entire frame.
	return b.file.Read(p[:min(len(p), cleanupReadWindowBytes)])
}

func (b *CleanupReadBuffer) Read(p []byte) (int, error) {
	n, err := b.input.Read(p)
	b.offset += int64(n)
	return n, err
}

func (b *CleanupReadBuffer) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		if offset == 0 {
			// Report consumed bytes, including error-path short reads, without
			// seeking the underlying file for each validated frame.
			return b.offset, nil
		}
		offset -= int64(b.input.Buffered())
	}
	if b.fileIO != nil {
		b.fileIO.Lock()
		defer b.fileIO.Unlock()
		if b.released.Load() {
			return 0, os.ErrClosed
		}
	}
	position, err := b.file.Seek(offset, whence)
	if err == nil {
		var input io.Reader = b.file
		if b.fileIO != nil {
			input = (*cleanupPhysicalReader)(b)
		}
		b.input.Reset(input)
		b.offset = position
	}
	return position, err
}
