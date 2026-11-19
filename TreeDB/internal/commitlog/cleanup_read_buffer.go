package commitlog

import (
	"bufio"
	"io"
	"os"
)

// CleanupReadBuffer reuses one read-ahead window across an invocation's closed
// stream segments. Only one Reader may use the buffer at a time. The caller
// retains its exact file and append-exclusion authority.
type CleanupReadBuffer struct {
	input  *bufio.Reader
	file   *os.File
	offset int64
}

// Open preserves the normal format detection and reader. Fixed containers
// keep their extent reader and complete reserve validation.
func (b *CleanupReadBuffer) Open(file *os.File, opts Options) (*Reader, error) {
	r, err := NewReaderFromFileWithOptions(file, opts)
	if err != nil || b == nil || r.fixed != nil {
		return r, err
	}
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	if b.input == nil {
		b.input = bufio.NewReaderSize(file, 64<<10)
	} else {
		b.input.Reset(file)
	}
	b.file, b.offset = file, offset
	r.input = b
	return r, nil
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
	position, err := b.file.Seek(offset, whence)
	if err == nil {
		b.input.Reset(b.file)
		b.offset = position
	}
	return position, err
}
