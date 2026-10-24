package commitlog

import (
	"context"
	"io"
	"os"
)

// CommandWALSnapshotPrefix borrows an exact command-segment handle. Capture
// requires append exclusion; its owner must keep the handle and the accepted
// prefix alive until export finishes. Subsequent appends and rotations may run.
// SizeBytes is the exported physical size, including initialized reserve.
type CommandWALSnapshotPrefix struct {
	file      *os.File
	size, end int64
	header    []byte
}

// CaptureCommandWALSnapshotPrefix validates the container and freezes its
// occupied extent boundary. Reserve is synthesized during export so a later
// append cannot enter this cut. The header is retained separately because an
// empty segment may be resized before its first oversized atomic frame.
// Legacy streams retain their captured EOF. No source bytes are changed.
func CaptureCommandWALSnapshotPrefix(file *os.File) (*CommandWALSnapshotPrefix, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	state, _, err := loadFixedWAL(file)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	prefix := &CommandWALSnapshotPrefix{file: file, size: info.Size(), end: info.Size()}
	if state != nil {
		// Snapshot capture follows publication; it cannot authorize discarding an
		// identified incomplete command or other data rejected by ordinary recovery.
		if len(state.incomplete) != 0 {
			return nil, ErrCorrupt
		}
		prefix.size, prefix.end = state.capacity, state.physical
		prefix.header = make([]byte, fixedWALHeaderSize)
		if _, err := file.ReadAt(prefix.header, 0); err != nil {
			return nil, err
		}
	}
	return prefix, nil
}

func (prefix *CommandWALSnapshotPrefix) SizeBytes() int64 { return prefix.size }

// WriteToContext reads only immutable occupied bytes from the retained handle.
// It never resolves the original pathname or reads its later mutable reserve.
func (prefix *CommandWALSnapshotPrefix) WriteToContext(ctx context.Context, dst io.Writer) error {
	if prefix == nil || prefix.file == nil {
		return os.ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	write := func(p []byte) error {
		n, err := dst.Write(p)
		if err != nil {
			return err
		}
		if n != len(p) {
			return io.ErrShortWrite
		}
		return nil
	}
	offset := int64(0)
	if len(prefix.header) != 0 {
		if err := write(prefix.header); err != nil {
			return err
		}
		offset = int64(len(prefix.header))
	}
	var buffer [64 * 1024]byte
	for offset < prefix.end {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(int64(len(buffer)), prefix.end-offset)
		if _, err := prefix.file.ReadAt(buffer[:n], offset); err != nil {
			return err
		}
		if err := write(buffer[:n]); err != nil {
			return err
		}
		offset += n
	}
	clear(buffer[:])
	for offset < prefix.size {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(int64(len(buffer)), prefix.size-offset)
		if err := write(buffer[:n]); err != nil {
			return err
		}
		offset += n
	}
	return ctx.Err()
}
