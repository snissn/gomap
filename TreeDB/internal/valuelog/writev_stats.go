package valuelog

// RawWritevStats captures low-level writev syscall behavior for grouped raw
// frame appends.
type RawWritevStats struct {
	Syscalls uint64
	Bytes    uint64
	Iovecs   uint64
	Flushes  uint64
}

// RawWriteStats captures plain write(2)-based append behavior.
type RawWriteStats struct {
	Syscalls uint64
	Bytes    uint64
	Calls    uint64
}

// RawWritevStats returns cumulative writev syscall counters for this writer.
func (w *Writer) RawWritevStats() RawWritevStats {
	if w == nil {
		return RawWritevStats{}
	}
	return RawWritevStats{
		Syscalls: w.rawWritevSyscalls.Load(),
		Bytes:    w.rawWritevBytes.Load(),
		Iovecs:   w.rawWritevIovecs.Load(),
		Flushes:  w.rawWritevFlushes.Load(),
	}
}

// RawWriteStats returns cumulative plain write(2) counters for this writer.
func (w *Writer) RawWriteStats() RawWriteStats {
	if w == nil {
		return RawWriteStats{}
	}
	return RawWriteStats{
		Syscalls: w.rawWriteSyscalls.Load(),
		Bytes:    w.rawWriteBytes.Load(),
		Calls:    w.rawWriteCalls.Load(),
	}
}
