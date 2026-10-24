package commitlog

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

const (
	fixedWALHeaderSize    = 4096
	fixedWALWitnessSize   = 96
	fixedWALAlignment     = 64
	fixedWALDefaultTarget = 1 << 20
)

// Initialization uses distinct operations so failure tests can inject an actual
// partial reserve write or full-sync failure without changing append I/O.
var fixedWALInitializeWrite = writeFixedAt
var fixedWALInitializeSync = stableio.SyncFile
var fixedWALUnacceptedSync = stableio.SyncFile

var fixedWALMagic = [8]byte{'T', 'D', 'B', 'W', 'A', 'L', 3, 0}
var fixedWALExtentMagic = [8]byte{'T', 'D', 'B', 'E', 'X', 'T', 3, 0}
var fixedWALEndMagic = [8]byte{'T', 'D', 'B', 'E', 'N', 'D', 3, 0}

type fixedWALExtent struct{ physical, logical, length, end int64 }
type fixedWAL struct {
	identity                    [16]byte
	capacity, physical, logical int64
	pending                     []byte
	lastLSN                     uint64
	reconstructedFooters        uint64
	incomplete                  map[int64]*CommandWALIncompleteFrame
	extents                     []fixedWALExtent
}

func fixedWALAlign(n int64) int64 { return (n + fixedWALAlignment - 1) &^ (fixedWALAlignment - 1) }

// initializeFixedWAL writes every reserved byte and persists content before the
// file is eligible for append. Truncate/fallocate alone is deliberately absent.
func initializeFixedWAL(file *os.File, target int64) (*fixedWAL, error) {
	if target <= 0 {
		target = fixedWALDefaultTarget
	}
	// A minimally sized record consumes an extent witness pair and alignment.
	// Reserve four physical bytes per logical byte so even tiny frames fit.
	capacity := fixedWALAlign(target*4 + fixedWALHeaderSize + 1024)
	state := &fixedWAL{capacity: capacity, physical: fixedWALHeaderSize}
	if _, err := rand.Read(state.identity[:]); err != nil {
		return nil, err
	}
	header := make([]byte, fixedWALHeaderSize)
	copy(header, fixedWALMagic[:])
	copy(header[8:24], state.identity[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(capacity))
	binary.LittleEndian.PutUint32(header[32:36], crc.Checksum(header[:32]))
	block := make([]byte, 256<<10)
	if err := fixedWALInitializeWrite(file, header, 0); err != nil {
		return nil, err
	}
	for off := int64(len(header)); off < capacity; {
		n := min(int64(len(block)), capacity-off)
		if err := fixedWALInitializeWrite(file, block[:n], off); err != nil {
			return nil, err
		}
		off += n
	}
	if err := fixedWALInitializeSync(file); err != nil {
		return nil, err
	}
	return state, nil
}
func writeFixedAt(file io.WriterAt, p []byte, offset int64) error {
	for len(p) > 0 {
		n, err := file.WriteAt(p, offset)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
		offset += int64(n)
	}
	return nil
}

// loadFixedWAL returns nil for the original stream format. For containers every
// reserve byte is examined: a zero header is never an EOF decision by itself.
func loadFixedWAL(file *os.File) (*fixedWAL, []byte, error) {
	var prefix [8]byte
	n, err := file.ReadAt(prefix[:], 0)
	if err != nil && err != io.EOF {
		return nil, nil, err
	}
	if n < 8 || prefix != fixedWALMagic {
		return nil, nil, nil
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if info.Size() < fixedWALHeaderSize {
		return nil, nil, ErrCorrupt
	}
	header := make([]byte, fixedWALHeaderSize)
	if _, err = file.ReadAt(header, 0); err != nil {
		return nil, nil, err
	}
	capacity := int64(binary.LittleEndian.Uint64(header[24:32]))
	if capacity != info.Size() || capacity < fixedWALHeaderSize || crc.Checksum(header[:32]) != binary.LittleEndian.Uint32(header[32:36]) || !allZeroBytes(header[36:]) {
		return nil, nil, ErrCorrupt
	}
	state := &fixedWAL{capacity: capacity, physical: fixedWALHeaderSize}
	copy(state.identity[:], header[8:24])
	for pos := int64(fixedWALHeaderSize); pos < capacity; {
		// Read a bounded reserve window, rather than allocating physical capacity.
		var probe [fixedWALWitnessSize]byte
		count := min(int64(len(probe)), capacity-pos)
		if _, err = file.ReadAt(probe[:count], pos); err != nil {
			return nil, nil, err
		}
		if allZeroBytes(probe[:count]) {
			// Full remainder scan detects a lost header, stale suffix, or unknown data.
			block := make([]byte, 64<<10)
			for off := pos; off < capacity; {
				size := min(int64(len(block)), capacity-off)
				if _, err = file.ReadAt(block[:size], off); err != nil {
					return nil, nil, err
				}
				if !allZeroBytes(block[:size]) {
					return nil, nil, fmt.Errorf("%w: fixed WAL occupied bytes after gap at %d", ErrCorrupt, pos)
				}
				off += size
			}
			break
		}
		if !validFixedWALWitness(probe[:], fixedWALExtentMagic, state.identity, pos) {
			return nil, nil, fmt.Errorf("%w: fixed WAL start at %d", ErrCorrupt, pos)
		}
		start := int64(binary.LittleEndian.Uint64(probe[32:40]))
		length := int64(binary.LittleEndian.Uint64(probe[40:48]))
		end := int64(binary.LittleEndian.Uint64(probe[48:56]))
		// Check bounds with subtraction before alignment/addition or allocation.
		// A valid witness CRC is integrity evidence, not a trusted memory size.
		remaining := capacity - pos
		if remaining < 2*fixedWALWitnessSize || length <= 0 || length > remaining-2*fixedWALWitnessSize || uint64(length) > uint64(^uint(0)>>1) {
			return nil, nil, ErrCorrupt
		}
		footerOffset := fixedWALAlign(pos + fixedWALWitnessSize + length)
		if footerOffset > capacity-fixedWALWitnessSize {
			return nil, nil, ErrCorrupt
		}
		if start != state.logical || length <= 0 || end != fixedWALAlign(pos+fixedWALWitnessSize+length)+fixedWALWitnessSize || end > capacity || end < pos {
			return nil, nil, ErrCorrupt
		}
		payload := make([]byte, length)
		if _, err = file.ReadAt(payload, pos+fixedWALWitnessSize); err != nil {
			return nil, nil, err
		}
		footer := make([]byte, fixedWALWitnessSize)
		if _, err = file.ReadAt(footer, end-fixedWALWitnessSize); err != nil {
			return nil, nil, err
		}
		expected := append([]byte(nil), probe[:]...)
		copy(expected, fixedWALEndMagic[:])
		binary.LittleEndian.PutUint32(expected[92:96], crc.Checksum(expected[:92]))
		var first, last uint64
		dataComplete := crc.Checksum(payload) == binary.LittleEndian.Uint32(probe[72:76])
		if !dataComplete {
			// Only a positively bounded new extent whose end is still exactly
			// initialized reserve can represent an interrupted relaxed frame.
			// An intact or partly written end plus damaged data stays corrupt.
			if !allZeroBytes(footer) {
				return nil, nil, ErrCorrupt
			}
			if len(payload) < segmentHeaderSize+56 || int(binary.LittleEndian.Uint32(payload[:4])) != len(payload)-segmentHeaderSize {
				return nil, nil, ErrCorrupt
			}
			env, e := decodeCommandFrameV2TailIdentity(payload[segmentHeaderSize:])
			if e != nil {
				return nil, nil, e
			}
			if env.DurabilityClass != CommandDurabilityRelaxed {
				return nil, nil, ErrCorrupt
			}
			if e = validateCommandEnvelopeV2Identity(env); e != nil {
				return nil, nil, e
			}
			first, last = env.LSN, env.LSN
			if state.incomplete == nil {
				state.incomplete = make(map[int64]*CommandWALIncompleteFrame)
			}
			state.incomplete[start] = &CommandWALIncompleteFrame{Envelope: env, EndOffset: start + length}
		} else {
			first, last, err = validateFixedWALFrames(payload)
			if err != nil {
				return nil, nil, err
			}
			// Narrow C reconstructs redundant framing only after the entire
			// unchanged V2 frame, CRC, identity, and order have validated.
			if !bytes.Equal(footer, expected) {
				if bytes.Equal(footer[:8], fixedWALEndMagic[:]) && binary.LittleEndian.Uint32(footer[92:96]) == crc.Checksum(footer[:92]) {
					return nil, nil, fmt.Errorf("%w: mismatched intact fixed WAL footer", ErrCorrupt)
				}
				if len(payload) < segmentHeaderSize+6 || binary.LittleEndian.Uint16(payload[segmentHeaderSize+4:segmentHeaderSize+6]) != CommandFrameVersionV2 {
					return nil, nil, ErrCorrupt
				}
				state.reconstructedFooters++
			}
		}
		if first != binary.LittleEndian.Uint64(probe[56:64]) || last != binary.LittleEndian.Uint64(probe[64:72]) {
			return nil, nil, ErrCorrupt
		}
		if state.lastLSN != 0 && first <= state.lastLSN {
			return nil, nil, ErrCommandWALDuplicateLSN
		}
		padding := make([]byte, end-fixedWALWitnessSize-pos-fixedWALWitnessSize-length)
		if len(padding) > 0 {
			if _, err = file.ReadAt(padding, pos+fixedWALWitnessSize+length); err != nil {
				return nil, nil, err
			}
			if !allZeroBytes(padding) {
				return nil, nil, ErrCorrupt
			}
		}
		state.extents = append(state.extents, fixedWALExtent{pos, start, length, end})
		state.logical += length
		state.physical = end
		state.lastLSN = last
		pos = end
	}
	return state, nil, nil
}
func validFixedWALWitness(p []byte, magic [8]byte, identity [16]byte, physical int64) bool {
	return len(p) == fixedWALWitnessSize && bytes.Equal(p[:8], magic[:]) && bytes.Equal(p[8:24], identity[:]) && binary.LittleEndian.Uint64(p[24:32]) == uint64(physical) && binary.LittleEndian.Uint32(p[92:96]) == crc.Checksum(p[:92]) && allZeroBytes(p[76:92])
}
func validateFixedWALFrames(data []byte) (first, last uint64, err error) {
	for len(data) > 0 {
		if len(data) < segmentHeaderSize {
			return 0, 0, ErrCommandWALTerminalTail
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if length <= 0 || length > len(data)-segmentHeaderSize {
			return 0, 0, ErrCommandWALTerminalTail
		}
		payload := data[segmentHeaderSize : segmentHeaderSize+length]
		if crc.Checksum(payload) != binary.LittleEndian.Uint32(data[4:8]) {
			return 0, 0, ErrCorrupt
		}
		env, e := DecodeCommandFrame(payload)
		if e != nil {
			return 0, 0, e
		}
		if first == 0 {
			first = env.LSN
		}
		if last != 0 && env.LSN <= last {
			return 0, 0, ErrCommandWALDuplicateLSN
		}
		last = env.LSN
		data = data[segmentHeaderSize+length:]
	}
	return first, last, nil
}
func (state *fixedWAL) flushExtent(w *Writer) error {
	if len(state.pending) == 0 {
		return nil
	}
	first, last, err := validateFixedWALFrames(state.pending)
	if err != nil {
		return err
	}
	end := fixedWALAlign(state.physical+fixedWALWitnessSize+int64(len(state.pending))) + fixedWALWitnessSize
	if end > state.capacity {
		return fmt.Errorf("commitlog: initialized WAL capacity exhausted")
	}
	header := make([]byte, fixedWALWitnessSize)
	copy(header, fixedWALExtentMagic[:])
	copy(header[8:24], state.identity[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(state.physical))
	binary.LittleEndian.PutUint64(header[32:40], uint64(state.logical))
	binary.LittleEndian.PutUint64(header[40:48], uint64(len(state.pending)))
	binary.LittleEndian.PutUint64(header[48:56], uint64(end))
	binary.LittleEndian.PutUint64(header[56:64], first)
	binary.LittleEndian.PutUint64(header[64:72], last)
	binary.LittleEndian.PutUint32(header[72:76], crc.Checksum(state.pending))
	binary.LittleEndian.PutUint32(header[92:96], crc.Checksum(header[:92]))
	image := make([]byte, end-state.physical)
	copy(image, header)
	copy(image[fixedWALWitnessSize:], state.pending)
	copy(header, fixedWALEndMagic[:])
	binary.LittleEndian.PutUint32(header[92:96], crc.Checksum(header[:92]))
	copy(image[len(image)-fixedWALWitnessSize:], header)
	started := time.Now()
	err = writeFixedAt(w.f, image, state.physical)
	w.observeWriteSyscalls(1, uint64(len(image)), time.Since(started), err)
	if err != nil {
		return err
	}
	state.extents = append(state.extents, fixedWALExtent{state.physical, state.logical, int64(len(state.pending)), end})
	state.logical += int64(len(state.pending))
	state.physical = end
	state.lastLSN = last
	state.pending = state.pending[:0]
	return nil
}

func (w *Writer) flushFixed() error {
	if w.fixed == nil {
		return nil
	}
	if err := w.fixed.flush(w); err != nil {
		return w.poisonCommandBuffer(err)
	}
	return nil
}

// discardUnacceptedFixedSuccessor is only for an exact freshly created
// successor that has never been installed and cannot contain an accepted frame.
// Unix removes and syncs its name. Windows offers only child-based creation
// persistence, so it restores a valid empty successor through the exact handle.
// Both keep a failed reserve from blocking recovery of the accepted predecessor.
func discardUnacceptedFixedSuccessor(parent, file *os.File) error {
	name := filepath.Base(file.Name())
	if err := rootpublication.ValidateStableChildLink(parent, file, name); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return neutralizeUnacceptedFixedSuccessor(file)
	}
	if err := rootpublication.RemoveStableChildFile(parent, name); err != nil {
		return err
	}
	return rootpublication.SyncStableNamespace(parent)
}

// Only discardUnacceptedFixedSuccessor may supply a proven fresh, never-installed
// handle. Empty legacy streams are canonical journal segments. Restoring one is
// a create-only operation, not a claim of Windows rename/removal persistence.
// Any error is retained by the caller's poisoned writer; it cannot permit ACK.
func neutralizeUnacceptedFixedSuccessor(file *os.File) error {
	if err := file.Truncate(0); err != nil {
		return err
	}
	return fixedWALUnacceptedSync(file)
}

// prepareFixedSuccessor never changes the current writer. Capacity and full
// initialization sync must succeed before a successor handle/state is installed.
func (w *Writer) prepareFixedSuccessor(file *os.File) (*fixedWAL, int64, error) {
	state, _, err := loadFixedWAL(file)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	needsCapacity := state != nil && state.logical == 0 && !state.fitsEmptyFrame(w.fixedIncomingBytes)
	if (state == nil && w.fixedTarget > 0 && info.Size() == 0) || needsCapacity {
		state, err = initializeFixedWAL(file, max(w.fixedTarget, w.fixedIncomingBytes))
		if err != nil {
			return nil, 0, err
		}
	}
	if state != nil {
		return state, state.logical, nil
	}
	if _, err = file.Seek(info.Size(), io.SeekStart); err != nil {
		return nil, 0, err
	}
	return nil, info.Size(), nil
}

func (state *fixedWAL) fitsEmptyFrame(frameBytes int64) bool {
	required := fixedWALAlign(fixedWALHeaderSize+fixedWALWitnessSize+frameBytes) + fixedWALWitnessSize
	return required <= state.capacity
}

// ensureEmptyFixedFrameCapacity runs before LSN reservation. A failed in-place
// initialization has uncertain bytes/identity, so every later write fails stop.
func (w *Writer) ensureEmptyFixedFrameCapacity(frameBytes int64) error {
	if err := w.commandBufferError(); err != nil {
		return err
	}
	if w.fixed == nil || w.ActiveBytes() != 0 {
		return nil
	}
	if w.fixed.fitsEmptyFrame(frameBytes) {
		return nil
	}
	state, err := initializeFixedWAL(w.f, max(w.fixedTarget, frameBytes))
	if err != nil {
		return w.poisonCommandBuffer(err)
	}
	w.fixed = state
	return nil
}

// StableBytes is a physical immutable extent boundary; Size is logical stream
// length. Stable resource frontiers must never use reserved physical capacity.
func (w *Writer) StableBytes() int64 {
	if w.fixed != nil {
		return w.fixed.physical
	}
	return w.Size()
}

// LogicalFileSize is used by recovery and maintenance, which operate in logical
// stream coordinates. Stable resource tokens continue to use physical bounds.
func LogicalFileSize(file *os.File) (int64, error) {
	state, _, err := loadFixedWAL(file)
	if err != nil {
		return 0, err
	}
	if state != nil {
		return state.logical, nil
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (state *fixedWAL) flush(w *Writer) error {
	if len(state.pending) == 0 {
		return nil
	}
	if _, _, err := validateFixedWALFrames(state.pending); err != nil {
		return err
	}
	base := state.pending
	pending := base
	for len(pending) > 0 {
		size := segmentHeaderSize + int(binary.LittleEndian.Uint32(pending[:4]))
		state.pending = pending[:size]
		if err := state.flushExtent(w); err != nil {
			state.pending = pending
			return err
		}
		pending = pending[size:]
	}
	state.pending = base[:0]
	return nil
}

// TruncateLogicalFile removes only complete frame extents. Retained extents are
// never rewritten; repair therefore cannot tear a previously certified frame.
// Callers retain existing append exclusion, exact-handle identity, and sync.
func TruncateLogicalFile(file *os.File, offset int64) error {
	state, _, err := loadFixedWAL(file)
	if err != nil {
		return err
	}
	if state == nil {
		return file.Truncate(offset)
	}
	physical := int64(fixedWALHeaderSize)
	logical := int64(0)
	for _, extent := range state.extents {
		if logical == offset {
			break
		}
		logical += extent.length
		physical = extent.end
		if logical > offset {
			return fmt.Errorf("%w: logical repair is inside a frame", ErrCorrupt)
		}
	}
	if logical != offset {
		return ErrCorrupt
	}
	zero := make([]byte, 64<<10)
	for physical < state.capacity {
		size := min(int64(len(zero)), state.capacity-physical)
		if err := writeFixedAt(file, zero[:size], physical); err != nil {
			return err
		}
		physical += size
	}
	return nil
}

func syncCommandWALFile(file *os.File) error {
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil && err != io.EOF {
		return err
	}
	if magic == fixedWALMagic {
		return syncWALData(file)
	}
	return stableio.SyncFile(file)
}

// fixedWALStream presents validated logical bytes from exact immutable extents
// without materializing or copying the complete log on every reader open.
type fixedWALStream struct {
	file   *os.File
	state  *fixedWAL
	offset int64
}

func (stream *fixedWALStream) Read(p []byte) (int, error) {
	if stream.offset >= stream.state.logical {
		return 0, io.EOF
	}
	n := 0
	for len(p) > 0 && stream.offset < stream.state.logical {
		index := sort.Search(len(stream.state.extents), func(i int) bool { e := stream.state.extents[i]; return e.logical+e.length > stream.offset })
		extent := stream.state.extents[index]
		inside := stream.offset - extent.logical
		size := min(int64(len(p)), extent.length-inside)
		count, err := stream.file.ReadAt(p[:size], extent.physical+fixedWALWitnessSize+inside)
		n += count
		stream.offset += int64(count)
		p = p[count:]
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
func (stream *fixedWALStream) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += stream.offset
	case io.SeekEnd:
		offset += stream.state.logical
	default:
		return 0, os.ErrInvalid
	}
	if offset < 0 {
		return 0, os.ErrInvalid
	}
	stream.offset = offset
	return offset, nil
}

// CommandWALIncompleteFrame retains only independently validated V2 identity
// from a bounded interrupted relaxed extent. It is not a valid replay frame or
// durability authority. Readers continue so later positive barriers are seen.
type CommandWALIncompleteFrame struct {
	Envelope  CommandEnvelope
	EndOffset int64
}

func (*CommandWALIncompleteFrame) Error() string { return ErrCommandWALTerminalTail.Error() }
func (*CommandWALIncompleteFrame) Unwrap() error { return ErrCommandWALTerminalTail }
