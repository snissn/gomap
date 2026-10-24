package commitlog

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
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
var fixedWALRepairWrite = writeFixedAt
var fixedWALRepairSync = stableio.SyncFile
var fixedWALRepairOpen = rootpublication.OpenStableChildFile

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
	extents                     []fixedWALExtent // Read/repair index populated by loadFixedWAL.
}

func fixedWALAlign(n int64) int64 { return (n + fixedWALAlignment - 1) &^ (fixedWALAlignment - 1) }

// FixedWALCreationSupported reports whether atomic retained-parent publication
// and namespace persistence are supported. Other platforms retain the original
// stream WAL and full-file sync; existing fixed containers remain readable.
func FixedWALCreationSupported() bool { return fixedWALCreationSupported }

// initializeFixedWAL publishes a fully initialized private file over an unused
// empty segment. A crash before publication leaves the canonical empty stream;
// a crash after it leaves a complete fixed container. Neither path needs a
// corruption exception in loadFixedWAL. Published containers are never resized.
// The replacement handle must reach every later resource/namespace token.
func initializeFixedWAL(file **os.File, target int64, parent *os.File) (*fixedWAL, error) {
	if !FixedWALCreationSupported() {
		return nil, rootpublication.ErrNamespacePersistenceUnsupported
	}
	previous := *file
	info, err := previous.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != 0 {
		oldState, _, loadErr := loadFixedWAL(previous)
		if loadErr != nil {
			return nil, loadErr
		}
		if oldState == nil || oldState.logical != 0 {
			return nil, fmt.Errorf("commitlog: fixed WAL publication requires an unused empty segment")
		}
	}
	if parent == nil {
		parent, err = rootpublication.OpenStableParent(filepath.Dir(previous.Name()))
		if err != nil {
			return nil, err
		}
		defer parent.Close()
	}
	name := filepath.Base(previous.Name())
	if err = rootpublication.ValidateStableChildLink(parent, previous, name); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	privateName := fmt.Sprintf(".wal-initialize-%x", nonce)
	private, err := rootpublication.OpenStableChildFile(parent, privateName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = private.Close()
		// Interrupted private names are ignored by segment discovery. Removal
		// is only hygiene and never an accepted-frame durability obligation.
		removePrivateFixedWAL(parent, privateName)
	}()
	if err = durabilitycut.EmitStableNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceCommandWAL, parent.Name(), "", private.Name(), parent, private, "", privateName); err != nil {
		return nil, err
	}
	state, err := writeInitializedFixedWAL(private, target)
	if err != nil {
		return nil, err
	}
	initializedInfo, err := private.Stat()
	if err != nil {
		return nil, err
	}
	if err = rootpublication.ValidateStableChildLink(parent, previous, name); err != nil {
		return nil, err
	}
	// Retain custody of the old empty handle until the replacement is open.
	// A failed reopen must not leave the caller holding a closed descriptor.
	publishErr := publishInitializedFixedWAL(parent, privateName, name)
	replacement, openErr := rootpublication.OpenStableChildFile(parent, name, os.O_RDWR, 0)
	if openErr != nil {
		return nil, errors.Join(publishErr, openErr)
	}
	*file = replacement // Also retain custody after a failed namespace sync.
	closeErr := previous.Close()
	if publishErr != nil || closeErr != nil {
		return nil, errors.Join(publishErr, closeErr)
	}
	replacementInfo, err := replacement.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(initializedInfo, replacementInfo) {
		return nil, fmt.Errorf("%w: fixed WAL publication changed initialized identity", rootpublication.ErrResourceConflict)
	}
	return state, nil
}

// writeInitializedFixedWAL writes only an unpublished private file. Every
// reserved byte reaches stable storage before the canonical name can see it.
func writeInitializedFixedWAL(file *os.File, target int64) (*fixedWAL, error) {
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
	root := filepath.Dir(file.Name())
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, root, file.Name(), file); err != nil {
		return nil, err
	}
	if err := fixedWALInitializeSync(file); err != nil {
		return nil, err
	}
	if err := durabilitycut.EmitStablePath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, root, file.Name(), file); err != nil {
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
				// Count uses the standard library's byte scan rather than one
				// instrumented Go read per byte across the initialized reserve.
				// Every byte remains covered by the corruption check.
				if bytes.Count(block[:size], []byte{0}) != int(size) {
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
		// Only the LSN escapes this validation pass. Keep the full decoder's
		// integrity checks while borrowing bytes owned by this call's input.
		env, e := decodeCommandFrame(payload, true)
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

// flushExtent emits exactly one frame from flush's fully validated, exclusively
// owned pending buffer. Its caller supplies the validated frame's common V1/V2
// LSN; no borrowed decoder state or mutable scratch escapes the synchronous write.
func (state *fixedWAL) flushExtent(w *Writer, frame []byte, lsn uint64) error {
	end := fixedWALAlign(state.physical+fixedWALWitnessSize+int64(len(frame))) + fixedWALWitnessSize
	if end > state.capacity {
		return fmt.Errorf("commitlog: initialized WAL capacity exhausted")
	}
	// Flush has already copied the writer's buffered frames into pending.
	// Reuse its encode storage until synchronous WriteAt completes; captured
	// syncs retain the file and physical prefix, never this mutable scratch.
	imageSize := int(end - state.physical)
	var image []byte
	if w.commandBufRetain > 0 && imageSize > w.commandBufRetain {
		image = make([]byte, imageSize)
	} else {
		if cap(w.scratch) < imageSize {
			w.scratch = make([]byte, imageSize)
		}
		image = w.scratch[:imageSize]
	}
	clear(image) // Reserved witness bytes and alignment padding stay canonical.
	header := image[:fixedWALWitnessSize]
	copy(header, fixedWALExtentMagic[:])
	copy(header[8:24], state.identity[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(state.physical))
	binary.LittleEndian.PutUint64(header[32:40], uint64(state.logical))
	binary.LittleEndian.PutUint64(header[40:48], uint64(len(frame)))
	binary.LittleEndian.PutUint64(header[48:56], uint64(end))
	binary.LittleEndian.PutUint64(header[56:64], lsn)
	binary.LittleEndian.PutUint64(header[64:72], lsn)
	binary.LittleEndian.PutUint32(header[72:76], crc.Checksum(frame))
	binary.LittleEndian.PutUint32(header[92:96], crc.Checksum(header[:92]))
	copy(image[fixedWALWitnessSize:], frame)
	footer := image[len(image)-fixedWALWitnessSize:]
	copy(footer, header)
	copy(footer, fixedWALEndMagic[:])
	binary.LittleEndian.PutUint32(footer[92:96], crc.Checksum(footer[:92]))
	started := time.Now()
	err := writeFixedAt(w.f, image, state.physical)
	w.observeWriteSyscalls(1, uint64(len(image)), time.Since(started), err)
	if err != nil {
		return err
	}
	state.logical += int64(len(frame))
	state.physical = end
	state.lastLSN = lsn
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
func (w *Writer) prepareFixedSuccessor(file **os.File, parent *os.File) (*fixedWAL, int64, error) {
	state, _, err := loadFixedWAL(*file)
	if err != nil {
		return nil, 0, err
	}
	info, err := (*file).Stat()
	if err != nil {
		return nil, 0, err
	}
	needsCapacity := state != nil && state.logical == 0 && !state.fitsEmptyFrame(w.fixedIncomingBytes)
	if (state == nil && w.fixedTarget > 0 && info.Size() == 0) || needsCapacity {
		state, err = initializeFixedWAL(file, max(w.fixedTarget, w.fixedIncomingBytes), parent)
		if err != nil {
			return nil, 0, err
		}
	}
	if state != nil {
		return state, state.logical, nil
	}
	if _, err = (*file).Seek(info.Size(), io.SeekStart); err != nil {
		return nil, 0, err
	}
	return nil, info.Size(), nil
}

func (state *fixedWAL) fitsEmptyFrame(frameBytes int64) bool {
	required := fixedWALAlign(fixedWALHeaderSize+fixedWALWitnessSize+frameBytes) + fixedWALWitnessSize
	return required <= state.capacity
}

// fixedFitsAppend includes accepted but unflushed logical bytes. Each minimum
// frame needs at most four physical bytes per logical byte, so this bound
// avoids walking/copying buffered frames and remains independent of the current
// configured logical target (which may exceed this reopened file's capacity).
func (w *Writer) fixedFitsAppend(frameBytes int64) bool {
	if w.fixed == nil {
		return true
	}
	remaining := w.fixed.capacity - w.fixed.physical
	incoming := fixedWALAlign(w.fixed.physical+fixedWALWitnessSize+frameBytes) + fixedWALWitnessSize - w.fixed.physical
	buffered := w.ActiveBytes() - w.fixed.logical
	return incoming <= remaining && buffered >= 0 && buffered <= (remaining-incoming)/4
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
	// Only the append owner calls flush. Readers and repair load their own
	// immutable extent indexes; the writer needs only the physical/logical cut.
	state.extents = nil
	if len(state.pending) == 0 {
		return nil
	}
	// Validate every frame before emitting any extent: malformed later bytes
	// must not leave a physically written prefix. This pass retains all outer
	// checksums, strict envelope/payload validation and monotone LSN checks.
	if _, _, err := validateFixedWALFrames(state.pending); err != nil {
		return err
	}
	base := state.pending
	pending := base
	for len(pending) > 0 {
		size := segmentHeaderSize + int(binary.LittleEndian.Uint32(pending[:4]))
		// Both supported versions store LSN at bytes 20:28 of their common
		// header. The full pass above proved these bounds and this exact identity;
		// append exclusion keeps pending unchanged throughout both passes.
		lsn := binary.LittleEndian.Uint64(pending[segmentHeaderSize+20 : segmentHeaderSize+28])
		if err := state.flushExtent(w, pending[:size], lsn); err != nil {
			state.pending = pending
			return err
		}
		pending = pending[size:]
	}
	state.pending = base[:0]
	return nil
}

// TruncateLogicalFile is exclusive startup repair, before writer construction.
// For a fixed container it atomically publishes a fully synced replacement with
// the original header/retained complete extents and initialized zero reserve.
// It transfers the caller's owned handle to the canonical replacement; other
// retained handles keep the unchanged original inode. It cannot update a live
// Writer's buffered state, resource tokens, or logical/physical frontiers.
// On error callers must abort/close, including after ambiguous namespace sync.
// Stream files retain their original in-place truncation behavior.
func TruncateLogicalFile(file **os.File, offset int64) error {
	if file == nil || *file == nil {
		return os.ErrInvalid
	}
	previous := *file
	state, _, err := loadFixedWAL(previous)
	if err != nil {
		return err
	}
	if state == nil {
		return previous.Truncate(offset)
	}
	physical := int64(fixedWALHeaderSize)
	logical := int64(0)
	for _, extent := range state.extents {
		if logical == offset {
			break
		}
		if state.incomplete[extent.logical] != nil {
			return fmt.Errorf("%w: logical repair retains an incomplete frame", ErrCorrupt)
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
	if offset == state.logical {
		return nil // The validated reserve is already empty.
	}
	if !FixedWALCreationSupported() {
		return rootpublication.ErrNamespacePersistenceUnsupported
	}
	parent, err := rootpublication.OpenStableParent(filepath.Dir(previous.Name()))
	if err != nil {
		return err
	}
	defer parent.Close()
	name := filepath.Base(previous.Name())
	if err = rootpublication.ValidateStableChildLink(parent, previous, name); err != nil {
		return err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return err
	}
	privateName := fmt.Sprintf(".wal-repair-%x", nonce)
	private, err := rootpublication.OpenStableChildFile(parent, privateName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		_ = private.Close()
		removePrivateFixedWAL(parent, privateName)
	}()
	if err = durabilitycut.EmitStableNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceCommandWAL, parent.Name(), "", private.Name(), parent, private, "", privateName); err != nil {
		return err
	}
	block := make([]byte, 64<<10)
	for off := int64(0); off < state.capacity; {
		size := min(int64(len(block)), state.capacity-off)
		clear(block[:size])
		if retained := min(size, physical-off); retained > 0 {
			if _, err = previous.ReadAt(block[:retained], off); err != nil {
				return err
			}
		}
		if err = fixedWALRepairWrite(private, block[:size], off); err != nil {
			return err
		}
		off += size
	}
	if err = durabilitycut.EmitStablePath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, parent.Name(), private.Name(), private); err != nil {
		return err
	}
	if err = fixedWALRepairSync(private); err != nil {
		return err
	}
	if err = durabilitycut.EmitStablePath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, parent.Name(), private.Name(), private); err != nil {
		return err
	}
	initializedInfo, err := private.Stat()
	if err != nil {
		return err
	}
	if err = rootpublication.ValidateStableChildLink(parent, previous, name); err != nil {
		return err
	}
	// Full private sync precedes atomic publication. Even if the namespace
	// barrier fails, the canonical name can expose only the original retryable
	// image or the complete repair. Keep old custody until a new handle exists.
	publishErr := publishInitializedFixedWAL(parent, privateName, name)
	replacement, openErr := fixedWALRepairOpen(parent, name, os.O_RDWR, 0)
	if openErr != nil {
		return errors.Join(publishErr, openErr)
	}
	*file = replacement
	closeErr := previous.Close()
	if publishErr != nil || closeErr != nil {
		return errors.Join(publishErr, closeErr)
	}
	replacementInfo, err := replacement.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(initializedInfo, replacementInfo) {
		return fmt.Errorf("%w: fixed WAL repair changed initialized identity", rootpublication.ErrResourceConflict)
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
