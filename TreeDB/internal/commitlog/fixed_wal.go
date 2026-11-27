package commitlog

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

const (
	fixedWALHeaderSize              = 4096
	fixedWALWitnessSize             = 96
	fixedWALAlignment               = 64
	fixedWALDefaultCapacity         = 4_199_424
	fixedWALDefaultPhysicalBudget   = 64 << 20
	fixedWALBulkSteppingMinIncoming = 2 << 20
	fixedWALBulkSteppingMinFrames   = 8
	fixedWALReadBufferSize          = 64 << 10
	fixedWALPendingRetainMaxCap     = 8 << 20
	fixedWALExtentBufPoolMaxCap     = 8 << 20
	fixedWALExtentBufPoolMinSize    = 256 << 10
)

// Read-only zero block shared by writeInitializedFixedWAL to avoid allocating
// per segment initialization and reduce pwrite syscall count for multi-MiB reserves.
var fixedWALZeroBlock [1 << 20]byte

var crc32IEEECombineMats [32][32]uint32

func init() {
	var odd, even [32]uint32
	odd[0] = crc32.IEEE
	row := uint32(1)
	for n := 1; n < 32; n++ {
		odd[n] = row
		row <<= 1
	}
	gf2MatrixSquare(&even, &odd)
	gf2MatrixSquare(&odd, &even)
	// odd is now the operator for 4 zero bits; square once more for 1 zero byte (8 bits).
	gf2MatrixSquare(&crc32IEEECombineMats[0], &odd)
	for i := 1; i < len(crc32IEEECombineMats); i++ {
		gf2MatrixSquare(&crc32IEEECombineMats[i], &crc32IEEECombineMats[i-1])
	}
}

func gf2MatrixTimes(mat *[32]uint32, vec uint32) uint32 {
	var sum uint32
	for i := 0; vec != 0; i++ {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
		vec >>= 1
	}
	return sum
}

func gf2MatrixSquare(dst, src *[32]uint32) {
	for n := 0; n < 32; n++ {
		dst[n] = gf2MatrixTimes(src, src[n])
	}
}

// crc32IEEECombine combines crc1 = CRC32_IEEE(a) and crc2 = CRC32_IEEE(b) where
// len(b) == len2, returning CRC32_IEEE(a || b).
func crc32IEEECombine(crc1, crc2 uint32, len2 int) uint32 {
	if len2 <= 0 {
		return crc1
	}
	rem := uint64(len2)
	for bit := 0; rem != 0 && bit < len(crc32IEEECombineMats); bit, rem = bit+1, rem>>1 {
		if rem&1 != 0 {
			crc1 = gf2MatrixTimes(&crc32IEEECombineMats[bit], crc1)
		}
	}
	return crc1 ^ crc2
}

var fixedWALExtentBufBucketCaps = [...]int{
	256 << 10,
	512 << 10,
	1 << 20,
	2 << 20,
	4 << 20,
	fixedWALExtentBufPoolMaxCap,
}

type fixedWALExtentBufLease struct {
	buf []byte
}

var fixedWALExtentBufPools [len(fixedWALExtentBufBucketCaps)]sync.Pool

const (
	fixedWALExtentBufStrongMaxSlots = 4
	fixedWALExtentBufStrongMaxBytes = 12 << 20
)

var (
	fixedWALExtentBufStrongMu    sync.Mutex
	fixedWALExtentBufStrongSlots [fixedWALExtentBufStrongMaxSlots]*fixedWALExtentBufLease
	fixedWALExtentBufStrongCount int
	fixedWALExtentBufStrongBytes int
	fixedWALReadBufPool          sync.Pool
)

func acquireFixedWALReadBuf(r io.Reader) *bufio.Reader {
	if v := fixedWALReadBufPool.Get(); v != nil {
		if br, ok := v.(*bufio.Reader); ok && br != nil {
			br.Reset(r)
			return br
		}
	}
	return bufio.NewReaderSize(r, fixedWALReadBufferSize)
}

func releaseFixedWALReadBuf(br *bufio.Reader) {
	if br == nil {
		return
	}
	br.Reset(nil)
	fixedWALReadBufPool.Put(br)
}

func acquireFixedWALExtentBuf(size int) (*fixedWALExtentBufLease, []byte) {
	for i, capLimit := range fixedWALExtentBufBucketCaps {
		if size <= capLimit {
			// Check bounded strong slots first so GC does not evict hot 2-4 MiB
			// WAL extent/scan buffers mid-run. Accept exact or next-size bucket.
			maxAccept := capLimit
			if i+1 < len(fixedWALExtentBufBucketCaps) {
				maxAccept = fixedWALExtentBufBucketCaps[i+1]
			}
			fixedWALExtentBufStrongMu.Lock()
			best := -1
			for idx := fixedWALExtentBufStrongCount - 1; idx >= 0; idx-- {
				c := cap(fixedWALExtentBufStrongSlots[idx].buf)
				if c >= size && c <= maxAccept {
					if best < 0 || c < cap(fixedWALExtentBufStrongSlots[best].buf) {
						best = idx
						if c == capLimit {
							break
						}
					}
				}
			}
			if best >= 0 {
				lease := fixedWALExtentBufStrongSlots[best]
				last := fixedWALExtentBufStrongCount - 1
				fixedWALExtentBufStrongSlots[best] = fixedWALExtentBufStrongSlots[last]
				fixedWALExtentBufStrongSlots[last] = nil
				fixedWALExtentBufStrongCount = last
				fixedWALExtentBufStrongBytes -= cap(lease.buf)
				fixedWALExtentBufStrongMu.Unlock()
				return lease, lease.buf[:size]
			}
			fixedWALExtentBufStrongMu.Unlock()

			if v := fixedWALExtentBufPools[i].Get(); v != nil {
				if lease, ok := v.(*fixedWALExtentBufLease); ok && lease != nil && cap(lease.buf) >= size && cap(lease.buf) <= fixedWALExtentBufPoolMaxCap {
					return lease, lease.buf[:size]
				}
			}
			lease := &fixedWALExtentBufLease{buf: make([]byte, capLimit)}
			return lease, lease.buf[:size]
		}
	}
	return nil, make([]byte, size)
}

func releaseFixedWALExtentBuf(lease *fixedWALExtentBufLease) {
	if lease == nil || cap(lease.buf) > fixedWALExtentBufPoolMaxCap {
		return
	}
	c := cap(lease.buf)
	if c >= fixedWALExtentBufPoolMinSize {
		lease.buf = lease.buf[:0]
		fixedWALExtentBufStrongMu.Lock()
		if fixedWALExtentBufStrongCount < fixedWALExtentBufStrongMaxSlots && fixedWALExtentBufStrongBytes+c <= fixedWALExtentBufStrongMaxBytes {
			fixedWALExtentBufStrongSlots[fixedWALExtentBufStrongCount] = lease
			fixedWALExtentBufStrongCount++
			fixedWALExtentBufStrongBytes += c
			fixedWALExtentBufStrongMu.Unlock()
			return
		}
		fixedWALExtentBufStrongMu.Unlock()
	}
	for i := len(fixedWALExtentBufBucketCaps) - 1; i >= 0; i-- {
		if c >= fixedWALExtentBufBucketCaps[i] {
			lease.buf = lease.buf[:0]
			fixedWALExtentBufPools[i].Put(lease)
			return
		}
	}
}

func acquireFixedWALPendingBuf(size int) []byte {
	if size < fixedWALExtentBufPoolMinSize || size > fixedWALExtentBufPoolMaxCap {
		return nil
	}
	_, buf := acquireFixedWALExtentBuf(size)
	return buf
}

func releaseFixedWALPendingBuf(buf []byte) {
	if cap(buf) < fixedWALExtentBufPoolMinSize || cap(buf) > fixedWALExtentBufPoolMaxCap {
		return
	}
	releaseFixedWALExtentBuf(&fixedWALExtentBufLease{buf: buf[:0]})
}

func transferFixedWALPendingBuf(dst, src *fixedWAL) {
	if dst == nil || src == nil || len(src.pending) != 0 || cap(src.pending) <= cap(dst.pending) || cap(src.pending) > fixedWALPendingRetainMaxCap {
		return
	}
	dst.pending = src.pending[:0]
	src.pending = nil
}

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
type fixedWALValidatedFrame struct {
	logicalStart int64
	logicalEnd   int64
	payloadLen   int64
	lsn          uint64
}
type fixedWAL struct {
	identity                    [16]byte
	capacity, physical, logical int64
	pending                     []byte
	pendingLogical              int64
	pendingPhysical             int64
	pendingTrustedEnd           int
	pendingTrustedLastLSN       uint64
	maxIncoming                 int64
	lastLSN                     uint64
	reconstructedFooters        uint64
	incomplete                  map[int64]*CommandWALIncompleteFrame
	extents                     []fixedWALExtent         // Read/repair index populated by loadFixedWAL.
	validatedFrames             []fixedWALValidatedFrame // Validated frame summaries populated by loadFixedWAL.
}

func fixedWALAlign(n int64) int64 { return (n + fixedWALAlignment - 1) &^ (fixedWALAlignment - 1) }

// FixedWALCreationSupported reports whether atomic retained-parent publication
// and namespace persistence are supported. Other platforms retain the original
// stream WAL and full-file sync; existing fixed containers remain readable.
func FixedWALCreationSupported() bool { return fixedWALCreationSupported }

// fixedWALCapacity validates an exact physical byte budget, independently of
// the journal's logical rotation target. Published files are never resized.
func fixedWALCapacity(capacity int64) (int64, error) {
	if capacity < fixedWALHeaderSize+2*fixedWALWitnessSize+fixedWALAlignment || capacity > int64(1<<63-1)-(fixedWALAlignment-1) {
		return 0, ErrRecordTooLarge
	}
	return fixedWALAlign(capacity), nil
}

func fixedWALPhysicalBudget(target int64) (int64, error) {
	if target <= 0 {
		return fixedWALDefaultPhysicalBudget, nil
	}
	const overhead = fixedWALHeaderSize + 2*fixedWALWitnessSize + (fixedWALAlignment - 1)
	if target > int64(1<<63-1)-overhead-(fixedWALAlignment-1) {
		return 0, ErrRecordTooLarge
	}
	return fixedWALAlign(target + overhead), nil
}

// fixedWALExtentEnd is the actual existing on-disk extent geometry. Capacity
// adds its final alignment, without changing a witness offset or format byte.
func fixedWALExtentEnd(start, frameBytes int64) (int64, error) {
	const overhead = 2*fixedWALWitnessSize + (fixedWALAlignment - 1)
	if start < fixedWALHeaderSize || frameBytes < 0 || start > int64(1<<63-1)-overhead || frameBytes > int64(1<<63-1)-overhead-start {
		return 0, ErrRecordTooLarge
	}
	return fixedWALAlign(start+fixedWALWitnessSize+frameBytes) + fixedWALWitnessSize, nil
}

func fixedWALFrameCapacity(frameBytes int64) (int64, error) {
	end, err := fixedWALExtentEnd(fixedWALHeaderSize, frameBytes)
	if err != nil || end > int64(1<<63-1)-(fixedWALAlignment-1) {
		return 0, ErrRecordTooLarge
	}
	return fixedWALAlign(end), nil
}

type fixedWALRotationReason uint8

const (
	fixedWALForcedRotation fixedWALRotationReason = iota
	fixedWALLogicalRotation
	fixedWALPressureRotation
)

type fixedWALSizePlan struct{ capacity, incoming int64 }

// The plan lives only during existing exclusive writer ownership. It is a
// physical reserve decision, never a logical frontier or durability receipt.
func (w *Writer) fixedSuccessorSizePlan() (fixedWALSizePlan, error) {
	return fixedSuccessorSizePlan(w.fixed, w.fixedBudget, w.fixedRotationReason, w.fixedIncomingBytes)
}

// Immutable inputs also let a journal prepare a predicted pressure reserve
// without exposing the live Writer to its worker.
func fixedSuccessorSizePlan(current *fixedWAL, budget int64, reason fixedWALRotationReason, incoming int64) (fixedWALSizePlan, error) {
	plan := fixedWALSizePlan{incoming: incoming}
	if budget <= 0 {
		return plan, nil
	}
	// Keep normal successors within the existing ahead-reserve limit.
	// The exact incoming frame can still require a larger private file.
	budget = min(budget, fixedWALDefaultPhysicalBudget)
	base := min(int64(fixedWALDefaultCapacity), budget)
	normal := base
	if reason != fixedWALForcedRotation && current != nil {
		previous := min(current.capacity, budget)
		normal = max(base, previous)
		if reason == fixedWALPressureRotation {
			// Cap before addition: an oversized atomic frame grants no growth beyond B.
			normal = budget
			if previous < budget-previous {
				normal = previous + previous
			}
		}
	}
	if plan.incoming > 0 {
		required, err := fixedWALFrameCapacity(plan.incoming)
		if err != nil {
			return fixedWALSizePlan{}, err
		}
		if reason == fixedWALPressureRotation && current != nil {
			if plan.incoming >= fixedWALBulkSteppingMinIncoming {
				// Multi-MiB bulk frames quickly churn through 4 -> 8 -> 16 MiB
				// containers (causing repeated directory fsyncs, zero-fills, and
				// cleanup scans). Step in quantized doublings (matching both
				// ahead-reserve and synchronous rotation across slightly varying
				// frame sizes) until the successor holds at least 8 frames or
				// reaches the normal budget cap.
				for normal < budget && normal/required < fixedWALBulkSteppingMinFrames {
					if normal >= budget-normal {
						normal = budget
						break
					}
					normal += normal
				}
			}
		}
		normal = max(normal, required)
	}
	capacity, err := fixedWALCapacity(normal)
	if err != nil {
		return fixedWALSizePlan{}, err
	}
	plan.capacity = capacity
	return plan, nil
}

// initializeFixedWAL publishes a fully initialized private file over an unused
// empty segment. A crash before publication leaves the canonical empty stream;
// a crash after it leaves a complete fixed container. Neither path needs a
// corruption exception in loadFixedWAL. Published containers are never resized.
// The replacement handle must reach every later resource/namespace token.
func initializeFixedWAL(file **os.File, capacity int64, parent *os.File) (*fixedWAL, error) {
	return initializeFixedWALWithParentSync(file, capacity, parent, true)
}

func initializeFixedWALWithParentSync(file **os.File, capacity int64, parent *os.File, syncParent bool) (*fixedWAL, error) {
	if !FixedWALCreationSupported() {
		return nil, rootpublication.ErrNamespacePersistenceUnsupported
	}
	if _, err := fixedWALCapacity(capacity); err != nil {
		return nil, err
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
		syncParent = true
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
	state, err := writeInitializedFixedWAL(private, capacity)
	if err != nil {
		return nil, err
	}
	return publishFixedWALReplacementWithParentSync(file, parent, private, privateName, state, syncParent)
}

// publishFixedWALReplacement is shared by synchronous initialization and the
// journal's ahead reserve. Only the append owner calls it; private is already
// fully initialized and must not be written again.
func publishFixedWALReplacement(file **os.File, parent, private *os.File, privateName string, state *fixedWAL) (*fixedWAL, error) {
	return publishFixedWALReplacementWithParentSync(file, parent, private, privateName, state, true)
}

func publishFixedWALReplacementWithParentSync(file **os.File, parent, private *os.File, privateName string, state *fixedWAL, syncParent bool) (*fixedWAL, error) {
	previous := *file
	name := filepath.Base(previous.Name())
	if err := rootpublication.ValidateStableChildLink(parent, private, privateName); err != nil {
		return nil, err
	}
	initializedInfo, err := private.Stat()
	if err != nil {
		return nil, err
	}
	if !initializedInfo.Mode().IsRegular() || initializedInfo.Size() != state.capacity {
		return nil, fmt.Errorf("%w: initialized fixed WAL reserve changed shape", rootpublication.ErrResourceConflict)
	}
	if err = rootpublication.ValidateStableChildLink(parent, previous, name); err != nil {
		return nil, err
	}
	// Retain custody of the old empty handle until the replacement is open.
	// A failed reopen must not leave the caller holding a closed descriptor.
	publishErr := publishInitializedFixedWALWithSync(parent, privateName, name, syncParent)
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

var errFixedWALAheadCancelled = errors.New("commitlog: fixed WAL ahead reserve cancelled")

// writeInitializedFixedWAL writes only an unpublished private file. Every
// reserved byte reaches stable storage before the canonical name can see it.
func writeInitializedFixedWAL(file *os.File, capacity int64) (*fixedWAL, error) {
	return writeInitializedFixedWALCancelable(file, capacity, nil)
}

func writeInitializedFixedWALCancelable(file *os.File, capacity int64, cancelled func() bool) (*fixedWAL, error) {
	// Validate before creating buffers or writing the unpublished file.
	capacity, err := fixedWALCapacity(capacity)
	if err != nil {
		return nil, err
	}
	if cancelled != nil && cancelled() {
		return nil, errFixedWALAheadCancelled
	}
	state := &fixedWAL{capacity: capacity, physical: fixedWALHeaderSize}
	if _, err := rand.Read(state.identity[:]); err != nil {
		return nil, err
	}
	header := make([]byte, fixedWALHeaderSize)
	copy(header, fixedWALMagic[:])
	copy(header[8:24], state.identity[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(capacity))
	binary.LittleEndian.PutUint32(header[32:36], crc.Checksum(header[:32]))
	if err := fixedWALInitializeWrite(file, header, 0); err != nil {
		return nil, err
	}
	const writebackHintChunk = 4 << 20
	hintWriteback := !durabilitycut.Enabled() && capacity >= writebackHintChunk
	var hintedEnd int64
	for off := int64(len(header)); off < capacity; {
		if cancelled != nil && cancelled() {
			return nil, errFixedWALAheadCancelled
		}
		n := min(int64(len(fixedWALZeroBlock)), capacity-off)
		if err := fixedWALInitializeWrite(file, fixedWALZeroBlock[:n], off); err != nil {
			return nil, err
		}
		off += n
		if hintWriteback && off-hintedEnd >= writebackHintChunk {
			hintFixedWALWriteback(file, hintedEnd, off-hintedEnd)
			hintedEnd = off
		}
	}
	if cancelled != nil && cancelled() {
		return nil, errFixedWALAheadCancelled
	}
	if hintWriteback && hintedEnd < capacity {
		hintFixedWALWriteback(file, hintedEnd, capacity-hintedEnd)
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

// readFixedWALFull preserves the exact-read error identity of file.ReadAt.
func readFixedWALFull(input io.Reader, p []byte) (int, error) {
	n, err := io.ReadFull(input, p)
	if err == io.ErrUnexpectedEOF {
		// The former exact ReadAt calls reported EOF even after a partial read.
		err = io.EOF
	}
	return n, err
}

// HasFixedWALMagic only classifies the leading bytes of an open file.
// Callers must still validate the complete container before trusting its data.
func HasFixedWALMagic(file *os.File) (bool, error) {
	var prefix [8]byte
	n, err := file.ReadAt(prefix[:], 0)
	if err != nil && err != io.EOF {
		return false, err
	}
	return n == len(prefix) && prefix == fixedWALMagic, nil
}

// loadFixedWAL returns nil for the original stream format. For containers every
// reserve byte is examined: a zero header is never an EOF decision by itself.
func loadFixedWAL(file *os.File) (*fixedWAL, []byte, error) {
	fixed, err := HasFixedWALMagic(file)
	if err != nil || !fixed {
		return nil, nil, err
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
	// Keep all validation on this exact descriptor and captured capacity. A
	// scan owns its cursor and payload scratch; no bytes escape into the extent
	// index or incomplete-frame identities. Buffering removes per-witness reads
	// without treating initialized reserve as an implicit end marker.
	// readFixedWALFull preserves the exact-read error identity of file.ReadAt.
	section := io.NewSectionReader(file, fixedWALHeaderSize, capacity-fixedWALHeaderSize)
	input := acquireFixedWALReadBuf(section)
	defer releaseFixedWALReadBuf(input)
	var (
		payload      []byte
		payloadLease *fixedWALExtentBufLease
	)
	defer func() {
		releaseFixedWALExtentBuf(payloadLease)
	}()
	// io.Reader and the checksum implementation may retain these addresses in
	// compiler escape analysis. Own them once per scan, not once per extent.
	var probe, footer, expected [fixedWALWitnessSize]byte
	var padding [fixedWALAlignment - 1]byte
	for pos := int64(fixedWALHeaderSize); pos < capacity; {
		// Read a bounded reserve window, rather than allocating physical capacity.
		clear(probe[:])
		count := min(int64(len(probe)), capacity-pos)
		if _, err = readFixedWALFull(input, probe[:count]); err != nil {
			return nil, nil, err
		}
		if allZeroBytes(probe[:count]) {
			// Full remainder scan detects a lost header, stale suffix, or unknown data.
			for off := pos + count; off < capacity; {
				size := int(min(int64(fixedWALReadBufferSize), capacity-off))
				block, readErr := input.Peek(size)
				if readErr != nil {
					return nil, nil, readErr
				}
				// Count uses the standard library's byte scan rather than one
				// instrumented Go read per byte across the initialized reserve.
				// Every byte remains covered by the corruption check.
				if bytes.Count(block, []byte{0}) != size {
					return nil, nil, fmt.Errorf("%w: fixed WAL occupied bytes after gap at %d", ErrCorrupt, pos)
				}
				if _, err = input.Discard(size); err != nil {
					return nil, nil, err
				}
				off += int64(size)
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
		if int64(cap(payload)) < length {
			releaseFixedWALExtentBuf(payloadLease)
			if int(length) >= fixedWALExtentBufPoolMinSize {
				payloadLease, payload = acquireFixedWALExtentBuf(int(length))
			} else {
				payloadLease = nil
				payload = make([]byte, int(length))
			}
		} else {
			payload = payload[:int(length)]
		}
		paddingBytes := int(footerOffset - pos - fixedWALWitnessSize - length)
		if length >= fixedWALReadBufferSize {
			if _, err = file.ReadAt(payload, pos+fixedWALWitnessSize); err != nil {
				return nil, nil, err
			}
			var tailBuf [fixedWALAlignment - 1 + fixedWALWitnessSize]byte
			tailLen := paddingBytes + fixedWALWitnessSize
			if _, err = file.ReadAt(tailBuf[:tailLen], pos+fixedWALWitnessSize+length); err != nil {
				return nil, nil, err
			}
			copy(padding[:paddingBytes], tailBuf[:paddingBytes])
			copy(footer[:], tailBuf[paddingBytes:tailLen])
			if _, err = section.Seek(end-fixedWALHeaderSize, io.SeekStart); err != nil {
				return nil, nil, err
			}
			input.Reset(section)
		} else {
			if _, err = readFixedWALFull(input, payload); err != nil {
				return nil, nil, err
			}
			if _, err = readFixedWALFull(input, padding[:paddingBytes]); err != nil {
				return nil, nil, err
			}
			if _, err = readFixedWALFull(input, footer[:]); err != nil {
				return nil, nil, err
			}
		}
		expected = probe
		copy(expected[:], fixedWALEndMagic[:])
		binary.LittleEndian.PutUint32(expected[92:96], crc.Checksum(expected[:92]))
		wantExtentCRC := binary.LittleEndian.Uint32(probe[72:76])
		first, last, extentCRC, frames, frameErr := validateFixedWALFramesWithSummary(payload, start, state.validatedFrames)
		if frameErr == nil && extentCRC == wantExtentCRC {
			state.validatedFrames = frames
			// Narrow C reconstructs redundant framing only after the entire
			// unchanged V2 frame, CRC, identity, and order have validated.
			if !bytes.Equal(footer[:], expected[:]) {
				if bytes.Equal(footer[:8], fixedWALEndMagic[:]) && binary.LittleEndian.Uint32(footer[92:96]) == crc.Checksum(footer[:92]) {
					return nil, nil, fmt.Errorf("%w: mismatched intact fixed WAL footer", ErrCorrupt)
				}
				if len(payload) < segmentHeaderSize+6 || binary.LittleEndian.Uint16(payload[segmentHeaderSize+4:segmentHeaderSize+6]) != CommandFrameVersionV2 {
					return nil, nil, ErrCorrupt
				}
				state.reconstructedFooters++
			}
		} else {
			dataComplete := crc.Checksum(payload) == wantExtentCRC
			if !dataComplete {
				// Only a positively bounded new extent whose end is still exactly
				// initialized reserve can represent an interrupted relaxed frame.
				// An intact or partly written end plus damaged data stays corrupt.
				if !allZeroBytes(footer[:]) {
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
			} else if frameErr != nil {
				return nil, nil, frameErr
			} else {
				return nil, nil, ErrCorrupt
			}
		}
		if first != binary.LittleEndian.Uint64(probe[56:64]) || last != binary.LittleEndian.Uint64(probe[64:72]) {
			return nil, nil, ErrCorrupt
		}
		if state.lastLSN != 0 && first <= state.lastLSN {
			return nil, nil, ErrCommandWALDuplicateLSN
		}
		if !allZeroBytes(padding[:paddingBytes]) {
			return nil, nil, ErrCorrupt
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
	first, last, _, _, err = validateFixedWALFramesWithSummary(data, -1, nil)
	return first, last, err
}

func validateFixedWALFramesWithSummary(data []byte, logicalBase int64, dst []fixedWALValidatedFrame) (first, last uint64, extentCRC uint32, frames []fixedWALValidatedFrame, err error) {
	frames = dst
	logicalOff := logicalBase
	haveCRC := false
	for len(data) > 0 {
		if len(data) < segmentHeaderSize {
			return 0, 0, 0, dst, ErrCommandWALTerminalTail
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if length <= 0 || length > len(data)-segmentHeaderSize {
			return 0, 0, 0, dst, ErrCommandWALTerminalTail
		}
		payload := data[segmentHeaderSize : segmentHeaderSize+length]
		payloadCRC := crc.Checksum(payload)
		if payloadCRC != binary.LittleEndian.Uint32(data[4:8]) {
			return 0, 0, 0, dst, ErrCorrupt
		}
		// Only the LSN escapes this validation pass. Keep the full decoder's
		// integrity checks while borrowing bytes owned by this call's input.
		env, e := decodeCommandFrame(payload, true)
		if e != nil {
			return 0, 0, 0, dst, e
		}
		if first == 0 {
			first = env.LSN
		}
		if last != 0 && env.LSN <= last {
			return 0, 0, 0, dst, ErrCommandWALDuplicateLSN
		}
		last = env.LSN
		frameLen := segmentHeaderSize + length
		frameCRC := crc32IEEECombine(crc.Checksum(data[:segmentHeaderSize]), payloadCRC, length)
		if !haveCRC {
			extentCRC = frameCRC
			haveCRC = true
		} else {
			extentCRC = crc32IEEECombine(extentCRC, frameCRC, frameLen)
		}
		if dst != nil || logicalBase >= 0 {
			frames = append(frames, fixedWALValidatedFrame{
				logicalStart: logicalOff,
				logicalEnd:   logicalOff + int64(frameLen),
				payloadLen:   int64(length),
				lsn:          env.LSN,
			})
		}
		logicalOff += int64(frameLen)
		data = data[frameLen:]
	}
	return first, last, extentCRC, frames, nil
}

func validateTrustedFixedWALFrames(data []byte) error {
	var last uint64
	for len(data) > 0 {
		if len(data) < segmentHeaderSize {
			return ErrCommandWALTerminalTail
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if length < commandFrameHeaderSize || length > len(data)-segmentHeaderSize {
			return ErrCommandWALTerminalTail
		}
		payload := data[segmentHeaderSize : segmentHeaderSize+length]
		if crc.Checksum(payload) != binary.LittleEndian.Uint32(data[4:8]) {
			return ErrCorrupt
		}
		lsn := binary.LittleEndian.Uint64(payload[20:28])
		if lsn == 0 {
			return ErrCorrupt
		}
		if last != 0 && lsn <= last {
			return ErrCommandWALDuplicateLSN
		}
		last = lsn
		data = data[segmentHeaderSize+length:]
	}
	return nil
}

// flushExtent emits exactly one frame from flush's fully validated, exclusively
// owned pending buffer. Its caller supplies the validated frame's common V1/V2
// LSN; no borrowed decoder state or mutable scratch escapes the synchronous write.
func (state *fixedWAL) flushExtent(w *Writer, frame []byte, lsn uint64) error {
	end, geometryErr := fixedWALExtentEnd(state.physical, int64(len(frame)))
	if geometryErr != nil {
		return geometryErr
	}
	if end > state.capacity {
		return fmt.Errorf("commitlog: initialized WAL capacity exhausted")
	}
	var frameCRC uint32
	if len(frame) >= segmentHeaderSize {
		// flush() already validated frame[4:8] == crc.Checksum(frame[segmentHeaderSize:]),
		// so combine the 8-byte header CRC with the validated payload CRC in O(1).
		frameCRC = crc32IEEECombine(crc.Checksum(frame[:segmentHeaderSize]), binary.LittleEndian.Uint32(frame[4:8]), len(frame)-segmentHeaderSize)
	} else {
		frameCRC = crc.Checksum(frame)
	}
	imageSize := int(end - state.physical)
	if fixedWALSupportsPwritev && len(frame) >= 4096 {
		var header [fixedWALWitnessSize]byte
		copy(header[:], fixedWALExtentMagic[:])
		copy(header[8:24], state.identity[:])
		binary.LittleEndian.PutUint64(header[24:32], uint64(state.physical))
		binary.LittleEndian.PutUint64(header[32:40], uint64(state.logical))
		binary.LittleEndian.PutUint64(header[40:48], uint64(len(frame)))
		binary.LittleEndian.PutUint64(header[48:56], uint64(end))
		binary.LittleEndian.PutUint64(header[56:64], lsn)
		binary.LittleEndian.PutUint64(header[64:72], lsn)
		binary.LittleEndian.PutUint32(header[72:76], frameCRC)
		binary.LittleEndian.PutUint32(header[92:96], crc.Checksum(header[:92]))
		padLen := imageSize - 2*fixedWALWitnessSize - len(frame)
		var tail [fixedWALAlignment - 1 + fixedWALWitnessSize]byte
		footer := tail[padLen : padLen+fixedWALWitnessSize]
		copy(footer, header[:])
		copy(footer, fixedWALEndMagic[:])
		binary.LittleEndian.PutUint32(footer[92:96], crc.Checksum(footer[:92]))
		started := time.Now()
		syscalls, err := writeFixedAtParts(w.f, state.physical, header[:], frame, tail[:padLen+fixedWALWitnessSize])
		if syscalls == 0 {
			syscalls = 1
		}
		w.observeWriteSyscalls(syscalls, uint64(imageSize), time.Since(started), err)
		if err != nil {
			return err
		}
		state.logical += int64(len(frame))
		state.physical = end
		state.lastLSN = lsn
		return nil
	}
	// Flush has already copied the writer's buffered frames into pending.
	// Reuse its encode storage until synchronous WriteAt completes; captured
	// syncs retain the file and physical prefix, never this mutable scratch.
	var (
		image []byte
		lease *fixedWALExtentBufLease
	)
	if (w.commandBufRetain > 0 && imageSize > w.commandBufRetain) || (cap(w.scratch) < imageSize && imageSize >= fixedWALExtentBufPoolMinSize) {
		lease, image = acquireFixedWALExtentBuf(imageSize)
		if lease != nil {
			defer releaseFixedWALExtentBuf(lease)
		}
	} else {
		if cap(w.scratch) < imageSize {
			w.scratch = make([]byte, imageSize)
		}
		image = w.scratch[:imageSize]
	}
	header := image[:fixedWALWitnessSize]
	clear(header) // Reserved witness bytes stay canonical.
	copy(header, fixedWALExtentMagic[:])
	copy(header[8:24], state.identity[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(state.physical))
	binary.LittleEndian.PutUint64(header[32:40], uint64(state.logical))
	binary.LittleEndian.PutUint64(header[40:48], uint64(len(frame)))
	binary.LittleEndian.PutUint64(header[48:56], uint64(end))
	binary.LittleEndian.PutUint64(header[56:64], lsn)
	binary.LittleEndian.PutUint64(header[64:72], lsn)
	binary.LittleEndian.PutUint32(header[72:76], frameCRC)
	binary.LittleEndian.PutUint32(header[92:96], crc.Checksum(header[:92]))
	copy(image[fixedWALWitnessSize:], frame)
	padStart := fixedWALWitnessSize + len(frame)
	footerStart := len(image) - fixedWALWitnessSize
	if padStart < footerStart {
		clear(image[padStart:footerStart]) // Alignment padding stays canonical zero.
	}
	footer := image[footerStart:]
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
func (w *Writer) prepareFixedSuccessor(file **os.File, parent *os.File, plan fixedWALSizePlan) (*fixedWAL, int64, walContainerSelection, error) {
	return w.prepareFixedSuccessorAhead(file, parent, plan, nil)
}

func (w *Writer) prepareFixedSuccessorAhead(file **os.File, parent *os.File, plan fixedWALSizePlan, ahead *fixedWALAheadReserve) (*fixedWAL, int64, walContainerSelection, error) {
	return w.prepareFixedSuccessorAheadWithParentSync(file, parent, plan, ahead, true)
}

func (w *Writer) prepareFixedSuccessorAheadWithParentSync(file **os.File, parent *os.File, plan fixedWALSizePlan, ahead *fixedWALAheadReserve, syncParent bool) (*fixedWAL, int64, walContainerSelection, error) {
	state, _, err := loadFixedWAL(*file)
	if err != nil {
		return nil, 0, walContainerSelection{}, err
	}
	selection := existingWALContainer(*file, state, w.preferExt4Stream)
	info, err := (*file).Stat()
	if err != nil {
		return nil, 0, selection, err
	}
	if state == nil && info.Size() == 0 {
		selection = selectWALContainer(*file, plan.capacity, w.productionMediumPolicy, w.preferExt4Stream)
	}
	needsCapacity := state != nil && state.logical == 0 && !state.fitsEmptyFrame(plan.incoming)
	if (state == nil && selection.fixed) || needsCapacity {
		if ahead != nil && state == nil && info.Size() == 0 && plan.capacity == ahead.state.capacity {
			state, err = publishFixedWALReplacementWithParentSync(file, parent, ahead.file, ahead.name, ahead.state, syncParent)
		} else {
			state, err = initializeFixedWALWithParentSync(file, plan.capacity, parent, syncParent)
		}
		if err != nil {
			return nil, 0, selection, err
		}
	}
	if state != nil {
		end, sizeErr := fixedWALExtentEnd(state.physical, plan.incoming)
		if plan.incoming > 0 && (sizeErr != nil || end > state.capacity) {
			return nil, 0, selection, ErrRecordTooLarge
		}
		return state, state.logical, selection, nil
	}
	if _, err = (*file).Seek(info.Size(), io.SeekStart); err != nil {
		return nil, 0, selection, err
	}
	return nil, info.Size(), selection, nil
}

func fixedWALCapacityFitsEmptyFrame(capacity, frameBytes int64) bool {
	required, err := fixedWALExtentEnd(fixedWALHeaderSize, frameBytes)
	return err == nil && required <= capacity
}

func (state *fixedWAL) fitsEmptyFrame(frameBytes int64) bool {
	return fixedWALCapacityFitsEmptyFrame(state.capacity, frameBytes)
}

func (state *fixedWAL) recordPendingFrame(frameBytes int64) {
	if state == nil || frameBytes <= 0 {
		return
	}
	if int64(len(state.pending)) != state.pendingLogical+frameBytes {
		state.pendingLogical = 0
		state.pendingPhysical = 0
		return
	}
	end, err := fixedWALExtentEnd(state.physical+state.pendingPhysical, frameBytes)
	if err != nil {
		state.pendingLogical = 0
		state.pendingPhysical = 0
		return
	}
	state.pendingLogical += frameBytes
	state.pendingPhysical = end - state.physical
}

// fixedFitsAppend includes accepted but unflushed logical bytes. Direct V2
// frames buffered in w.fixed.pending carry exact extent accounting so multi-frame
// group-commit waves fill the container rather than rotating early at 25%.
// Any remaining buffered bytes retain the conservative 4x upper bound without
// walking/copying buffered frames.
func (w *Writer) fixedFitsAppend(frameBytes int64) bool {
	if w.fixed == nil {
		return true
	}
	buffered := w.ActiveBytes() - w.fixed.logical
	if buffered < 0 {
		return false
	}
	pendingLogical := w.fixed.pendingLogical
	pendingPhysical := w.fixed.pendingPhysical
	if pendingLogical != int64(len(w.fixed.pending)) || pendingLogical > buffered || pendingPhysical < pendingLogical {
		pendingLogical = 0
		pendingPhysical = 0
	}
	basePhysical := w.fixed.physical + pendingPhysical
	if basePhysical > w.fixed.capacity {
		return false
	}
	end, err := fixedWALExtentEnd(basePhysical, frameBytes)
	if err != nil || end > w.fixed.capacity {
		return false
	}
	unaccounted := buffered - pendingLogical
	return unaccounted <= (w.fixed.capacity-end)/4
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
	state.validatedFrames = nil
	if len(state.pending) == 0 {
		state.pendingTrustedEnd = 0
		state.pendingTrustedLastLSN = 0
		return nil
	}
	// Validate every frame before emitting any extent: malformed later bytes
	// must not leave a physically written prefix. Frames emitted exclusively via
	// direct V2 prepared encoders already passed envelope/payload validation, so
	// verify outer checksums, length bounds, and monotone LSNs without repeating
	// full payload decode/comparison walks.
	if state.pendingTrustedEnd == len(state.pending) {
		if err := validateTrustedFixedWALFrames(state.pending); err != nil {
			return err
		}
	} else if _, _, err := validateFixedWALFrames(state.pending); err != nil {
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
			state.pendingLogical = 0
			state.pendingPhysical = 0
			state.pendingTrustedEnd = 0
			state.pendingTrustedLastLSN = 0
			return err
		}
		pending = pending[size:]
	}
	state.pending = base[:0]
	state.pendingLogical = 0
	state.pendingPhysical = 0
	state.pendingTrustedEnd = 0
	state.pendingTrustedLastLSN = 0
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
	file           *os.File
	state          *fixedWAL
	offset         int64
	index          int
	input          *io.SectionReader
	buffer         *bufio.Reader
	physicalOffset int64
}

func (stream *fixedWALStream) Read(p []byte) (int, error) {
	if stream.offset >= stream.state.logical {
		return 0, io.EOF
	}
	n := 0
	for len(p) > 0 && stream.offset < stream.state.logical {
		index := stream.index
		if index >= len(stream.state.extents) || stream.offset < stream.state.extents[index].logical || stream.offset >= stream.state.extents[index].logical+stream.state.extents[index].length {
			// Sequential reads advance in constant time. Random seeks retain
			// the existing validated extent index and logarithmic lookup.
			if index+1 < len(stream.state.extents) && stream.offset == stream.state.extents[index+1].logical {
				index++
			} else {
				index = sort.Search(len(stream.state.extents), func(i int) bool { e := stream.state.extents[i]; return e.logical+e.length > stream.offset })
			}
			stream.index = index
		}
		extent := stream.state.extents[index]
		inside := stream.offset - extent.logical
		size := min(int64(len(p)), extent.length-inside)
		physical := extent.physical + fixedWALWitnessSize + inside
		if stream.buffer == nil {
			stream.input = io.NewSectionReader(stream.file, 0, stream.state.physical)
			stream.buffer = bufio.NewReaderSize(stream.input, fixedWALReadBufferSize)
		}
		if physical != stream.physicalOffset {
			skip := physical - stream.physicalOffset
			if skip >= 0 && skip <= int64(stream.buffer.Buffered()) {
				_, _ = stream.buffer.Discard(int(skip))
			} else {
				if _, err := stream.input.Seek(physical, io.SeekStart); err != nil {
					return n, err
				}
				stream.buffer.Reset(stream.input)
			}
			stream.physicalOffset = physical
		}
		count, err := readFixedWALFull(stream.buffer, p[:size])
		stream.physicalOffset += int64(count)
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
