package valuelog

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"os"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// COWCodecRetention is implemented by the SAME enclosing admitted read owner.
// These methods admit actual containing allocations, never discover an owner,
// acquire a file, or create a separate accounting authority. Release follows
// removal of the final codec alias; physical owner retirement remains external.
type COWCodecRetention interface {
	RetainCodecBytes(uint64) bool
	ReleaseCodecBytes(uint64)
}

// COWZSTDContinuation phases the original codec and its actual native file
// input. The caller retains immutable owner/definition/input/output backings
// until Cancel returns true. A file stream needs only the admitted 4096-byte
// input slot; oversized values use BoundedSpans and original necessary history.
// No ordinary decoder, dictionary cache, or complete-value fallback exists.
type COWZSTDContinuation struct {
	Owner                                                 any
	Retention                                             COWCodecRetention
	ProducerID                                            uint64
	Definition                                            []byte
	DictionaryOwner                                       any
	decoder                                               *zstd.Decoder
	work                                                  *iterator.OrdinalScanWork
	hasher                                                hash.Hash
	hashAt                                                int
	digest                                                [sha256.Size]byte
	phase                                                 uint8
	input, raw, slot                                      []byte
	file                                                  *os.File
	fileOffset                                            int64
	fileLength, fileAt                                    uint64
	pending, fed                                          int
	crc, expectedCRC                                      uint32
	verifyCRC, streaming, spans, begun, closing, complete bool
	detached                                              bool
	err                                                   error
}

// Reserve and Available expose the SAME current quantum to the library.
func (c *COWZSTDContinuation) Reserve(r, b uint64) bool { return c.work != nil && c.work.Reserve(r, b) }
func (c *COWZSTDContinuation) Available() (uint64, uint64) {
	if c.work == nil || c.work.Records > c.work.RecordLimit || c.work.Bytes > c.work.ByteLimit {
		return 0, 0
	}
	return c.work.RecordLimit - c.work.Records, c.work.ByteLimit - c.work.Bytes
}
func (c *COWZSTDContinuation) Retain(n uint64) bool {
	return c.Retention != nil && c.Retention.RetainCodecBytes(n)
}
func (c *COWZSTDContinuation) Release(n uint64) { c.Retention.ReleaseCodecBytes(n) }

func (c *COWZSTDContinuation) Begin(input, raw []byte, w *iterator.OrdinalScanWork) (bool, error) {
	return c.begin(input, raw, nil, nil, 0, 0, 0, 0, false, false, w)
}

// BeginSpans uses the same original decoder with its necessary admitted
// physical history. The caller supplies a finite output span to each Step.
func (c *COWZSTDContinuation) BeginSpans(input []byte, w *iterator.OrdinalScanWork) (bool, error) {
	return c.begin(input, nil, nil, nil, 0, 0, 0, 0, false, true, w)
}

// BeginFile receives the exact producer payload range and a CRC seed for the
// already-validated original header/directory. Every ReadAt, checksum scan and
// FeedInput copy reserves before effects; an exhausted feed retains this SAME
// slot instead of reading again. CRC mismatch prevents successful completion.
func (c *COWZSTDContinuation) BeginFile(f *os.File, offset int64, length uint64, slot, raw []byte, spans bool, crcSeed, expectedCRC uint32, verifyCRC bool, w *iterator.OrdinalScanWork) (bool, error) {
	return c.begin(nil, raw, slot, f, offset, length, crcSeed, expectedCRC, verifyCRC, spans, w)
}
func (c *COWZSTDContinuation) begin(input, raw, slot []byte, f *os.File, offset int64, length uint64, seed, want uint32, verify, spans bool, w *iterator.OrdinalScanWork) (bool, error) {
	if c.begun && !c.complete || c.closing {
		return false, ErrCOWReadPending
	}
	if c.Owner == nil || c.Retention == nil || (c.ProducerID == 0) != (len(c.Definition) == 0) || c.ProducerID != 0 && c.DictionaryOwner == nil ||
		f != nil && (offset < 0 || length == 0 || len(slot) < 4096) ||
		cowBuffersOverlap(input, raw) || cowBuffersOverlap(raw, c.Definition) || cowBuffersOverlap(slot, raw) || cowBuffersOverlap(slot, c.Definition) {
		return false, ErrCOWReadCapacity
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))) {
		return false, nil
	}
	if c.decoder != nil {
		c.phase = 5
	}
	c.complete = false
	c.detached = false
	c.fileAt = 0
	c.pending = 0
	c.fed = 0
	c.input, c.raw, c.slot, c.file = input, raw, slot, f
	c.fileOffset, c.fileLength, c.crc, c.expectedCRC = offset, length, seed, want
	c.verifyCRC, c.streaming, c.spans, c.begun = verify, f != nil, spans, true
	return true, nil
}

// BindWork borrows the existing caller-owned, preadmitted current work object
// only for the duration of a concrete native call. DropWork follows that call.
// Splitting this binding keeps ordinary allocation-free block paths from making
// their stack work escape merely because a different codec needs callbacks.
func (c *COWZSTDContinuation) BindWork(w *iterator.OrdinalScanWork) { c.work = w }
func (c *COWZSTDContinuation) DropWork()                            { c.work = nil }
func (c *COWZSTDContinuation) Step(output []byte, w *iterator.OrdinalScanWork) (int, bool, error) {
	c.BindWork(w)
	defer c.DropWork()
	return c.stepBound(output, w)
}
func (c *COWZSTDContinuation) stepBound(output []byte, w *iterator.OrdinalScanWork) (int, bool, error) {
	if c.err != nil {
		return 0, false, c.err
	}
	if !c.begun || c.closing || c.work != w {
		return 0, false, ErrCOWReadPending
	}
	if c.complete {
		return 0, true, nil
	}
	n, done, err := c.step(output)
	if err != nil {
		c.err = err
	}
	return n, done, err
}
func (c *COWZSTDContinuation) step(output []byte) (int, bool, error) {
	if r, b := c.Available(); r == 0 || b == 0 {
		return 0, false, nil
	}
	for {
		switch c.phase {
		case 0:
			if len(c.Definition) == 0 {
				c.phase = 2
				continue
			}
			// sha256's original fixed digest allocation fits this conservative
			// containing charge (including allocator zeroing), before New.
			if !c.Reserve(1, 256) {
				return 0, false, nil
			}
			if !c.Retain(256) {
				return 0, false, zstd.ErrWorkRefused
			}
			c.hasher = sha256.New()
			c.phase = 1
		case 1:
			if c.hashAt < len(c.Definition) {
				_, avail := c.Available()
				if avail <= 128 {
					return 0, false, nil
				}
				n := int(min(uint64(len(c.Definition)-c.hashAt), min(avail-128, uint64(4096))))
				if n == 0 || !c.Reserve(1, uint64(n)+128) {
					return 0, false, nil
				}
				_, _ = c.hasher.Write(c.Definition[c.hashAt : c.hashAt+n])
				c.hashAt += n
				continue
			}
			if !c.Reserve(1, 256) {
				return 0, false, nil
			}
			c.hasher.Sum(c.digest[:0])
			if binary.BigEndian.Uint64(c.digest[:8]) != c.ProducerID || len(c.Definition) < 8 {
				return 0, false, ErrCorrupt
			}
			c.hasher = nil
			c.Release(256)
			c.phase = 2
		case 2:
			var ok bool
			var err error
			c.decoder, ok, err = zstd.NewBoundedReader(c)
			if err != nil || !ok {
				return 0, false, err
			}
			c.phase = 3
		case 3, 5:
			dict := zstd.BorrowedDictionary{}
			if c.ProducerID != 0 {
				dict = zstd.BorrowedDictionary{ID: binary.LittleEndian.Uint32(c.Definition[4:8]), Data: c.Definition, Owner: c.DictionaryOwner}
			}
			mode := zstd.BoundedRaw
			if c.spans {
				mode = zstd.BoundedSpans
			}
			var ok bool
			var err error
			if c.phase == 5 {
				if c.streaming {
					ok, err = c.decoder.ResetStreamStep(c.Owner, mode, c.raw, dict)
				} else {
					ok, err = c.decoder.ResetStep(c.input, c.Owner, mode, c.raw, dict)
				}
			} else if c.streaming {
				ok, err = c.decoder.BeginStream(c.Owner, mode, c.raw, dict)
			} else {
				ok, err = c.decoder.Begin(c.input, c.Owner, mode, c.raw, dict)
			}
			if err != nil || !ok {
				return 0, false, err
			}
			c.phase = 4
		case 4:
			if c.streaming {
				if c.pending > c.fed {
					n, _, err := c.decoder.FeedInput(c.slot[c.fed:c.pending], c.fileAt == c.fileLength)
					c.fed += n
					if err != nil || n == 0 {
						return 0, false, err
					}
					if c.fed != c.pending {
						continue
					}
					c.pending, c.fed = 0, 0
				}
				if need, yes := c.decoder.InputRequest(); yes {
					if c.fileAt == c.fileLength {
						_, ok, err := c.decoder.FeedInput(nil, true)
						if err != nil || !ok {
							return 0, false, err
						}
						continue
					}
					price := 2
					if c.verifyCRC {
						price++
					}
					n := cowContinuationChunk(c.work, min(need, int(min(c.fileLength-c.fileAt, uint64(4096)))), price)
					if n == 0 || !c.Reserve(1, uint64(n*price)) {
						return 0, false, nil
					}
					if _, err := c.file.ReadAt(c.slot[:n], c.fileOffset+int64(c.fileAt)); err != nil {
						return 0, false, err
					}
					if c.verifyCRC {
						c.crc = crc.Update(c.crc, c.slot[:n])
					}
					c.fileAt += uint64(n)
					c.pending = n
					continue
				}
			}
			n, done, err := c.decoder.Step(output)
			if err != nil {
				return n, false, err
			}
			if !done {
				return n, false, nil
			}
			c.phase = 6
			return n, false, nil
		case 6:
			if !c.Reserve(1, 16) {
				return 0, false, nil
			}
			if c.streaming && (c.fileAt != c.fileLength || c.pending != 0) {
				return 0, false, ErrCorrupt
			}
			if c.streaming && c.verifyCRC && c.crc != c.expectedCRC {
				return 0, false, ErrCorrupt
			}
			c.complete = true
			return 0, true, nil
		}
	}
}
func (c *COWZSTDContinuation) RawResult() ([]byte, error) {
	if !c.complete || c.decoder == nil {
		return nil, ErrCOWReadPending
	}
	return c.decoder.RawResult()
}

// detachBound retires old immutable input/output aliases before the native
// reader reuses either containing backing. Reset's new empty input is only a
// suspended placeholder; no parsing occurs before the next real Begin.
func (c *COWZSTDContinuation) detachBound(w *iterator.OrdinalScanWork) (bool, error) {
	if c.work != w {
		return false, ErrCOWCodecWorkUnbound
	}
	if c.decoder != nil && !c.detached {
		ok, err := c.decoder.ResetStep(nil, c.Owner, zstd.BoundedRaw, nil, zstd.BorrowedDictionary{})
		if err != nil || !ok {
			return false, err
		}
		c.detached = true
	}
	if !c.Reserve(1, 96) {
		return false, nil
	}
	c.input, c.raw, c.slot, c.file = nil, nil, nil, nil
	c.begun = false
	c.complete = false
	c.phase = 5
	return true, nil
}

// Cancel retains the original dictionary/file/output owner through the last
// library alias. It releases the caller-reachable decoder control only after
// dropping that control pointer. No physical resource refund is claimed here.
func (c *COWZSTDContinuation) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	c.BindWork(w)
	defer c.DropWork()
	return c.cancelBound(w)
}
func (c *COWZSTDContinuation) cancelBound(w *iterator.OrdinalScanWork) (bool, error) {
	if c.work != w {
		return false, ErrCOWReadPending
	}
	if !c.closing {
		if !c.Reserve(1, 1) {
			return false, nil
		}
		c.closing = true
	}
	if c.decoder != nil {
		ok, err := c.decoder.CloseStep()
		if err != nil || !ok {
			return false, err
		}
		if !c.Reserve(1, 16) {
			return false, nil
		}
		c.decoder = nil
		c.Release(zstd.BoundedControlSize())
	}
	if c.hasher != nil {
		if !c.Reserve(1, 256) {
			return false, nil
		}
		c.hasher = nil
		c.Release(256)
	}
	if !c.Reserve(1, uint64(unsafe.Sizeof(*c))) {
		return false, nil
	}
	*c = COWZSTDContinuation{}
	return true, nil
}
