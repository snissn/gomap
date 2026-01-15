package slab

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type SlabWriter struct {
	mu sync.Mutex
	s  *SlabFile
	cond *sync.Cond

	activeBuf []byte
	offset    int64 // Virtual offset of the next write (includes activeBuf + pending)
	rotating  bool

	// Channels for buffer exchange
	pendingCh chan []byte // Buffers full, waiting to be written
	freeCh    chan []byte // Buffers empty, ready to be reused

	// Error handling
	errOnce sync.Once
	errVal  atomic.Value
	closed bool
	stopCh chan struct{}
	stopOnce sync.Once
	doneCh chan struct{} // Closed when flushLoop exits

	// tracking durable offset
	durableSize atomic.Int64
	durableMu   sync.Mutex
	durableCond *sync.Cond

	bufferSize int

	// Test hooks
	testFlushPaused atomic.Bool
}

func NewSlabWriter(s *SlabFile, bufferSize int) *SlabWriter {
	if bufferSize <= 0 {
		bufferSize = 4 << 20 // 4MB default
	}
	w := &SlabWriter{
		s:          s,
		activeBuf:  make([]byte, 0, bufferSize),
		offset:     s.Size(),
		pendingCh:  make(chan []byte, 1),
		freeCh:     make(chan []byte, 1),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		bufferSize: bufferSize,
	}
	w.cond = sync.NewCond(&w.mu)
	w.durableCond = sync.NewCond(&w.durableMu)
	w.durableSize.Store(s.Size())

	// Create the second buffer and put it in free pool
	w.freeCh = make(chan []byte, 2)
	w.freeCh <- make([]byte, 0, bufferSize)

	go w.flushLoop()
	return w
}

func (w *SlabWriter) Write(data []byte) (int64, error) {
	w.mu.Lock()
	for w.rotating {
		w.cond.Wait()
	}
	if err := w.terminalErr(); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	if w.closed {
		w.mu.Unlock()
		return 0, fmt.Errorf("writer closed")
	}

	// 1. Flush if buffer full
	if len(w.activeBuf)+len(data) > cap(w.activeBuf) {
		if err := w.rotateBufferLocked(); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	}

	// 2. Handle Oversized Data (too big for empty buffer)
	if len(data) > cap(w.activeBuf) {
		// activeBuf is empty now (rotated).
		// Send large buffer directly to pendingCh.
		// Note: We copy data to avoid ownership issues.
		largeBuf := make([]byte, len(data))
		copy(largeBuf, data)

		retOffset := w.offset
		w.offset += int64(len(data))

		w.mu.Unlock()
		select {
		case w.pendingCh <- largeBuf:
		case <-w.stopCh:
			w.mu.Lock()
			w.offset -= int64(len(data))
			w.mu.Unlock()
			if err := w.terminalErr(); err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("writer closed")
		case <-w.doneCh:
			w.mu.Lock()
			w.offset -= int64(len(data))
			defer w.mu.Unlock()
			if err := w.terminalErr(); err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("writer closed")
		}
		return retOffset, nil
	}

	// 3. Normal Append
	retOffset := w.offset
	w.activeBuf = append(w.activeBuf, data...)
	w.offset += int64(len(data))
	w.mu.Unlock()

	return retOffset, nil
}

func (w *SlabWriter) WriteBatch(buf []byte, ignoreBoundary bool) (int64, error) {
	if ignoreBoundary {
		// Force flush to ensure sequentiality before raw write
		if err := w.Sync(); err != nil {
			return 0, err
		}
		// Direct write to slab
		off, err := w.s.WriteBatch(buf, true)
		if err == nil {
			w.mu.Lock()
			w.offset = w.s.Size() // Re-sync offset
			w.mu.Unlock()

			// Re-sync durable size since we bypassed flushLoop
			w.durableSize.Store(w.s.Size())
			w.signalDurable()
		}
		return off, err
	}
	return w.Write(buf)
}

// rotateBufferLocked swaps the active buffer out to pending and gets a fresh one.
//
// Important: This function must not release w.mu while w.activeBuf is in a
// transient state (e.g. nil). Releasing the lock during rotation allows other
// goroutines to observe w.activeBuf=nil and attempt their own rotate/flush,
// which can break internal invariants and lead to Flush/WaitForDurable hangs.
func (w *SlabWriter) rotateBufferLocked() error {
	for w.rotating {
		w.cond.Wait()
	}
	if err := w.terminalErr(); err != nil {
		return err
	}
	if w.closed {
		return fmt.Errorf("writer closed")
	}

	// INV-1: activeBuf is never nil while w.mu is unlocked.
	fullBuf := w.activeBuf
	var nextBuf []byte
	select {
	case nextBuf = <-w.freeCh:
	default:
		nextBuf = make([]byte, 0, w.bufferSize)
	}
	w.activeBuf = nextBuf[:0]

	if len(fullBuf) == 0 {
		select {
		case w.freeCh <- fullBuf[:0]:
		default:
		}
		return nil
	}

	w.rotating = true
	w.mu.Unlock()
	stopped := false
	select {
	case w.pendingCh <- fullBuf:
	case <-w.stopCh:
		stopped = true
	case <-w.doneCh:
		stopped = true
	}
	w.mu.Lock()
	w.rotating = false
	w.cond.Broadcast()
	if stopped {
		return w.reportClosedLocked()
	}
	if err := w.terminalErr(); err != nil {
		return err
	}
	if w.closed {
		return fmt.Errorf("writer closed")
	}
	return nil
}

func (w *SlabWriter) reportClosedLocked() error {
	if err := w.terminalErr(); err != nil {
		return err
	}
	if w.closed {
		return fmt.Errorf("writer closed")
	}
	return fmt.Errorf("writer stopped unexpectedly")
}

func (w *SlabWriter) flushLoop() {
	defer close(w.doneCh)

	for {
		for w.testFlushPaused.Load() {
			select {
			case <-w.stopCh:
				return
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
		select {
		case buf := <-w.pendingCh:
			if len(buf) > 0 {
				_, err := w.s.WriteBatch(buf, false)
				if err != nil {
					w.setTerminalErr(err)
					return
				}
				w.durableSize.Store(w.s.Size())
				w.signalDurable()
			}

			// Recycle logic
			recycleBuf := buf
			if cap(buf) > w.bufferSize {
				// Drop oversized buffer, allocate standard one
				recycleBuf = make([]byte, 0, w.bufferSize)
			} else {
				recycleBuf = buf[:0]
			}

			select {
			case w.freeCh <- recycleBuf:
			default:
				// Buffer pool full, drop this buffer to avoid deadlock.
				// With freeCh cap=2, this only happens during heavy oversized write pressure.
			}
		case <-w.stopCh:
			for {
				select {
				case buf := <-w.pendingCh:
					if len(buf) > 0 {
						_, err := w.s.WriteBatch(buf, false)
						if err != nil {
							w.setTerminalErr(err)
							return
						}
						w.durableSize.Store(w.s.Size())
						w.signalDurable()
					}
					recycleBuf := buf
					if cap(buf) > w.bufferSize {
						recycleBuf = make([]byte, 0, w.bufferSize)
					} else {
						recycleBuf = buf[:0]
					}
					select {
					case w.freeCh <- recycleBuf:
					default:
					}
				default:
					return
				}
			}
		}
	}
}

func (w *SlabWriter) Sync() error {
	if err := w.flushBuffers(); err != nil {
		return err
	}
	return w.s.Sync()
}

// Flush forces buffered data to be written to the slab without fsync.
func (w *SlabWriter) Flush() error {
	return w.flushBuffers()
}

func (w *SlabWriter) flushBuffers() error {
	w.mu.Lock()
	if err := w.terminalErr(); err != nil {
		w.mu.Unlock()
		return err
	}
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("writer closed")
	}
	target := w.offset
	// Force rotation to flush activeBuf
	if len(w.activeBuf) > 0 {
		if err := w.rotateBufferLocked(); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	w.mu.Unlock()
	return w.waitForDurable(target)
}

func (w *SlabWriter) Close() error {
	// Mark closed to prevent new writes
	w.mu.Lock()
	if err := w.terminalErr(); err != nil {
		w.mu.Unlock()
		return err
	}
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.cond.Broadcast()
	w.mu.Unlock()

	w.stopOnce.Do(func() { close(w.stopCh) })

	<-w.doneCh
	return w.terminalErr()
}

func (w *SlabWriter) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

func (w *SlabWriter) ResetOffset(offset int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.offset = offset
	w.durableSize.Store(offset)
	w.signalDurable()
}

func (w *SlabWriter) WaitForOffset(offset int64) error {
	if offset <= 0 {
		return nil
	}
	if w.durableSize.Load() >= offset {
		return nil
	}
	w.mu.Lock()
	if err := w.terminalErr(); err != nil {
		w.mu.Unlock()
		return err
	}
	if w.durableSize.Load() < offset && len(w.activeBuf) > 0 {
		if err := w.rotateBufferLocked(); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	w.mu.Unlock()
	w.durableMu.Lock()
	defer w.durableMu.Unlock()
	for w.durableSize.Load() < offset {
		w.mu.Lock()
		err := w.terminalErr()
		closed := w.closed
		w.mu.Unlock()
		if err != nil {
			return err
		}
		if closed {
			select {
			case <-w.doneCh:
				if w.durableSize.Load() >= offset {
					return nil
				}
				return fmt.Errorf("writer closed before reaching offset")
			default:
			}
		}
		w.durableCond.Wait()
	}
	return nil
}

func (w *SlabWriter) waitForDurable(offset int64) error {
	if offset <= 0 {
		return nil
	}
	if w.durableSize.Load() >= offset {
		return nil
	}
	w.durableMu.Lock()
	defer w.durableMu.Unlock()
	for w.durableSize.Load() < offset {
		w.mu.Lock()
		err := w.terminalErr()
		closed := w.closed
		w.mu.Unlock()
		if err != nil {
			return err
		}
		if closed {
			select {
			case <-w.doneCh:
				if w.durableSize.Load() >= offset {
					return nil
				}
				return fmt.Errorf("writer closed before reaching offset")
			default:
			}
		}
		w.durableCond.Wait()
	}
	return nil
}

func (w *SlabWriter) signalDurable() {
	w.durableMu.Lock()
	w.durableCond.Broadcast()
	w.durableMu.Unlock()
}

func (w *SlabWriter) setTerminalErr(err error) {
	if err == nil {
		return
	}
	w.errOnce.Do(func() {
		w.errVal.Store(err)
		w.stopOnce.Do(func() { close(w.stopCh) })
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
		w.signalDurable()
	})
}

func (w *SlabWriter) terminalErr() error {
	if v := w.errVal.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// TestFillFreeCh non-blockingly fills freeCh if empty.
// Used for regression testing of dropped buffers.
func (w *SlabWriter) TestFillFreeCh() {
	select {
	case w.freeCh <- make([]byte, 0, w.bufferSize):
	default:
	}
}

// TestPauseFlushLoop pauses the background flush loop before the next write.
func (w *SlabWriter) TestPauseFlushLoop() {
	w.testFlushPaused.Store(true)
}

// TestResumeFlushLoop resumes the background flush loop.
func (w *SlabWriter) TestResumeFlushLoop() {
	w.testFlushPaused.Store(false)
}
