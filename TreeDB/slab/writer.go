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

	activeBuf []byte
	offset    int64 // Virtual offset of the next write (includes activeBuf + pending)

	// Channels for buffer exchange
	pendingCh chan []byte // Buffers full, waiting to be written
	freeCh    chan []byte // Buffers empty, ready to be reused

	// Error handling
	err    error
	closed bool
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
		doneCh:     make(chan struct{}),
		bufferSize: bufferSize,
	}
	w.durableCond = sync.NewCond(&w.durableMu)
	w.durableSize.Store(s.Size())

	// Create the second buffer and put it in free pool
	w.freeCh <- make([]byte, 0, bufferSize)

	go w.flushLoop()
	return w
}

func (w *SlabWriter) Write(data []byte) (int64, error) {
	w.mu.Lock()
	if w.err != nil {
		w.mu.Unlock()
		return 0, w.err
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
		case <-w.doneCh:
			w.mu.Lock()
			defer w.mu.Unlock()
			return 0, w.err
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
// Releases lock while waiting on channels to prevent deadlock.
// Re-acquires lock before returning.
func (w *SlabWriter) rotateBufferLocked() error {
	fullBuf := w.activeBuf
	w.activeBuf = nil
	w.mu.Unlock()

	// Send full buffer
	if len(fullBuf) > 0 {
		select {
		case w.pendingCh <- fullBuf:
		case <-w.doneCh:
			w.mu.Lock()
			return w.reportClosedLocked()
		}
	} else {
		// If empty (shouldn't happen in normal flow, but safety check), reuse it?
		// No, we already set nil. Just put it back in freeCh if we grabbed it?
		// Actually if len==0 we shouldn't be here, but let's handle.
		// Just treat it as "skipped".
		// But we need a buffer for w.activeBuf.
		// We have to get one from freeCh.
		// If we didn't send to pendingCh, freeCh might be empty if the other buffer is pending.
		// Actually, if we didn't send fullBuf, we should just keep using it.
		// But we set it nil.
		// Simpler: Just send it. flushLoop handles empty buffers.
		select {
		case w.pendingCh <- fullBuf:
		case <-w.doneCh:
			w.mu.Lock()
			return w.reportClosedLocked()
		}
	}

	// Get free buffer
	var nextBuf []byte
	select {
	case nextBuf = <-w.freeCh:
	case <-w.doneCh:
		w.mu.Lock()
		return w.reportClosedLocked()
	}

	w.mu.Lock()
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return fmt.Errorf("writer closed")
	}
	w.activeBuf = nextBuf[:0]
	return nil
}

func (w *SlabWriter) reportClosedLocked() error {
	if w.err != nil {
		return w.err
	}
	return fmt.Errorf("writer stopped unexpectedly")
}

func (w *SlabWriter) flushLoop() {
	defer close(w.doneCh)

	for buf := range w.pendingCh {
		// Test hook: pause flush loop
		for w.testFlushPaused.Load() {
			time.Sleep(10 * time.Millisecond)
		}

		if len(buf) > 0 {
			_, err := w.s.WriteBatch(buf, false)
			if err != nil {
				w.mu.Lock()
				w.err = err
				w.mu.Unlock()
				w.signalDurable()
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
			// Should not happen if logic is correct (1-in 1-out)
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
	if w.err != nil {
		err := w.err
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
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	// Flush remaining data
	if len(w.activeBuf) > 0 {
		if err := w.rotateBufferLocked(); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	w.closed = true
	w.mu.Unlock()
	w.signalDurable()

	close(w.pendingCh)
	<-w.doneCh

	return w.err
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
	if w.err != nil {
		err := w.err
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
		err := w.err
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
		err := w.err
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
