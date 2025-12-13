package caching

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type blockingBackend struct {
	next atomic.Int32

	writeSyncCh chan int
	releaseCh   []chan struct{}

	mu   sync.Mutex
	data map[string][]byte
}

func newBlockingBackend(maxBatches int) *blockingBackend {
	b := &blockingBackend{
		writeSyncCh: make(chan int, maxBatches),
		releaseCh:   make([]chan struct{}, maxBatches),
		data:        make(map[string][]byte),
	}
	for i := range b.releaseCh {
		b.releaseCh[i] = make(chan struct{})
	}
	return b
}

func (b *blockingBackend) Get(key []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	val, ok := b.data[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), val...), nil
}

func (b *blockingBackend) Iterator(_, _ []byte) (iterator.UnsafeIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *blockingBackend) ReverseIterator(_, _ []byte) (iterator.UnsafeIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *blockingBackend) NewBatch() BatchInterface {
	id := int(b.next.Add(1) - 1)
	if id >= len(b.releaseCh) {
		panic("test backend: unexpected batch allocation")
	}
	return &blockingBatch{backend: b, id: id}
}

func (b *blockingBackend) Close() error             { return nil }
func (b *blockingBackend) Print() error             { return nil }
func (b *blockingBackend) Stats() map[string]string { return nil }

func (b *blockingBackend) release(id int) {
	close(b.releaseCh[id])
}

type blockingBatch struct {
	backend *blockingBackend
	id      int
}

func (bb *blockingBatch) Set(key, value []byte) error {
	bb.backend.mu.Lock()
	bb.backend.data[string(key)] = append([]byte(nil), value...)
	bb.backend.mu.Unlock()
	return nil
}

func (bb *blockingBatch) Delete(key []byte) error {
	bb.backend.mu.Lock()
	delete(bb.backend.data, string(key))
	bb.backend.mu.Unlock()
	return nil
}

func (bb *blockingBatch) Write() error              { return nil }
func (bb *blockingBatch) Close() error              { return nil }
func (bb *blockingBatch) GetByteSize() (int, error) { return 0, nil }

func (bb *blockingBatch) WriteSync() error {
	bb.backend.writeSyncCh <- bb.id
	<-bb.backend.releaseCh[bb.id]
	return nil
}

func TestFlushOne_ConcurrentDoesNotPanic(t *testing.T) {
	backend := newBlockingBackend(2)

	mem := memtable.New()
	mem.Set([]byte("k"), []byte("v"))

	db := &DB{
		backend: backend,
		mutable: memtable.New(),
		queue:   []*memtable.Memtable{mem},
	}

	var wg sync.WaitGroup
	panicCh := make(chan any, 2)
	wg.Add(2)
	flush := func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()
		db.flushOne()
	}

	go flush()

	var firstID int
	select {
	case firstID = <-backend.writeSyncCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for first flush to reach WriteSync")
	}

	go flush()

	var secondID int
	haveSecond := false
	select {
	case secondID = <-backend.writeSyncCh:
		haveSecond = true
	case <-time.After(2 * time.Second):
		// If flushes are serialized, the second goroutine may never reach WriteSync.
	}

	backend.release(firstID)

	deadline := time.Now().Add(2 * time.Second)
	for {
		db.mu.Lock()
		qlen := len(db.queue)
		db.mu.Unlock()
		if qlen == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for queue to drain (len=%d)", qlen)
		}
		time.Sleep(1 * time.Millisecond)
	}

	if haveSecond {
		backend.release(secondID)
	}

	wg.Wait()

	select {
	case p := <-panicCh:
		t.Fatalf("flushOne panicked: %v", p)
	default:
	}

	got, err := backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("backend.Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("backend value mismatch: got %q want %q", got, "v")
	}
}
