package treedb

import (
	"errors"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

// CommitFuture observes one accepted atomic publication's final ACK. Storage
// owns progress and cleanup even when no caller observes the future. The zero
// value is a completed success, used by synchronous compatibility paths.
type CommitFuture struct {
	done chan struct{}
	err  error // immutable after done closes
}

var completedCommitDone = func() <-chan struct{} { c := make(chan struct{}); close(c); return c }()

// Done closes only after durability, COW acknowledgement and owned-resource
// release. Abandoning Done or Wait never cancels an accepted commit.
func (f *CommitFuture) Done() <-chan struct{} {
	if f == nil || f.done == nil {
		return completedCommitDone
	}
	return f.done
}

// Wait observes the immutable completion result. A storage error can be commit
// ambiguous and must not be interpreted as permission to replay the commit.
func (f *CommitFuture) Wait() error {
	<-f.Done()
	if f == nil {
		return nil
	}
	return f.err
}

const commitCompletionCapacity = 64

type commitCompletionWork struct {
	batch  *commandWALPublicBatch
	future *CommitFuture
	floor  *mvccadmission.CompletionLease
	start  time.Time
}

// Only preparation/enqueue is serialized here. Physical durability uses the
// original public WAL registration/capture owner after submitMu releases.
type commitCompletionOwner struct {
	mu              sync.Mutex
	submitMu        sync.Mutex
	closeOnce       sync.Once
	started, closed bool
	slots           chan struct{}
	work            chan commitCompletionWork
	closing, done   chan struct{}
}

func (o *commitCompletionOwner) reserve(db *DB) error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrClosed
	}
	if !o.started {
		o.slots = make(chan struct{}, commitCompletionCapacity)
		o.work = make(chan commitCompletionWork, commitCompletionCapacity)
		o.closing = make(chan struct{})
		o.done = make(chan struct{})
		o.started = true
		go o.run(db)
	}
	o.mu.Unlock()
	// Capacity waits own neither database lifecycle nor append/publication gates.
	select {
	case o.slots <- struct{}{}:
	case <-o.closing:
		return ErrClosed
	}
	select {
	case <-o.closing:
		<-o.slots
		return ErrClosed
	default:
		return nil
	}
}

func (o *commitCompletionOwner) stopping() bool {
	select {
	case <-o.closing:
		return true
	default:
		return false
	}
}

func (o *commitCompletionOwner) run(db *DB) {
	defer close(o.done)
	for w := range o.work {
		err := w.batch.completeCOWWrite(nil)
		err = errors.Join(err, w.batch.Close())
		db.observePublicBatchWrite(true, w.start, err)
		err = w.floor.Result(err)
		w.floor.Release()
		db.lifecycleMu.RUnlock()
		<-o.slots
		w.future.err = err
		close(w.future.done)
	}
}

func (o *commitCompletionOwner) close() {
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closed = true
		if !o.started {
			o.mu.Unlock()
			return
		}
		close(o.closing)
		o.mu.Unlock()
		// A Submit that passed reserve either enqueues before this lock, or
		// sees closing before beginning lifecycle admission. No send races Close.
		o.submitMu.Lock()
		close(o.work)
		o.submitMu.Unlock()
		<-o.done
	})
}
