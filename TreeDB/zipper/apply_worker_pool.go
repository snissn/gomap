package zipper

import (
	"errors"
	"sync"
	"sync/atomic"
)

var errApplyWorkerPoolClosed = errors.New("zipper: apply worker pool closed")

type applyWorkerPoolTask struct {
	run      *applyWorkerPoolRun
	workerID int
}

type applyWorkerPoolRun struct {
	count   int
	workers int
	fn      func(workerID, job int)
	seeded  bool
	next    atomic.Int64
	wg      sync.WaitGroup
}

// ApplyWorkerPool is a small reusable worker pool for opt-in flush/apply COW
// work. It runs caller-supplied jobs to completion before Run returns; it does
// not own durable output, publish roots, or cancel in-flight work on first error.
// Callers are responsible for collecting worker-local errors/stats and for
// discarding unpublished output on guarded-publish failure.
type ApplyWorkerPool struct {
	tasks chan applyWorkerPoolTask

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// NewApplyWorkerPool starts workers reusable across apply attempts. Values <=0
// create a nil-equivalent pool with one worker slot; callers normally avoid
// constructing a pool unless configured concurrency is >1.
func NewApplyWorkerPool(workers int) *ApplyWorkerPool {
	if workers < 1 {
		workers = 1
	}
	p := &ApplyWorkerPool{tasks: make(chan applyWorkerPoolTask, workers*2)}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *ApplyWorkerPool) worker() {
	defer p.wg.Done()
	for task := range p.tasks {
		run := task.run
		if run == nil || run.fn == nil || run.count <= 0 {
			if run != nil {
				run.wg.Done()
			}
			continue
		}
		if run.seeded && task.workerID < run.count {
			run.fn(task.workerID, task.workerID)
		}
		for {
			job := int(run.next.Add(1)) - 1
			if job >= run.count {
				break
			}
			run.fn(task.workerID, job)
		}
		run.wg.Done()
	}
}

// Run schedules up to workers reusable workers over count jobs. It blocks until
// all scheduled workers finish. A closed pool returns errApplyWorkerPoolClosed.
func (p *ApplyWorkerPool) Run(workers, count int, fn func(workerID, job int)) error {
	return p.run(workers, count, fn, false)
}

// RunSeeded schedules one initial job per workerID before dynamic work stealing.
// This preserves worker-owned resources such as selected leaf-log lanes while
// retaining load balancing for the remaining jobs.
func (p *ApplyWorkerPool) RunSeeded(workers, count int, fn func(workerID, job int)) error {
	return p.run(workers, count, fn, true)
}

func (p *ApplyWorkerPool) run(workers, count int, fn func(workerID, job int), seeded bool) error {
	if count <= 0 || fn == nil {
		return nil
	}
	if workers <= 1 || p == nil {
		for job := 0; job < count; job++ {
			fn(0, job)
		}
		return nil
	}
	if workers > count {
		workers = count
	}
	if workers < 1 {
		workers = 1
	}

	run := &applyWorkerPoolRun{count: count, workers: workers, fn: fn, seeded: seeded}
	if seeded {
		run.next.Store(int64(workers))
	}
	run.wg.Add(workers)

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errApplyWorkerPoolClosed
	}
	for workerID := 0; workerID < workers; workerID++ {
		p.tasks <- applyWorkerPoolTask{run: run, workerID: workerID}
	}
	p.mu.Unlock()

	run.wg.Wait()
	return nil
}

// Close stops idle workers after all tasks already submitted through Run have
// completed. Concurrent Run after Close returns errApplyWorkerPoolClosed.
func (p *ApplyWorkerPool) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.tasks)
	p.mu.Unlock()
	p.wg.Wait()
}
