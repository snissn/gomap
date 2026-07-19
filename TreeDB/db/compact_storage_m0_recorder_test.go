package db

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

type compactStorageM0StableCall struct {
	Phase, Resource, CallType string
	Count                     uint64
	Duration                  time.Duration
}

type compactStorageM0StableRecorder struct {
	mu      sync.Mutex
	phase   string
	started map[string]time.Time
	calls   map[string]*compactStorageM0StableCall
	total   atomic.Uint64
}

func newCompactStorageM0StableRecorder() *compactStorageM0StableRecorder {
	return &compactStorageM0StableRecorder{started: make(map[string]time.Time), calls: make(map[string]*compactStorageM0StableCall)}
}

func (r *compactStorageM0StableRecorder) beginPhase(phase string) {
	r.mu.Lock()
	r.phase = phase
	r.mu.Unlock()
}
func (r *compactStorageM0StableRecorder) endPhase(phase string) {
	r.mu.Lock()
	if r.phase == phase {
		r.phase = ""
	}
	r.mu.Unlock()
}

func (r *compactStorageM0StableRecorder) observe(event durabilitycut.Event) error {
	call, before, ok := compactStorageM0CallType(event.Point)
	if !ok {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	phase := r.phase
	if phase == "" {
		phase = "unavailable"
	}
	key := phase + "|" + string(event.Resource) + "|" + call
	if before {
		r.started[key] = time.Now()
		return nil
	}
	started, ok := r.started[key]
	if !ok {
		return nil
	}
	delete(r.started, key)
	entry := r.calls[key]
	if entry == nil {
		entry = &compactStorageM0StableCall{Phase: phase, Resource: string(event.Resource), CallType: call}
		r.calls[key] = entry
	}
	entry.Count++
	entry.Duration += time.Since(started)
	r.total.Add(1)
	return nil
}

func (r *compactStorageM0StableRecorder) totalCalls() uint64 { return r.total.Load() }

func compactStorageM0CallType(point durabilitycut.Point) (string, bool, bool) {
	switch point {
	case durabilitycut.BeforeUserspaceFlush:
		return "userspace-flush", true, true
	case durabilitycut.AfterUserspaceFlush:
		return "userspace-flush", false, true
	case durabilitycut.BeforeDependencyFileSync:
		return "file-stable", true, true
	case durabilitycut.AfterDependencyFileSync:
		return "file-stable", false, true
	case durabilitycut.BeforeNewFileDirectorySync:
		return "directory-stable", true, true
	case durabilitycut.AfterNewFileDirectorySync:
		return "directory-stable", false, true
	case durabilitycut.BeforeIndexDataSync:
		return "index-stable", true, true
	case durabilitycut.AfterIndexDataSync:
		return "index-stable", false, true
	case durabilitycut.BeforeMetaSync:
		return "meta-stable", true, true
	case durabilitycut.AfterMetaSync:
		return "meta-stable", false, true
	default:
		return "", false, false
	}
}

func installCompactStorageM0Recorder(r *compactStorageM0StableRecorder) func() {
	return durabilitycut.Install(r.observe)
}
