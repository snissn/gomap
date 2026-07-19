package db

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

type compactStorageM0StableCall struct {
	phase, resource, callType string
	count                     uint64
	duration                  time.Duration
	unmatchedStarts           uint64
	unmatchedFinishes         uint64
}

type compactStorageM0PhaseFrontier struct {
	phase                               string
	beforeVisible, beforeDurable        uint64
	afterVisible, afterDurable          uint64
	stableCallsBefore, stableCallsAfter uint64
	available                           bool
}

type compactStorageM0ForegroundHandshake struct {
	attempted chan struct{}
	armed     atomic.Bool
	once      sync.Once
}

func newCompactStorageM0ForegroundHandshake() *compactStorageM0ForegroundHandshake {
	return &compactStorageM0ForegroundHandshake{attempted: make(chan struct{})}
}

func (h *compactStorageM0ForegroundHandshake) arm() {
	h.armed.Store(true)
}

func (h *compactStorageM0ForegroundHandshake) observe(event durabilitycut.Event) {
	if h != nil && h.armed.Load() && event.Point == durabilitycut.BeforeUserspaceFlush {
		h.once.Do(func() { close(h.attempted) })
	}
}

type compactStorageM0StableRecorder struct {
	db        *DB
	mu        sync.Mutex
	phase     string
	started   map[string][]time.Time
	calls     map[string]*compactStorageM0StableCall
	frontiers map[string][]compactStorageM0PhaseFrontier
	total     atomic.Uint64
}

func newCompactStorageM0StableRecorder(db *DB) *compactStorageM0StableRecorder {
	return &compactStorageM0StableRecorder{
		db: db, started: make(map[string][]time.Time), calls: make(map[string]*compactStorageM0StableCall),
		frontiers: make(map[string][]compactStorageM0PhaseFrontier),
	}
}

func (r *compactStorageM0StableRecorder) beginPhase(phase string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phase = phase
	frontier := compactStorageM0PhaseFrontier{phase: phase, stableCallsBefore: r.total.Load()}
	if r.db != nil && r.db.rootPublication != nil && r.db.rootPublication.coordinator != nil {
		stats := r.db.rootPublication.coordinator.Stats()
		frontier.available = true
		frontier.beforeVisible = stats.VisibleCommitSeq
		frontier.beforeDurable = stats.DurableCommitSeq
	}
	r.frontiers[phase] = append(r.frontiers[phase], frontier)
}

func (r *compactStorageM0StableRecorder) endPhase(phase string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	records := r.frontiers[phase]
	if len(records) > 0 {
		record := &records[len(records)-1]
		record.stableCallsAfter = r.total.Load()
		if record.available {
			stats := r.db.rootPublication.coordinator.Stats()
			record.afterVisible = stats.VisibleCommitSeq
			record.afterDurable = stats.DurableCommitSeq
		}
		r.frontiers[phase] = records
	}
	if r.phase == phase {
		r.phase = ""
	}
}

func (r *compactStorageM0StableRecorder) observe(event durabilitycut.Event) error {
	callType, before, ok := compactStorageM0CallType(event.Point)
	if !ok {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	phase := r.phase
	if phase == "" {
		phase = "outside-phase"
	}
	key := strings.Join([]string{phase, string(event.Resource), callType, event.Path}, "|")
	entry := r.calls[key]
	if entry == nil {
		entry = &compactStorageM0StableCall{phase: phase, resource: string(event.Resource), callType: callType}
		r.calls[key] = entry
	}
	if before {
		r.started[key] = append(r.started[key], time.Now())
		return nil
	}
	started := r.started[key]
	if len(started) == 0 {
		entry.unmatchedFinishes++
		return nil
	}
	begin := started[len(started)-1]
	r.started[key] = started[:len(started)-1]
	entry.count++
	entry.duration += time.Since(begin)
	r.total.Add(1)
	return nil
}

func (r *compactStorageM0StableRecorder) totalCalls() uint64 { return r.total.Load() }

func (r *compactStorageM0StableRecorder) phaseCallCount(phase string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total uint64
	for _, call := range r.calls {
		if call.phase == phase {
			total += call.count
		}
	}
	return total
}

func (r *compactStorageM0StableRecorder) measurements() []compactStorageMeasurementStableCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	aggregated := make(map[string]*compactStorageMeasurementStableCall)
	for key, call := range r.calls {
		aggregateKey := strings.Join([]string{call.phase, call.resource, call.callType}, "|")
		entry := aggregated[aggregateKey]
		if entry == nil {
			entry = &compactStorageMeasurementStableCall{
				Phase: call.phase, Resource: call.resource, CallType: call.callType,
			}
			aggregated[aggregateKey] = entry
		}
		entry.Count += call.count
		entry.WallTimeNanos += call.duration.Nanoseconds()
		entry.UnmatchedStarts += uint64(len(r.started[key]))
		entry.UnmatchedFinishes += call.unmatchedFinishes
	}
	out := make([]compactStorageMeasurementStableCall, 0, len(aggregated))
	for _, call := range aggregated {
		out = append(out, *call)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Phase != out[j].Phase {
			return out[i].Phase < out[j].Phase
		}
		if out[i].Resource != out[j].Resource {
			return out[i].Resource < out[j].Resource
		}
		return out[i].CallType < out[j].CallType
	})
	return out
}

func (r *compactStorageM0StableRecorder) checkpointMeasurements(phases []CompactStoragePhaseStats) []compactStorageMeasurementCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	wall := make(map[string][]int64)
	for _, phase := range phases {
		if strings.HasPrefix(phase.Name, "checkpoint") {
			wall[phase.Name] = append(wall[phase.Name], phase.WallTimeNanos)
		}
	}
	var out []compactStorageMeasurementCheckpoint
	for phase, records := range r.frontiers {
		if !strings.HasPrefix(phase, "checkpoint") {
			continue
		}
		for i, record := range records {
			m := compactStorageMeasurementCheckpoint{
				Phase: phase, Availability: compactStorageMeasurementUnavailable,
				StableCallCounter: compactStorageMeasurementObserved,
				StableCalls:       record.stableCallsAfter - record.stableCallsBefore,
			}
			if i < len(wall[phase]) {
				m.WallTimeNanos = wall[phase][i]
			}
			if record.available {
				m.Availability = compactStorageMeasurementObserved
				m.BeforeVisibleFrontier, m.BeforeDurableFrontier = record.beforeVisible, record.beforeDurable
				m.AfterVisibleFrontier, m.AfterDurableFrontier = record.afterVisible, record.afterDurable
				m.ExactCoverageBefore = record.beforeDurable >= record.beforeVisible
				m.ExactCoverageAfter = record.afterDurable >= record.afterVisible
				if m.ExactCoverageBefore {
					m.CoverageReason = "durable-frontier-already-covered"
				} else {
					m.CoverageReason = "wait-through-visible-frontier"
				}
			} else {
				m.CoverageReason = "root-publication-stats-unavailable"
			}
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Phase < out[j].Phase })
	return out
}

func compactStorageM0CallType(point durabilitycut.Point) (string, bool, bool) {
	switch point {
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
	case durabilitycut.BeforeDeletionDirectorySync:
		return "deletion-directory-stable", true, true
	case durabilitycut.AfterDeletionDirectorySync:
		return "deletion-directory-stable", false, true
	default:
		return "", false, false
	}
}

func installCompactStorageM0Recorder(r *compactStorageM0StableRecorder, foregroundHandshake *compactStorageM0ForegroundHandshake) func() {
	return durabilitycut.Install(func(event durabilitycut.Event) error {
		foregroundHandshake.observe(event)
		if r == nil {
			return nil
		}
		return r.observe(event)
	})
}
