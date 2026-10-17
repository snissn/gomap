package main

import (
	"runtime"
	"sync/atomic"
	"time"
)

// Only the paced writer mutates these counters. The sequence makes each cut
// snapshot coherent without a lock or allocation on the read path. A successful
// commit is recorded even if a later batch Close or overwrite commit fails.
type quicksilverWriterProgress struct {
	sequence         atomic.Uint64
	groups           atomic.Uint64
	targets          atomic.Uint64
	commits          atomic.Uint64
	sets             atomic.Uint64
	deletes          atomic.Uint64
	checkpoints      atomic.Uint64
	checkpointActive atomic.Bool
	readerCutTaken   atomic.Bool
}

type quicksilverProgressSnapshot struct {
	ElapsedNS        int64  `json:"elapsed_ns"`
	Groups           uint64 `json:"completed_groups"`
	Targets          uint64 `json:"completed_mutation_targets"`
	Commits          uint64 `json:"successful_commits"`
	Sets             uint64 `json:"committed_set_operations"`
	Deletes          uint64 `json:"committed_delete_operations"`
	Checkpoints      uint64 `json:"completed_checkpoints"`
	CheckpointActive bool   `json:"checkpoint_in_flight"`
}

func (p *quicksilverWriterProgress) commit(sets, deletes int) {
	if p == nil {
		return
	}
	p.sequence.Add(1)
	p.commits.Add(1)
	p.sets.Add(uint64(sets))
	p.deletes.Add(uint64(deletes))
	p.sequence.Add(1)
}
func (p *quicksilverWriterProgress) group(targets int) {
	p.sequence.Add(1)
	p.targets.Add(uint64(targets))
	p.groups.Add(1)
	p.sequence.Add(1)
}
func (p *quicksilverWriterProgress) startCheckpoint() {
	p.sequence.Add(1)
	p.checkpointActive.Store(true)
	p.sequence.Add(1)
}
func (p *quicksilverWriterProgress) checkpoint(success bool) {
	p.sequence.Add(1)
	if success {
		p.checkpoints.Add(1)
	}
	p.checkpointActive.Store(false)
	p.sequence.Add(1)
}
func (p *quicksilverWriterProgress) snapshot(clock time.Time) quicksilverProgressSnapshot {
	for {
		seq := p.sequence.Load()
		if seq%2 != 0 {
			runtime.Gosched()
			continue
		}
		s := quicksilverProgressSnapshot{Groups: p.groups.Load(), Targets: p.targets.Load(), Commits: p.commits.Load(), Sets: p.sets.Load(), Deletes: p.deletes.Load(), Checkpoints: p.checkpoints.Load(), CheckpointActive: p.checkpointActive.Load()}
		if p.sequence.Load() == seq {
			s.ElapsedNS = time.Since(clock).Nanoseconds()
			return s
		}
	}
}

// These are process-wide Go counters. Native allocations/RSS and final
// checkpoint/close/reopen remain separate evidence, not attributed here.
type quicksilverAllocationCut struct {
	AllocatedBytes uint64 `json:"process_allocated_bytes"`
	Mallocs        uint64 `json:"process_mallocs"`
	HeapBefore     uint64 `json:"process_heap_alloc_before"`
	HeapAfter      uint64 `json:"process_heap_alloc_after"`
	GCPauseNS      uint64 `json:"process_gc_pause_ns"`
	GCCycles       uint32 `json:"process_gc_cycles"`
}

func quicksilverAllocationDelta(before, after *runtime.MemStats) quicksilverAllocationCut {
	return quicksilverAllocationCut{AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Mallocs: after.Mallocs - before.Mallocs, HeapBefore: before.HeapAlloc, HeapAfter: after.HeapAlloc, GCPauseNS: after.PauseTotalNs - before.PauseTotalNs, GCCycles: after.NumGC - before.NumGC}
}

type quicksilverCutProgress struct {
	Before quicksilverProgressSnapshot `json:"before"`
	After  quicksilverProgressSnapshot `json:"after"`
}
type quicksilverConcurrentWork struct {
	Schema              int                         `json:"schema"`
	Mode                string                      `json:"mode"`
	AllocationScope     string                      `json:"allocation_scope"`
	CPUProfileScope     string                      `json:"cpu_profile_scope"`
	AllocsProfileScope  string                      `json:"allocs_profile_scope"`
	RequestedReads      int                         `json:"requested_reads"`
	RequestedReadCounts []int                       `json:"requested_read_counts"`
	CompletedReadCounts []int                       `json:"completed_read_counts"`
	RequestedTargets    int                         `json:"requested_mutation_targets"`
	CompletedReads      int                         `json:"completed_reads"`
	ReaderJoined        bool                        `json:"reader_joined"`
	WriterJoined        bool                        `json:"writer_joined"`
	Completed           bool                        `json:"completed"`
	Error               string                      `json:"error,omitempty"`
	ReaderElapsedNS     int64                       `json:"reader_elapsed_ns"`
	BothJoinElapsedNS   int64                       `json:"both_join_elapsed_ns"`
	ReaderCut           quicksilverAllocationCut    `json:"reader_cut"`
	BothJoin            *quicksilverAllocationCut   `json:"both_join,omitempty"`
	ReaderCutProgress   quicksilverCutProgress      `json:"reader_cut_progress"`
	AfterBothJoin       quicksilverProgressSnapshot `json:"after_both_join"`
	BytesPerRead        float64                     `json:"both_join_bytes_per_read"`
	MallocsPerRead      float64                     `json:"both_join_mallocs_per_read"`
	BytesPerTarget      float64                     `json:"both_join_bytes_per_mutation_target"`
	MallocsPerTarget    float64                     `json:"both_join_mallocs_per_mutation_target"`
}

func quicksilverExpectedWriter(c quicksilverConfig) (groups, commits, checkpoints, sets, deletes int) {
	groups = (c.Updates + 999) / 1000
	checkpoints = min(4, groups)
	commits = groups
	sets = c.Updates
	if c.Case == "realistic" {
		deletes = (c.Updates + 2) / 4
		sets = c.Updates - deletes + 3*(c.Updates/4)
		for off := 0; off < c.Updates; off += 1000 {
			if min(1000, c.Updates-off) >= 4 {
				commits += 3
			}
		}
	}
	return
}
func quicksilverWriterComplete(c quicksilverConfig, s quicksilverProgressSnapshot) bool {
	g, m, h, sets, deletes := quicksilverExpectedWriter(c)
	return s.Groups == uint64(g) && s.Targets == uint64(c.Updates) && s.Commits == uint64(m) && s.Checkpoints == uint64(h) && s.Sets == uint64(sets) && s.Deletes == uint64(deletes) && !s.CheckpointActive
}
