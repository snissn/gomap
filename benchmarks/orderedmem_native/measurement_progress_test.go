package orderedmemnative

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

// This fake exercises only the harness. No database, disk, benchmark, profiler,
// or GC diagnostic is opened by these counter tests.
type progressEngine struct {
	failTimestamp uint64
	failPersist   bool
}

func (e *progressEngine) Commit(ts uint64, _ []mvcc.Mutation) error {
	if ts == e.failTimestamp {
		return errors.New("injected complete-batch refusal")
	}
	return nil
}
func (*progressEngine) Get([]byte, uint64) (record, error) { return record{}, nil }
func (*progressEngine) Range([]byte, []byte, uint64, int) ([]record, error) {
	return nil, nil
}
func (*progressEngine) History(uint64) (historyCursor, error) { return nil, nil }
func (e *progressEngine) Persist() error {
	if e.failPersist {
		return errors.New("injected persistence refusal")
	}
	return nil
}
func (*progressEngine) Stats() map[string]string { return nil }
func (*progressEngine) Close() error             { return nil }

func progressOperations(n int) []operation {
	ops := make([]operation, n)
	for i := range ops {
		ops[i] = operation{kind: 'c', timestamp: uint64(i + 1), mutations: make([]mvcc.Mutation, 2)}
	}
	return ops
}

func TestProgressPreservesPartialSuccessfulWork(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  config
		engine  progressEngine
		wantOps int
	}{
		{"serial-refusal", config{Case: "commit", Workers: 1}, progressEngine{failTimestamp: 3}, 2},
		// Even timestamps belong to worker 1 and odd timestamps to worker 0.
		// Worker 0 stops at timestamp 3; worker 1 completes all its own work.
		{"shared-refusal", config{Case: "shared", Workers: 2}, progressEngine{failTimestamp: 3}, 4},
		{"maintenance-refusal", config{Case: "lifecycle", Workers: 1, CheckpointEvery: 2}, progressEngine{failPersist: true}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := progressOperations(6)
			latencies := make([]int64, len(ops))
			p := &packet{}
			err := measure(p, "foreground", len(ops), 12, func(ph *phase) error {
				return execute(&tc.engine, ops, tc.config, ph, latencies)
			})
			if err == nil {
				t.Fatal("expected refusal")
			}
			ph := p.Phases[0]
			if ph.Completed || ph.PlannedOperations != 6 || ph.PlannedMutations != 12 || ph.Operations != tc.wantOps || ph.Mutations != tc.wantOps*2 {
				t.Fatalf("wrong partial work: %+v", ph)
			}
			samples := 0
			for _, value := range latencies {
				if value > 0 {
					samples++
				}
			}
			if samples != tc.wantOps {
				t.Fatalf("successful latency samples=%d want=%d", samples, tc.wantOps)
			}
		})
	}
}
