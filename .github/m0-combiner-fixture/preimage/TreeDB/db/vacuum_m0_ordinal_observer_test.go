package db

// Opt-in diagnostic copy of the original foreground writer. The original
// writer remains unchanged and is used whenever the observer flag is absent.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"testing"
	"time"
)

const m0OrdinalLimit = 4096

var m0OrdinalObserverDir = flag.String("m0-ordinal-observer-dir", "", "diagnostic only: existing absolute output directory; requires one production bytes_1x iteration and enabled trace")

type m0OrdinalRecord struct {
	Iteration  int
	Ordinal    int
	Kind       string
	DurationNS int64
	Error      bool
	Status     string
	Region     string
	Recorded   bool
}

type m0OrdinalObserver struct {
	dir                                                  string
	ctx                                                  context.Context
	records                                              [m0OrdinalLimit]m0OrdinalRecord
	written                                              int
	lost                                                 bool
	joined, timerStopped, statsObserved, exposureChecked bool
	traceEnabledAtStart                                  bool
	hooks, points, ranges, exposureMisses                uint64
	foregroundError, vacuumError                         bool
	latencyMatch                                         bool
	stats                                                VacuumOnlineStats
}

func newM0OrdinalRecords() *m0OrdinalObserver {
	o := &m0OrdinalObserver{ctx: context.Background()}
	for i := range o.records {
		kind := "set"
		if i%2 != 0 {
			kind = "delete-range"
		}
		o.records[i] = m0OrdinalRecord{Iteration: 0, Ordinal: i, Kind: kind, Status: "unrecorded", Region: fmt.Sprintf("m0/production/bytes_1x/iteration/0000/operation/%04d/%s", i, kind)}
	}
	return o
}

func newM0OrdinalObserver(b *testing.B, legacy bool, subcase string) *m0OrdinalObserver {
	if *m0OrdinalObserverDir == "" {
		return nil
	}
	if legacy || subcase != "bytes_1x" || b.N != 1 {
		b.Fatal("M0 observer requires original production bytes_1x, -benchtime=1x and -count=1")
	}
	if !trace.IsEnabled() {
		b.Fatal("M0 observer requires actual -test.trace collection")
	}
	if !filepath.IsAbs(*m0OrdinalObserverDir) {
		b.Fatal("M0 observer requires absolute output directory")
	}
	info, err := os.Stat(*m0OrdinalObserverDir)
	if err != nil || !info.IsDir() {
		b.Fatal("M0 observer output directory missing", err)
	}
	o := newM0OrdinalRecords() // All 4096 records and labels precede ResetTimer.
	o.dir = *m0OrdinalObserverDir
	o.traceEnabledAtStart = true
	return o
}

func (o *m0OrdinalObserver) record(operation int, elapsed time.Duration, err error) {
	if operation < 0 || operation >= len(o.records) || o.records[operation].Recorded {
		o.lost = true
		return
	}
	r := &o.records[operation]
	r.DurationNS = elapsed.Nanoseconds()
	r.Error = err != nil
	r.Status = "success"
	if err != nil {
		r.Status = "operation-error"
	}
	r.Recorded = true
	o.written++
}

// Called only after the original writerDone receive. No concurrent reader
// touches records while the foreground goroutine owns them.
func (o *m0OrdinalObserver) afterJoin(f vacuumCollectionForegroundResult, vacuumErr error, hooks uint64) {
	if o == nil {
		return
	}
	o.joined = true
	o.hooks, o.points, o.ranges = hooks, f.points, f.ranges
	o.foregroundError, o.vacuumError = f.err != nil, vacuumErr != nil
	o.latencyMatch = len(f.latencies) == len(o.records)
	if o.latencyMatch {
		for i, elapsed := range f.latencies {
			if o.records[i].DurationNS != elapsed.Nanoseconds() {
				o.latencyMatch = false
				break
			}
		}
	}
}

func (o *m0OrdinalObserver) validate(closeErr error, failed, traceEnabledAtEnd bool) error {
	if !o.joined || !o.timerStopped || !o.statsObserved || !o.exposureChecked || closeErr != nil || failed {
		return errors.New("M0 incomplete original join/timer/Stats/exposure/checked-Close lifecycle or original failure")
	}
	if !o.traceEnabledAtStart || !traceEnabledAtEnd {
		return errors.New("M0 trace not enabled throughout observed lifecycle")
	}
	if o.lost || o.written != m0OrdinalLimit || !o.latencyMatch {
		return errors.New("M0 incomplete/lost ordinal set or original duration mismatch")
	}
	if o.points != 2048 || o.ranges != 2048 || o.hooks != 1 || o.exposureMisses != 0 || o.foregroundError || o.vacuumError || o.stats.ConcurrentMutationAborts != 0 {
		return errors.New("M0 original counts/error/miss/abort/hook contract not satisfied")
	}
	for i, r := range o.records {
		kind := "set"
		if i%2 != 0 {
			kind = "delete-range"
		}
		identity := fmt.Sprintf("m0/production/bytes_1x/iteration/0000/operation/%04d/%s", i, kind)
		if !r.Recorded || r.Iteration != 0 || r.Ordinal != i || r.Kind != kind || r.Region != identity || r.DurationNS < 0 || r.Error || r.Status != "success" {
			return fmt.Errorf("M0 invalid ordinal record at %d", i)
		}
	}
	return nil
}

func (o *m0OrdinalObserver) finish(b *testing.B, closeErr error) {
	if o == nil {
		return
	}
	// An original early fatal is retained by raw logs, not turned into a
	// successful zero-work packet. Never write while its timer is still active.
	if !o.timerStopped {
		b.Error("M0 incomplete: original timer stop unreached; no ordinal success receipt")
		return
	}
	traceEnd := trace.IsEnabled()
	validationErr := o.validate(closeErr, b.Failed(), traceEnd)
	status, reason := "ORDINALS_COMPLETE_TRACE_ARTIFACT_VALIDATION_PENDING", ""
	if validationErr != nil {
		status, reason = "INCOMPLETE", validationErr.Error()
	}
	value := struct {
		Schema, Status, Reason                                                   string
		DiagnosticOnly                                                           bool
		Iterations, Bound, Written                                               int
		Lost, Joined, TimerStopped, StatsObserved, ExposureChecked, CheckedClose bool
		TraceEnabledAtStart, TraceEnabledAtEnd                                   bool
		PhaseHookCount, Points, Ranges, ExposureMisses                           uint64
		ForegroundError, VacuumError, OriginalDurationsMatch                     bool
		Stats                                                                    VacuumOnlineStats
		Records                                                                  []m0OrdinalRecord
		TraceValidation, Interpretation, SourceAndELFBinding                     string
	}{"m0-ordinal-observer-v1", status, reason, true, 1, m0OrdinalLimit, o.written,
		o.lost, o.joined, o.timerStopped, o.statsObserved, o.exposureChecked, closeErr == nil,
		o.traceEnabledAtStart, traceEnd, o.hooks, o.points, o.ranges, o.exposureMisses,
		o.foregroundError, o.vacuumError, o.latencyMatch, o.stats, o.records[:],
		"External owner must parse retained closed standard trace and require each of these 4096 region identities exactly once; enabled flags and JSON alone do not certify a complete trace.",
		"All 4096 operations precede warmed and final cutover. Region calls bracket original timers but perturb scheduling; rows are diagnostic, never M0 CV acceptance. No queue-versus-execution claim follows without same-request evidence.",
		"External exact-source/ELF/workflow manifest required."}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		b.Fatal(err)
	}
	if err := m0OrdinalWrite(filepath.Join(o.dir, "m0-ordinals.json"), data); err != nil {
		b.Fatal(err)
	}
	if validationErr != nil {
		b.Error(validationErr)
	}
}

func m0OrdinalWrite(path string, data []byte) error {
	if len(data) == 0 || len(data) > 32<<20 {
		return errors.New("M0 diagnostic output outside 1..32MiB")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, writeErr := f.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	return errors.Join(writeErr, closeErr)
}

func m0CompleteOrdinalFixture() *m0OrdinalObserver {
	o := newM0OrdinalRecords()
	o.joined, o.timerStopped, o.statsObserved, o.exposureChecked = true, true, true, true
	o.traceEnabledAtStart, o.latencyMatch = true, true
	o.points, o.ranges, o.hooks = 2048, 2048, 1
	for i := range o.records {
		o.record(i, time.Duration(i+1), nil)
	}
	return o
}

func TestM0OrdinalObserverCompletenessControls(t *testing.T) {
	if err := m0CompleteOrdinalFixture().validate(nil, false, true); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*m0OrdinalObserver)
	}{
		{"missing", func(o *m0OrdinalObserver) { o.records[4095].Recorded = false }},
		{"duplicate", func(o *m0OrdinalObserver) { o.record(0, 1, nil) }},
		{"overflow", func(o *m0OrdinalObserver) { o.record(4096, 1, nil) }},
		{"ordinal", func(o *m0OrdinalObserver) { o.records[1].Ordinal = 0 }},
		{"region", func(o *m0OrdinalObserver) { o.records[1].Region = o.records[0].Region }},
		{"kind", func(o *m0OrdinalObserver) { o.records[1].Kind = "set" }},
		{"duration", func(o *m0OrdinalObserver) { o.records[1].DurationNS = -1 }},
		{"error", func(o *m0OrdinalObserver) { o.records[1].Error = true }},
		{"join", func(o *m0OrdinalObserver) { o.joined = false }},
		{"timer", func(o *m0OrdinalObserver) { o.timerStopped = false }},
		{"stats", func(o *m0OrdinalObserver) { o.statsObserved = false }},
		{"exposure", func(o *m0OrdinalObserver) { o.exposureChecked = false }},
		{"trace", func(o *m0OrdinalObserver) { o.traceEnabledAtStart = false }},
		{"counts", func(o *m0OrdinalObserver) { o.ranges-- }},
		{"hook", func(o *m0OrdinalObserver) { o.hooks = 0 }},
		{"mismatch", func(o *m0OrdinalObserver) { o.latencyMatch = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := m0CompleteOrdinalFixture()
			tc.change(o)
			if err := o.validate(nil, false, true); err == nil {
				t.Fatal("invalid observation accepted")
			}
		})
	}
	o := m0CompleteOrdinalFixture()
	if o.validate(errors.New("close"), false, true) == nil || o.validate(nil, true, true) == nil || o.validate(nil, false, false) == nil {
		t.Fatal("failed lifecycle accepted")
	}
}

func TestM0OrdinalObserverJoinKeepsOriginalDurations(t *testing.T) {
	o := m0CompleteOrdinalFixture()
	f := vacuumCollectionForegroundResult{points: 2048, ranges: 2048, latencies: make([]time.Duration, m0OrdinalLimit)}
	for i := range f.latencies {
		f.latencies[i] = time.Duration(i + 1)
	}
	o.afterJoin(f, nil, 1)
	if err := o.validate(nil, false, true); err != nil {
		t.Fatal(err)
	}
	f.latencies[99]++
	o.afterJoin(f, nil, 1)
	if err := o.validate(nil, false, true); err == nil {
		t.Fatal("different measured duration accepted")
	}
}

// Derived opt-in writer; original API/timer/warmed/join ordering is preserved.
func vacuumCollectionForegroundWriterWithOrdinals(db *DB, start, stop <-chan struct{}, warmed chan<- struct{}, done chan<- vacuumCollectionForegroundResult, observer *m0OrdinalObserver) {
	const warmOperations = 4096
	<-start
	result := vacuumCollectionForegroundResult{latencies: make([]time.Duration, 0, warmOperations*2)}
	var warmOnce sync.Once
	finish := func() {
		warmOnce.Do(func() { close(warmed) })
		done <- result
	}
	defer finish()
	for operation := 0; ; operation++ {
		select {
		case <-stop:
			return
		default:
		}
		if operation >= len(observer.records) {
			observer.lost = true
			result.err = errors.New("M0 ordinal bound exceeded")
			return
		}
		region := trace.StartRegion(observer.ctx, observer.records[operation].Region)
		started := time.Now()
		if operation%2 == 0 {
			key := []byte(fmt.Sprintf("foreground/point/%06d", operation%256))
			value := []byte(fmt.Sprintf("value/%06d", operation))
			result.err = db.Set(key, value)
			result.points++
		} else {
			batch := db.NewBatch()
			rangeID := operation % 64
			startKey := []byte(fmt.Sprintf("foreground/range/%03d/a", rangeID))
			endKey := []byte(fmt.Sprintf("foreground/range/%03d/z", rangeID))
			result.err = batch.DeleteRange(startKey, endKey)
			if result.err == nil {
				result.err = batch.Write()
			}
			if closeErr := batch.Close(); result.err == nil {
				result.err = closeErr
			}
			result.ranges++
		}
		elapsed := time.Since(started)
		region.End()
		observer.record(operation, elapsed, result.err)
		result.latencies = append(result.latencies, elapsed)
		if len(result.latencies) >= warmOperations {
			warmOnce.Do(func() { close(warmed) })
			return
		}
		if result.err != nil {
			return
		}
	}
}
