//go:build mvcc_native_foreground && treedb_test

package mvcc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This is an opt-in causal pilot, not a retained latency benchmark. Fixed-size
// buckets include real Store/caching lock wait; no kernel or hook is paused.
type foregroundLatency struct {
	Count, TotalNS, MaxNS uint64
	Buckets               [8]uint64
}

func (h *foregroundLatency) add(d time.Duration) {
	ns := uint64(d)
	h.Count++
	h.TotalNS += ns
	if ns > h.MaxNS {
		h.MaxNS = ns
	}
	limits := [7]uint64{10000, 100000, 1000000, 5000000, 10000000, 50000000, 100000000}
	i := 0
	for i < 7 && ns > limits[i] {
		i++
	}
	h.Buckets[i]++
}

type foregroundResult struct {
	AcceptanceCausePresent                                                                                                                           bool
	AcceptanceCauseType, AcceptanceCauseText                                                                                                         string
	LastCOWPhase, LastCOWSet, LastCOWItem                                                                                                            int
	LastCOWAuxiliaryPhase, LastCOWPrunePhase, LastCOWMaterializerPhase, LastCOWBeginPhase                                                            int
	LastCOWGeneration, LastCOWCommit                                                                                                                 uint64
	LastAcceptancePhase                                                                                                                              int
	LastEnqueuePhase                                                                                                                                 uint64
	FinalWorkRecords, FinalWorkBytes, AcceptanceCostRecords, AcceptanceCostBytes, SourceCostRecords, SourceCostBytes                                 uint64
	LastBackendRoot, LastBackendCommit, CurrentBackendRoot, CurrentBackendCommit, LastPublicationGeneration                                          uint64
	ReadersStopWithWriter, ForcedBudgetError                                                                                                         bool
	LastAction                                                                                                                                       uint8
	LastNativePhase, LastNativeFramePhase, LastNativeLeafPhase                                                                                       uint8
	LastNativeRunIndex                                                                                                                               int
	LastChunk                                                                                                                                        uint64
	DrainCalls                                                                                                                                       uint64
	Schema                                                                                                                                           string `json:"schema"`
	N                                                                                                                                                int    `json:"n"`
	Mode, Algorithm                                                                                                                                  string
	PID                                                                                                                                              int
	Calls, Records, Bytes, Pruned, MaxRecords, MaxBytes, WriterActiveCalls, ACKWhileWriterActive, ACKAfterWriterStop                                 uint64
	CancelTransitions, CancelDrains, FloorRecaptures, QualificationResets, NewPreparations, Refusals, MinimumRecords, MinimumBytes                   uint64
	Reads, Writes, ReadIntervalsOverlappingQuantum, WriteIntervalsOverlappingQuantum, ForegroundIntervalsAtQuantumStart                              uint64
	WriterDurationNS                                                                                                                                 uint64
	WriterStopReason                                                                                                                                 string
	ReadLatency, WriteLatency, QuantumLatency                                                                                                        foregroundLatency
	PartialPrivateOutput, CompletedWhileWriterActive, CompletedAfterStop, PhysicalOracle, WriterOracle, PointerOracle, OldReaderOracle, ReopenOracle bool
	Error                                                                                                                                            string
}

func foregroundPayload(i uint64) []byte {
	p := bytes.Repeat([]byte{0x71}, 8192)
	binary.LittleEndian.PutUint64(p[len(p)-8:], i)
	return p
}
func TestNativePruneForegroundPilot(t *testing.T) {
	out := os.Getenv("MVCC_FOREGROUND_RESULT")
	if out == "" {
		t.Skip("opt-in driver only")
	}
	n, e := strconv.Atoi(os.Getenv("MVCC_FOREGROUND_N"))
	if e != nil || n < 64 || n > 128 {
		t.Fatal("N64/128 causal pilot only")
	}
	mode, algorithm := os.Getenv("MVCC_FOREGROUND_MODE"), os.Getenv("MVCC_FOREGROUND_ALGORITHM")
	if mode != "burst" && mode != "growth" && mode != "churn" {
		t.Fatal("mode")
	}
	if algorithm != "bounded" && algorithm != "unbounded" {
		t.Fatal("algorithm")
	}
	r := foregroundResult{ReadersStopWithWriter: os.Getenv("MVCC_FOREGROUND_READER_STOP_WITH_WRITER") == "1", ForcedBudgetError: os.Getenv("MVCC_FOREGROUND_FORCE_BUDGET_ERROR") == "1", Schema: "gomap-native-foreground-v2", N: n, Mode: mode, Algorithm: algorithm, PID: os.Getpid()}
	defer func() {
		data, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		if err = os.WriteFile(out, append(data, '\n'), 0600); err != nil {
			t.Error(err)
		}
	}()
	fail := func(err error) { r.Error = err.Error(); t.Fatal(err) }
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 2048
	db, err := treedb.Open(opts)
	if err != nil {
		fail(err)
	}
	defer func() { _ = db.Close() }()
	s := New(db)
	history, writer := []byte("foreground-history"), []byte("foreground-writer")
	for i := 1; i <= n; i++ {
		value := bytes.Repeat([]byte{0x61}, 1024)
		if i == n {
			value = foregroundPayload(0)
		}
		if err = s.CommitAt(uint64(i), []Mutation{{Key: history, Value: value}}, CommitRelaxed); err != nil {
			fail(err)
		}
	}
	if err = s.CommitAt(uint64(n+1), []Mutation{{Key: writer, Value: foregroundPayload(0)}}, CommitRelaxed); err != nil {
		fail(err)
	}
	if err = s.AdvanceDiscardFloor(uint64(n), CommitDurable); err != nil {
		fail(err)
	}
	if err = db.Checkpoint(); err != nil {
		fail(err)
	}
	if err = db.Close(); err != nil {
		fail(err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		fail(err)
	}
	s = New(db)
	oldKey, _ := mvcckey.Encode(history, 1)
	oldValue, err := db.Get(oldKey)
	if err != nil {
		fail(err)
	}
	reader := db.AcquireSnapshot()
	if reader == nil {
		fail(fmt.Errorf("raw reader"))
	}
	defer reader.Close()
	var cursor *PruneCursor
	foregroundStarted := false
	quantum := func() (PruneStats, error) {
		before := cursor
		opts := PruneOptions{BatchSize: 1, Mode: CommitRelaxed, Cursor: cursor}
		if algorithm == "bounded" {
			opts.WorkRecords = 32
			opts.WorkBytes = 1 << 20
		}
		start := time.Now()
		stats, err := s.PruneVersions(opts)
		r.QuantumLatency.add(time.Since(start))
		r.Calls++
		r.FinalWorkRecords = stats.WorkRecords
		r.FinalWorkBytes = stats.WorkBytes
		r.Records += stats.WorkRecords
		r.Bytes += stats.WorkBytes
		r.Pruned += stats.Pruned
		if stats.WorkRecords > r.MaxRecords {
			r.MaxRecords = stats.WorkRecords
		}
		if stats.WorkBytes > r.MaxBytes {
			r.MaxBytes = stats.WorkBytes
		}
		cursor = stats.Cursor
		if algorithm == "bounded" && (stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || (r.ForcedBudgetError && foregroundStarted)) {
			err = fmt.Errorf("quantum exceeded %+v", stats)
			if r.ForcedBudgetError && foregroundStarted {
				err = fmt.Errorf("forced harness budget error")
			}
		}
		if before != nil && cursor != nil {
			if before.action != pruneCancelGroup && cursor.action == pruneCancelGroup {
				r.CancelTransitions++
			}
			if before.action == pruneCancelGroup && cursor.action != pruneCancelGroup {
				r.CancelDrains++
			}
			if !before.loadingFloor && cursor.loadingFloor {
				r.FloorRecaptures++
			}
			if before.integrityDone && !cursor.integrityDone {
				r.QualificationResets++
			}
			if cursor.preparation != nil && before.preparation != cursor.preparation {
				r.NewPreparations++
			}
		}
		var large *iterator.OrdinalUnitTooLarge
		if errors.As(err, &large) {
			r.Refusals++
			r.MinimumRecords = large.Records
			r.MinimumBytes = large.Bytes
		}
		return stats, err
	}
	if algorithm == "bounded" {
		for i := 0; i < 8192; i++ {
			stats, err := quantum()
			if err != nil {
				fail(err)
			}
			if stats.Complete {
				fail(fmt.Errorf("completed before private output"))
			}
			if cursor != nil && cursor.preparation != nil {
				state := cursor.preparation.MaintenanceEligibilityStateForTest()
				if !state.Accepted && !state.EOF && state.Private.AllocatedPages > 0 && state.Private.Native.OutputBufferBytes > 0 {
					r.PartialPrivateOutput = true
					break
				}
			}
		}
		if !r.PartialPrivateOutput {
			fail(fmt.Errorf("no actual private output"))
		}
	}
	var inQuantum, readInCall, writeInCall, active atomic.Bool
	var acknowledged atomic.Uint64
	var stopReads atomic.Bool
	var wg sync.WaitGroup
	writerDone := make(chan struct{})
	errCh := make(chan error, 2)
	active.Store(true)
	writerStart := time.Now()
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer close(writerDone)
		defer active.Store(false)
		defer func() { r.WriterDurationNS = uint64(time.Since(writerStart)) }()
		limit := uint64(256)
		if mode == "burst" {
			limit = 16
		}
		if mode == "churn" {
			limit = 1024
		}
		for i := uint64(1); i <= limit; i++ {
			ts := uint64(n + 1)
			if mode != "churn" {
				ts += i
			}
			writeInCall.Store(true)
			overlap := inQuantum.Load()
			start := time.Now()
			e := s.CommitAt(ts, []Mutation{{Key: writer, Value: foregroundPayload(i)}}, CommitRelaxed)
			r.WriteLatency.add(time.Since(start))
			overlap = overlap || inQuantum.Load()
			writeInCall.Store(false)
			if overlap {
				r.WriteIntervalsOverlappingQuantum++
			}
			if e != nil {
				errCh <- e
				return
			}
			acknowledged.Store(i)
			r.Writes++
			if mode != "burst" && time.Since(writerStart) >= 120*time.Millisecond {
				r.WriterStopReason = "observation-duration"
				return
			}
			if mode == "burst" {
				time.Sleep(time.Millisecond)
			} else {
				runtime.Gosched()
			}
		}
		r.WriterStopReason = "write-cap"
		if mode == "burst" {
			r.WriterStopReason = "finite-burst"
		}
	}()
	go func() {
		defer wg.Done()
		for !stopReads.Load() {
			if r.ReadersStopWithWriter && !active.Load() {
				return
			}
			readInCall.Store(true)
			overlap := inQuantum.Load()
			lower := acknowledged.Load()
			start := time.Now()
			v, e := s.GetAt(writer, ^uint64(0))
			r.ReadLatency.add(time.Since(start))
			overlap = overlap || inQuantum.Load()
			readInCall.Store(false)
			if overlap {
				r.ReadIntervalsOverlappingQuantum++
			}
			if e != nil {
				errCh <- e
				return
			}
			if v.State != Present || len(v.Value) != 8192 || binary.LittleEndian.Uint64(v.Value[len(v.Value)-8:]) < lower {
				errCh <- fmt.Errorf("writer visibility/read envelope")
				return
			}
			r.Reads++
			runtime.Gosched()
		}
	}()
	foregroundStarted = true
	completed := false
	var pruneErr error
	for i := 0; i < 32768; i++ {
		if readInCall.Load() || writeInCall.Load() {
			r.ForegroundIntervalsAtQuantumStart++
		}
		inQuantum.Store(true)
		stats, e := quantum()
		inQuantum.Store(false)
		if active.Load() {
			r.WriterActiveCalls++
			r.ACKWhileWriterActive += stats.Pruned
		} else {
			r.ACKAfterWriterStop += stats.Pruned
			r.DrainCalls++
		}
		if e != nil {
			pruneErr = e
			break
		}
		if stats.Complete || algorithm == "unbounded" {
			completed = true
			r.CompletedWhileWriterActive = active.Load()
			r.CompletedAfterStop = !r.CompletedWhileWriterActive
			break
		}
		runtime.Gosched()
	}
	<-writerDone
	stopReads.Store(true)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			fail(e)
		}
	}
	if pruneErr != nil {
		r.Error = pruneErr.Error()
	}
	if !completed && r.Error == "" {
		r.Error = "finite observation plus drain did not complete"
	}
	if cursor != nil {
		r.LastAction = cursor.action
		r.LastBackendRoot = cursor.ordinal.BackendRoot
		r.LastBackendCommit = cursor.ordinal.BackendCommit
		r.LastPublicationGeneration = cursor.ordinal.PublicationGeneration
		if snap := db.AcquireSnapshot(); snap != nil {
			if state := snap.State(); state != nil {
				r.CurrentBackendRoot = state.RootPageID
				r.CurrentBackendCommit = state.CommitSeq
			}
			_ = snap.Close()
		}
		if cursor.preparation != nil {
			state := cursor.preparation.MaintenanceEligibilityStateForTest()
			r.LastChunk = state.Chunk
			// Exact source-bound terminal scalar observation through the existing tagged
			// PrivateOwner. No unsafe access, mutation, hidden owner or repeated registry.
			if state.PrivateOwner != nil {
				owner := reflect.ValueOf(state.PrivateOwner).Elem().FieldByName("accept")
				if !owner.IsNil() {
					a := owner.Elem()
					cause := a.FieldByName("cause")
					r.AcceptanceCausePresent = cause.IsValid() && !cause.IsNil()
					if r.AcceptanceCausePresent {
						value := cause.Elem()
						r.AcceptanceCauseType = value.Type().String()
						if value.Kind() == reflect.Pointer {
							text := value.Elem().FieldByName("s")
							if text.IsValid() && text.Kind() == reflect.String {
								r.AcceptanceCauseText = text.String()
							}
						}
					}
					cow := a.FieldByName("cow")
					if !cow.IsNil() {
						v := cow.Elem()
						r.LastCOWPhase = int(v.FieldByName("phase").Int())
						r.LastCOWSet = int(v.FieldByName("set").Int())
						r.LastCOWItem = int(v.FieldByName("item").Int())
						r.LastCOWGeneration = v.FieldByName("generationID").Uint()
						r.LastCOWCommit = v.FieldByName("commitSeq").Uint()
						for name, destination := range map[string]*int{"auxiliary": &r.LastCOWAuxiliaryPhase, "prune": &r.LastCOWPrunePhase, "materializer": &r.LastCOWMaterializerPhase, "begin": &r.LastCOWBeginPhase} {
							step := v.FieldByName(name)
							if !step.IsNil() {
								phase := step.Elem().FieldByName("phase")
								if phase.IsValid() {
									if phase.Kind() == reflect.Uint8 {
										*destination = int(phase.Uint())
									} else {
										*destination = int(phase.Int())
									}
								}
							}
						}
					}
					r.LastAcceptancePhase = int(a.FieldByName("phase").Int())
					cost := a.FieldByName("activationCost")
					r.AcceptanceCostRecords = cost.FieldByName("Records").Uint()
					r.AcceptanceCostBytes = cost.FieldByName("Bytes").Uint()
					r.SourceCostRecords = a.FieldByName("sourceRecords").Uint()
					r.SourceCostBytes = a.FieldByName("sourceBytes").Uint()
					enqueue := a.FieldByName("enqueue")
					if !enqueue.IsNil() {
						r.LastEnqueuePhase = enqueue.Elem().FieldByName("phase").Uint()
					}
				}
			}
			r.LastNativePhase = state.Private.Native.Phase
			r.LastNativeFramePhase = state.Private.Native.FramePhase
			r.LastNativeLeafPhase = state.Private.Native.LeafPhase
			r.LastNativeRunIndex = state.Private.Native.RunIndex
			t.Logf("terminal causal state action=%d floorLoading=%v integrity=%v native=%+v", cursor.action, cursor.loadingFloor, cursor.integrityDone, state)
		}
	}
	if r.Writes == 0 || r.Reads == 0 || r.WriterActiveCalls == 0 || r.ForegroundIntervalsAtQuantumStart+r.ReadIntervalsOverlappingQuantum+r.WriteIntervalsOverlappingQuantum == 0 {
		fail(fmt.Errorf("no genuine overlap/progress witness"))
	}
	if completed && r.Pruned != uint64(n-1) {
		fail(fmt.Errorf("ACK=%d want=%d", r.Pruned, n-1))
	}
	if cursor != nil {
		if err = cursor.Close(); err != nil {
			fail(err)
		}
	}
	kept, err := reader.Get(oldKey)
	if err != nil || !bytes.Equal(kept, oldValue) {
		fail(fmt.Errorf("old reader: %v", err))
	}
	r.OldReaderOracle = true
	if err = reader.Close(); err != nil {
		fail(err)
	}
	expected := int(r.Writes) + 2
	if mode == "churn" {
		expected = 2
	}
	expected += n - 1 - int(r.Pruned)
	check := func() {
		eligibilityPhysicalCount(t, s, expected)
		requireResult(t, s, history, uint64(n+1), Present, uint64(n), foregroundPayload(0))
		if mode == "churn" {
			requireResult(t, s, writer, uint64(n+1), Present, uint64(n+1), foregroundPayload(r.Writes))
		} else {
			for i := uint64(0); i <= r.Writes; i++ {
				requireResult(t, s, writer, uint64(n+1)+i, Present, uint64(n+1)+i, foregroundPayload(i))
			}
		}
	}
	check()
	r.PhysicalOracle = true
	r.WriterOracle = true
	// Pointer kind is checked against actual published entries after reopen below.
	if err = db.Checkpoint(); err != nil {
		fail(err)
	}
	if err = db.Close(); err != nil {
		fail(err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		fail(err)
	}
	s = New(db)
	check()
	r.ReopenOracle = true
	proof := db.AcquireSnapshot()
	if proof == nil {
		fail(fmt.Errorf("pointer proof snapshot"))
	}
	for _, key := range [][]byte{history, writer} {
		ts := uint64(n)
		if bytes.Equal(key, writer) {
			ts = uint64(n + 1)
			if mode != "churn" {
				ts += r.Writes
			}
		}
		physical, _ := mvcckey.Encode(key, ts)
		entry, e := proof.GetEntryExact(physical)
		if e != nil || entry.Flags&node.FlagPointer == 0 || entry.ValuePtr.FileID == 0 {
			_ = proof.Close()
			fail(fmt.Errorf("actual surviving pointer kind: %v", e))
		}
	}
	if e := proof.Close(); e != nil {
		fail(e)
	}
	r.PointerOracle = true
	if r.Error != "" {
		t.Error(r.Error)
	}
}

// Exercise the actual pilot's error/worker-join/oracle path in a child process.
// The diagnostic flag forces only the harness predicate; product stats are real.
func TestNativePruneForegroundBudgetErrorJoinsWorkers(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir() + "/forced-error.json"
	command := exec.Command(binary, "-test.run=^TestNativePruneForegroundPilot$", "-test.count=1", "-test.timeout=30s", "-test.v")
	command.Env = append(os.Environ(), "MVCC_FOREGROUND_RESULT="+out, "MVCC_FOREGROUND_N=64", "MVCC_FOREGROUND_MODE=burst", "MVCC_FOREGROUND_ALGORITHM=bounded", "MVCC_FOREGROUND_READER_STOP_WITH_WRITER=0", "MVCC_FOREGROUND_FORCE_BUDGET_ERROR=1")
	raw, err := command.CombinedOutput()
	if err == nil || strings.Contains(string(raw), "DATA RACE") || strings.Contains(string(raw), "test timed out") {
		t.Fatalf("expected clean rejected pilot: %v\n%s", err, raw)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("missing joined result: %v\n%s", err, raw)
	}
	var result foregroundResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if !result.ForcedBudgetError || result.Error != "forced harness budget error" || result.Writes != 16 || result.Reads == 0 || !result.PhysicalOracle || !result.WriterOracle || !result.PointerOracle || !result.OldReaderOracle || !result.ReopenOracle {
		t.Fatalf("error did not reach joined cleanup/oracles: %+v\n%s", result, raw)
	}
}
