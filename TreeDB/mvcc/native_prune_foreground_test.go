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

// Raw durations have a fixed storage ceiling. A full recorder rejects retained
// qualification; it never silently drops a sample. Each worker owns its buffers.
const foregroundSampleCapacity = 65536

type foregroundSamples struct {
	DurationsNS             []uint64
	FirstStartNS, LastEndNS uint64
	Overflow                bool
}

func (h *foregroundSamples) init() { h.DurationsNS = make([]uint64, 0, foregroundSampleCapacity) }
func (h *foregroundSamples) add(start, end time.Time, origin time.Time) {
	if len(h.DurationsNS) == foregroundSampleCapacity {
		h.Overflow = true
		return
	}
	if len(h.DurationsNS) == 0 {
		h.FirstStartNS = uint64(start.Sub(origin))
	}
	h.LastEndNS = uint64(end.Sub(origin))
	h.DurationsNS = append(h.DurationsNS, uint64(end.Sub(start)))
}

type foregroundRetained struct {
	SetupBuckets                                                                                         [8]uint64
	Clock, PhaseRule                                                                                     string
	Capacity                                                                                             int
	SetupCalls, SetupTotalNS, SetupMaxNS, WriterStopNS, WorkersJoinedNS, CleanupEndNS, CleanupDurationNS uint64
	DBDir                                                                                                string
	OwnersClosed                                                                                         bool
	ReadActive, ReadDrain, WriteActive, QuantumActive, QuantumDrain                                      foregroundSamples
	AfterWorkers, AfterCleanup                                                                           foregroundMetrics
}
type foregroundResult struct {
	Retained                                                                                                                                         *foregroundRetained `json:"retained,omitempty"`
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
	Reads, ReadsAfterWriterStop, Writes, ReadIntervalsOverlappingQuantum, WriteIntervalsOverlappingQuantum, ForegroundIntervalsAtQuantumStart        uint64
	WriterDurationNS                                                                                                                                 uint64
	WriterStopReason                                                                                                                                 string
	ReadLatency, WriteLatency, QuantumLatency                                                                                                        foregroundLatency
	PartialPrivateOutput, CompletedWhileWriterActive, CompletedAfterStop, PhysicalOracle, WriterOracle, PointerOracle, OldReaderOracle, ReopenOracle bool
	Error                                                                                                                                            string
}

// Sample the required peer state at public return, then end our call bracket.
// Timing, terminal decisions and accounting belong after this boundary.
// Publish before sampling the peer. These adjacent caller instructions define
// an approximate sampled envelope, not continuous internal-call overlap.
func foregroundEntryBoundary(inCall *atomic.Bool, sample func() bool) bool {
	inCall.Store(true)
	return sample()
}

func foregroundReturnBoundary(inCall, peer *atomic.Bool) bool {
	sampled := peer.Load()
	inCall.Store(false)
	return sampled
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
	retained := os.Getenv("MVCC_FOREGROUND_RETAINED") == "1"
	if e != nil || n < 64 || (!retained && n > 128) || (retained && n > 8192) {
		t.Fatal("pilot N64/128; retained N64..8192 only")
	}
	mode, algorithm := os.Getenv("MVCC_FOREGROUND_MODE"), os.Getenv("MVCC_FOREGROUND_ALGORITHM")
	if mode != "burst" && mode != "growth" && mode != "churn" {
		t.Fatal("mode")
	}
	if algorithm != "bounded" && algorithm != "unbounded" {
		t.Fatal("algorithm")
	}
	r := foregroundResult{ReadersStopWithWriter: os.Getenv("MVCC_FOREGROUND_READER_STOP_WITH_WRITER") == "1", ForcedBudgetError: os.Getenv("MVCC_FOREGROUND_FORCE_BUDGET_ERROR") == "1", Schema: "gomap-native-foreground-v2", N: n, Mode: mode, Algorithm: algorithm, PID: os.Getpid()}
	if retained {
		if !foregroundMetricsEnabled {
			t.Fatal("retained mode requires mvcc_native_prune tag and unmerged native observer/runtime dependency")
		}
		r.Schema = "gomap-native-foreground-retained-v1"
		r.Retained = &foregroundRetained{Clock: "Go time.Now monotonic duration in nanoseconds", PhaseRule: "read start before writerDone closes = active; read start after writerDone closes = drain; quantum active flag sampled at entry; writes active; setup excluded; cleanup after workers join includes oracles checkpoint reopen and final close", Capacity: foregroundSampleCapacity}
		for _, h := range []*foregroundSamples{&r.Retained.ReadActive, &r.Retained.ReadDrain, &r.Retained.WriteActive, &r.Retained.QuantumActive, &r.Retained.QuantumDrain} {
			h.init()
		}
	}
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
	if retained {
		r.Retained.DBDir = opts.Dir
	}
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
	var active atomic.Bool
	var inQuantum, readInCall, writeInCall atomic.Bool
	var measurementStart time.Time
	quantum := func() (PruneStats, error, bool) {
		before := cursor
		opts := PruneOptions{BatchSize: 1, Mode: CommitRelaxed, Cursor: cursor}
		if algorithm == "bounded" {
			opts.WorkRecords = 32
			opts.WorkBytes = 1 << 20
		}
		startedActive := active.Load()
		start := time.Now()
		if foregroundStarted {
			if foregroundEntryBoundary(&inQuantum, func() bool { return readInCall.Load() || writeInCall.Load() }) {
				r.ForegroundIntervalsAtQuantumStart++
			}
		}
		stats, err := s.PruneVersions(opts)
		// Keep the first post-return activity snapshot for ACK/completion attribution.
		writerActiveAtReturn := foregroundReturnBoundary(&inQuantum, &active)
		end := time.Now()
		r.QuantumLatency.add(end.Sub(start))
		if retained && foregroundStarted {
			h := &r.Retained.QuantumDrain
			if startedActive {
				h = &r.Retained.QuantumActive
			}
			h.add(start, end, measurementStart)
		}
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
		return stats, err, writerActiveAtReturn
	}
	if algorithm == "bounded" {
		for i := 0; i < 8192; i++ {
			stats, err, _ := quantum()
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
	var acknowledged atomic.Uint64
	var stopReads atomic.Bool
	var wg sync.WaitGroup
	writerDone := make(chan struct{})
	readerDone := make(chan struct{})
	postWriterRead := make(chan struct{})
	writerStarted := make(chan struct{})
	errCh := make(chan error, 2)
	if retained {
		r.Retained.SetupCalls = r.Calls
		r.Retained.SetupTotalNS = r.QuantumLatency.TotalNS
		r.Retained.SetupMaxNS = r.QuantumLatency.MaxNS
		r.Retained.SetupBuckets = r.QuantumLatency.Buckets
		foregroundMetricsBegin()
		defer foregroundMetricsStop()
	}
	measurementStart = time.Now()
	wg.Add(2)
	defer func() {
		stopReads.Store(true)
		wg.Wait()
	}()
	go func() {
		defer wg.Done()
		defer close(writerDone)
		defer active.Store(false)
		writerStart := time.Now()
		limit := uint64(256)
		if mode == "burst" {
			limit = 16
		}
		if mode == "churn" {
			limit = 1024
		}
		if retained && mode != "burst" {
			limit = 8192
		}
		duration := 120 * time.Millisecond
		if retained {
			duration = time.Second
		}
		for i := uint64(1); i <= limit; i++ {
			ts := uint64(n + 1)
			if mode != "churn" {
				ts += i
			}
			mutations := []Mutation{{Key: writer, Value: foregroundPayload(i)}}
			if i == 1 {
				active.Store(true)
				close(writerStarted)
			}
			start := time.Now()
			overlap := foregroundEntryBoundary(&writeInCall, inQuantum.Load)
			e := s.CommitAt(ts, mutations, CommitRelaxed)
			overlap = foregroundReturnBoundary(&writeInCall, &inQuantum) || overlap
			end := time.Now()
			durationStop := mode != "burst" && end.Sub(writerStart) >= duration
			// Decide every terminal path at the public write return, before bookkeeping.
			terminal := e != nil || i == limit || durationStop
			if terminal {
				active.Store(false)
				r.WriterDurationNS = uint64(end.Sub(writerStart))
				if retained {
					r.Retained.WriterStopNS = uint64(end.Sub(measurementStart))
				}
			}
			r.WriteLatency.add(end.Sub(start))
			if retained {
				r.Retained.WriteActive.add(start, end, measurementStart)
			}
			if overlap {
				r.WriteIntervalsOverlappingQuantum++
			}
			if e != nil {
				errCh <- e
				return
			}
			acknowledged.Store(i)
			r.Writes++
			if durationStop {
				r.WriterStopReason = "observation-duration"
				return
			}
			if terminal {
				break
			}
			if mode == "burst" {
				if i < limit {
					time.Sleep(time.Millisecond)
				}
			} else {
				runtime.Gosched()
			}
		}
		r.WriterStopReason = "write-cap"
		if mode == "burst" {
			r.WriterStopReason = "finite-burst"
		}
	}()
	<-writerStarted
	go func() {
		defer wg.Done()
		defer close(readerDone)
		for !stopReads.Load() {
			if r.ReadersStopWithWriter && !active.Load() {
				return
			}
			// Only a read started after the final write can witness this phase.
			afterWriter := false
			select {
			case <-writerDone:
				afterWriter = true
			default:
			}
			lower := acknowledged.Load()
			start := time.Now()
			overlap := foregroundEntryBoundary(&readInCall, inQuantum.Load)
			v, e := s.GetAt(writer, ^uint64(0))
			overlap = foregroundReturnBoundary(&readInCall, &inQuantum) || overlap
			end := time.Now()
			r.ReadLatency.add(end.Sub(start))
			if retained {
				h := &r.Retained.ReadActive
				if afterWriter {
					h = &r.Retained.ReadDrain
				}
				h.add(start, end, measurementStart)
			}
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
			if afterWriter {
				r.ReadsAfterWriterStop++
				if r.ReadsAfterWriterStop == 1 {
					close(postWriterRead)
				}
			}
			runtime.Gosched()
		}
	}()
	foregroundStarted = true
	completed := false
	var pruneErr error
	for i := 0; i < 32768; i++ {
		stats, e, writerActiveAtReturn := quantum()
		if writerActiveAtReturn {
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
			r.CompletedWhileWriterActive = writerActiveAtReturn
			r.CompletedAfterStop = !r.CompletedWhileWriterActive
			break
		}
		runtime.Gosched()
	}
	<-writerDone
	if !r.ReadersStopWithWriter {
		select {
		case <-postWriterRead:
		case <-readerDone:
		}
	}
	stopReads.Store(true)
	wg.Wait()
	if retained {
		r.Retained.WorkersJoinedNS = uint64(time.Since(measurementStart))
		r.Retained.AfterWorkers = foregroundMetricsSnapshot()
	}
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
	if !r.ReadersStopWithWriter && r.ReadsAfterWriterStop == 0 {
		fail(fmt.Errorf("no completed read started after writer stop"))
	}
	if r.Writes == 0 || r.Reads == 0 || r.WriterActiveCalls == 0 || r.ForegroundIntervalsAtQuantumStart+r.ReadIntervalsOverlappingQuantum+r.WriteIntervalsOverlappingQuantum == 0 {
		fail(fmt.Errorf("no observed call-envelope overlap/progress witness"))
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
	if retained {
		if err = db.Close(); err != nil {
			fail(err)
		}
		r.Retained.OwnersClosed = true
		r.Retained.CleanupEndNS = uint64(time.Since(measurementStart))
		r.Retained.CleanupDurationNS = r.Retained.CleanupEndNS - r.Retained.WorkersJoinedNS
		r.Retained.AfterCleanup = foregroundMetricsSnapshot()
		foregroundMetricsStop()
	}
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
	if !result.ForcedBudgetError || result.Error != "forced harness budget error" || result.Writes != 16 || result.Reads == 0 || result.ReadsAfterWriterStop == 0 || !result.PhysicalOracle || !result.WriterOracle || !result.PointerOracle || !result.OldReaderOracle || !result.ReopenOracle {
		t.Fatalf("error did not reach joined cleanup/oracles: %+v\n%s", result, raw)
	}
}

func TestNativePruneForegroundRecorderBounds(t *testing.T) {
	h := foregroundSamples{}
	h.init()
	begin := time.Now()
	h.add(begin, begin.Add(time.Nanosecond), begin)
	if len(h.DurationsNS) != 1 || h.DurationsNS[0] != 1 || h.FirstStartNS != 0 || h.LastEndNS != 1 || h.Overflow {
		t.Fatal("monotonic raw interval")
	}
	h.DurationsNS = h.DurationsNS[:foregroundSampleCapacity]
	h.add(begin, begin.Add(time.Nanosecond), begin)
	if !h.Overflow || len(h.DurationsNS) != foregroundSampleCapacity || cap(h.DurationsNS) != foregroundSampleCapacity {
		t.Fatal("recorder must reject overflow without allocation")
	}
}

// A counterpart starting during post-return bookkeeping must not observe an
// already returned public call, including every terminal write path.
func TestNativePruneForegroundEntryBoundary(t *testing.T) {
	for _, operation := range []string{"write", "read", "quantum"} {
		t.Run(operation, func(t *testing.T) {
			var inCall, peer atomic.Bool
			for _, occupied := range []bool{false, true} {
				inCall.Store(false)
				peer.Store(occupied)
				sampled := foregroundEntryBoundary(&inCall, func() bool {
					if !inCall.Load() {
						t.Fatal("peer sampled before entry marker publication")
					}
					return peer.Load()
				})
				if sampled != occupied || !inCall.Load() {
					t.Fatal("entry did not retain its marker and peer observation")
				}
			}
		})
	}
}

func TestNativePruneForegroundReturnBoundary(t *testing.T) {
	for _, operation := range []string{"write-cap", "write-duration", "write-error", "read", "quantum"} {
		t.Run(operation, func(t *testing.T) {
			var inCall, peer, bookkeeping atomic.Bool
			returned := make(chan bool)
			resume := make(chan struct{})
			finished := make(chan struct{})
			inCall.Store(true)
			go func() {
				// Model a completed public call and block before clocks, terminal
				// state, error delivery or metrics can be recorded.
				sampled := foregroundReturnBoundary(&inCall, &peer)
				returned <- sampled
				<-resume
				bookkeeping.Store(true)
				close(finished)
			}()
			if sampled := <-returned; sampled || bookkeeping.Load() || inCall.Load() {
				close(resume)
				<-finished
				t.Fatal("returned call leaked into bookkeeping interval")
			}
			peer.Store(true) // A new counterpart starts while bookkeeping is paused.
			if inCall.Load() {
				t.Error("new counterpart counted a returned call as overlap")
			}
			close(resume)
			<-finished
			if !bookkeeping.Load() {
				t.Fatal("caller did not resume bookkeeping")
			}
			inCall.Store(true)
			if !foregroundReturnBoundary(&inCall, &peer) || inCall.Load() {
				t.Fatal("lost a peer genuinely active at return")
			}
			peer.Store(false)
			inCall.Store(true)
			if foregroundReturnBoundary(&inCall, &peer) || inCall.Load() {
				t.Fatal("serialized calls reported overlap")
			}
		})
	}
}
