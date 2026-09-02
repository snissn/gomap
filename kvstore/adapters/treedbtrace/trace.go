package treedbtrace

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
)

const (
	traceEnvPath        = "TREEDB_TRACE_PATH"
	traceEnvEveryN      = "TREEDB_TRACE_EVERY_N"
	traceEnvSummaryPath = "TREEDB_TRACE_SUMMARY_PATH"
	tracePhaseDefault   = "unknown"
)

type traceLogger struct {
	enabled      bool
	path         string
	summaryPath  string
	everyN       uint64
	counter      atomic.Uint64
	iterCounter  atomic.Uint64
	openDBs      atomic.Int64
	closed       atomic.Bool
	phase        atomic.Value
	mu           sync.Mutex
	f            *os.File
	w            *bufio.Writer
	phaseCounts  map[string]*traceCounts
	flushEveryN  uint64
	lastFlushCtr uint64
}

type traceCounts struct {
	Gets              int64
	Has               int64
	Set               int64
	Delete            int64
	SetSync           int64
	DeleteSync        int64
	BatchWrite        int64
	BatchOps          int64
	BatchBytes        int64
	IterCreate        int64
	IterNext          int64
	IterClose         int64
	IterDurationNanos int64
}

type traceEvent struct {
	TS         int64  `json:"ts_unix_nano"`
	Op         string `json:"op"`
	Phase      string `json:"phase,omitempty"`
	IterID     uint64 `json:"iter_id,omitempty"`
	KeyLen     int    `json:"key_len,omitempty"`
	ValueLen   int    `json:"value_len,omitempty"`
	BatchOps   int    `json:"batch_ops,omitempty"`
	BatchBytes int    `json:"batch_bytes,omitempty"`
	IterKind   string `json:"iter_kind,omitempty"`
	IterNexts  int    `json:"iter_nexts,omitempty"`
	IterMillis int64  `json:"iter_ms,omitempty"`
	StartLen   int    `json:"iter_start_len,omitempty"`
	EndLen     int    `json:"iter_end_len,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

var (
	traceOnce sync.Once
	traceInst *traceLogger
	phaseBus  tracePhaseBus
)

type tracePhaseBus struct {
	mu    sync.Mutex
	once  sync.Once
	phase atomic.Value
	dbs   map[*treedb.DB]int

	// test hook: called after a DB is registered and before the initial phase
	// application. Nil in production.
	registerBeforeApply func()
}

func (b *tracePhaseBus) init() {
	b.once.Do(func() {
		b.phase.Store(tracePhaseDefault)
		b.dbs = make(map[*treedb.DB]int)
	})
}

func (b *tracePhaseBus) current() string {
	b.init()
	if v := b.phase.Load(); v != nil {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return tracePhaseDefault
}

func normalizeTracePhase(phase string) string {
	phase = strings.TrimSpace(strings.ToLower(phase))
	if phase == "" {
		return tracePhaseDefault
	}
	return phase
}

func maintenancePhaseForTrace(phase string) treedb.MaintenancePhase {
	switch normalizeTracePhase(phase) {
	case "restore":
		return treedb.MaintenancePhaseRestore
	case "catchup":
		return treedb.MaintenancePhaseCatchUp
	default:
		return treedb.MaintenancePhaseSteady
	}
}

func (b *tracePhaseBus) set(phase string) string {
	b.init()
	phase = normalizeTracePhase(phase)
	b.phase.Store(phase)
	b.mu.Lock()
	dbs := make([]*treedb.DB, 0, len(b.dbs))
	for db := range b.dbs {
		dbs = append(dbs, db)
	}
	b.mu.Unlock()
	maintenancePhase := maintenancePhaseForTrace(phase)
	for _, db := range dbs {
		if db != nil {
			db.SetMaintenancePhase(maintenancePhase)
		}
	}
	return phase
}

func (b *tracePhaseBus) register(db *treedb.DB) {
	if db == nil {
		return
	}
	b.init()
	b.mu.Lock()
	b.dbs[db]++
	beforeApply := b.registerBeforeApply
	b.mu.Unlock()
	if beforeApply != nil {
		beforeApply()
	}
	for {
		phase := b.current()
		db.SetMaintenancePhase(maintenancePhaseForTrace(phase))
		if b.current() == phase {
			return
		}
	}
}

func (b *tracePhaseBus) unregister(db *treedb.DB) {
	if db == nil {
		return
	}
	b.init()
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := b.dbs[db]; n > 1 {
		b.dbs[db] = n - 1
		return
	}
	delete(b.dbs, db)
}

func getTrace() *traceLogger {
	traceOnce.Do(initTrace)
	return traceInst
}

func initTrace() {
	path := os.Getenv(traceEnvPath)
	if path == "" {
		return
	}
	everyN := uint64(1)
	if v := os.Getenv(traceEnvEveryN); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			everyN = n
		}
	}
	summaryPath := os.Getenv(traceEnvSummaryPath)
	if summaryPath == "" {
		summaryPath = path + ".summary.json"
	}
	f, err := os.Create(path)
	if err != nil {
		return
	}
	t := &traceLogger{
		enabled:     true,
		path:        path,
		summaryPath: summaryPath,
		everyN:      everyN,
		f:           f,
		w:           bufio.NewWriterSize(f, 256*1024),
		phaseCounts: make(map[string]*traceCounts),
		flushEveryN: 1000,
	}
	t.phase.Store(tracePhaseDefault)
	traceInst = t
}

// SetTracePhase updates the current phase tag for trace events and also drives
// the TreeDB maintenance-phase bridge used by the treedbtrace wrapper, even
// when JSONL tracing is disabled.
func SetTracePhase(phase string) {
	phase = phaseBus.set(phase)
	t := getTrace()
	if t == nil || !t.enabled {
		return
	}
	t.phase.Store(phase)
	t.logEvent(traceEvent{
		Op:     "phase",
		Phase:  phase,
		Detail: "SetTracePhase",
	})
}

// CurrentTracePhase returns the current phase label used by the trace bridge.
func CurrentTracePhase() string {
	return phaseBus.current()
}

func (t *traceLogger) registerDB() {
	if t == nil || !t.enabled {
		return
	}
	t.openDBs.Add(1)
}

func (t *traceLogger) closeDB() {
	if t == nil || !t.enabled {
		return
	}
	if t.openDBs.Add(-1) == 0 {
		t.writeSummary()
		t.close()
	}
}

func (t *traceLogger) close() {
	if t == nil || !t.enabled || t.closed.Load() {
		return
	}
	t.closed.Store(true)
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = t.w.Flush()
	_ = t.f.Sync()
	_ = t.f.Close()
}

func (t *traceLogger) phaseName() string {
	if t == nil || !t.enabled {
		return ""
	}
	if v := t.phase.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return tracePhaseDefault
}

func (t *traceLogger) logEvent(ev traceEvent) {
	if t == nil || !t.enabled {
		return
	}
	if t.closed.Load() {
		return
	}
	cur := t.counter.Add(1)
	if ev.Op != "phase" && t.everyN > 1 && (cur%t.everyN) != 0 {
		return
	}
	ev.TS = time.Now().UnixNano()
	ev.Phase = t.phaseName()
	buf, err := json.Marshal(ev)
	if err != nil {
		return
	}
	t.mu.Lock()
	_, _ = t.w.Write(buf)
	_ = t.w.WriteByte('\n')
	if t.flushEveryN > 0 && (cur-t.lastFlushCtr) >= t.flushEveryN {
		_ = t.w.Flush()
		t.lastFlushCtr = cur
	}
	t.mu.Unlock()
}

func (t *traceLogger) addCount(phase string, fn func(c *traceCounts)) {
	if t == nil || !t.enabled {
		return
	}
	t.mu.Lock()
	c := t.phaseCounts[phase]
	if c == nil {
		c = &traceCounts{}
		t.phaseCounts[phase] = c
	}
	fn(c)
	t.mu.Unlock()
}

func (t *traceLogger) writeSummary() {
	if t == nil || !t.enabled {
		return
	}
	t.mu.Lock()
	snapshot := make(map[string]traceCounts, len(t.phaseCounts))
	for k, v := range t.phaseCounts {
		snapshot[k] = *v
	}
	t.mu.Unlock()

	buf, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(t.summaryPath, buf, 0644)
}

func (t *traceLogger) nextIterID() uint64 {
	if t == nil || !t.enabled {
		return 0
	}
	return t.iterCounter.Add(1)
}

func (t *traceLogger) noteIterClose(iterID uint64, kind string, nexts int, dur time.Duration) {
	phase := t.phaseName()
	t.addCount(phase, func(c *traceCounts) {
		c.IterClose++
		c.IterNext += int64(nexts)
		c.IterDurationNanos += dur.Nanoseconds()
	})
	t.logEvent(traceEvent{
		Op:         "iter_close",
		IterID:     iterID,
		IterKind:   kind,
		IterNexts:  nexts,
		IterMillis: dur.Milliseconds(),
	})
}

func (t *traceLogger) noteIterCreate(iterID uint64, kind string, start, end []byte) {
	phase := t.phaseName()
	t.addCount(phase, func(c *traceCounts) {
		c.IterCreate++
	})
	t.logEvent(traceEvent{
		Op:       "iter_create",
		IterID:   iterID,
		IterKind: kind,
		StartLen: len(start),
		EndLen:   len(end),
	})
}

func (t *traceLogger) noteBatchWrite(ops, bytes int) {
	phase := t.phaseName()
	t.addCount(phase, func(c *traceCounts) {
		c.BatchWrite++
		c.BatchOps += int64(ops)
		c.BatchBytes += int64(bytes)
	})
	t.logEvent(traceEvent{
		Op:         "batch_write",
		BatchOps:   ops,
		BatchBytes: bytes,
	})
}

func (t *traceLogger) noteOp(op string, keyLen, valueLen int) {
	phase := t.phaseName()
	t.addCount(phase, func(c *traceCounts) {
		switch op {
		case "get":
			c.Gets++
		case "has":
			c.Has++
		case "set":
			c.Set++
		case "delete":
			c.Delete++
		case "set_sync":
			c.SetSync++
		case "delete_sync":
			c.DeleteSync++
		}
	})
	t.logEvent(traceEvent{
		Op:       op,
		KeyLen:   keyLen,
		ValueLen: valueLen,
	})
}

func (t *traceLogger) String() string {
	if t == nil {
		return "trace<nil>"
	}
	return fmt.Sprintf("trace<enabled=%v path=%s>", t.enabled, t.path)
}

type traceIterator struct {
	inner  kvstore.Iterator
	tracer *traceLogger
	iterID uint64
	kind   string
	start  time.Time
	nexts  int
}

func (t *traceIterator) Valid() bool { return t.inner.Valid() }

func (t *traceIterator) Next() {
	if t.inner.Valid() {
		t.nexts++
	}
	t.inner.Next()
}

func (t *traceIterator) Key() []byte { return t.inner.Key() }

func (t *traceIterator) Value() []byte { return t.inner.Value() }

func (t *traceIterator) KeyCopy(dst []byte) []byte { return t.inner.KeyCopy(dst) }

func (t *traceIterator) ValueCopy(dst []byte) []byte { return t.inner.ValueCopy(dst) }

func (t *traceIterator) Error() error { return t.inner.Error() }

func (t *traceIterator) Close() error {
	if t.tracer != nil {
		t.tracer.noteIterClose(t.iterID, t.kind, t.nexts, time.Since(t.start))
	}
	return t.inner.Close()
}
