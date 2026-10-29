package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// This is actual pending typed construction, not an allocator filler or a
// reduced-limit fixture. Each public request is called once. A bounded prefix
// stops at the first original construction watermark, before the next producer
// can perform maintenance. The original 4096-row service control is separate.
type nativePressureDrainFixture struct {
	owner   *treedb.DB
	backend *backenddb.DB
	manager *CollectionManager
	col     *Collection
	held    *CollectionReadView
	budget  interface {
		Stats() memtable.COWStats
		Limits() memtable.COWLimits
	}
	rows int
}

func openNativePressureDrainFixture(t *testing.T, dir string, sizeCleanup, create bool) *nativePressureDrainFixture {
	t.Helper()
	return openNativePressureDrainFixtureWithTarget(t, dir, sizeCleanup, create, 0)
}

func openNativePressureDrainFixtureWithTarget(t *testing.T, dir string, sizeCleanup, create bool, segmentTarget int64) *nativePressureDrainFixture {
	t.Helper()
	f := new(nativePressureDrainFixture)
	t.Cleanup(func() { f.close(t) })
	opts := nativeTypedGroupOptions(dir)
	if segmentTarget > 0 {
		// Existing logical segment target only; original frame limit, COW limits
		// and soft size-maintenance threshold remain unchanged.
		opts.CommandWALSegmentTargetBytes = segmentTarget
	}
	// Leave the original positive, soft size trigger enabled, without a timer
	// racing the observed prefix. This small fixture never reaches 2 GiB.
	if sizeCleanup {
		opts.MaxWALBytes = 2 << 30
	}
	var err error
	f.owner, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	f.backend, err = f.owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	f.manager = NewCollectionManager(f.backend)
	if create {
		_, err = f.manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore: &ColumnStoreConfig{
				Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
			},
		}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	f.col, err = f.manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	read, err := f.col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	f.budget = read.capture.NativeAllocator().(interface {
		Stats() memtable.COWStats
		Limits() memtable.COWLimits
	})
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.budget.Limits().Validate(); err != nil {
		t.Fatal(err)
	}
	if limits := f.budget.Limits(); limits != memtable.DefaultCOWLimits() {
		t.Fatalf("fixture changed original finite limits: %+v", limits)
	}
	if create {
		if err = f.owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *nativePressureDrainFixture) close(t *testing.T) {
	t.Helper()
	if f.held != nil {
		if err := f.held.Close(); err != nil {
			t.Error(err)
		}
		f.held = nil
	}
	if f.manager != nil {
		if err := f.manager.Close(); err != nil {
			t.Error(err)
		}
		f.manager = nil
	}
	if f.owner == nil {
		return
	}
	if err := f.owner.Close(); err != nil {
		t.Error(err)
	}
	f.owner = nil
	if s := f.budget.Stats(); s.TotalBytes != 0 || s.ReservedBytes != 0 || s.ExternalLeases != 0 || s.Sources != 0 || s.Views != 0 || s.Generations != 0 {
		t.Errorf("original finite owner after Close: %+v", s)
	}
	pins := f.backend.ValueLogIdentityPinRegistry()
	if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
		t.Errorf("original stable custody after Close: %+v: %v", s, pins.CleanupError())
	}
}

func nativePressureDrainRow(i int) ([]byte, []byte, []float32) {
	id := []byte(fmt.Sprintf("row-%04d", i))
	// Full retained JSON is genuine published input. The repeated payload has
	// no text index and no artificial allocator role.
	doc := []byte(fmt.Sprintf(`{"embedding":[%d,1],"content":"%s"}`, i+1, strings.Repeat("payload-", 64)))
	return id, doc, []float32{float32(i + 1), 1}
}

func (f *nativePressureDrainFixture) append(t *testing.T, n int) uint64 {
	t.Helper()
	ids, docs, vectors := make([][]byte, n), make([][]byte, n), make([][]float32, n)
	for i := range ids {
		ids[i], docs[i], vectors[i] = nativePressureDrainRow(f.rows + i)
	}
	lsn := f.backend.CommandWALNextLSN()
	got, stats, err := f.col.InsertBatchWithStatsValidatedFloat32Projection(ids, docs, "embedding", VectorMetricCosine, vectors)
	if err != nil || len(got) != n || stats.Documents != n {
		t.Fatalf("once-only public insert rows=%d count=%d ids=%q stats=%+v: %v", f.rows, n, got, stats, err)
	}
	for i := range ids {
		if !bytes.Equal(got[i], ids[i]) {
			t.Fatalf("insert identity %d: %q", i, got[i])
		}
	}
	if next := f.backend.CommandWALNextLSN(); next != lsn+1 {
		t.Fatalf("one original command: lsn=%d next=%d", lsn, next)
	}
	f.rows += n
	return lsn
}

func (f *nativePressureDrainFixture) assertRows(t *testing.T) {
	t.Helper()
	for i := 0; i < f.rows; i++ {
		id, want, _ := nativePressureDrainRow(i)
		got, err := f.col.Get(id)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("latest row %q: %s: %v", id, got, err)
		}
	}
	missing, _, _ := nativePressureDrainRow(f.rows)
	if got, err := f.col.Get(missing); err != nil || got != nil {
		t.Fatalf("unaccepted suffix %q: %s: %v", missing, got, err)
	}
}

func (f *nativePressureDrainFixture) assertHeld(t *testing.T) {
	t.Helper()
	ids := make([][]byte, 65)
	for i := range ids {
		ids[i], _, _ = nativePressureDrainRow(i)
	}
	got, err := f.held.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil || len(got.Results) != len(ids) {
		t.Fatalf("held first command: %+v: %v", got, err)
	}
	for i, result := range got.Results {
		_, want, _ := nativePressureDrainRow(i)
		if result.Found != (i < 64) || (i < 64 && !bytes.Equal(result.Document, want)) || (i == 64 && result.Document != nil) {
			t.Fatalf("held row %d changed: %+v", i, result)
		}
	}
}

func (f *nativePressureDrainFixture) pressurePrefix(t *testing.T) (backenddb.NativeProducerObservation, uint64) {
	t.Helper()
	var observation backenddb.NativeProducerObservation
	var last uint64
	for batch := 0; batch < 16; batch++ {
		last = f.append(t, 64)
		if f.held == nil {
			var err error
			f.held, err = f.col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
		}
		var err error
		observation, err = f.backend.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
		p := observation.Semantic
		constructionPressure := p.ConstructionBytes != 0 && (p.ConstructionBytes >= p.MaxInFlightBytes-p.MaxInFlightBytes/4 || p.ConstructionBytes >= p.MaxTotalBytes-p.MaxTotalBytes/4 || p.Sources >= p.MaxSources)
		if observation.UnderPressure && constructionPressure {
			f.assertHeld(t)
			t.Logf("real accepted construction prefix rows=%d last_lsn=%d observation=%+v budget=%+v", f.rows, last, observation, f.budget.Stats())
			return observation, last
		}
		if observation.UnderPressure {
			t.Fatalf("fixture reached other pressure before typed construction: %+v", observation)
		}
	}
	t.Fatalf("bounded actual prefix did not reach original construction pressure: rows=%d observation=%+v", f.rows, observation)
	return observation, last
}

func nativePressureDrainStat(t *testing.T, f *nativePressureDrainFixture, key string) uint64 {
	t.Helper()
	value := f.owner.Stats()[key]
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("existing statistic %s=%q: %v", key, value, err)
	}
	return n
}

// All semantic/durable/refund/held-cut oracles precede the rotation oracle, so
// the old full-checkpoint route must fail for the intended physical distinction.
func (f *nativePressureDrainFixture) prepare(t *testing.T) (string, uint64) {
	t.Helper()
	before, last := f.pressurePrefix(t)
	path := f.owner.Stats()["treedb.command_wal.writer.path"]
	if path == "" {
		t.Fatal("missing original active journal identity")
	}
	beforeNext := f.backend.CommandWALNextLSN()
	after, err := f.backend.PrepareNativeProducerObservation()
	if err != nil {
		t.Fatal(err)
	}
	if after.LogicalSequence != before.LogicalSequence || f.backend.CommandWALNextLSN() != beforeNext {
		t.Fatalf("maintenance changed original command identity: before=%+v after=%+v next=%d want=%d", before, after, f.backend.CommandWALNextLSN(), beforeNext)
	}
	if after.Semantic.ConstructionBytes != 0 || after.Semantic.Sources != 0 {
		t.Fatalf("accepted construction not retired: before=%+v after=%+v", before, after)
	}
	state, ok := f.backend.StateToken()
	if !ok || state.AppliedCommandLSN != last {
		t.Fatalf("physical prefix=%+v want original last LSN=%d", state, last)
	}
	if durable := nativePressureDrainStat(t, f, "treedb.command_wal.durable_wal_lsn"); durable < last {
		t.Fatalf("prefix not durable: %d < %d", durable, last)
	}
	f.assertRows(t)
	f.assertHeld(t)
	// The held opening retains its genuine old ownership. Releasing it must
	// remain safe after the producer has joined its own original refund frontier;
	// final full Close below independently requires every original receipt zero.
	if s := f.budget.Stats(); s.ReservedBytes > f.budget.Limits().MaxInFlightBytes {
		t.Fatalf("finite owner after prepare: %+v", s)
	}
	t.Logf("completed original pressure prefix before=%+v after=%+v budget=%+v", before, after, f.budget.Stats())
	return path, last
}

func TestNativeTypedProducerPressureKeepsActiveWAL(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("size-cleanup-%t", enabled), func(t *testing.T) {
			f := openNativePressureDrainFixture(t, t.TempDir(), enabled, true)
			path, _ := f.prepare(t)
			afterPath := f.owner.Stats()["treedb.command_wal.writer.path"]
			if (afterPath == path) != enabled {
				t.Fatalf("pressure preparation rotation: size cleanup=%t before=%q after=%q", enabled, path, afterPath)
			}
			// The same original journal must remain appendable after the pressure
			// prefix. Explicit Checkpoint retains its original rotation contract.
			last := f.append(t, 1)
			if got := f.owner.Stats()["treedb.command_wal.writer.path"]; got != afterPath {
				t.Fatalf("small suffix unexpectedly changed active journal: %q -> %q", afterPath, got)
			}
			f.assertRows(t)
			f.assertHeld(t)
			if err := f.owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			state, ok := f.backend.StateToken()
			if !ok || state.AppliedCommandLSN != last {
				t.Fatalf("explicit checkpoint lost suffix: %+v want=%d", state, last)
			}
			if got := f.owner.Stats()["treedb.command_wal.writer.path"]; got == afterPath {
				t.Fatal("explicit full checkpoint lost original rotation behavior")
			}
			f.assertRows(t)
			f.assertHeld(t)
		})
	}
}

func TestNativeTypedProducerPressureActiveWALRecovery(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	const childEnv = "GOMAP_NATIVE_PRESSURE_DRAIN_CRASH_DIR"
	const marker = "NATIVE PRESSURE ACTIVE WAL ACCEPTED"
	if dir := os.Getenv(childEnv); dir != "" {
		f := openNativePressureDrainFixture(t, dir, true, true)
		path, prefix := f.prepare(t)
		if got := f.owner.Stats()["treedb.command_wal.writer.path"]; got != path {
			t.Fatalf("pressure rotated active journal before crash: %q -> %q", path, got)
		}
		suffix := f.append(t, 1)
		state, ok := f.backend.StateToken()
		if !ok || state.AppliedCommandLSN != prefix || suffix != prefix+1 {
			t.Fatalf("suffix must remain WAL-only after physical prefix: %+v prefix=%d suffix=%d", state, prefix, suffix)
		}
		if got := f.owner.Stats()["treedb.command_wal.writer.path"]; got != path {
			t.Fatalf("suffix not in original active journal: %q -> %q", path, got)
		}
		if durable := nativePressureDrainStat(t, f, "treedb.command_wal.durable_wal_lsn"); durable < suffix {
			t.Fatalf("successful suffix ACK not durable: %d < %d", durable, suffix)
		}
		f.assertRows(t)
		f.assertHeld(t)
		// No Close, checkpoint, or extra barrier. This is a process-crash proof,
		// not a power-loss claim. The old held cut is still live at process exit.
		fmt.Printf("%s rows=%d prefix=%d suffix=%d\n", marker, f.rows, prefix, suffix)
		os.Exit(97)
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeTypedProducerPressureActiveWALRecovery$", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	output, err := cmd.CombinedOutput()
	t.Logf("actual active-journal child:\n%s", output)
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 97 || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("active-journal child: %v: %v\n%s", ctx.Err(), err, output)
	}
	var rows int
	var prefix, suffix uint64
	for _, line := range strings.Split(string(output), "\n") {
		if n, _ := fmt.Sscanf(line, marker+" rows=%d prefix=%d suffix=%d", &rows, &prefix, &suffix); n == 3 {
			break
		}
	}
	if rows <= 64 || rows > 1025 || prefix == 0 || suffix != prefix+1 {
		t.Fatalf("missing exact accepted crash boundary: rows=%d prefix=%d suffix=%d", rows, prefix, suffix)
	}
	f := openNativePressureDrainFixture(t, dir, true, false)
	f.rows = rows
	if next := f.backend.CommandWALNextLSN(); next != suffix+1 {
		t.Fatalf("recovery reassigned accepted commands: next=%d want=%d", next, suffix+1)
	}
	f.assertRows(t)
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	state, ok := f.backend.StateToken()
	if !ok || state.AppliedCommandLSN != suffix {
		t.Fatalf("recovered durable physical frontier: %+v want=%d", state, suffix)
	}
	f.close(t)
	if t.Failed() {
		return
	}
	// A second ordinary reopen checks that replay's original suffix survives
	// subsequent full checkpoint and owner cleanup, without recreating schema.
	f = openNativePressureDrainFixture(t, dir, true, false)
	f.rows = rows
	f.assertRows(t)
}

// Natural command-frame rollover still closes journal segments while pressure
// leaves the active writer appendable. The existing explicit maintenance path
// subsequently reclaims covered files; no hard size bound or automatic-worker
// scheduling deadline is inferred from this bounded lifecycle control.
func TestNativeTypedProducerPressureNaturalRolloverAndCleanup(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	f := openNativePressureDrainFixtureWithTarget(t, t.TempDir(), true, true, 1)
	removedBefore := nativePressureDrainStat(t, f, "treedb.command_wal.cleanup.removed_segments_total")
	for pass := 0; pass < 2; pass++ {
		before := f.owner.Stats()["treedb.command_wal.writer.path"]
		path, _ := f.prepare(t)
		if path == before {
			t.Fatalf("pass %d did not exercise actual natural command rollover", pass)
		}
		if after := f.owner.Stats()["treedb.command_wal.writer.path"]; after != path {
			t.Fatalf("pass %d pressure rotated natural active writer: %q -> %q", pass, path, after)
		}
	}
	if err := f.held.Close(); err != nil {
		t.Fatal(err)
	}
	f.held = nil
	// This is the original full checkpoint boundary, including fallback root
	// refresh and covered-file cleanup. It is not another pressure attempt.
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if removed := nativePressureDrainStat(t, f, "treedb.command_wal.cleanup.removed_segments_total"); removed <= removedBefore {
		t.Fatalf("covered natural segments not reclaimed by original maintenance: before=%d after=%d", removedBefore, removed)
	}
	state, ok := f.backend.StateToken()
	if !ok || state.AppliedCommandLSN != f.backend.CommandWALNextLSN()-1 {
		t.Fatalf("maintenance lost original accepted coverage: %+v", state)
	}
	f.assertRows(t)
}
