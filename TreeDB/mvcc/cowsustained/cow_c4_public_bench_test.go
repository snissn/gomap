package cowsustained

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	cowhelpers "github.com/snissn/gomap/TreeDB/internal/cowbench"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var c4OutputDir = flag.String("cow-c4-public-output-dir", "", "absolute existing directory for immutable C4 public-call receipts")
var c4Invocation atomic.Uint64

const c4GroupWidth = 16
const c4MaxEpochs = 8

// A finite schedule can complete without the scheduler overlapping public
// calls. That is a qualification refusal, distinct from a lifecycle failure.
// Benchmarks and collectors still reject it; unit construction checks cleanup.
var errC4NoPublicOverlap = errors.New("no actual public-call overlap observed")

type c4Call struct {
	Phase        string `json:"phase"`
	Operation    string `json:"operation"`
	Input        uint64 `json:"input"`
	Output       uint64 `json:"output"`
	StartNS      int64  `json:"start_ns"`
	CompletionNS int64  `json:"completion_ns"`
	DurationNS   int64  `json:"duration_ns"`
	Outcome      string `json:"outcome"`
	Error        string `json:"error"`
}
type c4Boundary struct {
	Phase string            `json:"phase"`
	Stats map[string]string `json:"stats"`
}
type c4LayoutProof struct {
	Phase        string            `json:"phase"`
	Entries      uint64            `json:"entries"`
	Pointers     uint64            `json:"pointers"`
	Inline       uint64            `json:"inline"`
	OwnersBefore map[string]string `json:"owners_before"`
	OwnersAfter  map[string]string `json:"owners_after"`
}
type c4Record struct {
	LifecycleOutcome                 string                   `json:"lifecycle_outcome"`
	LifecycleError                   string                   `json:"lifecycle_error"`
	SchemaVersion                    int                      `json:"schema_version"`
	Leaf                             string                   `json:"leaf"`
	Profile                          string                   `json:"profile"`
	Mode                             string                   `json:"mode"`
	Layout                           string                   `json:"layout"`
	Keys                             int                      `json:"keys"`
	Epochs                           int                      `json:"epochs"`
	GroupWidth                       int                      `json:"group_width"`
	PinRing                          int                      `json:"pin_ring"`
	RecorderCapacity                 int                      `json:"recorder_capacity"`
	Options                          treedb.COWMemtableLimits `json:"limits"`
	Shards                           int                      `json:"shards"`
	FlushThreshold                   int64                    `json:"flush_threshold"`
	BackgroundCheckpointInterval     int64                    `json:"background_checkpoint_interval"`
	BackgroundCheckpointIdleDuration int64                    `json:"background_checkpoint_idle_duration"`
	MaxWALBytes                      int64                    `json:"max_wal_bytes"`
	BackgroundIndexVacuumInterval    int64                    `json:"background_index_vacuum_interval"`
	ValueLogGenerationPolicy         uint8                    `json:"value_log_generation_policy"`
	DisableSideStores                bool                     `json:"disable_side_stores"`
	PointerThreshold                 int                      `json:"pointer_threshold"`
	ForcePointers                    bool                     `json:"force_pointers"`
	OrdinaryACK                      string                   `json:"ordinary_ack"`
	ReadCutCapability                bool                     `json:"read_cut_capability"`
	Calls                            []c4Call                 `json:"calls"`
	Boundaries                       []c4Boundary             `json:"boundaries"`
	LayoutProofs                     []c4LayoutProof          `json:"layout_proofs"`
	OracleReceipts                   []string                 `json:"oracle_receipts"`
	NativeEligibility                string                   `json:"native_eligibility"`
	WholeMaintenanceCharge           string                   `json:"whole_maintenance_charge"`
	Qualification                    string                   `json:"qualification"`
	OverlappingReaders               uint64                   `json:"overlapping_readers"`
	mu                               sync.Mutex
	origin                           time.Time
}

func (r *c4Record) call(phase, op string, input uint64, fn func() (uint64, error)) error {
	begin := time.Now()
	out, err := fn()
	end := time.Now()
	row := c4Call{Phase: phase, Operation: op, Input: input, Output: out, StartNS: begin.Sub(r.origin).Nanoseconds(), CompletionNS: end.Sub(r.origin).Nanoseconds(), DurationNS: end.Sub(begin).Nanoseconds(), Outcome: "success"}
	if err != nil {
		row.Outcome = "error"
		row.Error = err.Error()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.Calls) == cap(r.Calls) {
		return errors.Join(err, errors.New("C4 recorder capacity exhausted"))
	}
	r.Calls = append(r.Calls, row)
	return err
}
func c4Limits() treedb.COWMemtableLimits {
	return treedb.COWMemtableLimits{MaxViews: 256, MaxGenerations: 64, MaxSources: 32, MaxResources: 256, MaxGenerationBytes: 256 << 20, MaxTotalBytes: 2 << 30, MaxRetiredBytes: 2 << 30, MaxInFlightBytes: 64 << 20}
}
func c4Options(profile treedb.Profile, mode string, pointers bool, dir string) treedb.Options {
	o := treedb.OptionsFor(profile, dir)
	o.MemtableMode = mode
	o.MemtableShards = 4
	o.DisableSideStores = true
	o.BackgroundCheckpointInterval = -1
	o.BackgroundCheckpointIdleDuration = -1
	o.MaxWALBytes = -1
	o.BackgroundIndexVacuumInterval = -1
	o.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	o.FlushThreshold = 16 << 20
	o.COWMemtableLimits = c4Limits()
	o.ValueLog.ForcePointers = pointers
	o.ValueLog.PointerThreshold = 1 << 30
	if pointers {
		o.ValueLog.PointerThreshold = 1
	}
	return o
}

// Finite construction owns every checkpoint window; background maintenance is
// explicitly disabled for every comparator and reopen, while storage stays persistent.
func c4AdmitOptions(o treedb.Options) error {
	if o.ValueLog.Generational.Policy != treedb.ValueLogGenerationOff || o.BackgroundCheckpointInterval != -1 || o.BackgroundCheckpointIdleDuration != -1 || o.MaxWALBytes != -1 || o.BackgroundIndexVacuumInterval != -1 {
		return errors.New("requested manual maintenance options mismatch")
	}
	return nil
}
func c4AdmitMaintenance(stats map[string]string) error {
	expected := map[string]string{
		"treedb.cache.vlog_generation.policy":                            "1",
		"treedb.cache.vlog_generation.enabled":                           "false",
		"treedb.cache.vlog_generation.scheduler_state":                   "disabled",
		"treedb.cache.vlog_generation.maintenance.active":                "false",
		"treedb.cache.vlog_generation.checkpoint_kick.active":            "false",
		"treedb.cache.vlog_generation.checkpoint_kick.pending":           "false",
		"treedb.cache.vlog_generation.rewrite.queue.pending":             "false",
		"treedb.cache.vlog_generation.rewrite.queue.running":             "false",
		"treedb.bg_vacuum.enabled":                                       "false",
		"treedb.cache.auto_checkpoint.count":                             "0",
		"treedb.bg_vacuum.runs":                                          "0",
		"treedb.bg_vacuum.vacuums":                                       "0",
		"treedb.bg_vacuum.vacuum_attempts":                               "0",
		"treedb.cache.vlog_generation.checkpoint_kick.runs":              "0",
		"treedb.cache.vlog_generation.maintenance.attempts":              "0",
		"treedb.cache.vlog_generation.maintenance.acquired":              "0",
		"treedb.cache.vlog_generation.maintenance.passes.noop":           "0",
		"treedb.cache.vlog_generation.maintenance.passes.with_rewrite":   "0",
		"treedb.cache.vlog_generation.maintenance.passes.with_gc":        "0",
		"treedb.cache.vlog_generation.maintenance.passes.with_leaf_pack": "0",
		"treedb.cache.vlog_generation.gc.runs":                           "0",
		"treedb.cache.vlog_generation.rewrite.runs":                      "0",
		"treedb.cache.vlog_generation.vacuum.runs":                       "0",
	}
	for k, want := range expected {
		if stats[k] != want {
			return fmt.Errorf("resolved manual maintenance %s: got %q want %q", k, stats[k], want)
		}
	}
	return nil
}

func c4Keys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("c4/%04d", i/2))
		if i%2 != 0 {
			keys[i] = append(keys[i], '/', 'a')
		}
	}
	return keys
}

// Shared fixture payloads are immutable; the mvcc.Store owns its encoded copies.
var c4Payloads = func() [c4MaxEpochs + 1][]byte {
	var values [c4MaxEpochs + 1][]byte
	for e := range values {
		values[e] = bytes.Repeat([]byte{byte(65 + e)}, 256)
	}
	return values
}()

func c4Value(epoch int) []byte { return c4Payloads[epoch] }

// Full retained history follows logical-key order and descending timestamps.
// Equal-timestamp ordinary/grouped replacements retain one record; each epoch
// adds exactly its insert, tombstone, and reinsert to the immutable seed.
func c4Expected(keys [][]byte, epochs int) []mvcc.Version {
	want := make([]mvcc.Version, 0, len(keys)*(1+3*epochs))
	for _, key := range keys {
		for e := epochs; e > 0; e-- {
			ts := uint64(10 * e)
			want = append(want,
				mvcc.Version{Key: key, Value: c4Value(e), Timestamp: ts + 3, State: mvcc.Present},
				mvcc.Version{Key: key, Timestamp: ts + 2, State: mvcc.Tombstone},
				mvcc.Version{Key: key, Value: c4Value(e), Timestamp: ts + 1, State: mvcc.Present})
		}
		want = append(want, mvcc.Version{Key: key, Value: c4Value(0), Timestamp: 1, State: mvcc.Present})
	}
	return want
}

func c4CheckIterator(it *mvcc.VersionIterator, want []mvcc.Version) (uint64, error) {
	var n uint64
	var err error
	for it.Valid() {
		v := it.EntryView()
		if int(n) >= len(want) {
			err = fmt.Errorf("extra version %d", n)
			break
		}
		w := want[n]
		if v.State != w.State || v.Timestamp != w.Timestamp || !bytes.Equal(v.Key, w.Key) || !bytes.Equal(v.Value, w.Value) {
			err = fmt.Errorf("history mismatch at %d", n)
			break
		}
		n++
		it.Next()
	}
	stats := it.Stats()
	err = errors.Join(err, it.Error(), it.Close())
	if err == nil && (int(n) != len(want) || stats.Retained != n || stats.Visited != n || stats.Skipped != 0) {
		err = fmt.Errorf("history output %d expected %d stats %+v", n, len(want), stats)
	}
	return n, err
}
func c4History(s *mvcc.Store, want []mvcc.Version, ts uint64) (uint64, error) {
	it, err := s.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: ts})
	if err != nil {
		return 0, err
	}
	return c4CheckIterator(it, want)
}
func (r *c4Record) boundary(db *treedb.DB, phase string) error {
	stats := db.Stats()
	if err := c4AdmitMaintenance(stats); err != nil {
		return err
	}
	checkpointKey := "treedb.cache.checkpoint.runs"
	checkpointCount, parseErr := strconv.ParseUint(stats[checkpointKey], 10, 64)
	if parseErr != nil {
		return fmt.Errorf("required checkpoint counter: %w", parseErr)
	}
	var expectedCheckpoints uint64
	if len(r.Boundaries) > 0 && r.Boundaries[len(r.Boundaries)-1].Phase != "reopen_counter_reset" {
		previous, e := strconv.ParseUint(r.Boundaries[len(r.Boundaries)-1].Stats[checkpointKey], 10, 64)
		if e != nil {
			return e
		}
		expectedCheckpoints = previous
		if phase == "pinned_checkpoint" || phase == "preclose" || strings.HasSuffix(phase, "_checkpoint") {
			expectedCheckpoints++
		}
	}
	if checkpointCount != expectedCheckpoints {
		return fmt.Errorf("manual checkpoint window %s: got %d want %d", phase, checkpointCount, expectedCheckpoints)
	}
	if err := cowhelpers.ACKRouting(treedb.Profile(r.Profile), stats); err != nil {
		return err
	}
	required := []string{"treedb.cache.snapshot.rotations_total", "treedb.cache.snapshot.rotated_shards_total", "treedb.cache.snapshot.enqueued_records_total", "treedb.commit_seq", "treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total"}
	if r.Mode == "cow_btree" {
		for _, k := range []string{"total_bytes", "history_bytes", "reserved_bytes", "retired_bytes", "peak_bytes", "control_bytes", "deferred_bytes", "external_bytes", "views", "generations", "sources", "external_leases", "active_cuts", "current_roots", "frozen_roots", "capture_calls_total", "prepare_calls_total", "publications_total", "rollovers_total", "handoffs_total"} {
			required = append(required, "treedb.cache.cow."+k)
		}
	}
	for _, k := range required {
		x, err := strconv.ParseUint(stats[k], 10, 64)
		if err != nil {
			return fmt.Errorf("required counter %s: %w", k, err)
		}
		if r.Profile == string(treedb.ProfileNoWALFast) && strings.HasPrefix(k, "treedb.command_wal.") && x != 0 {
			return fmt.Errorf("NoWAL observed nonzero counter %s", k)
		}
		if len(r.Boundaries) > 0 && strings.HasSuffix(k, "_total") {
			prev := r.Boundaries[len(r.Boundaries)-1].Stats[k]
			if prev != "" {
				y, e := strconv.ParseUint(prev, 10, 64)
				if e != nil || x < y {
					return fmt.Errorf("counter regression %s", k)
				}
			}
		}
	}
	r.Boundaries = append(r.Boundaries, c4Boundary{phase, stats})
	return nil
}

// Representation diagnostics are outside the epoch timer. Each actual public
// snapshot call is retained; the pure layout validator performs no public calls.
// MVCC logical tombstones are ordinary stored values and obey the same layout.
func (r *c4Record) proveLayout(db *treedb.DB, phase string, want []mvcc.Version, pointers bool) (err error) {
	proof := c4LayoutProof{Phase: phase, OwnersBefore: map[string]string{}, OwnersAfter: map[string]string{}}
	ownerKeys := []string{"treedb.cache.cow.views", "treedb.cache.cow.active_cuts", "treedb.cache.cow.external_leases"}
	if r.Mode == "cow_btree" {
		stats := db.Stats()
		for _, k := range ownerKeys {
			if _, e := strconv.ParseUint(stats[k], 10, 64); e != nil {
				return fmt.Errorf("layout owner counter %s: %w", k, e)
			}
			proof.OwnersBefore[k] = stats[k]
		}
	}
	var snapshot treedb.Snapshot
	err = r.call(phase, "AcquireSnapshot", 0, func() (uint64, error) {
		snapshot = db.AcquireSnapshot()
		if snapshot == nil {
			return 0, errors.New("layout snapshot unavailable")
		}
		return 1, nil
	})
	if snapshot != nil {
		defer func() {
			err = errors.Join(err, r.call(phase, "Snapshot.Close", 0, func() (uint64, error) { return 0, snapshot.Close() }))
			if r.Mode == "cow_btree" {
				stats := db.Stats()
				for _, k := range ownerKeys {
					proof.OwnersAfter[k] = stats[k]
					if stats[k] != proof.OwnersBefore[k] {
						err = errors.Join(err, fmt.Errorf("layout diagnostic leaked owner %s: %s -> %s", k, proof.OwnersBefore[k], stats[k]))
					}
				}
			}
			if err == nil {
				r.LayoutProofs = append(r.LayoutProofs, proof)
			}
		}()
	}
	if err != nil {
		return err
	}
	for _, version := range want {
		physical, e := mvcckey.Encode(version.Key, version.Timestamp)
		if e != nil {
			return e
		}
		if err = r.call(phase, "Snapshot.GetEntryExact", 1, func() (uint64, error) {
			entry, e := snapshot.GetEntryExact(physical)
			if e != nil {
				return 0, e
			}
			if !bytes.Equal(entry.Key, physical) {
				return 0, errors.New("layout physical key mismatch")
			}
			if e = cowhelpers.ValueLayout(entry, pointers); e != nil {
				return 0, e
			}
			proof.Entries++
			if entry.Flags&node.FlagPointer != 0 {
				proof.Pointers++
			} else {
				proof.Inline++
			}
			return 1, nil
		}); err != nil {
			return err
		}
	}
	return nil
}
func c4Emit(r *c4Record) error {
	dir := *c4OutputDir
	if dir == "" {
		return nil
	}
	if !filepath.IsAbs(dir) {
		return errors.New("C4 output directory must be absolute")
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("C4 output destination is not a directory")
	}
	sum := sha256.Sum256([]byte(r.Leaf))
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	// Exclusive creation refuses accidental reuse; all calibration invocations
	// remain visible to the collector. Failed writes never look like complete JSON.
	name := fmt.Sprintf("c4-%x-N%d-invocation%d.json", sum[:16], r.Epochs, c4Invocation.Add(1))
	f, err := os.CreateTemp(dir, ".c4-incomplete-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	_, err = f.Write(data)
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return os.Link(temp, filepath.Join(dir, name))
}

// Fixed -benchtime=1x..8x denotes complete finite lifecycle epochs, never an
// adaptive duration campaign. Go allocations include oracle/recorder memory;
// COW Stats remain separate engine allocation charges. Native work is pending.
func BenchmarkCOWSustainedPublicMVCC(b *testing.B) {
	for _, p := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, m := range []string{"cow_btree", "append_only", "btree"} {
			for _, ptr := range []bool{false, true} {
				layout := "inline"
				if ptr {
					layout = "forced_pointer"
				}
				for _, n := range []int{512, 1024} {
					b.Run(fmt.Sprintf("%s/%s/%s/N%d", p, m, layout, n), func(b *testing.B) {
						if b.N < 1 || b.N > c4MaxEpochs {
							b.Fatal("use fixed -benchtime=1x..8x")
						}
						b.ReportAllocs()
						b.ResetTimer()
						r, err := c4Run(b, p, m, ptr, n, b.N)
						b.StopTimer()
						if err != nil {
							if err != errC4NoPublicOverlap {
								r.LifecycleOutcome = "error"
								r.LifecycleError = err.Error()
							}
							if emitErr := c4Emit(r); emitErr != nil {
								b.Error(emitErr)
							}
							b.Fatal(err)
						}
						if err = c4Emit(r); err != nil {
							b.Fatal(err)
						}
						b.ReportMetric(float64(len(r.Calls))/float64(b.N), "public_calls/op")
						b.ReportMetric(float64(r.OverlappingReaders), "overlapping_readers")
						b.ReportMetric(1, "close_ok")
					})
				}
			}
		}
	}
}
func c4Run(t testing.TB, p treedb.Profile, mode string, ptr bool, n, epochs int) (*c4Record, error) {
	if (n != 512 && n != 1024) || epochs < 1 || epochs > c4MaxEpochs {
		return nil, errors.New("unsupported finite schedule")
	}
	opts := c4Options(p, mode, ptr, t.TempDir())
	layout := "inline"
	if ptr {
		layout = "forced_pointer"
	}
	capacity := n*(epochs*8+8) + 512 + n*(3+6*epochs) + 6
	r := &c4Record{LifecycleOutcome: "success", SchemaVersion: 2, Leaf: fmt.Sprintf("%s/%s/%s/N%d", p, mode, layout, n), Profile: string(p), Mode: mode, Layout: layout, Keys: n, Epochs: epochs, GroupWidth: c4GroupWidth, PinRing: 2, RecorderCapacity: capacity, Options: opts.COWMemtableLimits, Shards: opts.MemtableShards, FlushThreshold: opts.FlushThreshold, BackgroundCheckpointInterval: int64(opts.BackgroundCheckpointInterval), BackgroundCheckpointIdleDuration: int64(opts.BackgroundCheckpointIdleDuration), MaxWALBytes: opts.MaxWALBytes, BackgroundIndexVacuumInterval: int64(opts.BackgroundIndexVacuumInterval), ValueLogGenerationPolicy: uint8(opts.ValueLog.Generational.Policy), DisableSideStores: opts.DisableSideStores, PointerThreshold: opts.ValueLog.PointerThreshold, ForcePointers: opts.ValueLog.ForcePointers, OrdinaryACK: p.OrdinaryAckClass(), ReadCutCapability: mode == "cow_btree", NativeEligibility: "PENDING", WholeMaintenanceCharge: "PENDING", Qualification: "pending_native_observations", Calls: make([]c4Call, 0, capacity), Boundaries: make([]c4Boundary, 0, epochs*8+16), origin: time.Now()}
	if err := c4AdmitOptions(opts); err != nil {
		return r, err
	}
	var db *treedb.DB
	var pins []*mvcc.VersionIterator
	bench, _ := t.(*testing.B)
	if bench != nil {
		bench.StopTimer()
	}
	defer func() {
		for _, it := range pins {
			if x := it.Close(); x != nil {
				t.Error(x)
			}
		}
		if db != nil {
			if x := db.Close(); x != nil {
				t.Error(x)
			}
		}
	}()
	call := r.call
	err := call("setup", "Open", 0, func() (uint64, error) { var e error; db, e = treedb.Open(opts); return 1, e })
	if err != nil {
		return r, err
	}
	validate := func() error {
		stats := db.Stats()
		if stats["treedb.profile.resolved"] != string(p) || stats["treedb.cache.memtable_mode"] != mode || stats["treedb.profile.ordinary_ack_class"] != p.OrdinaryAckClass() || db.SupportsMVCCReadCut() != (mode == "cow_btree") {
			return errors.New("resolved mode/profile/ACK/capability mismatch")
		}
		return cowhelpers.ACKRouting(p, stats)
	}
	if err = validate(); err != nil {
		return r, err
	}
	if err = r.boundary(db, "opened"); err != nil {
		return r, err
	}
	backendStart := r.Boundaries[len(r.Boundaries)-1].Stats["treedb.commit_seq"]
	lag := func() error {
		current, x := strconv.ParseUint(db.Stats()["treedb.commit_seq"], 10, 64)
		baseline, y := strconv.ParseUint(backendStart, 10, 64)
		if x != nil || y != nil || current < baseline {
			return errors.New("invalid or regressing backend commit sequence")
		}
		if mode == "cow_btree" && current != baseline {
			return errors.New("backend commit sequence changed during declared lag phase")
		}
		return nil
	}
	s := mvcc.New(db)
	keys := c4Keys(n)
	oracle := make([][]mvcc.Version, epochs+1)
	for e := range oracle {
		oracle[e] = c4Expected(keys, e)
	}
	commit := func(phase string, groups []mvcc.CommitGroup) error {
		count := 0
		for _, g := range groups {
			count += len(g.Mutations)
		}
		return call(phase, "CommitGroupAt", uint64(count), func() (uint64, error) {
			e := s.CommitGroupAt(groups, mvcc.CommitRelaxed)
			if e != nil {
				return 0, e
			}
			return uint64(count), nil
		})
	}
	muts := func(start, end, e int, del bool) []mvcc.Mutation {
		v := make([]mvcc.Mutation, end-start)
		for i := range v {
			v[i] = mvcc.Mutation{Key: keys[start+i], Value: c4Value(e), Delete: del}
		}
		return v
	}
	for start := 0; start < n; start += c4GroupWidth {
		if err = commit("seed", []mvcc.CommitGroup{{Timestamp: 1, Mutations: muts(start, start+c4GroupWidth, 0, false)}}); err != nil {
			return r, err
		}
	}
	if err = r.boundary(db, "seed"); err != nil {
		return r, err
	}
	for i := 0; i < 2; i++ {
		err = call("pin", "IterateVersions.acquire", 0, func() (uint64, error) {
			it, e := s.IterateVersions(mvcc.VersionIteratorOptions{})
			if e == nil {
				pins = append(pins, it)
			}
			return 1, e
		})
		if err != nil {
			return r, err
		}
	}
	check := func(phase string, e int) error {
		for _, key := range keys {
			err := call(phase, "GetAt", 1, func() (uint64, error) {
				ts := uint64(1)
				if e > 0 {
					ts = uint64(e*10 + 3)
				}
				v, x := s.GetAt(key, ts)
				if x == nil && (v.State != mvcc.Present || v.Timestamp != ts || !bytes.Equal(v.Value, c4Value(e))) {
					x = errors.New("point oracle mismatch")
				}
				return 1, x
			})
			if err != nil {
				return err
			}
		}
		if e > 0 {
			for _, key := range keys {
				for _, deleted := range []bool{false, true} {
					ts := uint64(e*10 + 1)
					state := mvcc.Present
					value := c4Value(e)
					if deleted {
						ts++
						state = mvcc.Tombstone
						value = nil
					}
					if err := call(phase, "GetAt.historical", 1, func() (uint64, error) {
						v, x := s.GetAt(key, ts)
						if x == nil && (v.State != state || v.Timestamp != ts || !bytes.Equal(v.Value, value)) {
							x = errors.New("historical point mismatch")
						}
						return 1, x
					}); err != nil {
						return err
					}
				}
			}
		}
		return call(phase, "IterateVersions.full", 0, func() (uint64, error) { return c4History(s, oracle[e], 0) })
	}
	if err = check("seed_oracle", 0); err != nil {
		return r, err
	}
	if err = r.proveLayout(db, "seed_layout", oracle[0], ptr); err != nil {
		return r, err
	}
	if bench != nil {
		bench.StartTimer()
	}
	for e := 1; e <= epochs; e++ {
		phase := fmt.Sprintf("epoch_%d", e)
		ts := uint64(e * 10)
		for start := 0; start < n; start += c4GroupWidth {
			end := start + c4GroupWidth
			if err = commit(phase+"_growth", []mvcc.CommitGroup{{Timestamp: ts + 1, Mutations: muts(start, end, e, false)}, {Timestamp: ts + 2, Mutations: muts(start, end, e, true)}, {Timestamp: ts + 3, Mutations: muts(start, end, e, false)}}); err != nil {
				return r, err
			}
		}
		if err = lag(); err != nil {
			return r, err
		}
		if err = r.boundary(db, phase+"_growth"); err != nil {
			return r, err
		}
		err = call(phase+"_ordinary", "CommitAt", 1, func() (uint64, error) {
			return 1, s.CommitAt(ts+3, []mvcc.Mutation{{Key: keys[0], Value: c4Value(e)}}, mvcc.CommitRelaxed)
		})
		if err != nil {
			return r, err
		}
		beforeCount := len(oracle[e])
		for start := 0; start < n; start += c4GroupWidth {
			if err = commit(phase+"_replacement", []mvcc.CommitGroup{{Timestamp: ts + 3, Mutations: muts(start, start+c4GroupWidth, e, false)}}); err != nil {
				return r, err
			}
		}
		if err = check(phase+"_oracle", e); err != nil {
			return r, err
		}
		r.OracleReceipts = append(r.OracleReceipts, fmt.Sprintf("%s:history=%d;replacement_adds=0", phase, beforeCount))
		// All concurrent reads use the already committed epoch; replacement writes
		// preserve bytes and timestamps, making every coherent cut oracle exact.
		start := make(chan struct{})
		ready := make(chan struct{}, 3)
		results := make(chan error, 3)
		first := len(r.Calls)
		go func() {
			ready <- struct{}{}
			<-start
			var x error
			for j := 0; j < n/c4GroupWidth && x == nil; j++ {
				x = commit(phase+"_overlap", []mvcc.CommitGroup{{Timestamp: ts + 3, Mutations: muts(j*c4GroupWidth, (j+1)*c4GroupWidth, e, false)}})
			}
			results <- x
		}()
		go func() {
			ready <- struct{}{}
			<-start
			var x error
			for _, key := range keys {
				if x != nil {
					break
				}
				x = call(phase+"_overlap", "GetAt", 1, func() (uint64, error) {
					v, y := s.GetAt(key, ts+3)
					if y == nil && (v.State != mvcc.Present || v.Timestamp != ts+3 || !bytes.Equal(v.Value, c4Value(e))) {
						y = errors.New("overlap point mismatch")
					}
					return 1, y
				})
			}
			results <- x
		}()
		go func() {
			ready <- struct{}{}
			<-start
			results <- call(phase+"_overlap", "IterateVersions.full", 0, func() (uint64, error) { return c4History(s, oracle[e], 0) })
		}()
		// Join startup before releasing real calls. Readiness itself is not an
		// overlapping public operation; only the recorded call intervals count.
		for j := 0; j < 3; j++ {
			<-ready
		}
		close(start)
		for j := 0; j < 3; j++ {
			err = errors.Join(err, <-results)
		}
		if err != nil {
			return r, err
		}
		for _, read := range r.Calls[first:] {
			if read.Operation == "CommitGroupAt" {
				continue
			}
			for _, write := range r.Calls[first:] {
				if write.Operation == "CommitGroupAt" && read.StartNS < write.CompletionNS && write.StartNS < read.CompletionNS {
					r.OverlappingReaders++
					break
				}
			}
		}
		if err = r.boundary(db, phase+"_joined"); err != nil {
			return r, err
		}
		if err = lag(); err != nil {
			return r, err
		}
		checkpointPhase := phase + "_checkpoint"
		if e == epochs {
			checkpointPhase = "pinned_checkpoint"
		}
		if err = call(checkpointPhase, "Checkpoint", 0, func() (uint64, error) { return 0, db.Checkpoint() }); err != nil {
			return r, err
		}
		if err = r.boundary(db, checkpointPhase); err != nil {
			return r, err
		}
		checkpointSeq := db.Stats()["treedb.commit_seq"]
		beforeSeq, x := strconv.ParseUint(backendStart, 10, 64)
		afterSeq, y := strconv.ParseUint(checkpointSeq, 10, 64)
		if x != nil || y != nil || afterSeq < beforeSeq || (mode == "cow_btree" && afterSeq == beforeSeq) {
			return r, errors.New("epoch checkpoint lacks required backend progress")
		}
		if mode == "cow_btree" {
			r.OracleReceipts = append(r.OracleReceipts, phase+":backend_lag=unchanged;checkpoint=advanced")
		} else {
			r.OracleReceipts = append(r.OracleReceipts, phase+":backend_progress=observed;checkpoint=complete")
		}
		// A complete public checkpoint starts a new bounded cached-lag window.
		// The original seed iterator owners stay alive across every window.
		backendStart = checkpointSeq
	}
	if bench != nil {
		bench.StopTimer()
	}
	if mode == "cow_btree" {
		before := db.Stats()
		floor, e := s.DiscardFloor()
		if e != nil {
			return r, e
		}
		err = call("unsupported_prune", "PruneVersions", 0, func() (uint64, error) {
			_, x := s.PruneVersions(mvcc.PruneOptions{Mode: mvcc.CommitDurable})
			if !errors.Is(x, caching.ErrCOWUnsupported) {
				return 0, fmt.Errorf("unsupported prune: %v", x)
			}
			return 0, nil
		})
		if err != nil {
			return r, err
		}
		after := db.Stats()
		for _, key := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total", "treedb.cache.cow.publications_total", "treedb.cache.cow.capture_calls_total"} {
			if before[key] != after[key] {
				return r, fmt.Errorf("refused prune changed %s", key)
			}
		}
		f, x := s.DiscardFloor()
		if x != nil || f != floor {
			return r, errors.New("refused prune changed floor")
		}
		r.OracleReceipts = append(r.OracleReceipts, "unsupported_prune:before_effects")
	}
	if err = lag(); err != nil {
		return r, err
	}
	if mode == "cow_btree" {
		r.OracleReceipts = append(r.OracleReceipts, "backend_lag:unchanged_backend_commit_sequence_with_visible_history", "checkpoint:backend_commit_sequence_advanced")
	} else {
		r.OracleReceipts = append(r.OracleReceipts, "backend_progress:observed_nonregressing_with_visible_history", "checkpoint:completed_public_calls")
	}
	checkpointStats := r.Boundaries[len(r.Boundaries)-1].Stats
	if ptr {
		raw, x := strconv.ParseUint(checkpointStats["treedb.cache.vlog_payload_kind.raw_bytes.single_value"], 10, 64)
		if x != nil || raw == 0 {
			return r, errors.New("forced pointer lacks actual single-value vlog payload observation")
		}
		r.OracleReceipts = append(r.OracleReceipts, "forced_pointer:positive_single_value_vlog_raw_bytes")
	}
	if err = r.proveLayout(db, "checkpoint_layout", oracle[epochs], ptr); err != nil {
		return r, err
	}
	for _, it := range pins {
		if err = call("old_pin_release", "IterateVersions.consume_close", 0, func() (uint64, error) { return c4CheckIterator(it, oracle[0]) }); err != nil {
			return r, err
		}
	}
	pins = nil
	r.OracleReceipts = append(r.OracleReceipts, "old_pins:immutable_seed_after_checkpoint")
	if err = r.boundary(db, "released"); err != nil {
		return r, err
	}
	if err = check("released_oracle", epochs); err != nil {
		return r, err
	}
	if err = call("released_checkpoint", "Checkpoint", 0, func() (uint64, error) { return 0, db.Checkpoint() }); err != nil {
		return r, err
	}
	if err = r.boundary(db, "preclose"); err != nil {
		return r, err
	}
	if err = call("close", "Close", 0, func() (uint64, error) { return 0, db.Close() }); err != nil {
		return r, err
	}
	db = nil
	if err = call("reopen", "Open", 0, func() (uint64, error) { var x error; db, x = treedb.Open(opts); return 1, x }); err != nil {
		return r, err
	}
	if err = validate(); err != nil {
		return r, err
	}
	s = mvcc.New(db)
	// Reopen counters are a new process-local DB authority, so they start a new
	// ledger rather than pretending monotonicity across the Close boundary.
	r.Boundaries = append(r.Boundaries, c4Boundary{Phase: "reopen_counter_reset", Stats: map[string]string{}})
	if err = check("reopen_oracle", epochs); err != nil {
		return r, err
	}
	if err = r.proveLayout(db, "reopen_layout", oracle[epochs], ptr); err != nil {
		return r, err
	}
	if err = r.boundary(db, "reopened"); err != nil {
		return r, err
	}
	r.OracleReceipts = append(r.OracleReceipts, "reopen:complete_point_history_payload")
	if err = call("final_close", "Close", 0, func() (uint64, error) { return 0, db.Close() }); err != nil {
		return r, err
	}
	db = nil
	sort.Slice(r.Calls, func(i, j int) bool { return r.Calls[i].StartNS < r.Calls[j].StartNS })
	if r.OverlappingReaders == 0 {
		r.LifecycleOutcome = "refused"
		r.LifecycleError = errC4NoPublicOverlap.Error()
		return r, errC4NoPublicOverlap
	}
	return r, nil
}
