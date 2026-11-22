// Command raw_cow_txn_sustained measures public durable raw TreeDB transactions.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

const (
	initialDebit = uint64(1 << 60)
	maxOps       = 2_000_000
	maxPairs     = 65_536

	logicalSeq  = "treedb.cache.cow.txn.logical_seq"
	nextLSN     = "treedb.command_wal.next_lsn"
	durableLSN  = "treedb.command_wal.durable_wal_lsn"
	walAppends  = "treedb.command_wal.append.count_total"
	rawAppends  = "treedb.command_wal.append.entry_scan.count_total"
	activeTxns  = "treedb.cache.conditional_txn.active"
	publication = "treedb.cache.cow.publications_total"

	autoCount = "treedb.cache.auto_checkpoint.count"
	autoTime  = "treedb.cache.auto_checkpoint.last_unix_nano"
)

type config struct {
	Dir            string `json:"-"`
	Clients        int    `json:"clients"`
	PairsPerClient int    `json:"pairs_per_client"`
	OpsPerClient   int    `json:"operations_per_client"`
	MaxAttempts    int    `json:"max_attempts"`
	Layout         string `json:"layout"`
	HoldReader     bool   `json:"hold_reader"`
	Phases         bool   `json:"phase_timers"`
}

func (c config) validate() error {
	if c.Clients < 1 || c.Clients > 64 || c.PairsPerClient < 1 ||
		c.PairsPerClient > maxPairs/c.Clients || c.OpsPerClient < 1 ||
		c.OpsPerClient > maxOps/c.Clients || c.MaxAttempts < 1 || c.MaxAttempts > 4096 {
		return errors.New("invalid client, pair, operation, or attempt bound")
	}
	if c.Layout != "disjoint" && c.Layout != "hot" {
		return fmt.Errorf("unsupported layout %q", c.Layout)
	}
	return nil
}

func (c config) pairs() int {
	if c.Layout == "hot" {
		return c.PairsPerClient
	}
	return c.Clients * c.PairsPerClient
}

func profileOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.MemtableMode = "cow_btree"
	return opts
}

func pairKeys(i int) ([]byte, []byte) {
	var debit, credit [9]byte
	binary.BigEndian.PutUint64(debit[:8], uint64(i))
	copy(credit[:], debit[:])
	credit[8] = 1
	return debit[:], credit[:]
}

func seed(db *treedb.DB, pairs int) (err error) {
	b := db.NewBatch()
	defer func() { err = errors.Join(err, b.Close()) }()
	var debit, credit [8]byte
	binary.BigEndian.PutUint64(debit[:], initialDebit)
	for i := range pairs {
		a, z := pairKeys(i)
		if err := b.Set(a, debit[:]); err != nil {
			return err
		}
		if err := b.Set(z, credit[:]); err != nil {
			return err
		}
	}
	return b.WriteSync()
}

type getter func([]byte) ([]byte, error)

func balances(get getter, a, z []byte) (uint64, uint64, error) {
	x, err := get(a)
	if err != nil {
		return 0, 0, err
	}
	y, err := get(z)
	if err != nil {
		return 0, 0, err
	}
	if len(x) != 8 || len(y) != 8 {
		return 0, 0, fmt.Errorf("balance lengths %d/%d, want 8/8", len(x), len(y))
	}
	left, right := binary.BigEndian.Uint64(x), binary.BigEndian.Uint64(y)
	if left > initialDebit || right != initialDebit-left {
		return 0, 0, fmt.Errorf("imbalanced account pair: %d + %d", left, right)
	}
	return left, right, nil
}

func stageValues(tx *treedb.ConditionalTxn, a, z []byte, left, right uint64) error {
	if left == 0 {
		return errors.New("debit exhausted")
	}
	var debit, credit [8]byte
	binary.BigEndian.PutUint64(debit[:], left-1)
	binary.BigEndian.PutUint64(credit[:], right+1)
	if err := tx.Set(a, debit[:]); err != nil {
		return err
	}
	return tx.Set(z, credit[:])
}

func stageTransfer(tx *treedb.ConditionalTxn, debit, credit []byte) error {
	left, right, err := balances(tx.Get, debit, credit)
	if err != nil {
		return err
	}
	return stageValues(tx, debit, credit, left, right)
}

type phaseTotals struct {
	BeginNS  int64 `json:"begin_ns"`
	ReadNS   int64 `json:"read_ns"`
	StageNS  int64 `json:"stage_ns"`
	CommitNS int64 `json:"commit_ns"`
}

func attemptTransfer(db *treedb.DB, a, z []byte, p *phaseTotals) error {
	var start time.Time
	if p != nil {
		start = time.Now()
	}
	tx, err := db.NewConditionalTxn()
	if p != nil {
		p.BeginNS += time.Since(start).Nanoseconds()
	}
	if err != nil {
		// A retryable value is only recognized at the public Commit boundary.
		return fmt.Errorf("begin transaction: %w", err)
	}
	if p == nil {
		err = stageTransfer(tx, a, z)
	} else {
		start = time.Now()
		left, right, readErr := balances(tx.Get, a, z)
		p.ReadNS += time.Since(start).Nanoseconds()
		err = readErr
		if err == nil {
			start = time.Now()
			err = stageValues(tx, a, z, left, right)
			p.StageNS += time.Since(start).Nanoseconds()
		}
	}
	if err != nil {
		return errors.Join(fmt.Errorf("stage transaction: %w", err), tx.Close())
	}
	// Public Commit consumes the handle even when it rejects.
	if p != nil {
		start = time.Now()
	}
	err = tx.Commit()
	if p != nil {
		p.CommitNS += time.Since(start).Nanoseconds()
	}
	return err
}

type clientReceipt struct {
	Offered   uint64       `json:"offered"`
	Completed uint64       `json:"completed"`
	Attempts  uint64       `json:"attempts"`
	Conflicts uint64       `json:"conflicts"`
	TooOld    uint64       `json:"too_old"`
	Phases    *phaseTotals `json:"phases,omitempty"`
	Error     string       `json:"error,omitempty"`
	latencies []int64
}

type receipt struct {
	Schema          int               `json:"schema"`
	Config          config            `json:"config"`
	Profile         string            `json:"profile"`
	Ack             string            `json:"ack"`
	Success         bool              `json:"success"`
	ForegroundStart int64             `json:"foreground_start_unix_ns"`
	ForegroundEnd   int64             `json:"foreground_end_unix_ns"`
	ForegroundNS    int64             `json:"foreground_ns"`
	CheckpointNS    int64             `json:"checkpoint_ns"`
	CloseNS         int64             `json:"close_ns"`
	HeldVerified    bool              `json:"held_verified"`
	Completed       uint64            `json:"completed"`
	Counts          []uint64          `json:"pair_transfers"`
	Clients         []clientReceipt   `json:"clients"`
	P50NS           int64             `json:"p50_ns"`
	P99NS           int64             `json:"p99_ns"`
	P999NS          int64             `json:"p99_9_ns"`
	MaxNS           int64             `json:"max_ns"`
	AllocatedBytes  uint64            `json:"allocated_bytes"`
	AllocationCount uint64            `json:"allocations"`
	HeapBytesStart  uint64            `json:"heap_alloc_before_bytes"`
	HeapBytesEnd    uint64            `json:"heap_alloc_after_bytes"`
	PeakRSSBytes    uint64            `json:"process_peak_rss_bytes,omitempty"`
	StatsBefore     map[string]string `json:"stats_before"`
	StatsAfter      map[string]string `json:"stats_after_foreground"`
	StatsCheckpoint map[string]string `json:"stats_after_checkpoint"`
	AutoInWindow    bool              `json:"auto_completion_in_foreground"`
}

func getCounter(stats map[string]string, key string) (uint64, error) {
	n, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("stat %s=%q: %w", key, stats[key], err)
	}
	return n, nil
}

func receiptStats(db *treedb.DB) map[string]string {
	filtered := make(map[string]string)
	for k, v := range db.Stats() {
		switch {
		case k == activeTxns, k == "treedb.cache.memtable_mode",
			strings.HasPrefix(k, "treedb.cache.cow."),
			strings.HasPrefix(k, "treedb.cache.auto_checkpoint."),
			strings.HasPrefix(k, "treedb.command_wal."),
			strings.HasPrefix(k, "treedb.cache.checkpoint."):
			filtered[k] = v
		}
	}
	return filtered
}

func proveCommitAccounting(before, after map[string]string, count uint64) error {
	if before["treedb.cache.memtable_mode"] != "cow_btree" ||
		after["treedb.cache.memtable_mode"] != "cow_btree" {
		return errors.New("did not activate the intended cow_btree route")
	}
	deltas := make(map[string]uint64)
	for _, k := range []string{logicalSeq, publication, rawAppends, nextLSN, walAppends} {
		a, errA := getCounter(before, k)
		z, errZ := getCounter(after, k)
		if err := errors.Join(errA, errZ); err != nil {
			return err
		}
		if z < a {
			return fmt.Errorf("%s regressed %d to %d", k, a, z)
		}
		deltas[k] = z - a
	}
	for _, k := range []string{logicalSeq, publication, rawAppends} {
		if deltas[k] != count {
			return fmt.Errorf("%s advanced %d, want %d accepted commits", k, deltas[k], count)
		}
	}
	// The physical clock also counts root-neutral durable-prefix barriers.
	// Those frames are not mutation frames or logical transactions.
	if deltas[nextLSN] != deltas[walAppends] || deltas[nextLSN] < count {
		return fmt.Errorf("WAL next-LSN advance %d, total appends %d, raw appends %d", deltas[nextLSN], deltas[walAppends], count)
	}
	d, errD := getCounter(after, durableLSN)
	n, errN := getCounter(after, nextLSN)
	active, errA := getCounter(after, activeTxns)
	if err := errors.Join(errD, errN, errA); err != nil {
		return err
	}
	if n == 0 || d != n-1 || active != 0 {
		return fmt.Errorf("unexpected durable/next LSN %d/%d or active handles %d", d, n, active)
	}
	return nil
}

func auditBalances(get getter, expected []uint64) error {
	for i, transfers := range expected {
		a, z := pairKeys(i)
		left, right, err := balances(get, a, z)
		if err != nil {
			return fmt.Errorf("pair %d: %w", i, err)
		}
		if transfers > initialDebit || right != transfers || left != initialDebit-transfers {
			return fmt.Errorf("pair %d has %d/%d, want credit %d", i, left, right, transfers)
		}
	}
	return nil
}

func audit(db *treedb.DB, expected []uint64) (err error) {
	if err := auditBalances(db.Get, expected); err != nil {
		return err
	}
	it, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	count := 0
	for ; it.Valid(); it.Next() {
		count++
		if count > 2*len(expected) {
			return errors.New("unexpected keys in fixture")
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if count != 2*len(expected) {
		return fmt.Errorf("fixture key count %d, want %d", count, 2*len(expected))
	}
	return nil
}

func reopenAudit(dir string, expected []uint64) (err error) {
	if len(expected) == 0 || len(expected) > maxPairs {
		return errors.New("invalid reopen receipt pair count")
	}
	// COW rejects ReadOnly at Open. Use the same supported COW route and
	// issue reads only, then check the original Close on this fresh owner.
	db, err := treedb.Open(profileOptions(dir))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	return audit(db, expected)
}

func observedAuto(out receipt) bool {
	before, err1 := getCounter(out.StatsBefore, autoCount)
	after, err2 := getCounter(out.StatsAfter, autoCount)
	completed, err3 := getCounter(out.StatsAfter, autoTime)
	return errors.Join(err1, err2, err3) == nil && after > before &&
		completed <= math.MaxInt64 && int64(completed) > out.ForegroundStart &&
		int64(completed) < out.ForegroundEnd
}

func peakRSS() uint64 {
	// Optional Linux process-wide peak, including setup and retained benchmark buffers.
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if s, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			f := strings.Fields(s)
			if len(f) == 2 && f[1] == "kB" {
				n, _ := strconv.ParseUint(f[0], 10, 64)
				return 1024 * n
			}
			break
		}
	}
	return 0
}

func percentile(sorted []int64, thousandths int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[(len(sorted)*thousandths+999)/1000-1]
}

func run(ctx context.Context, c config) (out receipt, err error) {
	out = receipt{Schema: 1, Config: c, Profile: string(treedb.ProfileCommandWALDurable), Ack: "ordinary-durable-commit"}
	if err := c.validate(); err != nil {
		return out, err
	}
	if c.Dir == "" {
		return out, errors.New("missing fresh database directory")
	}
	if err := os.Mkdir(c.Dir, 0700); err != nil {
		return out, fmt.Errorf("create a new database directory: %w", err)
	}
	db, err := treedb.Open(profileOptions(c.Dir))
	if err != nil {
		return out, err
	}
	var held treedb.Snapshot
	defer func() {
		if held != nil {
			err = errors.Join(err, held.Close())
		}
		if db != nil {
			err = errors.Join(err, db.Close())
		}
		out.Success = err == nil
	}()
	if err := seed(db, c.pairs()); err != nil {
		return out, fmt.Errorf("seed: %w", err)
	}
	if err := db.Checkpoint(); err != nil {
		return out, fmt.Errorf("seed checkpoint: %w", err)
	}
	if c.HoldReader {
		held = db.AcquireSnapshot()
		if held == nil {
			return out, errors.New("could not capture independent held reader")
		}
	}

	type keys struct{ a, z []byte }
	pairs := make([]keys, c.pairs())
	for i := range pairs {
		pairs[i].a, pairs[i].z = pairKeys(i)
	}
	out.Clients = make([]clientReceipt, c.Clients)
	out.Counts = make([]uint64, c.pairs())
	for i := range out.Clients {
		out.Clients[i].latencies = make([]int64, 0, c.OpsPerClient)
		if c.Phases {
			out.Clients[i].Phases = &phaseTotals{}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ready, joined sync.WaitGroup
	ready.Add(c.Clients)
	joined.Add(c.Clients)
	start := make(chan struct{})
	for client := range c.Clients {
		go func() {
			defer joined.Done()
			cr := &out.Clients[client]
			fail := func(err error) { cr.Error = err.Error(); cancel() }
			base := 0
			if c.Layout == "disjoint" {
				base = client * c.PairsPerClient
			}
			ready.Done()
			<-start
			for i := range c.OpsPerClient {
				if e := ctx.Err(); e != nil {
					fail(e)
					return
				}
				pair := pairs[base+i%c.PairsPerClient]
				cr.Offered++
				started := time.Now()
				for n := range c.MaxAttempts {
					cr.Attempts++
					e := attemptTransfer(db, pair.a, pair.z, cr.Phases)
					switch e {
					case nil:
						cr.Completed++
						cr.latencies = append(cr.latencies, time.Since(started).Nanoseconds())
					case treedb.ErrConcurrentModification:
						cr.Conflicts++
					case treedb.ErrTxnTooOld:
						cr.TooOld++
					default:
						fail(e)
						return
					}
					if e == nil {
						break
					}
					if n+1 == c.MaxAttempts {
						fail(fmt.Errorf("exhausted %d attempts: %w", c.MaxAttempts, e))
						return
					}
					if e := ctx.Err(); e != nil {
						fail(e)
						return
					}
					runtime.Gosched()
				}
			}
		}()
	}
	ready.Wait()
	out.StatsBefore = receiptStats(db)
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	begin := time.Now()
	out.ForegroundStart = begin.UnixNano()
	close(start)
	joined.Wait()
	end := time.Now()
	out.ForegroundEnd, out.ForegroundNS = end.UnixNano(), end.Sub(begin).Nanoseconds()
	runtime.ReadMemStats(&memAfter)
	out.AllocatedBytes = memAfter.TotalAlloc - memBefore.TotalAlloc
	out.AllocationCount = memAfter.Mallocs - memBefore.Mallocs
	out.HeapBytesStart, out.HeapBytesEnd = memBefore.HeapAlloc, memAfter.HeapAlloc
	out.PeakRSSBytes = peakRSS()
	out.StatsAfter = receiptStats(db)
	out.AutoInWindow = observedAuto(out)

	latencies := make([]int64, 0, c.Clients*c.OpsPerClient)
	var failures []error
	for client, r := range out.Clients {
		out.Completed += r.Completed
		base := 0
		if c.Layout == "disjoint" {
			base = client * c.PairsPerClient
		}
		for i := uint64(0); i < r.Completed; i++ {
			out.Counts[base+int(i)%c.PairsPerClient]++
		}
		latencies = append(latencies, r.latencies...)
		if r.Error != "" {
			failures = append(failures, fmt.Errorf("client %d: %s", client, r.Error))
		}
		if r.Completed != uint64(c.OpsPerClient) || r.Offered != r.Completed ||
			r.Attempts != r.Completed+r.Conflicts+r.TooOld {
			failures = append(failures, fmt.Errorf("client %d incomplete or inconsistent accounting", client))
		}
	}
	slices.Sort(latencies)
	out.P50NS, out.P99NS, out.P999NS, out.MaxNS = percentile(latencies, 500), percentile(latencies, 990), percentile(latencies, 999), percentile(latencies, 1000)
	if err := errors.Join(failures...); err != nil {
		return out, err
	}
	if err := proveCommitAccounting(out.StatsBefore, out.StatsAfter, out.Completed); err != nil {
		return out, err
	}

	beforeCheckpoint := time.Now()
	err = db.Checkpoint()
	out.CheckpointNS = time.Since(beforeCheckpoint).Nanoseconds()
	if err != nil {
		return out, fmt.Errorf("terminal checkpoint: %w", err)
	}
	out.StatsCheckpoint = receiptStats(db)
	if err := audit(db, out.Counts); err != nil {
		return out, fmt.Errorf("live audit: %w", err)
	}
	if held != nil {
		if err := auditBalances(held.Get, make([]uint64, c.pairs())); err != nil {
			return out, fmt.Errorf("held cut: %w", err)
		}
		closeErr := held.Close()
		held = nil
		if closeErr != nil {
			return out, fmt.Errorf("held close: %w", closeErr)
		}
		out.HeldVerified = true
	}
	beforeClose := time.Now()
	err = db.Close()
	db = nil
	out.CloseNS = time.Since(beforeClose).Nanoseconds()
	return out, err
}

func readReceipt(path string) (receipt, error) {
	file, err := os.Open(path)
	if err != nil {
		return receipt{}, err
	}
	defer file.Close()
	var r receipt
	err = json.NewDecoder(file).Decode(&r)
	if err != nil {
		return r, err
	}
	if err := r.Config.validate(); err != nil {
		return r, err
	}
	var total uint64
	for _, n := range r.Counts {
		if n > maxOps {
			return r, errors.New("invalid transfer count in receipt")
		}
		total += n
	}
	if r.Schema != 1 || !r.Success || r.Config.pairs() != len(r.Counts) ||
		r.Completed != uint64(r.Config.Clients*r.Config.OpsPerClient) || total != r.Completed {
		return r, errors.New("not a complete successful transfer receipt")
	}
	return r, nil
}

func command() error {
	var c config
	flag.StringVar(&c.Dir, "dir", "", "fresh database directory (existing database directory for -audit-receipt)")
	flag.IntVar(&c.Clients, "clients", 1, "number of concurrent clients")
	flag.IntVar(&c.PairsPerClient, "pairs", 128, "distinct pair slots per client (shared across clients in hot layout)")
	flag.IntVar(&c.OpsPerClient, "ops", 1000, "fixed logical transfers per client")
	flag.IntVar(&c.MaxAttempts, "max-attempts", 512, "attempt bound per logical transfer")
	flag.StringVar(&c.Layout, "layout", "disjoint", "disjoint or hot")
	flag.BoolVar(&c.HoldReader, "hold-reader", false, "keep an independent cut through foreground and checked checkpoint")
	flag.BoolVar(&c.Phases, "phases", false, "record client phase diagnostics, not throughput comparison")
	prior := flag.String("audit-receipt", "", "fresh-process reopen audit of this run receipt")
	flag.Parse()
	if *prior != "" {
		if c.Dir == "" {
			return errors.New("missing audit database directory")
		}
		r, err := readReceipt(*prior)
		if err != nil {
			return err
		}
		if err := reopenAudit(c.Dir, r.Counts); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema": 1, "reopen_verified": true, "pairs": len(r.Counts), "transfers": r.Completed,
		})
	}
	r, err := run(context.Background(), c)
	return errors.Join(err, json.NewEncoder(os.Stdout).Encode(r))
}

func main() {
	if err := command(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
