package mvcctest

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

//go:embed testdata/public_trace_v1.json
var goldenTraceJSON []byte

type traceMutation struct {
	KeyHex   string `json:"key_hex"`
	ValueHex string `json:"value_hex"`
	Delete   bool   `json:"delete"`
}

type traceCommit struct {
	Timestamp uint64          `json:"timestamp"`
	Mutations []traceMutation `json:"mutations"`
}

type traceRead struct {
	KeyHex        string `json:"key_hex"`
	ReadTimestamp uint64 `json:"read_timestamp"`
	State         string `json:"state"`
	Version       uint64 `json:"version"`
	ValueHex      string `json:"value_hex"`
}

type tracePrune struct {
	Visited  uint64 `json:"visited"`
	Skipped  uint64 `json:"skipped"`
	Retained uint64 `json:"retained"`
	Pruned   uint64 `json:"pruned"`
}

type goldenTrace struct {
	Schema              string        `json:"schema"`
	Commits             []traceCommit `json:"commits"`
	ReadsBeforeFloor    []traceRead   `json:"reads_before_floor"`
	VersionsBeforeFloor []string      `json:"versions_before_floor"`
	DiscardFloor        uint64        `json:"discard_floor"`
	Prune               tracePrune    `json:"prune"`
	ReadsAfterPrune     []traceRead   `json:"reads_after_prune"`
	VersionsAfterPrune  []string      `json:"versions_after_prune"`
}

// Run executes the reusable public-surface conformance suite.
func Run(t *testing.T, open OpenFunc) {
	t.Helper()
	if open == nil {
		t.Fatal("mvcctest: nil OpenFunc")
	}
	t.Run("golden_reopen_floor_prune", func(t *testing.T) { runGolden(t, open) })
	for _, durability := range []DurabilityClass{DurabilityWALOnRelaxed, DurabilityWALOffRelaxed} {
		durability := durability
		t.Run(string(durability), func(t *testing.T) {
			t.Run("validation_boundaries", func(t *testing.T) { runBoundaries(t, open, durability) })
			t.Run("iterator_options_seek_stats_ownership", func(t *testing.T) { runIteratorSurface(t, open, durability) })
			t.Run("randomized_oracle", func(t *testing.T) { runRandomized(t, open, durability) })
			t.Run("concurrent_snapshot_readers", func(t *testing.T) { runConcurrent(t, open, durability) })
		})
	}
}

func runGolden(t *testing.T, open OpenFunc) {
	var trace goldenTrace
	if err := json.Unmarshal(goldenTraceJSON, &trace); err != nil {
		t.Fatalf("decode golden trace: %v", err)
	}
	if trace.Schema != "treedb-mvcc-public-trace-v1" {
		t.Fatalf("golden schema=%q", trace.Schema)
	}
	dir := t.TempDir()
	adapter := requireOpen(t, open, dir, DurabilityDurable)
	for _, commit := range trace.Commits {
		mutations := make([]mvcc.Mutation, len(commit.Mutations))
		for i, mutation := range commit.Mutations {
			mutations[i] = mvcc.Mutation{
				Key:    decodeHex(t, mutation.KeyHex),
				Value:  decodeHex(t, mutation.ValueHex),
				Delete: mutation.Delete,
			}
		}
		if err := adapter.CommitAt(commit.Timestamp, mutations, mvcc.CommitDurable); err != nil {
			t.Fatalf("CommitAt(%d): %v", commit.Timestamp, err)
		}
	}
	requireReads(t, adapter, trace.ReadsBeforeFloor)
	requireVersions(t, adapter, trace.VersionsBeforeFloor)
	requireClose(t, adapter)

	adapter = requireOpen(t, open, dir, DurabilityDurable)
	requireReads(t, adapter, trace.ReadsBeforeFloor)
	requireVersions(t, adapter, trace.VersionsBeforeFloor)
	if err := adapter.AdvanceDiscardFloor(trace.DiscardFloor, mvcc.CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	stats, err := adapter.PruneVersions(mvcc.PruneOptions{BatchSize: 2, Mode: mvcc.CommitDurable})
	if err != nil {
		t.Fatalf("PruneVersions: %v", err)
	}
	if stats.Visited != trace.Prune.Visited || stats.Skipped != trace.Prune.Skipped ||
		stats.Retained != trace.Prune.Retained || stats.Pruned != trace.Prune.Pruned {
		t.Fatalf("prune stats=%+v want=%+v", stats, trace.Prune)
	}
	if stats.Visited != stats.Retained+stats.Pruned || stats.Skipped > stats.Retained {
		t.Fatalf("prune accounting=%+v", stats)
	}
	for _, timestamp := range []uint64{1, trace.DiscardFloor} {
		if _, err := adapter.GetAt([]byte("any"), timestamp); !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
			t.Fatalf("GetAt at/below floor timestamp=%d err=%v", timestamp, err)
		}
	}
	requireReads(t, adapter, trace.ReadsAfterPrune)
	requireVersions(t, adapter, trace.VersionsAfterPrune)
	requireClose(t, adapter)

	adapter = requireOpen(t, open, dir, DurabilityDurable)
	floor, err := adapter.DiscardFloor()
	if err != nil || floor != trace.DiscardFloor {
		t.Fatalf("reopened floor=%d err=%v want=%d", floor, err, trace.DiscardFloor)
	}
	requireReads(t, adapter, trace.ReadsAfterPrune)
	requireVersions(t, adapter, trace.VersionsAfterPrune)
	requireClose(t, adapter)
}

func runBoundaries(t *testing.T, open OpenFunc, durability DurabilityClass) {
	adapter := requireOpen(t, open, t.TempDir(), durability)
	defer requireClose(t, adapter)
	if err := adapter.CommitAt(0, []mvcc.Mutation{{Key: []byte("k")}}, mvcc.CommitRelaxed); !errors.Is(err, mvcc.ErrZeroTimestamp) {
		t.Fatalf("zero timestamp err=%v", err)
	}
	if _, err := adapter.GetAt([]byte("k"), 0); !errors.Is(err, mvcc.ErrZeroTimestamp) {
		t.Fatalf("zero read timestamp err=%v", err)
	}
	if err := adapter.CommitAt(1, []mvcc.Mutation{{Key: nil}, {Key: []byte{}}}, mvcc.CommitRelaxed); !errors.Is(err, mvcc.ErrDuplicateKey) {
		t.Fatalf("nil/empty duplicate err=%v", err)
	}
	if err := adapter.CommitAt(1, []mvcc.Mutation{{Key: []byte("k")}}, mvcc.CommitDurable); err != nil {
		t.Fatalf("explicit durable boundary on relaxed ordinary-ACK profile: %v", err)
	}
	if err := adapter.CommitAt(5, []mvcc.Mutation{{Key: []byte("k"), Value: []byte("v")}}, mvcc.CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := adapter.AdvanceDiscardFloor(5, mvcc.CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := adapter.AdvanceDiscardFloor(4, mvcc.CommitRelaxed); !errors.Is(err, mvcc.ErrDiscardFloorRegression) {
		t.Fatalf("floor regression err=%v", err)
	}
	if err := adapter.CommitAt(5, []mvcc.Mutation{{Key: []byte("late")}}, mvcc.CommitRelaxed); !errors.Is(err, mvcc.ErrVersionBelowDiscardFloor) {
		t.Fatalf("commit at floor err=%v", err)
	}
}

func runIteratorSurface(t *testing.T, open OpenFunc, durability DurabilityClass) {
	adapter := requireOpen(t, open, t.TempDir(), durability)
	defer requireClose(t, adapter)
	type mutationAt struct {
		key       string
		timestamp uint64
		value     string
		deleted   bool
	}
	for _, mutation := range []mutationAt{
		{key: "a", timestamp: 10, value: "a10"},
		{key: "a", timestamp: 20, deleted: true},
		{key: "a", timestamp: 30, value: "a30"},
		{key: "b", timestamp: 5, value: "b5"},
		{key: "b", timestamp: 25, value: "b25"},
		{key: "ba", timestamp: 15, value: "ba15"},
		{key: "c", timestamp: 1, value: "c1"},
	} {
		err := adapter.CommitAt(mutation.timestamp, []mvcc.Mutation{{
			Key: []byte(mutation.key), Value: []byte(mutation.value), Delete: mutation.deleted,
		}}, mvcc.CommitRelaxed)
		if err != nil {
			t.Fatalf("CommitAt(%q,%d): %v", mutation.key, mutation.timestamp, err)
		}
	}

	forward, err := adapter.IterateVersions(mvcc.VersionIteratorOptions{
		LowerBound: []byte("a"), UpperBound: []byte("c"),
	})
	if err != nil {
		t.Fatalf("forward bounded iterator: %v", err)
	}
	requireIteratorLabels(t, forward, []string{
		"61@30:present:613330", "61@20:tombstone:", "61@10:present:613130",
		"62@25:present:623235", "62@5:present:6235", "6261@15:present:62613135",
	})

	reverse, err := adapter.IterateVersions(mvcc.VersionIteratorOptions{
		Prefix: []byte("b"), ReadTimestamp: 20, Reverse: true,
	})
	if err != nil {
		t.Fatalf("reverse prefix iterator: %v", err)
	}
	stats := requireIteratorLabels(t, reverse, []string{
		"6261@15:present:62613135", "62@5:present:6235",
	})
	if stats.Visited != 3 || stats.Skipped != 1 || stats.Retained != 2 || stats.Visited != stats.Skipped+stats.Retained {
		t.Fatalf("reverse iterator stats=%+v", stats)
	}

	seek, err := adapter.IterateVersions(mvcc.VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("forward seek iterator: %v", err)
	}
	seek.Seek([]byte("b"), 20)
	if !seek.Valid() {
		t.Fatalf("forward Seek invalid: %v", seek.Error())
	}
	owned := seek.Entry()
	if string(owned.Key) != "b" || owned.Timestamp != 5 || string(owned.Value) != "b5" {
		t.Fatalf("forward Seek entry=%+v", owned)
	}
	seek.Next()
	if string(owned.Key) != "b" || owned.Timestamp != 5 || string(owned.Value) != "b5" {
		t.Fatalf("Entry bytes changed after Next: %+v", owned)
	}
	seek.Seek([]byte("b"), 20)
	mutableCopy := seek.Entry()
	mutableCopy.Key[0], mutableCopy.Value[0] = 'x', 'x'
	seek.Seek([]byte("b"), 20)
	again := seek.Entry()
	if string(again.Key) != "b" || again.Timestamp != 5 || string(again.Value) != "b5" {
		t.Fatalf("Entry bytes were not caller-owned: %+v", again)
	}
	if err := seek.Close(); err != nil {
		t.Fatalf("close forward seek iterator: %v", err)
	}
	if string(owned.Key) != "b" || owned.Timestamp != 5 || string(owned.Value) != "b5" {
		t.Fatalf("Entry bytes changed after Close: %+v", owned)
	}

	reverseSeek, err := adapter.IterateVersions(mvcc.VersionIteratorOptions{Reverse: true})
	if err != nil {
		t.Fatalf("reverse seek iterator: %v", err)
	}
	reverseSeek.Seek([]byte("b"), 20)
	if !reverseSeek.Valid() {
		t.Fatalf("reverse Seek invalid: %v", reverseSeek.Error())
	}
	reverseEntry := reverseSeek.Entry()
	if string(reverseEntry.Key) != "b" || reverseEntry.Timestamp != 25 || string(reverseEntry.Value) != "b25" {
		t.Fatalf("reverse Seek entry=%+v", reverseEntry)
	}
	if err := reverseSeek.Close(); err != nil {
		t.Fatalf("close reverse seek iterator: %v", err)
	}
}

type oracleVersion struct {
	timestamp uint64
	state     mvcc.ReadState
	value     []byte
}

func runRandomized(t *testing.T, open OpenFunc, durability DurabilityClass) {
	adapter := requireOpen(t, open, t.TempDir(), durability)
	defer requireClose(t, adapter)
	rng := rand.New(rand.NewSource(3673))
	keys := [][]byte{nil, {0}, {0, 'a'}, {'a'}, {'a', 0, 'b'}, {0xff}, {0xff, 0, 1}}
	history := make(map[string][]oracleVersion, len(keys))
	for step := 1; step <= 320; step++ {
		key := keys[rng.Intn(len(keys))]
		deleted := rng.Intn(5) == 0
		value := []byte{byte(step), byte(step >> 8), byte(rng.Intn(256))}
		if rng.Intn(9) == 0 {
			value = []byte{}
		}
		if err := adapter.CommitAt(uint64(step), []mvcc.Mutation{{Key: key, Value: value, Delete: deleted}}, mvcc.CommitRelaxed); err != nil {
			t.Fatalf("step %d CommitAt: %v", step, err)
		}
		state := mvcc.Present
		if deleted {
			state = mvcc.Tombstone
			value = nil
		}
		history[string(key)] = append(history[string(key)], oracleVersion{timestamp: uint64(step), state: state, value: append([]byte(nil), value...)})
		for probe := 0; probe < 3; probe++ {
			probeKey := keys[rng.Intn(len(keys))]
			readTimestamp := uint64(1 + rng.Intn(step))
			want := oracleAt(history[string(probeKey)], readTimestamp)
			got, err := adapter.GetAt(probeKey, readTimestamp)
			if err != nil {
				t.Fatalf("step %d GetAt(%x,%d): %v", step, probeKey, readTimestamp, err)
			}
			if got.State != want.state || got.Timestamp != want.timestamp || !bytes.Equal(got.Value, want.value) {
				t.Fatalf("step %d GetAt(%x,%d)=%+v want=%+v", step, probeKey, readTimestamp, got, want)
			}
		}
	}

	want := make([]string, 0, 320)
	for key, versions := range history {
		for i := len(versions) - 1; i >= 0; i-- {
			want = append(want, versionLabel([]byte(key), versions[i].timestamp, versions[i].state, versions[i].value))
		}
	}
	sort.Slice(want, func(i, j int) bool {
		leftKey, leftTimestamp := labelIdentity(want[i])
		rightKey, rightTimestamp := labelIdentity(want[j])
		if cmp := bytes.Compare(leftKey, rightKey); cmp != 0 {
			return cmp < 0
		}
		return leftTimestamp > rightTimestamp
	})
	requireVersions(t, adapter, want)
}

func runConcurrent(t *testing.T, open OpenFunc, durability DurabilityClass) {
	adapter := requireOpen(t, open, t.TempDir(), durability)
	defer requireClose(t, adapter)
	for timestamp := uint64(1); timestamp <= 16; timestamp++ {
		if err := adapter.CommitAt(timestamp, []mvcc.Mutation{{Key: []byte("k"), Value: []byte(fmt.Sprint(timestamp))}}, mvcc.CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	pinned, err := adapter.IterateVersions(mvcc.VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("open pinned iterator: %v", err)
	}
	pinnedClosed := false
	defer func() {
		if !pinnedClosed {
			_ = pinned.Close()
		}
	}()

	// Prove one post-barrier read/commit overlap at the public Adapter call
	// boundary. The wrappers make both invocations enter before either may call
	// through and return, so this is independent of which goroutine the
	// scheduler runs first and does not require backend-specific hooks.
	originalCommitAt := adapter.CommitAt
	originalGetAt := adapter.GetAt
	proofCommitCall := make(chan struct{})
	proofReadCall := make(chan struct{})
	proofAbort := make(chan struct{})
	var proofAbortOnce sync.Once
	var proofEnabled atomic.Bool
	var proofReadClaimed atomic.Bool
	var proofCommitEntered atomic.Bool
	var proofReadEntered atomic.Bool
	abortProof := func() { proofAbortOnce.Do(func() { close(proofAbort) }) }
	waitForProofPeer := func(peer <-chan struct{}) error {
		select {
		case <-peer:
			return nil
		case <-proofAbort:
			return errors.New("mvcctest: concurrent overlap proof aborted")
		}
	}
	adapter.CommitAt = func(timestamp uint64, mutations []mvcc.Mutation, mode mvcc.CommitMode) error {
		if !proofEnabled.Load() || timestamp != 18 {
			return originalCommitAt(timestamp, mutations, mode)
		}
		proofCommitEntered.Store(true)
		close(proofCommitCall)
		if err := waitForProofPeer(proofReadCall); err != nil {
			return err
		}
		return originalCommitAt(timestamp, mutations, mode)
	}
	adapter.GetAt = func(key []byte, timestamp uint64) (mvcc.Result, error) {
		if !proofEnabled.Load() || !proofReadClaimed.CompareAndSwap(false, true) {
			return originalGetAt(key, timestamp)
		}
		proofReadEntered.Store(true)
		close(proofReadCall)
		if err := waitForProofPeer(proofCommitCall); err != nil {
			return mvcc.Result{}, err
		}
		return originalGetAt(key, timestamp)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 9)
	reportError := func(err error) {
		abortProof()
		errCh <- err
	}
	start := make(chan struct{})
	writerStarted := make(chan struct{})
	overlap := make(chan struct{})
	readerProgress := make(chan struct{}, 8)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := adapter.CommitAt(17, []mvcc.Mutation{{Key: []byte("k"), Value: []byte("17")}}, mvcc.CommitRelaxed); err != nil {
			reportError(err)
			close(writerStarted)
			return
		}
		close(writerStarted)
		<-overlap
		for timestamp := uint64(18); timestamp <= 80; timestamp++ {
			if err := adapter.CommitAt(timestamp, []mvcc.Mutation{{Key: []byte("k"), Value: []byte(fmt.Sprint(timestamp))}}, mvcc.CommitRelaxed); err != nil {
				reportError(err)
				return
			}
		}
	}()
	for reader := 0; reader < 8; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			<-writerStarted
			for i := 0; i < 100; i++ {
				got, err := adapter.GetAt([]byte("k"), 80)
				if err != nil || got.State != mvcc.Present || got.Timestamp < 1 || got.Timestamp > 80 || string(got.Value) != fmt.Sprint(got.Timestamp) {
					reportError(fmt.Errorf("concurrent GetAt result=%+v err=%v", got, err))
					return
				}
				if i == 0 {
					readerProgress <- struct{}{}
					<-overlap
				}
			}
		}()
	}
	close(start)
	<-writerStarted
	var barrierErr error
	for reader := 0; reader < 8; reader++ {
		select {
		case <-readerProgress:
		case err := <-errCh:
			barrierErr = fmt.Errorf("reader failed before overlap barrier: %w", err)
		}
		if barrierErr != nil {
			break
		}
	}
	if barrierErr == nil {
		proofEnabled.Store(true)
	}
	close(overlap)
	wg.Wait()
	abortProof()
	close(errCh)
	if barrierErr != nil {
		t.Fatal(barrierErr)
	}
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !proofCommitEntered.Load() || !proofReadEntered.Load() {
		t.Fatalf("post-barrier Adapter calls did not overlap: commit=%t read=%t", proofCommitEntered.Load(), proofReadEntered.Load())
	}

	var pinnedLabels []string
	for pinned.Valid() {
		entry := pinned.Entry()
		pinnedLabels = append(pinnedLabels, versionLabel(entry.Key, entry.Timestamp, entry.State, entry.Value))
		pinned.Next()
	}
	if err := pinned.Error(); err != nil {
		t.Fatalf("pinned iterator: %v", err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatalf("close pinned iterator: %v", err)
	}
	pinnedClosed = true
	if len(pinnedLabels) != 16 {
		t.Fatalf("pinned labels len=%d want=16", len(pinnedLabels))
	}
	if pinnedLabels[0] != "6b@16:present:3136" || pinnedLabels[15] != "6b@1:present:31" {
		t.Fatalf("pinned labels len=%d first=%q last=%q", len(pinnedLabels), pinnedLabels[0], pinnedLabels[len(pinnedLabels)-1])
	}
	fresh, err := adapter.GetAt([]byte("k"), 80)
	if err != nil || fresh.State != mvcc.Present || fresh.Timestamp != 80 || string(fresh.Value) != "80" {
		t.Fatalf("fresh GetAt=%+v err=%v", fresh, err)
	}
}

func requireOpen(t testing.TB, open OpenFunc, dir string, durability DurabilityClass) Adapter {
	t.Helper()
	adapter, err := open(dir, durability)
	if err != nil {
		t.Fatalf("open %s: %v", durability, err)
	}
	if err := adapter.validate(); err != nil {
		_ = closeAdapter(adapter)
		t.Fatal(err)
	}
	return adapter
}

func requireClose(t testing.TB, adapter Adapter) {
	t.Helper()
	if err := closeAdapter(adapter); err != nil {
		t.Fatalf("close adapter: %v", err)
	}
}

func requireReads(t testing.TB, adapter Adapter, reads []traceRead) {
	t.Helper()
	for _, read := range reads {
		key := decodeHex(t, read.KeyHex)
		wantState := parseState(t, read.State)
		wantValue := decodeHex(t, read.ValueHex)
		got, err := adapter.GetAt(key, read.ReadTimestamp)
		if err != nil {
			t.Fatalf("GetAt(%x,%d): %v", key, read.ReadTimestamp, err)
		}
		if got.State != wantState || got.Timestamp != read.Version || !bytes.Equal(got.Value, wantValue) {
			t.Fatalf("GetAt(%x,%d)=%+v want state=%v timestamp=%d value=%x", key, read.ReadTimestamp, got, wantState, read.Version, wantValue)
		}
	}
}

func requireVersions(t testing.TB, adapter Adapter, want []string) {
	t.Helper()
	it, err := adapter.IterateVersions(mvcc.VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("IterateVersions: %v", err)
	}
	requireIteratorLabels(t, it, want)
}

func requireIteratorLabels(t testing.TB, it Iterator, want []string) mvcc.VersionIteratorStats {
	t.Helper()
	var got []string
	for it.Valid() {
		entry := it.Entry()
		got = append(got, versionLabel(entry.Key, entry.Timestamp, entry.State, entry.Value))
		it.Next()
	}
	stats := it.Stats()
	iterErr := it.Error()
	closeErr := it.Close()
	if iterErr != nil || closeErr != nil {
		t.Fatalf("iterate versions err=%v close=%v", iterErr, closeErr)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("versions:\n got %v\nwant %v", got, want)
	}
	return stats
}

func oracleAt(versions []oracleVersion, timestamp uint64) oracleVersion {
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].timestamp <= timestamp {
			return versions[i]
		}
	}
	return oracleVersion{state: mvcc.Absent}
}

func versionLabel(key []byte, timestamp uint64, state mvcc.ReadState, value []byte) string {
	return fmt.Sprintf("%x@%d:%s:%x", key, timestamp, stateLabel(state), value)
}

func labelIdentity(label string) ([]byte, uint64) {
	at := strings.IndexByte(label, '@')
	colon := strings.IndexByte(label[at+1:], ':') + at + 1
	keyHex := label[:at]
	timestamp, _ := strconv.ParseUint(label[at+1:colon], 10, 64)
	key, _ := hex.DecodeString(keyHex)
	return key, timestamp
}

func stateLabel(state mvcc.ReadState) string {
	switch state {
	case mvcc.Absent:
		return "absent"
	case mvcc.Present:
		return "present"
	case mvcc.Tombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("state(%d)", state)
	}
}

func parseState(t testing.TB, label string) mvcc.ReadState {
	t.Helper()
	switch label {
	case "absent":
		return mvcc.Absent
	case "present":
		return mvcc.Present
	case "tombstone":
		return mvcc.Tombstone
	default:
		t.Fatalf("unknown state %q", label)
		return mvcc.Absent
	}
}

func decodeHex(t testing.TB, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode hex %q: %v", value, err)
	}
	return decoded
}
