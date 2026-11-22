package treedb

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	rawTxnQueuedCrashRootEnv     = "TREEDB_TEST_RAW_TXN_QUEUED_CRASH_ROOT"
	rawTxnQueuedCrashReceiptName = "acknowledged-queued-pair.json"
)

var rawTxnQueuedCrashStems = [...]string{"seed-one", "seed-two", "seed-extra", "first", "follower"}

type rawTxnQueuedCrashReceipt struct {
	Seal                       rootpublication.SealedPrefix
	Frontier                   rootpublication.LogicalFrontiers
	Applied, Next, Checkpoints uint64
	Revisions                  map[string]EntryRevision
}

func (r rawTxnQueuedCrashReceipt) check(t *testing.T) {
	t.Helper()
	if r.Checkpoints != 1 || r.Frontier.S != r.Seal || r.Seal.CanonicalLSN == 0 ||
		r.Applied != r.Seal.CanonicalLSN || r.Frontier.V != r.Seal.LogicalSequence+2 ||
		r.Frontier.W != r.Frontier.C || r.Next != r.Frontier.C+1 ||
		r.Frontier.C < r.Applied+2 || r.Frontier.C > r.Applied+4 {
		t.Fatalf("require two new ACKs beyond one sealed DATA prefix: %+v", r)
	}
}

// The queued fixture has already verified both ordinary durable public
// Commit returns. Exit before releasing the held cut or running any cleanup.
func exitRawTxnQueuedCrashChild(t *testing.T, db *DB, dir string, seal rootpublication.SealedPrefix, after rootpublication.LogicalFrontiers, applied, next, checkpoints uint64) {
	t.Helper()
	r := rawTxnQueuedCrashReceipt{seal, after, applied, next, checkpoints, make(map[string]EntryRevision)}
	r.check(t)
	for _, stem := range rawTxnQueuedCrashStems {
		for _, suffix := range []string{"/a", "/b"} {
			key := stem + suffix
			got, revision, err := db.GetVersioned([]byte(key))
			if err != nil || string(got) != stem || revision == LegacyEntryRevision {
				t.Fatalf("acknowledged live key %q = %q, revision %d: %v", key, got, revision, err)
			}
			r.Revisions[key] = revision
		}
	}
	marker, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	// A receipt next to, not inside, the DB. Do not sync or checkpoint the store here.
	if err := os.WriteFile(filepath.Join(dir, rawTxnQueuedCrashReceiptName), marker, 0600); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func TestPublicRawCOWTxnQueuedPairAbruptReopen(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestPublicRawCOWTxnRechecksQueuedPairAfterCheckpoint$/^live-size-worker$")
	cmd.Env = append(os.Environ(), rawTxnQueuedCrashRootEnv+"="+root)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("queued ACK child: %v (timeout: %v)\n%s", err, ctx.Err(), output)
	}
	data, err := os.ReadFile(filepath.Join(root, rawTxnQueuedCrashReceiptName))
	if err != nil {
		t.Fatalf("missing exact pre-exit ACK receipt: %v", err)
	}
	var r rawTxnQueuedCrashReceipt
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatalf("invalid pre-exit ACK receipt: %v", err)
	}
	r.check(t)
	if len(r.Revisions) != 2*len(rawTxnQueuedCrashStems) {
		t.Fatalf("incomplete ACK receipt: %+v", r.Revisions)
	}
	dir := filepath.Join(root, "db")
	layout, err := resolveOpenDirLayout(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	// Open would replay the suffix. Inspect the actual command WAL first.
	verifyRawTxnQueuedCrashWAL(t, scanPublicCommandWALV2(t, layout.mainDir), r)
	opts := cowPublicCrashOptions(ProfileCommandWALDurable, dir)
	configureRawTxnQueuedPair(&opts, 2<<30)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("fresh Open after abrupt exit: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if got := publicStatUint64(t, db, "treedb.applied_command_lsn"); got != r.Frontier.C {
		t.Fatalf("recovered applied LSN = %d, want %d", got, r.Frontier.C)
	}
	for _, stem := range rawTxnQueuedCrashStems {
		for _, suffix := range []string{"/a", "/b"} {
			key := stem + suffix
			got, revision, err := db.GetVersioned([]byte(key))
			if err != nil || string(got) != stem || revision == LegacyEntryRevision || revision != r.Revisions[key] {
				t.Fatalf("replayed key %q = %q, revision %d want %d: %v", key, got, revision, r.Revisions[key], err)
			}
		}
	}
	cache := db.cached
	if err := db.Close(); err != nil {
		t.Fatalf("checked recovered Close: %v", err)
	}
	requirePublicCOWFinalZero(t, cache)
}

func verifyRawTxnQueuedCrashWAL(t *testing.T, frames []commitlog.CommandEnvelope, r rawTxnQueuedCrashReceipt) {
	t.Helper()
	last, stable := r.Applied, r.Applied
	seen := make(map[string]bool, 2)
	for _, f := range frames {
		if f.LSN <= r.Applied {
			continue
		}
		if f.LSN != last+1 || f.LSN > r.Frontier.C || f.Version != commitlog.CommandFrameVersionV2 {
			t.Fatalf("unexpected physical V2 WAL suffix frame after %d: %+v", last, f)
		}
		last = f.LSN
		switch f.Kind {
		case commitlog.CommandKindDurablePrefixBarrier:
			if f.DurabilityClass != commitlog.CommandDurabilityDurable || f.Scope != commitlog.CommandScopeSystem ||
				f.PayloadFormat != commitlog.PayloadFormatDurablePrefixBarrierV1 || len(f.Payload) != 0 || len(seen) == 0 {
				t.Fatalf("unexpected original group barrier: %+v", f)
			}
		case commitlog.CommandKindRawKVBatch:
			ops, err := commitlog.DecodeRawKVBatchPayload(f.Payload)
			if err != nil || len(ops) != 2 {
				t.Fatalf("queued raw transaction LSN %d has %d operations: %v", f.LSN, len(ops), err)
			}
			stem, _, _ := strings.Cut(string(ops[0].Key), "/")
			if (stem != "first" && stem != "follower") || seen[stem] {
				t.Fatalf("duplicate/foreign raw transaction at LSN %d: %+v", f.LSN, ops)
			}
			seen[stem] = true
			suffixes := make(map[string]bool, 2)
			for _, op := range ops {
				key := string(op.Key)
				if (key != stem+"/a" && key != stem+"/b") || suffixes[key] || op.Op != commitlog.RawKVOpSet ||
					string(op.Value) != stem || op.Revision == 0 || op.Revision != uint64(r.Revisions[key]) {
					t.Fatalf("raw transaction LSN %d is not the exact acknowledged pair: %+v", f.LSN, ops)
				}
				suffixes[key] = true
			}
		default:
			t.Fatalf("unexpected command in acknowledged suffix at LSN %d: %v", f.LSN, f.Kind)
		}
		if f.DurabilityClass == commitlog.CommandDurabilityDurable {
			stable = f.LSN
		}
	}
	if len(seen) != 2 || last != r.Frontier.C || stable != r.Frontier.W {
		t.Fatalf("physical ACK suffix: pairs=%v last=%d stable=%d want %+v", seen, last, stable, r.Frontier)
	}
}
