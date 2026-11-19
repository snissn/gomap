package treedb

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Only inspect the existing channel's length: the real worker must consume its
// own notification. This avoids replacing its timer or command-WAL bytes hook.
func cowIdlePendingNotifications(database *DB) int {
	return reflect.ValueOf(database.cached).Elem().FieldByName("autoCheckpointWriteCh").Len()
}

func cowIdleForegroundWriteTime(t *testing.T, database *DB) time.Time {
	t.Helper()
	const stat = "treedb.cache.vlog_generation.maintenance.foreground_last_write_unix_nano"
	raw := database.cached.Stats()[stat]
	stamp, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || stamp <= 0 {
		t.Fatalf("missing foreground activity time %q: %v", raw, err)
	}
	return time.Unix(0, stamp)
}

func TestCOWAutoCheckpointIdleActualActivity(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileCommandWALDurable} {
		for _, engine := range []string{"cow_btree", "cow_ordered"} {
			t.Run(string(profile)+"/"+engine, func(t *testing.T) {
				// Scale the complete activity window and its scheduling bounds together.
				// A one-second idle interval leaves headroom for durable I/O and race CI.
				const idle = time.Second
				database, opts, _ := cowPublicContractOpen(t, profile, func(o *Options) {
					o.MemtableMode = engine
					// The idle floor is 1 MiB at this threshold; the complete fixture is
					// far below the 16 MiB flush boundary (the helper defaults to 1 GiB).
					o.FlushThreshold = 16 << 20
					o.BackgroundCheckpointInterval = -1
					o.MaxWALBytes = -1
					o.BackgroundCheckpointIdleDuration = idle
				})
				if !database.cached.COWMode() || !database.commandWALCached || database.Stats()["treedb.cache.memtable_mode"] != engine {
					t.Fatal("public COW/external-command-WAL owner did not activate")
				}
				entered := make(chan time.Time, 8)
				database.cached.SetCommandWALCheckpointCutoverHook(func() {
					// This is maintenance entry, before checkpoint/cleanup completion.
					entered <- time.Now()
					database.snapshotPublicCommandWALCheckpointCutover()
				})
				initialCommit := database.backend.State().CommitSeq
				seed := bytes.Repeat([]byte("seed-value/"), 1024)
				batch := database.NewBatch()
				if err := batch.Set([]byte("seed"), seed); err != nil {
					t.Fatal(err)
				}
				// Forced value pointers contribute little command-WAL pressure.
				// Large keys supply >1 MiB in one actual public publication.
				for i := 0; i < 700; i++ {
					key := []byte(fmt.Sprintf("pressure/%04d/%s", i, bytes.Repeat([]byte("k"), 1700)))
					if err := batch.Set(key, []byte("persistent-pointer-value")); err != nil {
						t.Fatal(err)
					}
				}
				if err := batch.Write(); err != nil {
					t.Fatal(err)
				}
				if err := batch.Close(); err != nil {
					t.Fatal(err)
				}
				notifiedBytes := database.publicCommandWALAutoCheckpointBytes()
				if notifiedBytes < 1<<20 || notifiedBytes >= 2<<20 {
					t.Fatalf("seed WAL pressure=%d, want [1MiB,2MiB)", notifiedBytes)
				}
				// No writes preceded this one batch publication, and noteWrite runs
				// once at its end. Its successful notification boundary is
				// therefore exactly notifiedBytes; later deltas use that cut.
				// A single acknowledged write crossed the 1 MiB coalescing threshold;
				// len==0 now proves the worker consumed that notification, not the test.
				deadline := time.Now().Add(idle / 2)
				for cowIdlePendingNotifications(database) != 0 {
					if time.Now().After(deadline) {
						t.Fatal("worker did not consume seed notification")
					}
					time.Sleep(time.Millisecond)
				}
				if database.backend.State().CommitSeq != initialCommit {
					t.Fatal("seed unexpectedly flushed before the active window")
				}
				t.Logf("owner=%s profile=%s notified_bytes=%d flush_threshold=%d", engine, profile, notifiedBytes, opts.FlushThreshold)
				activity := []time.Time{cowIdleForegroundWriteTime(t, database)}
				started := time.Now()
				ticker := time.NewTicker(idle / 5)
				defer ticker.Stop()
				lastWrite := started
				var writes []time.Duration
				var cutover time.Time
				var want []byte
				activeUntil := started.Add(3 * idle)
			active:
				for time.Now().Before(activeUntil) {
					select {
					case cutover = <-entered:
						break active
					case <-ticker.C:
						lastWriteStarted := time.Now()
						if !lastWriteStarted.Before(activeUntil) {
							break active
						}
						want = []byte(fmt.Sprintf("acknowledged-small-write-%d", len(writes)))
						if gap := lastWriteStarted.Sub(lastWrite); gap >= idle/2 {
							t.Fatalf("active writer scheduling gap=%s exceeds half the idle interval", gap)
						}
						if err := database.Set([]byte("active"), want); err != nil {
							t.Fatal(err)
						}
						lastWrite = time.Now()
						writes = append(writes, lastWrite.Sub(started))
						stamp := cowIdleForegroundWriteTime(t, database)
						if stamp.Before(lastWriteStarted) || stamp.After(lastWrite) {
							t.Fatalf("write did not record foreground activity within the call: start=%s activity=%s ACK=%s", lastWriteStarted, stamp, lastWrite)
						}
						activity = append(activity, stamp)
						growth := database.publicCommandWALAutoCheckpointBytes() - notifiedBytes
						if growth < 0 || growth >= 1<<20 || cowIdlePendingNotifications(database) != 0 {
							t.Fatalf("small writes invalidated coalescing: relative_growth=%d pending=%d", growth, cowIdlePendingNotifications(database))
						}
						if got, err := database.Get([]byte("active")); err != nil || !bytes.Equal(got, want) {
							t.Fatalf("ACK readback=%q,%v", got, err)
						}
					}
				}
				if cutover.IsZero() {
					// A durable ACK can finish after the activity deadline. The cutover's
					// own timestamp identifies its phase, even when the event is queued.
					flushed := database.backend.State().CommitSeq != initialCommit
					select {
					case cutover = <-entered:
					default:
						if flushed {
							t.Fatal("backend flushed without the corresponding maintenance cutover")
						}
					}
				}
				t.Logf("actual write ACK offsets=%v relative_wal_growth=%d", writes, database.publicCommandWALAutoCheckpointBytes()-notifiedBytes)
				if cutover.IsZero() {
					select {
					case cutover = <-entered:
					case <-time.After(5 * idle):
						t.Fatal("genuinely idle maintenance never entered")
					}
				}
				// The timestamp can be recorded before a durable ACK returns. Use
				// the latest recorded activity preceding the actual cutover.
				lastActivity := activity[0]
				for _, stamp := range activity[1:] {
					if stamp.After(cutover) {
						break
					}
					lastActivity = stamp
				}
				quiet := cutover.Sub(lastActivity)
				if cutover.Before(activeUntil) {
					t.Fatalf("idle maintenance entered inside active window: elapsed=%s quiet=%s ACK offsets=%v", cutover.Sub(started), quiet, writes)
				}
				if quiet < idle {
					t.Fatalf("idle entry after only %s quiet; want %s, ACK offsets=%v", quiet, idle, writes)
				}
				t.Logf("genuine idle maintenance entered after %s quiet", quiet)
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				for key, value := range map[string][]byte{"seed": seed, "active": want} {
					if got, err := reopened.Get([]byte(key)); err != nil || !bytes.Equal(got, value) {
						t.Errorf("reopened %s len=%d err=%v", key, len(got), err)
					}
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
