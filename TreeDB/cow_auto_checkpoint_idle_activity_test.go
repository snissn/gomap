package treedb

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// Only inspect the existing channel's length: the real worker must consume its
// own notification. This avoids replacing its timer or command-WAL bytes hook.
func cowIdlePendingNotifications(database *DB) int {
	return reflect.ValueOf(database.cached).Elem().FieldByName("autoCheckpointWriteCh").Len()
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
				started := time.Now()
				ticker := time.NewTicker(idle / 5)
				defer ticker.Stop()
				lastWrite := started
				var writes []time.Duration
				lastWriteStarted := started
				var want []byte
				activeUntil := started.Add(3 * idle)
				for time.Now().Before(activeUntil) {
					select {
					case at := <-entered:
						t.Fatalf("idle maintenance entered during writes at %s, last ACK age=%s, writes=%v", at.Format(time.RFC3339Nano), at.Sub(lastWrite), writes)
					case <-ticker.C:
						want = []byte(fmt.Sprintf("acknowledged-small-write-%d", len(writes)))
						lastWriteStarted = time.Now()
						if gap := lastWriteStarted.Sub(lastWrite); gap >= idle/2 {
							t.Fatalf("active writer scheduling gap=%s exceeds half the idle interval", gap)
						}
						if err := database.Set([]byte("active"), want); err != nil {
							t.Fatal(err)
						}
						lastWrite = time.Now()
						writes = append(writes, lastWrite.Sub(started))
						growth := database.publicCommandWALAutoCheckpointBytes() - notifiedBytes
						if growth < 0 || growth >= 1<<20 || cowIdlePendingNotifications(database) != 0 {
							t.Fatalf("small writes invalidated coalescing: relative_growth=%d pending=%d", growth, cowIdlePendingNotifications(database))
						}
						if got, err := database.Get([]byte("active")); err != nil || !bytes.Equal(got, want) {
							t.Fatalf("ACK readback=%q,%v", got, err)
						}
					}
				}
				select {
				case at := <-entered:
					t.Fatalf("idle maintenance entered during active window: %v", at)
				default:
				}
				if database.backend.State().CommitSeq != initialCommit {
					t.Fatal("active window unexpectedly flushed")
				}
				t.Logf("actual write ACK offsets=%v relative_wal_growth=%d", writes, database.publicCommandWALAutoCheckpointBytes()-notifiedBytes)
				select {
				case at := <-entered:
					// noteWrite runs between write start and acknowledgement.
					if quiet := at.Sub(lastWriteStarted); quiet < idle {
						t.Fatalf("idle entry after only %s quiet; want %s", quiet, idle)
					}
					t.Logf("genuine idle maintenance entered after %s quiet", at.Sub(lastWrite))
				case <-time.After(5 * idle):
					t.Fatal("genuinely idle maintenance never entered")
				}
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
