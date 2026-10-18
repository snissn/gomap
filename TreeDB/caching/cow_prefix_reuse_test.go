package caching

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Repeated actual disk handoffs must retire the complete captured prefix and
// reuse finite logical source capacity. This is a correctness/diagnostic
// control, not a sustained-throughput or physical-file-space qualification.
func TestCOWAcceptedPrefixRepeatedDrainReuse(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(fmt.Sprintf("CommandWAL=%t", wal), func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, wal)
			writeValue := func(key, value []byte) (err error) {
				if !wal {
					return cache.Set(key, value)
				}
				b := cache.NewBatchWithSize(1)
				defer func() {
					if closeErr := b.Close(); err == nil {
						err = closeErr
					}
				}()
				if err = b.Set(key, value); err != nil {
					return err
				}
				owner := cache.cow.txnOwner
				if err = b.AuthorizeCOWAppend(owner); err != nil {
					return err
				}
				err = b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error {
					if err := b.AcquireCOWAppendGuard(owner); err != nil {
						return err
					}
					prepare := func() error {
						b.AssignExternalCommandWALPointRevisions()
						return b.PrepareExternalCommandWALPublication()
					}
					lsn, appendErr := backend.AppendPreparedRawKVUnderGuard(b.COWAppendGuard(), prepare, b.ExternalCommandWALFinalizer(), b.Replay, 1, backenddb.RawKVCommandWALAppendRelaxed)
					receiptErr := b.RecordCOWAppend(owner, lsn, appendErr, false)
					if appendErr != nil {
						return appendErr
					}
					return receiptErr
				})
				b.CompleteCOWAcknowledgement(owner, false, err)
				return err
			}
			limits := cache.cow.budget.Limits()
			key := []byte("reuse/000")
			original := bytes.Repeat([]byte("held-original-pointer/"), 200)
			if err := writeValue(key, original); err != nil {
				t.Fatal(err)
			}
			held := cache.AcquireSnapshot()
			if held == nil {
				t.Fatal("old pointer snapshot unavailable")
			}
			defer held.Close()
			value := bytes.Repeat([]byte{0x61}, 4096)
			for cycle := range 8 {
				before := cache.cow.prefix.LogicalFrontiers()
				var firstACK time.Time
				var longestACK time.Duration
				for write := range 128 {
					value[0], value[1] = byte(cycle), byte(write)
					start := time.Now()
					if err := writeValue([]byte(fmt.Sprintf("reuse/%03d", write%64)), value); err != nil {
						t.Fatal(err)
					}
					ack := time.Now()
					longestACK = max(longestACK, ack.Sub(start))
					if firstACK.IsZero() {
						firstACK = ack
					}
				}
				accepted := cache.cow.prefix.LogicalFrontiers()
				if accepted.V != before.V+128 || accepted.S != before.S || accepted.W > accepted.C || accepted.S.LogicalSequence > accepted.V {
					t.Fatalf("ordinary ACKs did not preserve independent frontiers: before=%+v accepted=%+v", before, accepted)
				}
				if wal && (accepted.C != before.C+128 || accepted.C == 0) {
					t.Fatalf("actual canonical appends missing: before=%+v accepted=%+v", before, accepted)
				}
				if !wal && (accepted.C != 0 || accepted.W != 0 || accepted.S.CanonicalLSN != 0) {
					t.Fatalf("NoWAL fabricated a canonical identity: %+v", accepted)
				}
				drainStart := time.Now()
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				drained := cache.cow.prefix.LogicalFrontiers()
				sealTime := time.Now()
				if drained.V != accepted.V || drained.C != accepted.C || drained.S != (rootpublication.SealedPrefix{LogicalSequence: accepted.V, CanonicalLSN: accepted.C}) || drained.W > drained.C {
					t.Fatalf("drain did not seal the exact captured complete prefix: accepted=%+v drained=%+v", accepted, drained)
				}
				if cycle < 4 {
					got, err := held.Get(key)
					if err != nil || !bytes.Equal(got, original) {
						t.Fatalf("held pointer changed across cycle %d: bytes=%d err=%v", cycle, len(got), err)
					}
				}
				if cycle == 3 {
					if err := held.Close(); err != nil {
						t.Fatal(err)
					}
				}
				stats := cache.COWMemoryStats()
				if cycle >= 4 && (stats.Views != 0 || stats.Sources != 0 || stats.Generations != limitsForCOWReuseShards) {
					t.Fatalf("unpinned logical owners did not return to the fixed shard bound: %+v", stats)
				}
				if cache.cow.budget.Limits() != limits {
					t.Fatal("drain changed the finite original admission limits")
				}
				t.Logf("COW_PREFIX_REUSE cycle=%d held=%t before=%+v accepted=%+v drained=%+v max_ack_ns=%d first_ack_to_seal_ns=%d explicit_drain_ns=%d memory=%+v", cycle, cycle < 3, before, accepted, drained, longestACK.Nanoseconds(), sealTime.Sub(firstACK).Nanoseconds(), sealTime.Sub(drainStart).Nanoseconds(), stats)
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.Views != 0 || stats.Generations != 0 || stats.Sources != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("final original ownership debt: %+v", stats)
			}
		})
	}
}

// nativeBridgeFixture binds the production cache with exactly two shards.
const limitsForCOWReuseShards = 2
