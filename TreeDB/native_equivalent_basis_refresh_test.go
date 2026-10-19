package treedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// CRUD uses the ordinary activated public frontend. The existing fenced
// maintenance seam schedules a real rewrite without draining the newer suffix;
// ValueLogRewriteOnline's public wrapper normally checkpoints that suffix first.
func TestNativeExistingCollectionEquivalentBasisRewrite(t *testing.T) {
	testNativeExistingCollectionEquivalentBasisRewrite(t, false)
}

// Cancellation completes one synced physical batch and its bookkeeping while
// preserving the unprocessed source and unsealed logical suffix.
func TestNativeExistingCollectionEquivalentBasisRewritePartialCancel(t *testing.T) {
	testNativeExistingCollectionEquivalentBasisRewrite(t, true)
}

func testNativeExistingCollectionEquivalentBasisRewrite(t *testing.T, partialCancel bool) {
	t.Helper()
	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	seed, seedClose, _, err := OpenBackendWithCachedLeafLogStats(opts)
	if err != nil {
		t.Fatal(err)
	}
	seedClosed := false
	t.Cleanup(func() {
		if !seedClosed {
			if e := seedClose(); e != nil {
				t.Errorf("seed Close: %v", e)
			}
		}
	})
	manager := collections.NewCollectionManager(seed)
	for _, name := range []string{"rows", "alias"} {
		if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	// Maintained physical setup: one persistent pointer root is named by two
	// canonical collection descriptors. Apply a system delta so the original
	// manager-created metadata and incarnation records remain eligible for import.
	document := []byte(`{"body":"` + string(bytes.Repeat([]byte("persistent"), 512)) + `"}`)
	documents := [][]byte{document}
	if partialCancel {
		documents = append(documents, []byte(`{"body":"`+string(bytes.Repeat([]byte("remaining"), 512))+`"}`))
	}
	ptrs, err := seed.AppendValueLogValues(documents)
	if err != nil {
		t.Fatal(err)
	}
	rootTable, err := memtable.NewWithCapacityMode(len(documents), memtable.ModeHashSorted)
	if err != nil {
		t.Fatal(err)
	}
	rootTable.SetEntry([]byte("old"), nil, ptrs[0], node.FlagPointer)
	if partialCancel {
		if ptrs[1].FileID != ptrs[0].FileID {
			t.Fatal("partial rewrite fixture needs both pointers in the selected source")
		}
		rootTable.SetEntry([]byte("remaining"), nil, ptrs[1], node.FlagPointer)
	}
	rootTable.Freeze()
	_, roots, err := seed.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		Iter: rootTable.NewIterator(nil, nil), StoragePolicy: backenddb.OrderedRootStoragePagerLeaves,
	}}, func(roots []uint64) (iterator.UnsafeIterator, error) {
		descriptors, e := memtable.NewWithCapacityMode(2, memtable.ModeHashSorted)
		if e != nil {
			return nil, e
		}
		for _, name := range []string{"rows", "alias"} {
			var raw [8]byte
			binary.BigEndian.PutUint64(raw[:], roots[0])
			descriptors.Set([]byte("collections/root/"+name+"/primary"), raw[:])
		}
		descriptors.Freeze()
		return descriptors.NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	seed.ReleaseValueLogValues(ptrs)
	if len(roots) != 1 || roots[0] == 0 {
		t.Fatalf("seed roots: %v", roots)
	}
	if err = seedClose(); err != nil {
		t.Fatal(err)
	}
	seedClosed = true

	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if e := database.Close(); e != nil {
				t.Errorf("cleanup Close: %v", e)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	// Import the complete physical catalog and seal a native engine while leaving
	// the aliased physical roots untouched. The following row remains unsealed.
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "spare"}); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rows, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	alias, err := manager.OpenCollection("alias")
	if err != nil {
		t.Fatal(err)
	}
	held, err := rows.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := held.Close(); e != nil {
			t.Errorf("held Close: %v", e)
		}
	})
	heldAlias, err := alias.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := heldAlias.Close(); e != nil {
			t.Errorf("held alias Close: %v", e)
		}
	})
	newer := []byte(`{"n":2}`)
	if _, err = rows.InsertBatch([][]byte{[]byte("new")}, [][]byte{newer}); err != nil {
		t.Fatal(err)
	}
	before, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	sequence, lsn := before.LogicalSequence(), backend.CommandWALNextLSN()
	if err = before.Close(); err != nil {
		t.Fatal(err)
	}
	prefix, _, err := backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	frontiers := prefix.LogicalFrontiers()
	if frontiers.V <= frontiers.S.LogicalSequence {
		t.Fatalf("fixture lacks an unsealed logical suffix: %+v", frontiers)
	}
	diagnosticLimits := opts.COWMemtableLimits
	if diagnosticLimits == (COWMemtableLimits{}) {
		diagnosticLimits = DefaultCOWMemtableLimits()
	}
	logMemory := func(stage string, scope int, key string, readErr error) {
		if !partialCancel {
			return
		}
		memory := database.cached.COWMemoryStats()
		counters := database.cached.Stats()
		available := func(limit, used uint64) uint64 {
			if used >= limit {
				return 0
			}
			return limit - used
		}
		t.Logf("partial read admission stage=%s scope=%d key=%q error=%v limits=%+v memory=%+v available_in_flight=%d available_total=%d available_retired=%d maintenance_capacity=%s maintenance_used=%s", stage, scope, key, readErr, diagnosticLimits, memory, available(diagnosticLimits.MaxInFlightBytes, memory.ReservedBytes), available(diagnosticLimits.MaxTotalBytes, memory.TotalBytes), available(diagnosticLimits.MaxRetiredBytes, memory.HistoryBytes+memory.ReservedBytes+memory.DeferredBytes), counters["treedb.cache.cow.maintenance_capacity_bytes"], counters["treedb.cache.cow.maintenance_used_bytes"])
	}
	physical := func(stage string) (uint64, page.ValuePtr, page.ValuePtr) {
		s := backend.AcquireSnapshot()
		if s == nil {
			t.Fatal("nil physical snapshot")
		}
		defer func() {
			if e := s.Close(); e != nil {
				t.Errorf("physical Close: %v", e)
			}
		}()
		token, ok := s.StateToken()
		if !ok {
			t.Fatal("physical token")
		}
		logShape := func(key string, ptr page.ValuePtr) {
			if !partialCancel {
				return
			}
			file, identity, e := s.PinnedValueLogFile(ptr.FileID)
			if e != nil {
				t.Fatalf("diagnostic pinned %s: %v", key, e)
			}
			limit := diagnosticLimits.MaxInFlightBytes
			shape, e := valuelog.InspectCOWRecord(file, ptr, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
			if e != nil {
				t.Fatalf("diagnostic shape %s: %v", key, e)
			}
			// Use the maintained envelope with the read workspace's capacity
			// rounding. This lower bound omits its small owner/lease wrappers.
			var decoderLowerBound uint64
			if shape.Compressed && shape.DictID == 0 && shape.Codec == valuelog.BlockCodecZSTD {
				capacity := func(n uint64) uint64 {
					c := uint64(1024)
					for c < n {
						c *= 2
					}
					return c
				}
				raw, input := capacity(shape.RawBytes), capacity(shape.PayloadBytes())
				sizes, e := valuelog.COWDecoderRetentionSizes(0, raw, input, max(raw, 2048))
				if e != nil {
					t.Fatalf("diagnostic decoder %s: %v", key, e)
				}
				for _, size := range []uint64{sizes.OwnerWrapper, sizes.DefinitionCopy, sizes.RawBacking, sizes.InputBacking, sizes.SequenceBacking} {
					decoderLowerBound += memtable.COWAllocationCharge(size)
				}
				decoderLowerBound += uint64(sizes.FixedAllocationCount) * memtable.COWAllocationCharge(sizes.FixedAllocationBytes)
			}
			t.Logf("partial physical shape stage=%s root=%+v key=%s pointer=%+v identity=%+v compressed=%t codec=%v dictionary=%d record_bytes=%d raw_bytes=%d payload_bytes=%d value_bytes=%d decoder_lower_bound=%d", stage, token, key, ptr, identity, shape.Compressed, shape.Codec, shape.DictID, shape.RecordBytes, shape.RawBytes, shape.PayloadBytes(), shape.ValueBytes, decoderLowerBound)
		}
		var root uint64
		for _, name := range []string{"rows", "alias"} {
			raw, e := s.GetAtRoot(token.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
			if e != nil || len(raw) != 8 {
				t.Fatalf("descriptor %s: %x %v", name, raw, e)
			}
			r := binary.BigEndian.Uint64(raw)
			if root != 0 && root != r {
				t.Fatalf("qualified alias diverged: %d %d", root, r)
			}
			root = r
		}
		entry, e := s.GetEntryAtRoot(root, []byte("old"))
		if e != nil || entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("actual persistent pointer: %+v %v", entry, e)
		}
		logShape("old", entry.ValuePtr)
		var remaining page.ValuePtr
		if partialCancel {
			entry, e := s.GetEntryAtRoot(root, []byte("remaining"))
			if e != nil || entry.Flags&node.FlagPointer == 0 {
				t.Fatalf("actual remaining pointer: %+v %v", entry, e)
			}
			remaining = entry.ValuePtr
			logShape("remaining", remaining)
		}
		return root, entry.ValuePtr, remaining
	}
	oldRoot, oldPointer, remainingPointer := physical("before rewrite")
	protection := backenddb.ValueLogRewriteOnlineOptions{SourceFileIDs: []uint32{oldPointer.FileID}, BatchSize: 1}
	protection.ProtectedPaths = database.cached.ValueLogProtectedPaths()
	if len(protection.ProtectedPaths) == 0 {
		protection.ProtectedPaths = []string{""}
	}
	protection.LeafGenerationProtectedRootIDs, protection.LeafGenerationProtectedSystemRootIDs = database.cached.ProtectedLeafGenerationRootIDPair()
	protection.ReserveRIDs = database.cached.ReserveValueLogRIDs
	// An ordinary backend alias cannot borrow the role merely because the same
	// DB is activated. Refusal must precede copying or physical publication.
	refused, refuseErr := backend.ValueLogRewriteOnline(context.Background(), protection)
	if !errors.Is(refuseErr, backenddb.ErrNativeSemanticUnsupported) || refused.RecordsCopied != 0 {
		t.Fatalf("ordinary backend rewrite alias: stats=%+v error=%v", refused, refuseErr)
	}
	unchangedRoot, unchangedPointer, unchangedRemaining := physical("after refused alias")
	if unchangedRoot != oldRoot || unchangedPointer != oldPointer || unchangedRemaining != remainingPointer {
		t.Fatal("refused alias changed physical roots")
	}
	var rewriteCtx context.Context = context.Background()
	reservations := 0
	if partialCancel {
		var cancel context.CancelFunc
		rewriteCtx, cancel = context.WithCancel(rewriteCtx)
		defer cancel()
		protection.SyncEachBatch = true
		reserve := protection.ReserveRIDs
		protection.ReserveRIDs = func(count int) (uint64, error) {
			start, err := reserve(count)
			if err == nil {
				reservations++
				if reservations == 1 {
					// The first batch owns the original allocator's exact range.
					// It completes append/sync/swap without another context check;
					// cancellation is observed before scanning the remaining pointer.
					cancel()
				}
			}
			return start, err
		}
	}
	// Use the named operation on the SAME public cache, without the outer
	// public convenience checkpoint: the native suffix must remain unsealed.
	stats, err := database.cached.ValueLogRewriteOnline(rewriteCtx, protection)
	logMemory("after rewrite", -1, "", err)
	if partialCancel {
		if !errors.Is(err, context.Canceled) || errors.Is(err, backenddb.ErrRecoveryRequired) || reservations != 1 {
			t.Fatalf("completed-prefix cancellation: reservations=%d stats=%+v error=%v", reservations, stats, err)
		}
	} else if err != nil {
		t.Fatalf("equivalent physical rewrite: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("real aliased pointer rewrite: %+v", stats)
	}
	if partialCancel && (stats.ValueRecordsCopied != 1 || stats.ValueBytesCopied != int64(len(document))) {
		t.Fatalf("partial rewrite copied something other than the first pointer: %+v", stats)
	}
	newRoot, newPointer, newRemainingPointer := physical("after rewrite")
	if oldRoot == newRoot || oldPointer == newPointer {
		t.Fatalf("rewrite did not move root/pointer: %d/%+v -> %d/%+v", oldRoot, oldPointer, newRoot, newPointer)
	}
	if newRemainingPointer != remainingPointer {
		t.Fatalf("rewrite moved the unprocessed pointer: %+v -> %+v", remainingPointer, newRemainingPointer)
	}
	if got := prefix.LogicalFrontiers(); got != frontiers {
		t.Fatalf("physical rewrite changed original logical frontiers: %+v -> %+v", frontiers, got)
	}
	if err := prefix.AcceptedError(); err != nil {
		t.Fatalf("equivalent rewrite poisoned original accepted custody: %v", err)
	}
	after, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if after.LogicalSequence() != sequence || backend.CommandWALNextLSN() != lsn {
		t.Fatal("physical maintenance changed logical sequence/LSN")
	}
	if err = after.Close(); err != nil {
		t.Fatal(err)
	}
	for scope, c := range []*collections.Collection{rows, alias} {
		logMemory("before current Get", scope, "old", nil)
		got, e := c.Get([]byte("old"))
		logMemory("after current Get", scope, "old", e)
		if e != nil || !bytes.Equal(got, document) {
			t.Fatalf("current rewritten value: %q %v", got, e)
		}
		if partialCancel {
			logMemory("before current Get", scope, "remaining", nil)
			got, e = c.Get([]byte("remaining"))
			logMemory("after current Get", scope, "remaining", e)
			if e != nil || !bytes.Equal(got, documents[1]) {
				t.Fatalf("current unprocessed value: %q %v", got, e)
			}
		}
	}
	logMemory("before current Get", 0, "new", nil)
	got, err := rows.Get([]byte("new"))
	logMemory("after current Get", 0, "new", err)
	if err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("unsealed suffix: %q %v", got, err)
	}
	logMemory("before current Get", 1, "new", nil)
	got, err = alias.Get([]byte("new"))
	logMemory("after current Get", 1, "new", err)
	if err != nil || got != nil {
		t.Fatalf("qualified alias absence: %q %v", got, err)
	}
	for scope, v := range []*collections.CollectionReadView{held, heldAlias} {
		logMemory("before held Fetch", scope, "old,new", nil)
		result, e := v.FetchDocumentsByID([][]byte{[]byte("old"), []byte("new")}, collections.DocumentFetchOptions{})
		logMemory("after held Fetch", scope, "old,new", e)
		if e != nil || len(result.Results) != 2 || !result.Results[0].Found || !bytes.Equal(result.Results[0].Document, document) || result.Results[1].Found {
			t.Fatalf("old complete view: %+v %v", result, e)
		}
		if partialCancel {
			logMemory("before held Fetch", scope, "remaining", nil)
			result, e := v.FetchDocumentsByID([][]byte{[]byte("remaining")}, collections.DocumentFetchOptions{})
			logMemory("after held Fetch", scope, "remaining", e)
			if e != nil || len(result.Results) != 1 || !result.Results[0].Found || !bytes.Equal(result.Results[0].Document, documents[1]) {
				t.Fatalf("old unprocessed value: %+v %v", result, e)
			}
		}
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = heldAlias.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	registry, cache := backend.ValueLogIdentityPinRegistry(), database.cached
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("original pins: %+v", s)
	}
	if err := registry.CleanupError(); err != nil {
		t.Fatalf("original physical cleanup: %v", err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed = false
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	for _, name := range []string{"rows", "alias"} {
		c, e := manager.OpenCollection(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.Get([]byte("old"))
		if e != nil || !bytes.Equal(got, document) {
			t.Fatalf("reopen %s: %q %v", name, got, e)
		}
		if partialCancel {
			got, e = c.Get([]byte("remaining"))
			if e != nil || !bytes.Equal(got, documents[1]) {
				t.Fatalf("reopen unprocessed %s: %q %v", name, got, e)
			}
		}
		got, e = c.Get([]byte("new"))
		if name == "rows" {
			if e != nil || !bytes.Equal(got, newer) {
				t.Fatalf("reopen suffix: %q %v", got, e)
			}
		} else if e != nil || got != nil {
			t.Fatalf("reopen alias absence: %q %v", got, e)
		}
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	registry, cache = backend.ValueLogIdentityPinRegistry(), database.cached
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("reopen pins: %+v", s)
	}
	if err := registry.CleanupError(); err != nil {
		t.Fatalf("reopened physical cleanup: %v", err)
	}
}
