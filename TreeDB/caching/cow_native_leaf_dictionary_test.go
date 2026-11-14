//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// The ordinary caching appender writes dictionary zero. Real generation packing
// then selects the persisted outer-leaf dictionary and publishes a packed
// leaf with its stable dictionary resource closure. No leaf-log/decoder hook or
// fabricated page is installed. This fixture compiles on the causal predecessor.
func cowNativePackedDictionaryLeafFixture(t *testing.T) (*DB, *backenddb.DB, page.LeafLogPtr, uint64, []byte) {
	t.Helper()
	return cowNativePackedDictionaryLeafFixtureWithEncoding(t, false)
}

// The paired real-engine control opts into the maintained dictionary appender
// before any fixture writes. The common fixture retains its ordinary Block
// appender; only this explicit encoding choice makes catalog publication eligible
// for dictionary reads on both physical operands.
func cowNativePackedDictionaryLeafFixtureWithEncoding(t *testing.T, dictionaryAppender bool) (*DB, *backenddb.DB, page.LeafLogPtr, uint64, []byte) {
	t.Helper()
	return cowNativePackedDictionaryLeafFixtureWithReadPolicy(t, dictionaryAppender, memtable.COWLimits{}, -1)
}

func cowNativePackedDictionaryLeafFixtureWithReadPolicy(t *testing.T, dictionaryAppender bool, limits memtable.COWLimits, cacheEntries int) (*DB, *backenddb.DB, page.LeafLogPtr, uint64, []byte) {
	t.Helper()
	requireLeafGenerationPackPromotionSupport(t)
	t.Setenv(envDisableVlogGenerationLoop, "1")
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")
	dir := t.TempDir()
	var store *dictdb.Store
	backendDir := filepath.Join(dir, "backend")
	if dictionaryAppender {
		// Durable recovery resolves sibling dictionary dependencies from the
		// public root/{maindb,dictdb} layout used by the paired reopen control.
		backendDir = filepath.Join(dir, "maindb")
	}
	backend, err := backenddb.Open(backenddb.Options{
		Dir: backendDir, CommandWAL: true,
		Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true,
		IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: cacheEntries,
		ValueLog: backenddb.ValueLogOptions{
			DictLookup: func(id uint64) ([]byte, error) {
				if store == nil {
					return nil, valuelog.ErrMissingDict
				}
				return store.GetDictBytes(context.Background(), id)
			},
			DictCurrentForClass: func(ctx context.Context, class string) (uint64, error) {
				if store == nil {
					return 0, nil
				}
				return store.GetCurrentForClass(ctx, class)
			},
			DictLeafPayloadMode: func(ctx context.Context, id uint64) (bool, bool, error) {
				if store == nil {
					return false, false, nil
				}
				return store.GetLeafPayloadMode(ctx, id)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	opts := Options{
		TransactionOwner: owner, MemtableMode: "cow_ordered", ExternalCommandWAL: true, COWMemtableLimits: limits,
		MemtableShards: 2, FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold: 256, ValueLogCompression: uint8(vlogCompressionBlock),
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogGenerationLeafSegmentTargetBytes: 8 << 20,
	}
	if dictionaryAppender {
		// Match the existing dictionary-only leaf producer at owner creation.
		// Medium/size admits outer-leaf dictionaries without adaptive Block
		// selection; retain the explicitly persisted dictionary, not training.
		opts.ValueLogCompression = uint8(vlogCompressionDict)
		opts.ValueLogAutoPolicy = uint8(vlogAutoSize)
		opts.ValueLogCompressionAutotune = valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium}
		opts.ValueLogDictIncompressibleHoldBytes = -1
		opts.ValueLogDictTrain.TrainBytes = -1
	}
	db, err := Open(dir, backend, opts)
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)
	want := bytes.Repeat([]byte{1}, 128)
	samples := make([][]byte, 4)
	for sample := range samples {
		leaf := make([]byte, page.PageSize)
		builder := node.NewBuilderWithOptions(leaf, page.PageTypeLeaf, node.BuilderOptions{})
		for i := 0; i < 16; i++ {
			key := []byte(fmt.Sprintf("pack-%05d", sample*16+i))
			if err := builder.AddLeafEntry(key, want, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
		}
		builder.FinishNoNode()
		samples[sample], _, err = valuelog.MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			t.Fatal(err)
		}
	}
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID: 73, Contents: samples, History: append([]byte(nil), samples[0]...),
		Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err = dictdb.Open(filepath.Join(dir, "dictdb"), backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	dictID, err := store.PutDictBytes(context.Background(), dictionary)
	if err == nil {
		err = store.SetCurrentForClass(context.Background(), "outer_leaf", dictID)
	}
	if err == nil {
		err = store.SetLeafPayloadMode(context.Background(), dictID, false)
	}
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	db.SetDictStore(store)
	var dictionaryLog *cowDictionaryLeafLog
	if dictionaryAppender {
		// Initialization only: the original owner has not written any fixture
		// data or activated native semantics. Keep the real stable-resource,
		// RID allocation, segment registration and publication lifecycle across
		// every append lane, as newCachingLeafPageLog does for outer leaves.
		// The restricted interface still excludes prepared append methods.
		dictionaryLog = &cowDictionaryLeafLog{
			cowDictionaryLeafLifecycle: &cachingLeafPageLogGroup{db: db}, db: db, dictID: dictID, dictionary: dictionary,
		}
		backend.SetLeafPageLog(dictionaryLog)
	}
	privateCOWPackWrite(t, db, backend, 1, 4096)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	db.leafLog.vlogMu.Lock()
	rotateErr := db.rotateValueLogMuHeld(&db.leafLog)
	db.leafLog.vlogMu.Unlock()
	if rotateErr != nil {
		t.Fatal(rotateErr)
	}
	for round := 2; round <= 4; round++ {
		privateCOWPackWrite(t, db, backend, round, 2048)
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	capturePointer := func() page.LeafLogPtr {
		t.Helper()
		var key, leaf [page.PageSize]byte
		var ptr page.LeafLogPtr
		stop := errors.New("capture fixed leaf pointer")
		_, err := db.cow.cut.basis.snapshot.GetEntryExactWithFixedScratch([]byte("pack-03000"), key[:], leaf[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) { ptr = p; return nil, stop })
		if !errors.Is(err, stop) || ptr.FileID == 0 {
			t.Fatalf("fixed leaf pointer: %+v %v", ptr, err)
		}
		return ptr
	}
	sourcePtr := capturePointer()
	// Every flush worker has its own append lane. Rotate all of them, then
	// publish a change in a different leaf so the fixed source is sealed and
	// registered before planning. A default-lane rotation alone is insufficient.
	for _, l := range db.leafLogAppendLanesSnapshot() {
		l.vlogMu.Lock()
		rotateErr := db.rotateValueLogMuHeld(l)
		l.vlogMu.Unlock()
		if rotateErr != nil {
			t.Fatal(rotateErr)
		}
	}
	privateCOWPackWrite(t, db, backend, 5, 1)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if ptr := capturePointer(); ptr != sourcePtr {
		t.Fatalf("disjoint seal publication changed fixed leaf: before=%+v after=%+v", sourcePtr, ptr)
	}
	// Use the real owner-bound pack/adoption seam, with deterministic selection
	// rather than a scheduler tick. The read itself never recaptures or retries.
	var stats backenddb.LeafGenerationPackRunOnceStats
	var handoffsBefore uint64
	packErr := func() error {
		db.flushMu.Lock()
		defer db.flushMu.Unlock()
		if dictionaryLog != nil {
			cowRequireDictionaryLeafOwnerContract(t, dictionaryLog)
			handoffsBefore = dictionaryLog.handoffs.Load()
		}
		original, ok := db.cow.cut.basis.snapshot.StateToken()
		if !ok {
			return errors.New("missing original basis")
		}
		var err error
		stats, err = backend.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), backenddb.LeafGenerationPackFromPlanOptions{
			Sync: true, Force: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1,
			// Select all eligible generations within the unchanged copy bound. A
			// two-generation reclaim optimum may omit the fixed source entirely.
			MaxBytesToCopy: 32 << 20, ReserveRIDs: db.ReserveValueLogRIDs,
		})
		if err != nil {
			return err
		}
		return db.adoptPrivateCOWLeafBasisWithTransition(original, stats.Transition)
	}()
	if packErr != nil || !stats.Ran || stats.Pack.BytesCopied == 0 {
		t.Fatalf("real dictionary pack: ran=%t copied=%d error=%v", stats.Ran, stats.Pack.BytesCopied, packErr)
	}
	if dictionaryLog != nil && (dictionaryLog.handoffs.Load() <= handoffsBefore || stats.Pack.CopyAttempts == 0) {
		// The backend calls the original handoff before incrementing CopyAttempts
		// or creating staging. A hidden cached-owner wrapper must fail this test.
		t.Fatal("installed dictionary appender did not reach original handoff before private copy")
	}
	sourceSelected := false
	for _, rawID := range stats.Pack.SourceFileIDs {
		sourceSelected = sourceSelected || rawID == page.ValueLogSegmentID(sourcePtr.FileID)
	}
	if !sourceSelected {
		t.Fatalf("fixed source was not packed: ptr=%+v sources=%v", sourcePtr, stats.Pack.SourceFileIDs)
	}
	basis := db.cow.cut.basis
	ptr := capturePointer()
	created := false
	for _, rawID := range stats.Pack.CreatedFileIDs {
		created = created || rawID == page.ValueLogSegmentID(ptr.FileID)
	}
	if ptr == sourcePtr || !created {
		t.Fatalf("fixed leaf was not relocated to a created file: before=%+v after=%+v created=%v", sourcePtr, ptr, stats.Pack.CreatedFileIDs)
	}
	file, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(file, ptr.ValuePtr(), db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID != dictID || shape.Codec != valuelog.BlockCodecNone {
		t.Fatalf("real packed dictionary shape: %+v %v; want dictionary %d", shape, err, dictID)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	return db, backend, ptr, dictID, want
}

func TestCOWNativePackedDictionaryLeaf(t *testing.T) {
	for _, entryRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "leaf", true: "entry"}[entryRead], func(t *testing.T) {
			db, backend, ptr, dictID, want := cowNativePackedDictionaryLeafFixture(t)
			a, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			b, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			tx, err := backend.BeginNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			capture := a.(*NativeSemanticCapture)
			basis, budget := capture.cut.basis, db.cow.budget
			if b.(*NativeSemanticCapture).cut.basis != basis || tx.(*nativeConditionalTxn).snap.cowCut.basis != basis {
				t.Fatal("opening scopes do not retain the exact packed basis")
			}
			token, ok := basis.snapshot.StateToken()
			if !ok {
				t.Fatal("missing packed root")
			}
			read := func(scope backenddb.NativePhysicalReader, leafReader backenddb.NativePhysicalLeafReader) ([]byte, error) {
				if entryRead {
					entry, err := scope.NativeEntryAtRoot(token.RootPageID, []byte("pack-03000"), true)
					return entry.Value, err
				}
				var dst [page.PageSize]byte
				data, err := leafReader.NativeLeafPage(ptr, dst[:])
				if err != nil {
					return nil, err
				}
				if len(data) != page.PageSize || &data[0] != &dst[0] {
					return nil, errors.New("native leaf did not use caller destination")
				}
				view := node.NewNodeView(data)
				ordinal, found, err := view.SearchLeaf([]byte("pack-03000"))
				if err != nil || !found {
					return nil, errors.New("native dictionary leaf lost exact key")
				}
				entry, err := view.GetLeafEntry(ordinal)
				return entry.Value, err
			}
			readCapture := func() ([]byte, error) { return read(capture, capture) }
			// Original captures without an independent provider remain unsupported,
			// even when a definition could be looked up through a mutable DB.
			provider := capture.dictionary
			capture.dictionary = nil
			got, readErr := readCapture()
			capture.dictionary = provider
			if got != nil || !errors.Is(readErr, valuelog.ErrCOWReadUnsupported) {
				t.Fatalf("missing original provider: %d %v", len(got), readErr)
			}
			// Warm only admitted workspace/slot metadata. A real partial dictionary
			// preparation failure must refund everything it obtained and propagate.
			db.cow.physicalMu.Lock()
			shared, err := db.cow.physicalWorkspaceLocked(db.cow.readLimits())
			if err == nil {
				_, err = shared.addCodecSlot()
			}
			db.cow.physicalMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			before := budget.Stats()
			capture.dictionary = cowNativeDefinitionRefusal{provider}
			got, readErr = readCapture()
			capture.dictionary = provider
			after := budget.Stats()
			if got != nil || !errors.Is(readErr, errCOWNativeDefinitionRefused) || before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
				t.Fatalf("partial definition refusal/refund: len=%d err=%v before=%+v after=%+v", len(got), readErr, before, after)
			}
			// Exhaust the original finite parent, then retry after its real refund.
			before = budget.Stats()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
			if err != nil {
				t.Fatal(err)
			}
			got, readErr = readCapture()
			pressure.Close()
			if got != nil || !errors.Is(readErr, memtable.ErrCOWCapacity) {
				t.Fatalf("original dictionary admission refusal: %d %v", len(got), readErr)
			}
			check := func(scope backenddb.NativePhysicalReader, leafReader backenddb.NativePhysicalLeafReader) {
				t.Helper()
				got, err := read(scope, leafReader)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("original packed dictionary read: %q %v", got, err)
				}
				got[0] ^= 1 // subsequent reads must not observe caller mutation
			}
			check(capture, capture)
			check(tx.(backenddb.NativePhysicalReader), tx.(backenddb.NativePhysicalLeafReader))
			workspace := db.cow.physicalReader
			if workspace != shared || workspace.snapshot != nil || workspace.basis != nil || len(workspace.codecs) != 1 || workspace.codecs[0] == nil || workspace.codecs[0].id != dictID || workspace.codecs[0].definition == nil {
				t.Fatal("native leaf failed to share independently owned dictionary storage")
			}
			codec := workspace.codecs[0]
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			// A later checkpoint installs a new basis without changing held files,
			// original roots or the cache-owned definition/decoder lifetime.
			privateCOWPackWrite(t, db, backend, 6, 1)
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if db.cow.cut.basis == basis {
				t.Fatal("checkpoint did not replace packed basis")
			}
			for i := 0; i < 8; i++ {
				check(b.(backenddb.NativePhysicalReader), b.(backenddb.NativePhysicalLeafReader))
				check(tx.(backenddb.NativePhysicalReader), tx.(backenddb.NativePhysicalLeafReader))
			}
			if db.cow.physicalReader != workspace || workspace.codecs[0] != codec || len(basis.reader.codecs) != 0 {
				t.Fatal("basis handoff replaced or duplicated dictionary decoder")
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// After DB.Close the index pager is withdrawn; the held pointer/file
			// and already admitted dictionary remain valid through leaf copy-out.
			if !entryRead {
				check(nil, b.(backenddb.NativePhysicalLeafReader))
			}
		})
	}
}

// Regression-only policy control: factory-equivalent limits, the maintained pack
// producer, bounded cache and exported native physical entry API.
func TestCOWNativePackedDictionaryCompatibleCachePolicy(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 512 << 20
	limits.MaxTotalBytes, limits.MaxRetiredBytes = 1<<30, 1<<30
	db, backend, ptr, _, want := cowNativePackedDictionaryLeafFixtureWithReadPolicy(t, false, limits, 128)
	if db.cow.readLimits().MaxValueBytes != 64<<20 || db.cow.budget.Limits().MaxInFlightBytes != 512<<20 {
		t.Fatal("factory-equivalent policy not activated")
	}
	captured, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Close()
	capture := captured.(*NativeSemanticCapture)
	token, ok := capture.cut.basis.snapshot.StateToken()
	if !ok {
		t.Fatal("missing retained physical root")
	}
	f, _, err := capture.cut.basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID == 0 {
		t.Fatalf("actual maintained dictionary pack absent: %+v %v", shape, err)
	}
	hits := cowLeafCacheCounter(t, backend, "hits")
	for i := 0; i < 12; i++ {
		entry, err := capture.NativeEntryAtRoot(token.RootPageID, []byte("pack-03000"), true)
		if err != nil || !bytes.Equal(entry.Value, want) {
			t.Fatalf("native entry %d: %q %v", i, entry.Value, err)
		}
		entry.Value[0] ^= 1
	}
	if got := cowLeafCacheCounter(t, backend, "hits") - hits; got == 0 {
		t.Fatalf("compatible native dictionary reads bypassed verified cache: hits=%d canonical=%+v native_inflight=%d", got, db.cow.readLimits(), limits.MaxInFlightBytes)
	}
}

func TestCOWNativeDictionaryCacheBidirectionalPolicyAndConstruction(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 512 << 20
	limits.MaxTotalBytes = 1 << 30
	limits.MaxRetiredBytes = 1 << 30
	db, _, ptr, _, _ := cowNativePackedDictionaryLeafFixtureWithReadPolicy(t, false, limits, 128)
	// Two genuine dictionary segments make MaxResources=1 an insufficient
	// capture envelope. Reopening the real store starts its next append file;
	// the target dictionary and producer credit are never fabricated.
	db.cow.closePhysicalReader()
	if err := db.dictStore.(*dictdb.Store).Close(); err != nil {
		t.Fatal(err)
	}
	store, err := dictdb.Open(filepath.Join(filepath.Dir(db.dir), "dictdb"), backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutDictBytes(context.Background(), bytes.Repeat([]byte("second-real-definition"), 4096)); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	db.SetDictStore(store)
	basis := db.cow.cut.basis
	original := db.dictStore
	provider := original.(cowDictionaryReadProvider)
	independent := cowCacheDictionaryRead{provider: provider, cache: db.cow}
	native := valuelog.COWReadLimits{MaxRecordBytes: 512 << 20, MaxRawBytes: 512 << 20, MaxValueBytes: 512 << 20}
	canonical := db.cow.readLimits()
	w, err := admitCOWReadWorkspace(db.cow.budget, native)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	var dst [page.PageSize]byte
	read := func(policy valuelog.COWReadLimits, capability *cowCacheDictionaryRead) ([]byte, error) {
		w.limits = policy
		return basis.readPhysicalLeafWithDictionary(db.cow, w, ptr, dst[:0], true, false, capability)
	}
	for i := 0; i < 4; i++ {
		if _, err := read(native, &independent); err != nil {
			t.Fatal(err)
		}
	}
	db.cow.closePhysicalReader()
	calls := 0
	refusal := errors.New("definition construction refused")
	refusing := cowDictionaryLeafAdmissionStore{DictStore: original, prepare: func(_ uint64, _ valuelog.COWReadLimits, maxResources int, admit func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
		calls++
		if maxResources != db.cow.budget.Limits().MaxResources {
			t.Fatalf("changed original resource envelope: %d", maxResources)
		}
		// The original callback must still refuse an excessive resource map.
		sizes := dictdb.DictionaryReadAllocationSizes{}
		sizes.Snapshot.ValueLog.MapHint = maxResources + 1
		_, err := admit(sizes)
		if !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("resource construction admission: %v", err)
		}
		return nil, refusal
	}}
	denied := cowCacheDictionaryRead{provider: refusing, cache: db.cow}
	if _, err := read(canonical, &denied); !errors.Is(err, refusal) {
		t.Fatalf("canonical caller accepted broader native credit: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stricter policy did not consult original provider: %d", calls)
	}
	for i := 0; i < 4; i++ {
		if _, err := read(canonical, &independent); err != nil {
			t.Fatal(err)
		}
	}
	db.cow.closePhysicalReader()
	// The physical decoder owner, rather than the point's inspection workspace,
	// supplies the construction resource envelope. Use a genuinely admitted
	// smaller owner, not a provider assertion against the default envelope.
	smallLimits := db.cow.budget.Limits()
	smallLimits.MaxResources = 1
	smallBudget, err := memtable.NewCOWBudget(smallLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer smallBudget.Close()
	smallReader, err := admitCOWReadWorkspace(smallBudget, canonical)
	if err != nil {
		t.Fatal(err)
	}
	db.cow.physicalMu.Lock()
	db.cow.physicalReader = smallReader
	db.cow.physicalMu.Unlock()
	defer db.cow.closePhysicalReader()
	calls = 0
	if _, err := read(native, &denied); err != nil || calls != 0 {
		t.Fatalf("native failed to reuse actual canonical credit: calls=%d err=%v", calls, err)
	}
	if _, err := read(canonical, &denied); err != nil || calls != 0 {
		t.Fatalf("warm read constructed dictionary resources: calls=%d err=%v", calls, err)
	}
	if _, err := read(native, nil); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
		t.Fatalf("warm hit bypassed native capability: %v", err)
	}
	stricter := canonical
	stricter.MaxRecordBytes--
	// The actual provider must capture both real dictionary segments. Check
	// that the smaller physical construction limit is applied.
	smallCalls := 0
	smallProvider := cowDictionaryLeafAdmissionStore{DictStore: original, prepare: func(id uint64, policy valuelog.COWReadLimits, maxResources int, admit func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
		smallCalls++
		if maxResources != 1 {
			t.Fatalf("smaller physical envelope lost: %d", maxResources)
		}
		return provider.PrepareDictionaryReadDefinition(id, policy, maxResources, admit)
	}}
	smallCapability := cowCacheDictionaryRead{provider: smallProvider, cache: db.cow}
	if _, err := read(canonical, &smallCapability); err != nil || smallCalls != 0 {
		t.Fatalf("warm page constructed under smaller envelope: calls=%d err=%v", smallCalls, err)
	}
	if _, err := read(stricter, &smallCapability); !errors.Is(err, dictdb.ErrReadDefinitionCapacity) || smallCalls != 1 {
		t.Fatalf("cold construction ignored smaller resource envelope: calls=%d err=%v", smallCalls, err)
	}
}
