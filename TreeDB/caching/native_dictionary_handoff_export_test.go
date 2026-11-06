//go:build linux

package caching

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// These test-only exports compose the existing original-owner pack fixture with
// collections from caching_test, avoiding the production import cycle. Linux is
// the existing exact-handle pack promotion platform; there is no runtime skip.
func NewNativeDictionaryHandoffTestFixture(t *testing.T) (*DB, *backenddb.DB, uint64) {
	t.Helper()
	db, backend, _, dictID, _ := cowNativePackedDictionaryLeafFixtureWithEncoding(t, true)
	return db, backend, dictID
}

// PackNativeDictionaryHandoffTestFixture uses the SAME owner and maintained
// operation/adoption path. It seals existing lanes; it fabricates no catalog,
// root, pin or dictionary record and changes no codec or storage policy.
func PackNativeDictionaryHandoffTestFixture(db *DB) (backenddb.LeafGenerationPackRunOnceStats, error) {
	var stats backenddb.LeafGenerationPackRunOnceStats
	err := db.RunBackendMaintenance(func() error {
		for _, lane := range db.leafLogAppendLanesSnapshot() {
			err := func() error {
				lane.vlogMu.Lock()
				defer lane.vlogMu.Unlock()
				return db.rotateValueLogMuHeld(lane)
			}()
			if err != nil {
				return err
			}
		}
		var err error
		stats, err = db.backend.(*backenddb.DB).LeafGenerationPackRunOnceForPrivateCOW(db.cow.txnOwner, context.Background(), backenddb.LeafGenerationPackFromPlanOptions{
			Sync: true, Force: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1,
			MaxBytesToCopy: 32 << 20, ReserveRIDs: db.ReserveValueLogRIDs,
		})
		return err
	})
	return stats, err
}

// NativeDictionaryHandoffTestReceipt contains detached operation metadata.
// Neither the paired scope nor its Snapshots/definition owners escape the call.
type NativeDictionaryHandoffTestReceipt struct {
	Before, After                              backenddb.StateToken
	BeforeLeaf, AfterLeaf                      page.LeafLogPtr
	BeforeReads, AfterReads, Definitions       int
	DefinitionReleases                         int
	DictionarySHA256                           [32]byte
	DefinitionSHA256                           [32]byte
	DictionaryIndex, DictionaryFile            rootpublication.StableIdentity
	BeforeFile, AfterFile                      rootpublication.StableIdentity
	SourceSelected, DestinationCreated         bool
	UnscopedRefused, ReversedRefused, Refunded bool
	Pack                                       backenddb.LeafGenerationPackRunOnceStats
}

type nativeDictionaryHandoffForwarder struct {
	scope                 *cowMaintenanceLeafScope
	dictID                uint64
	beforeLeaf, afterLeaf page.LeafLogPtr
	before, after         int
	beforeSeen, afterSeen bool
}

func (r *nativeDictionaryHandoffForwarder) MatchesNativeMaintenanceSnapshots(before, after *backenddb.Snapshot) bool {
	return r.scope.MatchesNativeMaintenanceSnapshots(before, after)
}

func (r *nativeDictionaryHandoffForwarder) ReadNativeMaintenanceDictionaryLeaf(snapshot *backenddb.Snapshot, ptr page.LeafLogPtr, shape valuelog.COWRecordShape, dst []byte) ([]byte, error) {
	// Fixed tree reads pass caller-owned page backing as leafScratch[:0].
	// Validate its full page capacity; decoding must return that same backing.
	if !shape.Compressed || shape.DictID != r.dictID || shape.Codec != valuelog.BlockCodecNone || cap(dst) != page.PageSize {
		return nil, fmt.Errorf("real engine dictionary shape/scratch: compressed=%t dictionaryMatches=%t codec=%d scratchLen=%d scratchCap=%d", shape.Compressed, shape.DictID == r.dictID, shape.Codec, len(dst), cap(dst))
	}
	got, err := r.scope.ReadNativeMaintenanceDictionaryLeaf(snapshot, ptr, shape, dst)
	if err != nil {
		return nil, err
	}
	if len(got) != page.PageSize || cap(got) != page.PageSize || &got[0] != &dst[:page.PageSize][0] {
		return nil, errors.New("real dictionary reader did not fill caller scratch")
	}
	switch snapshot {
	case r.scope.before:
		r.before++
		r.beforeSeen = r.beforeSeen || ptr == r.beforeLeaf
	case r.scope.after:
		r.after++
		r.afterSeen = r.afterSeen || ptr == r.afterLeaf
	default:
		return nil, backenddb.ErrRootPublicationBasisMismatch
	}
	return got, nil
}

// Capture only the pointer visited by an actual SYSTEM key lookup. The sentinel
// proves provenance, never successful decoding; the real engine must subsequently
// decode this exact pointer on BOTH operands through the forwarding reader.
func nativeDictionaryCatalogPointer(snapshot *backenddb.Snapshot) (page.LeafLogPtr, error) {
	token, ok := snapshot.StateToken()
	if !ok || token.SystemRootPageID == 0 {
		return page.LeafLogPtr{}, backenddb.ErrRootPublicationBasisMismatch
	}
	reader, err := snapshot.NativeReaderAtRoot(token.SystemRootPageID)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	var key, leaf [page.PageSize]byte
	var ptr page.LeafLogPtr
	stop := errors.New("capture actual native SYSTEM operand")
	_, err = reader.GetEntryExactWithFixedScratch([]byte("collections/native/v1/next-incarnation"), key[:], leaf[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
		ptr = p
		return nil, stop
	})
	if !errors.Is(err, stop) || ptr.FileID == 0 {
		return page.LeafLogPtr{}, fmt.Errorf("native SYSTEM dictionary operand unavailable: pointer=%+v error=%v", ptr, err)
	}
	return ptr, nil
}

// CheckNativeDictionaryHandoffForTest borrows the FIRST owner's real capture.
// The fixed engine interface is invoked under writerMu, exactly as in install;
// all potentially blocking semantic destruction/join occurs AFTER the maintained
// maintenance call has dropped flushMu. Partial results retain original custody.
func CheckNativeDictionaryHandoffForTest(db *DB, capture backenddb.NativeSemanticCapture, dictID uint64) (receipt NativeDictionaryHandoffTestReceipt, resultErr error) {
	c := db.cow
	backend := db.backend.(*backenddb.DB)
	var after *cowBackendBasis
	var scope *cowMaintenanceLeafScope
	var states [3]backenddb.NativeSemanticState
	var provider *cowMaintenanceProviderControl
	defer func() {
		// RunBackendMaintenance's own deferred unlocks have already executed,
		// including on panic/Goexit. Release only detaches original cleanup work.
		for _, state := range states {
			if state != nil {
				record := state.Release()
				c.semanticMu.Lock()
				c.enqueueSemanticLocked(record)
				c.semanticMu.Unlock()
				c.wakeSemanticCleanup()
			}
		}
		if scope != nil {
			receipt.Definitions = scope.used
			if scope.used == 1 && scope.entries[0].owner != nil {
				receipt.DefinitionSHA256 = sha256.Sum256(scope.entries[0].owner.Bytes)
			}
			c.retireCOWMaintenanceLeaves(&scope)
		}
		joinErr := db.joinSemanticCleanupFrontier()
		resultErr = errors.Join(resultErr, joinErr)
		if after != nil {
			resultErr = errors.Join(resultErr, after.release())
		}
		// Only a successful original worker join synchronizes its counters.
		if joinErr == nil && provider != nil {
			receipt.DefinitionReleases = provider.releases
			receipt.Refunded = provider.releases > 0 && !provider.underGate
		}
	}()
	// Bind the persisted definition's actual producer-owned index/file closure
	// before entering maintenance. Release this independent diagnostic capture
	// immediately: it must not supply the paired scope's read or pin authority.
	resultErr = func() (captureErr error) {
		resources, err := db.dictStore.(*dictdb.Store).CaptureDictionaryResources(context.Background(), dictID)
		if err != nil {
			return err
		}
		defer func() { captureErr = errors.Join(captureErr, resources.ReleaseWithError()) }()
		for _, token := range resources.Tokens() {
			obligations := token.LogicalObligations()
			if len(obligations) != 1 || obligations[0].Generation != dictID || obligations[0].FileID != dictID || obligations[0].Length == 0 {
				return errors.New("persisted dictionary closure has the wrong definition identity")
			}
			receipt.DictionarySHA256 = obligations[0].Digest
			switch token.LogicalLane() {
			case "dictdb/index":
				receipt.DictionaryIndex = token.Identity()
			case "dictdb/value-log":
				receipt.DictionaryFile = token.Identity()
			}
		}
		if receipt.DictionaryIndex.ObjectID == ([16]byte{}) || receipt.DictionaryFile.ObjectID == ([16]byte{}) {
			return errors.New("actual dictionary index/file identity unavailable")
		}
		return nil
	}()
	if resultErr != nil {
		return receipt, resultErr
	}
	resultErr = db.RunBackendMaintenance(func() error {
		var err error
		// This second pack rewrites the already packed, sealed real catalog.
		// Force selects dense sources but does not waive owner/root validation.
		receipt.Pack, err = backend.LeafGenerationPackRunOnceForPrivateCOW(c.txnOwner, context.Background(), backenddb.LeafGenerationPackFromPlanOptions{
			Sync: true, Force: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1,
			MaxBytesToCopy: 32 << 20, ReserveRIDs: db.ReserveValueLogRIDs,
		})
		if err != nil {
			return err
		}
		if !receipt.Pack.Ran || receipt.Pack.Pack.BytesCopied == 0 {
			return errors.New("native dictionary handoff pack did no physical work")
		}
		after, err = c.captureBackendBasis(backend)
		if err != nil {
			return err
		}
		var beforeOK, afterOK bool
		receipt.Before, beforeOK = capture.Snapshot().StateToken()
		receipt.After, afterOK = after.snapshot.StateToken()
		if !beforeOK || !afterOK || receipt.Before.SystemRootPageID == receipt.After.SystemRootPageID || receipt.Before.AppliedCommandLSN != receipt.After.AppliedCommandLSN {
			return backenddb.ErrRootPublicationBasisMismatch
		}
		receipt.BeforeLeaf, err = nativeDictionaryCatalogPointer(capture.Snapshot())
		if err != nil {
			return err
		}
		receipt.AfterLeaf, err = nativeDictionaryCatalogPointer(after.snapshot)
		if err != nil {
			return err
		}
		for _, rawID := range receipt.Pack.Pack.SourceFileIDs {
			receipt.SourceSelected = receipt.SourceSelected || rawID == page.ValueLogSegmentID(receipt.BeforeLeaf.FileID)
		}
		for _, rawID := range receipt.Pack.Pack.CreatedFileIDs {
			receipt.DestinationCreated = receipt.DestinationCreated || rawID == page.ValueLogSegmentID(receipt.AfterLeaf.FileID)
		}
		if receipt.BeforeLeaf == receipt.AfterLeaf || !receipt.SourceSelected || !receipt.DestinationCreated {
			return errors.New("actual native SYSTEM dictionary operand was not relocated")
		}
		beforeFile, beforeIdentity, err := capture.Snapshot().PinnedValueLogFile(receipt.BeforeLeaf.ValueLogFileID())
		receipt.BeforeFile = beforeIdentity
		if err != nil {
			return err
		}
		afterFile, afterIdentity, err := after.snapshot.PinnedValueLogFile(receipt.AfterLeaf.ValueLogFileID())
		receipt.AfterFile = afterIdentity
		if err != nil || receipt.BeforeFile.ObjectID == ([16]byte{}) || receipt.AfterFile.ObjectID == ([16]byte{}) || receipt.BeforeFile == receipt.AfterFile {
			return errors.Join(errors.New("actual SYSTEM source/destination identities unavailable or unchanged"), err)
		}
		// Relocation alone does not prove dictionary encoding: publication can
		// append a new SYSTEM leaf. Inspect BOTH exact captured records under
		// their own snapshot authority before invoking any real engine refresh.
		beforeShape, err := valuelog.InspectCOWLeafRecord(beforeFile, receipt.BeforeLeaf.ValuePtr(), c.readLimits())
		if err != nil {
			return fmt.Errorf("inspect actual BEFORE SYSTEM operand: %w", err)
		}
		if !beforeShape.Compressed || beforeShape.DictID != dictID || beforeShape.Codec != valuelog.BlockCodecNone {
			return fmt.Errorf("actual BEFORE SYSTEM dictionary shape: compressed=%t dictionaryMatches=%t codec=%d", beforeShape.Compressed, beforeShape.DictID == dictID, beforeShape.Codec)
		}
		afterShape, err := valuelog.InspectCOWLeafRecord(afterFile, receipt.AfterLeaf.ValuePtr(), c.readLimits())
		if err != nil {
			return fmt.Errorf("inspect actual AFTER SYSTEM operand: %w", err)
		}
		if !afterShape.Compressed || afterShape.DictID != dictID || afterShape.Codec != valuelog.BlockCodecNone {
			return fmt.Errorf("actual AFTER SYSTEM dictionary shape: compressed=%t dictionaryMatches=%t codec=%d", afterShape.Compressed, afterShape.DictID == dictID, afterShape.Codec)
		}
		store := db.dictStore.(*dictdb.Store)
		definition, err := store.GetDictBytes(context.Background(), dictID)
		if err != nil || len(definition) == 0 {
			return errors.Join(errors.New("persisted dictionary definition unavailable"), err)
		}
		if receipt.DictionarySHA256 != sha256.Sum256(definition) {
			return errors.New("persisted dictionary closure and definition bytes differ")
		}
		scope, err = db.prepareCOWMaintenanceLeaves()
		if err != nil {
			return err
		}
		scope.before, scope.after = capture.Snapshot(), after.snapshot
		provider = &cowMaintenanceProviderControl{store: store, cache: c}
		scope.provider = provider
		forward := &nativeDictionaryHandoffForwarder{scope: scope, dictID: dictID, beforeLeaf: receipt.BeforeLeaf, afterLeaf: receipt.AfterLeaf}
		return func() error {
			c.writerMu.Lock()
			defer c.writerMu.Unlock()
			if c.cut.basis.snapshot != capture.Snapshot() || c.cut.semanticRootSet != capture.State() || c.cut.logicalSeq != capture.LogicalSequence() {
				return backenddb.ErrRootPublicationBasisMismatch
			}
			refresher, ok := capture.State().(backenddb.NativeSemanticMaintenanceBasisRefresher)
			unscoped, unscopedOK := capture.State().(backenddb.NativeSemanticBasisRefresher)
			if !ok || !unscopedOK {
				return backenddb.ErrNativeSemanticUnsupported
			}
			var unscopedErr, reversedErr error
			states[0], unscopedErr = unscoped.PrepareBasisRefresh(scope.before, scope.after)
			receipt.UnscopedRefused = states[0] == nil && errors.Is(unscopedErr, valuelog.ErrCOWReadUnsupported)
			states[1], reversedErr = refresher.PrepareBasisRefreshWithMaintenanceLeaves(scope.after, scope.before, forward)
			receipt.ReversedRefused = states[1] == nil && errors.Is(reversedErr, backenddb.ErrRootPublicationBasisMismatch) && forward.before == 0 && forward.after == 0
			if !receipt.UnscopedRefused || !receipt.ReversedRefused {
				return errors.Join(errors.New("real engine dictionary negative controls did not refuse"), unscopedErr, reversedErr)
			}
			ordinary := c.budget.Stats()
			states[2], err = refresher.PrepareBasisRefreshWithMaintenanceLeaves(scope.before, scope.after, forward)
			receipt.BeforeReads, receipt.AfterReads = forward.before, forward.after
			if err != nil {
				return err
			}
			if states[2] == nil || states[2] == capture.State() || !forward.beforeSeen || !forward.afterSeen || forward.before == 0 || forward.after == 0 || provider.calls != 1 || scope.used != 1 {
				return errors.New("real engine did not rebind through both authenticated SYSTEM dictionary operands")
			}
			if got := c.budget.Stats(); got.ExternalBytes != ordinary.ExternalBytes || got.ExternalLeases != ordinary.ExternalLeases {
				return errors.New("real maintenance dictionary refresh borrowed ordinary capacity")
			}
			return nil
		}()
	})
	return receipt, resultErr
}

func CloseNativeDictionaryHandoffTestFixture(db *DB) error {
	c := db.cow
	registry := db.backend.(*backenddb.DB).StableResourceIdentityPinRegistry()
	if err := db.Close(); err != nil {
		return err
	}
	if c.budget.Stats().TotalBytes != 0 || c.maintenance.Stats().TotalBytes != 0 || registry.ActivePins() != 0 {
		return errors.New("native dictionary fixture retained budget or physical pins after checked Close")
	}
	return nil
}

// Reopen only the persisted fixture; no corpus is regenerated and no pack runs.
func ReopenNativeDictionaryHandoffTestFixture(t *testing.T, original *DB) (*DB, *backenddb.DB) {
	t.Helper()
	dir := filepath.Dir(original.dir)
	store, err := dictdb.Open(filepath.Join(dir, "dictdb"), backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := backenddb.Open(backenddb.Options{
		Dir: filepath.Join(dir, "backend"), CommandWAL: true,
		Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true,
		IndexOuterLeavesInValueLog: true, LeafPageReadCacheEntries: -1,
		ValueLog: backenddb.ValueLogOptions{
			DictLookup:          func(id uint64) ([]byte, error) { return store.GetDictBytes(context.Background(), id) },
			DictCurrentForClass: store.GetCurrentForClass,
			DictLeafPayloadMode: store.GetLeafPayloadMode,
		},
	})
	if err != nil {
		t.Fatal(errors.Join(err, store.Close()))
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(errors.Join(err, backend.Close(), store.Close()))
	}
	db, err := Open(dir, backend, Options{
		TransactionOwner: owner, MemtableMode: "cow_ordered", ExternalCommandWAL: true,
		MemtableShards: 2, FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold: 256, ValueLogCompression: uint8(vlogCompressionBlock),
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogGenerationLeafSegmentTargetBytes: 8 << 20,
	})
	if err != nil {
		t.Fatal(errors.Join(err, backend.Close(), store.Close()))
	}
	db.SetDictStore(store)
	t.Cleanup(func() {
		if !db.closing.Load() {
			if err := CloseNativeDictionaryHandoffTestFixture(db); err != nil {
				t.Error(err)
			}
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		t.Fatal(err)
	}
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatal(err)
	}
	return db, backend
}
