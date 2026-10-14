package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestOfflineVacuumOriginalSourceCloseRefusalRetainsLock(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ChunkSize: 64 << 10}
	seed, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	var original *indexGen
	var sourceBorrows uint64
	attempts := 0
	previous := testDBCloseHook
	testDBCloseHook = func(d *DB) {
		if _, err := os.Stat(filepath.Join(dir, indexNewFileName)); err != nil {
			return
		}
		original = d.idx.Load()
		if original == nil || original.primary != nil {
			t.Fatalf("fixture missed ordinary DATA source: %p", original)
		}
		sourceBorrows = original.wrapperBorrows
		attempts++
		if err := original.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
			t.Errorf("physical fault: %v", err)
		}
	}
	defer func() { testDBCloseHook = previous }()
	err = VacuumIndexOffline(opts)
	if !errors.Is(err, os.ErrClosed) || attempts != 1 {
		t.Fatalf("actual source physical refusal lost: attempts=%d err=%v", attempts, err)
	}
	var receipt *OpenRecoveryCleanupError
	if !errors.As(err, &receipt) || receipt.generation == nil || receipt.generation.startupDB == nil ||
		receipt.generation != original || receipt.generation.startupLock == nil ||
		receipt.generation.startupCustody != nil ||
		receipt.generation.wrapperBorrows != sourceBorrows ||
		receipt.generation.metadataCharge == 0 {
		t.Fatalf("offline cleanup dropped original source/replacement/LOCK custody: source=%p receipt=%p borrows=%d before=%d err=%v",
			original, receipt, original.wrapperBorrows, sourceBorrows, err)
	}
	g := receipt.generation
	if g.startupDB.idx.Load() != nil {
		t.Fatal("source discovery was not withdrawn by real Close")
	}
	second, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(lockErr, lockfile.ErrLocked) {
		t.Fatalf("offline source refusal released LOCK: %v", lockErr)
	}
	charge := g.metadata.Bytes()
	if receipt.Close() != receipt.Unwrap() ||
		g.metadata.Bytes() != charge || attempts != 1 {
		t.Fatal("repeat completion replayed or refunded physical refusal")
	}
}

func TestOfflineReplacementOriginalPagerCompletion(t *testing.T) {
	for _, fault := range []bool{false, true} {
		name := "consumed"
		if fault {
			name = "physical-refusal"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ChunkSize: 64 << 10}
			seed, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err = seed.SetSync([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err = seed.Close(); err != nil {
				t.Fatal(err)
			}
			lock, err := lockfile.Acquire(filepath.Join(dir, "LOCK"))
			if err != nil {
				t.Fatal(err)
			}
			opts.ReadOnly = true
			source, err := openReadOnlyNoLockWithRegistryV6(opts, rootpublication.NewIdentityPinRegistry(), lock)
			if err != nil {
				t.Fatal(err)
			}
			replacement, err := newStartupIndexGeneration(source, lock)
			if err != nil {
				t.Fatal(err)
			}
			if replacement.metadataCharge == 0 || replacement.startupCustody == nil {
				t.Fatal("original containing admission missing")
			}
			if err = replacement.openOfflineReplacementPagerV1(dir, opts.ChunkSize); err != nil {
				t.Fatal(err)
			}
			originalPager := replacement.pager
			if fault {
				if err = originalPager.WithStableResourceFile(func(file *os.File) error { return file.Close() }); err != nil {
					t.Fatal(err)
				}
			}
			operation := errors.New("original offline operation")
			completionOwner, completionCharge, first := replacement.finishOfflineReplacementV1(operation)
			if !errors.Is(first, operation) {
				t.Fatalf("lost operation: %v", first)
			}
			if fault {
				var receipt *OpenRecoveryCleanupError
				if !errors.Is(first, os.ErrClosed) || !errors.As(first, &receipt) || receipt.generation != replacement ||
					replacement.pager != originalPager || replacement.startupDB != source || replacement.startupLock != lock || replacement.metadataCharge == 0 {
					t.Fatalf("original physical Pager custody lost: %v", first)
				}
				charge := replacement.metadata.Bytes()
				nextOwner, nextCharge, nextErr := replacement.finishOfflineReplacementV1(nil)
				if completionOwner != nil || completionCharge != 0 || nextOwner != nil || nextCharge != 0 ||
					nextErr != first || replacement.metadata.Bytes() != charge || receipt.Close() != receipt.Unwrap() {
					t.Fatal("repeated completion retried or refunded")
				}
				other, e := lockfile.Acquire(filepath.Join(dir, "LOCK"))
				if other != nil {
					_ = other.Close()
				}
				if !errors.Is(e, lockfile.ErrLocked) {
					t.Fatalf("physical refusal lost LOCK: %v", e)
				}
			} else {
				var receipt *OpenRecoveryCleanupError
				if errors.As(first, &receipt) || replacement.metadataCharge != 0 || replacement.startupDB != nil || replacement.startupCustody != nil || replacement.pager != nil {
					t.Fatalf("successful original cleanup retained/refunded incorrectly: %v", first)
				}
				if completionOwner == nil || completionCharge == 0 {
					t.Fatal("actual consumed replacement did not return containing completion")
				}
				replacement, originalPager = nil, nil
				completionOwner.RemovePending(completionCharge)
				if completionOwner.Bytes() != 0 {
					t.Fatalf("successful original completion left charge: %d", completionOwner.Bytes())
				}
				if err = lock.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// Real proof builds append through the same leaf writer after both selected
// slot trees have already been captured. Every resulting reference must enter
// the final manifest before its record/capsule is sealed.
func TestVacuumPrimaryProofOuterLeafSegmentsSealAfterAllRoots(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true, ChunkSize: 64 << 10, KeepRecent: 1, PreferAppendAlloc: true, IndexOuterLeavesInValueLog: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{MaxSegmentBytes: 512, Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	defer func() {
		if d != nil {
			if e := d.Close(); e != nil {
				t.Error(e)
			}
		}
		if e := log.Close(); e != nil {
			t.Error(e)
		}
	}()
	for gen := 1; gen <= 4; gen++ {
		batch := d.NewBatch()
		for i := 0; i < 384; i++ {
			if err = batch.Set([]byte(fmt.Sprintf("key/%06d", i)), bytes.Repeat([]byte{byte(gen)}, 96)); err != nil {
				t.Fatal(err)
			}
		}
		if err = batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
		if err = batch.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if d.idx.Load().primary == nil || !d.idx.Load().primary.CapsuleFormatV6() {
		t.Fatal("fixture missed actual V6 producer")
	}
	beforeProof := map[uint32]bool{}
	d.vacuumPagerSyncHook = func(phase vacuumPagerSyncPhase) {
		if phase != vacuumPagerSyncFinal {
			return
		}
		files, e := listLeafGenerationBootstrapFiles(LeafLogDirPath(dir))
		if e != nil {
			t.Fatal(e)
		}
		for _, f := range files {
			beforeProof[f.rawFileID] = true
		}
	}
	if err = d.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.vacuumPagerSyncHook = nil
	if len(beforeProof) == 0 {
		t.Fatal("fixture missed actual selected-slot leaf output")
	}
	manifest := map[uint32]bool{}
	for _, gen := range d.leafGenerationManifest.Generations {
		for _, id := range gen.FileIDs {
			manifest[id] = true
		}
	}
	basis := d.recoverableDurableBasisLocked()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("capture")
	}
	lateProofRefs := 0
	for i, rootID := range basis.primaryReadRoots {
		if rootID == 0 {
			t.Fatalf("missing actual slot/proof %d", i)
		}
		refs := collectLeafRefIDsFromRoot(t, d, rootID)
		if len(refs) == 0 {
			t.Fatalf("slot/proof %d lost actual outer-leaf dispatch", i)
		}
		for ptr := range refs {
			if !manifest[ptr.FileID] {
				t.Fatalf("slot/proof %d segment %d absent from final manifest", i, ptr.FileID)
			}
			if i >= 2 && !beforeProof[ptr.FileID] {
				lateProofRefs++
			}
		}
		got, e := snap.GetAtRoot(rootID, []byte("key/000000"))
		if e != nil || len(got) != 96 {
			t.Fatalf("slot/proof %d unreadable: %v", i, e)
		}
	}
	if err = snap.Close(); err != nil {
		t.Fatal(err)
	}
	if lateProofRefs == 0 {
		t.Fatal("fixture did not create real late proof leaf segments")
	}
	latest := d.durableRoot.slot
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, primaryIndexFileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	image := make([]byte, 3*page.PageSize)
	offset := int64(2+latest*3) * page.PageSize
	if _, err = f.ReadAt(image, offset); err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), image...)
	bad[48] ^= 1
	if _, err = f.WriteAt(bad, offset); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	ro := opts
	ro.ReadOnly = true
	fallback, err := Open(ro)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fallback.Get([]byte("key/000000"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{3}, 96)) {
		t.Fatalf("independent fallback leaf closure: %v", err)
	}
	if err = fallback.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(image, offset); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	got, err = reopened.Get([]byte("key/000000"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{4}, 96)) {
		t.Fatalf("latest leaf closure: %v", err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

// Canonical rebinding consumes only the old proof. The admitted exact parents
// stay in their original DATA generation and PRIMARY owner until terminal Close.
func TestPrimaryJointNamespaceRebindKeepsOriginalParents(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(map[bool]string{false: "consumed-proof", true: "physical-parent-refusal"}[fault], func(t *testing.T) {
			dir := t.TempDir()
			d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			if err = d.SetSync([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			g := d.idx.Load()
			if g.primaryOwner == nil {
				t.Fatal("fixture missed original PRIMARY owner")
			}
			for _, item := range []struct {
				name string
				p    *pager.Pager
			}{{indexFileName, g.pager}, {primaryIndexFileName, g.primary.Pager()}} {
				tok, e := g.stablePagerNamespaceToken(dir, item.name, item.p)
				if e != nil {
					t.Fatal(e)
				}
				if e = tok.ReleaseWithError(); e != nil {
					t.Fatal(e)
				}
			}
			dataParent, primaryParent := g.stableNamespaceParent, g.primaryOwner.namespaceParent
			dataCharge, primaryCharge := g.stableNamespaceParentCharge, g.primaryOwner.namespaceParentCharge
			if dataParent == nil || primaryParent == nil || dataCharge == 0 || primaryCharge == 0 {
				t.Fatal("original admitted parent birth missing")
			}
			if fault {
				if err = dataParent.Close(); err != nil {
					t.Fatal(err)
				}
			}
			first := g.rebindPrimaryJointNamespaceV6(dir)
			if g.stableNamespaceParent != dataParent || g.primaryOwner.namespaceParent != primaryParent ||
				g.stableNamespaceParentCharge != dataCharge || g.primaryOwner.namespaceParentCharge != primaryCharge {
				t.Fatal("canonical rebind replaced/refunded original admitted parents")
			}
			if fault {
				if !errors.Is(first, syscall.EBADF) && !errors.Is(first, os.ErrClosed) {
					t.Fatalf("original parent refusal lost: %v", first)
				}
				d.retainOriginalCleanupError(first)
				before := g.metadata.Bytes()
				if again := g.rebindPrimaryJointNamespaceV6(dir); again != first || g.metadata.Bytes() != before {
					t.Fatalf("failed rebinding replayed or refunded: %v", again)
				}
				// Genuine terminal cleanup still observes the failed original parent.
				if err = d.Close(); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("terminal custody lost: %v", err)
				}
			} else {
				if first != nil {
					t.Fatal(first)
				}
				if err = d.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// Exercise the actual standalone-leaf setup refusal before a replacement
// generation can exist. The unrelated directory obstruction is intentional;
// the physical source Close refusal must still preserve its original LOCK.
func TestOfflineVacuumSetupRefusalRetainsOriginalSourceLock(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true,
		ChunkSize: 64 << 10, IndexOuterLeavesInValueLog: true}
	seed, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	// Read-only Open does not consume the unused column-assets namespace.
	// The actual maintenance writer must create/validate all layout directories.
	obstruction := filepath.Join(dir, "column_assets")
	if err = os.Remove(obstruction); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err = os.WriteFile(obstruction, []byte("setup obstruction"), 0600); err != nil {
		t.Fatal(err)
	}
	var original *indexGen
	attempts := 0
	old := testDBCloseHook
	testDBCloseHook = func(d *DB) {
		original = d.idx.Load()
		if original == nil || !d.readOnly || original.primary != nil {
			t.Fatalf("missed actual ordinary read-only source: %p", original)
		}
		attempts++
		if err := original.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
			t.Errorf("actual source physical fault: %v", err)
		}
	}
	defer func() { testDBCloseHook = old }()
	err = VacuumIndexOffline(opts)
	var receipt *OpenRecoveryCleanupError
	if attempts != 1 || !errors.Is(err, os.ErrClosed) || !errors.As(err, &receipt) ||
		receipt.generation != original || original.startupDB == nil ||
		original.startupLock == nil || original.metadataCharge == 0 {
		t.Fatalf("setup refusal lost actual source Close/LOCK custody: attempts=%d err=%v", attempts, err)
	}
	if _, err := os.Stat(filepath.Join(dir, indexNewFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("setup refusal reached replacement effects: %v", err)
	}
	other, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(lockErr, lockfile.ErrLocked) {
		t.Fatalf("setup/source refusal released original LOCK: %v", lockErr)
	}
	charge := original.metadata.Bytes()
	if receipt.Close() != receipt.Unwrap() || attempts != 1 || original.metadata.Bytes() != charge {
		t.Fatal("repeated refusal replayed physical cleanup or refunded original source")
	}
}

// Refuse the actual replacement-generation admission after capturing the real
// read-only source. Refusal must not allocate a replacement or change its source
// borrow, and failed source disposal must retain the preborn original receipt.
func TestOfflineVacuumReplacementAdmissionRefusalRetainsOriginalSource(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ChunkSize: 64 << 10}
	seed, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	d, err := openReadOnlyNoLockWithRegistryV6(opts, registry, lock)
	if err != nil {
		t.Fatal(err)
	}
	source := d.idx.Load()
	source.retainOfflineSourceV1(d, lock)
	if source.startupCustody != nil || source.startupManager != d.valueLogManager || source.startupLock != lock {
		t.Fatal("source did not reuse actual successful-Open operands")
	}
	owner := source.metadata
	budget := &originalClosureAdmissionBudget{limit: math.MaxUint64}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	before, borrows := owner.Bytes(), source.wrapperBorrows
	budget.setLimit(before)
	replacement, admissionErr := newStartupIndexGeneration(d, lock)
	budget.setLimit(math.MaxUint64)
	if replacement != nil || !errors.Is(admissionErr, retainedalloc.ErrCapacity) ||
		owner.Bytes() != before || source.wrapperBorrows != borrows {
		t.Fatalf("refused original replacement changed source/admission: replacement=%p err=%v", replacement, admissionErr)
	}
	enrollment.Close()
	attempts := 0
	previous := testDBCloseHook
	testDBCloseHook = func(actual *DB) {
		if actual != d {
			t.Fatal("missed original source")
		}
		attempts++
		if err := source.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
			t.Error(err)
		}
	}
	defer func() { testDBCloseHook = previous }()
	metadata, charge, refusal := source.finishOfflineSourceV1(admissionErr)
	var receipt *OpenRecoveryCleanupError
	if metadata != nil || charge != 0 || attempts != 1 ||
		!errors.Is(refusal, retainedalloc.ErrCapacity) || !errors.Is(refusal, os.ErrClosed) ||
		!errors.As(refusal, &receipt) || receipt.generation != source ||
		source.startupDB != d || source.startupLock != lock ||
		// Completed physical-Close call borrows drain on refusal; the original
		// maintenance source borrow and failed physical receipt remain.
		source.wrapperBorrows != borrows || source.metadataCharge == 0 {
		t.Fatalf("admission/source Close lost original custody: completion=%p/%d attempts=%d source=%p receipt=%p borrows=%d before=%d err=%v",
			metadata, charge, attempts, source, receipt, source.wrapperBorrows, borrows, refusal)
	}
	registry.Close()
	charged := owner.Bytes()
	if againMetadata, againCharge, again := source.finishOfflineSourceV1(nil); again != receipt ||
		againMetadata != nil || againCharge != 0 || attempts != 1 || owner.Bytes() != charged {
		t.Fatal("repeated source cleanup replayed or refunded")
	}
	other, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(lockErr, lockfile.ErrLocked) {
		t.Fatalf("source admission failure lost LOCK: %v", lockErr)
	}
}

// An ordinary setup refusal with successful source disposal is not physical
// failure debt. It drains the original containing contribution and LOCK.
func TestOfflineVacuumSetupRefusalConsumesOriginalSourceSuccessfully(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true,
		ChunkSize: 64 << 10, IndexOuterLeavesInValueLog: true}
	seed, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	obstruction := filepath.Join(dir, "column_assets")
	if err = os.Remove(obstruction); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err = os.WriteFile(obstruction, []byte("setup obstruction"), 0600); err != nil {
		t.Fatal(err)
	}
	var owner *retainedalloc.Owner
	attempts := 0
	previous := testDBCloseHook
	testDBCloseHook = func(d *DB) {
		g := d.idx.Load()
		if g == nil || g.startupDB != d || g.startupManager != d.valueLogManager ||
			g.startupLock == nil || g.wrapperBorrows != 1 {
			t.Fatal("original source borrow missing")
		}
		owner = g.metadata
		attempts++
	}
	defer func() { testDBCloseHook = previous }()
	err = VacuumIndexOffline(opts)
	var receipt *OpenRecoveryCleanupError
	if err == nil || errors.As(err, &receipt) || errors.Is(err, ErrRecoveryRequired) ||
		attempts != 1 || owner == nil || owner.Bytes() != 0 {
		t.Fatalf("successful source cleanup retained false refusal/debt: attempts=%d err=%v", attempts, err)
	}
	reopened, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if lockErr != nil {
		t.Fatalf("successful source cleanup retained LOCK: %v", lockErr)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

// The no-segment refusal occurs after the genuine read-only source has opened
// and before any rewrite writer/replacement exists. Its actual physical Close
// result must determine original source/LOCK custody.
func TestOfflineRewriteNoSegmentsOriginalSourceCompletion(t *testing.T) {
	for _, fault := range []bool{false, true} {
		name := "consumed"
		if fault {
			name = "physical-refusal"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, DisableBackgroundPrune: true, ChunkSize: 64 << 10}
			seed, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err = seed.Close(); err != nil {
				t.Fatal(err)
			}
			var original *indexGen
			var owner *retainedalloc.Owner
			attempts := 0
			previous := testDBCloseHook
			testDBCloseHook = func(d *DB) {
				original = d.idx.Load()
				if original == nil || !d.readOnly || original.primary != nil {
					t.Fatal("fixture missed original ordinary DATA source")
				}
				owner = original.metadata
				attempts++
				if fault {
					if err := original.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
						t.Error(err)
					}
				}
			}
			defer func() { testDBCloseHook = previous }()
			_, err = ValueLogRewriteOffline(opts)
			if err == nil || !strings.Contains(err.Error(), "no value-log segments found") || attempts != 1 {
				t.Fatalf("actual no-segment refusal missing: attempts=%d err=%v", attempts, err)
			}
			var receipt *OpenRecoveryCleanupError
			if fault {
				if !errors.Is(err, os.ErrClosed) || !errors.As(err, &receipt) ||
					receipt.generation != original || original.startupDB == nil ||
					original.startupLock == nil || original.metadataCharge == 0 {
					t.Fatalf("rewrite source physical refusal lost original receipt/LOCK: %v", err)
				}
				charged := owner.Bytes()
				if receipt.Close() != receipt.Unwrap() || owner.Bytes() != charged || attempts != 1 {
					t.Fatal("repeat receipt observation replayed or refunded actual refusal")
				}
			} else {
				if errors.As(err, &receipt) || errors.Is(err, ErrRecoveryRequired) || owner.Bytes() != 0 {
					t.Fatalf("successful no-segment source completion retained false debt: %v", err)
				}
			}
			next, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
			if fault {
				if next != nil {
					_ = next.Close()
				}
				if !errors.Is(lockErr, lockfile.ErrLocked) {
					t.Fatalf("failed source lost LOCK: %v", lockErr)
				}
			} else {
				if lockErr != nil {
					t.Fatalf("successful source retained LOCK: %v", lockErr)
				}
				if err := next.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(filepath.Join(dir, indexNewFileName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("no-segment refusal produced replacement: %v", err)
			}
		})
	}
}

// The actual V6 caller closes its source before COMMIT, while its admitted
// operation parent remains live through disposition. A later physical refusal
// must keep the same source receipt/LOCK; ordinary semantic errors still drain.
func TestOfflineSourceLateNamespaceRefusalRetainsOriginalLock(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(map[bool]string{false: "semantic-refusal", true: "late-parent-refusal"}[fault], func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			seed, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err = seed.SetSync([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err = seed.Close(); err != nil {
				t.Fatal(err)
			}
			lock, err := lockfile.Acquire(filepath.Join(dir, "LOCK"))
			if err != nil {
				t.Fatal(err)
			}
			opts.ReadOnly = true
			d, err := openReadOnlyNoLockWithRegistryV6(opts, rootpublication.NewIdentityPinRegistry(), lock)
			if err != nil {
				t.Fatal(err)
			}
			source := d.idx.Load()
			source.retainOfflineSourceV1(d, lock)
			scope, err := d.captureOfflinePrimaryScopeV6()
			if err != nil {
				t.Fatal(err)
			}
			parent := scope.stableNamespaceParent
			scopeOwner, scopeCharge := scope.stableNamespaceMetadata, scope.stableNamespaceParentCharge
			if parent == nil || scopeCharge == 0 || source.metadataCharge == 0 {
				t.Fatal("actual admitted source/namespace operands missing")
			}
			// This is the real source physical completion before the outer
			// namespace callback, rather than a fabricated cached nil result.
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			if d.idx.Load() != nil {
				t.Fatal("original source discovery did not drain")
			}
			if fault {
				if err = parent.Close(); err != nil {
					t.Fatal(err)
				}
			}
			metadata, charge, cleanup := scope.releaseOfflineNamespaceScopeV6()
			operation := errors.New("ordinary semantic operation refusal")
			if !fault {
				if cleanup != nil || metadata != scopeOwner || charge != scopeCharge {
					t.Fatalf("successful original scope disposal: %v charge=%d", cleanup, charge)
				}
				scope, parent = nil, nil
				metadata.RemovePending(charge)
			} else {
				if !errors.Is(cleanup, os.ErrClosed) || metadata != nil || charge != 0 ||
					scope.stableNamespaceParent != parent || scope.stableNamespaceParentCharge != scopeCharge {
					t.Fatalf("actual late namespace refusal lost operand/debt: %v", cleanup)
				}
				operation = errors.Join(operation, cleanup)
			}
			metadata, charge, first := source.finishOfflineSourceV1(operation)
			if !fault {
				var fatal *OpenRecoveryCleanupError
				if !errors.Is(first, operation) || errors.As(first, &fatal) || recoveryCleanupOwnsLockV6(first, lock) {
					t.Fatalf("semantic refusal falsely retained fatal source custody: %v", first)
				}
				source, d = nil, nil
				if metadata != nil && charge != 0 {
					metadata.RemovePending(charge)
				}
				if err = lock.Close(); err != nil {
					t.Fatal(err)
				}
				next, err := lockfile.Acquire(filepath.Join(dir, "LOCK"))
				if err != nil {
					t.Fatal(err)
				}
				if err = next.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}
			var fatal *OpenRecoveryCleanupError
			if metadata != nil || charge != 0 || !errors.Is(first, os.ErrClosed) ||
				!errors.As(first, &fatal) || fatal.generation != source ||
				source.startupDB != d || source.startupLock != lock ||
				source.metadataCharge == 0 || !recoveryCleanupOwnsLockV6(first, lock) {
				t.Fatalf("late physical refusal lost original source/LOCK custody: %v", first)
			}
			retained := scopeOwner.Bytes()
			againOwner, againCharge, again := scope.releaseOfflineNamespaceScopeV6()
			sourceOwner, sourceCharge, sourceAgain := source.finishOfflineSourceV1(nil)
			if againOwner != nil || againCharge != 0 || sourceOwner != nil || sourceCharge != 0 ||
				sourceAgain != first || !errors.Is(again, os.ErrClosed) || scopeOwner.Bytes() != retained ||
				scope.stableNamespaceParent != parent || scope.stableNamespaceParentCharge != scopeCharge {
				t.Fatal("repeated completion replayed/refunded actual late refusal")
			}
			next, lockErr := lockfile.Acquire(filepath.Join(dir, "LOCK"))
			if next != nil {
				_ = next.Close()
			}
			if !errors.Is(lockErr, lockfile.ErrLocked) {
				t.Fatalf("late namespace refusal released LOCK: %v", lockErr)
			}
		})
	}
}
