package db

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
		if original == nil {
			t.Fatalf("fixture missed ordinary DATA source: %p", original)
		}
		sourceBorrows = original.wrapperBorrowCountV6()
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
		receipt.generation.wrapperBorrowCountV6() != sourceBorrows ||
		receipt.generation.metadataCharge == 0 {
		t.Fatalf("offline cleanup dropped original source/replacement/LOCK custody: source=%p receipt=%p borrows=%d before=%d err=%v",
			original, receipt, original.wrapperBorrowCountV6(), sourceBorrows, err)
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
	// Modeled process shutdown follows all fatal custody and no-replay checks.
	if err = g.startupLock.Close(); err != nil {
		t.Fatal(err)
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
				// Model process shutdown only after fatal receipt, retained
				// charge and no-replay assertions; never retry the failed pager.
				if err = lock.Close(); err != nil {
					t.Fatal(err)
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
		if original == nil || !d.readOnly {
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
	// Model process shutdown only after the fatal-custody oracle finishes.
	// This OS LOCK release does not retry failed cleanup or refund its charge.
	defer func() {
		if err := original.startupLock.Close(); err != nil {
			t.Error(err)
		}
	}()
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
	before, borrows := owner.Bytes(), source.wrapperBorrowCountV6()
	budget.setLimit(before)
	replacement, admissionErr := newStartupIndexGeneration(d, lock)
	budget.setLimit(math.MaxUint64)
	if replacement != nil || !errors.Is(admissionErr, retainedalloc.ErrCapacity) ||
		owner.Bytes() != before || source.wrapperBorrowCountV6() != borrows {
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
		source.wrapperBorrowCountV6() != borrows || source.metadataCharge == 0 {
		t.Fatalf("admission/source Close lost original custody: completion=%p/%d attempts=%d source=%p receipt=%p borrows=%d before=%d err=%v",
			metadata, charge, attempts, source, receipt, source.wrapperBorrowCountV6(), borrows, refusal)
	}
	registry.Close()
	// Model process shutdown only after the fatal-custody oracle finishes.
	// This OS LOCK release does not retry failed cleanup or refund its charge.
	defer func() {
		if err := source.startupLock.Close(); err != nil {
			t.Error(err)
		}
	}()
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
			g.startupLock == nil || g.wrapperBorrowCountV6() != 1 {
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
				if original == nil || !d.readOnly {
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
			if fault {
				// Model process shutdown after the exact retained LOCK, sticky
				// receipt and no-refund assertions; do not retry failed Close.
				if err := original.startupLock.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// The actual V6 caller closes its source before COMMIT, while its admitted
// operation parent remains live through disposition. A later physical refusal
// must keep the same source receipt/LOCK; ordinary semantic errors still drain.
