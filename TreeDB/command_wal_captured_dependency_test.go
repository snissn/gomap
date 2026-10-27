package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// A relaxed external-RID prefix needs one dependency drain before its durable
// marker. The marker's journal hook owns that drain, including when it rotates.
// Retaining the same handles must not cause a second physical sync of the cut.
func TestPublicCapturedPrefixDrainsDependenciesOnce(t *testing.T) {
	for _, mode := range []string{"btree", "cow_ordered"} {
		for _, rotate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rotate=%t", mode, rotate), func(t *testing.T) {
				opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
				opts.MemtableMode = mode
				opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
				if rotate {
					opts.CommandWALSegmentTargetBytes = 1
				}
				database, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				// Exceed the materialized-RID ceiling, requiring the original
				// persistent value-log generation in the recovery closure.
				value := bytes.Repeat([]byte("external"), 9<<10)
				for _, key := range []string{"first", "second"} {
					if err := database.Set([]byte(key), value); err != nil {
						t.Fatal(err)
					}
				}
				var before, after, markers int
				wantDrains := 1
				if mode == "btree" {
					// Ordinary cached Set keeps values in its memtable; there is
					// no physical external-RID debt at this boundary.
					wantDrains = 0
				}
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if e.Resource == durabilitycut.ResourceAuxiliary {
						switch e.Point {
						case durabilitycut.BeforeDependencyFileSync:
							before++
						case durabilitycut.AfterDependencyFileSync:
							after++
						}
					}
					if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend {
						markers++
						if before != wantDrains || after != wantDrains {
							return fmt.Errorf("marker dependency drains: before=%d after=%d want=%d", before, after, wantDrains)
						}
					}
					return nil
				})
				err = writeEmptyPublicCommandWALSync(database)
				restore()
				if err != nil {
					t.Fatal(err)
				}
				if before != wantDrains || after != wantDrains || markers != 1 {
					t.Fatalf("drain before/after=%d/%d markers=%d, want %d/%d/1", before, after, markers, wantDrains, wantDrains)
				}
				frames := scanPublicCommandWALV2(t, opts.Dir)
				if len(frames) != 3 || frames[2].Kind != commitlog.CommandKindDurablePrefixBarrier {
					t.Fatalf("frames=%+v, want two mutations and one durable marker", frames)
				}
				// Copy before Close can checkpoint: recovery must replay the
				// acknowledged prefix and resolve both persistent pointers.
				image := filepath.Join(t.TempDir(), "crash-image")
				if err := copyPublicCommandWALCrashImage(opts.Dir, image); err != nil {
					t.Fatal(err)
				}
				opts.Dir = image
				recovered, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				for _, key := range []string{"first", "second"} {
					requireRawKVValue(t, recovered, []byte(key), value)
				}
			})
		}
	}
}

func TestPublicCapturedPrefixDependencyFailurePrecedesMarker(t *testing.T) {
	opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Set([]byte("external"), bytes.Repeat([]byte("x"), 65<<10)); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("dependency drain failed")
	nextLSN := database.backend.CommandWALNextLSN()
	var markers int
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceAuxiliary && e.Point == durabilitycut.BeforeDependencyFileSync {
			return injected
		}
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend {
			markers++
		}
		return nil
	})
	err = writeEmptyPublicCommandWALSync(database)
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("sync error=%v, want original dependency failure", err)
	}
	if markers != 0 {
		t.Fatalf("appended %d markers after failed dependency drain", markers)
	}
	if got := database.backend.CommandWALNextLSN(); got != nextLSN {
		t.Fatalf("dependency failure consumed LSN: next=%d want=%d", got, nextLSN)
	}
	// No marker was assigned. Preserve the existing recoverable pre-append
	// failure semantics, including cancellation of its accepted ticket.
	if err := writeEmptyPublicCommandWALSync(database); err != nil {
		t.Fatalf("retry unassigned boundary: %v", err)
	}
}
