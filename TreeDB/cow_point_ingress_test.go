package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

// Ownership assertions intentionally pass with owned ingress too. Removing the
// redundant point copy is a representation optimization, not a public bug fix.
func TestCOWPointIngressOwnershipThroughReturnAndReopen(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		for _, mode := range []string{"cow_btree", "cow_ordered"} {
			for _, pointer := range []bool{false, true} {
				for _, syncWrite := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/pointer=%t/sync=%t", profile, mode, pointer, syncWrite), func(t *testing.T) {
						database, opts, _ := cowPublicContractOpen(t, profile, func(o *Options) {
							o.MemtableMode = mode
							o.ValueLog.ForcePointers = pointer
							o.ValueLog.PointerThreshold = 1 << 30
							if pointer {
								o.ValueLog.PointerThreshold = 1
							}
							o.BackgroundCheckpointInterval = -1
							o.BackgroundCheckpointIdleDuration = -1
							o.MaxWALBytes = -1
						})
						if got := database.Stats()["treedb.cache.memtable_mode"]; got != mode {
							t.Fatalf("selected mode=%q want %q", got, mode)
						}
						owner := database.NewMVCCAdmission()
						if owner == nil {
							t.Fatal("missing original MVCC admission")
						}
						defer owner.Close()
						floorKey := []byte{0, 'T', 'D', 'B', 'M', 'V', 'C', 'C', 0, 'M', 1, 'd', 'f'}
						floorRecord := make([]byte, 9)
						floorRecord[0] = 1
						binary.BigEndian.PutUint64(floorRecord[1:], 1)
						if err := database.SetPointWithMVCCInput(floorKey, floorRecord, syncWrite, owner.FloorInput(floorKey, floorRecord, 1)); err != nil {
							t.Fatal(err)
						}
						observedFloor, err := database.Get(floorKey)
						if err != nil || !bytes.Equal(observedFloor, floorRecord) {
							t.Fatalf("actual floor=%x err=%v", observedFloor, err)
						}
						work := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
						request, ok, err := owner.CaptureRequest(binary.BigEndian.Uint64(observedFloor[1:]), &work)
						if err != nil || !ok {
							t.Fatalf("capture available=%t err=%v", ok, err)
						}
						qualified, err := mvcckey.Encode([]byte("qualified"), 11)
						if err != nil {
							t.Fatal(err)
						}
						valueSize := 64
						if pointer {
							valueSize = 4096
						}
						want := bytes.Repeat([]byte{'v'}, valueSize)
						key, value := bytes.Clone(qualified), bytes.Clone(want)
						if err := database.SetPointWithMVCCInput(key, value, syncWrite, owner.Input(key, 11, 1)); err != nil {
							t.Fatal(err)
						}
						key[0], value[0] = 'X', 'X' // Reuse only after the synchronous return.
						if ok, err := request.Validate(&work); err != nil || !ok {
							t.Fatalf("qualified point lost owner: available=%t err=%v", ok, err)
						}
						write := database.Set
						if syncWrite {
							write = database.SetSync
						}
						key, value = []byte("point"), bytes.Clone(want)
						if err := write(key, value); err != nil {
							t.Fatal(err)
						}
						key[0], value[0] = 'X', 'X'
						if ok, err := request.Validate(&work); ok || !errors.Is(err, mvccadmission.ErrRequestInvalid) {
							t.Fatalf("raw point did not invalidate owner: available=%t err=%v", ok, err)
						}
						// Arbitrary public Batch.Set must still own inputs before Write.
						pending := database.NewBatchWithSize(1)
						defer pending.Close()
						key, value = []byte("batch"), bytes.Clone(want)
						if err := pending.Set(key, value); err != nil {
							t.Fatal(err)
						}
						key[0], value[0] = 'X', 'X'
						if err := pending.Write(); err != nil {
							t.Fatal(err)
						}
						if err := pending.Close(); err != nil {
							t.Fatal(err)
						}
						old := database.AcquireSnapshot()
						if old == nil {
							t.Fatal("missing retained point cut")
						}
						defer old.Close()
						for _, originalKey := range [][]byte{qualified, []byte("point"), []byte("batch")} {
							cowPublicContractSnapshotValue(t, old, string(originalKey), want, true)
							entry, err := old.GetEntry(originalKey)
							if err != nil || (entry.Flags&node.FlagPointer != 0) != pointer {
								t.Fatalf("layout key=%x flags=%x err=%v want pointer=%t", originalKey, entry.Flags, err, pointer)
							}
						}
						// This later explicit sync consumes any pending relaxed WAL work
						// after every caller buffer above was overwritten.
						if err := database.SetSync([]byte("barrier"), []byte("flush earlier points")); err != nil {
							t.Fatal(err)
						}
						if profile != ProfileNoWALFast {
							latest := make(map[string]commitlog.RawKVOperation)
							for _, frame := range cowPublicCrashCaptureFrames(t, opts.Dir) {
								for _, op := range frame.Operations {
									latest[string(op.Key)] = op
								}
							}
							for _, originalKey := range [][]byte{qualified, []byte("point"), []byte("batch")} {
								op, found := latest[string(originalKey)]
								if !found {
									t.Fatalf("WAL lost original key %x", originalKey)
								}
								if pointer {
									if (op.Op != commitlog.RawKVOpSetRID && op.Op != commitlog.RawKVOpSetMaterializedRID) || op.RID == 0 {
										t.Fatalf("WAL lost pointer identity: %+v", op)
									}
									if op.Op == commitlog.RawKVOpSetMaterializedRID && !bytes.Equal(op.Value, want) {
										t.Fatal("WAL materialized caller mutation")
									}
								} else if op.Op != commitlog.RawKVOpSet || !bytes.Equal(op.Value, want) {
									t.Fatal("WAL retained caller mutation")
								}
							}
						}
						if err := database.Set([]byte("point"), []byte("replacement")); err != nil {
							t.Fatal(err)
						}
						if err := database.Checkpoint(); err != nil {
							t.Fatal(err)
						}
						cowPublicContractSnapshotValue(t, old, "point", want, true)
						cowPublicContractSnapshotValue(t, old, string(qualified), want, true)
						if err := old.Close(); err != nil {
							t.Fatal(err)
						}
						if err := database.Close(); err != nil {
							t.Fatal(err)
						}
						reopened, err := Open(opts)
						if err != nil {
							t.Fatal(err)
						}
						defer reopened.Close()
						for originalKey, expected := range map[string][]byte{string(qualified): want, "point": []byte("replacement"), "batch": want} {
							if got, err := reopened.Get([]byte(originalKey)); err != nil || !bytes.Equal(got, expected) {
								t.Fatalf("reopen key=%x value=%x err=%v", originalKey, got, err)
							}
						}
					})
				}
			}
		}
	}
}

func TestCOWPointIngressFiniteConstructorRefusal(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		for _, mode := range []string{"cow_btree", "cow_ordered"} {
			t.Run(fmt.Sprintf("%s/%s", profile, mode), func(t *testing.T) {
				database, _, _ := cowPublicContractOpen(t, profile, func(o *Options) { o.MemtableMode = mode })
				if err := database.Set([]byte("keep"), []byte("original")); err != nil {
					t.Fatal(err)
				}
				owner := database.NewMVCCAdmission()
				if owner == nil {
					t.Fatal("missing admission")
				}
				defer owner.Close()
				qualified, err := mvcckey.Encode([]byte("refused-qualified"), 11)
				if err != nil {
					t.Fatal(err)
				}
				before := database.cached.COWMemoryStats()
				limits := DefaultCOWMemtableLimits()
				leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
				if before.ReservedBytes+leaseBytes >= limits.MaxInFlightBytes {
					t.Fatal("fixture has no finite constructor headroom")
				}
				filler, err := database.cached.AcquireCOWAllocation(limits.MaxInFlightBytes - before.ReservedBytes - leaseBytes)
				if err != nil {
					t.Fatal(err)
				}
				defer filler.Close()
				full := database.cached.COWMemoryStats()
				stats := database.Stats()
				for _, syncWrite := range []bool{false, true} {
					key, value := []byte("refused"), []byte("not installed")
					write := database.Set
					if syncWrite {
						write = database.SetSync
					}
					if err := write(key, value); !errors.Is(err, memtable.ErrCOWCapacity) {
						t.Fatalf("point sync=%t refusal=%v", syncWrite, err)
					}
					key[0], value[0] = 'X', 'X'
					key, value = bytes.Clone(qualified), []byte("not installed")
					if err := database.SetPointWithMVCCInput(key, value, syncWrite, owner.Input(key, 11, 0)); !errors.Is(err, memtable.ErrCOWCapacity) {
						t.Fatalf("qualified sync=%t refusal=%v", syncWrite, err)
					}
					key[0], value[0] = 'X', 'X'
				}
				if got := database.cached.COWMemoryStats(); got != full {
					t.Fatalf("constructor refusal changed charges: before=%+v after=%+v", full, got)
				}
				after := database.Stats()
				for _, counter := range []string{"treedb.cache.cow.publications_total", "treedb.command_wal.max_lsn", "treedb.command_wal.bytes"} {
					if stats[counter] != after[counter] {
						t.Fatalf("constructor refusal changed %s: %q -> %q", counter, stats[counter], after[counter])
					}
				}
				filler.Close()
				if got, err := database.Get([]byte("keep")); err != nil || string(got) != "original" {
					t.Fatalf("seed=%q err=%v", got, err)
				}
				for _, key := range [][]byte{[]byte("refused"), qualified} {
					if found, err := database.Has(key); err != nil || found {
						t.Fatalf("refused key=%x found=%t err=%v", key, found, err)
					}
				}
				if err := database.Set([]byte("resumed"), []byte("accepted")); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestCOWPointIngressPreparationFailureOwnsNothing(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		for _, mode := range []string{"cow_btree", "cow_ordered"} {
			t.Run(fmt.Sprintf("%s/%s", profile, mode), func(t *testing.T) {
				database, _, _ := cowPublicContractOpen(t, profile, func(o *Options) { o.MemtableMode = mode; o.BackgroundCheckpointInterval = -1 })
				if err := database.Set([]byte("keep"), []byte("original")); err != nil {
					t.Fatal(err)
				}
				old := database.AcquireSnapshot()
				if old == nil {
					t.Fatal("missing predecessor")
				}
				defer old.Close()
				before := database.Stats()
				memory := database.cached.COWMemoryStats()
				injected := errors.New("point ingress: refuse after canonical preparation")
				key, value := []byte("refused"), bytes.Repeat([]byte{'r'}, 4096)
				err := func() error {
					restore := durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Point == durabilitycut.AfterCOWCanonicalPreparation && event.Resource == durabilitycut.ResourceAuxiliary {
							return injected
						}
						return nil
					})
					defer restore()
					return database.SetSync(key, value)
				}()
				if !errors.Is(err, injected) {
					t.Fatalf("actual preparation failure=%v", err)
				}
				key[0], value[0] = 'X', 'X'
				after := database.Stats()
				for _, counter := range []string{"treedb.cache.cow.publications_total", "treedb.command_wal.max_lsn", "treedb.command_wal.bytes"} {
					if before[counter] != after[counter] {
						t.Fatalf("failed point changed %s", counter)
					}
				}
				if got := database.cached.COWMemoryStats(); got.ExternalLeases != memory.ExternalLeases || got.Views != memory.Views {
					t.Fatalf("failure retained point owners: before=%+v after=%+v", memory, got)
				}
				if found, err := database.Has([]byte("refused")); err != nil || found {
					t.Fatalf("failed point found=%t err=%v", found, err)
				}
				cowPublicContractSnapshotValue(t, old, "keep", []byte("original"), true)
				key, value = []byte("refused"), []byte("retry accepted")
				if err := database.SetSync(key, value); err != nil {
					t.Fatal(err)
				}
				key[0], value[0] = 'X', 'X'
				if got, err := database.Get([]byte("refused")); err != nil || string(got) != "retry accepted" {
					t.Fatalf("retry=%q err=%v", got, err)
				}
			})
		}
	}
}
