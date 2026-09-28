package nativewire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const replacementReceiverRecordMaxBytesV1 = 16 << 10
const replacementReceiverRecordNameV1 = "replacement-receiver-v1.json"
const replacementReceiverRecordTempV1 = "replacement-receiver-v1.tmp"

type replacementReceiverRecordV1 struct {
	Format uint16                                  `json:"format"`
	Begin  raftplacement.ReplicaReplacementBeginV1 `json:"begin"`
	Seed   raftcluster.ReplacementSnapshotSeedV1   `json:"seed"`
	Phase  replacementReceiverPhaseV1              `json:"phase"`
}

// One receiver record belongs to one anchored group namespace. OperationID is
// logical metadata only; it never enters a filesystem path. Parent-relative
// mutation keeps a path rebind from redirecting a phase write to another group.
type replacementReceiverOwnerV1 struct {
	mu     sync.Mutex
	parent *os.File
	lock   *lockfile.Lock
	record replacementReceiverRecordV1
	poison error
}

func encodeReplacementReceiverRecordV1(record replacementReceiverRecordV1) ([]byte, error) {
	if record.Format != 1 || record.Seed.Validate() != nil || record.Seed.Manifest.GroupID != record.Begin.GroupID {
		return nil, raftcluster.ErrInvalidConfig
	}
	beginRaw, err := json.Marshal(record.Begin)
	if err != nil {
		return nil, err
	}
	if _, err := raftplacement.DecodeReplicaReplacementBeginV1(beginRaw); err != nil {
		return nil, err
	}
	switch record.Phase {
	case replacementReceiverPreparedV1, replacementReceiverInstallingV1, replacementReceiverInstalledV1, replacementReceiverAddIntentV1:
	default:
		return nil, raftcluster.ErrInvalidConfig
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(raw) > replacementReceiverRecordMaxBytesV1 {
		return nil, raftcluster.ErrAdmissionUnavailable
	}
	return raw, nil
}

func decodeReplacementReceiverRecordV1(raw []byte) (replacementReceiverRecordV1, error) {
	var record replacementReceiverRecordV1
	if len(raw) == 0 || len(raw) > replacementReceiverRecordMaxBytesV1 {
		return record, raftcluster.ErrInvalidConfig
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return record, raftcluster.ErrInvalidConfig
	}
	canonical, err := encodeReplacementReceiverRecordV1(record)
	if err != nil {
		return record, err
	}
	if !bytes.Equal(raw, canonical) {
		return record, raftcluster.ErrInvalidConfig
	}
	return record, nil
}

// dir is the existing group directory derived from validated fixed-node config.
// The caller has already proved committed operation/seed authority and target
// freshness before creating a first record. Existing records must match both
// bindings exactly, including on restart; no replacement operation overwrites
// an unresolved record.
func openReplacementReceiverOwnerV1(dir string, begin raftplacement.ReplicaReplacementBeginV1, seed raftcluster.ReplacementSnapshotSeedV1) (*replacementReceiverOwnerV1, error) {
	wanted := replacementReceiverRecordV1{Format: 1, Begin: begin, Seed: seed, Phase: replacementReceiverPreparedV1}
	if _, err := encodeReplacementReceiverRecordV1(wanted); err != nil {
		return nil, err
	}
	parent, err := rootpublication.OpenStableParent(dir)
	if err != nil {
		return nil, err
	}
	lock, err := lockfile.Acquire(filepath.Join(dir, "replacement-receiver-v1.lock"))
	if err != nil {
		return nil, errors.Join(err, parent.Close())
	}
	owner := &replacementReceiverOwnerV1{parent: parent, lock: lock, record: wanted}
	fail := func(err error) (*replacementReceiverOwnerV1, error) { return nil, errors.Join(err, owner.Close()) }
	if err := validateReplacementReceiverLockV1(parent, lock); err != nil {
		return fail(err)
	}
	file, err := rootpublication.OpenStableChildFile(parent, replacementReceiverRecordNameV1, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		if err := owner.persistPhase(replacementReceiverPreparedV1); err != nil {
			return fail(err)
		}
		return owner, nil
	}
	if err != nil {
		return fail(err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, replacementReceiverRecordMaxBytesV1+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return fail(err)
	}
	actual, err := decodeReplacementReceiverRecordV1(raw)
	if err != nil {
		return fail(err)
	}
	wantedRaw, _ := raftplacement.EncodeReplicaReplacementBeginV1(begin)
	actualRaw, _ := raftplacement.EncodeReplicaReplacementBeginV1(actual.Begin)
	if !bytes.Equal(wantedRaw, actualRaw) || !raftcluster.SameReplacementSnapshotSeedV1(seed, actual.Seed) {
		return fail(raftplacement.ErrCatalogMetaConflict)
	}
	// Replay the namespace barrier after a prior crash/ambiguous rename before
	// using the durable cutoff. No old in-memory phase survives this reopen.
	if err := rootpublication.SyncStableNamespace(parent); err != nil {
		return fail(err)
	}
	owner.record = actual
	return owner, nil
}

func (o *replacementReceiverOwnerV1) persistPhase(phase replacementReceiverPhaseV1) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.parent == nil {
		return os.ErrClosed
	}
	if o.poison != nil {
		return o.poison
	}
	if phase != o.record.Phase && !(o.record.Phase == replacementReceiverPreparedV1 && phase == replacementReceiverInstallingV1 ||
		o.record.Phase == replacementReceiverInstallingV1 && phase == replacementReceiverInstalledV1 ||
		o.record.Phase == replacementReceiverInstalledV1 && phase == replacementReceiverAddIntentV1) {
		return raftcluster.ErrInvalidConfig
	}
	candidate := o.record
	candidate.Phase = phase
	raw, err := encodeReplacementReceiverRecordV1(candidate)
	if err != nil {
		return err
	}
	// At most one fixed temporary file exists across retries/crashes. It has no
	// published authority, and is removed only beneath this retained parent.
	if err := rootpublication.RemoveStableChildFile(o.parent, replacementReceiverRecordTempV1); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := rootpublication.OpenStableChildFile(o.parent, replacementReceiverRecordTempV1, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	if err := errors.Join(writeErr, rootpublication.SyncStableFile(file), file.Close()); err != nil {
		o.poison = err
		return err
	}
	if err := rootpublication.RenameStableChildFile(o.parent, replacementReceiverRecordTempV1, replacementReceiverRecordNameV1); err != nil {
		o.poison = err
		return err
	}
	if err := rootpublication.SyncStableNamespace(o.parent); err != nil {
		o.poison = err
		return err
	}
	o.record = candidate
	return nil
}

func (o *replacementReceiverOwnerV1) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	var err error
	if o.parent != nil {
		err = o.parent.Close()
		o.parent = nil
	}
	if o.lock != nil {
		err = errors.Join(err, o.lock.Close())
		o.lock = nil
	}
	return err
}

// Verify the actual acquired lock file through the already captured parent.
// Checking the current directory path alone would miss a rename-away-and-back
// window during Acquire. No phase write is allowed on an unmatched pair.
func validateReplacementReceiverLockV1(parent *os.File, lock *lockfile.Lock) error {
	linked, err := rootpublication.OpenStableChildFile(parent, "replacement-receiver-v1.lock", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	same, checkErr := lock.SameFile(linked)
	closeErr := linked.Close()
	if checkErr != nil || closeErr != nil {
		return errors.Join(checkErr, closeErr)
	}
	if !same {
		return raftcluster.ErrInvalidConfig
	}
	return nil
}
