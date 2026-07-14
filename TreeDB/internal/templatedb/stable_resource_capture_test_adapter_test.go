package templatedb

import (
	"path/filepath"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type stableTestKV struct {
	db *backenddb.DB
}

func (kv stableTestKV) Get(key []byte) ([]byte, error) {
	return kv.db.Get(key)
}

func (kv stableTestKV) SetSync(key, value []byte) error {
	return kv.db.SetSync(key, value)
}

func (kv stableTestKV) DeleteSync(key []byte) error {
	return kv.db.DeleteSync(key)
}

func (kv stableTestKV) NewBatch() Batch {
	return kv.db.NewBatch()
}

func (kv stableTestKV) AcquireStableTemplateSnapshot() StablePhysicalSnapshot {
	if kv.db == nil {
		return nil
	}
	snapshot := kv.db.AcquireStableSnapshot()
	if snapshot == nil {
		return nil
	}
	return &stableTestSnapshot{snapshot: snapshot, dir: kv.db.Dir()}
}

type stableTestSnapshot struct {
	snapshot *backenddb.Snapshot
	dir      string
}

func (snapshot *stableTestSnapshot) Get(key []byte) ([]byte, error) {
	return snapshot.snapshot.Get(key)
}

func (snapshot *stableTestSnapshot) GetEntry(key []byte) (StablePhysicalEntry, error) {
	entry, err := snapshot.snapshot.GetEntryExact(key)
	if err != nil {
		return StablePhysicalEntry{}, err
	}
	return StablePhysicalEntry{
		Pointer:      entry.Flags&node.FlagPointer != 0,
		FileID:       entry.ValuePtr.FileID,
		Offset:       entry.ValuePtr.Offset,
		RecordLength: uint64(page.ValuePtrRecordLength(entry.ValuePtr)),
	}, nil
}

func (snapshot *stableTestSnapshot) ValueLogDiagnosticPath(fileID uint32) (string, error) {
	state := snapshot.snapshot.State()
	if state == nil || state.ValueLogSet == nil || state.ValueLogSet.Files[fileID] == nil {
		return "", rootpublication.ErrUnresolvedResource
	}
	return filepath.Rel(snapshot.dir, state.ValueLogSet.Files[fileID].Path)
}

func (snapshot *stableTestSnapshot) NewStableIndexResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	return snapshot.snapshot.NewStableIndexResourceToken(spec, NewStableTemplateResourceToken)
}

func (snapshot *stableTestSnapshot) NewStableValueLogResourceToken(fileID uint32, spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	return snapshot.snapshot.NewStableValueLogPhysicalResourceToken(fileID, spec, NewStableTemplateResourceToken)
}

func (snapshot *stableTestSnapshot) Close() error {
	return snapshot.snapshot.Close()
}
