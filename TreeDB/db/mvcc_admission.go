package db

import (
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/page"
)

// NewMVCCAdmission binds one individual Store to this exact DB incarnation.
func (db *DB) NewMVCCAdmission() *mvccadmission.Capability {
	if db == nil || db.closing.Load() {
		return nil
	}
	return db.mvccAdmission.Issue()
}

// SetWithMVCCInput preserves the ordinary batch publication path. Input is
// consumed at its real accepted postimage, including accepted-error outcomes.
func (b *Batch) SetWithMVCCInput(key, value []byte, input mvccadmission.Input) error {
	return b.batch.SetWithMVCCInput(key, value, input)
}

// SetPointWithMVCCInput preserves the generic update-key and combiner routes.
func (db *DB) SetPointWithMVCCInput(key, value []byte, sync bool, input mvccadmission.Input) error {
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	guard := db.lockUpdateKey(key)
	defer guard.Unlock()
	if handled, err := db.writeViaCommitCombinerWithMVCCInput(key, value, false, sync, input); handled {
		return err
	}
	return db.writeSingleKVWithMVCCInput(key, value, false, sync, input)
}

// BeginMVCCInputCut is the cached producer's accepted-input projection. The
// caller completes it under its actual publication locks, before any backend
// sync or flush. It carries no serialized state and cannot survive a return.
func (db *DB) BeginMVCCInputCut() mvccadmission.Cut {
	return db.mvccAdmission.Begin()
}

func (b *Batch) SetWithRevisionAndMVCCInput(key, value []byte, revision page.EntryRevision, input mvccadmission.Input) error {
	return b.batch.SetWithRevisionAndMVCCInput(key, value, revision, input)
}
func (b *Batch) SetViewWithRevisionAndMVCCInput(key, value []byte, revision page.EntryRevision, input mvccadmission.Input) error {
	return b.batch.SetViewWithRevisionAndMVCCInput(key, value, revision, input)
}
func (b *Batch) MergeMVCCInputSummary(summary mvccadmission.Summary) {
	b.logicalMVCCInput = true
	b.batch.MergeMVCCInputSummary(summary)
}
