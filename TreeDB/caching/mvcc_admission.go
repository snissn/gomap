package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/page"
)

func (db *DB) beginMVCCInputCut() mvccadmission.Cut {
	if provider, ok := db.backend.(interface{ BeginMVCCInputCut() mvccadmission.Cut }); ok {
		return provider.BeginMVCCInputCut()
	}
	return mvccadmission.Cut{}
}

func (db *DB) SetPointWithMVCCInput(key, value []byte, sync bool, input mvccadmission.Input) error {
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	db.waitForCheckpointForWrite()
	guard := db.lockUpdateKey(key)
	defer guard.Unlock()
	return db.setWithMVCCInput(key, value, sync, input)
}

func (db *DB) SetAfterCommandWALAppendWithMVCCInput(key, value []byte, input mvccadmission.Input, appendCommand func(func() page.EntryRevision) error) error {
	return db.setAfterCommandWALAppendWithMVCCInput(key, value, input, appendCommand)
}
