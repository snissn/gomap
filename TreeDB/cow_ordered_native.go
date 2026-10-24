package treedb

import (
	"errors"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

var errLogicalMVCCInput = errors.New("treedb: logical MVCC operands do not match admitted input")

// SupportsLogicalMVCC identifies the optional logical-history data plane. The
// original COW cache still owns preparation, durability and publication.
func (db *DB) SupportsLogicalMVCC() bool {
	cached, _, err := db.captureReadOwners()
	return err == nil && cached != nil && cached.SupportsLogicalMVCC()
}

// SubmitLogicalMVCC transfers one Store-admitted aggregate to database-owned
// completion. For native COW command-WAL durable writes it returns after the
// canonical append and atomic install, before the final physical WAL sync.
// The floor lease and private batch remain owned through final acknowledgement.
// Other modes complete synchronously through CommitLogicalMVCC. Errors returned
// directly can be commit ambiguous, just as for the synchronous producer.
func (db *DB) SubmitLogicalMVCC(mutations []mvccadmission.LogicalMutation, input mvccadmission.LogicalInput, syncWrite bool, floor *mvccadmission.CompletionLease) (future *CommitFuture, err error) {
	transferred := false
	defer func() {
		if !transferred {
			floor.Release()
		}
	}()
	if db == nil {
		return nil, ErrClosed
	}
	if !syncWrite || !db.commandWALCached {
		if err := db.CommitLogicalMVCC(mutations, input, syncWrite); err != nil {
			return nil, err
		}
		return &CommitFuture{}, nil
	}
	o := &db.commitCompletions
	if err := o.reserve(db); err != nil {
		return nil, err
	}
	defer func() {
		if !transferred {
			<-o.slots
		}
	}()
	o.submitMu.Lock()
	defer o.submitMu.Unlock()
	if o.stopping() {
		return nil, ErrClosed
	}
	if err := db.beginPublicOperation(); err != nil {
		return nil, err
	}
	defer func() {
		if !transferred {
			db.lifecycleMu.RUnlock()
		}
	}()
	if db.cached == nil || !db.cached.SupportsLogicalMVCC() {
		return nil, caching.ErrCOWUnsupported
	}
	if len(mutations) == 0 {
		return &CommitFuture{}, nil
	}
	if !input.Matches(mutations) {
		return nil, errLogicalMVCCInput
	}
	b := newCommandWALPublicBatch(db, db.cached.NewBatchWithSize(len(mutations)), len(mutations))
	defer func() {
		if !transferred {
			err = errors.Join(err, b.Close())
		}
	}()
	if _, err = b.GetByteSize(); err != nil {
		return nil, err
	}
	if _, err = b.StageLogicalMVCC(mutations, input); err != nil {
		return nil, err
	}
	start := time.Now()
	if err = b.prepareCOWWrite(true); err != nil {
		err = b.completeCOWWrite(err)
		db.observePublicBatchWrite(true, start, err)
		return nil, err
	}
	future = &CommitFuture{done: make(chan struct{})}
	// Slots include the active work item. This enqueue cannot wait for disk.
	o.work <- commitCompletionWork{batch: b, future: future, floor: floor, start: start}
	transferred = true
	return future, nil
}

// CommitLogicalMVCC publishes one Store-admitted group through the ordinary
// native batch producer. Application timestamps remain distinct from native
// entry revisions and from the publication sequence. Delete creates a logical
// MVCC tombstone, rather than deleting a retained physical version.
//
// The initial projection deliberately uses the canonical physical command
// format. Its admission input follows each owned physical key into the same
// accepted publication cut; this method grants no independent authority.
func (db *DB) CommitLogicalMVCC(mutations []mvccadmission.LogicalMutation, input mvccadmission.LogicalInput, syncWrite bool) (err error) {
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	if db.cached == nil || !db.cached.SupportsLogicalMVCC() {
		db.lifecycleMu.RUnlock()
		return caching.ErrCOWUnsupported
	}
	if len(mutations) == 0 {
		db.lifecycleMu.RUnlock()
		return nil
	}
	if !input.Matches(mutations) {
		db.lifecycleMu.RUnlock()
		return errLogicalMVCCInput
	}

	// Encode into the native batch's own admitted arena. Its ordinary COW
	// producer still prepares the complete private postimage before WAL append.
	var b Batch = db.cached.NewBatchWithSize(len(mutations))
	if db.commandWALCached {
		b = newCommandWALPublicBatch(db, b, len(mutations))
	}
	// Write enters its original public admission; do not recursively retain
	// this read lock while a queued Close may be waiting for that boundary.
	db.lifecycleMu.RUnlock()
	defer func() { err = errors.Join(err, b.Close()) }()
	if _, err := b.GetByteSize(); err != nil {
		return err
	}
	producer, ok := b.(interface {
		StageLogicalMVCC([]mvccadmission.LogicalMutation, mvccadmission.LogicalInput) (int, error)
	})
	if !ok {
		return caching.ErrCOWUnsupported
	}
	if _, err := producer.StageLogicalMVCC(mutations, input); err != nil {
		return err
	}
	if syncWrite {
		return b.WriteSync()
	}
	return b.Write()
}
