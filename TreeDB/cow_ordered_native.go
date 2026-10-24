package treedb

import (
	"errors"
	"sync"
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

// LogicalMVCCCompletion owns one installed logical command through its original
// acknowledgement. The caller supplies and reuses the record only after
// Complete. No engine queue, worker or independent publication authority is
// created. A dedicated completion owner must call Complete even if its client
// stops waiting; Close and discard-floor advancement can wait for that owner.
// Do not copy a record after first use.
type LogicalMVCCCompletion struct {
	mu        sync.Mutex
	db        *DB
	batch     *commandWALPublicBatch
	started   time.Time
	syncWrite bool
	err       error
}

// Complete drives the existing captured-prefix coordinator and releases the
// batch/lifecycle custody exactly once. Repeated calls return the same result.
func (c *LogicalMVCCCompletion) Complete() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batch == nil {
		return c.err
	}
	b, db := c.batch, c.db
	c.err = b.completeCOWWrite(c.syncWrite, nil)
	db.observePublicBatchWrite(c.syncWrite, c.started, c.err)
	c.err = errors.Join(c.err, b.Close())
	c.batch, c.db = nil, nil
	db.lifecycleMu.RUnlock()
	return c.err
}

// SubmitLogicalMVCC performs the same canonical validation, preparation,
// append/registration and cut installation as CommitLogicalMVCC. Only its
// physical acknowledgement is handed to the caller-owned completion record.
// Other profiles complete synchronously through the established writer.
func (db *DB) SubmitLogicalMVCC(mutations []mvccadmission.LogicalMutation, input mvccadmission.LogicalInput, syncWrite bool, completion *LogicalMVCCCompletion) (err error) {
	if completion == nil {
		return errLogicalMVCCInput
	}
	completion.mu.Lock()
	defer completion.mu.Unlock()
	if completion.batch != nil {
		return errors.New("treedb: completion still owns a command")
	}
	completion.err = nil
	defer func() {
		if err != nil {
			completion.err = err
		}
	}()
	if !syncWrite || db == nil || !db.commandWALCached || db.publicBatchWriteSyncPhaseEnabled {
		err = db.CommitLogicalMVCC(mutations, input, syncWrite)
		completion.err = err
		return err
	}
	if err = db.beginPublicOperation(); err != nil {
		return err
	}
	retained := false
	defer func() {
		if !retained {
			db.lifecycleMu.RUnlock()
		}
	}()
	if db.cached == nil || !db.cached.SupportsLogicalMVCC() {
		return caching.ErrCOWUnsupported
	}
	if len(mutations) == 0 {
		return nil
	}
	if !input.Matches(mutations) {
		return errLogicalMVCCInput
	}
	// This record owns the original public batch and its admitted backing. It
	// never borrows the caller's mutation slice after submission returns.
	var b Batch = db.cached.NewBatchWithSize(len(mutations))
	b = newCommandWALPublicBatch(db, b, len(mutations))
	public, ok := b.(*commandWALPublicBatch)
	if !ok {
		return errors.Join(caching.ErrCOWUnsupported, b.Close())
	}
	submitted := false
	started := time.Now()
	if _, err = b.GetByteSize(); err == nil {
		_, err = public.StageLogicalMVCC(mutations, input)
	}
	if err == nil {
		// Ordered submission owns a fully collected cut, just like the
		// incremental builder. Do not collect the same work a second time.
		// This scheduling hint creates no force ticket or durability authority.
		public.cowState.collectionSealed = true
		submitted = true
		err = public.submitCOWWrite(true)
	}
	if err != nil {
		// An append/installation error still completes its original ticket and
		// poison handling. It is never resubmitted or converted to a size signal.
		if submitted {
			err = public.completeCOWWrite(true, err)
		}
		db.observePublicBatchWrite(true, started, err)
		return errors.Join(err, public.Close())
	}
	completion.db, completion.batch, completion.started, completion.syncWrite = db, public, started, true
	retained = true
	return nil
}
