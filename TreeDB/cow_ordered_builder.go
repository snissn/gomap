package treedb

import (
	"errors"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

// LogicalMVCCBuilder owns an unassigned native batch and one public lifecycle
// read lock. The Store owns serialization and discard-floor custody. Never copy
// an active builder. Append only stages owned operands; Submit prepares one cut.
type LogicalMVCCBuilder struct {
	db    *DB
	batch *commandWALPublicBatch
	err   error
}

// SupportsLogicalMVCCStaging identifies the exact incremental staging route.
// Consumers can cache this immutable profile predicate while opening their DB;
// unsupported profiles preserve their original synchronous FIFO writer.
func (db *DB) SupportsLogicalMVCCStaging() bool {
	if db == nil {
		return false
	}
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	return db.supportsLogicalMVCCStagingLocked()
}

func (db *DB) supportsLogicalMVCCStagingLocked() bool {
	return db.cached != nil && db.cached.SupportsLogicalMVCC() && db.commandWALCached && !db.publicBatchWriteSyncPhaseEnabled
}

// BeginLogicalMVCC holds lifecycle custody until Abort or transfer at Submit.
// Unsupported routes retain their existing synchronous entry points.
func (db *DB) BeginLogicalMVCC(builder *LogicalMVCCBuilder) error {
	if builder == nil || builder.db != nil {
		return errors.New("treedb: invalid or active logical builder")
	}
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	if !db.supportsLogicalMVCCStagingLocked() {
		db.lifecycleMu.RUnlock()
		return caching.ErrCOWUnsupported
	}
	inner := db.cached.NewBatchWithSize(0)
	public := newCommandWALPublicBatch(db, inner, 0)
	if _, err := public.GetByteSize(); err != nil {
		err = errors.Join(err, public.Close())
		db.lifecycleMu.RUnlock()
		return err
	}
	builder.db, builder.batch, builder.err = db, public, nil
	return nil
}

// Append copies admitted operands into the original native owned arena. It
// assigns no command and performs no root-dependent COW preparation.
func (b *LogicalMVCCBuilder) Append(mutations []mvccadmission.LogicalMutation, input mvccadmission.LogicalInput) error {
	if b == nil || b.batch == nil {
		return ErrClosed
	}
	if b.err != nil {
		return b.err
	}
	if _, b.err = b.batch.StageLogicalMVCC(mutations, input); b.err != nil {
		return b.err
	}
	if len(mutations) != 0 {
		b.err = durabilitycut.EmitBasic(durabilitycut.AfterLogicalMVCCStaging, durabilitycut.ResourceCommandWAL, b.db.dir)
	}
	return b.err
}

// Abort releases the unassigned batch and lifecycle custody exactly once.
func (b *LogicalMVCCBuilder) Abort() error {
	if b == nil || b.batch == nil {
		return nil
	}
	db, batch := b.db, b.batch
	b.db, b.batch = nil, nil
	err := batch.Close()
	db.lifecycleMu.RUnlock()
	return err
}

// Submit seals the existing owned batch, without reentering public lifecycle
// admission. Failed submissions leave no caller-owned pending completion.
func (b *LogicalMVCCBuilder) Submit(syncWrite bool, c *LogicalMVCCCompletion) (err error) {
	if b == nil || b.batch == nil {
		return ErrClosed
	}
	if c == nil {
		return errors.Join(errors.New("treedb: nil completion record"), b.Abort())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batch != nil {
		return errors.Join(errors.New("treedb: completion still owns a command"), b.Abort())
	}
	c.err = nil
	if b.err != nil {
		err = errors.Join(b.err, b.Abort())
		c.err = err
		return err
	}
	db, batch, started := b.db, b.batch, time.Now()
	err = batch.submitCOWWrite(syncWrite)
	if err != nil {
		err = batch.completeCOWWrite(syncWrite, err)
		db.observePublicBatchWrite(syncWrite, started, err)
		err = errors.Join(err, b.Abort())
		c.err = err
		return err
	}
	c.db, c.batch, c.started, c.syncWrite = db, batch, started, syncWrite
	b.db, b.batch = nil, nil
	return nil
}
