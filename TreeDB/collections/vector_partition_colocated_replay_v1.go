package collections

import (
	"context"
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func replayColocatedVectorMutationCommandWALV1(db *backenddb.DB, env commitlog.CommandEnvelope, v commitlog.ColocatedVectorMutationWALV1) error {
	if err := v.ValidateV1(); err != nil {
		return err
	}
	intent, err := db.NewCommandWALReplayIntent(env)
	if err != nil {
		return err
	}
	c, err := newCommandWALReplayCollectionManager(db).openCollectionWithCommandWALIntent(v.Collection, intent)
	if err != nil {
		return err
	}
	// Redo may not reinterpret a covered original outcome against a later source.
	// Coverage and exact command/scope are required before the shortcut.
	outcome, known, err := c.ReadVectorPartitionColocatedOutcomeV1(v.Scope, v.Attempt, v.CommandDigest)
	if err != nil {
		return err
	}
	if known {
		if outcome.Term != v.Term || outcome.Index != v.Index || outcome.Matched != v.Matched || outcome.Affected != v.Affected {
			return ErrVectorIndexPartitionLiveMismatchV1
		}
		return db.PublishCommandWALNoop(intent, false)
	}
	return c.withPreparedCommandWALMutationAndReplayIntent(func() func() { return c.lockVectorIndexCoverageMutationWithColdCarrier(true) }, false, intent, func(owner *CommandWALAdmittedCollection) error {
		v, err = owner.PrepareVectorPartitionColocatedMutationV1(context.Background(), v, true)
		if err != nil {
			return err
		}
		matched, affected := int64(0), int64(0)
		if v.Delete {
			n, err := owner.DeleteBatchWithCommandWALIntent([][]byte{v.ID}, intent)
			if err != nil {
				return err
			}
			affected = int64(n)
		} else {
			m, n, err := owner.ReplaceBatchWithCommandWALIntent([][]byte{v.ID}, [][]byte{v.Document}, intent)
			if err != nil {
				return err
			}
			matched, affected = int64(m), int64(n)
		}
		proof, err := owner.ProvePreparedVectorPartitionColocatedMutationV1(context.Background(), matched, affected)
		if err != nil {
			return err
		}
		outcome, known, err := c.ReadVectorPartitionColocatedOutcomeV1(v.Scope, v.Attempt, v.CommandDigest)
		if err != nil || !known || proof.Coverage != outcome.Coverage || proof.Revision != outcome.Revision {
			return errors.Join(ErrVectorIndexPartitionLiveMismatchV1, err)
		}
		return nil
	})
}
