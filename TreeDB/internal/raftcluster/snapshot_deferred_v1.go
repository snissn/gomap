package raftcluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// RaftSnapshotCapturerV1 captures an immutable durable cut without constructing
// its archive on the HashiCorp FSM callback goroutine.
type RaftSnapshotCapturerV1 interface {
	CaptureRaftSnapshotV1() (RaftSnapshotV1, error)
}

type deferredRaftSnapshotV1 struct {
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	materialize func(context.Context) (RaftSnapshotV1, error)
	release     func() error
	result      RaftSnapshotV1
	err         error
	released    bool
}

// NewDeferredRaftSnapshotV1 transfers an already captured immutable cut to the
// snapshot carrier. The callback must derive and validate its final manifest
// from that cut; no provisional logical digest is exposed. Clones share this
// single ownership lifetime, just as archive-backed clones share one archive.
func NewDeferredRaftSnapshotV1(materialize func(context.Context) (RaftSnapshotV1, error), release func() error) (RaftSnapshotV1, error) {
	if materialize == nil || release == nil {
		return RaftSnapshotV1{}, ErrInvalidSnapshotManifest
	}
	ctx, cancel := context.WithCancel(context.Background())
	return RaftSnapshotV1{deferred: &deferredRaftSnapshotV1{ctx: ctx, cancel: cancel, materialize: materialize, release: release}}, nil
}

// Materialize completes deferred work exactly once. Ordinary snapshots pass
// through unchanged. A failed attempt is terminal; its capture is released.
func (s RaftSnapshotV1) Materialize() (RaftSnapshotV1, error) {
	if s.deferred == nil {
		return s, nil
	}
	if s.Manifest != (SnapshotManifestV1{}) || len(s.Payload) != 0 || s.ArchivePath != "" {
		return RaftSnapshotV1{}, ErrInvalidSnapshotManifest
	}
	d := s.deferred
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.released {
		return RaftSnapshotV1{}, fmt.Errorf("%w: captured snapshot released", ErrInvalidSnapshotManifest)
	}
	if d.materialize != nil {
		d.result, d.err = d.materialize(d.ctx)
		d.materialize = nil
		if d.err == nil {
			d.err = d.ctx.Err()
		}
		if d.err == nil {
			if d.result.deferred != nil {
				d.err = fmt.Errorf("%w: recursive deferred snapshot", ErrInvalidSnapshotManifest)
			} else {
				d.err = d.result.Validate()
			}
		}
		d.err = errors.Join(d.err, d.release())
		d.release = nil
		if d.err != nil {
			if d.result.deferred == nil {
				d.err = errors.Join(d.err, d.result.Release())
			}
			d.result = RaftSnapshotV1{}
		}
	}
	return d.result, d.err
}

func (s RaftSnapshotV1) validateCapturedV1() error {
	if s.deferred == nil {
		return s.Validate()
	}
	if s.Manifest != (SnapshotManifestV1{}) || len(s.Payload) != 0 || s.ArchivePath != "" {
		return ErrInvalidSnapshotManifest
	}
	d := s.deferred
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.released {
		return ErrInvalidSnapshotManifest
	}
	return d.err
}

func (d *deferredRaftSnapshotV1) close() error {
	// Cancel before waiting for an in-progress materializer, so its bounded
	// copy loops can observe release without waiting for the whole archive.
	d.cancel()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.released {
		return nil
	}
	d.released = true
	err := d.result.Release()
	if d.release != nil {
		err = errors.Join(err, d.release())
		d.release = nil
	}
	d.materialize = nil
	return err
}
