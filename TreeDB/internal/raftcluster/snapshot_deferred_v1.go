package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// RaftSnapshotCapturerV1 captures an immutable durable cut without constructing
// its archive on the HashiCorp FSM callback goroutine.
type RaftSnapshotCapturerV1 interface {
	CaptureRaftSnapshotV1() (RaftSnapshotV1, error)
}

type deferredRaftSnapshotV1 struct {
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	materialize    func(context.Context) (RaftSnapshotV1, error)
	releaseCapture func() error
	releaseOwner   func() error
	// result is unowned: this owner alone controls its archive's lifetime.
	result   RaftSnapshotV1
	err      error
	released bool
	readers  int
}

// NewDeferredRaftSnapshotV1 transfers an immutable cut to the snapshot carrier.
// releaseCapture runs after materialization (or abandonment); releaseOwner runs
// only after final release/error and the last archive reader closes. Clones and
// the finalized result share this single ownership lifetime.
func NewDeferredRaftSnapshotV1(materialize func(context.Context) (RaftSnapshotV1, error), releaseCapture, releaseOwner func() error) (RaftSnapshotV1, error) {
	if materialize == nil || releaseCapture == nil {
		return RaftSnapshotV1{}, ErrInvalidSnapshotManifest
	}
	ctx, cancel := context.WithCancel(context.Background())
	return RaftSnapshotV1{deferred: &deferredRaftSnapshotV1{ctx: ctx, cancel: cancel, materialize: materialize, releaseCapture: releaseCapture, releaseOwner: releaseOwner}}, nil
}

// Materialize completes deferred work exactly once. A finalized result keeps
// its public manifest and archive path while sharing the original owner.
func (s RaftSnapshotV1) Materialize() (RaftSnapshotV1, error) {
	if s.deferred == nil {
		if s.owner != nil {
			s.owner.mu.Lock()
			defer s.owner.mu.Unlock()
			if err := s.owner.validateReadyLocked(s); err != nil {
				return RaftSnapshotV1{}, err
			}
		}
		return s, nil
	}
	if s.owner != nil || s.Manifest != (SnapshotManifestV1{}) || len(s.Payload) != 0 || s.ArchivePath != "" {
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
			if d.result.deferred != nil || d.result.owner != nil {
				d.err = fmt.Errorf("%w: recursive owned snapshot", ErrInvalidSnapshotManifest)
			} else {
				d.err = d.result.Validate()
			}
		}
		d.err = errors.Join(d.err, d.releaseCaptureLocked())
		if d.err != nil {
			d.cancel()
			// Never recurse into an invalid nested owner returned by a callback.
			if d.result.deferred != nil || d.result.owner != nil {
				d.result = RaftSnapshotV1{}
			}
			d.err = errors.Join(d.err, d.finishLocked())
		}
	}
	if d.err != nil {
		return RaftSnapshotV1{}, d.err
	}
	ready := d.result
	ready.owner = d
	return ready, nil
}

func (s RaftSnapshotV1) validateCapturedV1() error {
	if s.deferred == nil {
		return s.Validate()
	}
	if s.owner != nil || s.Manifest != (SnapshotManifestV1{}) || len(s.Payload) != 0 || s.ArchivePath != "" {
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

func (d *deferredRaftSnapshotV1) validateReadyLocked(s RaftSnapshotV1) error {
	if d.released || d.err != nil || d.materialize != nil || !snapshotManifestV1Equal(s.Manifest, d.result.Manifest) || s.ArchivePath != d.result.ArchivePath || !bytes.Equal(s.Payload, d.result.Payload) {
		return ErrInvalidSnapshotManifest
	}
	return d.ctx.Err()
}

func (d *deferredRaftSnapshotV1) releaseCaptureLocked() error {
	if d.releaseCapture == nil {
		return nil
	}
	err := d.releaseCapture()
	if err == nil {
		d.releaseCapture = nil
	}
	return err
}

func (d *deferredRaftSnapshotV1) finishLocked() error {
	if d.readers != 0 || d.releaseCapture != nil {
		return nil
	}
	if err := d.result.Release(); err != nil {
		// Retain the cleanup target and admission for an explicit retry.
		return err
	}
	d.result = RaftSnapshotV1{}
	if d.releaseOwner != nil {
		if err := d.releaseOwner(); err != nil {
			return err
		}
		d.releaseOwner = nil
	}
	return nil
}

func (d *deferredRaftSnapshotV1) close() error {
	// Cancel before waiting for an in-progress materializer. Archive readers
	// observe cancellation between reads; an arbitrary blocked sink cannot be
	// interrupted here and keeps admission until its reader is closed.
	d.cancel()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.released = true
	d.materialize = nil
	// A previous cleanup error is retryable even after use was revoked.
	return errors.Join(d.releaseCaptureLocked(), d.finishLocked())
}

func (d *deferredRaftSnapshotV1) openArchive(s RaftSnapshotV1) (io.ReadCloser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.validateReadyLocked(s); err != nil {
		return nil, err
	}
	src, err := d.result.OpenArchive()
	if err != nil {
		return nil, err
	}
	d.readers++
	return &ownedSnapshotArchiveReaderV1{ReadCloser: src, owner: d}, nil
}

type ownedSnapshotArchiveReaderV1 struct {
	io.ReadCloser
	owner *deferredRaftSnapshotV1
	once  sync.Once
	err   error
}

func (r *ownedSnapshotArchiveReaderV1) Read(p []byte) (int, error) {
	if err := r.owner.ctx.Err(); err != nil {
		return 0, err
	}
	return r.ReadCloser.Read(p)
}

func (r *ownedSnapshotArchiveReaderV1) Close() error {
	r.once.Do(func() {
		r.err = r.ReadCloser.Close()
		r.owner.mu.Lock()
		defer r.owner.mu.Unlock()
		r.owner.readers--
		if r.owner.released {
			r.err = errors.Join(r.err, r.owner.releaseCaptureLocked(), r.owner.finishLocked())
		}
	})
	return r.err
}

// RetryCleanupV1 retries a revoked or failed carrier without revoking a live
// export. Admission callers must still enforce their single-operation limit.
// An active materializer is never waited on by this opportunistic retry.
func (s RaftSnapshotV1) RetryCleanupV1() error {
	d := s.deferred
	if d == nil {
		d = s.owner
	}
	if d == nil || !d.mu.TryLock() {
		return nil
	}
	defer d.mu.Unlock()
	if !d.released && d.err == nil && d.ctx.Err() == nil {
		return nil
	}
	d.cancel()
	d.released = true
	d.materialize = nil
	return errors.Join(d.releaseCaptureLocked(), d.finishLocked())
}
