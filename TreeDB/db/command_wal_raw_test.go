package db

import (
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALIntentZeroValueLSNSentinelsM10C(t *testing.T) {
	var nilIntent *CommandWALIntent
	if got := nilIntent.AssignedLSN(); got != 0 {
		t.Fatalf("nil AssignedLSN=%d, want 0", got)
	}
	if got, replay := nilIntent.ReplayAssignedLSN(); got != 0 || replay {
		t.Fatalf("nil ReplayAssignedLSN=(%d,%t), want (0,false)", got, replay)
	}

	var zero CommandWALIntent
	if got := zero.AssignedLSN(); got != 0 {
		t.Fatalf("zero AssignedLSN=%d, want 0", got)
	}
	if got, replay := zero.ReplayAssignedLSN(); got != 0 || replay {
		t.Fatalf("zero ReplayAssignedLSN=(%d,%t), want (0,false)", got, replay)
	}
}

func TestCommandWALReplayIntentZeroLSNFailsClosedM10C(t *testing.T) {
	intent := NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	})
	if got := intent.AssignedLSN(); got != 0 {
		t.Fatalf("zero-lsn replay AssignedLSN=%d, want 0", got)
	}
	if got, replay := intent.ReplayAssignedLSN(); got != 0 || replay {
		t.Fatalf("zero-lsn replay ReplayAssignedLSN=(%d,%t), want (0,false)", got, replay)
	}

	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.AppendCommandWALIntent(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent zero-lsn replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing assigned lsn") {
		t.Fatalf("AppendCommandWALIntent zero-lsn replay error=%v, want missing assigned lsn", err)
	}
	if err := d.PublishCommandWALNoop(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("PublishCommandWALNoop zero-lsn replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing assigned lsn") {
		t.Fatalf("PublishCommandWALNoop zero-lsn replay error=%v, want missing assigned lsn", err)
	}
}

func TestCommandWALReplayIntentRequiresActiveRecoveryFrameM10C(t *testing.T) {
	intent := NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		LSN:           7,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	})
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.AppendCommandWALIntent(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent fabricated replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "active recovery frame") {
		t.Fatalf("AppendCommandWALIntent fabricated replay error=%v, want active recovery frame", err)
	}
	if err := d.PublishCommandWALNoop(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("PublishCommandWALNoop fabricated replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "active recovery frame") {
		t.Fatalf("PublishCommandWALNoop fabricated replay error=%v, want active recovery frame", err)
	}
}

func TestCommandWALReplayIntentConstructorRequiresActiveRecoveryFrameM10C(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent zero lsn error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing assigned lsn") {
		t.Fatalf("NewCommandWALReplayIntent zero lsn error=%v, want missing assigned lsn", err)
	}

	if _, err := d.NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		LSN:           7,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent outside recovery error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "no active recovery frame") {
		t.Fatalf("NewCommandWALReplayIntent outside recovery error=%v, want no active recovery frame", err)
	}
}

func TestCommandWALReplayIntentRequiresActiveRecoveryTokenM10C(t *testing.T) {
	env := commitlog.CommandEnvelope{
		LSN:           7,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	d.commandWALReplayLSN.Store(env.LSN)

	if _, err := d.NewCommandWALReplayIntent(env); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent missing token error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "no active recovery token") {
		t.Fatalf("NewCommandWALReplayIntent missing token error=%v, want no active recovery token", err)
	}

	d.commandWALReplayLSN.Store(env.LSN + 1)
	d.commandWALReplayToken.Store(99)
	if _, err := d.NewCommandWALReplayIntent(env); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent lsn mismatch error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "does not match active recovery frame lsn") {
		t.Fatalf("NewCommandWALReplayIntent lsn mismatch error=%v, want active recovery frame lsn mismatch", err)
	}

	d.commandWALReplayLSN.Store(env.LSN)
	d.commandWALReplayToken.Store(99)

	forged := NewCommandWALReplayIntent(env)
	if _, err := d.AppendCommandWALIntent(forged, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent forged replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing recovery token") {
		t.Fatalf("AppendCommandWALIntent forged replay error=%v, want missing recovery token", err)
	}

	forged = newCommandWALReplayIntent(env, 100)
	if _, err := d.AppendCommandWALIntent(forged, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent forged replay token mismatch error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "recovery token mismatch") {
		t.Fatalf("AppendCommandWALIntent forged replay token mismatch error=%v, want recovery token mismatch", err)
	}

	authorized, err := d.NewCommandWALReplayIntent(env)
	if err != nil {
		t.Fatalf("NewCommandWALReplayIntent active recovery: %v", err)
	}
	if got, err := d.AppendCommandWALIntent(authorized, false); err != nil || got != env.LSN {
		t.Fatalf("AppendCommandWALIntent authorized replay=(%d,%v), want (%d,nil)", got, err, env.LSN)
	}
}
