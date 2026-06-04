package db

import (
	"errors"
	"path/filepath"
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

func TestTrustedCommandWALIntentAppendsCanonicalCollectionPayload(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("user-001"), Document: []byte(`{"_id":"user-001"}`)},
	})
	if err != nil {
		_ = d.Close()
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	intent, err := d.NewTrustedCommandWALIntent(
		commitlog.CommandKindCollectionInsertBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionInsertBatchByIDV1,
		payload,
	)
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewTrustedCommandWALIntent: %v", err)
	}
	lsn, err := d.AppendCommandWALIntent(intent, false)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	if lsn == 0 || intent.AssignedLSN() != lsn {
		_ = d.Close()
		t.Fatalf("trusted intent lsn=%d assigned=%d, want non-zero match", lsn, intent.AssignedLSN())
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != lsn || env.Kind != commitlog.CommandKindCollectionInsertBatchByID || env.Scope != commitlog.CommandScopeCollection || env.PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
		t.Fatalf("decoded trusted command identity mismatch: %+v", env)
	}
	if _, err := commitlog.DecodeCollectionInsertBatchByIDPayload(env.Payload); err != nil {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload: %v", err)
	}
}

func TestAppendRawKVSingleCommandWALSupportsDeleteRange(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	lsn, err := d.AppendRawKVSingleCommandWAL(commitlog.RawKVOperation{
		Op:    commitlog.RawKVOpDeleteRange,
		Key:   nil,
		Value: []byte("m"),
	}, true)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendRawKVSingleCommandWAL DeleteRange: %v", err)
	}
	if lsn == 0 {
		_ = d.Close()
		t.Fatalf("AppendRawKVSingleCommandWAL DeleteRange lsn=0")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != lsn || env.Kind != commitlog.CommandKindRawKVBatch || env.Scope != commitlog.CommandScopeRawKV || env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		t.Fatalf("decoded DeleteRange command identity mismatch: %+v", env)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpDeleteRange || ops[0].Key != nil || string(ops[0].Value) != "m" {
		t.Fatalf("decoded DeleteRange ops=%+v, want single [nil,m)", ops)
	}
}

func TestCommandWALReplayIntentZeroLSNFailsClosedM10C(t *testing.T) {
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}, 0)
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
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{
		LSN:           7,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}, 0)
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

	forged := newCommandWALReplayIntent(env, 0)
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
