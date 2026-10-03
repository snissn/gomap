package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

const windowResourceTokenBytes = 2048

type windowResourceBoundary struct {
	Version             int
	RunID, Phase, Nonce string
	PublishedUTC        time.Time
	MeasuredOriginUTC   time.Time
	ActualDurationNS    int64
	StopReason          string
	AcknowledgedUTC     time.Time
	WaitNS              int64
}
type windowResourceGate struct {
	dir, runID, nonce string
	identity          os.FileInfo
	expected          map[string][]byte
}

// This is a trusted run-local filesystem handshake, not an untrusted-path API.
// Claim and receipts are retained. A consumed directory must never be reused.
func newWindowResourceGate(ctx context.Context, dir, runID string) (*windowResourceGate, error) {
	if dir == "" {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(dir) || len(dir) > 1024 || !asciiID(runID) {
		return nil, errors.New("invalid resource gate directory or run ID")
	}
	identity, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !identity.IsDir() || identity.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("resource gate requires a regular directory")
	}
	g := &windowResourceGate{dir: dir, runID: runID, identity: identity, expected: make(map[string][]byte)}
	if err := g.check(ctx, ""); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	g.nonce = hex.EncodeToString(nonce[:])
	raw, err := json.Marshal(windowResourceBoundary{Version: 1, RunID: runID, Phase: "claim", Nonce: g.nonce, PublishedUTC: time.Now().UTC()})
	if err != nil {
		return nil, err
	}
	if err := g.publish(ctx, "claim.json", append(raw, '\n')); err != nil {
		return nil, err
	}
	return g, g.check(ctx, "")
}
func windowResourceRead(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > windowResourceTokenBytes {
		return nil, errors.New("resource gate token must be bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	if statErr != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, errors.Join(errors.New("resource gate token identity changed"), statErr, file.Close())
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, windowResourceTokenBytes+1))
	err = errors.Join(readErr, file.Close(), ctx.Err())
	if len(raw) > windowResourceTokenBytes {
		err = errors.Join(err, errors.New("resource gate token exceeds byte bound"))
	}
	return raw, err
}
func (g *windowResourceGate) check(ctx context.Context, pendingAck string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	identity, err := os.Lstat(g.dir)
	if err != nil {
		return err
	}
	if !identity.IsDir() || !os.SameFile(g.identity, identity) {
		return errors.New("resource gate directory identity changed")
	}
	dir, err := os.Open(g.dir)
	if err != nil {
		return err
	}
	entries, readErr := dir.ReadDir(6) // At most claim plus two receipts and two acknowledgments.
	closeErr := dir.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return errors.Join(readErr, closeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	for _, entry := range entries {
		want, known := g.expected[entry.Name()]
		if !known {
			return errors.New("resource gate contains reused, stale or unexpected token")
		}
		raw, err := windowResourceRead(ctx, filepath.Join(g.dir, entry.Name()))
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, want) {
			return errors.New("resource gate token does not match run, phase and nonce")
		}
	}
	// Published receipts and accepted acknowledgments cannot disappear.
	for name := range g.expected {
		if name != pendingAck {
			raw, err := windowResourceRead(ctx, filepath.Join(g.dir, name))
			if err != nil {
				return err
			}
			if !bytes.Equal(raw, g.expected[name]) {
				return errors.New("resource gate receipt changed")
			}
		}
	}
	return ctx.Err()
}
func (g *windowResourceGate) publish(ctx context.Context, name string, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(raw) > windowResourceTokenBytes {
		return errors.New("resource gate receipt exceeds byte bound")
	}
	file, err := os.OpenFile(filepath.Join(g.dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, writeErr := file.Write(raw)
	if writeErr == nil && n != len(raw) {
		writeErr = io.ErrShortWrite
	}
	err = errors.Join(writeErr, file.Close(), ctx.Err())
	if err == nil {
		g.expected[name] = raw
	}
	return err
}
func (g *windowResourceGate) wait(ctx context.Context, phase string, r *windowReport) error {
	if g == nil {
		return ctx.Err()
	}
	if phase != "ready" && phase != "done" {
		return errors.New("invalid resource gate phase")
	}
	if err := g.check(ctx, ""); err != nil {
		return err
	}
	boundary := windowResourceBoundary{Version: 1, RunID: g.runID, Phase: phase, Nonce: g.nonce, PublishedUTC: time.Now().UTC(), MeasuredOriginUTC: r.MeasuredOriginUTC, ActualDurationNS: r.ActualDurationNS, StopReason: r.StopReason}
	raw, err := json.Marshal(boundary)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := g.publish(ctx, phase+".json", raw); err != nil {
		return err
	}
	r.ResourceBoundaries = append(r.ResourceBoundaries, boundary)
	target := &r.ResourceBoundaries[len(r.ResourceBoundaries)-1]
	started := time.Now()
	defer func() { target.WaitNS = time.Since(started).Nanoseconds() }()
	g.expected[phase+".ack"] = raw
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := g.check(ctx, phase+".ack"); err != nil {
			return err
		}
		ack, err := windowResourceRead(ctx, filepath.Join(g.dir, phase+".ack"))
		if err == nil {
			if !bytes.Equal(ack, raw) {
				return errors.New("resource gate acknowledgment binding mismatch")
			}
			target.AcknowledgedUTC = time.Now().UTC()
			return ctx.Err()
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Both waits occur outside windowPhase's measured clock. Clients remain owned
// and open until the enclosing run joins/drains and then releases this handshake.
func windowMeasure(ctx context.Context, clients []ownedVectorClient, in *recallInput, r *windowReport, budget *int, gate *windowResourceGate) error {
	if gate == nil {
		return windowPhase(ctx, clients, in, r, false, budget)
	}
	if err := gate.wait(ctx, "ready", r); err != nil {
		return err
	}
	phaseErr := windowPhase(ctx, clients, in, r, false, budget)
	gateErr := gate.wait(ctx, "done", r)
	return errors.Join(phaseErr, gateErr)
}
