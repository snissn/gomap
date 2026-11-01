package ycsbevidence

// This private evidence control is deliberately separate from Mongo wire APIs.
// It is enabled only by YCSB_OWNER_CHECKPOINT=1, needs the live observer, and
// admits one checkpoint plus bounded read-only phase observations. No callback
// runs on a timed Mongo request path.
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

type CheckpointRequest struct {
	SchemaVersion  int     `json:"schema_version"`
	OwnerID        string  `json:"owner_id"`
	RequestID      string  `json:"request_id"`
	Client         Process `json:"client"`
	SourceID       string  `json:"source_id"`
	FixtureID      string  `json:"fixture_id,omitempty"`
	SnapshotSHA256 string  `json:"snapshot_sha256,omitempty"`
	Operation      string  `json:"operation,omitempty"`
	CampaignID     string  `json:"campaign_id,omitempty"`
	PhaseID        string  `json:"phase_id,omitempty"`
	Stem           string  `json:"stem,omitempty"`
	Boundary       string  `json:"boundary,omitempty"`
	Sequence       int     `json:"sequence,omitempty"`
}
type CheckpointReceipt struct {
	SchemaVersion int               `json:"schema_version"`
	Kind          string            `json:"kind"`
	Request       CheckpointRequest `json:"request"`
	Owner         Observation       `json:"owner"`
	Flush         Result            `json:"collections_flush_all"`
	Checkpoint    Result            `json:"collections_checkpoint_storage"`
	Result        Result            `json:"result"`
}
type checkpointEndpoint struct {
	SchemaVersion int    `json:"schema_version"`
	Kind          string `json:"kind"`
	OwnerID       string `json:"owner_id"`
	Address       string `json:"address"`
}
type checkpointResponse struct {
	Receipt CheckpointReceipt `json:"receipt"`
	Error   string            `json:"error"`
}
type CheckpointControl struct {
	listener net.Listener
	done     chan error
}

// StartCheckpointControl requires callbacks from the actual owning collection
// manager. A borrowed backend Checkpoint cannot seal native COW accepted state.
func (o *Observer) StartCheckpointControl(flush, checkpoint func() error) (*CheckpointControl, error) {
	phase := os.Getenv("YCSB_OWNER_PHASE_STATS")
	if phase != "" && (phase != "1" || os.Getenv("YCSB_OWNER_CHECKPOINT") != "1" || o == nil || o.backend == nil || o.stats == nil || o.dir == "" || o.ownerID == "") {
		return nil, errors.New("phase stats require explicit checkpoint control and actual live observer")
	}
	if os.Getenv("YCSB_OWNER_CHECKPOINT") == "" {
		return nil, nil
	}
	if os.Getenv("YCSB_OWNER_CHECKPOINT") != "1" || o == nil || flush == nil || checkpoint == nil {
		return nil, errors.New("checkpoint control requires explicit opt-in and actual live owner")
	}
	return o.startCheckpointControl(flush, checkpoint)
}
func (o *Observer) startCheckpointControl(flush, checkpoint func() error) (*CheckpointControl, error) {
	phaseEnabled := os.Getenv("YCSB_OWNER_PHASE_STATS") == "1"
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	endpoint := checkpointEndpoint{1, "private_owner_checkpoint_control", o.ownerID, ln.Addr().String()}
	if err = writeNew(filepath.Join(o.dir, "owner.checkpoint-control.json"), endpoint); err != nil {
		ln.Close()
		return nil, err
	}
	control := &CheckpointControl{ln, make(chan error, 1)}
	go func() {
		var retainedErr error
		var phaseFailure error
		var phases phaseStatsOrder
		defer func() { control.done <- retainedErr }()
		for {
			conn, err := ln.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					retainedErr = errors.Join(retainedErr, err)
				}
				return
			}
			// Bounded framing; backend execution is bounded by the owner's external
			// supervisor. Close waits for the actual callback, never races owner Close.
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			var request CheckpointRequest
			var captured bytes.Buffer
			bounded := io.LimitReader(conn, 8192)
			decoder := json.NewDecoder(io.TeeReader(bounded, &captured))
			decoder.DisallowUnknownFields()
			err = decoder.Decode(&request)
			if err == nil {
				// Inspect every operation occurrence, including an earlier phase
				// value hidden by a duplicate. Legacy checkpoint decoding stays
				// unchanged; only phase requests require the strict JSON frame.
				phase, frameErr := phaseStatsRequestJSON(captured.Bytes()[:decoder.InputOffset()])
				if phase {
					err = frameErr
					if err == nil {
						err = phaseStatsRequestTail(captured.Bytes()[decoder.InputOffset():], bounded)
					}
				}
			}
			_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))
			response := checkpointResponse{}
			var wireResponse any = &response
			phaseAdmitted := false
			if err == nil && phaseFailure != nil {
				err = fmt.Errorf("phase stats control is terminal: %w", phaseFailure)
			}
			if err == nil && request.Operation == "phase_stats" {
				phaseResponse := phaseStatsResponse{}
				if !phaseEnabled {
					err = errors.New("phase stats are disabled")
				} else {
					err = phases.validate(o, request)
				}
				if err == nil {
					phaseAdmitted = true
					// Once admitted, an observation or evidence-write failure is
					// terminal. Never retry an observation behind a failed receipt.
					err = writeNew(filepath.Join(o.dir, fmt.Sprintf("owner.phase-stats-%02d-request.json", request.Sequence)), request)
					if err == nil {
						phaseResponse.Receipt, err = o.phaseStats(request)
						err = errors.Join(err, writeNew(filepath.Join(o.dir, fmt.Sprintf("owner.phase-stats-%02d.json", request.Sequence)), phaseResponse.Receipt))
					}
					if err != nil {
						phaseFailure = err
						retainedErr = errors.Join(retainedErr, err)
					} else {
						phases.last = request
					}
				}
				if err != nil {
					phaseResponse.Error = err.Error()
				}
				wireResponse = &phaseResponse
			} else if err == nil && request.Operation != "" {
				err = errors.New("unknown owner control operation")
			} else if err == nil && (request.SchemaVersion != 1 || request.OwnerID != o.ownerID || len(request.RequestID) != 32 || len(request.SnapshotSHA256) != 64 || request.SourceID == "" || request.FixtureID == "" || request.Client.PID <= 0 || request.Client.StartTicks == 0 || len(request.Client.EXESHA256) != 64 || request.CampaignID != "" || request.PhaseID != "" || request.Stem != "" || request.Boundary != "" || request.Sequence != 0) {
				err = errors.New("checkpoint request identity mismatch")
			} else if err == nil {
				// Reserve the one allowed mutation before executing either callback.
				err = writeNew(filepath.Join(o.dir, "owner.checkpoint-request.json"), request)
				if err == nil {
					response.Receipt, err = o.checkpoint(request, flush, checkpoint)
					receiptErr := writeNew(filepath.Join(o.dir, "owner.checkpoint.json"), response.Receipt)
					retainedErr = errors.Join(retainedErr, err, receiptErr)
					err = errors.Join(err, receiptErr)
				}
			}
			if err != nil {
				response.Error = err.Error()
			}
			if err = json.NewEncoder(conn).Encode(wireResponse); err != nil {
				retainedErr = errors.Join(retainedErr, err)
				if phaseAdmitted {
					phaseFailure = errors.Join(phaseFailure, err)
				}
			}
			_ = conn.Close()
		}
	}()
	return control, nil
}
func (o *Observer) checkpoint(request CheckpointRequest, flush, checkpoint func() error) (CheckpointReceipt, error) {
	r := CheckpointReceipt{SchemaVersion: 2, Kind: "explicit_live_owner_checkpoint", Request: request}
	var actualErr error
	r.Result = Call(func() error {
		var err error
		r.Owner, err = o.observe()
		if err != nil {
			actualErr = err
			return err
		}
		r.Flush = Call(flush)
		if !r.Flush.Success {
			actualErr = fmt.Errorf("collections FlushAll: %s", r.Flush.Error)
			return actualErr
		}
		r.Checkpoint = Call(checkpoint)
		if !r.Checkpoint.Success {
			actualErr = fmt.Errorf("collections CheckpointStorage: %s", r.Checkpoint.Error)
			return actualErr
		}
		return nil
	})
	return r, actualErr
}
func (c *CheckpointControl) Close() error {
	if c == nil {
		return nil
	}
	err := c.listener.Close()
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	return errors.Join(err, <-c.done)
}

// RequestCheckpoint is used by the separately supervised snapshot fixture,
// after its read-only snapshot. A raw server-side receipt is required as well.
func RequestCheckpoint(ctx context.Context, dir, sourceID, fixtureID, snapshotPath string) (CheckpointReceipt, error) {
	var endpoint checkpointEndpoint
	raw, err := os.ReadFile(filepath.Join(dir, "owner.checkpoint-control.json"))
	if err != nil {
		return CheckpointReceipt{}, err
	}
	if err = json.Unmarshal(raw, &endpoint); err != nil {
		return CheckpointReceipt{}, err
	}
	addr, err := net.ResolveTCPAddr("tcp4", endpoint.Address)
	if err != nil || addr == nil || !addr.IP.IsLoopback() || endpoint.SchemaVersion != 1 || endpoint.Kind != "private_owner_checkpoint_control" {
		return CheckpointReceipt{}, errors.New("invalid private checkpoint endpoint")
	}
	var opened Observation
	raw, err = os.ReadFile(filepath.Join(dir, "owner.open.json"))
	if err != nil {
		return CheckpointReceipt{}, err
	}
	if err = json.Unmarshal(raw, &opened); err != nil {
		return CheckpointReceipt{}, err
	}
	if opened.OwnerID != endpoint.OwnerID {
		return CheckpointReceipt{}, errors.New("stale checkpoint control owner")
	}
	client, _, _, err := processIdentity(opened.DataDir)
	if err != nil {
		return CheckpointReceipt{}, err
	}
	snapshot, err := os.ReadFile(snapshotPath)
	if err != nil {
		return CheckpointReceipt{}, err
	}
	sum := sha256.Sum256(snapshot)
	token := make([]byte, 16)
	if _, err = rand.Read(token); err != nil {
		return CheckpointReceipt{}, err
	}
	request := CheckpointRequest{SchemaVersion: 1, OwnerID: opened.OwnerID, RequestID: hex.EncodeToString(token), Client: client, SourceID: sourceID, FixtureID: fixtureID, SnapshotSHA256: hex.EncodeToString(sum[:])}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp4", endpoint.Address)
	if err != nil {
		return CheckpointReceipt{}, err
	}
	defer conn.Close()
	deadline := time.Now().Add(2 * time.Minute)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	if err = json.NewEncoder(conn).Encode(request); err != nil {
		return CheckpointReceipt{}, err
	}
	var response checkpointResponse
	if err = json.NewDecoder(io.LimitReader(conn, 1<<20)).Decode(&response); err != nil {
		return CheckpointReceipt{}, err
	}
	if response.Error != "" {
		return response.Receipt, errors.New(response.Error)
	}
	if response.Receipt.SchemaVersion != 2 || response.Receipt.Request != request || response.Receipt.Kind != "explicit_live_owner_checkpoint" || !response.Receipt.Result.Success {
		return response.Receipt, errors.New("checkpoint response mismatch")
	}
	return response.Receipt, nil
}
