package ycsbevidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Scan the two flat phase request objects with the standard JSON tokenizer.
// Keep scanning after a duplicate so an earlier phase operation cannot be
// hidden by a later value and escape into the legacy checkpoint arm.
func phaseStatsObject(raw []byte) (map[string]json.RawMessage, bool, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false, errors.New("phase stats requires a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	seen := make(map[string]bool)
	phase := false
	var duplicate error
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return nil, phase, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, phase, errors.New("invalid phase stats member")
		}
		var value json.RawMessage
		if err = d.Decode(&value); err != nil {
			return nil, phase, err
		}
		folded := strings.ToLower(name)
		if seen[folded] && duplicate == nil {
			duplicate = fmt.Errorf("duplicate phase stats member %q", name)
		}
		seen[folded] = true
		fields[name] = value
		if folded == "operation" {
			var operation string
			if json.Unmarshal(value, &operation) == nil && operation == "phase_stats" {
				phase = true
			}
		}
	}
	_, err = d.Token()
	return fields, phase, errors.Join(duplicate, err)
}

func phaseStatsRequestJSON(raw []byte) (bool, error) {
	fields, phase, err := phaseStatsObject(raw)
	if !phase || err != nil {
		return phase, err
	}
	if len(fields) != 11 {
		return true, errors.New("phase stats requires exact request members")
	}
	for name, value := range fields {
		switch name {
		case "schema_version", "owner_id", "request_id", "client", "source_id", "operation", "campaign_id", "phase_id", "stem", "boundary", "sequence":
		default:
			return true, fmt.Errorf("noncanonical phase stats member %q", name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return true, fmt.Errorf("null phase stats member %q", name)
		}
	}
	client, _, err := phaseStatsObject(fields["client"])
	if err != nil {
		return true, err
	}
	if len(client) != 4 {
		return true, errors.New("phase stats requires exact client members")
	}
	for name, value := range client {
		switch name {
		case "pid", "start_ticks", "exe_sha256", "gomaxprocs_unset":
		default:
			return true, fmt.Errorf("noncanonical phase stats client member %q", name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return true, fmt.Errorf("null phase stats client member %q", name)
		}
	}
	return true, nil
}

// JSON may be pretty printed. Its complete object must be followed only by
// whitespace through a newline, not EOF on the still-open request/response
// connection. The shared limited reader bounds the complete frame to 8192 bytes.
func phaseStatsRequestTail(buffered []byte, unread io.Reader) error {
	for _, b := range buffered {
		if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
			return errors.New("trailing phase stats JSON/content")
		}
	}
	reader := io.MultiReader(bytes.NewReader(buffered), unread)
	var next [1]byte
	for {
		if _, err := io.ReadFull(reader, next[:]); err != nil {
			return fmt.Errorf("phase stats requires a bounded newline-terminated frame: %w", err)
		}
		switch next[0] {
		case '\n':
			return nil
		case ' ', '\t', '\r':
		default:
			return errors.New("trailing phase stats JSON/content")
		}
	}
}

// phaseStatsReceipt is a read-only observation of the original live owner.
// Counters may include concurrent maintenance and are not one atomic snapshot.
type phaseStatsReceipt struct {
	SchemaVersion int               `json:"schema_version"`
	Kind          string            `json:"kind"`
	Request       CheckpointRequest `json:"request"`
	Owner         Observation       `json:"owner"`
	Result        Result            `json:"result"`
}

type phaseStatsResponse struct {
	Receipt phaseStatsReceipt `json:"receipt"`
	Error   string            `json:"error"`
}

// The existing control serializes all requests, so only its last accepted
// request is needed. There is no registry or observer goroutine.
type phaseStatsOrder struct{ last CheckpointRequest }

func lowerHex(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (p *phaseStatsOrder) validate(o *Observer, r CheckpointRequest) error {
	if r.SchemaVersion != 1 || r.OwnerID != o.ownerID || !lowerHex(r.RequestID, 32) || r.SourceID == "" ||
		r.CampaignID == "" || r.PhaseID == "" || r.FixtureID != "" || r.SnapshotSHA256 != "" ||
		r.Sequence < 1 || r.Sequence > 64 || r.Sequence != p.last.Sequence+1 {
		return errors.New("phase stats request identity/sequence mismatch")
	}
	stem := "load"
	if pair := (r.Sequence - 1) / 2; pair != 0 {
		stem = fmt.Sprintf("run_%d", pair)
	}
	boundary := "before"
	if r.Sequence%2 == 0 {
		boundary = "after"
	}
	prefix := r.CampaignID + ":treedb_mongo:" + stem + ":"
	if r.Stem != stem || r.Boundary != boundary || !strings.HasPrefix(r.PhaseID, prefix) || !lowerHex(strings.TrimPrefix(r.PhaseID, prefix), 32) {
		return errors.New("phase stats stem/boundary/phase mismatch")
	}
	if p.last.Sequence != 0 && (r.CampaignID != p.last.CampaignID || r.SourceID != p.last.SourceID || r.RequestID == p.last.RequestID || r.Sequence%2 == 0 && r.PhaseID != p.last.PhaseID) {
		return errors.New("phase stats campaign/source/pair mismatch")
	}
	return validateLiveProcess(r.Client)
}

func (o *Observer) phaseStats(request CheckpointRequest) (phaseStatsReceipt, error) {
	r := phaseStatsReceipt{SchemaVersion: 1, Kind: "read_only_live_owner_phase_stats", Request: request}
	var observeErr error
	r.Result = Call(func() error {
		r.Owner, observeErr = o.observe()
		return observeErr
	})
	return r, observeErr
}
