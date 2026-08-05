package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type standaloneWriteConcernPolicy uint8

const (
	standaloneWriteConcernVisible standaloneWriteConcernPolicy = iota
	standaloneWriteConcernJournal
)

type standaloneWriteConcern struct {
	policy standaloneWriteConcernPolicy
}

type standaloneWriteConcernParseError struct {
	code     int32
	codeName string
	message  string
	timeout  bool
}

func (e standaloneWriteConcernParseError) Error() string { return e.message }

// StandaloneWriteConcernStats is a monotonic snapshot of standalone write
// concern requests and acknowledgement boundaries for this Server.
type StandaloneWriteConcernStats struct {
	Requests                      uint64
	VisibleRequests               uint64
	JournalRequests               uint64
	LogicalWrites                 uint64
	VisibleAcknowledgements       uint64
	JournalAcknowledgements       uint64
	SyncAttempts                  uint64
	PhysicalSyncBoundaries        uint64
	PreMutationRejections         uint64
	MalformedRejections           uint64
	UnsupportedRejections         uint64
	TimeoutRejections             uint64
	SyncFailures                  uint64
	DurabilityUnavailableFailures uint64
	SyncBoundaryFailures          uint64
	UncertainOutcomes             uint64
	AcknowledgementNanos          uint64
	JournalSyncNanos              uint64
}

type standaloneWriteConcernStats struct {
	requests                      atomic.Uint64
	visibleRequests               atomic.Uint64
	journalRequests               atomic.Uint64
	logicalWrites                 atomic.Uint64
	visibleAcknowledgements       atomic.Uint64
	journalAcknowledgements       atomic.Uint64
	syncAttempts                  atomic.Uint64
	physicalSyncBoundaries        atomic.Uint64
	preMutationRejections         atomic.Uint64
	malformedRejections           atomic.Uint64
	unsupportedRejections         atomic.Uint64
	timeoutRejections             atomic.Uint64
	syncFailures                  atomic.Uint64
	durabilityUnavailableFailures atomic.Uint64
	syncBoundaryFailures          atomic.Uint64
	uncertainOutcomes             atomic.Uint64
	acknowledgementNanos          atomic.Uint64
	journalSyncNanos              atomic.Uint64
}

// StandaloneWriteConcernStats returns the current standalone acknowledgement
// diagnostics. Cluster acknowledgement diagnostics remain authoritative in
// the cluster submitter path and are intentionally not mixed into this view.
func (s *Server) StandaloneWriteConcernStats() StandaloneWriteConcernStats {
	if s == nil {
		return StandaloneWriteConcernStats{}
	}
	stats := &s.writeConcernStats
	return StandaloneWriteConcernStats{
		Requests:                      stats.requests.Load(),
		VisibleRequests:               stats.visibleRequests.Load(),
		JournalRequests:               stats.journalRequests.Load(),
		LogicalWrites:                 stats.logicalWrites.Load(),
		VisibleAcknowledgements:       stats.visibleAcknowledgements.Load(),
		JournalAcknowledgements:       stats.journalAcknowledgements.Load(),
		SyncAttempts:                  stats.syncAttempts.Load(),
		PhysicalSyncBoundaries:        stats.physicalSyncBoundaries.Load(),
		PreMutationRejections:         stats.preMutationRejections.Load(),
		MalformedRejections:           stats.malformedRejections.Load(),
		UnsupportedRejections:         stats.unsupportedRejections.Load(),
		TimeoutRejections:             stats.timeoutRejections.Load(),
		SyncFailures:                  stats.syncFailures.Load(),
		DurabilityUnavailableFailures: stats.durabilityUnavailableFailures.Load(),
		SyncBoundaryFailures:          stats.syncBoundaryFailures.Load(),
		UncertainOutcomes:             stats.uncertainOutcomes.Load(),
		AcknowledgementNanos:          stats.acknowledgementNanos.Load(),
		JournalSyncNanos:              stats.journalSyncNanos.Load(),
	}
}

func standaloneWriteCommand(name string) bool {
	switch name {
	case "create", "createIndexes", "delete", "dropIndexes", "findAndModify", "insert", "update":
		return true
	default:
		return false
	}
}

func (s *Server) standaloneWriteCommandResponse(ctx context.Context, name string, command wire.Document, sequences []wire.DocumentSequence, cursorOwner int64) (wire.Document, error) {
	start := time.Now()
	stats := &s.writeConcernStats
	stats.requests.Add(1)
	concern, err := parseStandaloneWriteConcern(command)
	if err != nil {
		stats.preMutationRejections.Add(1)
		var parseErr standaloneWriteConcernParseError
		if errors.As(err, &parseErr) {
			if parseErr.timeout {
				stats.timeoutRejections.Add(1)
			}
			if parseErr.code == commandCodeFailedToParse || parseErr.code == commandCodeBadValue {
				stats.malformedRejections.Add(1)
			} else {
				stats.unsupportedRejections.Add(1)
			}
			return commandError(parseErr.code, parseErr.codeName, parseErr.message)
		}
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if concern.policy == standaloneWriteConcernJournal {
		stats.journalRequests.Add(1)
	} else {
		stats.visibleRequests.Add(1)
	}

	response, err := s.dispatchCommandResponse(ctx, name, command, sequences, cursorOwner)
	if err != nil || !mongoCommandResponseOK(response) {
		stats.acknowledgementNanos.Add(uint64(time.Since(start)))
		return response, err
	}
	stats.logicalWrites.Add(1)
	if concern.policy == standaloneWriteConcernVisible {
		stats.visibleAcknowledgements.Add(1)
		stats.acknowledgementNanos.Add(uint64(time.Since(start)))
		return response, nil
	}

	stats.syncAttempts.Add(1)
	syncStart := time.Now()
	physicalSync, err := s.syncStandaloneWriteConcern()
	stats.journalSyncNanos.Add(uint64(time.Since(syncStart)))
	stats.acknowledgementNanos.Add(uint64(time.Since(start)))
	if physicalSync {
		stats.physicalSyncBoundaries.Add(1)
	}
	if err == nil {
		stats.journalAcknowledgements.Add(1)
		return response, nil
	}
	stats.syncFailures.Add(1)
	if errors.Is(err, backenddb.ErrClosed) {
		stats.durabilityUnavailableFailures.Add(1)
	} else {
		stats.syncBoundaryFailures.Add(1)
	}
	stats.uncertainOutcomes.Add(1)
	return appendStandaloneWriteConcernError(response, err)
}

func (s *Server) syncStandaloneWriteConcern() (physicalSync bool, err error) {
	if s == nil {
		return false, errors.New("Mongo gateway standalone writeConcern sync is unavailable")
	}
	if s.standaloneWriteConcernSync != nil {
		return s.standaloneWriteConcernSync()
	}
	if s.Collections == nil {
		return false, errors.New("Mongo gateway standalone writeConcern sync has no collection manager")
	}
	return s.Collections.SyncForStandaloneWriteConcern()
}

func parseStandaloneWriteConcern(command wire.Document) (standaloneWriteConcern, error) {
	concern := standaloneWriteConcern{policy: standaloneWriteConcernVisible}
	elements, err := bson.Raw(command).Elements()
	if err != nil {
		return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("Mongo command document is malformed: %v", err)}
	}
	var value bson.RawValue
	for _, element := range elements {
		key, err := element.KeyErr()
		if err != nil {
			return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("Mongo command document is malformed: %v", err)}
		}
		if key != "writeConcern" {
			continue
		}
		if !value.IsZero() {
			return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: "Mongo command field \"writeConcern\" is duplicated"}
		}
		value = element.Value()
	}
	if value.IsZero() {
		return concern, nil
	}
	raw, ok := value.DocumentOK()
	if !ok {
		return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: "Mongo command field \"writeConcern\" must be a document"}
	}
	elements, err = raw.Elements()
	if err != nil {
		return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("Mongo command field \"writeConcern\" is malformed: %v", err)}
	}
	seen := make(map[string]struct{}, len(elements))
	for _, element := range elements {
		key, err := element.KeyErr()
		if err != nil {
			return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("Mongo command field \"writeConcern\" is malformed: %v", err)}
		}
		if _, ok := seen[key]; ok {
			return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: fmt.Sprintf("Mongo writeConcern field %q is duplicated", key)}
		}
		seen[key] = struct{}{}
		switch key {
		case "w":
			if err := parseStandaloneWriteConcernW(element.Value()); err != nil {
				return concern, err
			}
		case "j":
			journal, ok := element.Value().BooleanOK()
			if !ok {
				return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: "Mongo command field \"writeConcern.j\" must be a boolean"}
			}
			if journal {
				concern.policy = standaloneWriteConcernJournal
			}
		case "wtimeout":
			timeout, ok := strictBSONInt64(element.Value())
			if !ok {
				return concern, standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: "Mongo command field \"writeConcern.wtimeout\" must be an integer"}
			}
			if timeout < 0 {
				return concern, standaloneWriteConcernParseError{code: commandCodeBadValue, codeName: "BadValue", message: "Mongo gateway standalone writeConcern wtimeout must be non-negative"}
			}
			if timeout > 0 {
				return concern, standaloneWriteConcernParseError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: "Mongo gateway standalone does not support positive wtimeout because the TreeDB sync boundary is not interruptible", timeout: true}
			}
		case "wtimeoutMS":
			return concern, standaloneWriteConcernParseError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: "Mongo gateway standalone does not support deprecated writeConcern field \"wtimeoutMS\""}
		default:
			return concern, standaloneWriteConcernParseError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: fmt.Sprintf("Mongo gateway standalone writeConcern does not support %q", key)}
		}
	}
	return concern, nil
}

func parseStandaloneWriteConcernW(value bson.RawValue) error {
	if w, ok := strictBSONInt64(value); ok {
		switch {
		case w == 1:
			return nil
		case w == 0:
			return standaloneWriteConcernParseError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: "Mongo gateway standalone does not support unacknowledged w:0 writes"}
		case w > 1:
			return standaloneWriteConcernParseError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: "Mongo gateway standalone does not provide replica acknowledgement for numeric w greater than 1"}
		default:
			return standaloneWriteConcernParseError{code: commandCodeBadValue, codeName: "BadValue", message: "Mongo gateway standalone writeConcern w must be non-negative"}
		}
	}
	if w, ok := value.StringValueOK(); ok {
		if w == "majority" {
			return standaloneWriteConcernParseError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: "Mongo gateway standalone cannot satisfy writeConcern majority without replica commit proof"}
		}
		return standaloneWriteConcernParseError{code: commandCodeWriteConcernFailed, codeName: "WriteConcernFailed", message: fmt.Sprintf("Mongo gateway standalone does not support writeConcern tag %q", w)}
	}
	return standaloneWriteConcernParseError{code: commandCodeFailedToParse, codeName: "FailedToParse", message: "Mongo command field \"writeConcern.w\" must be integer 1"}
}

func mongoCommandResponseOK(response wire.Document) bool {
	if len(response) == 0 {
		return false
	}
	value := bson.Raw(response).Lookup("ok")
	if number, ok := value.DoubleOK(); ok {
		return number == 1
	}
	if number, ok := value.Int32OK(); ok {
		return number == 1
	}
	if number, ok := value.Int64OK(); ok {
		return number == 1
	}
	return false
}

func appendStandaloneWriteConcernError(response wire.Document, syncErr error) (wire.Document, error) {
	reason := "sync_boundary_failed"
	if errors.Is(syncErr, backenddb.ErrClosed) {
		reason = "durability_unavailable"
	}
	var document bson.D
	if err := bson.Unmarshal(response, &document); err != nil {
		return commandError(commandCodeWriteConcernFailed, "WriteConcernFailed", "Mongo gateway standalone mutation may have occurred but the requested TreeDB sync boundary failed: "+syncErr.Error())
	}
	document = append(document,
		bson.E{Key: "writeConcernError", Value: bson.D{
			{Key: "code", Value: commandCodeWriteConcernFailed},
			{Key: "codeName", Value: "WriteConcernFailed"},
			{Key: "errmsg", Value: "Mongo gateway standalone mutation may have occurred but the requested TreeDB sync boundary failed: " + syncErr.Error()},
			{Key: "errInfo", Value: bson.D{{Key: "treedbMutationMayHaveOccurred", Value: true}, {Key: "treedbDurabilityUncertain", Value: true}, {Key: "treedbFailureReason", Value: reason}}},
		}},
		bson.E{Key: "errorLabels", Value: bson.A{"TreeDBWriteConcernUncertain"}},
	)
	return marshalDocument(document)
}
