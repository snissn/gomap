package nativewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// ErrServerClosed reports that the server is closed or refusing connections.
var ErrServerClosed = errors.New("nativewire: server is closed")

// WireError is an error response decoded from a remote native-wire peer.
type WireError struct {
	Code      iwire.ErrorCode
	Retryable bool
	Message   string
}

func (e *WireError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return fmt.Sprintf("nativewire: remote error code %d", e.Code)
	}
	return fmt.Sprintf("nativewire: remote error code %d: %s", e.Code, e.Message)
}

// IsCatalogVersionMismatch reports whether err is a remote catalog-version
// guard failure.
func IsCatalogVersionMismatch(err error) bool {
	return isRemoteError(err, iwire.ErrCatalogVersionMismatch)
}

// ClusterRouteErrorMetadata is the nativewire projection of stable route-error
// metadata returned for redirect-first cluster write failures.
type ClusterRouteErrorMetadata struct {
	Class         string
	Database      string
	Catalog       string
	Collection    string
	Shape         string
	GroupID       string
	Members       []string
	LeaderHint    string
	PlacementMode string
	RouteKey      string
	TokenKnown    bool
	Token         uint64
	PartitionID   string
	LocalGroupID  string
}

// Clone returns an independent copy of the metadata.
func (m ClusterRouteErrorMetadata) Clone() ClusterRouteErrorMetadata {
	m.Members = append([]string(nil), m.Members...)
	return m
}

// ClusterRouteErrorMetadataOf returns stable route metadata carried by err.
func ClusterRouteErrorMetadataOf(err error) (ClusterRouteErrorMetadata, bool) {
	if err == nil {
		return ClusterRouteErrorMetadata{}, false
	}
	var routed interface {
		ClusterRouteErrorMetadata() ClusterRouteErrorMetadata
	}
	if errors.As(err, &routed) {
		metadata := routed.ClusterRouteErrorMetadata()
		if clusterRouteErrorMetadataPresent(metadata) {
			return metadata.Clone(), true
		}
	}
	if route, ok := raftcluster.RouteErrorMetadataOf(err); ok {
		return clusterRouteErrorMetadataFromRaft(route), true
	}
	var wireErr *WireError
	if errors.As(err, &wireErr) {
		if wireErr.Code != iwire.ErrReadOnly {
			return ClusterRouteErrorMetadata{}, false
		}
		return parseClusterRouteErrorMetadata(wireErr.Message)
	}
	var protocolErr *iwire.ProtocolError
	if errors.As(err, &protocolErr) {
		if protocolErr.Code != iwire.ErrReadOnly {
			return ClusterRouteErrorMetadata{}, false
		}
		return parseClusterRouteErrorMetadata(protocolErr.Reason)
	}
	return ClusterRouteErrorMetadata{}, false
}

func clusterRouteErrorMetadataPresent(metadata ClusterRouteErrorMetadata) bool {
	return metadata.Class != ""
}

func clusterRouteErrorMetadataFromRaft(route raftcluster.RouteErrorMetadata) ClusterRouteErrorMetadata {
	return ClusterRouteErrorMetadata{
		Class:         string(route.Class),
		Database:      route.Database,
		Catalog:       route.Catalog,
		Collection:    route.Collection,
		Shape:         route.Shape,
		GroupID:       route.GroupID,
		Members:       append([]string(nil), route.Members...),
		LeaderHint:    route.LeaderHint,
		PlacementMode: route.PlacementMode,
		RouteKey:      route.RouteKey,
		TokenKnown:    route.TokenKnown,
		Token:         route.Token,
		PartitionID:   route.PartitionID,
		LocalGroupID:  route.LocalGroupID,
	}
}

func parseClusterRouteErrorMetadata(message string) (ClusterRouteErrorMetadata, bool) {
	if message == "" || !strings.Contains(message, "route_error_class=") {
		return ClusterRouteErrorMetadata{}, false
	}
	fields := make(map[string]string)
	for _, field := range strings.Fields(message) {
		field = strings.Trim(field, " ;,\t\r\n")
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		value = strings.Trim(value, " ;,\t\r\n")
		if key != "route_members" {
			value = decodeClusterRouteErrorField(value)
		}
		fields[key] = value
	}
	class := fields["route_error_class"]
	if class == "" {
		return ClusterRouteErrorMetadata{}, false
	}
	metadata := ClusterRouteErrorMetadata{
		Class:         class,
		Database:      fields["route_database"],
		Catalog:       fields["route_catalog"],
		Collection:    fields["route_collection"],
		Shape:         fields["route_shape"],
		GroupID:       fields["route_group"],
		LeaderHint:    fields["leader_hint"],
		PlacementMode: fields["route_placement"],
		RouteKey:      fields["route_key"],
		PartitionID:   fields["route_partition"],
		LocalGroupID:  fields["local_group"],
	}
	if rawMembers := fields["route_members"]; rawMembers != "" {
		rawMembers := strings.Split(rawMembers, ",")
		metadata.Members = make([]string, 0, len(rawMembers))
		for _, member := range rawMembers {
			if member != "" {
				metadata.Members = append(metadata.Members, decodeClusterRouteErrorField(member))
			}
		}
	}
	if fields["route_token_known"] == "true" {
		metadata.TokenKnown = true
	}
	if rawToken := fields["route_token"]; rawToken != "" {
		if token, err := strconv.ParseUint(rawToken, 10, 64); err == nil {
			metadata.Token = token
		}
	}
	return metadata, true
}

func decodeClusterRouteErrorField(value string) string {
	decoded, err := url.QueryUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

func clusterRouteErrorMetadataFields(metadata ClusterRouteErrorMetadata) string {
	metadata = redactClusterRouteErrorMetadata(metadata)
	var fields []string
	appendField := func(key, value string) {
		if value != "" {
			fields = append(fields, key+"="+url.QueryEscape(value))
		}
	}
	appendField("route_error_class", metadata.Class)
	appendField("route_group", metadata.GroupID)
	appendField("leader_hint", metadata.LeaderHint)
	if len(metadata.Members) != 0 {
		escaped := make([]string, 0, len(metadata.Members))
		for _, member := range metadata.Members {
			if member != "" {
				escaped = append(escaped, url.QueryEscape(member))
			}
		}
		if len(escaped) != 0 {
			fields = append(fields, "route_members="+strings.Join(escaped, ","))
		}
	}
	appendField("route_shape", metadata.Shape)
	appendField("route_placement", metadata.PlacementMode)
	appendField("route_key", metadata.RouteKey)
	if metadata.TokenKnown {
		appendField("route_token_known", "true")
		appendField("route_token", strconv.FormatUint(metadata.Token, 10))
	}
	appendField("route_partition", metadata.PartitionID)
	appendField("local_group", metadata.LocalGroupID)
	return strings.Join(fields, " ")
}

// redactClusterRouteErrorMetadata removes tenant-sensitive namespace fields
// before route details cross a protocol boundary. Stable routing coordinates
// remain available for redirect and diagnostics.
func redactClusterRouteErrorMetadata(metadata ClusterRouteErrorMetadata) ClusterRouteErrorMetadata {
	metadata.Database = ""
	metadata.Catalog = ""
	metadata.Collection = ""
	return metadata
}

func errorCodeFor(err error) iwire.ErrorCode {
	if err == nil {
		return 0
	}
	if code, ok := iwire.ErrorCodeOf(err); ok {
		return code
	}
	switch {
	case errors.Is(err, context.Canceled):
		return iwire.ErrCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return iwire.ErrTimeout
	}
	switch documentservice.ErrorCodeOf(err) {
	case documentservice.CodeInvalidRequest, documentservice.CodeMalformedJSON:
		return iwire.ErrInvalidCommand
	case documentservice.CodeIndexNotFound:
		return iwire.ErrIndexNotFound
	case documentservice.CodeIndexUnavailable:
		return iwire.ErrConsistencyUnavailable
	case documentservice.CodeIndexStale, documentservice.CodeSnapshotMismatch, documentservice.CodeConflict:
		return iwire.ErrCatalogChanged
	case documentservice.CodeUnsupported:
		return iwire.ErrUnsupportedFeature
	}
	switch {
	case errors.Is(err, collections.ErrCollectionNotFound):
		return iwire.ErrCollectionNotFound
	case errors.Is(err, collections.ErrIndexNotFound):
		return iwire.ErrIndexNotFound
	case errors.Is(err, collections.ErrDuplicateDocumentID):
		return iwire.ErrDuplicateDocumentID
	case errors.Is(err, collections.ErrDocumentExists):
		return iwire.ErrDocumentExists
	case errors.Is(err, collections.ErrUniqueIndexConflict):
		return iwire.ErrUniqueIndexConflict
	case errors.Is(err, collections.ErrDurabilityUnavailable):
		return iwire.ErrDurabilityUnavailable
	case errors.Is(err, collections.ErrCommitAmbiguous):
		return iwire.ErrCommitAmbiguous
	case errors.Is(err, raftcluster.ErrCommitAmbiguous):
		return iwire.ErrCommitAmbiguous
	case errors.Is(err, raftcluster.ErrAdmissionUnavailable):
		return iwire.ErrDurabilityUnavailable
	case errors.Is(err, raftcluster.ErrNotLeader):
		return iwire.ErrReadOnly
	case errors.Is(err, collections.ErrRecoveryRequired):
		return iwire.ErrDurabilityUnavailable
	case errors.Is(err, backenddb.ErrCommandWALRejected):
		return iwire.ErrUnsupportedFeature
	case errors.Is(err, backenddb.ErrClosed), errors.Is(err, ErrServerClosed), errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe):
		return iwire.ErrCanceled
	}
	return iwire.ErrInternal
}

func isMalformedProtocolError(err error) bool {
	code, ok := iwire.ErrorCodeOf(err)
	return ok && code == iwire.ErrMalformedFrame
}

func retryableError(code iwire.ErrorCode) bool {
	switch code {
	case iwire.ErrTimeout, iwire.ErrCanceled, iwire.ErrResourceExhausted, iwire.ErrDurabilityUnavailable, iwire.ErrConsistencyUnavailable:
		return true
	default:
		return false
	}
}
