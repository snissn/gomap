package mongogateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// MongoGatewayCapabilitySchema and MongoGatewayCapabilityVersion identify the
// canonical standalone capability-manifest format.
const (
	MongoGatewayCapabilitySchema  = "treedb.mongo-gateway.capability-manifest"
	MongoGatewayCapabilityVersion = 1
)

// MongoCapabilityStatus is the executable support classification for one capability.
type MongoCapabilityStatus string

// Supported capability classifications.
const (
	MongoCapabilitySupported       MongoCapabilityStatus = "supported"
	MongoCapabilitySupportedSubset MongoCapabilityStatus = "supported subset"
	MongoCapabilityRejected        MongoCapabilityStatus = "rejected"
	MongoCapabilityNotImplemented  MongoCapabilityStatus = "not implemented"
	MongoCapabilityBenchmarkOnly   MongoCapabilityStatus = "benchmark-only"
)

// MongoGatewayAdvertisedCapabilities contains protocol metadata derived from the manifest.
type MongoGatewayAdvertisedCapabilities struct {
	DeploymentMode               string   `json:"deployment_mode"`
	GitVersion                   string   `json:"git_version"`
	MongoVersion                 string   `json:"mongo_version"`
	MongoVersionArray            [4]int32 `json:"mongo_version_array"`
	MinWireVersion               int32    `json:"min_wire_version"`
	MaxWireVersion               int32    `json:"max_wire_version"`
	LogicalSessionTimeoutMinutes int32    `json:"logical_session_timeout_minutes"`
}

// MongoGatewayCapability describes one uniquely identified executable capability row.
type MongoGatewayCapability struct {
	ID       string                `json:"id"`
	Category string                `json:"category"`
	Feature  string                `json:"feature"`
	Status   MongoCapabilityStatus `json:"status"`
}

// MongoGatewayCapabilitySummary groups capability rows for generated user-facing summaries.
type MongoGatewayCapabilitySummary struct {
	ID            string   `json:"id"`
	Label         string   `json:"label"`
	CapabilityIDs []string `json:"capability_ids"`
	Note          string   `json:"note"`
}

// MongoGatewayCapabilityManifest is the factual source for executable rows,
// advertised metadata, and generated compatibility summaries.
type MongoGatewayCapabilityManifest struct {
	Schema       string                             `json:"schema"`
	Version      int                                `json:"version"`
	Advertised   MongoGatewayAdvertisedCapabilities `json:"advertised"`
	Capabilities []MongoGatewayCapability           `json:"capabilities"`
	Summaries    []MongoGatewayCapabilitySummary    `json:"summaries"`
}

var mongoGatewayCapabilityManifest = MongoGatewayCapabilityManifest{
	Schema:  MongoGatewayCapabilitySchema,
	Version: MongoGatewayCapabilityVersion,
	Advertised: MongoGatewayAdvertisedCapabilities{
		DeploymentMode:               "standalone",
		GitVersion:                   "treedb-mongo-gateway",
		MongoVersion:                 "7.0.0",
		MongoVersionArray:            [4]int32{7, 0, 0, 0},
		MinWireVersion:               0,
		MaxWireVersion:               21,
		LogicalSessionTimeoutMinutes: 30,
	},
	Capabilities: []MongoGatewayCapability{
		{ID: "wire.hello-command", Category: "wire", Feature: "hello command", Status: MongoCapabilitySupported},
		{ID: "wire.ping-command", Category: "wire", Feature: "ping command", Status: MongoCapabilitySupported},
		{ID: "wire.connectionstatus-command", Category: "wire", Feature: "connectionStatus command (#1473)", Status: MongoCapabilitySupportedSubset},
		{ID: "wire.hostinfo-command", Category: "wire", Feature: "hostInfo command (#1473)", Status: MongoCapabilitySupportedSubset},
		{ID: "wire.buildinfo-command", Category: "wire", Feature: "buildInfo command (#1473)", Status: MongoCapabilitySupportedSubset},
		{ID: "crud.insert-explicit-id", Category: "crud", Feature: "insert explicit _id", Status: MongoCapabilitySupported},
		{ID: "crud.find-by-id-equality", Category: "crud", Feature: "find by _id equality", Status: MongoCapabilitySupported},
		{ID: "query.indexed-equality-and-range-predicates", Category: "query", Feature: "indexed equality and range predicates", Status: MongoCapabilitySupportedSubset},
		{ID: "query.in-on-indexed-scalar-fields", Category: "query", Feature: "$in on indexed scalar fields", Status: MongoCapabilitySupportedSubset},
		{ID: "query.top-level-or-expressions", Category: "query", Feature: "top-level $or expressions", Status: MongoCapabilitySupportedSubset},
		{ID: "query.projection-sort-skip-and-limit", Category: "query", Feature: "projection, sort, skip, and limit", Status: MongoCapabilitySupportedSubset},
		{ID: "cursor.getmore-and-killcursors", Category: "cursor", Feature: "getMore and killCursors", Status: MongoCapabilitySupported},
		{ID: "read-concern.local-available-readconcern-maps-to-local-stale", Category: "read concern", Feature: "local/available readConcern maps to local_stale", Status: MongoCapabilitySupportedSubset},
		{ID: "read-concern-gap.majority-linearizable-and-snapshot-readconcern", Category: "read concern gap", Feature: "majority, linearizable, and snapshot readConcern", Status: MongoCapabilityRejected},
		{ID: "write-concern.standalone-w1-and-journal", Category: "write concern", Feature: "standalone absent/default, w:1, and journal acknowledgement (#4060)", Status: MongoCapabilitySupportedSubset},
		{ID: "write-concern-gap.unacknowledged-replica-and-timeout", Category: "write concern gap", Feature: "standalone w:0, replica acknowledgement, and positive wtimeout", Status: MongoCapabilityRejected},
		{ID: "crud.updateone-set-by-id", Category: "crud", Feature: "updateOne $set by _id", Status: MongoCapabilitySupportedSubset},
		{ID: "crud.delete-by-id", Category: "crud", Feature: "delete by _id", Status: MongoCapabilitySupportedSubset},
		{ID: "metadata.listcollections", Category: "metadata", Feature: "listCollections", Status: MongoCapabilitySupportedSubset},
		{ID: "metadata.listdatabases", Category: "metadata", Feature: "listDatabases", Status: MongoCapabilitySupportedSubset},
		{ID: "metadata.create-collection", Category: "metadata", Feature: "create collection", Status: MongoCapabilitySupportedSubset},
		{ID: "session.logical-session-handshake-and-endsessions", Category: "session", Feature: "logical session handshake and endSessions", Status: MongoCapabilitySupportedSubset},
		{ID: "metadata.createindexes-listindexes-and-dropindexes", Category: "metadata", Feature: "createIndexes, listIndexes, and dropIndexes", Status: MongoCapabilitySupportedSubset},
		{ID: "document.native-bson-storage-mode", Category: "document", Feature: "native BSON storage mode", Status: MongoCapabilitySupportedSubset},
		{ID: "query-gap.dotted-projection", Category: "query gap", Feature: "dotted projection", Status: MongoCapabilityRejected},
		{ID: "update-subset.natural-order-arbitrary-filter-update-delete-and-findandmodify", Category: "update subset", Feature: "natural-order arbitrary-filter update, delete, and findAndModify", Status: MongoCapabilitySupportedSubset},
		{ID: "update.exact-id-upsert", Category: "update", Feature: "exact _id upsert", Status: MongoCapabilitySupportedSubset},
		{ID: "update.multi-update-and-batch-ordering", Category: "update", Feature: "bounded multi update and ordered or unordered batch errors", Status: MongoCapabilitySupportedSubset},
		{ID: "update.inc", Category: "update", Feature: "$inc", Status: MongoCapabilitySupportedSubset},
		{ID: "update.unset", Category: "update", Feature: "$unset", Status: MongoCapabilitySupportedSubset},
		{ID: "update.nested-set-unset-inc-and-bounded-array-modifiers-no-numeric-array-index-paths", Category: "update", Feature: "nested $set/$unset/$inc and bounded array modifiers (no numeric array-index paths)", Status: MongoCapabilitySupportedSubset},
		{ID: "update.replaceone-by-exact-id", Category: "update", Feature: "ReplaceOne by exact _id", Status: MongoCapabilitySupportedSubset},
		{ID: "index-gap.compound-index", Category: "index gap", Feature: "compound index", Status: MongoCapabilityRejected},
		{ID: "index.bson-ordered-v2-without-treedbvaluetype", Category: "index", Feature: "BSON v2 index without treedbValueType", Status: MongoCapabilitySupportedSubset},
		{ID: "read-command.aggregate-match-project-sort-skip-limit-count", Category: "read command", Feature: "aggregate match/project/sort/skip/limit/count", Status: MongoCapabilitySupportedSubset},
		{ID: "command-gap.serverstatus", Category: "command gap", Feature: "serverStatus", Status: MongoCapabilityNotImplemented},
		{ID: "command-gap.top", Category: "command gap", Feature: "top", Status: MongoCapabilityNotImplemented},
		{ID: "command-gap.dbstats", Category: "command gap", Feature: "dbStats", Status: MongoCapabilityNotImplemented},
		{ID: "read-command.count-filter-skip-limit", Category: "read command", Feature: "count filter/skip/limit", Status: MongoCapabilitySupportedSubset},
		{ID: "read-command.distinct-top-level-field-with-filter", Category: "read command", Feature: "distinct top-level field with filter", Status: MongoCapabilitySupportedSubset},
		{ID: "read-command-gap.maxtimems-on-aggregate-count-distinct", Category: "read command gap", Feature: "maxTimeMS on aggregate/count/distinct", Status: MongoCapabilityRejected},
		{ID: "update-subset.findandmodify-exact-id-no-match", Category: "update subset", Feature: "findAndModify exact _id no-match", Status: MongoCapabilitySupportedSubset},
		{ID: "transaction-gap.transactions-and-retryable-writes", Category: "transaction gap", Feature: "transactions and retryable writes", Status: MongoCapabilityNotImplemented},
		{ID: "security-gap.authentication-and-authorization", Category: "security gap", Feature: "authentication and authorization", Status: MongoCapabilityNotImplemented},
		{ID: "security.transport-tls-and-safe-remote-listen", Category: "security", Feature: "TLS transport and safe remote listen (#4057)", Status: MongoCapabilitySupportedSubset},
		{ID: "cluster-gap.replica-set-and-sharding-advertisement", Category: "cluster gap", Feature: "replica-set and sharding advertisement", Status: MongoCapabilityNotImplemented},
	},
	Summaries: []MongoGatewayCapabilitySummary{
		{
			ID:    "standalone-crud",
			Label: "Standalone CRUD",
			CapabilityIDs: []string{
				"crud.insert-explicit-id",
				"crud.find-by-id-equality",
				"crud.updateone-set-by-id",
				"crud.delete-by-id",
			},
			Note: "Explicit-ID CRUD plus bounded multi update/delete and ordered or unordered insert/update/delete batches; a positive maxTimeMS shortens the shared five-second command deadline; per-document atomicity only, never a transaction.",
		},
		{
			ID:    "standalone-write-concern",
			Label: "Standalone write concern",
			CapabilityIDs: []string{
				"write-concern.standalone-w1-and-journal",
				"write-concern-gap.unacknowledged-replica-and-timeout",
			},
			Note: "Absent/default and w:1 use the selected profile's ordinary acknowledgement boundary; j:true closes a real command-WAL or checkpoint sync boundary. Unacknowledged, replica, and interruptible-timeout semantics reject before mutation.",
		},
		{
			ID:    "aggregation-count-distinct",
			Label: "Aggregation, count, and distinct",
			CapabilityIDs: []string{
				"read-command.aggregate-match-project-sort-skip-limit-count",
				"read-command.count-filter-skip-limit",
				"read-command.distinct-top-level-field-with-filter",
			},
			Note: "Bounded standalone subsets only; unsupported stages, dotted distinct keys, and maxTimeMS reject.",
		},
		{
			ID:    "administrative-diagnostics",
			Label: "Administrative diagnostics",
			CapabilityIDs: []string{
				"command-gap.serverstatus",
				"command-gap.top",
				"command-gap.dbstats",
			},
			Note: "serverStatus, top, and dbStats are not implemented.",
		},
		{
			ID:            "logical-sessions",
			Label:         "Logical sessions",
			CapabilityIDs: []string{"session.logical-session-handshake-and-endsessions"},
			Note:          "Driver-interoperability metadata only; no transaction or causal-session semantics.",
		},
		{
			ID:            "transport-security",
			Label:         "Transport security",
			CapabilityIDs: []string{"security.transport-tls-and-safe-remote-listen"},
			Note:          "Loopback plaintext remains available; non-loopback standalone listeners require TLS unless an explicit insecure override is selected. TLS 1.2+ with bounded handshakes is supported; authentication and authorization remain separate gaps.",
		},
		{
			ID:    "scalar-indexes",
			Label: "Scalar indexes",
			CapabilityIDs: []string{
				"metadata.createindexes-listindexes-and-dropindexes",
				"index-gap.compound-index",
				"index.bson-ordered-v2-without-treedbvaluetype",
			},
			Note: "BSON collections default ordinary single-field ascending indexes to BSON-ordered v2; explicit treedbValueType remains the legacy homogeneous path. Compound and descending indexes remain rejected.",
		},
		{
			ID:            "authentication-authorization",
			Label:         "Authentication and authorization",
			CapabilityIDs: []string{"security-gap.authentication-and-authorization"},
			Note:          "The current standalone gateway assumes a trusted local deployment.",
		},
		{
			ID:            "transactions-retryable-writes",
			Label:         "Transactions and retryable writes",
			CapabilityIDs: []string{"transaction-gap.transactions-and-retryable-writes"},
			Note:          "Transaction markers reject and commitTransaction is unavailable.",
		},
		{
			ID:            "replica-set-sharding",
			Label:         "Replica set and sharding",
			CapabilityIDs: []string{"cluster-gap.replica-set-and-sharding-advertisement"},
			Note:          "Standalone hello metadata does not advertise replica-set or sharded-server behavior.",
		},
	},
}

var mongoGatewayCapabilityIdentity = sync.OnceValue(func() string {
	return mongoGatewayCapabilityIdentityForManifest(mongoGatewayCapabilityManifest)
})

// MongoGatewayCapabilities returns a deep copy of the canonical capability manifest.
func MongoGatewayCapabilities() MongoGatewayCapabilityManifest {
	manifest := mongoGatewayCapabilityManifest
	manifest.Capabilities = append([]MongoGatewayCapability(nil), manifest.Capabilities...)
	manifest.Summaries = make([]MongoGatewayCapabilitySummary, len(mongoGatewayCapabilityManifest.Summaries))
	for i, summary := range mongoGatewayCapabilityManifest.Summaries {
		manifest.Summaries[i] = summary
		manifest.Summaries[i].CapabilityIDs = append([]string(nil), summary.CapabilityIDs...)
	}
	return manifest
}

// ValidateMongoGatewayCapabilityManifest rejects incomplete, duplicate, or
// internally inconsistent capability metadata.
func ValidateMongoGatewayCapabilityManifest(manifest MongoGatewayCapabilityManifest) error {
	if manifest.Schema != MongoGatewayCapabilitySchema {
		return fmt.Errorf("capability schema %q does not match %q", manifest.Schema, MongoGatewayCapabilitySchema)
	}
	if manifest.Version != MongoGatewayCapabilityVersion {
		return fmt.Errorf("capability version %d does not match %d", manifest.Version, MongoGatewayCapabilityVersion)
	}
	if manifest.Advertised.DeploymentMode != "standalone" {
		return fmt.Errorf("advertised deployment mode %q is not standalone", manifest.Advertised.DeploymentMode)
	}
	if manifest.Advertised.GitVersion == "" || manifest.Advertised.MongoVersion == "" || manifest.Advertised.MaxWireVersion < manifest.Advertised.MinWireVersion {
		return errors.New("advertised Mongo version and wire range must be valid")
	}
	if manifest.Advertised.LogicalSessionTimeoutMinutes <= 0 {
		return errors.New("advertised logical session timeout must be positive")
	}

	if len(manifest.Capabilities) == 0 {
		return errors.New("capability manifest must contain at least one capability")
	}

	capabilities := make(map[string]MongoGatewayCapability, len(manifest.Capabilities))
	for _, capability := range manifest.Capabilities {
		if capability.ID == "" || capability.Category == "" || capability.Feature == "" {
			return fmt.Errorf("incomplete capability: %+v", capability)
		}
		if !validMongoCapabilityStatus(capability.Status) {
			return fmt.Errorf("capability %q has invalid status %q", capability.ID, capability.Status)
		}
		if _, exists := capabilities[capability.ID]; exists {
			return fmt.Errorf("duplicate capability id %q", capability.ID)
		}
		capabilities[capability.ID] = capability
	}

	summaryIDs := make(map[string]struct{}, len(manifest.Summaries))
	for _, summary := range manifest.Summaries {
		if summary.ID == "" || summary.Label == "" || len(summary.CapabilityIDs) == 0 {
			return fmt.Errorf("incomplete capability summary: %+v", summary)
		}
		if _, exists := summaryIDs[summary.ID]; exists {
			return fmt.Errorf("duplicate capability summary id %q", summary.ID)
		}
		summaryIDs[summary.ID] = struct{}{}
		seen := make(map[string]struct{}, len(summary.CapabilityIDs))
		for _, capabilityID := range summary.CapabilityIDs {
			if _, exists := seen[capabilityID]; exists {
				return fmt.Errorf("capability summary %q repeats capability %q", summary.ID, capabilityID)
			}
			seen[capabilityID] = struct{}{}
			if _, exists := capabilities[capabilityID]; !exists {
				return fmt.Errorf("capability summary %q references unknown capability %q", summary.ID, capabilityID)
			}
		}
	}
	return nil
}

// MongoGatewayCapabilityIdentity returns the deterministic identity of the
// canonical manifest.
func MongoGatewayCapabilityIdentity() string {
	return mongoGatewayCapabilityIdentity()
}

func mongoGatewayCapabilityIdentityForManifest(manifest MongoGatewayCapabilityManifest) string {
	payload, err := json.Marshal(manifest)
	if err != nil {
		panic(fmt.Sprintf("marshal Mongo gateway capability manifest: %v", err))
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("%s/v%d/sha256:%s", manifest.Schema, manifest.Version, hex.EncodeToString(digest[:]))
}

func mongoGatewayCapabilityIndex(manifest MongoGatewayCapabilityManifest) map[string]MongoGatewayCapability {
	index := make(map[string]MongoGatewayCapability, len(manifest.Capabilities))
	for _, capability := range manifest.Capabilities {
		index[capability.ID] = capability
	}
	return index
}

func mongoGatewayCapabilitySummaryStatus(manifest MongoGatewayCapabilityManifest, summary MongoGatewayCapabilitySummary) (MongoCapabilityStatus, error) {
	index := mongoGatewayCapabilityIndex(manifest)
	allSupported := true
	hasSupported := false
	hasNotImplemented := false
	for _, capabilityID := range summary.CapabilityIDs {
		capability, ok := index[capabilityID]
		if !ok {
			return "", fmt.Errorf("summary %q references unknown capability %q", summary.ID, capabilityID)
		}
		switch capability.Status {
		case MongoCapabilitySupported:
			hasSupported = true
		case MongoCapabilitySupportedSubset:
			hasSupported = true
			allSupported = false
		case MongoCapabilityNotImplemented:
			hasNotImplemented = true
			allSupported = false
		default:
			allSupported = false
		}
	}
	if hasSupported {
		if allSupported {
			return MongoCapabilitySupported, nil
		}
		return MongoCapabilitySupportedSubset, nil
	}
	if hasNotImplemented {
		return MongoCapabilityNotImplemented, nil
	}
	return MongoCapabilityRejected, nil
}

func validMongoCapabilityStatus(status MongoCapabilityStatus) bool {
	switch status {
	case MongoCapabilitySupported, MongoCapabilitySupportedSubset, MongoCapabilityRejected, MongoCapabilityNotImplemented, MongoCapabilityBenchmarkOnly:
		return true
	default:
		return false
	}
}
