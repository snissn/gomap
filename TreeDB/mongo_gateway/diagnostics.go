package mongogateway

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Diagnostics deliberately reports only values that this standalone gateway can
// observe authoritatively. In particular, it never derives storage bytes from a
// document scan and it refuses routed reads before opening local metadata.

type mongoCommandDiagnostic struct {
	Count        int64
	Errors       int64
	LatencyNanos int64
}

type mongoNamespaceDiagnostic struct {
	Count        int64
	Errors       int64
	LatencyNanos int64
}

var errDiagnosticLiveDocumentCap = errors.New("Mongo gateway diagnostics live document count exceeds bounded scan limit")

// diagnosticPhysicalWorkBudget bounds iterator/source work separately from the
// public live-document budget. A single primary source needs one positioning
// inspection and, for a complete count, at most one advance per live document.
// Giving diagnostics twice the public document cap leaves that bounded EOF
// proof available while tombstones and shadowed entries still consume the same
// finite physical-work budget.
func diagnosticPhysicalWorkBudget(maxDocuments int) int {
	if maxDocuments <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if maxDocuments > maxInt/2 {
		return maxInt
	}
	return maxDocuments * 2
}

func commandResponseOK(response wire.Document) bool {
	if response == nil {
		return false
	}
	ok, valid := bson.Raw(response).Lookup("ok").DoubleOK()
	return valid && ok == 1
}

func (s *Server) noteDiagnosticCommand(name string, command wire.Document, elapsed time.Duration, failed bool) {
	if s == nil {
		return
	}
	s.diagnosticsMu.Lock()
	if s.diagnosticsCommands == nil {
		s.diagnosticsCommands = make(map[string]mongoCommandDiagnostic)
	}
	if s.diagnosticsNamespaces == nil {
		s.diagnosticsNamespaces = make(map[string]mongoNamespaceDiagnostic)
	}
	if diagnosticCommandAdmitted(name) {
		metric := s.diagnosticsCommands[name]
		metric.Count++
		metric.LatencyNanos += elapsed.Nanoseconds()
		if failed {
			metric.Errors++
		}
		s.diagnosticsCommands[name] = metric
	} else {
		s.diagnosticsDroppedCommands++
	}
	if namespace := diagnosticCommandNamespace(name, command); namespace != "" && !failed {
		if _, exists := s.diagnosticsNamespaces[namespace]; !exists && len(s.diagnosticsNamespaces) >= s.maxFindScanDocuments() {
			s.diagnosticsDroppedNamespaces++
			s.diagnosticsMu.Unlock()
			return
		}
		ns := s.diagnosticsNamespaces[namespace]
		ns.Count++
		ns.LatencyNanos += elapsed.Nanoseconds()
		s.diagnosticsNamespaces[namespace] = ns
	}
	s.diagnosticsMu.Unlock()
}

func diagnosticCommandAdmitted(name string) bool {
	_, admitted := mongoGatewaySupportedCommands[name]
	return admitted
}

func diagnosticCommandNamespace(name string, command wire.Document) string {
	// Only commands whose primary argument is a collection name contribute to
	// namespace totals. Several supported administrative commands use the
	// command-name field for a username or another non-namespace argument.
	if !diagnosticCollectionScopedCommand(name) {
		return ""
	}
	db, ok := bson.Raw(command).Lookup("$db").StringValueOK()
	if !ok || db == "" {
		return ""
	}
	field := name
	if name == "getMore" {
		field = "collection"
	}
	collection, ok := bson.Raw(command).Lookup(field).StringValueOK()
	if !ok || collection == "" {
		return ""
	}
	return db + "." + collection
}

func diagnosticCollectionScopedCommand(name string) bool {
	switch name {
	case "aggregate", "collStats", "count", "create", "createIndexes", "delete", "distinct", "dropIndexes", "find", "findAndModify", "getMore", "insert", "killCursors", "listIndexes", "update":
		return true
	default:
		return false
	}
}

func (s *Server) diagnosticsSnapshot() (uptime int64, commands map[string]mongoCommandDiagnostic, namespaces map[string]mongoNamespaceDiagnostic, droppedCommands, droppedNamespaces int64) {
	if s == nil {
		return 0, nil, nil, 0, 0
	}
	if !s.diagnosticsStartedAt.IsZero() {
		uptime = int64(time.Since(s.diagnosticsStartedAt).Seconds())
	}
	s.diagnosticsMu.Lock()
	commands = make(map[string]mongoCommandDiagnostic, len(s.diagnosticsCommands))
	for name, metric := range s.diagnosticsCommands {
		commands[name] = metric
	}
	namespaces = make(map[string]mongoNamespaceDiagnostic, len(s.diagnosticsNamespaces))
	for name, metric := range s.diagnosticsNamespaces {
		namespaces[name] = metric
	}
	droppedCommands, droppedNamespaces = s.diagnosticsDroppedCommands, s.diagnosticsDroppedNamespaces
	s.diagnosticsMu.Unlock()
	return uptime, commands, namespaces, droppedCommands, droppedNamespaces
}

func (s *Server) serverStatusResponse(command wire.Document) (wire.Document, error) {
	if err := validateDiagnosticsCommand(command, "serverStatus"); err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if doc, err, rejected := s.rejectClusterRoutedLocalMetadataRead("serverStatus"); rejected {
		return doc, err
	}
	uptime, commands, _, droppedCommands, droppedNamespaces := s.diagnosticsSnapshot()
	var currentConns int64
	s.connMu.Lock()
	currentConns = int64(len(s.conns))
	s.connMu.Unlock()
	commandDoc := make(bson.D, 0, len(commands))
	opcounters := bson.D{
		{Key: "insert", Value: int64(0)},
		{Key: "query", Value: int64(0)},
		{Key: "update", Value: int64(0)},
		{Key: "delete", Value: int64(0)},
		{Key: "getmore", Value: int64(0)},
		{Key: "command", Value: int64(0)},
	}
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		metric := commands[name]
		commandDoc = append(commandDoc, bson.E{Key: name, Value: bson.D{{Key: "total", Value: metric.Count}, {Key: "failed", Value: metric.Errors}, {Key: "latencyMicros", Value: metric.LatencyNanos / 1000}}})
		for i := range opcounters {
			if opcounters[i].Key == diagnosticOpCounter(name) {
				opcounters[i].Value = opcounters[i].Value.(int64) + metric.Count
				break
			}
		}
	}
	response := bson.D{
		{Key: "host", Value: "treedb-mongo-gateway"},
		{Key: "process", Value: "treedb-mongo-gateway"},
		{Key: "uptime", Value: uptime},
		{Key: "localTime", Value: time.Now().UTC()},
		{Key: "connections", Value: bson.D{{Key: "current", Value: currentConns}}},
		{Key: "cursors", Value: bson.D{{Key: "open", Value: s.cursorCount.Load()}}},
		{Key: "opcounters", Value: opcounters},
		{Key: "metrics", Value: bson.D{{Key: "treedb", Value: bson.D{{Key: "commands", Value: commandDoc}, {Key: "droppedCommandMetrics", Value: droppedCommands}, {Key: "droppedNamespaceMetrics", Value: droppedNamespaces}}}}},
	}
	if s.diagnosticCommandWALEnabled != nil {
		response = append(response, bson.E{Key: "storage", Value: bson.D{{Key: "treedb", Value: bson.D{{Key: "commandWAL", Value: s.diagnosticCommandWALEnabled()}}}}})
	}
	response = append(response, bson.E{Key: "ok", Value: 1.0})
	return marshalDocument(response)
}

// diagnosticOpCounter is intentionally conservative: only operations whose
// gateway handler has the same operation class as MongoDB's counter are placed
// in the corresponding standard field. Everything else is an administrative
// command and is counted as command.
func diagnosticOpCounter(name string) string {
	switch name {
	case "insert":
		return "insert"
	case "find":
		return "query"
	case "update":
		return "update"
	case "delete":
		return "delete"
	case "getMore":
		return "getmore"
	default:
		return "command"
	}
}

func (s *Server) dbStatsResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	if err := validateDiagnosticsCommand(command, "dbStats"); err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if doc, err, rejected := s.rejectClusterRoutedLocalMetadataRead("dbStats"); rejected {
		return doc, err
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if err := validateMongoDatabaseName(db); err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	metas, err := s.diagnosticMetas(db, cursorOwner)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	remainingDocuments := s.maxFindScanDocuments()
	remainingPhysical := diagnosticPhysicalWorkBudget(remainingDocuments)
	var objects, indexes int64
	for _, meta := range metas {
		count, inspected, truncated, err := s.diagnosticCollectionCountWithin(meta.Name, remainingDocuments, remainingPhysical)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		if truncated {
			return commandError(commandCodeBadValue, "BadValue", "Mongo gateway diagnostics document count exceeds bounded scan limit")
		}
		objects += count
		remainingDocuments -= int(count)
		remainingPhysical -= inspected
		indexes += diagnosticIndexCount(meta)
	}
	return marshalDocument(bson.D{{Key: "db", Value: db}, {Key: "collections", Value: int64(len(metas))}, {Key: "objects", Value: objects}, {Key: "indexes", Value: indexes}, {Key: "ok", Value: 1.0}})
}

func (s *Server) collStatsResponse(command wire.Document, cursorOwner int64) (wire.Document, error) {
	if err := validateDiagnosticsCommand(command, "collStats"); err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if doc, err, rejected := s.rejectClusterRoutedLocalMetadataRead("collStats"); rejected {
		return doc, err
	}
	collection, err := commandString(command, "collStats")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	db, err := commandString(command, "$db")
	if err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	name, err := gatewayCollectionName(db, collection)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	if s.authenticationRequired() && !s.authorizedResource(cursorOwner, db, collection, authorizationMetadataRead) {
		return commandError(13, "Unauthorized", "not authorized")
	}
	col, err := s.openCollectionCached(name)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return commandError(commandCodeNamespaceNotFound, "NamespaceNotFound", "collection not found: "+name)
	}
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	count, err := s.diagnosticCollectionCount(name)
	if err != nil {
		return commandError(commandCodeBadValue, "BadValue", err.Error())
	}
	meta := col.MetaView()
	return marshalDocument(bson.D{{Key: "ns", Value: name}, {Key: "count", Value: count}, {Key: "nindexes", Value: diagnosticIndexCount(meta)}, {Key: "ok", Value: 1.0}})
}

func (s *Server) topResponse(command wire.Document) (wire.Document, error) {
	if err := validateDiagnosticsCommand(command, "top"); err != nil {
		return commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
	}
	if doc, rejected, err := rejectUnsupportedReadConcern(command); rejected {
		return doc, err
	}
	if doc, err, rejected := s.rejectClusterRoutedLocalMetadataRead("top"); rejected {
		return doc, err
	}
	_, _, namespaces, _, _ := s.diagnosticsSnapshot()
	totals := make(bson.D, 0, len(namespaces))
	names := make([]string, 0, len(namespaces))
	for namespace := range namespaces {
		names = append(names, namespace)
	}
	sort.Strings(names)
	for _, namespace := range names {
		metric := namespaces[namespace]
		// Match the Mongo top namespace/event envelope while reporting only the
		// aggregate event category that the gateway can classify truthfully.
		totals = append(totals, bson.E{Key: namespace, Value: bson.D{{Key: "total", Value: bson.D{{Key: "time", Value: metric.LatencyNanos / 1000}, {Key: "count", Value: metric.Count}}}}})
	}
	return marshalDocument(bson.D{{Key: "totals", Value: totals}, {Key: "ok", Value: 1.0}})
}

func validateDiagnosticsCommand(command wire.Document, commandName string) error {
	elements, err := bson.Raw(command).Elements()
	if err != nil {
		return err
	}
	allowed := map[string]struct{}{commandName: {}, "$db": {}, "lsid": {}, "$clusterTime": {}, "comment": {}, "readConcern": {}}
	for _, element := range elements {
		if _, ok := allowed[element.Key()]; !ok {
			return errors.New("unsupported Mongo gateway " + commandName + " option: " + element.Key())
		}
		switch element.Key() {
		case "lsid", "$clusterTime", "readConcern":
			if element.Value().Type != bson.TypeEmbeddedDocument {
				return errors.New("Mongo gateway " + commandName + " option " + element.Key() + " must be a document")
			}
		case "comment":
			if element.Value().Type != bson.TypeString {
				return errors.New("Mongo gateway " + commandName + " option comment must be a string")
			}
		}
	}
	if _, err := commandString(command, "$db"); err != nil {
		return err
	}
	value := bson.Raw(command).Lookup(commandName)
	if value.IsZero() {
		return errors.New("Mongo command missing " + commandName)
	}
	if commandName == "collStats" {
		collection, ok := value.StringValueOK()
		if !ok || collection == "" {
			return errors.New("Mongo command collStats must be a non-empty string")
		}
		return nil
	}
	if !diagnosticNumericOne(value) {
		return errors.New("Mongo command " + commandName + " must be numeric 1")
	}
	return nil
}

func diagnosticNumericOne(value bson.RawValue) bool {
	switch value.Type {
	case bson.TypeInt32:
		n, ok := value.Int32OK()
		return ok && n == 1
	case bson.TypeInt64:
		n, ok := value.Int64OK()
		return ok && n == 1
	case bson.TypeDouble:
		n, ok := value.DoubleOK()
		return ok && n == 1
	default:
		return false
	}
}

func (s *Server) diagnosticMetas(db string, cursorOwner int64) ([]collections.CollectionMeta, error) {
	if s == nil || s.Collections == nil {
		return nil, errors.New("Mongo gateway collection manager is not configured")
	}
	all, truncated, err := s.Collections.ListCollectionsBounded(s.maxFindScanDocuments())
	if err != nil {
		return nil, err
	}
	if truncated {
		return nil, errors.New("Mongo gateway diagnostics collection enumeration exceeds bounded limit")
	}
	prefix := db + "."
	metas := make([]collections.CollectionMeta, 0)
	for _, meta := range all {
		collection, ok := strings.CutPrefix(meta.Name, prefix)
		if !ok || (s.authenticationRequired() && !s.authorizedResource(cursorOwner, db, collection, authorizationMetadataRead)) {
			continue
		}
		metas = append(metas, meta)
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Name < metas[j].Name })
	return metas, nil
}

func (s *Server) diagnosticCollectionCount(name string) (int64, error) {
	maxDocuments := s.maxFindScanDocuments()
	count, _, truncated, err := s.diagnosticCollectionCountWithin(name, maxDocuments, diagnosticPhysicalWorkBudget(maxDocuments))
	if err != nil {
		return 0, err
	}
	if truncated {
		return 0, errors.New("Mongo gateway diagnostics document count exceeds bounded scan limit")
	}
	return count, nil
}

// diagnosticIndexCount reports every authoritative collection index definition
// exposed by CollectionMeta, including the implicit primary index.
func diagnosticIndexCount(meta collections.CollectionMeta) int64 {
	return int64(1 + len(meta.Indexes) + len(meta.VectorIndexes) + len(meta.TextIndexes))
}

func (s *Server) diagnosticCollectionCountWithin(name string, maxDocuments, maxPhysical int) (count int64, inspected int, truncated bool, err error) {
	col, err := s.openCollectionCached(name)
	if err != nil {
		return 0, 0, false, err
	}
	inspected, truncated, err = col.ScanDocumentIDsPhysicalFunc(maxPhysical, func([]byte) (bool, error) {
		if count >= int64(maxDocuments) {
			return false, errDiagnosticLiveDocumentCap
		}
		count++
		return true, nil
	})
	if errors.Is(err, errDiagnosticLiveDocumentCap) {
		return count, inspected, true, nil
	}
	if err != nil {
		return 0, inspected, false, err
	}
	return count, inspected, truncated, nil
}
