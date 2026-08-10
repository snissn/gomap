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
	metric := s.diagnosticsCommands[name]
	metric.Count++
	metric.LatencyNanos += elapsed.Nanoseconds()
	if failed {
		metric.Errors++
	}
	s.diagnosticsCommands[name] = metric
	if namespace := diagnosticCommandNamespace(name, command); namespace != "" {
		ns := s.diagnosticsNamespaces[namespace]
		ns.Count++
		ns.LatencyNanos += elapsed.Nanoseconds()
		if failed {
			ns.Errors++
		}
		s.diagnosticsNamespaces[namespace] = ns
	}
	s.diagnosticsMu.Unlock()
}

func diagnosticCommandNamespace(name string, command wire.Document) string {
	if name == "top" || name == "serverStatus" || name == "dbStats" || name == "listDatabases" || name == "ping" || name == "hello" || name == "isMaster" || name == "ismaster" {
		return ""
	}
	db, ok := bson.Raw(command).Lookup("$db").StringValueOK()
	if !ok || db == "" {
		return ""
	}
	collection, ok := bson.Raw(command).Lookup(name).StringValueOK()
	if !ok || collection == "" {
		return ""
	}
	return db + "." + collection
}

func (s *Server) diagnosticsSnapshot() (uptime int64, commands map[string]mongoCommandDiagnostic, namespaces map[string]mongoNamespaceDiagnostic) {
	if s == nil {
		return 0, nil, nil
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
	s.diagnosticsMu.Unlock()
	return uptime, commands, namespaces
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
	uptime, commands, _ := s.diagnosticsSnapshot()
	var currentConns int64
	if s != nil {
		s.connMu.Lock()
		currentConns = int64(len(s.conns))
		s.connMu.Unlock()
	}
	commandDoc := make(bson.D, 0, len(commands))
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		metric := commands[name]
		commandDoc = append(commandDoc, bson.E{Key: name, Value: bson.D{{Key: "total", Value: metric.Count}, {Key: "failed", Value: metric.Errors}, {Key: "latencyMicros", Value: metric.LatencyNanos / 1000}}})
	}
	return marshalDocument(bson.D{
		{Key: "host", Value: "treedb-mongo-gateway"},
		{Key: "process", Value: "treedb-mongo-gateway"},
		{Key: "uptime", Value: uptime},
		{Key: "localTime", Value: time.Now().UTC()},
		{Key: "connections", Value: bson.D{{Key: "current", Value: currentConns}}},
		{Key: "cursors", Value: bson.D{{Key: "open", Value: s.cursorCount.Load()}}},
		{Key: "opcounters", Value: commandDoc},
		{Key: "ok", Value: 1.0},
	})
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
	var objects, indexes int64
	for _, meta := range metas {
		count, err := s.diagnosticCollectionCount(meta.Name)
		if err != nil {
			return commandError(commandCodeBadValue, "BadValue", err.Error())
		}
		objects += count
		indexes += int64(1 + len(meta.Indexes) + len(meta.VectorIndexes))
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
	return marshalDocument(bson.D{{Key: "ns", Value: name}, {Key: "count", Value: count}, {Key: "nindexes", Value: int64(1 + len(meta.Indexes) + len(meta.VectorIndexes))}, {Key: "ok", Value: 1.0}})
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
	_, _, namespaces := s.diagnosticsSnapshot()
	totals := make(bson.D, 0, len(namespaces))
	names := make([]string, 0, len(namespaces))
	for namespace := range namespaces {
		names = append(names, namespace)
	}
	sort.Strings(names)
	for _, namespace := range names {
		metric := namespaces[namespace]
		totals = append(totals, bson.E{Key: namespace, Value: bson.D{{Key: "count", Value: metric.Count}, {Key: "errors", Value: metric.Errors}, {Key: "latencyMicros", Value: metric.LatencyNanos / 1000}}})
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
	}
	if _, err := commandString(command, "$db"); err != nil {
		return err
	}
	return nil
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
	col, err := s.openCollectionCached(name)
	if err != nil {
		return 0, err
	}
	var count int64
	truncated, err := col.ScanDocumentIDsFunc(s.maxFindScanDocuments(), func([]byte) (bool, error) { count++; return true, nil })
	if err != nil {
		return 0, err
	}
	if truncated {
		return 0, errors.New("Mongo gateway diagnostics document count exceeds bounded scan limit")
	}
	return count, nil
}
