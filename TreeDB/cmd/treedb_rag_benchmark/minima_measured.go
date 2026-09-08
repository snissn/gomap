package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

const minimaMeasuredSchema = "treedb_rag_application/minima_measured_v1"

// These are historical, trace-derived qualification limits, not freeze options.
const (
	minimaMeasuredLoadCap    = int64(1_200_000_000_000)
	minimaMeasuredRestartCap = int64(20_000_000_000)
	minimaMeasuredRSSCap     = int64(9_126_805_504)
	minimaMeasuredSearchCap  = int64(2_127_956)
	minimaMeasuredDiskCap    = int64(2_967_728_546)
)

type minimaMeasuredInterval struct {
	StartedMonotonicNS int64 `json:"started_monotonic_ns"`
	EndedMonotonicNS   int64 `json:"ended_monotonic_ns"`
}

type minimaMeasuredRequest struct {
	BatchStart         *uint64                          `json:"batch_start,omitempty"`
	RequestedIDs       []string                         `json:"requested_ids,omitempty"`
	ResultIDs          []string                         `json:"result_ids,omitempty"`
	OperationName      string                           `json:"operation_name"`
	RequestSequence    uint64                           `json:"request_sequence"`
	Operation          string                           `json:"operation"`
	Scenario           string                           `json:"scenario"`
	Phase              string                           `json:"phase"`
	LifetimeOrdinal    int                              `json:"lifetime_ordinal"`
	StartedMonotonicNS int64                            `json:"started_monotonic_ns"`
	EndedMonotonicNS   int64                            `json:"ended_monotonic_ns"`
	Outcome            string                           `json:"outcome"`
	Error              string                           `json:"error,omitempty"`
	Transport          string                           `json:"transport"`
	CommandVersion     *uint64                          `json:"command_version,omitempty"`
	ExpectedGeneration *uint64                          `json:"expected_generation,omitempty"`
	ResultCount        *uint64                          `json:"result_count,omitempty"`
	RequestedCount     *uint64                          `json:"requested_count,omitempty"`
	MissingCount       *uint64                          `json:"missing_count,omitempty"`
	Projection         string                           `json:"projection,omitempty"`
	DenseWork          *documentservice.DenseSearchWork `json:"dense_work,omitempty"`
}

type minimaWorkObservation struct {
	Availability       string              `json:"availability"`
	Reason             string              `json:"reason,omitempty"`
	StartedMonotonicNS int64               `json:"started_monotonic_ns"`
	EndedMonotonicNS   int64               `json:"ended_monotonic_ns"`
	Work               *workstats.Snapshot `json:"work,omitempty"`
}
type minimaTerminalWork struct {
	Event            string             `json:"event"`
	Version          uint64             `json:"version"`
	ContractVersion  string             `json:"contract_version"`
	CleanupCompleted bool               `json:"cleanup_completed"`
	ShutdownFailures uint64             `json:"shutdown_failures"`
	Work             workstats.Snapshot `json:"work"`
}
type minimaOwnedExit struct {
	Availability         string `json:"availability"`
	Reason               string `json:"reason,omitempty"`
	PID                  int    `json:"pid"`
	LinuxProcessIdentity string `json:"linux_process_identity"`
	ObservedMonotonicNS  int64  `json:"observed_monotonic_ns"`
	ExitCode             *int   `json:"exit_code,omitempty"`
	PeakRSSBytes         *int64 `json:"peak_rss_bytes,omitempty"`
	Source               string `json:"source"`
	Scope                string `json:"scope"`
}
type minimaProcessLifetime struct {
	Ordinal              int                   `json:"ordinal"`
	PID                  int                   `json:"pid"`
	LinuxProcessIdentity string                `json:"linux_process_identity"`
	FirstWork            minimaWorkObservation `json:"first_work"`
	LastLiveWork         minimaWorkObservation `json:"last_live_work"`
	TerminalWork         *minimaTerminalWork   `json:"terminal_work,omitempty"`
	TerminalError        string                `json:"terminal_error,omitempty"`
	Exit                 minimaOwnedExit       `json:"exit"`
}

type minimaMeasuredFreezeManifest struct {
	Schema              string `json:"schema"`
	Fixture             string `json:"fixture"`
	InputSHA256         string `json:"input_sha256"`
	CorpusSHA256        string `json:"corpus_sha256"`
	QuerySHA256         string `json:"query_sha256"`
	OperationSHA256     string `json:"operation_sha256"`
	ExpectedStateSHA256 string `json:"expected_state_sha256"`
}
type minimaMeasuredFreeze struct {
	Version                   uint64                       `json:"version"`
	HarnessCommit             string                       `json:"harness_commit"`
	ReviewedProductCommit     string                       `json:"reviewed_product_commit"`
	Manifest                  minimaMeasuredFreezeManifest `json:"manifest"`
	Configuration             map[string]map[string]string `json:"configuration"`
	CalibrationStatus         string                       `json:"calibration_status"`
	BoundedPairSHA256         []string                     `json:"bounded_pair_sha256"`
	OverheadDispositionSHA256 string                       `json:"overhead_disposition_sha256"`
	// SHA256 is assigned only after checking the externally supplied digest.
	SHA256 string `json:"-"`
}

var minimaMeasuredCommonConfigKeys = []string{
	"harness_commit", "reviewed_product_commit", "manifest_file_sha256", "runner_sha256",
	"shared_runner_sha256", "client_sha256", "product_source_sha256", "harness_source_sha256",
	"comparator_binary_sha256", "collection", "dimension", "metric", "scalar_fields", "top_k", "batch_size",
	"operation_timeout_seconds", "reader_concurrency", "writer_concurrency", "gomaxprocs", "cpu_affinity",
	"measurement_mode",
}
var minimaMeasuredTreeConfigKeys = []string{
	"product_commit", "service_binary_sha256", "service_binary_vcs_revision", "service_binary_vcs_modified",
	"vector_strategy", "transport", "control_transport", "ef_search", "column_graph_serving", "profile",
	"startup_reopen_timeout_seconds", "shutdown_timeout_seconds", "block_profile_rate", "mutex_profile_fraction",
}
var minimaMeasuredQdrantConfigKeys = []string{
	"server_version", "client_version", "optimizer_timeout_seconds", "write_wait", "point_id_mapping", "deployment", "image",
	"initial_upload_hnsw", "initial_upload_optimizers", "production_hnsw", "production_optimizers",
}

// Only the measured seam uses strict presence/type decoding. The producer field
// set comes from the real versioned structs, including embedded operation totals.
// Legacy artifact decoding intentionally retains its historical behavior.
func minimaMeasuredJSON(raw []byte, typ reflect.Type, path string) error {
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || len(raw) == 0 {
		return fmt.Errorf("%s: missing/null", path)
	}
	switch typ.Kind() {
	case reflect.Struct:
		dec := json.NewDecoder(bytes.NewReader(raw))
		token, err := dec.Token()
		if err != nil || token != json.Delim('{') {
			return fmt.Errorf("%s: expected object", path)
		}
		fields := make(map[string]reflect.StructField)
		var add func(reflect.Type)
		add = func(t reflect.Type) {
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				tag := strings.Split(f.Tag.Get("json"), ",")[0]
				if tag == "-" {
					continue
				}
				if f.Anonymous && tag == "" {
					add(f.Type)
					continue
				}
				if tag == "" {
					tag = f.Name
				}
				fields[tag] = f
			}
		}
		add(typ)
		seen := make(map[string]bool)
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || seen[key] {
				return fmt.Errorf("%s: duplicate/invalid field %v", path, keyToken)
			}
			seen[key] = true
			f, ok := fields[key]
			if !ok {
				return fmt.Errorf("%s: unknown field %s", path, key)
			}
			var value json.RawMessage
			if err := dec.Decode(&value); err != nil {
				return err
			}
			if err := minimaMeasuredJSON(value, f.Type, path+"."+key); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
		if _, err := dec.Token(); err != io.EOF {
			return fmt.Errorf("%s: trailing JSON", path)
		}
		for key, f := range fields {
			if !seen[key] && !strings.Contains(f.Tag.Get("json"), ",omitempty") {
				return fmt.Errorf("%s.%s: missing", path, key)
			}
		}
	case reflect.Slice, reflect.Array:
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		for i, value := range values {
			if err := minimaMeasuredJSON(value, typ.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		values, err := minimaMeasuredObject(raw)
		if err != nil {
			return err
		}
		for key, value := range values {
			if err := minimaMeasuredJSON(value, typ.Elem(), path+"."+key); err != nil {
				return err
			}
		}
	default:
		value := reflect.New(typ).Interface()
		if err := json.Unmarshal(raw, value); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

func minimaMeasuredObject(raw []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("measured field requires an object")
	}
	values := map[string]json.RawMessage{}
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if _, duplicate := values[key]; !ok || duplicate {
			return nil, fmt.Errorf("duplicate/invalid measured object key %v", token)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		values[key] = value
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("trailing measured JSON")
	}
	return values, nil
}

func loadMinimaMeasuredFreeze(path, expected string) (*minimaMeasuredFreeze, error) {
	if path == "" && expected == "" {
		return nil, nil
	}
	if path == "" || !minimaExactHex(expected, sha256.Size) {
		return nil, errors.New("measured freeze requires path and external SHA256 pin")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, errors.New("measured freeze exceeds 1 MiB")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != expected {
		return nil, errors.New("measured freeze bytes do not match external pin")
	}
	if err := minimaMeasuredJSON(raw, reflect.TypeFor[minimaMeasuredFreeze](), "freeze"); err != nil {
		return nil, err
	}
	var freeze minimaMeasuredFreeze
	if err := json.Unmarshal(raw, &freeze); err != nil {
		return nil, err
	}
	freeze.SHA256 = expected
	if err := validateMinimaMeasuredFreeze(&freeze); err != nil {
		return nil, err
	}
	// The validator binary is itself part of the fixed experiment. Archived
	// validation needs no repository or network, only its own natural build info.
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil, errors.New("comparator build info unavailable")
	}
	revision, modified := "", ""
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	binary, err := os.Open(executable)
	if err != nil {
		return nil, err
	}
	defer binary.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, binary); err != nil {
		return nil, err
	}
	if revision != freeze.HarnessCommit || modified != "false" {
		return nil, errors.New("comparator natural revision does not match freeze")
	}
	for _, name := range []string{"treedb", "qdrant"} {
		if freeze.Configuration[name]["comparator_binary_sha256"] != hex.EncodeToString(hash.Sum(nil)) {
			return nil, errors.New("comparator binary hash does not match freeze")
		}
	}
	return &freeze, nil
}

func validateMinimaMeasuredFreeze(f *minimaMeasuredFreeze) error {
	if f == nil || f.Version != 1 || !minimaExactHex(f.SHA256, 32) || !minimaExactHex(f.HarnessCommit, 20) || !minimaExactHex(f.ReviewedProductCommit, 20) {
		return errors.New("measured freeze identity unavailable")
	}
	if f.Manifest.Schema != minimaManifestSchema && f.Manifest.Schema != minimaBoundedManifestSchema {
		return errors.New("measured freeze manifest schema invalid")
	}
	if (f.Manifest.Schema == minimaBoundedManifestSchema && f.Manifest.Fixture == "") || !minimaExactHex(f.Manifest.InputSHA256, 32) {
		return errors.New("measured freeze input identity missing")
	}
	if f.CalibrationStatus != "reviewed" && (f.CalibrationStatus != "pending" || f.Manifest.Schema != minimaBoundedManifestSchema) {
		return errors.New("full measured freeze requires reviewed calibration")
	}
	if f.CalibrationStatus == "reviewed" {
		if len(f.BoundedPairSHA256) != 3 || !minimaExactHex(f.OverheadDispositionSHA256, 32) {
			return errors.New("reviewed freeze requires three bounded pairs and overhead disposition")
		}
		seen := map[string]bool{}
		for _, hash := range f.BoundedPairSHA256 {
			if !minimaExactHex(hash, 32) || seen[hash] {
				return errors.New("invalid/duplicate calibration hash")
			}
			seen[hash] = true
		}
	} else if len(f.BoundedPairSHA256) != 0 || f.OverheadDispositionSHA256 != "" {
		return errors.New("pending freeze cannot claim reviewed results")
	}
	if len(f.Configuration) != 2 {
		return errors.New("freeze requires both backend configurations")
	}
	for _, name := range []string{"treedb", "qdrant"} {
		keys := append([]string(nil), minimaMeasuredCommonConfigKeys...)
		if name == "treedb" {
			keys = append(keys, minimaMeasuredTreeConfigKeys...)
		} else {
			keys = append(keys, minimaMeasuredQdrantConfigKeys...)
		}
		config := f.Configuration[name]
		if len(config) != len(keys) {
			return fmt.Errorf("freeze %s configuration has missing/unknown keys", name)
		}
		for _, key := range keys {
			value := config[key]
			if value == "" {
				return fmt.Errorf("freeze %s configuration missing %s", name, key)
			}
			if strings.HasSuffix(key, "sha256") && !minimaExactHex(value, 32) {
				return fmt.Errorf("freeze %s hash invalid: %s", name, key)
			}
		}
		if config["harness_commit"] != f.HarnessCommit || config["reviewed_product_commit"] != f.ReviewedProductCommit || config["manifest_file_sha256"] != f.Manifest.InputSHA256 || config["measurement_mode"] != "measured" {
			return errors.New("freeze source/manifest/mode identity mismatch")
		}
	}
	tree := f.Configuration["treedb"]
	if tree["vector_strategy"] != "column_graph" || (tree["transport"] != "native" && tree["transport"] != "http") || tree["control_transport"] != "http" || tree["block_profile_rate"] != "0" || tree["mutex_profile_fraction"] != "0" {
		return errors.New("measured TreeDB requires selected typed route and zero profile sampling rates")
	}
	qdrant := f.Configuration["qdrant"]
	image, digest, pinned := strings.Cut(qdrant["image"], "@sha256:")
	if qdrant["deployment"] != "docker" || !pinned || image == "" || strings.Contains(image, "@") || !minimaExactHex(digest, 32) {
		return errors.New("measured Qdrant requires owned Docker and a digest-pinned image")
	}
	return nil
}

func validateMinimaMeasuredBinding(a *minimaArtifact, f *minimaMeasuredFreeze) error {
	if err := validateMinimaMeasuredFreeze(f); err != nil {
		return err
	}
	m := a.Manifest
	if a.FreezeSHA256 != f.SHA256 || m.Schema != f.Manifest.Schema || m.Fixture != f.Manifest.Fixture || m.CorpusSHA256 != f.Manifest.CorpusSHA256 || m.QuerySHA256 != f.Manifest.QuerySHA256 || m.OperationSHA256 != f.Manifest.OperationSHA256 || m.ExpectedStateSHA256 != f.Manifest.ExpectedStateSHA256 {
		return errors.New("measured artifact does not match trusted freeze/manifest")
	}
	for _, backend := range a.Backends {
		config, ok := f.Configuration[backend.Name]
		if !ok {
			return errors.New("unknown measured backend")
		}
		for key, want := range config {
			if backend.Configuration[key] != want {
				return fmt.Errorf("measured %s observed configuration differs: %s", backend.Name, key)
			}
		}
	}
	return nil
}

// This measured-only token walk protects intermediate phase/trace carriers too.
func minimaMeasuredNoDuplicateKeys(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func() error
	value = func() error {
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate measured JSON key")
				}
				seen[name] = true
				if err := value(); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := value(); err != nil {
					return err
				}
			}
		default:
			return errors.New("invalid measured JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing measured JSON")
	}
	return nil
}

func minimaMeasuredPresence(raw []byte) error {
	if err := minimaMeasuredNoDuplicateKeys(raw); err != nil {
		return err
	}
	top, err := minimaMeasuredObject(raw)
	if err != nil {
		return err
	}
	if len(top["freeze_sha256"]) == 0 {
		return errors.New("measured freeze_sha256 missing")
	}
	if _, ok := top["native_path_proof"]; ok {
		return errors.New("historical native_path_proof is forbidden under measured schema")
	}
	backends, err := minimaMeasuredObject(top["backend_raw_evidence"])
	if err != nil {
		return err
	}
	for name, rawBackend := range backends {
		fields, err := minimaMeasuredObject(rawBackend)
		if err != nil {
			return err
		}
		if err := minimaMeasuredJSON(fields["setup_interval"], reflect.TypeFor[minimaMeasuredInterval](), name+".setup_interval"); err != nil {
			return err
		}
		if err := minimaMeasuredJSON(fields["request_evidence"], reflect.TypeFor[[]minimaMeasuredRequest](), name+".request_evidence"); err != nil {
			return err
		}
		if name == "treedb" {
			if err := minimaMeasuredJSON(fields["process_lifetimes"], reflect.TypeFor[[]minimaProcessLifetime](), name+".process_lifetimes"); err != nil {
				return err
			}
		}
		var phases struct {
			Phases []struct {
				ResourceSegments []struct{ Start, End json.RawMessage } `json:"resource_segments"`
			} `json:"phases"`
		}
		if err := json.Unmarshal(fields["phase_attribution"], &phases); err != nil {
			return err
		}
		for _, phase := range phases.Phases {
			for _, segment := range phase.ResourceSegments {
				for _, endpoint := range []json.RawMessage{segment.Start, segment.End} {
					var values map[string]json.RawMessage
					if err := json.Unmarshal(endpoint, &values); err != nil {
						return err
					}
					if err := minimaMeasuredJSON(values["lifetime_ordinal"], reflect.TypeFor[int](), "lifetime_ordinal"); err != nil {
						return err
					}
					if name == "treedb" {
						if err := minimaMeasuredJSON(values["work_snapshot"], reflect.TypeFor[minimaWorkObservation](), "work_snapshot"); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	// These joins are outside producer objects but must not become implicit zero.
	var rows []struct {
		Operations struct {
			Timed struct {
				Queries []json.RawMessage `json:"queries"`
			} `json:"timed_execution_trace"`
			Reindex struct {
				Operations []struct {
					Queries []json.RawMessage `json:"reader_queries"`
				} `json:"operations"`
			} `json:"reindex_execution_trace"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(top["backends"], &rows); err != nil {
		return err
	}
	for _, row := range rows {
		queries := row.Operations.Timed.Queries
		for _, op := range row.Operations.Reindex.Operations {
			queries = append(queries, op.Queries...)
		}
		for _, query := range queries {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(query, &fields); err != nil {
				return err
			}
			if err := minimaMeasuredJSON(fields["request_sequence"], reflect.TypeFor[uint64](), "query.request_sequence"); err != nil {
				return err
			}
		}
	}
	return nil
}

func minimaWorkMonotonic(before, after workstats.Snapshot) bool {
	// All fields in these producer groups are cumulative. Cache residency and
	// current heap gauges are deliberately excluded (including at owner close).
	groupsBefore := []any{before.Graph, before.Output, before.Fold, before.IndexedJSON, before.Typed, before.Runtime, before.Replay, before.Scans}
	groupsAfter := []any{after.Graph, after.Output, after.Fold, after.IndexedJSON, after.Typed, after.Runtime, after.Replay, after.Scans}
	var monotonic func(reflect.Value, reflect.Value) bool
	monotonic = func(a, b reflect.Value) bool {
		if a.Kind() == reflect.Struct {
			for i := 0; i < a.NumField(); i++ {
				if !monotonic(a.Field(i), b.Field(i)) {
					return false
				}
			}
			return true
		}
		return b.Uint() >= a.Uint()
	}
	for i := range groupsBefore {
		if !monotonic(reflect.ValueOf(groupsBefore[i]), reflect.ValueOf(groupsAfter[i])) {
			return false
		}
	}
	a, b := before.RowIndexCache, after.RowIndexCache
	return after.Memory.TotalAlloc >= before.Memory.TotalAlloc && after.Memory.Mallocs >= before.Memory.Mallocs && after.Memory.NumGC >= before.Memory.NumGC &&
		b.Hits >= a.Hits && b.Misses >= a.Misses && b.Builds >= a.Builds && b.RowsVisited >= a.RowsVisited && b.Evictions >= a.Evictions && b.OversizedBypasses >= a.OversizedBypasses
}
func validateMinimaWork(w workstats.Snapshot, pid int, origin int64) error {
	if w.SchemaVersion != "treedb-work-v1" || w.Scope != "process" || w.OriginKind != "go_package_init" || w.PID != pid || w.OriginUnixNano <= 0 || (origin != 0 && w.OriginUnixNano != origin) || w.SnapshotUnixNano < w.OriginUnixNano {
		return errors.New("work process/package origin mismatch")
	}
	availability := reflect.ValueOf(w.Available)
	for i := 0; i < availability.NumField(); i++ {
		if !availability.Field(i).Bool() {
			return errors.New("work producer coverage unavailable")
		}
	}
	if w.IndexedJSON != (workstats.IndexedJSONStats{}) || w.Runtime != (workstats.RuntimeStats{}) || w.Replay.LegacyCollectionFrames != 0 || w.Replay.LegacyProjectionRowsDecoded != 0 {
		return errors.New("forbidden indexed JSON/runtime/legacy replay work")
	}
	for _, scan := range []workstats.ScanStats{w.Scans.DenseExact, w.Scans.FilteredCount, w.Scans.FilteredRetrieval, w.Scans.MutationMatch, w.Scans.CollectionExact, w.Scans.CollectionIndexedExact} {
		if scan != (workstats.ScanStats{}) {
			return errors.New("forbidden document scan work")
		}
	}
	for _, op := range []workstats.OperationStats{w.Graph.Requests, w.Graph.Filters, w.Output.Materialization.OperationStats, w.Output.Search.OperationStats, w.Output.GetMany.OperationStats, w.Fold.Build, w.Fold.Public, w.Fold.Renew} {
		if op.Completed > op.Attempts || op.Errors != op.Attempts-op.Completed {
			return errors.New("undrained operation work")
		}
	}
	if w.Replay.FramesApplied > w.Replay.FramesAttempted || w.Replay.FrameErrors != w.Replay.FramesAttempted-w.Replay.FramesApplied {
		return errors.New("undrained replay work")
	}
	return nil
}
func validateMinimaWorkObservation(o minimaWorkObservation, l minimaProcessLifetime, origin int64) error {
	if o.Availability != "measured" || o.Reason != "" || o.Work == nil || !minimaValidInterval(o.StartedMonotonicNS, o.EndedMonotonicNS) {
		return errors.New("work observation unavailable/invalid")
	}
	return validateMinimaWork(*o.Work, l.PID, origin)
}
func validateMinimaMeasuredLifetimes(raw minimaRawBackendEvidence, native bool) error {
	if len(raw.ProcessLifetimes) != 2 || raw.PhaseAttribution == nil {
		return errors.New("measured TreeDB requires two complete owned lifetimes")
	}
	for i, p := range raw.PhaseAttribution.Phases {
		for j, segment := range p.ResourceSegments {
			want := 0
			if i > minimaTreeDBRestartOrdinal || i == minimaTreeDBRestartOrdinal && j == 1 {
				want = 1
			}
			for _, endpoint := range []minimaRawPhaseResourceEndpoint{segment.Start, segment.End} {
				if endpoint.LifetimeOrdinal == nil || *endpoint.LifetimeOrdinal != want {
					return errors.New("phase endpoint lifetime ordinal invalid")
				}
			}
		}
	}
	var previousExit int64
	var empty, exact, hnsw, base, delta, scalar, text, getMany, fold bool
	for ordinal, l := range raw.ProcessLifetimes {
		if l.Ordinal != ordinal || l.PID <= 0 || l.LinuxProcessIdentity == "" || l.FirstWork.StartedMonotonicNS <= previousExit {
			return errors.New("invalid owned lifetime identity/order")
		}
		if err := validateMinimaWorkObservation(l.FirstWork, l, 0); err != nil {
			return err
		}
		origin := l.FirstWork.Work.OriginUnixNano
		if err := validateMinimaWorkObservation(l.LastLiveWork, l, origin); err != nil {
			return err
		}
		if l.LastLiveWork.StartedMonotonicNS < l.FirstWork.EndedMonotonicNS || !minimaWorkMonotonic(*l.FirstWork.Work, *l.LastLiveWork.Work) {
			return errors.New("lifetime work reset")
		}
		terminal := l.TerminalWork
		if terminal == nil || l.TerminalError != "" || terminal.Event != "treedb_document_service_terminal_work" || terminal.Version != 1 || terminal.ContractVersion != documentservice.ContractVersion || !terminal.CleanupCompleted || terminal.ShutdownFailures != 0 {
			return errors.New("terminal work missing/failed")
		}
		if err := validateMinimaWork(terminal.Work, l.PID, origin); err != nil {
			return err
		}
		if terminal.Work.SnapshotUnixNano < l.LastLiveWork.Work.SnapshotUnixNano || !minimaWorkMonotonic(*l.LastLiveWork.Work, terminal.Work) {
			return errors.New("terminal work precedes drained live work")
		}
		exit := l.Exit
		if exit.Availability != "measured" || exit.Reason != "" || exit.PID != l.PID || exit.LinuxProcessIdentity != l.LinuxProcessIdentity || exit.ObservedMonotonicNS <= l.LastLiveWork.EndedMonotonicNS || exit.ExitCode == nil || *exit.ExitCode != 0 || exit.PeakRSSBytes == nil || *exit.PeakRSSBytes <= 0 || exit.Source != "linux_wait4_ru_maxrss_kib_times_1024" || exit.Scope != "owned_process_start_through_exit" {
			return errors.New("through-exit owned wait4 evidence missing/failed")
		}
		previousExit = exit.ObservedMonotonicNS
		// Require the observed raw authority identity as well as the legacy command identity.
		pid, identity := raw.RestartBoundary.OldPID, raw.RestartBoundary.OldLinuxProcessIdentity
		if ordinal == 1 {
			pid, identity = raw.RestartBoundary.NewPID, raw.RestartBoundary.NewLinuxProcessIdentity
		}
		if l.PID != pid || l.LinuxProcessIdentity != identity {
			return errors.New("restart/lifetime identity mismatch")
		}
		previous := *l.FirstWork.Work
		for _, phase := range raw.PhaseAttribution.Phases {
			for _, segment := range phase.ResourceSegments {
				for _, endpoint := range []minimaRawPhaseResourceEndpoint{segment.Start, segment.End} {
					if endpoint.LifetimeOrdinal == nil || *endpoint.LifetimeOrdinal != ordinal {
						continue
					}
					if endpoint.PID != l.PID || endpoint.LinuxProcessIdentity != l.LinuxProcessIdentity || endpoint.WorkSnapshot == nil {
						return errors.New("phase work identity missing")
					}
					observation := *endpoint.WorkSnapshot
					if err := validateMinimaWorkObservation(observation, l, origin); err != nil {
						return err
					}
					if observation.Work.SnapshotUnixNano < previous.SnapshotUnixNano || !minimaWorkMonotonic(previous, *observation.Work) {
						return errors.New("phase work resets")
					}
					if endpoint.RSSBytes > *exit.PeakRSSBytes {
						return errors.New("through-exit peak smaller than live RSS")
					}
					previous = *observation.Work
				}
			}
		}
		if !minimaWorkMonotonic(previous, *l.LastLiveWork.Work) {
			return errors.New("last live work drops phase prefix")
		}
		empty = empty || terminal.Work.Graph.Empty > 0
		exact = exact || terminal.Work.Graph.Exact > 0
		hnsw = hnsw || terminal.Work.Graph.HNSW > 0
		base = base || terminal.Work.Graph.BaseANNScored > 0
		delta = delta || terminal.Work.Graph.DeltaScored > 0
		scalar = scalar || terminal.Work.Typed.ScalarRows > 0
		text = text || terminal.Work.Typed.TextRows > 0
		getMany = getMany || terminal.Work.Output.GetMany.Completed > 0
		fold = fold || terminal.Work.Fold.Public.Completed > 0
	}
	if err := validateMinimaMeasuredObservationWindows(raw); err != nil {
		return err
	}
	if !empty || !exact || !hnsw || !base || !delta || !scalar || !text || native && !getMany || !fold {
		return errors.New("complete lifecycle lacks positive typed graph/output/mutation/fold producers")
	}
	return nil
}

func minimaMeasuredDrainedEqual(a, b workstats.Snapshot) bool {
	// Observation/serialization allocations may change memory and cache gauges.
	// No graph, materialization, ingestion, replay or maintenance runs in the gap.
	return a.Graph == b.Graph && a.Output == b.Output && a.Fold == b.Fold &&
		a.IndexedJSON == b.IndexedJSON && a.Typed == b.Typed && a.Runtime == b.Runtime &&
		a.Replay == b.Replay && a.Scans == b.Scans
}

func validateMinimaMeasuredObservationWindows(raw minimaRawBackendEvidence) error {
	attribution := raw.PhaseAttribution
	phases := attribution.Phases
	restart := phases[minimaTreeDBRestartOrdinal]
	old, fresh := raw.ProcessLifetimes[0], raw.ProcessLifetimes[1]
	if old.LastLiveWork.StartedMonotonicNS < restart.StartNanos || old.Exit.ObservedMonotonicNS > restart.EndNanos ||
		fresh.FirstWork.StartedMonotonicNS < restart.StartNanos || fresh.FirstWork.EndedMonotonicNS > restart.EndNanos {
		return errors.New("shutdown/startup observations escape measured restart interval")
	}
	var previousEnd *minimaWorkObservation
	for i, phase := range phases {
		lower, upper := raw.SetupInterval.StartedMonotonicNS, attribution.TotalEndNanos
		if i > 0 {
			lower = phases[i-1].EndNanos
		}
		if i+1 < len(phases) {
			upper = phases[i+1].StartNanos
		}
		for j, segment := range phase.ResourceSegments {
			start, end := segment.Start.WorkSnapshot, segment.End.WorkSnapshot
			life := raw.ProcessLifetimes[*segment.Start.LifetimeOrdinal]
			if start.StartedMonotonicNS < life.FirstWork.StartedMonotonicNS || end.EndedMonotonicNS > life.LastLiveWork.EndedMonotonicNS ||
				start.EndedMonotonicNS > end.StartedMonotonicNS {
				return errors.New("phase observation escapes owned lifetime/order")
			}
			if i == minimaTreeDBRestartOrdinal && j == 1 {
				if !reflect.DeepEqual(*start, fresh.FirstWork) {
					return errors.New("restart start does not retain actual first work")
				}
			} else if start.StartedMonotonicNS < lower || start.EndedMonotonicNS > phase.StartNanos {
				return errors.New("phase start observation outside preceding observer gap")
			}
			if i == minimaTreeDBRestartOrdinal && j == 0 {
				if !reflect.DeepEqual(*end, old.LastLiveWork) {
					return errors.New("restart end does not retain actual old last work")
				}
			} else if end.StartedMonotonicNS < phase.EndNanos || end.EndedMonotonicNS > upper {
				return errors.New("phase end observation outside following observer gap")
			}
			if previousEnd != nil && !(i == minimaTreeDBRestartOrdinal && j == 1) {
				if previousEnd.EndedMonotonicNS > start.StartedMonotonicNS || !minimaMeasuredDrainedEqual(*previousEnd.Work, *start.Work) {
					return errors.New("adjacent drained phase observations disagree")
				}
			}
			previousEnd = end
		}
	}
	finalSegments := phases[len(phases)-1].ResourceSegments
	final := finalSegments[len(finalSegments)-1].End
	if raw.ResourceMeasurement.End == nil || final.DiskBytes != raw.ResourceMeasurement.End.DiskBytes {
		return errors.New("final aggregate storage differs from frozen live phase endpoint")
	}
	return nil
}

func validateMinimaDenseRequest(r minimaMeasuredRequest) error {
	if r.DenseWork == nil {
		return errors.New("selected search work unavailable")
	}
	w := r.DenseWork
	g := w.Graph
	f := g.Filter
	o := w.Output
	if w.Version != 1 || !g.Available || (g.Route != "" && g.Route != "typed_empty" && g.Route != "typed_exact" && g.Route != "typed_hnsw") {
		return errors.New("invalid dense work version/route/availability")
	}
	if r.Outcome == "error" {
		// A transport or client decoding error can follow successful service work.
		// Preserve those completion flags; errors never become qualification samples.
		return nil
	}
	if !w.Completed || !g.Completed || !f.Attempted || !f.Completed || !g.Snapshot.Available || !o.Attempted || !o.Completed || r.ResultCount == nil {
		return errors.New("successful search has incomplete local work")
	}
	s := g.Snapshot
	if s.SchemaHash == 0 || s.SchemaGeneration == 0 || s.CurrentCoverageLSN < s.BaseCoverageLSN || s.BaseManifest.Format != "tcs1" || s.CurrentManifest.Format != "tcs1" || s.BaseManifest.Version == 0 || s.CurrentManifest.Version == 0 {
		return errors.New("search captured owner identity invalid")
	}
	if o.Requested != *r.ResultCount || o.Fetched != *r.ResultCount || o.Missing != 0 || o.RetainedPayloadFetches != 0 {
		return errors.New("search output work disagrees with returned results")
	}
	if *r.ResultCount > 0 && (o.OutputBytes == 0 || o.JSONReconstructionRows != *r.ResultCount) {
		return errors.New("search full document output unproven")
	}
	switch {
	case f.EligibleRows == 0:
		if g.Route != "typed_empty" || g.BaseANNScored != 0 || g.ExactBaseScored != 0 || g.DeltaScored != 0 || *r.ResultCount != 0 {
			return errors.New("empty filter route/work mismatch")
		}
	case f.EligibleRows <= 4096:
		if g.Route != "typed_exact" || g.BaseANNScored != 0 || g.ExactBaseScored > f.EligibleRows || g.DeltaScored != f.EligibleRows-g.ExactBaseScored {
			return errors.New("exact filter route/work mismatch")
		}
	default:
		if g.Route != "typed_hnsw" || g.ExactBaseScored != 0 || g.BaseANNScored == 0 {
			return errors.New("HNSW filter route/work mismatch")
		}
	}
	return nil
}

// Reconcile request-local prefixes with the same lifetime's actual drained
// process totals. Subtracting from those totals avoids overflowing a forged
// sum. Other recorded engine consumers may contribute additional work.
func minimaMeasuredOutputDelta(before, after workstats.Snapshot) (workstats.Snapshot, error) {
	if !minimaWorkMonotonic(before, after) {
		return workstats.Snapshot{}, errors.New("work delta decreases")
	}
	var subtract func(reflect.Value, reflect.Value)
	subtract = func(a, b reflect.Value) {
		if a.Kind() == reflect.Struct {
			for i := 0; i < a.NumField(); i++ {
				subtract(a.Field(i), b.Field(i))
			}
			return
		}
		b.SetUint(b.Uint() - a.Uint())
	}
	subtract(reflect.ValueOf(before.Graph), reflect.ValueOf(&after.Graph).Elem())
	subtract(reflect.ValueOf(before.Output), reflect.ValueOf(&after.Output).Elem())
	return after, nil
}

func minimaMeasuredChargeRequests(remaining workstats.Snapshot, requests []minimaMeasuredRequest, ordinal int, phase string) error {
	charge := func(p *uint64, value uint64) bool {
		if value > *p {
			return false
		}
		*p -= value
		return true
	}
	for _, r := range requests {
		if r.LifetimeOrdinal != ordinal || phase != "" && r.Phase != phase {
			continue
		}

		if r.DenseWork != nil {
			w, g := r.DenseWork, &remaining.Graph
			local := w.Graph
			if !charge(&g.Requests.Attempts, 1) || !charge(&g.Requests.Completed, 1) ||
				!charge(&g.BaseANNScored, local.BaseANNScored) || !charge(&g.ExactBaseScored, local.ExactBaseScored) ||
				!charge(&g.DeltaScored, local.DeltaScored) || !charge(&g.BaseCandidates, local.BaseCandidates) ||
				!charge(&g.BaseEdges, local.BaseEdges) || !charge(&g.BaseShadowed, local.BaseShadowed) || !charge(&g.BaseResultIDs, local.BaseResultIDs) ||
				!charge(&g.Filters.Attempts, 1) || !charge(&g.Filters.Completed, 1) ||
				!charge(&g.FilterSourceIDs, local.Filter.SourceIDs) || !charge(&g.FilterSourceBytes, local.Filter.SourceBytes) ||
				!charge(&g.FilterInspectedEntries, local.Filter.InspectedEntries) || !charge(&g.FilterMappingWorkCharged, local.Filter.MappingWorkCharged) {
				return errors.New("request graph/filter work exceeds drained lifetime producers")
			}
			var route *uint64
			switch local.Route {
			case "typed_empty":
				route = &g.Empty
			case "typed_exact":
				route = &g.Exact
			case "typed_hnsw":
				route = &g.HNSW
			}
			if route == nil || !charge(route, 1) {
				return errors.New("request routes exceed lifetime producers")
			}
			o, out := &remaining.Output.Search, w.Output
			if !charge(&o.Attempts, 1) || !charge(&o.Completed, 1) || !charge(&o.Requested, out.Requested) ||
				!charge(&o.Fetched, out.Fetched) || !charge(&o.Missing, out.Missing) || !charge(&o.OutputBytes, out.OutputBytes) ||
				!charge(&o.RetainedPayloadFetches, out.RetainedPayloadFetches) || !charge(&o.JSONReconstructionRows, out.JSONReconstructionRows) ||
				!charge(&o.TypedColumnRows, out.TypedColumnRows) {
				return errors.New("request search output exceeds drained lifetime producers")
			}
		}
		if r.Operation == "fetch" && r.Transport == "native" {
			o := &remaining.Output.GetMany
			if !charge(&o.Attempts, 1) || !charge(&o.Completed, 1) || !charge(&o.Requested, *r.RequestedCount) ||
				!charge(&o.Fetched, *r.ResultCount) || !charge(&o.Missing, *r.MissingCount) {
				return errors.New("GetMany requests exceed drained lifetime producers")
			}
		}
	}
	return nil
}

func validateMinimaMeasuredRequestTotals(raw minimaRawBackendEvidence) error {
	for ordinal, lifetime := range raw.ProcessLifetimes {
		if lifetime.FirstWork.Work == nil || lifetime.LastLiveWork.Work == nil {
			return errors.New("request totals lack drained lifetime work")
		}
		remaining, err := minimaMeasuredOutputDelta(*lifetime.FirstWork.Work, *lifetime.LastLiveWork.Work)
		if err != nil {
			return err
		}
		for _, r := range raw.RequestEvidence {
			if r.LifetimeOrdinal == ordinal && (r.StartedMonotonicNS < lifetime.FirstWork.EndedMonotonicNS || r.EndedMonotonicNS > lifetime.LastLiveWork.StartedMonotonicNS) {
				return errors.New("request lies outside observed live lifetime")
			}
		}
		if err := minimaMeasuredChargeRequests(remaining, raw.RequestEvidence, ordinal, ""); err != nil {
			return err
		}
	}
	for _, phase := range raw.PhaseAttribution.Phases {
		for _, segment := range phase.ResourceSegments {
			remaining, err := minimaMeasuredOutputDelta(*segment.Start.WorkSnapshot.Work, *segment.End.WorkSnapshot.Work)
			if err != nil {
				return err
			}
			if err := minimaMeasuredChargeRequests(remaining, raw.RequestEvidence, *segment.Start.LifetimeOrdinal, phase.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateMinimaMeasuredRequests(a *minimaArtifact, b minimaBackendEvidence, raw minimaRawBackendEvidence) error {
	if raw.PhaseAttribution == nil || len(raw.RequestEvidence) == 0 {
		return errors.New("measured requests/phases missing")
	}
	phases := map[string]minimaRawPhaseBoundary{}
	for _, p := range raw.PhaseAttribution.Phases {
		phases[p.Name] = p
	}
	setup := raw.SetupInterval
	if setup == nil || !minimaValidInterval(setup.StartedMonotonicNS, setup.EndedMonotonicNS) || setup.EndedMonotonicNS > raw.PhaseAttribution.TotalStartNanos {
		return errors.New("setup interval missing/invalid")
	}
	if b.Name == "treedb" && (len(raw.ProcessLifetimes) == 0 || setup.StartedMonotonicNS < raw.ProcessLifetimes[0].FirstWork.EndedMonotonicNS) {
		return errors.New("setup precedes observed first lifetime")
	}
	requests := map[uint64]minimaMeasuredRequest{}
	searchCounts := map[string]int{}
	for i, r := range raw.RequestEvidence {
		if r.RequestSequence != uint64(i+1) || r.OperationName == "" || !slices.Contains([]string{"search", "write", "control", "fetch"}, r.Operation) || !slices.Contains([]string{"native", "http"}, r.Transport) || !minimaValidInterval(r.StartedMonotonicNS, r.EndedMonotonicNS) || r.LifetimeOrdinal < 0 || r.LifetimeOrdinal > 1 {
			return errors.New("invalid measured request identity/interval")
		}
		phase, ok := phases[r.Phase]
		if r.Phase == "setup" {
			phase = minimaRawPhaseBoundary{StartNanos: setup.StartedMonotonicNS, EndNanos: setup.EndedMonotonicNS}
			ok = r.LifetimeOrdinal == 0 && r.Operation == "write" && r.OperationName == "ensure_compatible_collection" && r.Scenario == "all" && r.Transport == "http"
		}
		if !ok || r.StartedMonotonicNS < phase.StartNanos || r.EndedMonotonicNS > phase.EndNanos {
			return fmt.Errorf("request %d is outside its phase", r.RequestSequence)
		}
		if r.Outcome != "success" && r.Outcome != "error" || (r.Outcome == "error") != (r.Error != "") {
			return errors.New("request outcome/error mismatch")
		}
		if r.Phase != "restart_open_readiness" && ((r.LifetimeOrdinal == 0 && phase.StartNanos >= phases["post_reopen"].StartNanos) || (r.LifetimeOrdinal == 1 && phase.StartNanos < phases["restart_open_readiness"].StartNanos)) {
			return errors.New("request belongs to wrong lifetime")
		}
		if b.Name == "qdrant" && (r.DenseWork != nil || r.CommandVersion != nil || r.ExpectedGeneration != nil) {
			return errors.New("Qdrant fabricated TreeDB proof")
		}
		if r.Operation == "search" {
			wantPhase := "lifecycle_mutations"
			switch r.OperationName {
			case "warmup_search", "preclose_reopen_baseline", "post_reopen_parity", "final_manifest_and_oracle_comparison", "timed_search_with_batch_insert":
				wantPhase = map[string]string{"warmup_search": "warmup_search", "preclose_reopen_baseline": "pre_close_queries", "post_reopen_parity": "post_reopen", "final_manifest_and_oracle_comparison": "final_state_scroll_artifact_work", "timed_search_with_batch_insert": "timed_search_write_overlap"}[r.OperationName]
			}
			if r.Phase != wantPhase || b.Name == "qdrant" && r.Transport != "http" || b.Name == "treedb" && r.Transport != b.Configuration["transport"] {
				return errors.New("search phase/transport differs from declared operation")
			}
			if _, ok := minimaScenarioMap(&a.Manifest)[r.Scenario]; !ok {
				return errors.New("search scenario unknown")
			}
			if b.Name == "treedb" {
				if r.ExpectedGeneration == nil || *r.ExpectedGeneration == 0 || (r.Transport != "native" && r.Transport != "http") || (r.Transport == "native" && (r.CommandVersion == nil || *r.CommandVersion != 2)) || (r.Transport == "http" && r.CommandVersion != nil) {
					return errors.New("search admission/transport identity missing")
				}
				if r.Outcome == "success" && r.DenseWork != nil && r.Scenario != "small" && r.Scenario != "mixed_broad_narrow" && r.Scenario != "all_match" {
					expected := uint64(minimaScenarioMap(&a.Manifest)[r.Scenario].EligibleRows)
					if r.DenseWork.Graph.Filter.EligibleRows != expected {
						return errors.New("stable scenario filter cardinality differs from manifest")
					}
				}
				if err := validateMinimaDenseRequest(r); err != nil {
					return fmt.Errorf("request %d: %w", r.RequestSequence, err)
				}
			}
			searchCounts[r.OperationName+"/"+r.Scenario]++
		} else if r.DenseWork != nil {
			return errors.New("nonsearch request carries dense work")
		}
		if r.Operation == "fetch" && r.Outcome == "success" && (r.RequestedCount == nil || r.ResultCount == nil || r.MissingCount == nil || *r.ResultCount > *r.RequestedCount || *r.MissingCount != *r.RequestedCount-*r.ResultCount || r.Projection == "") {
			return errors.New("requested fetch evidence incomplete")
		}
		if r.Operation == "fetch" && r.Outcome == "success" {
			projection := "payload_only_batch"
			if b.Name == "treedb" {
				projection = "payload_only_per_id"
				if b.Configuration["transport"] == "native" {
					projection = "full_fp32_document"
					if r.Transport != "native" || r.CommandVersion == nil || *r.CommandVersion != 2 {
						return errors.New("native full-document fetch admission missing")
					}
				} else if *r.RequestedCount != 1 || r.Transport != "http" {
					return errors.New("HTTP per-ID fetch scope changed")
				}
			}
			if r.Projection != projection || uint64(len(r.RequestedIDs)) != *r.RequestedCount || uint64(len(r.ResultIDs)) != *r.ResultCount {
				return errors.New("fetch projection/count differs from submitted/returned IDs")
			}
			ids := map[string]bool{}
			for _, id := range r.RequestedIDs {
				if id == "" || ids[id] {
					return errors.New("fetch requested IDs invalid/duplicate")
				}
				ids[id] = true
			}
			for _, id := range r.ResultIDs {
				if !ids[id] {
					return errors.New("fetch returned unrequested/duplicate ID")
				}
				delete(ids, id)
			}
		}
		requests[r.RequestSequence] = r
		if b.Operations.ManifestOrdered && r.Outcome != "success" {
			return errors.New("completed lifecycle contains a failed request")
		}
	}
	joined := map[uint64]bool{}
	join := func(sequence uint64, scenario, phase string, start, end int64, ids []string) error {
		r, ok := requests[sequence]
		if !ok || joined[sequence] || r.Operation != "search" || r.Scenario != scenario || r.Phase != phase || r.StartedMonotonicNS != start || r.EndedMonotonicNS != end || r.Outcome != "success" || r.ResultCount == nil || *r.ResultCount != uint64(len(ids)) {
			return errors.New("query trace/request join missing/duplicate/mismatch")
		}
		joined[sequence] = true
		return nil
	}
	for _, q := range b.Operations.TimedExecutionTrace.Queries {
		if err := join(q.RequestSequence, q.Scenario, "timed_search_write_overlap", q.StartedMonotonicNS, q.EndedMonotonicNS, q.ActualIDs); err != nil {
			return err
		}
	}
	for _, op := range b.Operations.ReindexExecutionTrace.Operations {
		for _, q := range op.ReaderQueries {
			if err := join(q.RequestSequence, q.Scenario, "lifecycle_mutations", q.StartedMonotonicNS, q.EndedMonotonicNS, q.ActualIDs); err != nil {
				return err
			}
		}
	}
	if b.Operations.ManifestOrdered {
		expected := map[string]int{}
		for _, op := range a.Manifest.Operations {
			for _, step := range op.Schedule {
				expected[op.Name+"/"+step.Scenario]++
			}
			if op.TimedPlan != nil {
				for i := 0; i < op.TimedPlan.QueryCount; i++ {
					expected[op.Name+"/"+op.TimedPlan.ScenarioOrder[i%len(op.TimedPlan.ScenarioOrder)]]++
				}
			}
			if op.ConcurrentPlan != nil {
				for _, assignment := range op.ConcurrentPlan.ReaderAssignments {
					expected[op.Name+"/"+assignment.Scenario]++
				}
			}
		}
		for _, spec := range a.Manifest.Corpora {
			expected["preclose_reopen_baseline/"+spec.Name]++
			expected["post_reopen_parity/"+spec.Name]++
		}
		if err := validateMinimaMeasuredPublicCalls(&a.Manifest, b, raw); err != nil {
			return err
		}
		if !reflect.DeepEqual(expected, searchCounts) {
			return errors.New("actual search ledger does not cover exactly the declared schedule and lifecycle calls")
		}
		for _, r := range requests {
			if r.Operation == "search" && (r.Phase == "timed_search_write_overlap" || r.OperationName == a.Manifest.Operations[4].Name || r.OperationName == a.Manifest.Operations[5].Name) && !joined[r.RequestSequence] {
				return errors.New("unjoined timed/mutation query")
			}
		}
	}

	return nil
}

// Match existing explicit public calls and batch identities to the frozen
// operation stream. Readiness is one public operation even when it polls RPCs.
func validateMinimaMeasuredPublicCalls(m *minimaManifest, b minimaBackendEvidence, raw minimaRawBackendEvidence) error {
	type expectedCall struct {
		name, operation, scenario, phase string
		batch                            *uint64
		ids                              []string
		rows                             *uint64
		missing                          bool
	}
	var expected []expectedCall
	count := func(n int) *uint64 { v := uint64(n); return &v }
	control := func(name, operation, scenario, phase string) {
		expected = append(expected, expectedCall{name: name, operation: operation, scenario: scenario, phase: phase})
	}
	batch := func(name, scenario, phase string, start, rows int) {
		expected = append(expected, expectedCall{name: name, operation: "write", scenario: scenario, phase: phase, batch: count(start), rows: count(rows)})
	}
	fetch := func(name, scenario, phase string, ids []string, missing bool) {
		if b.Name == "treedb" && b.Configuration["transport"] == "http" {
			for _, id := range ids {
				expected = append(expected, expectedCall{name: name, operation: "fetch", scenario: scenario, phase: phase, ids: []string{id}, rows: count(1), missing: missing})
			}
		} else {
			expected = append(expected, expectedCall{name: name, operation: "fetch", scenario: scenario, phase: phase, ids: ids, rows: count(len(ids)), missing: missing})
		}
	}
	control("ensure_compatible_collection", "write", "all", "setup")
	load := "initial_durable_load"
	control("initial_batch_insert", "control", "all", load) // drain/readiness
	control("initial_batch_insert", "control", "all", load) // schema validation
	if b.Name == "treedb" {
		control("column_graph_initial_build", "control", "all", load)
		control("column_graph_fold", "control", "all", "lifecycle_mutations")
		control("column_graph_reopen_schema", "control", "all", "restart_open_readiness")
		control("column_graph_reopen_ensure", "control", "all", "restart_open_readiness")
		control("idempotent_ensure_after_reopen", "control", "all", "post_reopen")
		control("idempotent_ensure_after_reopen", "control", "all", "post_reopen")
	} else {
		control("restore_production_configuration", "control", "all", load)
		control("idempotent_ensure_after_reopen", "write", "all", "restart_open_readiness")
		control("idempotent_ensure_after_reopen", "control", "all", "restart_open_readiness")
		control("idempotent_ensure_after_reopen", "control", "all", "restart_open_readiness")
	}
	control("final_manifest_and_oracle_comparison", "control", "all", "final_state_scroll_artifact_work")
	for _, op := range m.Operations {
		phase := "lifecycle_mutations"
		if op.Name == "initial_batch_insert" {
			phase = load
		}
		if op.TimedPlan != nil {
			phase = "timed_search_write_overlap"
		}
		ranges := op.InsertRanges
		if op.TimedPlan != nil {
			ranges = nil
			for _, round := range op.TimedPlan.Rounds {
				ranges = append(ranges, round.InsertRange)
			}
		}
		for _, insertion := range ranges {
			for offset := 0; offset < insertion.Rows; offset += m.Config.BatchSize {
				batch(op.Name, insertion.Scenario, phase, insertion.Start+offset, min(m.Config.BatchSize, insertion.Rows-offset))
			}
			if op.TimedPlan != nil {
				ids := make([]string, insertion.Rows)
				for i := range ids {
					ids[i] = fmt.Sprintf("minima/%s/%06d", insertion.Scenario, insertion.Start+i)
				}
				fetch(op.Name, insertion.Scenario, phase, ids, false)
				if b.Name == "qdrant" {
					control(op.Name, "control", insertion.Scenario, phase)
				}
			}
		}
		if len(op.Documents) > 0 {
			for offset := 0; offset < len(op.Documents); offset += m.Config.BatchSize {
				// Existing batch correlation uses generated ordinals, or local replacement offsets.
				start := offset
				if n, err := strconv.Atoi(strings.TrimPrefix(op.Documents[offset].ID, "minima/"+op.Target+"/")); err == nil {
					start = n
				}
				batch(op.Name, op.Target, phase, start, min(m.Config.BatchSize, len(op.Documents)-offset))
			}
			ids := make([]string, len(op.Documents))
			for i, doc := range op.Documents {
				ids[i] = doc.ID
			}
			fetch(op.Name, op.Target, phase, ids, false)
			if b.Name == "qdrant" {
				control(op.Name, "control", op.Target, phase)
			}
		}
		if op.Effect == "delete" {
			control(op.Name, "write", op.Target, phase)
			if b.Name == "qdrant" {
				control(op.Name, "control", op.Target, phase)
			}
			if op.Filter != nil {
				control(op.Name, "control", op.Target, phase)
				expected[len(expected)-1].missing = true // actual filtered count is zero
			} else {
				fetch(op.Name, op.Target, phase, op.IDs, true)
			}
		}
	}
	actual := make(map[string][]minimaMeasuredRequest)
	key := func(name, operation, scenario, phase string) string {
		return name + "/" + operation + "/" + scenario + "/" + phase
	}
	for _, r := range raw.RequestEvidence {
		if r.Operation != "search" {
			k := key(r.OperationName, r.Operation, r.Scenario, r.Phase)
			actual[k] = append(actual[k], r)
		}
	}
	for _, want := range expected {
		k := key(want.name, want.operation, want.scenario, want.phase)
		candidates := actual[k]
		match := -1
		for i, r := range candidates {
			if !reflect.DeepEqual(want.batch, r.BatchStart) || !reflect.DeepEqual(want.ids, r.RequestedIDs) {
				continue
			}
			if want.rows != nil && (r.RequestedCount == nil || *r.RequestedCount != *want.rows) {
				continue
			}
			if want.operation == "fetch" {
				expectedIDs := want.ids
				if want.missing {
					expectedIDs = nil
				}
				if len(r.ResultIDs) != len(expectedIDs) {
					continue
				}
			}
			if want.missing && want.operation == "control" && (r.ResultCount == nil || *r.ResultCount != 0) {
				continue
			}
			if want.batch != nil {
				if b.Name == "treedb" && (r.ResultCount == nil || *r.ResultCount != *want.rows || r.Transport != b.Configuration["transport"] ||
					r.Transport == "native" && (r.CommandVersion == nil || *r.CommandVersion != 1 || r.ExpectedGeneration == nil || *r.ExpectedGeneration == 0)) {
					continue
				}
			} else if want.operation != "fetch" && (r.Transport != "http" || r.CommandVersion != nil || r.ExpectedGeneration != nil) {
				continue
			}
			match = i
			break
		}
		if match < 0 {
			return fmt.Errorf("required public call/batch/fetch missing or invalid: %s", k)
		}
		candidates = append(candidates[:match], candidates[match+1:]...)
		actual[k] = candidates
	}
	for k, rows := range actual {
		if len(rows) > 0 {
			return fmt.Errorf("extra/unmatched public calls: %s", k)
		}
	}
	// The writer boundaries already recorded in the existing concurrency traces
	// must be the min/max of their actual submitted batch calls, not copied flags.
	interval := func(name, scenario string, insertion *minimaInsertRange, start, end int64) error {
		var first, last int64
		for _, r := range raw.RequestEvidence {
			if r.Operation != "write" || r.OperationName != name || r.Scenario != scenario {
				continue
			}
			if insertion != nil && (r.BatchStart == nil || *r.BatchStart < uint64(insertion.Start) || *r.BatchStart >= uint64(insertion.Start+insertion.Rows)) {
				continue
			}
			if first == 0 || r.StartedMonotonicNS < first {
				first = r.StartedMonotonicNS
			}
			last = max(last, r.EndedMonotonicNS)
		}
		if first != start || last != end {
			return errors.New("writer request intervals disagree with existing execution trace")
		}
		return nil
	}
	for _, round := range b.Operations.TimedExecutionTrace.Rounds {
		if err := interval("timed_search_with_batch_insert", round.InsertRange.Scenario, &round.InsertRange, round.WriterStartedMonotonicNS, round.WriterEndedMonotonicNS); err != nil {
			return err
		}
	}
	for _, observed := range b.Operations.ReindexExecutionTrace.Operations {
		op := m.Operations[observed.OperationOrdinal]
		if err := interval(op.Name, op.Target, nil, observed.MutationStartedMonotonicNS, observed.MutationEndedMonotonicNS); err != nil {
			return err
		}
	}
	return validateMinimaMeasuredPublicCallOrder(raw)
}

// Check existing public-operation completion boundaries. Queries may overlap
// their concurrent writer; only readiness/visibility claims require a drain.
func validateMinimaMeasuredPublicCallOrder(raw minimaRawBackendEvidence) error {
	groups := map[string][]minimaMeasuredRequest{}
	writes := map[string]int64{}
	var initialEnd, mutationEnd int64
	for _, r := range raw.RequestEvidence {
		key := r.OperationName + "/" + r.Scenario
		if r.Operation != "search" {
			groups[key+"/"+r.Operation] = append(groups[key+"/"+r.Operation], r)
		}
		if r.Operation == "write" {
			writes[key] = max(writes[key], r.EndedMonotonicNS)
			if r.Phase == "initial_durable_load" {
				initialEnd = max(initialEnd, r.EndedMonotonicNS)
			}
		}
		if r.Phase == "lifecycle_mutations" && r.OperationName != "column_graph_fold" {
			mutationEnd = max(mutationEnd, r.EndedMonotonicNS)
		}
	}
	after := func(key string, earliest int64) (int64, error) {
		calls := groups[key]
		slices.SortFunc(calls, func(a, b minimaMeasuredRequest) int {
			if a.StartedMonotonicNS < b.StartedMonotonicNS {
				return -1
			}
			if a.StartedMonotonicNS > b.StartedMonotonicNS {
				return 1
			}
			return 0
		})
		for _, r := range calls {
			if r.StartedMonotonicNS < earliest {
				return 0, fmt.Errorf("public call precedes required completion: %s", key)
			}
			earliest = r.EndedMonotonicNS
		}
		return earliest, nil
	}
	build := "column_graph_initial_build/all/control"
	if len(groups[build]) == 0 {
		build = "restore_production_configuration/all/control"
	}
	ready, err := after(build, initialEnd)
	if err != nil {
		return err
	}
	if _, err = after("initial_batch_insert/all/control", ready); err != nil {
		return err
	}
	// Each frozen timed range has one scenario. The required-call matcher and
	// existing trace validator establish the range/batch identities above.
	for key, end := range writes {
		if key == "ensure_compatible_collection/all" || strings.HasPrefix(key, "initial_batch_insert/") {
			continue
		}
		end, err = after(key+"/control", end)
		if err != nil {
			return err
		}
		if _, err = after(key+"/fetch", end); err != nil {
			return err
		}
	}
	if _, err = after("column_graph_fold/all/control", mutationEnd); err != nil {
		return err
	}
	schema, err := after("column_graph_reopen_schema/all/control", 0)
	if err != nil {
		return err
	}
	_, err = after("column_graph_reopen_ensure/all/control", schema)
	return err
}

type minimaMeasuredGate struct {
	Name       string
	Value, Cap int64
}

func validateMinimaMeasuredGateCaps(gates []minimaMeasuredGate) error {
	for _, gate := range gates {
		if gate.Value > gate.Cap {
			return fmt.Errorf("measured cap %s: %d > %d", gate.Name, gate.Value, gate.Cap)
		}
	}
	return nil
}

func minimaMeasuredGates(a *minimaArtifact) ([]minimaMeasuredGate, error) {
	raw, ok := a.RawEvidence["treedb"]
	if !ok || raw.PhaseAttribution == nil {
		return nil, errors.New("TreeDB measured gate inputs missing")
	}
	durations := map[string]int64{}
	for _, p := range raw.PhaseAttribution.Phases {
		if !minimaValidInterval(p.StartNanos, p.EndNanos) || p.DurationNanos != p.EndNanos-p.StartNanos {
			return nil, errors.New("gate phase interval invalid")
		}
		durations[p.Name] = p.DurationNanos
	}
	var peak int64
	for _, l := range raw.ProcessLifetimes {
		if l.Exit.Availability != "measured" || l.Exit.PeakRSSBytes == nil || *l.Exit.PeakRSSBytes <= 0 {
			return nil, errors.New("gate through-exit peak missing")
		}
		peak = max(peak, *l.Exit.PeakRSSBytes)
	}
	var times []int64
	for _, b := range a.Backends {
		if b.Name == "treedb" {
			for _, q := range b.Operations.TimedExecutionTrace.Queries {
				if !minimaValidInterval(q.StartedMonotonicNS, q.EndedMonotonicNS) {
					return nil, errors.New("gate query interval invalid")
				}
				times = append(times, q.EndedMonotonicNS-q.StartedMonotonicNS)
			}
		}
	}
	if len(times) != 1024 || durations["initial_durable_load"] <= 0 || durations["restart_open_readiness"] <= 0 || raw.ResourceMeasurement.End == nil || !raw.ResourceMeasurement.End.Captured || raw.ResourceMeasurement.End.DiskBytes <= 0 {
		return nil, errors.New("five gate populations incomplete")
	}
	slices.Sort(times)
	return []minimaMeasuredGate{{"load_readiness_ns", durations["initial_durable_load"], minimaMeasuredLoadCap}, {"restart_readiness_ns", durations["restart_open_readiness"], minimaMeasuredRestartCap}, {"through_exit_peak_rss_bytes", peak, minimaMeasuredRSSCap}, {"all_1024_outer_search_p50_ns", times[511], minimaMeasuredSearchCap}, {"final_live_disk_bytes", raw.ResourceMeasurement.End.DiskBytes, minimaMeasuredDiskCap}}, nil
}
func validateMinimaMeasuredArtifact(a *minimaArtifact, f *minimaMeasuredFreeze) error {
	if a.NativePathProof != nil {
		return errors.New("measured schema cannot use historical native proof")
	}
	if err := validateMinimaManifest(&a.Manifest); err != nil {
		return err
	}
	if err := validateMinimaMeasuredBinding(a, f); err != nil {
		return err
	}
	bounded := a.Manifest.Schema == minimaBoundedManifestSchema
	if a.State == "partial" {
		if a.Passing || a.Recommendation != "not_evaluated" {
			return errors.New("partial measured evidence cannot qualify")
		}
	} else if a.State != "pass" || !a.Passing || len(a.Failures) != 0 || bounded || len(a.Backends) != 2 {
		return errors.New("measured qualification requires two complete full backends")
	}
	if a.Passing && !slices.Contains([]string{"ready_direct", "ready_with_alpha_limitations", "not_ready", "unsuitable"}, a.Recommendation) {
		return errors.New("measured qualification readiness recommendation invalid")
	}
	backends := map[string]minimaBackendEvidence{}
	for _, b := range a.Backends {
		if _, exists := backends[b.Name]; exists {
			return errors.New("duplicate measured backend")
		}
		backends[b.Name] = b
		raw, ok := a.RawEvidence[b.Name]
		if !ok {
			return errors.New("measured backend raw evidence missing")
		}
		if !b.Operations.ManifestOrdered {
			if a.State != "partial" || len(a.Failures) == 0 || b.Reopen.CommittedParity || raw.FinalScrollState.Match {
				return errors.New("incomplete measured run has no failure or claims completed parity")
			}
			// Malformed present producer fields were already rejected at read. An
			// incomplete run is retained, never a completed comparator input.
			continue
		}
		if err := validateMinimaBackendLifecycle(b, &a.Manifest); err != nil {
			return err
		}
		if err := validateMinimaMeasuredRequests(a, b, raw); err != nil {
			return err
		}
		if raw.PhaseAttribution == nil {
			return errors.New("measured phase attribution missing")
		}
		if err := validateMinimaTreeDBPhaseAttribution(*raw.PhaseAttribution, raw.RestartBoundary); err != nil {
			return err
		}
		if b.Name == "treedb" {
			if err := validateMinimaMeasuredLifetimes(raw, b.Configuration["transport"] == "native"); err != nil {
				return err
			}
			if err := validateMinimaMeasuredRequestTotals(raw); err != nil {
				return err
			}
		}
	}
	if len(backends) == 0 || len(backends) != len(a.RawEvidence) {
		return errors.New("measured backend envelope invalid")
	}
	queries, specs := minimaQueryMap(&a.Manifest), minimaScenarioMap(&a.Manifest)
	seen := map[string]bool{}
	for _, row := range a.Scenarios {
		b, ok := backends[row.Backend]
		if !ok {
			return errors.New("scenario backend unknown")
		}
		if !b.Operations.ManifestOrdered {
			continue
		}
		spec, ok := specs[row.Scenario]
		key := row.Backend + "/" + row.Scenario
		if !ok || seen[key] {
			return errors.New("scenario missing/duplicate")
		}
		seen[key] = true
		if err := validateMinimaScenarioEvidence(row, spec, queries[row.Scenario], true); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	complete := map[string]minimaBackendEvidence{}
	for name, b := range backends {
		if b.Operations.ManifestOrdered {
			complete[name] = b
			for scenario := range specs {
				if !seen[name+"/"+scenario] {
					return errors.New("completed backend lacks scenario")
				}
			}
		}
	}
	if len(complete) > 0 {
		view := *a
		view.RawEvidence = map[string]minimaRawBackendEvidence{}
		for name := range complete {
			view.RawEvidence[name] = a.RawEvidence[name]
		}
		if err := validateMinimaRawEvidence(&view, complete); err != nil {
			return err
		}
	}
	if b, ok := complete["treedb"]; ok && b.Operations.ManifestOrdered {
		gates, err := minimaMeasuredGates(a)
		if err != nil {
			return err
		}
		if !bounded {
			if err := validateMinimaMeasuredGateCaps(gates); err != nil {
				return err
			}
		}
	}
	if a.Passing && len(complete) != 2 {
		return errors.New("incomplete measured comparison cannot qualify")
	}
	return nil
}
