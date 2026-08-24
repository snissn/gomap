// Command treedb_rag_interleave runs two benchmark binaries in per-cell
// counterbalanced ABBA order and records the exact worker rows.
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	expectedCellCount               = 384
	expectedWorkerEnvironmentPolicy = "fresh_unique_dir_per_cell"
	maxCellAttempts                 = 2
	workerShardSize                 = 8
)

var legOrder = [4]string{"A1", "B1", "B2", "A2"}

type config struct {
	ABinary, BBinary          string
	Output, Root              string
	WorkerArgs                []string
	ReadyTimeout, CellTimeout time.Duration
	Legs                      [4]legConfig
}

type legConfig struct {
	Name, Variant, Binary           string
	ProductBaseSHA, HarnessRevision string
}

type workerReady struct {
	Ready                bool   `json:"ready"`
	CellCount            int    `json:"cell_count"`
	ProductBaseSHA       string `json:"product_base_sha"`
	HarnessRevision      string `json:"harness_revision"`
	EnvironmentPolicy    string `json:"environment_policy"`
	FixtureSHA256        string `json:"fixture_sha256"`
	ConfigSHA256         string `json:"config_sha256"`
	SemanticVectorSHA256 string `json:"semantic_vector_sha256"`
}

type workerRequest struct {
	Ordinal int `json:"ordinal"`
}

type workerResponse struct {
	Ordinal int             `json:"ordinal"`
	Row     json.RawMessage `json:"row"`
	Error   string          `json:"error"`
}

type cellIdentity struct {
	Route       string `json:"route"`
	Projection  string `json:"projection"`
	Filter      string `json:"filter"`
	Collapse    string `json:"collapse"`
	Surface     string `json:"surface"`
	Embedding   string `json:"embedding"`
	VectorRoute string `json:"vector_route"`
	Clients     int    `json:"clients"`
}

type comparisonIdentity struct {
	WorkDigest    string `json:"work_digest"`
	Projection    string `json:"projection"`
	QualityDigest string `json:"quality_digest"`
}

type rowIdentity struct {
	Cell       *cellIdentity       `json:"cell"`
	Status     *string             `json:"status"`
	Comparison *comparisonIdentity `json:"comparison"`
}

type namedResponse struct {
	Leg      string
	Response workerResponse
}

type workerEvidence struct {
	Epoch                int       `json:"epoch"`
	Leg                  string    `json:"leg"`
	Variant              string    `json:"variant"`
	Command              []string  `json:"command"`
	BinarySHA256         string    `json:"binary_sha256"`
	ProductBaseSHA       string    `json:"product_base_sha"`
	HarnessRevision      string    `json:"harness_revision"`
	EnvironmentPolicy    string    `json:"environment_policy"`
	FixtureSHA256        string    `json:"fixture_sha256"`
	ConfigSHA256         string    `json:"config_sha256"`
	SemanticVectorSHA256 string    `json:"semantic_vector_sha256"`
	StartedAt            time.Time `json:"started_at"`
	ReadyAt              time.Time `json:"ready_at"`
	FinishedAt           time.Time `json:"finished_at"`
}

type legAttempt struct {
	Attempt    int       `json:"attempt"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Error      string    `json:"error,omitempty"`
}

type legRow struct {
	Leg      string          `json:"leg"`
	Attempts []legAttempt    `json:"attempts"`
	Row      json.RawMessage `json:"row"`
}

type cellEvidence struct {
	Ordinal int      `json:"ordinal"`
	Order   []string `json:"order"`
	Legs    []legRow `json:"legs"`
}

type artifact struct {
	SchemaVersion int              `json:"schema_version"`
	StartedAt     time.Time        `json:"started_at"`
	FinishedAt    time.Time        `json:"finished_at"`
	Root          string           `json:"root"`
	CellCount     int              `json:"cell_count"`
	Workers       []workerEvidence `json:"workers"`
	Cells         []cellEvidence   `json:"cells"`
}

type worker struct {
	cfg         legConfig
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	encoder     *json.Encoder
	decoder     *json.Decoder
	cellTimeout time.Duration
	stderr      bytes.Buffer
	evidence    workerEvidence
}

func orderForOrdinal(ordinal int) [4]string {
	if ordinal%2 == 0 {
		return legOrder
	}
	return [4]string{legOrder[3], legOrder[2], legOrder[1], legOrder[0]}
}

func validateResponse(ordinal int, response namedResponse) error {
	if response.Response.Ordinal != ordinal {
		return fmt.Errorf("%s: response ordinal %d, want %d", response.Leg, response.Response.Ordinal, ordinal)
	}
	if response.Response.Error != "" {
		return fmt.Errorf("%s: worker error: %s", response.Leg, response.Response.Error)
	}
	if len(response.Response.Row) == 0 {
		return fmt.Errorf("%s: response has no row", response.Leg)
	}
	return nil
}

func validateResponses(ordinal int, responses []namedResponse) error {
	if len(responses) != len(legOrder) {
		return fmt.Errorf("cell %d: got %d leg responses, want %d", ordinal, len(responses), len(legOrder))
	}
	var want rowIdentity
	for i, response := range responses {
		if err := validateResponse(ordinal, response); err != nil {
			return err
		}
		var got rowIdentity
		if err := json.Unmarshal(response.Response.Row, &got); err != nil {
			return fmt.Errorf("%s: decode row identity: %w", response.Leg, err)
		}
		if got.Cell == nil || got.Status == nil || *got.Status == "" || got.Comparison == nil {
			return fmt.Errorf("%s: row lacks cell, status, or comparison identity", response.Leg)
		}
		if got.Comparison.WorkDigest == "" || got.Comparison.Projection == "" || got.Comparison.QualityDigest == "" {
			return fmt.Errorf("%s: row comparison identity is incomplete", response.Leg)
		}
		if i == 0 {
			want = got
			continue
		}
		if *got.Cell != *want.Cell {
			return fmt.Errorf("%s: cell identity does not match %s", response.Leg, responses[0].Leg)
		}
		if *got.Comparison != *want.Comparison {
			return fmt.Errorf("%s: comparison identity does not match %s", response.Leg, responses[0].Leg)
		}
		// Capability-delivery candidates legitimately change unsupported control
		// cells to supported. Preserve each leg's status; only cell identity must
		// match for per-cell ordering.
	}
	return nil
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "treedb_rag_interleave: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	return runCoordinator(cfg)
}

func parseConfig(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("treedb_rag_interleave", flag.ContinueOnError)
	fs.StringVar(&cfg.ABinary, "a-binary", "", "benchmark binary for A1 and A2")
	fs.StringVar(&cfg.BBinary, "b-binary", "", "benchmark binary for B1 and B2")
	fs.StringVar(&cfg.Output, "out", "", "gzip JSON artifact path")
	fs.StringVar(&cfg.Root, "root", "", "root for the four worker databases")
	fs.DurationVar(&cfg.ReadyTimeout, "ready-timeout", 2*time.Minute, "maximum worker readiness time")
	fs.DurationVar(&cfg.CellTimeout, "cell-timeout", 5*time.Minute, "maximum time for one cell attempt")
	for index, name := range legOrder {
		leg := strings.ToLower(name)
		cfg.Legs[index].Name = name
		fs.StringVar(&cfg.Legs[index].ProductBaseSHA, leg+"-product-base-sha", "", "expected product SHA for "+name)
		fs.StringVar(&cfg.Legs[index].HarnessRevision, leg+"-harness-revision", "", "expected harness revision for "+name)
	}
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	cfg.WorkerArgs = append([]string(nil), fs.Args()...)
	if cfg.ABinary == "" || cfg.BBinary == "" || cfg.Output == "" || cfg.Root == "" {
		return config{}, errors.New("-a-binary, -b-binary, -out, and -root are required")
	}
	if cfg.ReadyTimeout <= 0 || cfg.CellTimeout <= 0 {
		return config{}, errors.New("-ready-timeout and -cell-timeout must be positive")
	}
	for i := range cfg.Legs {
		leg := &cfg.Legs[i]
		if leg.Name[0] == 'A' {
			leg.Variant, leg.Binary = "A", cfg.ABinary
		} else {
			leg.Variant, leg.Binary = "B", cfg.BBinary
		}
		if leg.ProductBaseSHA == "" || leg.HarnessRevision == "" {
			return config{}, fmt.Errorf("-%s-product-base-sha and -%s-harness-revision are required", strings.ToLower(leg.Name), strings.ToLower(leg.Name))
		}
	}
	for _, arg := range cfg.WorkerArgs {
		name := strings.SplitN(strings.TrimLeft(arg, "-"), "=", 2)[0]
		switch name {
		case "smoke":
			return config{}, errors.New("-smoke is diagnostic-only and cannot be forwarded by the final interleave coordinator")
		case "cell-worker", "dir", "product-base-sha", "harness-revision":
			return config{}, fmt.Errorf("worker argument %q is coordinator-owned", arg)
		}
	}
	return cfg, nil
}

func runCoordinator(cfg config) error {
	startedAt := time.Now().UTC()
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return fmt.Errorf("create root: %w", err)
	}
	binaryHashes := make(map[string]string, 2)
	for _, path := range []string{cfg.ABinary, cfg.BBinary} {
		if _, ok := binaryHashes[path]; ok {
			continue
		}
		hash, err := hashFile(path)
		if err != nil {
			return fmt.Errorf("hash binary %s: %w", path, err)
		}
		binaryHashes[path] = hash
	}

	var workers []*worker
	var byName map[string]*worker
	evidence := make([]workerEvidence, 0, len(cfg.Legs)*(expectedCellCount/workerShardSize))
	startEpoch := func(epoch int) error {
		workers = make([]*worker, 0, len(cfg.Legs))
		byName = make(map[string]*worker, len(cfg.Legs))
		for _, leg := range cfg.Legs {
			worker, err := startWorker(leg, cfg.Root, cfg.WorkerArgs, binaryHashes[leg.Binary], epoch, cfg.ReadyTimeout, cfg.CellTimeout)
			if worker != nil {
				workers = append(workers, worker)
			}
			if err != nil {
				stopWorkers(workers)
				if worker != nil && worker.stderr.Len() != 0 {
					return fmt.Errorf("start %s epoch %d: %w: %s", leg.Name, epoch, err, strings.TrimSpace(worker.stderr.String()))
				}
				return fmt.Errorf("start %s epoch %d: %w", leg.Name, epoch, err)
			}
			byName[leg.Name] = worker
		}
		want := workers[0].evidence
		if want.FixtureSHA256 == "" || want.ConfigSHA256 == "" || want.SemanticVectorSHA256 == "" {
			stopWorkers(workers)
			return fmt.Errorf("epoch %d: worker workload identity is incomplete", epoch)
		}
		for _, worker := range workers[1:] {
			got := worker.evidence
			if got.FixtureSHA256 != want.FixtureSHA256 || got.ConfigSHA256 != want.ConfigSHA256 || got.SemanticVectorSHA256 != want.SemanticVectorSHA256 {
				stopWorkers(workers)
				return fmt.Errorf("epoch %d: %s workload identity does not match %s", epoch, got.Leg, want.Leg)
			}
		}
		return nil
	}
	finishEpoch := func() error {
		if err := finishWorkers(workers); err != nil {
			return err
		}
		for _, worker := range workers {
			evidence = append(evidence, worker.evidence)
		}
		return nil
	}
	if err := startEpoch(0); err != nil {
		return err
	}

	cells := make([]cellEvidence, 0, expectedCellCount)
	for ordinal := range expectedCellCount {
		if ordinal > 0 && ordinal%workerShardSize == 0 {
			if err := finishEpoch(); err != nil {
				return err
			}
			if err := startEpoch(ordinal / workerShardSize); err != nil {
				return err
			}
		}
		order := orderForOrdinal(ordinal)
		named := make([]namedResponse, 0, len(order))
		rows := make([]legRow, 0, len(order))
		for _, name := range order {
			worker := byName[name]
			var response workerResponse
			attempts := make([]legAttempt, 0, maxCellAttempts)
			for attempt := 1; attempt <= maxCellAttempts; attempt++ {
				requestStarted := time.Now().UTC()
				var err error
				response, err = worker.request(ordinal)
				requestFinished := time.Now().UTC()
				if err != nil {
					stopWorkers(workers)
					return fmt.Errorf("cell %d %s attempt %d: %w", ordinal, name, attempt, err)
				}
				attempts = append(attempts, legAttempt{Attempt: attempt, StartedAt: requestStarted, FinishedAt: requestFinished, Error: response.Error})
				if response.Error == "" {
					break
				}
			}
			result := namedResponse{Leg: name, Response: response}
			if err := validateResponse(ordinal, result); err != nil {
				stopWorkers(workers)
				return fmt.Errorf("cell %d: %w", ordinal, err)
			}
			named = append(named, result)
			rows = append(rows, legRow{Leg: name, Attempts: attempts, Row: response.Row})
		}
		if err := validateResponses(ordinal, named); err != nil {
			stopWorkers(workers)
			return fmt.Errorf("cell %d: %w", ordinal, err)
		}
		cells = append(cells, cellEvidence{Ordinal: ordinal, Order: append([]string(nil), order[:]...), Legs: rows})
	}
	if err := finishEpoch(); err != nil {
		return err
	}
	finishedAt := time.Now().UTC()
	return writeArtifact(cfg.Output, artifact{
		SchemaVersion: 1,
		StartedAt:     startedAt,
		FinishedAt:    finishedAt,
		Root:          cfg.Root,
		CellCount:     expectedCellCount,
		Workers:       evidence,
		Cells:         cells,
	})
}

func decodeWithTimeout(decoder *json.Decoder, value any, limit time.Duration) error {
	done := make(chan error, 1)
	go func() {
		done <- decoder.Decode(value)
	}()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("timed out after %s", limit)
	}
}

func startWorker(cfg legConfig, root string, commonArgs []string, binaryHash string, epoch int, readyTimeout, cellTimeout time.Duration) (*worker, error) {
	args := []string{
		"-cell-worker",
		"-dir", filepath.Join(root, fmt.Sprintf("epoch-%03d", epoch), strings.ToLower(cfg.Name)),
		"-product-base-sha", cfg.ProductBaseSHA,
		"-harness-revision", cfg.HarnessRevision,
	}
	args = append(args, commonArgs...)
	cmd := exec.Command(cfg.Binary, args...)
	worker := &worker{cfg: cfg, cmd: cmd}
	cmd.Stderr = &worker.stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return worker, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return worker, err
	}
	worker.stdin = stdin
	worker.encoder = json.NewEncoder(stdin)
	worker.decoder = json.NewDecoder(bufio.NewReader(stdout))
	worker.evidence = workerEvidence{
		Epoch:           epoch,
		Leg:             cfg.Name,
		Variant:         cfg.Variant,
		Command:         append([]string(nil), cmd.Args...),
		BinarySHA256:    binaryHash,
		ProductBaseSHA:  cfg.ProductBaseSHA,
		HarnessRevision: cfg.HarnessRevision,
		StartedAt:       time.Now().UTC(),
	}
	if err := cmd.Start(); err != nil {
		return worker, err
	}
	worker.cellTimeout = cellTimeout
	var ready workerReady
	if err := decodeWithTimeout(worker.decoder, &ready, readyTimeout); err != nil {
		return worker, fmt.Errorf("decode readiness: %w", err)
	}
	worker.evidence.ReadyAt = time.Now().UTC()
	if !ready.Ready {
		return worker, errors.New("worker did not report ready")
	}
	if ready.CellCount != expectedCellCount {
		return worker, fmt.Errorf("cell_count %d, want %d", ready.CellCount, expectedCellCount)
	}
	if ready.ProductBaseSHA != cfg.ProductBaseSHA {
		return worker, fmt.Errorf("product_base_sha %q, want %q", ready.ProductBaseSHA, cfg.ProductBaseSHA)
	}
	if ready.HarnessRevision != cfg.HarnessRevision {
		return worker, fmt.Errorf("harness_revision %q, want %q", ready.HarnessRevision, cfg.HarnessRevision)
	}
	if ready.EnvironmentPolicy != expectedWorkerEnvironmentPolicy {
		return worker, fmt.Errorf("environment_policy %q, want %q", ready.EnvironmentPolicy, expectedWorkerEnvironmentPolicy)
	}
	worker.evidence.EnvironmentPolicy = ready.EnvironmentPolicy
	worker.evidence.FixtureSHA256 = ready.FixtureSHA256
	worker.evidence.ConfigSHA256 = ready.ConfigSHA256
	worker.evidence.SemanticVectorSHA256 = ready.SemanticVectorSHA256
	return worker, nil
}

func (worker *worker) request(ordinal int) (workerResponse, error) {
	if err := worker.encoder.Encode(workerRequest{Ordinal: ordinal}); err != nil {
		return workerResponse{}, fmt.Errorf("send request: %w", err)
	}
	var response workerResponse
	if err := decodeWithTimeout(worker.decoder, &response, worker.cellTimeout); err != nil {
		return workerResponse{}, fmt.Errorf("read response: %w", err)
	}
	return response, nil
}

func stopWorkers(workers []*worker) {
	for _, worker := range workers {
		if worker.stdin != nil {
			_ = worker.stdin.Close()
		}
		if worker.cmd.Process != nil {
			_ = worker.cmd.Process.Kill()
		}
	}
	for _, worker := range workers {
		if worker.cmd.Process != nil {
			_ = worker.cmd.Wait()
		}
	}
}

func finishWorkers(workers []*worker) error {
	for _, worker := range workers {
		if err := worker.stdin.Close(); err != nil {
			stopWorkers(workers)
			return fmt.Errorf("close %s input: %w", worker.cfg.Name, err)
		}
	}
	var firstErr error
	for _, worker := range workers {
		err := worker.cmd.Wait()
		worker.evidence.FinishedAt = time.Now().UTC()
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("wait for %s: %w: %s", worker.cfg.Name, err, strings.TrimSpace(worker.stderr.String()))
		}
	}
	return firstErr
}

func writeArtifact(path string, value artifact) (err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer func() {
		_ = file.Close()
		if err != nil {
			_ = os.Remove(temporary)
		}
	}()
	gzipWriter := gzip.NewWriter(file)
	if err = json.NewEncoder(gzipWriter).Encode(value); err != nil {
		_ = gzipWriter.Close()
		return err
	}
	if err = gzipWriter.Close(); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := directory.Close(); err == nil {
			err = closeErr
		}
	}()
	if err = directory.Sync(); err != nil {
		return err
	}
	return nil
}
