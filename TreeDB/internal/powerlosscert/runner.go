package powerlosscert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	PerformanceReportSchemaVersion = "treedb-power-loss-performance/v1"
	ManifestIDCurrentMain          = "dur-11-current-main"
)

type RunnerConfig struct {
	RepoRoot      string
	InventoryPath string
	PlanPath      string
	OutputRoot    string
	GoBinary      string
}

type PerformanceReport struct {
	SchemaVersion       string  `json:"schema_version"`
	RepositorySHA       string  `json:"repository_sha"`
	Cases               int     `json:"cases"`
	GenerationRuntimeMS int64   `json:"matrix_generation_runtime_ms"`
	ExecutionRuntimeMS  int64   `json:"execution_runtime_ms"`
	CasesPerSecond      float64 `json:"cases_per_second"`
	StableImageBytes    int64   `json:"stable_image_bytes"`
	ArtifactBytes       int64   `json:"artifact_bytes"`
	PeakMemoryBytes     uint64  `json:"peak_memory_bytes"`
	PeakMemoryAvailable bool    `json:"peak_memory_available"`
	Retries             int     `json:"retries"`
	FlakyRetries        int     `json:"flaky_retries"`
	CISplit             string  `json:"ci_scheduled_split"`
}

type RunnerResult struct {
	Bundle           Bundle
	Coverage         CoverageReport
	Selection        SelectionPlan
	Performance      PerformanceReport
	BundleSealSHA256 string
}

type executedCase struct {
	runCase   RunCase
	command   TestCommand
	trace     operationTraceArtifact
	recovery  recoveryTraceArtifact
	metrics   metricsArtifact
	stdout    string
	stderr    string
	exitCode  int
	peakBytes uint64
}

// Run executes an immutable certification plan from a clean exact repository
// SHA, writes one self-contained bundle, and verifies the completed bundle
// before returning. It never retries a failed witness: a rerun must use a new,
// empty output directory so failed-attempt provenance is not overwritten.
func Run(config RunnerConfig) (RunnerResult, error) {
	started := time.Now()
	repoRoot, inventoryPath, planPath, outputRoot, err := resolveRunnerPaths(config)
	if err != nil {
		return RunnerResult{}, err
	}
	inventoryData, err := os.ReadFile(inventoryPath)
	if err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: read risk inventory: %w", err)
	}
	inventory, err := ParseRiskInventory(inventoryData)
	if err != nil {
		return RunnerResult{}, err
	}
	if err := requireCommittedRiskInventory(repoRoot, inventoryData); err != nil {
		return RunnerResult{}, err
	}
	planData, err := os.ReadFile(planPath)
	if err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: read run plan: %w", err)
	}
	plan, err := ParseRunPlan(planData)
	if err != nil {
		return RunnerResult{}, err
	}
	if err := ValidateRunPlan(inventory, plan); err != nil {
		return RunnerResult{}, err
	}
	contractsPath := filepath.Join(repoRoot, "TreeDB", "testdata", "power_loss_witness_contracts.json")
	contractsData, err := os.ReadFile(contractsPath)
	if err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: read committed witness contracts: %w", err)
	}
	contracts, err := ParseWitnessContracts(contractsData)
	if err != nil {
		return RunnerResult{}, err
	}
	if err := ValidateWitnessContracts(plan, contracts); err != nil {
		return RunnerResult{}, err
	}
	if err := requireExactCleanRepository(repoRoot, plan.RepositoryRef, plan.RepositorySHA); err != nil {
		return RunnerResult{}, err
	}
	if err := requireEmptyOutputRoot(outputRoot); err != nil {
		return RunnerResult{}, err
	}
	for _, dir := range []string{outputRoot, filepath.Join(outputRoot, "binaries"), filepath.Join(outputRoot, "evidence"), filepath.Join(outputRoot, "inputs"), filepath.Join(outputRoot, "manifests")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return RunnerResult{}, fmt.Errorf("powerlosscert: create bundle directory: %w", err)
		}
	}
	if err := writeExclusive(filepath.Join(outputRoot, "risk_inventory.json"), inventoryData, 0o600); err != nil {
		return RunnerResult{}, err
	}
	if err := writeJSONExclusive(filepath.Join(outputRoot, "run_plan.json"), plan); err != nil {
		return RunnerResult{}, err
	}
	if err := writeExclusive(filepath.Join(outputRoot, "inputs", "power_loss_witness_contracts.json"), contractsData, 0o600); err != nil {
		return RunnerResult{}, err
	}
	ledgerSource := filepath.Join(repoRoot, "TreeDB", "testdata", "power_loss_counterexamples.json")
	ledgerData, err := os.ReadFile(ledgerSource)
	if err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: read committed counterexample ledger: %w", err)
	}
	if err := writeExclusive(filepath.Join(outputRoot, "inputs", "power_loss_counterexamples.json"), ledgerData, 0o600); err != nil {
		return RunnerResult{}, err
	}

	goBinary := config.GoBinary
	if goBinary == "" {
		goBinary = "go"
	}
	goBinary, err = resolveGoBinary(goBinary)
	if err != nil {
		return RunnerResult{}, err
	}
	binaries, binaryPaths, err := buildTestBinaries(repoRoot, outputRoot, goBinary, plan.Cases)
	if err != nil {
		return RunnerResult{}, err
	}
	if err := requireExactCleanRepository(repoRoot, plan.RepositoryRef, plan.RepositorySHA); err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: repository changed while building exact-SHA binaries: %w", err)
	}
	generationRuntime := time.Since(started)

	executionStarted := time.Now()
	executed := make([]executedCase, 0, len(plan.Cases))
	var peakBytes uint64
	peakAvailable := false
	for _, runCase := range plan.Cases {
		result, err := executeRunCase(outputRoot, plan, runCase, binaries[runCase.Package], binaryPaths[runCase.Package])
		if err != nil {
			return RunnerResult{}, err
		}
		executed = append(executed, result)
		if result.peakBytes > peakBytes {
			peakBytes = result.peakBytes
		}
		peakAvailable = peakAvailable || result.peakBytes > 0
	}
	if err := requireExactCleanRepository(repoRoot, plan.RepositoryRef, plan.RepositorySHA); err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: repository changed during exact-SHA execution: %w", err)
	}
	executionRuntime := time.Since(executionStarted)

	performance := PerformanceReport{
		SchemaVersion:       PerformanceReportSchemaVersion,
		RepositorySHA:       plan.RepositorySHA,
		Cases:               len(executed),
		GenerationRuntimeMS: generationRuntime.Milliseconds(),
		ExecutionRuntimeMS:  executionRuntime.Milliseconds(),
		PeakMemoryBytes:     peakBytes,
		PeakMemoryAvailable: peakAvailable,
		Retries:             0,
		FlakyRetries:        0,
		CISplit:             "pull-request CI runs schema/unit smoke; the retained exact-main modeled matrix is generated explicitly",
	}
	if executionRuntime > 0 {
		performance.CasesPerSecond = float64(len(executed)) / executionRuntime.Seconds()
	}
	for _, result := range executed {
		performance.StableImageBytes += result.metrics.StableImageBytes
	}
	artifactBytes, err := directoryRegularBytes(filepath.Join(outputRoot, "evidence"))
	if err != nil {
		return RunnerResult{}, err
	}
	if artifactBytes > plan.MaxBundleBytes {
		return RunnerResult{}, fmt.Errorf("powerlosscert: evidence bytes=%d exceed frozen bundle limit=%d", artifactBytes, plan.MaxBundleBytes)
	}
	performance.ArtifactBytes = artifactBytes
	if err := writeJSONExclusive(filepath.Join(outputRoot, "performance.json"), performance); err != nil {
		return RunnerResult{}, err
	}
	performanceSHA, err := fileSHA256(filepath.Join(outputRoot, "performance.json"))
	if err != nil {
		return RunnerResult{}, err
	}

	manifest, err := buildChildManifest(plan, outputRoot, binaries, executed, performanceSHA)
	if err != nil {
		return RunnerResult{}, err
	}
	manifestPath := filepath.Join(outputRoot, "manifests", ManifestIDCurrentMain+".json")
	if err := writeJSONExclusive(manifestPath, manifest); err != nil {
		return RunnerResult{}, err
	}
	if err := ValidateBundle(plan.RepositorySHA, inventory, []ChildManifest{manifest}); err != nil {
		return RunnerResult{}, err
	}
	if err := VerifyArtifacts(outputRoot, []ChildManifest{manifest}); err != nil {
		return RunnerResult{}, err
	}
	coverage, err := BuildCoverageReport(inventory, []ChildManifest{manifest})
	if err != nil {
		return RunnerResult{}, err
	}
	selection, err := SelectRepresentativeCases(inventory, []ChildManifest{manifest})
	if err != nil {
		return RunnerResult{}, err
	}
	if err := writeJSONExclusive(filepath.Join(outputRoot, "coverage_report.json"), coverage); err != nil {
		return RunnerResult{}, err
	}
	if err := writeJSONExclusive(filepath.Join(outputRoot, "selection_plan.json"), selection); err != nil {
		return RunnerResult{}, err
	}
	if err := writeSummary(outputRoot, plan, coverage, selection, performance); err != nil {
		return RunnerResult{}, err
	}
	bundleBytes, err := directoryRegularBytes(outputRoot)
	if err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: inspect completed bundle bytes: %w", err)
	}
	if bundleBytes > plan.MaxBundleBytes {
		return RunnerResult{}, fmt.Errorf("powerlosscert: completed bundle bytes=%d exceed frozen limit=%d", bundleBytes, plan.MaxBundleBytes)
	}
	sealSHA, err := WriteBundleSeal(outputRoot, plan.RepositorySHA)
	if err != nil {
		return RunnerResult{}, err
	}
	sealedBundleBytes, err := directoryRegularBytes(outputRoot)
	if err != nil {
		return RunnerResult{}, fmt.Errorf("powerlosscert: inspect sealed bundle bytes: %w", err)
	}
	if sealedBundleBytes > plan.MaxBundleBytes {
		return RunnerResult{}, fmt.Errorf("powerlosscert: sealed bundle bytes=%d exceed frozen limit=%d", sealedBundleBytes, plan.MaxBundleBytes)
	}
	if err := VerifyBundleSeal(outputRoot, plan.RepositorySHA); err != nil {
		return RunnerResult{}, err
	}
	bundle, err := LoadBundle(outputRoot)
	if err != nil {
		return RunnerResult{}, err
	}
	return RunnerResult{Bundle: bundle, Coverage: coverage, Selection: selection, Performance: performance, BundleSealSHA256: sealSHA}, nil
}

func resolveRunnerPaths(config RunnerConfig) (repoRoot, inventoryPath, planPath, outputRoot string, err error) {
	if config.RepoRoot == "" || config.InventoryPath == "" || config.PlanPath == "" || config.OutputRoot == "" {
		return "", "", "", "", fmt.Errorf("powerlosscert: repo root, inventory, plan, and output paths are required")
	}
	paths := []*string{&repoRoot, &inventoryPath, &planPath, &outputRoot}
	values := []string{config.RepoRoot, config.InventoryPath, config.PlanPath, config.OutputRoot}
	for i, value := range values {
		absolute, absErr := filepath.Abs(value)
		if absErr != nil {
			return "", "", "", "", fmt.Errorf("powerlosscert: resolve runner path %q: %w", value, absErr)
		}
		*paths[i] = filepath.Clean(absolute)
	}
	if outputRoot == repoRoot || pathWithin(repoRoot, outputRoot) || pathWithin(outputRoot, repoRoot) {
		return "", "", "", "", fmt.Errorf("powerlosscert: output root %q must be disjoint from the repository", outputRoot)
	}
	return repoRoot, inventoryPath, planPath, outputRoot, nil
}

func requireCommittedRiskInventory(repoRoot string, supplied []byte) error {
	committedPath := filepath.Join(repoRoot, "TreeDB", "testdata", "power_loss_risk_inventory.json")
	committed, err := os.ReadFile(committedPath)
	if err != nil {
		return fmt.Errorf("powerlosscert: read committed risk inventory: %w", err)
	}
	if !bytes.Equal(supplied, committed) {
		return fmt.Errorf("powerlosscert: supplied risk inventory is not byte-identical to the committed exact-SHA contract %q", committedPath)
	}
	return nil
}

func requireExactCleanRepository(repoRoot, repositoryRef, expectedSHA string) error {
	refSHA, err := commandOutput(repoRoot, nil, "git", "rev-parse", "--verify", repositoryRef+"^{commit}")
	if err != nil {
		return err
	}
	if strings.TrimSpace(refSHA) != expectedSHA {
		return fmt.Errorf("powerlosscert: certified ref %s=%s want exact plan SHA=%s", repositoryRef, strings.TrimSpace(refSHA), expectedSHA)
	}
	head, err := commandOutput(repoRoot, nil, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(head) != expectedSHA {
		return fmt.Errorf("powerlosscert: repository HEAD=%s want exact plan SHA=%s", strings.TrimSpace(head), expectedSHA)
	}
	status, err := commandOutput(repoRoot, nil, "git", "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) != "" {
		return fmt.Errorf("powerlosscert: repository has tracked or untracked changes; exact-main evidence requires a clean tree")
	}
	return nil
}

func requireEmptyOutputRoot(root string) error {
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("powerlosscert: inspect output root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("powerlosscert: output root must be a real directory")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("powerlosscert: inspect output root: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("powerlosscert: output root %q is not empty", root)
	}
	return nil
}

func buildTestBinaries(repoRoot, outputRoot, goBinary string, cases []RunCase) (map[string]Artifact, map[string]string, error) {
	packages := make(map[string]bool)
	for _, runCase := range cases {
		packages[runCase.Package] = true
	}
	ordered := make([]string, 0, len(packages))
	for pkg := range packages {
		ordered = append(ordered, pkg)
	}
	sort.Strings(ordered)
	artifacts := make(map[string]Artifact, len(ordered))
	absolutePaths := make(map[string]string, len(ordered))
	for _, pkg := range ordered {
		name := strings.NewReplacer("./", "", "/", "_", "\\", "_").Replace(pkg) + ".test"
		relPath := filepath.ToSlash(filepath.Join("binaries", name))
		absolute := filepath.Join(outputRoot, filepath.FromSlash(relPath))
		output, err := commandOutputWithEnvironment(repoRoot, certificationBuildEnvironment(), goBinary, "test", "-c", "-o", absolute, pkg)
		if err != nil {
			return nil, nil, fmt.Errorf("powerlosscert: build test binary for %s: %w\n%s", pkg, err, output)
		}
		digest, err := fileSHA256(absolute)
		if err != nil {
			return nil, nil, fmt.Errorf("powerlosscert: hash test binary for %s: %w", pkg, err)
		}
		artifacts[pkg] = Artifact{Kind: ArtifactKindTestBinary, Path: relPath, SHA256: digest}
		absolutePaths[pkg] = absolute
	}
	return artifacts, absolutePaths, nil
}

func resolveGoBinary(name string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("powerlosscert: resolve Go tool %q: %w", name, err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("powerlosscert: resolve absolute Go tool path: %w", err)
	}
	version, err := commandOutputWithEnvironment("", certificationBuildEnvironment(), path, "env", "GOVERSION")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(version) != runtime.Version() {
		return "", fmt.Errorf("powerlosscert: build Go version=%q does not match runner Go version=%q", strings.TrimSpace(version), runtime.Version())
	}
	return path, nil
}

func executeRunCase(outputRoot string, plan RunPlan, runCase RunCase, binary Artifact, binaryPath string) (executedCase, error) {
	evidenceDir := filepath.ToSlash(filepath.Join("evidence", runCase.ID))
	args := []string{
		"-test.run", "^" + runCase.TestName + "$",
		"-test.v",
		"-test.count=1",
		"-test.timeout=" + strconv.Itoa(plan.CaseTimeoutSeconds) + "s",
	}
	env := map[string]string{
		"GOWORK":                            "off",
		"TREEDB_POWERLOSS_CASE_ID":          runCase.ID,
		"TREEDB_POWERLOSS_CUT_ID":           runCase.CutID,
		"TREEDB_POWERLOSS_VARIANT_ID":       runCase.VariantID,
		"TREEDB_POWERLOSS_SEED":             strconv.FormatUint(runCase.Seed, 10),
		"TREEDB_POWERLOSS_EXPECT_CUT_POINT": runCase.CutPoint,
		"TREEDB_POWERLOSS_EVIDENCE_DIR":     evidenceDir,
		"TREEDB_POWERLOSS_REOPEN_MODE":      runCase.ReopenMode,
		"TREEDB_POWERLOSS_PROFILE":          runCase.Profile,
	}
	ledgerPath := filepath.Join(outputRoot, "inputs", "power_loss_counterexamples.json")
	if _, err := os.Stat(ledgerPath); err == nil {
		env["TREEDB_POWERLOSS_COUNTEREXAMPLE_LEDGER"] = "inputs/power_loss_counterexamples.json"
	}
	command := TestCommand{BinaryPath: binary.Path, Package: runCase.Package, TestName: runCase.TestName, Args: args, Env: env}
	runtimeEnv := certificationRuntimeEnvironment(env)
	command.Env = runtimeEnv
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(plan.CaseTimeoutSeconds)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = outputRoot
	cmd.Env = environmentList(runtimeEnv)
	stdout := boundedBuffer{limit: plan.MaxCapturedOutputBytes}
	stderr := boundedBuffer{limit: plan.MaxCapturedOutputBytes}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	started := time.Now()
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		exitCode = -1
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
		if ctx.Err() != nil {
			return executedCase{}, fmt.Errorf("powerlosscert: case %q exceeded frozen timeout %s: %w", runCase.ID, time.Duration(plan.CaseTimeoutSeconds)*time.Second, ctx.Err())
		}
		return executedCase{}, fmt.Errorf("powerlosscert: case %q failed with exit=%d: %w\nstdout:\n%s\nstderr:\n%s", runCase.ID, exitCode, err, stdout.String(), stderr.String())
	}
	if stdout.overflow || stderr.overflow || stdout.total+stderr.total > plan.MaxCapturedOutputBytes {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q captured output bytes=%d exceed frozen limit=%d", runCase.ID, stdout.total+stderr.total, plan.MaxCapturedOutputBytes)
	}
	peakBytes, _ := peakRSSBytes(cmd.ProcessState)
	root := filepath.Join(outputRoot, filepath.FromSlash(evidenceDir))
	evidenceBytes, err := directoryRegularBytes(root)
	if err != nil {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q inspect evidence bytes: %w", runCase.ID, err)
	}
	if evidenceBytes > plan.MaxCaseEvidenceBytes {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q evidence bytes=%d exceed frozen limit=%d", runCase.ID, evidenceBytes, plan.MaxCaseEvidenceBytes)
	}
	trace, err := readStrictJSON[operationTraceArtifact](filepath.Join(root, "operation_trace.json"))
	if err != nil {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q operation trace: %w", runCase.ID, err)
	}
	recovery, err := readStrictJSON[recoveryTraceArtifact](filepath.Join(root, "recovery_trace.json"))
	if err != nil {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q recovery trace: %w", runCase.ID, err)
	}
	metrics, err := readStrictJSON[metricsArtifact](filepath.Join(root, "metrics.json"))
	if err != nil {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q metrics: %w", runCase.ID, err)
	}
	if err := validateExecutedRecovery(runCase, trace, recovery); err != nil {
		return executedCase{}, err
	}
	log := commandLogArtifact{
		SchemaVersion: commandLogSchemaVersion,
		RepositorySHA: plan.RepositorySHA,
		BinaryPath:    binary.Path,
		BinarySHA256:  binary.SHA256,
		Package:       runCase.Package,
		TestName:      runCase.TestName,
		Args:          args,
		Env:           runtimeEnv,
		Outcome:       runCase.ExpectedOutcome,
		Completed:     true,
		ExitCode:      exitCode,
		Stdout:        stdout.String(),
		Stderr:        stderr.String(),
	}
	if log.Stdout == "" && log.Stderr == "" {
		log.Stdout = fmt.Sprintf("PASS %s (%s)", runCase.ID, time.Since(started))
	}
	if err := writeJSONExclusive(filepath.Join(root, "command_log.json"), log); err != nil {
		return executedCase{}, err
	}
	evidenceBytes, err = directoryRegularBytes(root)
	if err != nil {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q inspect completed evidence bytes: %w", runCase.ID, err)
	}
	if evidenceBytes > plan.MaxCaseEvidenceBytes {
		return executedCase{}, fmt.Errorf("powerlosscert: case %q completed evidence bytes=%d exceed frozen limit=%d", runCase.ID, evidenceBytes, plan.MaxCaseEvidenceBytes)
	}
	return executedCase{runCase: runCase, command: command, trace: trace, recovery: recovery, metrics: metrics, stdout: stdout.String(), stderr: stderr.String(), exitCode: exitCode, peakBytes: peakBytes}, nil
}

type boundedBuffer struct {
	buffer   bytes.Buffer
	limit    int64
	total    int64
	overflow bool
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	buffer.total += int64(len(data))
	remaining := buffer.limit - int64(buffer.buffer.Len())
	if remaining > 0 {
		writeBytes := int64(len(data))
		if writeBytes > remaining {
			writeBytes = remaining
		}
		_, _ = buffer.buffer.Write(data[:writeBytes])
	}
	if buffer.total > buffer.limit {
		buffer.overflow = true
	}
	return len(data), nil
}

func (buffer *boundedBuffer) String() string {
	return buffer.buffer.String()
}

func certificationBuildEnvironment() []string {
	values := certificationBaseEnvironment()
	values["GOENV"] = "off"
	values["GOFLAGS"] = ""
	values["GOTOOLCHAIN"] = "local"
	values["GOWORK"] = "off"
	return environmentList(values)
}

func certificationRuntimeEnvironment(overrides map[string]string) map[string]string {
	values := certificationBaseEnvironment()
	values["GOENV"] = "off"
	values["GOFLAGS"] = ""
	values["GOTOOLCHAIN"] = "local"
	values["GOWORK"] = "off"
	for name, value := range overrides {
		values[name] = value
	}
	return values
}

func certificationBaseEnvironment() map[string]string {
	values := map[string]string{
		"HOME":   os.TempDir(),
		"LANG":   "C",
		"LC_ALL": "C",
		"PATH":   "/usr/bin:/bin:/usr/sbin:/sbin",
		"TMPDIR": os.TempDir(),
		"TZ":     "UTC",
	}
	if runtime.GOOS == "windows" {
		for _, name := range []string{"COMSPEC", "PATHEXT", "SYSTEMROOT", "TEMP", "TMP", "USERPROFILE", "WINDIR"} {
			if value := os.Getenv(name); value != "" {
				values[name] = value
			}
		}
	}
	return values
}

func environmentList(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for name, value := range values {
		result = append(result, name+"="+value)
	}
	sort.Strings(result)
	return result
}

func validateExecutedRecovery(runCase RunCase, trace operationTraceArtifact, recovery recoveryTraceArtifact) error {
	prefix := fmt.Sprintf("powerlosscert: case %q", runCase.ID)
	if trace.CutID != runCase.CutID || trace.VariantID != runCase.VariantID || trace.Seed != strconv.FormatUint(runCase.Seed, 10) || trace.DeclaredCutPoint != runCase.CutPoint {
		return fmt.Errorf("%s operation trace does not match the frozen replay selector", prefix)
	}
	_, occurrence, err := parseCutAddress(runCase.CutID)
	if err != nil {
		return err
	}
	if trace.ObservedEventCount != occurrence+1 {
		return fmt.Errorf("%s observed events=%d want cut occurrence+1=%d", prefix, trace.ObservedEventCount, occurrence+1)
	}
	wantReadOnly := runCase.ReopenMode == reopenModeReadOnly
	if recovery.ReadOnly != wantReadOnly {
		return fmt.Errorf("%s recovery read_only=%t want=%t", prefix, recovery.ReadOnly, wantReadOnly)
	}
	want := runCase.ExpectedRecovery
	if recovery.Rejected != want.Rejected || recovery.ErrorType != want.ErrorType || recovery.CommitSeq != want.CommitSeq || recovery.AppliedLSN != want.AppliedLSN {
		return fmt.Errorf("%s recovery=(rejected=%t error_type=%q commit=%d applied=%d) want=(rejected=%t error_type=%q commit=%d applied=%d)", prefix, recovery.Rejected, recovery.ErrorType, recovery.CommitSeq, recovery.AppliedLSN, want.Rejected, want.ErrorType, want.CommitSeq, want.AppliedLSN)
	}
	if recovery.Rejected && recovery.Error == "" {
		return fmt.Errorf("%s rejected recovery has no error text", prefix)
	}
	if !recovery.Rejected && recovery.Error != "" {
		return fmt.Errorf("%s accepted recovery has error text %q", prefix, recovery.Error)
	}
	if err := validateRecoveryStats(recovery); err != nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	if got := observedWitnessState(recovery); got != runCase.State {
		return fmt.Errorf("%s observed recovery state=%+v want frozen state=%+v", prefix, got, runCase.State)
	}
	return nil
}

func observedWitnessState(recovery recoveryTraceArtifact) WitnessState {
	if recovery.Rejected {
		const unavailable = "unavailable: public open rejected before state selection"
		return WitnessState{
			RootMetaGeneration: unavailable, FreelistGeneration: unavailable,
			ExternalFrontiers: unavailable, NamespaceGeneration: unavailable,
			WALLineage: unavailable, DurableLSN: unavailable, CleanupPins: unavailable,
		}
	}
	stat := func(key string) string { return recovery.Stats[key] }
	return WitnessState{
		RootMetaGeneration: fmt.Sprintf("selected_slot=%s commit_seq=%s slot0=%s slot1=%s",
			stat("treedb.durable_root.selected_slot"), stat("treedb.durable_root.commit_seq"),
			stat("treedb.durable_root.slot0.commit_seq"), stat("treedb.durable_root.slot1.commit_seq")),
		FreelistGeneration: fmt.Sprintf("generation=%s", stat("treedb.durable_root.freelist.generation")),
		ExternalFrontiers: fmt.Sprintf("stable_image_tree_artifact=stable_image_tree.json manifest_entries=%s",
			stat("treedb.durable_root.manifest.entries")),
		NamespaceGeneration: "stable_image_tree_artifact=stable_image_tree.json",
		WALLineage:          fmt.Sprintf("profile=%s applied_lsn=%s", stat("treedb.profile.resolved"), stat("treedb.applied_command_lsn")),
		DurableLSN: fmt.Sprintf("durable_wal_lsn=%s durable_root_seq=%s",
			stat("treedb.command_wal.durable_wal_lsn"), stat("treedb.durable_root.durable_seq")),
		CleanupPins: fmt.Sprintf("slot0_commit_seq=%s slot1_commit_seq=%s",
			stat("treedb.durable_root.slot0.commit_seq"), stat("treedb.durable_root.slot1.commit_seq")),
	}
}

func buildChildManifest(plan RunPlan, outputRoot string, binaries map[string]Artifact, executed []executedCase, performanceSHA string) (ChildManifest, error) {
	packages := make([]string, 0, len(binaries))
	for pkg := range binaries {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	manifest := ChildManifest{
		SchemaVersion: ChildManifestSchemaVersion,
		ManifestID:    ManifestIDCurrentMain,
		RepositorySHA: plan.RepositorySHA,
		Issue:         plan.Issue,
		PullRequests:  append([]PullRequest(nil), plan.PullRequests...),
		Environment: Environment{
			GoVersion:       runtime.Version(),
			ToolVersion:     plan.ToolVersion,
			OS:              runtime.GOOS,
			Architecture:    runtime.GOARCH,
			FilesystemModel: plan.FilesystemModel,
		},
		ClaimBoundary: plan.ClaimBoundary,
	}
	for _, pkg := range packages {
		manifest.TestBinaries = append(manifest.TestBinaries, binaries[pkg])
	}
	for _, result := range executed {
		runCase := result.runCase
		_, occurrence, err := parseCutAddress(runCase.CutID)
		if err != nil {
			return ChildManifest{}, err
		}
		witness := Witness{
			ID:                     runCase.ID,
			EvidenceTier:           EvidenceTierModeledCrash,
			Profile:                runCase.Profile,
			Acknowledgement:        runCase.Acknowledgement,
			ResourceShapes:         append([]string(nil), runCase.ResourceShapes...),
			DependencyGraph:        runCase.DependencyGraph,
			StorageBoundaries:      append([]string(nil), runCase.StorageBoundaries...),
			WritebackVariant:       runCase.WritebackVariant,
			FailureClasses:         append([]string(nil), runCase.FailureClasses...),
			ExpectedDurableHorizon: runCase.ExpectedDurableHorizon,
			ExpectedOutcome:        runCase.ExpectedOutcome,
			ActualOutcome:          runCase.ExpectedOutcome,
			TypedError:             runCase.ExpectedTypedError,
			State:                  observedWitnessState(result.recovery),
			CounterexampleID:       runCase.CounterexampleID,
			NegativeControlID:      runCase.NegativeControlID,
			Seed:                   runCase.Seed,
			CutID:                  runCase.CutID,
			CutPoint:               runCase.CutPoint,
			CutOccurrence:          occurrence,
			ObservedEventCount:     result.trace.ObservedEventCount,
			Command:                result.command,
			CutExercised:           true,
			ClaimBoundary:          runCase.ClaimBoundary,
		}
		for _, kind := range requiredModeledArtifactKinds {
			name := modeledArtifactNames[kind]
			path := filepath.ToSlash(filepath.Join("evidence", runCase.ID, name))
			digest, err := fileSHA256(filepath.Join(outputRoot, filepath.FromSlash(path)))
			if err != nil {
				return ChildManifest{}, fmt.Errorf("powerlosscert: hash case %q artifact %q: %w", runCase.ID, path, err)
			}
			witness.Artifacts = append(witness.Artifacts, Artifact{Kind: kind, Path: path, SHA256: digest})
		}
		witness.Artifacts = append(witness.Artifacts, Artifact{Kind: ArtifactKindBenchmark, Path: "performance.json", SHA256: performanceSHA})
		manifest.Witnesses = append(manifest.Witnesses, witness)
	}
	return manifest, nil
}

func writeSummary(root string, plan RunPlan, coverage CoverageReport, selection SelectionPlan, performance PerformanceReport) error {
	var summary strings.Builder
	fmt.Fprintf(&summary, "# TreeDB exact-main power-loss certification\n\n")
	fmt.Fprintf(&summary, "- Repository SHA: `%s`\n", plan.RepositorySHA)
	fmt.Fprintf(&summary, "- Modeled cases executed: %d\n", performance.Cases)
	fmt.Fprintf(&summary, "- Representative cases selected: %d\n", len(selection.CaseIDs))
	fmt.Fprintf(&summary, "- Frozen coverage complete: %t\n", coverage.Complete)
	fmt.Fprintf(&summary, "- Execution runtime: %d ms (%.3f cases/s)\n", performance.ExecutionRuntimeMS, performance.CasesPerSecond)
	fmt.Fprintf(&summary, "- Stable-image bytes: %d\n", performance.StableImageBytes)
	fmt.Fprintf(&summary, "- Evidence artifact bytes: %d\n", performance.ArtifactBytes)
	if performance.PeakMemoryAvailable {
		fmt.Fprintf(&summary, "- Peak child memory: %d bytes\n", performance.PeakMemoryBytes)
	} else {
		fmt.Fprintf(&summary, "- Peak child memory: unavailable on this platform\n")
	}
	fmt.Fprintf(&summary, "- Retries/flaky retries: %d/%d\n", performance.Retries, performance.FlakyRetries)
	fmt.Fprintf(&summary, "\nClaim boundary: %s\n", plan.ClaimBoundary)
	return writeExclusive(filepath.Join(root, "summary.md"), []byte(summary.String()), 0o600)
}

func commandOutput(dir string, extraEnv []string, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), extraEnv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("powerlosscert: %s %s: %w", name, strings.Join(args, " "), err)
	}
	return string(output), nil
}

func commandOutputWithEnvironment(dir string, environment []string, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = environment
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("powerlosscert: %s %s: %w", name, strings.Join(args, " "), err)
	}
	return string(output), nil
}

func readStrictJSON[T any](path string) (T, error) {
	var zero T
	data, err := os.ReadFile(path)
	if err != nil {
		return zero, err
	}
	var value T
	if err := decodeStrict(data, &value); err != nil {
		return zero, err
	}
	return value, nil
}

func writeJSONExclusive(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeExclusive(path, append(data, '\n'), 0o600)
}

func writeExclusive(path string, data []byte, mode fs.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return fmt.Errorf("powerlosscert: create %q: %w", path, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func directoryRegularBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("powerlosscert: artifact tree contains non-regular path %q", path)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
