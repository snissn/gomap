// treedb_text_ingest_qual validates retained pure-text qualification artifacts.
package main

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
)

func main() {
	var manifestPath, reportPath, produceDir, produceModeDir, produceModeName string
	var produceScale, repetition int
	flag.StringVar(&manifestPath, "manifest", "", "path to manifest.json")
	flag.StringVar(&reportPath, "report", "", "path to report.json")
	flag.StringVar(&produceDir, "produce-smoke", "", "directory for real raw rows for all modes")
	flag.StringVar(&produceModeName, "produce-mode", "", "internal: produce one mode in a fresh child process")
	flag.StringVar(&produceModeDir, "produce-dir", "", "internal: raw-row directory for -produce-mode")
	flag.IntVar(&produceScale, "scale", 10_000, "source documents for producer modes")
	flag.IntVar(&repetition, "repetition", 1, "retained repetition number for producer modes")
	flag.Parse()
	if produceModeName != "" {
		if produceModeDir == "" {
			fmt.Fprintln(os.Stderr, "-produce-mode requires -produce-dir")
			os.Exit(2)
		}
		if err := produceOneMode(produceModeDir, produceModeName, produceScale, repetition); err != nil {
			fmt.Fprintf(os.Stderr, "produce mode: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if produceDir != "" {
		if err := produceSmoke(produceDir, produceScale, repetition); err != nil {
			fmt.Fprintf(os.Stderr, "produce smoke: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote raw smoke rows to %s (not a retained report)\n", produceDir)
		return
	}
	if manifestPath == "" || reportPath == "" {
		fmt.Fprintln(os.Stderr, "usage: treedb_text_ingest_qual -manifest manifest.json -report report.json (run inside the candidate Git checkout)")
		os.Exit(2)
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read manifest: %v\n", err)
		os.Exit(1)
	}
	var manifest manifest
	if err := decodeStrictJSON(manifestBytes, &manifest); err != nil {
		fmt.Fprintf(os.Stderr, "decode manifest: %v\n", err)
		os.Exit(1)
	}
	reportBytes, err := os.ReadFile(reportPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read report: %v\n", err)
		os.Exit(1)
	}
	var report report
	if err := decodeStrictJSON(reportBytes, &report); err != nil {
		fmt.Fprintf(os.Stderr, "decode report: %v\n", err)
		os.Exit(1)
	}
	sum := sha256.Sum256(manifestBytes)
	if err := validate(manifest, report, hex.EncodeToString(sum[:])); err != nil {
		fmt.Fprintf(os.Stderr, "invalid qualification artifact: %v\n", err)
		os.Exit(1)
	}
	if err := verifyRawRows(manifest, report, filepath.Dir(manifestPath)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid qualification raw evidence: %v\n", err)
		os.Exit(1)
	}
	if err := verifyFrozenBaseline(manifest, filepath.Dir(manifestPath)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid frozen baseline evidence: %v\n", err)
		os.Exit(1)
	}
	if err := verifyGitProvenance(manifest, gitBlobOID(manifestBytes), resolveLocalGit); err != nil {
		fmt.Fprintf(os.Stderr, "invalid qualification artifact Git provenance: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("valid pure-text qualification artifact")
}

func decodeStrictJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

type gitResolver func(args ...string) (string, error)

func resolveLocalGit(args ...string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(output)), nil
}

func verifyRawRows(m manifest, r report, artifactDir string) error {
	for _, candidate := range r.Rows {
		relativePath, err := rawRowRelativePath(candidate)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(filepath.Join(artifactDir, filepath.FromSlash(relativePath)))
		if err != nil {
			return fmt.Errorf("read %s: %w", relativePath, err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != m.RawRowsSHA256[relativePath] {
			return fmt.Errorf("%s digest does not match anchored manifest", relativePath)
		}
		var decoded row
		if err := decodeStrictJSON(raw, &decoded); err != nil {
			return fmt.Errorf("decode %s: %w", relativePath, err)
		}
		if !reflect.DeepEqual(decoded, candidate) {
			return fmt.Errorf("%s does not match its report row", relativePath)
		}
	}
	return nil
}

func verifyFrozenBaseline(m manifest, artifactDir string) error {
	const relativePath = "smoke-10k-r3/source_chunk.raw.json"
	raw, err := os.ReadFile(filepath.Join(artifactDir, filepath.FromSlash(relativePath)))
	if err != nil {
		return fmt.Errorf("read %s: %w", relativePath, err)
	}
	sum := sha256.Sum256(raw)
	acceptance := m.Acceptance.SourceChunk10K
	if got := hex.EncodeToString(sum[:]); got != acceptance.FrozenBaselineRowSHA256 {
		return fmt.Errorf("%s digest does not match acceptance baseline", relativePath)
	}
	var baseline struct {
		Mode                  string  `json:"mode"`
		SourceDocuments       int     `json:"source_documents"`
		WallSeconds           float64 `json:"wall_seconds"`
		PeakRSSBytes          metric  `json:"peak_rss_bytes"`
		CumulativeAllocations metric  `json:"cumulative_allocations"`
		Storage               storage `json:"storage"`
	}
	if err := json.Unmarshal(raw, &baseline); err != nil {
		return fmt.Errorf("decode %s: %w", relativePath, err)
	}
	if baseline.Mode != "source_chunk" ||
		baseline.SourceDocuments != acceptance.BaselineSourceDocuments ||
		baseline.WallSeconds != acceptance.BaselineWallSeconds ||
		baseline.PeakRSSBytes.State != "observed" || int64(baseline.PeakRSSBytes.Value) != acceptance.BaselinePeakRSSBytes ||
		baseline.CumulativeAllocations.State != "observed" || int64(baseline.CumulativeAllocations.Value) != acceptance.BaselineCumulativeAllocations ||
		baseline.Storage.PhysicalTotalWALExcludedBytes != acceptance.BaselinePhysicalTotalWALExcludedBytes {
		return fmt.Errorf("%s measurements do not match acceptance baseline", relativePath)
	}
	return nil
}

func gitBlobOID(data []byte) string {
	h := sha1.New()
	_, _ = fmt.Fprintf(h, "blob %d\x00", len(data))
	_, _ = h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func verifyGitProvenance(m manifest, manifestBlobOID string, resolve gitResolver) error {
	objectType, err := resolve("cat-file", "-t", m.Commit)
	if err != nil {
		return fmt.Errorf("resolve measured commit: %w", err)
	}
	if objectType != "commit" {
		return fmt.Errorf("measured commit object has type %q, want commit", objectType)
	}
	measuredTree, err := resolve("rev-parse", "--verify", m.Commit+"^{tree}")
	if err != nil {
		return fmt.Errorf("resolve measured commit tree: %w", err)
	}
	if measuredTree != m.TreeOID {
		return fmt.Errorf("measured commit tree is %s, want %s", measuredTree, m.TreeOID)
	}
	measuredTreeDB, err := resolve("rev-parse", "--verify", m.TreeOID+":"+qualificationTreeDBPath)
	if err != nil {
		return fmt.Errorf("resolve measured TreeDB subtree: %w", err)
	}
	if measuredTreeDB != m.TreeDBSubtreeOID {
		return fmt.Errorf("measured TreeDB subtree is %s, want %s", measuredTreeDB, m.TreeDBSubtreeOID)
	}
	measuredHarness, err := resolve("rev-parse", "--verify", m.TreeOID+":"+qualificationHarnessPath)
	if err != nil {
		return fmt.Errorf("resolve measured qualification harness subtree: %w", err)
	}
	if measuredHarness != m.QualificationHarnessSubtreeOID {
		return fmt.Errorf("measured qualification harness subtree is %s, want %s", measuredHarness, m.QualificationHarnessSubtreeOID)
	}
	measuredBlob, err := resolve("rev-parse", "--verify", m.TreeOID+":"+m.ImplementationPath)
	if err != nil {
		return fmt.Errorf("resolve measured implementation path: %w", err)
	}
	if measuredBlob != m.ImplementationBlobOID {
		return fmt.Errorf("measured implementation blob is %s, want %s", measuredBlob, m.ImplementationBlobOID)
	}
	objectType, err = resolve("cat-file", "-t", m.ImplementationBlobOID)
	if err != nil {
		return fmt.Errorf("resolve measured implementation blob: %w", err)
	}
	if objectType != "blob" {
		return fmt.Errorf("measured implementation object has type %q, want blob", objectType)
	}
	candidateTreeDB, err := resolve("rev-parse", "--verify", "HEAD:"+qualificationTreeDBPath)
	if err != nil {
		return fmt.Errorf("resolve candidate HEAD TreeDB subtree: %w", err)
	}
	if candidateTreeDB != m.TreeDBSubtreeOID {
		return fmt.Errorf("candidate HEAD TreeDB subtree is %s, want measured subtree %s", candidateTreeDB, m.TreeDBSubtreeOID)
	}
	candidateHarness, err := resolve("rev-parse", "--verify", "HEAD:"+qualificationHarnessPath)
	if err != nil {
		return fmt.Errorf("resolve candidate HEAD qualification harness subtree: %w", err)
	}
	if candidateHarness != m.QualificationHarnessSubtreeOID {
		return fmt.Errorf("candidate HEAD qualification harness subtree is %s, want measured subtree %s", candidateHarness, m.QualificationHarnessSubtreeOID)
	}
	candidateBlob, err := resolve("rev-parse", "--verify", "HEAD:"+m.ImplementationPath)
	if err != nil {
		return fmt.Errorf("resolve candidate HEAD implementation path: %w", err)
	}
	if candidateBlob != m.ImplementationBlobOID {
		return fmt.Errorf("candidate HEAD implementation blob is %s, want measured blob %s", candidateBlob, m.ImplementationBlobOID)
	}
	candidateManifestBlob, err := resolve("rev-parse", "--verify", "HEAD:"+qualificationManifestPath)
	if err != nil {
		return fmt.Errorf("resolve candidate HEAD qualification manifest: %w", err)
	}
	if candidateManifestBlob != manifestBlobOID {
		return fmt.Errorf("supplied qualification manifest blob is %s, want candidate HEAD blob %s", manifestBlobOID, candidateManifestBlob)
	}
	for _, path := range []string{qualificationTreeDBPath, qualificationHarnessPath} {
		status, err := resolve("status", "--porcelain=v1", "--untracked-files=all", "--", path)
		if err != nil {
			return fmt.Errorf("inspect candidate working tree under %s: %w", path, err)
		}
		if status != "" {
			return fmt.Errorf("candidate working tree has staged or unstaged changes under %s", path)
		}
	}
	return nil
}
