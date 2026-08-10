package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type localHNSWAttributionInputConfigV1 struct {
	Fixture                 fixtureManifest
	RetainedDB              string
	Descriptor              string
	CalibrationSplit        string
	HoldoutSplit            string
	TruthArtifact           string
	HistoricalSearchReports [3]string
	DescriptorSHA256        string
	CalibrationSplitSHA256  string
	HoldoutSplitSHA256      string
	TruthArtifactSHA256     string
	HistoricalReportSHA256  [3]string
}

type localHNSWAttributionInputBundleV1 struct {
	Config      localHNSWAttributionInputConfigV1
	Calibration localHNSWQuerySplitV1
	Holdout     localHNSWQuerySplitV1
}

func localHNSWAttributionInputsV1(cfg localHNSWAttributionInputConfigV1) (localHNSWAttributionInputBundleV1, error) {
	if cfg.Fixture.Queries <= 0 || !localHNSWAttributionSHA256V1(cfg.Fixture.Checksum) ||
		!localHNSWAttributionSHA256V1(cfg.DescriptorSHA256) ||
		!localHNSWAttributionSHA256V1(cfg.CalibrationSplitSHA256) ||
		!localHNSWAttributionSHA256V1(cfg.HoldoutSplitSHA256) ||
		!localHNSWAttributionSHA256V1(cfg.TruthArtifactSHA256) {
		return localHNSWAttributionInputBundleV1{}, errors.New("invalid local HNSW attribution input identity")
	}
	for _, digest := range cfg.HistoricalReportSHA256 {
		if !localHNSWAttributionSHA256V1(digest) {
			return localHNSWAttributionInputBundleV1{}, errors.New("invalid local HNSW attribution report identity")
		}
	}
	info, err := os.Lstat(cfg.RetainedDB)
	if err != nil {
		return localHNSWAttributionInputBundleV1{}, fmt.Errorf("invalid retained local HNSW database: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return localHNSWAttributionInputBundleV1{}, errors.New("invalid retained local HNSW database")
	}
	if err := localHNSWAttributionMatchFileSHA256V1(cfg.CalibrationSplit, localHNSWQuerySplitMaxBytesV1, cfg.CalibrationSplitSHA256); err != nil {
		return localHNSWAttributionInputBundleV1{}, fmt.Errorf("local HNSW calibration split: %w", err)
	}
	calibration, calibrationSHA, err := loadLocalHNSWQuerySplitV1(cfg.CalibrationSplit)
	if err != nil {
		return localHNSWAttributionInputBundleV1{}, fmt.Errorf("local HNSW calibration split: %w", err)
	}
	if calibrationSHA != cfg.CalibrationSplitSHA256 {
		return localHNSWAttributionInputBundleV1{}, errors.New("invalid local HNSW calibration split")
	}
	if err := localHNSWAttributionMatchFileSHA256V1(cfg.HoldoutSplit, localHNSWQuerySplitMaxBytesV1, cfg.HoldoutSplitSHA256); err != nil {
		return localHNSWAttributionInputBundleV1{}, fmt.Errorf("local HNSW holdout split: %w", err)
	}
	holdout, holdoutSHA, err := loadLocalHNSWQuerySplitV1(cfg.HoldoutSplit)
	if err != nil {
		return localHNSWAttributionInputBundleV1{}, fmt.Errorf("local HNSW holdout split: %w", err)
	}
	if holdoutSHA != cfg.HoldoutSplitSHA256 {
		return localHNSWAttributionInputBundleV1{}, errors.New("invalid local HNSW holdout split")
	}
	if err := validateLocalHNSWQuerySplitPairV1(calibration, holdout, cfg.Fixture, cfg.TruthArtifactSHA256); err != nil {
		return localHNSWAttributionInputBundleV1{}, err
	}
	if err := localHNSWAttributionMatchFileSHA256V1(cfg.Descriptor, m3VariantDescriptorMaxBytesV1, cfg.DescriptorSHA256); err != nil {
		return localHNSWAttributionInputBundleV1{}, fmt.Errorf("local HNSW descriptor: %w", err)
	}
	if err := localHNSWAttributionMatchFileSHA256V1(cfg.TruthArtifact, m8ProfileArtifactMaxBytesV1, cfg.TruthArtifactSHA256); err != nil {
		return localHNSWAttributionInputBundleV1{}, fmt.Errorf("local HNSW truth artifact: %w", err)
	}
	for i, path := range cfg.HistoricalSearchReports {
		if err := localHNSWAttributionMatchFileSHA256V1(path, m8ProfileArtifactMaxBytesV1, cfg.HistoricalReportSHA256[i]); err != nil {
			return localHNSWAttributionInputBundleV1{}, fmt.Errorf("local HNSW historical report %d: %w", i, err)
		}
	}
	return localHNSWAttributionInputBundleV1{Config: cfg, Calibration: calibration, Holdout: holdout}, nil
}

func localHNSWAttributionMatchFileSHA256V1(path string, maxBytes int64, want string) error {
	got, err := localHNSWAttributionRegularFileSHA256V1(path, maxBytes)
	if err != nil {
		return fmt.Errorf("invalid local HNSW attribution input file %q: %w", path, err)
	}
	if got != want {
		return fmt.Errorf("invalid local HNSW attribution input file %q: sha256 mismatch", path)
	}
	return nil
}

func localHNSWAttributionRegularFileSHA256V1(path string, maxBytes int64) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || maxBytes < 0 || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxBytes {
		return "", errors.New("invalid local HNSW attribution input file")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxBytes {
		return "", errors.New("invalid local HNSW attribution input file")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, maxBytes+1))
	if err != nil || n > maxBytes {
		return "", errors.New("invalid local HNSW attribution input file")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func localHNSWAttributionSHA256V1(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
