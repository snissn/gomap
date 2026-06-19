package collections

import (
	"fmt"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// ScalarU8CalibrationMode selects the scalar_u8 v1 calibration contract.
// Omitted scalar_u8 calibration config is equivalent to legacy mode for
// behavior and asset identity.
type ScalarU8CalibrationMode string

const (
	// ScalarU8CalibrationModeLegacy preserves the original scalar_u8 v1 contract:
	// one normalized uint8 code per vector dimension and no alpha side asset.
	ScalarU8CalibrationModeLegacy ScalarU8CalibrationMode = "legacy"
	// ScalarU8CalibrationModePerGranuleAlpha opts into the scalar_u8 v1
	// per-existing-storage-granule scalar alpha contract. Rebuild persists alpha
	// metadata and encodes rows with their granule alpha; search scoring consumes
	// the matching prepared alpha state and fails closed when it is unavailable.
	ScalarU8CalibrationModePerGranuleAlpha ScalarU8CalibrationMode = "per_granule_alpha"
)

// ScalarU8CalibrationGrouping names the source of groups used by per-granule
// alpha calibration. The contract deliberately uses existing storage/layout
// granules and does not define a vector-specific granule size.
type ScalarU8CalibrationGrouping string

const (
	ScalarU8CalibrationGroupingStorageLayoutGranule ScalarU8CalibrationGrouping = "storage_layout_granule"
)

// ScalarU8AlphaPolicyName selects a deterministic finite alpha policy.
type ScalarU8AlphaPolicyName string

const (
	// ScalarU8AlphaPolicyMaxAbs uses the maximum absolute normalized component in
	// each existing storage/layout granule as that granule's alpha.
	ScalarU8AlphaPolicyMaxAbs ScalarU8AlphaPolicyName = "max_abs"
	// ScalarU8AlphaPolicyAbsQuantile uses a fixed allowed high quantile of absolute
	// normalized components. The quantile parameter is encoded as integer PPM to
	// keep config identity deterministic.
	ScalarU8AlphaPolicyAbsQuantile ScalarU8AlphaPolicyName = "abs_quantile"

	// ScalarU8AlphaPolicyAbsQuantilePPM999 is the only accepted quantile parameter
	// for ScalarU8AlphaPolicyAbsQuantile in the initial contract (0.999000).
	ScalarU8AlphaPolicyAbsQuantilePPM999 uint32 = 999000
)

// ScalarU8AlphaPolicy is the deterministic policy used to derive alpha values.
// QuantilePPM is meaningful only for the abs_quantile policy and is intentionally
// constrained to a finite allowed set.
type ScalarU8AlphaPolicy struct {
	Name        ScalarU8AlphaPolicyName `json:"name,omitempty"`
	QuantilePPM uint32                  `json:"quantile_ppm,omitempty"`
}

// ScalarU8CalibrationConfig configures scalar_u8 calibration. A nil pointer on
// QuantizedVectorIndexDefinition preserves the legacy scalar_u8 v1 config and
// identity. Non-nil configs are normalized and validated with the definition.
type ScalarU8CalibrationConfig struct {
	Mode        ScalarU8CalibrationMode     `json:"mode,omitempty"`
	Grouping    ScalarU8CalibrationGrouping `json:"grouping,omitempty"`
	AlphaPolicy ScalarU8AlphaPolicy         `json:"alpha_policy,omitempty"`
}

// NormalizeScalarU8CalibrationConfig validates and canonicalizes the optional
// scalar_u8 calibration config on q. Nil preserves the legacy scalar_u8 v1
// behavior and empty codec identity; explicit empty/legacy configs normalize to
// legacy mode so callers that send the field get deterministic metadata back.
func NormalizeScalarU8CalibrationConfig(defName string, index int, q QuantizedVectorIndexDefinition) (*ScalarU8CalibrationConfig, error) {
	return normalizeScalarU8CalibrationConfig(defName, index, q)
}

func normalizeScalarU8CalibrationConfig(defName string, index int, q QuantizedVectorIndexDefinition) (*ScalarU8CalibrationConfig, error) {
	if q.ScalarU8Calibration == nil {
		return nil, nil
	}
	if q.Codec == "" {
		q.Codec = QuantizedVectorCodecScalarU8
	}
	if q.Codec != QuantizedVectorCodecScalarU8 {
		return nil, fmt.Errorf("collections: vector index %q quantized index[%d] scalar_u8_calibration requires codec %q", defName, index, QuantizedVectorCodecScalarU8)
	}
	cfg := *q.ScalarU8Calibration
	switch cfg.Mode {
	case "", ScalarU8CalibrationModeLegacy:
		if cfg.Grouping != "" {
			return nil, fmt.Errorf("collections: vector index %q quantized index %q legacy scalar_u8 calibration cannot set grouping %q", defName, q.Name, cfg.Grouping)
		}
		if !scalarU8AlphaPolicyZero(cfg.AlphaPolicy) {
			return nil, fmt.Errorf("collections: vector index %q quantized index %q legacy scalar_u8 calibration cannot set alpha_policy", defName, q.Name)
		}
		cfg.Mode = ScalarU8CalibrationModeLegacy
		return &cfg, nil
	case ScalarU8CalibrationModePerGranuleAlpha:
		if cfg.Grouping == "" {
			cfg.Grouping = ScalarU8CalibrationGroupingStorageLayoutGranule
		}
		if cfg.Grouping != ScalarU8CalibrationGroupingStorageLayoutGranule {
			return nil, fmt.Errorf("collections: vector index %q quantized index %q scalar_u8 per_granule_alpha grouping=%q is unsupported", defName, q.Name, cfg.Grouping)
		}
		policy, err := normalizeScalarU8AlphaPolicy(defName, q.Name, cfg.AlphaPolicy)
		if err != nil {
			return nil, err
		}
		cfg.AlphaPolicy = policy
		return &cfg, nil
	default:
		return nil, fmt.Errorf("collections: vector index %q quantized index %q scalar_u8 calibration mode %q is unsupported", defName, q.Name, cfg.Mode)
	}
}

func normalizeScalarU8AlphaPolicy(defName, qName string, policy ScalarU8AlphaPolicy) (ScalarU8AlphaPolicy, error) {
	switch policy.Name {
	case "", ScalarU8AlphaPolicyMaxAbs:
		if policy.QuantilePPM != 0 {
			return ScalarU8AlphaPolicy{}, fmt.Errorf("collections: vector index %q quantized index %q scalar_u8 alpha_policy max_abs cannot set quantile_ppm=%d", defName, qName, policy.QuantilePPM)
		}
		policy.Name = ScalarU8AlphaPolicyMaxAbs
		return policy, nil
	case ScalarU8AlphaPolicyAbsQuantile:
		if policy.QuantilePPM != ScalarU8AlphaPolicyAbsQuantilePPM999 {
			return ScalarU8AlphaPolicy{}, fmt.Errorf("collections: vector index %q quantized index %q scalar_u8 alpha_policy abs_quantile quantile_ppm=%d is unsupported", defName, qName, policy.QuantilePPM)
		}
		return policy, nil
	default:
		return ScalarU8AlphaPolicy{}, fmt.Errorf("collections: vector index %q quantized index %q scalar_u8 alpha_policy name %q is unsupported", defName, qName, policy.Name)
	}
}

func scalarU8AlphaPolicyZero(policy ScalarU8AlphaPolicy) bool {
	return policy.Name == "" && policy.QuantilePPM == 0
}

func scalarU8CalibrationConfigClone(in *ScalarU8CalibrationConfig) *ScalarU8CalibrationConfig {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func scalarU8CalibrationConfigEqual(a, b *ScalarU8CalibrationConfig) bool {
	if scalarU8CalibrationConfigLegacyEquivalent(a) && scalarU8CalibrationConfigLegacyEquivalent(b) {
		return true
	}
	return scalarU8CalibrationConfigStrictEqual(a, b)
}

func scalarU8CalibrationConfigStrictEqual(a, b *ScalarU8CalibrationConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func scalarU8CalibrationConfigLegacyEquivalent(cfg *ScalarU8CalibrationConfig) bool {
	if cfg == nil {
		return true
	}
	return (cfg.Mode == "" || cfg.Mode == ScalarU8CalibrationModeLegacy) &&
		cfg.Grouping == "" &&
		scalarU8AlphaPolicyZero(cfg.AlphaPolicy)
}

func scalarU8CalibrationMode(q QuantizedVectorIndexDefinition) ScalarU8CalibrationMode {
	if q.ScalarU8Calibration == nil || q.ScalarU8Calibration.Mode == "" {
		return ScalarU8CalibrationModeLegacy
	}
	return q.ScalarU8Calibration.Mode
}

func scalarU8CalibrationIsLegacy(q QuantizedVectorIndexDefinition) bool {
	return scalarU8CalibrationMode(q) == ScalarU8CalibrationModeLegacy
}

func scalarU8CalibrationCodecConfig(q QuantizedVectorIndexDefinition) ([]byte, uint64, error) {
	if q.Codec == "" {
		q.Codec = QuantizedVectorCodecScalarU8
	}
	if q.Codec != QuantizedVectorCodecScalarU8 {
		return nil, 0, fmt.Errorf("scalar_u8 calibration identity requires codec %q, got %q", QuantizedVectorCodecScalarU8, q.Codec)
	}
	cfg, err := normalizedScalarU8CalibrationConfigForIdentity(q)
	if err != nil {
		return nil, 0, err
	}
	if cfg == nil || cfg.Mode == ScalarU8CalibrationModeLegacy {
		return nil, 0, nil
	}
	buf := make([]byte, 0, 160)
	buf = append(buf, "scalar_u8:v1"...)
	buf = append(buf, ";calibration_mode="...)
	buf = append(buf, string(cfg.Mode)...)
	buf = append(buf, ";grouping="...)
	buf = append(buf, string(cfg.Grouping)...)
	buf = append(buf, ";alpha_policy="...)
	buf = append(buf, string(cfg.AlphaPolicy.Name)...)
	if cfg.AlphaPolicy.QuantilePPM != 0 {
		buf = append(buf, ";alpha_quantile_ppm="...)
		buf = strconv.AppendUint(buf, uint64(cfg.AlphaPolicy.QuantilePPM), 10)
	}
	return buf, xxhash.Sum64(buf), nil
}

func normalizedScalarU8CalibrationConfigForIdentity(q QuantizedVectorIndexDefinition) (*ScalarU8CalibrationConfig, error) {
	if q.ScalarU8Calibration == nil {
		return nil, nil
	}
	return normalizeScalarU8CalibrationConfig("", 0, q)
}

func scalarU8CalibrationConfigHashForAssetID(q QuantizedVectorIndexDefinition) (uint64, error) {
	_, hash, err := scalarU8CalibrationCodecConfig(q)
	if err != nil {
		return 0, err
	}
	return hash, nil
}
