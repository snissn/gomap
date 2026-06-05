package treedb

import (
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Re-export key option types/constants so callers can configure TreeDB without
// importing internal packages.

type DurabilityMode = db.DurabilityMode

const (
	DurabilityDurable       = db.DurabilityDurable
	DurabilityWALOnRelaxed  = db.DurabilityWALOnRelaxed
	DurabilityWALOffRelaxed = db.DurabilityWALOffRelaxed
)

type IntegrityMode = db.IntegrityMode

const (
	IntegrityVerify        = db.IntegrityVerify
	IntegritySkipChecksums = db.IntegritySkipChecksums
)

type ValueLogOptions = db.ValueLogOptions
type ValueLogDomainThreshold = db.ValueLogDomainThreshold

type ValueLogCompressionMode = db.ValueLogCompressionMode

const (
	ValueLogCompressionOff   = db.ValueLogCompressionOff
	ValueLogCompressionBlock = db.ValueLogCompressionBlock
	ValueLogCompressionDict  = db.ValueLogCompressionDict
	ValueLogCompressionAuto  = db.ValueLogCompressionAuto
)

type ValueLogBlockCodec = db.ValueLogBlockCodec

const (
	ValueLogBlockSnappy = db.ValueLogBlockSnappy
	ValueLogBlockLZ4    = db.ValueLogBlockLZ4
	ValueLogBlockZSTD   = db.ValueLogBlockZSTD
)

type ValueLogAutoPolicy = db.ValueLogAutoPolicy

const (
	ValueLogAutoThroughput = db.ValueLogAutoThroughput
	ValueLogAutoBalanced   = db.ValueLogAutoBalanced
	ValueLogAutoSize       = db.ValueLogAutoSize
)

type ValueLogDictClassMode = db.ValueLogDictClassMode

const (
	ValueLogDictClassSingle         = db.ValueLogDictClassSingle
	ValueLogDictClassSplitOuterLeaf = db.ValueLogDictClassSplitOuterLeaf
)

type ValueLogGenerationPolicy = db.ValueLogGenerationPolicy
type ValueLogGenerationConfig = db.ValueLogGenerationConfig

const (
	ValueLogGenerationDefault     = db.ValueLogGenerationDefault
	ValueLogGenerationOff         = db.ValueLogGenerationOff
	ValueLogGenerationHotWarmCold = db.ValueLogGenerationHotWarmCold
)

// Value-log compression autotune types (re-exported from internal packages so
// callers can configure Options without importing internal/).

type AutotuneMode = valuelog.AutotuneMode

const (
	AutotuneUnset      = valuelog.AutotuneUnset
	AutotuneOff        = valuelog.AutotuneOff
	AutotuneMedium     = valuelog.AutotuneMedium
	AutotuneAggressive = valuelog.AutotuneAggressive
)

type AutotuneOptions = valuelog.AutotuneOptions

// Dictionary training/lookup helpers for value-log compression.
type TrainConfig = compression.TrainConfig
type DictLookup = valuelog.DictLookup

// Zstd encoder levels (for dict-compressed value-log frames).
type ZSTDEncoderLevel = zstd.EncoderLevel

const (
	ZSTDLevelFastest = zstd.SpeedFastest
	ZSTDLevelDefault = zstd.SpeedDefault
	ZSTDLevelBetter  = zstd.SpeedBetterCompression
	ZSTDLevelBest    = zstd.SpeedBestCompression
)
