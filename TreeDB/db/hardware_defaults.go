package db

import "runtime"

const (
	// defaultJournalLaneCoalescingSafeCount is the conservative default total
	// journal/value-log lane count for the hot/warm/cold value-log policy. It
	// leaves one hot lane plus one warm and one cold lane, preserving cached-flush
	// backlog coalescing even when GOMAXPROCS is raised above the host's physical
	// core count. Wider lane layouts remain explicit opt-ins via JournalLanes.
	defaultJournalLaneCoalescingSafeCount = 3
)

// DetectPhysicalCores returns the best-effort number of physical CPU cores for
// the current host. A return value <=0 means the platform did not expose a
// reliable physical-core count. Runtime defaults must then fall back to a safe
// logical-CPU policy.
func DetectPhysicalCores() int {
	return detectPhysicalCoreCount()
}

func runtimeGOMAXPROCS() int {
	gomax := runtime.GOMAXPROCS(0)
	if gomax < 1 {
		gomax = 1
	}
	return gomax
}

func normalizeFlushApplyConcurrencyForGOMAXPROCS(workers, gomax int) int {
	if workers <= 1 {
		return 0
	}
	if gomax < 1 {
		gomax = 1
	}
	if workers > gomax {
		workers = gomax
	}
	if workers <= 1 {
		return 0
	}
	return workers
}

func hardwareAwareDefaultFlushApplyConcurrency(gomax, physicalCores int) int {
	workers := defaultFlushAdmissionAutoConcurrencyCap
	if physicalCores > 0 && physicalCores < workers {
		workers = physicalCores
	}
	return normalizeFlushApplyConcurrencyForGOMAXPROCS(workers, gomax)
}

func autoFlushApplyConcurrencyForHardware(configured, gomax, physicalCores int) int {
	if configured > 0 {
		return normalizeFlushApplyConcurrencyForGOMAXPROCS(configured, gomax)
	}
	return hardwareAwareDefaultFlushApplyConcurrency(gomax, physicalCores)
}

// JournalLaneDefaultDecision describes the lane topology selected for a new
// cached-mode open before recovery may widen it to include already-existing lane
// files.
type JournalLaneDefaultDecision struct {
	Configured       int
	Effective        int
	Defaulted        bool
	GOMAXPROCS       int
	PhysicalCores    int
	GenerationPolicy ValueLogGenerationPolicy
	HotLanes         int
	WarmLanes        int
	ColdLanes        int
}

// ResolveJournalLaneDefaults resolves the default journal/value-log lane count
// for cached mode. The default is intentionally coalescing-safe: hot/warm/cold
// generation uses three total lanes (one hot, one warm, one cold), while
// generation-off cached mode uses one hot lane. Explicit JournalLanes values are
// authoritative and are only classified for reporting.
func ResolveJournalLaneDefaults(configured, gomax, physicalCores int, generationPolicy ValueLogGenerationPolicy) JournalLaneDefaultDecision {
	if gomax < 1 {
		gomax = 1
	}
	decision := JournalLaneDefaultDecision{
		Configured:       configured,
		Defaulted:        configured <= 0,
		GOMAXPROCS:       gomax,
		PhysicalCores:    physicalCores,
		GenerationPolicy: generationPolicy,
	}
	laneCount := configured
	if decision.Defaulted {
		laneCount = defaultJournalLaneCountForPolicy(generationPolicy)
	}
	if laneCount < 1 {
		laneCount = 1
	}
	decision.Effective = laneCount
	decision.HotLanes = laneCount
	if journalLanePolicyUsesHotWarmCold(generationPolicy) && laneCount >= defaultJournalLaneCoalescingSafeCount {
		decision.WarmLanes = 1
		decision.ColdLanes = 1
		decision.HotLanes = laneCount - 2
	}
	return decision
}

func defaultJournalLaneCountForPolicy(generationPolicy ValueLogGenerationPolicy) int {
	if journalLanePolicyUsesHotWarmCold(generationPolicy) {
		return defaultJournalLaneCoalescingSafeCount
	}
	return 1
}

func journalLanePolicyUsesHotWarmCold(generationPolicy ValueLogGenerationPolicy) bool {
	switch generationPolicy {
	case ValueLogGenerationDefault, ValueLogGenerationHotWarmCold:
		return true
	default:
		return false
	}
}
