package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/snissn/gomap/kvstore"
)

type DBFactory func(dir string) (kvstore.DB, error)

var (
	dbFactories = make(map[string]DBFactory)
	dbOrder     = []string{}
	dbAliases   = make(map[string]string)
)

func RegisterDB(name string, factory DBFactory) {
	name = normalizeDBNameForLookup(name)
	if _, exists := dbFactories[name]; !exists {
		dbOrder = append(dbOrder, name)
	}
	dbFactories[name] = factory
}

// RegisterHiddenDB registers an explicit -dbs variant outside the default "all" set.
func RegisterHiddenDB(name string, factory DBFactory) {
	name = normalizeDBNameForLookup(name)
	dbFactories[name] = factory
}

func RegisterAlias(alias, target string) {
	alias = normalizeDBNameForLookup(alias)
	target = normalizeDBNameForLookup(target)
	dbAliases[alias] = target
}

func normalizeDBNameForLookup(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func canonicalDBName(name string) string {
	name = normalizeDBNameForLookup(name)
	original := name
	target, isAlias := dbAliases[name]
	if !isAlias {
		return name
	}
	seen := map[string]struct{}{name: {}}
	for {
		name = normalizeDBNameForLookup(target)
		target, isAlias = dbAliases[name]
		if !isAlias {
			return name
		}
		if _, cycle := seen[name]; cycle {
			return original
		}
		seen[name] = struct{}{}
	}
}

func GetDBFactory(name string) (DBFactory, error) {
	name = canonicalDBName(name)
	f, ok := dbFactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown DB: %q", name)
	}
	return f, nil
}

func GetAllDBNames() []string {
	// Return copy of order to ensure stability
	out := make([]string, len(dbOrder))
	copy(out, dbOrder)
	return out
}

func GetRegisteredDBsList() string {
	return strings.Join(dbOrder, ", ")
}

func dbUsageDetails() string {
	defaults := resolveDBs("all", "")
	details := []string{"DBs included in 'all'\n  " + strings.Join(defaults, ", ")}

	var variants []string
	for name := range dbFactories {
		if canonicalDBName(name) == name && !slices.Contains(defaults, name) {
			variants = append(variants, name)
		}
	}
	if len(variants) > 0 {
		slices.Sort(variants)
		details = append(details, "Explicit variants\n  "+strings.Join(variants, "\n  "))
	}

	var aliases []string
	for alias := range dbAliases {
		target := canonicalDBName(alias)
		if _, ok := dbFactories[target]; ok {
			aliases = append(aliases, alias+" -> "+target)
		}
	}
	if len(aliases) > 0 {
		slices.Sort(aliases)
		details = append(details, "Aliases\n  "+strings.Join(aliases, "\n  "))
	}
	return strings.Join(details, "\n")
}

// resolveDBs returns the list of DBs to run based on the comma-separated arg.
// If "all" is present, it includes the default DB set.
// It excludes any DBs listed in excludeArg.
func resolveDBs(arg, excludeArg string) []string {
	requested := parseList(arg)
	excluded := parseList(excludeArg)
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, name := range excluded {
		excludedSet[canonicalDBName(name)] = struct{}{}
	}

	var candidates []string
	if contains(requested, "all") {
		candidates = GetAllDBNames()
	} else {
		candidates = requested
	}

	out := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, name := range candidates {
		name = canonicalDBName(name)
		if _, skip := excludedSet[name]; skip {
			continue
		}
		if _, ok := dbFactories[name]; ok {
			if _, duplicate := seen[name]; duplicate {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, name)
			continue
		}
	}
	return out
}

func init() {
	// Pre-register legacy aliases or ensure order if needed.
	// Actual DBs register themselves in their init() functions.
}
