package main

import (
	"fmt"
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
	if _, exists := dbFactories[name]; !exists {
		dbOrder = append(dbOrder, name)
	}
	dbFactories[name] = factory
}

// RegisterHiddenDB registers a DB factory that can be selected explicitly via
// -dbs, but is intentionally omitted from the "all" DB list and from usage
// listings. This is useful for benchmark variants that should not run by
// default.
func RegisterHiddenDB(name string, factory DBFactory) {
	dbFactories[name] = factory
}

func RegisterAlias(alias, target string) {
	dbAliases[alias] = target
}

func canonicalDBName(name string) string {
	if target, isAlias := dbAliases[name]; isAlias {
		return target
	}
	return name
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

// resolveDBs returns the list of DBs to run based on the comma-separated arg.
// If "all" is present, it includes all registered DBs.
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
	for _, name := range candidates {
		name = canonicalDBName(name)
		if _, skip := excludedSet[name]; skip {
			continue
		}
		if _, ok := dbFactories[name]; ok {
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
