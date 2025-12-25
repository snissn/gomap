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

func RegisterAlias(alias, target string) {
	dbAliases[alias] = target
}

func GetDBFactory(name string) (DBFactory, error) {
	if target, isAlias := dbAliases[name]; isAlias {
		name = target
	}
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
func resolveDBs(arg string) []string {
	requested := parseList(arg)
	if contains(requested, "all") {
		return GetAllDBNames()
	}

	// Preserve order of registration for the requested ones, or use requested order?
	// Usually requested order is preferred by user.
	// But let's filter to ensure they exist.
	out := make([]string, 0, len(requested))
	for _, name := range requested {
		if _, ok := dbFactories[name]; ok {
			out = append(out, name)
		}
	}
	return out
}

func init() {
	// Pre-register legacy aliases or ensure order if needed.
	// Actual DBs register themselves in their init() functions.
}
