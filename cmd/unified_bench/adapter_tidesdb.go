package main

import (
	"fmt"

	"github.com/snissn/gomap/kvstore"
)

func init() {
	RegisterDB("tidesdb", NewTidesDB)
}

// NewTidesDB returns an actionable error in builds where the optional
// github.com/tidesdb/tidesdb-go dependency is not compiled in.
//
// To enable real TidesDB support, add a companion implementation file that
// imports tidesdb-go and constructs a kvstore.DB wrapper.
func NewTidesDB(_ string) (kvstore.DB, error) {
	return nil, fmt.Errorf("tidesdb adapter is not available in this build")
}
