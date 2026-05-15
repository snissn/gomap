//go:build !tidesdb

package main

import (
	"fmt"

	"github.com/snissn/gomap/kvstore"
)

func init() {
	RegisterDB("tidesdb", NewTidesDB)
}

func NewTidesDB(_ string) (kvstore.DB, error) {
	return nil, fmt.Errorf("tidesdb adapter requires build tag 'tidesdb' and github.com/tidesdb/tidesdb-go")
}
