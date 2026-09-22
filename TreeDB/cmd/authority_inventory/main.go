// Command authority_inventory renders the typed root-publication authority
// inventory into its reviewable specification table.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/authorityinventory"
)

func main() {
	if err := authorityinventory.Validate(authorityinventory.Rows); err != nil {
		fatal(err)
	}
	path := filepath.Join("..", "..", "docs", "spec", "authority-inventory.md")
	if err := os.WriteFile(path, authorityinventory.RenderMarkdown(authorityinventory.Rows), 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
