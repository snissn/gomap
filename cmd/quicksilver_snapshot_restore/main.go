// quicksilver_snapshot_restore rebinds only a private, closed fixture copy.
// The collector owns copying, byte equality and the subsequent full oracle.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func restore(ctx context.Context, root string) ([]string, error) {
	if root == "" {
		return nil, fmt.Errorf("missing private fixture directory")
	}
	main := filepath.Join(root, "maindb")
	if _, err := os.Stat(filepath.Join(main, "index.db")); os.IsNotExist(err) {
		if err := backenddb.RebindDurableRootSnapshotLayoutWithContextV1(ctx, root, ""); err != nil {
			return nil, err
		}
		return []string{"backend"}, nil
	} else if err != nil {
		return nil, err
	}
	var stores []string
	// Restore side-store indexes first so the main manifest binds their final
	// identities, using the same ordering as production snapshot restoration.
	for _, name := range []string{"dictdb", "templatedb"} {
		dir := filepath.Join(root, name)
		if _, err := os.Stat(filepath.Join(dir, "index.db")); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		if err := backenddb.RebindDurableRootSnapshotLayoutWithContextV1(ctx, dir, ""); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		stores = append(stores, name)
	}
	if err := backenddb.RebindDurableRootSnapshotLayoutWithContextV1(ctx, main, root); err != nil {
		return nil, err
	}
	return append(stores, "maindb"), nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: quicksilver_snapshot_restore <private-closed-fixture-copy>")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	stores, err := restore(ctx, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"operation": "RebindDurableRootSnapshotLayoutWithContextV1", "rebound": true, "stores": stores,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
