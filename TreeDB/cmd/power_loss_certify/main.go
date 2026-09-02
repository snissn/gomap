// Command power_loss_certify executes and verifies a frozen exact-SHA TreeDB
// power-loss certification plan.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/snissn/gomap/TreeDB/internal/powerlosscert"
)

func main() {
	var config powerlosscert.RunnerConfig
	flag.StringVar(&config.RepoRoot, "repo-root", ".", "clean repository root at the run plan's exact SHA")
	flag.StringVar(&config.InventoryPath, "inventory", "", "frozen risk inventory JSON")
	flag.StringVar(&config.PlanPath, "plan", "", "frozen exact-SHA run plan JSON")
	flag.StringVar(&config.OutputRoot, "out", "", "new or empty output directory")
	flag.StringVar(&config.GoBinary, "go", "go", "Go toolchain executable")
	flag.Parse()

	result, err := powerlosscert.Run(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("verified exact-main power-loss bundle: sha=%s seal_sha256=%s cases=%d selected=%d output=%s\n",
		result.Bundle.Manifests[0].RepositorySHA,
		result.BundleSealSHA256,
		result.Performance.Cases,
		len(result.Selection.CaseIDs),
		result.Bundle.Root,
	)
}
