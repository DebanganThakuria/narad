// Command local-cluster-driver drives load and chaos runs against a
// running Narad cluster over HTTP. It is invoked by
// scripts/local-cluster-e2e.sh and scripts/local-cluster-chaos.sh.
package main

import (
	"fmt"
	"os"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "local cluster integration failed:", err)
		os.Exit(1)
	}
}
