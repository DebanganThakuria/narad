// Command narad is the single Narad binary: the server and its full
// CLI in one artifact. Run `narad --help` for the command tree.
//
// Configuration precedence for the server (lowest to highest):
//
//	defaults  ->  --config file  ->  NARAD_* env vars  ->  CLI flags
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := route(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "narad:", err)
		os.Exit(1)
	}
}

// route hands everything to the cobra tree. Kept as a seam so tests
// drive the CLI exactly as main does.
func route(args []string) error {
	return runCobra(args)
}
