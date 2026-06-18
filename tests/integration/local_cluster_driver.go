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
