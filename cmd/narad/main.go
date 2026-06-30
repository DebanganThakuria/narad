// Command narad is the single Narad binary. It dispatches to the
// requested subcommand:
//
//	narad serve     run the HTTP API server
//	narad client    interact with a running narad serve over HTTP
//	narad version   print build version
//	narad help      print this help (also: -h, --help)
//
// Configuration precedence (lowest to highest):
//
//	defaults  ->  --config file  ->  NARAD_* env vars  ->  CLI flags
//
// Run `narad <subcommand> --help` for per-subcommand flags.
package main

import (
	"fmt"
	"os"
	"sort"
)

// subcommand is the contract every subcommand satisfies.
type subcommand func(args []string) error

// commands is the dispatcher table. Keep sorted alphabetically — the
// help output preserves this order.
var commands = map[string]struct {
	run   subcommand
	short string
}{
	"client":  {runClient, "interact with a running narad serve over HTTP"},
	"serve":   {runServe, "run the HTTP API server (default port 7942)"},
	"version": {runVersion, "print build version and exit"},
}

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "narad:", err)
		os.Exit(1)
	}
}

func dispatch(args []string) error {
	if len(args) == 0 {
		usage(os.Stdout)
		return nil
	}

	switch args[0] {
	case "-h", "--help", "help":
		usage(os.Stdout)
		return nil
	}

	cmd, ok := commands[args[0]]
	if !ok {
		usage(os.Stderr)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
	return cmd.run(args[1:])
}

func usage(w *os.File) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  narad <subcommand> [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Subcommands:")

	// Derive names from the dispatcher table and sort them so the help
	// output stays stable and can never drift from the command set.
	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		c := commands[n]
		fmt.Fprintf(w, "  %-9s  %s\n", n, c.short)
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run `narad <subcommand> --help` for per-subcommand flags.")
}
