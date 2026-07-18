package main

// `narad server start` — the friendly wrapper around the serve engine.
// --dev is the demo/laptop mode: loopback bind, auth off, data under
// ~/.narad, and a quickstart banner. Without --dev it is a plain
// passthrough to the production serve path (secure by default).

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func newServerCmd() *cobra.Command {
	server := &cobra.Command{
		Use:   "server",
		Short: "run and inspect a Narad server",
	}

	var (
		dev     bool
		port    int
		dataDir string
	)
	start := &cobra.Command{
		Use:   "start",
		Short: "start a broker node (--dev for a local, auth-off playground)",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			args := []string{}
			if dev {
				dir := dataDir
				if dir == "" {
					home, err := os.UserHomeDir()
					if err != nil {
						return err
					}
					dir = filepath.Join(home, ".narad", "data")
				}
				// Dev mode is explicitly a playground: plaintext HTTP on
				// loopback only, security off. The production path (no
				// --dev) keeps every secure default.
				if err := os.Setenv("NARAD_SECURITY_ENABLED", "false"); err != nil {
					return err
				}
				// Raft refuses an unspecified advertise address, so pin
				// the cluster transport to loopback alongside the API.
				if os.Getenv("NARAD_CLUSTER_ADDR") == "" {
					if err := os.Setenv("NARAD_CLUSTER_ADDR", fmt.Sprintf("127.0.0.1:%d", port+1)); err != nil {
						return err
					}
				}
				args = append(args,
					"--addr", fmt.Sprintf("127.0.0.1:%d", port),
					"--data-dir", dir,
					"--log-format", "text",
				)
				banner(port)
			} else {
				if dataDir != "" {
					args = append(args, "--data-dir", dataDir)
				}
				if port != 7942 {
					args = append(args, "--port", fmt.Sprintf("%d", port))
				}
			}
			return runServe(args)
		},
	}
	start.Flags().BoolVar(&dev, "dev", false, "local playground: loopback bind, auth OFF, data in ~/.narad/data")
	start.Flags().IntVar(&port, "port", 7942, "API port")
	start.Flags().StringVar(&dataDir, "data-dir", "", "storage directory (dev default: ~/.narad/data)")
	return append1(server, start)
}

func append1(parent *cobra.Command, child *cobra.Command) *cobra.Command {
	parent.AddCommand(child)
	return parent
}

func banner(port int) {
	fmt.Fprintf(os.Stderr, `
  %s  dev mode: auth OFF, bound to loopback only

  Try it from another terminal:

    narad topic add demo
    narad sub demo --peek        # terminal B: watch messages flow
    narad pub demo '{"hello":"narad"}' --count 100 --rate 20

  Or plain curl:

    curl -X POST 'http://127.0.0.1:%d/v1/topics' -d '{"name":"demo"}'

`, bold("narad"), port)
}
