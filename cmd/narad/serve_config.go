package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// serveFlags holds the `narad serve` CLI overrides. Each non-zero field
// wins over the corresponding config-file/env value.
type serveFlags struct {
	configPath  string
	port        int
	addr        string
	clusterPort int
	nodeID      string
	dataDir     string
	logLevel    string
	logFormat   string
	pprofAddr   string
}

func (f *serveFlags) applyTo(cfg *config.Config) {
	if f.port != 0 {
		cfg.HTTP.Addr = ":" + strconv.Itoa(f.port)
	}
	if f.addr != "" {
		cfg.HTTP.Addr = f.addr
	}
	if f.clusterPort != 0 {
		cfg.Cluster.Addr = ":" + strconv.Itoa(f.clusterPort)
	}
	if f.nodeID != "" {
		cfg.Cluster.NodeID = f.nodeID
	}
	if f.dataDir != "" {
		cfg.Storage.DataDir = f.dataDir
	}
	if f.logLevel != "" {
		cfg.Log.Level = f.logLevel
	}
	if f.logFormat != "" {
		cfg.Log.Format = f.logFormat
	}
	if f.pprofAddr != "" {
		cfg.HTTP.PprofAddr = f.pprofAddr
	}
}

// loadServeConfig parses the serve flags, loads the config file (if any),
// applies flag overrides, and validates the result. It returns (nil, nil)
// when -help was requested, so the caller exits cleanly.
func loadServeConfig(args []string) (*config.Config, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintln(out, "Usage: narad serve [flags]")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Run the Narad HTTP API server. Default port: 7942.")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Flags:")
		fs.PrintDefaults()
	}

	var f serveFlags
	fs.StringVar(&f.configPath, "config", "", "path to JSON config file (optional)")
	fs.IntVar(&f.port, "port", 0, "API listen port (overrides http.addr; e.g. --port 7942)")
	fs.StringVar(&f.addr, "addr", "", "API listen address (overrides http.addr; e.g. --addr 0.0.0.0:7942)")
	fs.IntVar(&f.clusterPort, "cluster-port", 0, "cluster listen port (overrides cluster.addr)")
	fs.StringVar(&f.nodeID, "node-id", "", "stable cluster node ID (overrides cluster.node_id)")
	fs.StringVar(&f.dataDir, "data-dir", "", "storage directory (overrides storage.data_dir)")
	fs.StringVar(&f.logLevel, "log-level", "", "log level: debug|info|warn|error (overrides log.level)")
	fs.StringVar(&f.logFormat, "log-format", "", "log format: json|text (overrides log.format)")
	fs.StringVar(&f.pprofAddr, "pprof-addr", "", "enable pprof on this address (e.g. 127.0.0.1:6060); empty disables")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}

	cfg, err := config.Load(f.configPath)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	f.applyTo(cfg)
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// resolveNodeID returns the configured node ID, falling back to the
// hostname so single-node setups need no explicit configuration.
func resolveNodeID(cfg *config.Config) (string, error) {
	if cfg != nil && cfg.Cluster.NodeID != "" {
		return cfg.Cluster.NodeID, nil
	}
	nodeID, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve node id: %w", err)
	}
	return nodeID, nil
}
