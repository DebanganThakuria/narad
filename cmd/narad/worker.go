package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/debanganthakuria/narad/internal/config"
	"github.com/debanganthakuria/narad/internal/observability/logger"
)

func runWorker(args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintln(out, "Usage: narad worker [flags]")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Run the Narad cluster worker. Default cluster port: 7943.")
		fmt.Fprintln(out, "(Replication is stubbed in V1; this binary boots and idles.)")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Flags:")
		fs.PrintDefaults()
	}

	configPath := fs.String("config", "", "path to JSON config file (optional)")
	clusterPort := fs.Int("cluster-port", 0, "cluster listen port (overrides cluster.addr)")
	clusterAddr := fs.String("cluster-addr", "", "cluster listen address (overrides cluster.addr)")
	dataDir := fs.String("data-dir", "", "storage directory (overrides storage.data_dir)")
	logLevel := fs.String("log-level", "", "log level: debug|info|warn|error")
	logFormat := fs.String("log-format", "", "log format: json|text")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	if *clusterPort != 0 {
		cfg.Cluster.Addr = ":" + strconv.Itoa(*clusterPort)
	}
	if *clusterAddr != "" {
		cfg.Cluster.Addr = *clusterAddr
	}
	if *dataDir != "" {
		cfg.Storage.DataDir = *dataDir
	}
	if *logLevel != "" {
		cfg.Log.Level = *logLevel
	}
	if *logFormat != "" {
		cfg.Log.Format = *logFormat
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	log, err := logger.New(cfg.Log.Format, cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("narad worker starting",
		"cluster_addr", cfg.Cluster.Addr,
		"enabled", cfg.Worker.Enabled,
		"version", versionString())
	<-ctx.Done()
	log.Info("narad worker stopped")
	return nil
}
