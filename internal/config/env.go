package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// applyEnv lets operators override individual fields without rewriting
// the config file. Each entry is intentionally explicit: there's no
// reflection magic — adding a new env override is a one-liner here.
func applyEnv(cfg *Config) error {
	if v, ok := os.LookupEnv("NARAD_HTTP_ADDR"); ok {
		cfg.HTTP.Addr = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_ADDR"); ok {
		cfg.Cluster.Addr = v
	}
	if err := envDuration("NARAD_HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_SHUTDOWN_GRACE", &cfg.HTTP.ShutdownGrace); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_MAX_CONSUME_WAIT", &cfg.HTTP.MaxConsumeWait); err != nil {
		return err
	}

	if v, ok := os.LookupEnv("NARAD_DATA_DIR"); ok {
		cfg.Storage.DataDir = v
	}
	if v, ok := os.LookupEnv("NARAD_FSYNC"); ok {
		cfg.Storage.Fsync = FsyncMode(v)
	}

	if v, ok := os.LookupEnv("NARAD_LOG_LEVEL"); ok {
		cfg.Log.Level = v
	}
	if v, ok := os.LookupEnv("NARAD_LOG_FORMAT"); ok {
		cfg.Log.Format = v
	}

	if v, ok := os.LookupEnv("NARAD_WORKER_ENABLED"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("NARAD_WORKER_ENABLED: %w", err)
		}
		cfg.Worker.Enabled = b
	}

	return nil
}

func envDuration(key string, dst *Duration) error {
	v, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = Duration(d)
	return nil
}
