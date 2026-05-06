package config

import (
	"errors"
	"fmt"
	"strings"
)

// Validate enforces invariants the rest of the system relies on. Callers
// should fail fast on a Validate error — Narad refuses to start with bad
// config rather than coping at runtime.
func (c *Config) Validate() error {
	var errs []string

	if strings.TrimSpace(c.HTTP.Addr) == "" {
		errs = append(errs, "http.addr must not be empty")
	}
	if c.HTTP.ReadTimeout <= 0 {
		errs = append(errs, "http.read_timeout must be > 0")
	}
	if c.HTTP.WriteTimeout <= 0 {
		errs = append(errs, "http.write_timeout must be > 0")
	}
	if c.HTTP.IdleTimeout <= 0 {
		errs = append(errs, "http.idle_timeout must be > 0")
	}
	if c.HTTP.ShutdownGrace <= 0 {
		errs = append(errs, "http.shutdown_grace must be > 0")
	}
	if c.HTTP.MaxConsumeWait < 0 {
		errs = append(errs, "http.max_consume_wait must be >= 0")
	}

	if strings.TrimSpace(c.Cluster.Addr) == "" {
		errs = append(errs, "cluster.addr must not be empty")
	}
	if c.HTTP.Addr == c.Cluster.Addr {
		errs = append(errs, "http.addr and cluster.addr must differ")
	}

	if strings.TrimSpace(c.Storage.DataDir) == "" {
		errs = append(errs, "storage.data_dir must not be empty")
	}
	switch c.Storage.Fsync {
	case FsyncPerWrite, FsyncBatched:
	default:
		errs = append(errs, fmt.Sprintf("storage.fsync %q is not one of [per_write, batched]", c.Storage.Fsync))
	}

	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("log.level %q is not one of [debug, info, warn, error]", c.Log.Level))
	}
	switch strings.ToLower(c.Log.Format) {
	case "json", "text":
	default:
		errs = append(errs, fmt.Sprintf("log.format %q is not one of [json, text]", c.Log.Format))
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New("config: " + strings.Join(errs, "; "))
}
