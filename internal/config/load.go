package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Load builds a Config from the given file path (if non-empty) and then
// overlays environment-variable overrides. An empty path is not an error;
// it means "use defaults plus env".
//
// Load does NOT call Validate. Callers should apply any further overrides
// (typically CLI flags) and then invoke (*Config).Validate() before using
// the result. The intended precedence is:
//
//	defaults  ->  config file  ->  env vars  ->  caller-applied flags
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		if err := loadFile(path, cfg); err != nil {
			return nil, fmt.Errorf("config: load file: %w", err)
		}
	}

	if err := applyEnv(cfg); err != nil {
		return nil, fmt.Errorf("config: apply env: %w", err)
	}

	return cfg, nil
}

func loadFile(path string, cfg *Config) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	return dec.Decode(cfg)
}
