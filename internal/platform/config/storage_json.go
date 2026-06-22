package config

import (
	"encoding/json"
	"fmt"
)

// UnmarshalJSON keeps file-based storage configuration intentionally narrow.
// Storage internals remain typed fields because runtime wiring needs defaults,
// but operators only configure where data lives.
func (c *StorageConfig) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for key := range raw {
		if key != "data_dir" {
			return fmt.Errorf("storage.%s is an internal setting and cannot be configured", key)
		}
	}

	var fileConfig struct {
		DataDir string `json:"data_dir"`
	}
	if err := json.Unmarshal(data, &fileConfig); err != nil {
		return err
	}
	if fileConfig.DataDir != "" {
		c.DataDir = fileConfig.DataDir
	}
	return nil
}
