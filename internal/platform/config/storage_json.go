package config

import (
	"encoding/json"
	"fmt"
)

// configurableStorageKeys are the only storage fields an operator may set in
// the config file. Where data lives, the compression tradeoff, and the ingress
// WAL shard count (a commit-throughput scaling knob, like partition count) are
// legitimate per-deployment choices; everything else (flush/fsync cadence,
// segment sizing) is an engine internal with production defaults and stays
// locked.
var configurableStorageKeys = map[string]bool{
	"data_dir":           true,
	"codec":              true,
	"compression_level":  true,
	"ingress_wal_shards": true,
}

// UnmarshalJSON keeps file-based storage configuration intentionally narrow:
// only the keys in configurableStorageKeys are accepted; any other storage
// field is rejected so internals are not tuned per deployment. Accepted
// values are still range-checked by Validate (e.g. codec ∈ {zstd,none}).
func (c *StorageConfig) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for key := range raw {
		if !configurableStorageKeys[key] {
			return fmt.Errorf("storage.%s is an internal setting and cannot be configured", key)
		}
	}

	var fileConfig struct {
		DataDir          string `json:"data_dir"`
		Codec            string `json:"codec"`
		CompressionLevel string `json:"compression_level"`
		IngressWALShards int    `json:"ingress_wal_shards"`
	}
	if err := json.Unmarshal(data, &fileConfig); err != nil {
		return err
	}
	if fileConfig.DataDir != "" {
		c.DataDir = fileConfig.DataDir
	}
	if fileConfig.Codec != "" {
		c.Codec = fileConfig.Codec
	}
	if fileConfig.CompressionLevel != "" {
		c.CompressionLevel = fileConfig.CompressionLevel
	}
	if fileConfig.IngressWALShards > 0 {
		c.IngressWALShards = fileConfig.IngressWALShards
	}
	return nil
}
