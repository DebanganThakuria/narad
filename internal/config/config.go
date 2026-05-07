// Package config defines Narad's runtime configuration and the rules for
// loading it from disk and environment.
//
// Load order: defaults -> JSON file (if a path is supplied) -> environment
// overrides. Callers then apply any CLI-flag overrides and call
// (*Config).Validate() before using the result.
package config

// Config is the top-level runtime configuration for every Narad binary.
type Config struct {
	HTTP    HTTPConfig    `json:"http"`
	Cluster ClusterConfig `json:"cluster"`
	Storage StorageConfig `json:"storage"`
	Topic   TopicConfig   `json:"topic"`
	Log     LogConfig     `json:"log"`
	Worker  WorkerConfig  `json:"worker"`
}
