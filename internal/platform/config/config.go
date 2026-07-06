package config

// Config is the top-level runtime configuration for every Narad binary.
type Config struct {
	HTTP     HTTPConfig     `json:"http"`
	Cluster  ClusterConfig  `json:"cluster"`
	Storage  StorageConfig  `json:"storage"`
	Topic    TopicConfig    `json:"topic"`
	Log      LogConfig      `json:"log"`
	Security SecurityConfig `json:"security"`
}
