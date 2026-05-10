package config

// WorkerConfig governs the background worker (replication driver, etc.).
type WorkerConfig struct {
	Enabled bool `json:"enabled"`
}
