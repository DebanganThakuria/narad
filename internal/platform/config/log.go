package config

// LogConfig governs the structured logger.
type LogConfig struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}
