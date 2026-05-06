package logger

import (
	"fmt"
	"log/slog"
	"strings"
)

// parseLevel maps human strings to slog.Level. "warning" is accepted as
// an alias for "warn".
func parseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logger: unsupported level %q", level)
	}
}
