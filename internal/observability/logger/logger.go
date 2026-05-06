// Package logger constructs the project's structured logger.
//
// We deliberately stay thin over log/slog: callers receive a
// *slog.Logger and use it directly. The only things this package owns
// are the format (json/text) and level mapping.
package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// New returns a *slog.Logger writing to stdout.
//
//	format: "json" or "text"
//	level:  "debug", "info", "warn", "error"
func New(format, level string) (*slog.Logger, error) {
	return NewWithWriter(os.Stdout, format, level)
}

// NewWithWriter is like New but lets callers redirect output (useful
// for tests).
func NewWithWriter(w io.Writer, format, level string) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{
		Level:     lvl,
		AddSource: true,
	}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("logger: unsupported format %q", format)
	}

	return slog.New(handler), nil
}
