package metastore

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
)

// NewRaftLogWriter returns an io.Writer for Config.Logger that forwards
// hashicorp/raft's log lines (and the Raft transport's) to log as
// structured records under component=raft, at the level Raft chose.
// Without it those lines are dropped, and with them the only account of
// elections, snapshot installs, and a peer that cannot complete the
// mutual-TLS handshake: an operator whose node could not join because
// its certificate was signed by the wrong CA would otherwise see nothing
// but a readiness probe that never turns green.
//
// Raft writes through hclog in its plain format:
//
//	2026-09-06T10:00:00.000+0530 [INFO]  raft: Installed remote snapshot
//
// The timestamp is dropped (slog stamps its own), the bracketed level is
// mapped, and the rest of the line becomes the message, key=value pairs
// included. Lines may arrive in fragments, so the writer buffers up to
// the newline.
func NewRaftLogWriter(log *slog.Logger) io.Writer {
	return &raftLogWriter{log: log.With("component", "raft")}
}

type raftLogWriter struct {
	log *slog.Logger
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *raftLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			// Partial line: put it back and wait for the rest.
			w.buf.Reset()
			w.buf.WriteString(line)
			break
		}
		w.emit(strings.TrimRight(line, "\r\n"))
	}
	return len(p), nil
}

// emit logs one complete hclog line.
func (w *raftLogWriter) emit(line string) {
	level, msg := parseHCLogLine(line)
	if strings.TrimSpace(msg) == "" {
		return
	}
	w.log.Log(context.Background(), level, msg)
}

// parseHCLogLine splits an hclog plain-format line into its level and
// the message that follows it. A line without a recognised level is
// logged verbatim at info.
func parseHCLogLine(line string) (slog.Level, string) {
	open := strings.Index(line, "[")
	closeIdx := strings.Index(line, "]")
	if open < 0 || closeIdx < open {
		return slog.LevelInfo, strings.TrimSpace(line)
	}
	msg := strings.TrimSpace(line[closeIdx+1:])
	switch strings.ToUpper(line[open+1 : closeIdx]) {
	case "TRACE", "DEBUG":
		return slog.LevelDebug, msg
	case "INFO":
		return slog.LevelInfo, msg
	case "WARN":
		return slog.LevelWarn, msg
	case "ERROR":
		return slog.LevelError, msg
	default:
		return slog.LevelInfo, strings.TrimSpace(line)
	}
}
