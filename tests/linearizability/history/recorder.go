package history

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Recorder appends records to a JSONL file.
//
// Every method is safe on a nil Recorder, so a caller records
// unconditionally and paying for a history stays a matter of passing a
// path.
type Recorder struct {
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	encoder *json.Encoder
	err     error
}

// NewRecorder opens path for writing, truncating any existing file. A
// path of "" returns a nil Recorder, which records nothing.
func NewRecorder(path string) (*Recorder, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create history %s: %w", path, err)
	}
	// 1 MiB. The hot path holds a mutex across the write, so the point of
	// the buffer is to make almost every record a memcpy and leave the
	// syscalls a few thousand records apart.
	writer := bufio.NewWriterSize(file, 1<<20)
	return &Recorder{
		file:    file,
		writer:  writer,
		encoder: json.NewEncoder(writer),
	}, nil
}

// Record appends one record.
//
// A write error is latched and reported by Close rather than returned
// here. The caller is a load generator whose job is to keep load
// flowing, and a truncated history is better diagnosed at the end than
// by aborting the run that produced it.
func (r *Recorder) Record(rec Record) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	if err := r.encoder.Encode(rec); err != nil {
		r.err = err
	}
}

// Meta writes the run-level record. Written first, so a reader knows the
// visibility timeout and the expected topology before it sees an
// operation.
//
// paths declares, per topic, the paths a message produced to it must be
// delivered on. A run with no fan-out passes each topic mapped to
// itself, which is what lets the checker treat a delivery on any other
// path as the misroute it would be.
func (r *Recorder) Meta(runID string, topics []string, paths map[string][]string, visibility time.Duration) {
	if r == nil {
		return
	}
	now := time.Now().UnixNano()
	r.Record(Record{
		Op:                  OpMeta,
		Call:                now,
		Ret:                 now,
		RunID:               runID,
		Topics:              topics,
		Paths:               paths,
		VisibilityTimeoutMs: visibility.Milliseconds(),
	})
}

// Close flushes and closes the file, reporting the first write error
// seen during the run.
func (r *Recorder) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writer != nil {
		if err := r.writer.Flush(); err != nil && r.err == nil {
			r.err = err
		}
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil && r.err == nil {
			r.err = err
		}
	}
	return r.err
}

// Path reports the file being written, for logging. Empty for a nil
// Recorder.
func (r *Recorder) Path() string {
	if r == nil || r.file == nil {
		return ""
	}
	return r.file.Name()
}
