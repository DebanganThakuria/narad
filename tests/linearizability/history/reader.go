package history

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// Log is a parsed history: the run's metadata, its client operations,
// and the fault windows the injector recorded alongside them.
type Log struct {
	// Meta is the run-level record. Zero if the history has none, which
	// is legal: a checker falls back to its flag defaults.
	Meta Record
	// HasMeta reports whether a meta record was present.
	HasMeta bool
	// Ops are the produce, deliver and ack records, sorted by call time.
	Ops []Record
	// Faults are the fault records, sorted by call time.
	Faults []Record
	// Truncated counts trailing partial lines skipped, one per file at
	// most. A run killed mid-write leaves one; anything else is a bug.
	Truncated int
	// Skewed counts records whose return preceded their call. The reader
	// clamps them (return = call) so the checker still runs, but a
	// non-zero count belongs in the report: it means the clock moved
	// under the run and the intervals are not fully trustworthy.
	Skewed int
}

// maxLineBytes caps one JSONL line. Records are small and fixed-shape;
// anything larger is a corrupt file rather than a long record.
const maxLineBytes = 1 << 20

// Read parses one or more JSONL history files and merges them into a
// single Log. Passing the driver's history and the fault injector's file
// together is the normal case.
//
// A trailing partial line is skipped rather than failing the read: a run
// killed while the buffer was half-flushed still has a valid prefix, and
// refusing to check it would throw away the evidence from exactly the
// runs most worth checking. A malformed line anywhere else is an error,
// because that is corruption rather than truncation.
func Read(paths ...string) (*Log, error) {
	log := &Log{}
	for _, path := range paths {
		if err := log.readFile(path); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(log.Ops, func(i, j int) bool { return log.Ops[i].Call < log.Ops[j].Call })
	sort.SliceStable(log.Faults, func(i, j int) bool { return log.Faults[i].Call < log.Faults[j].Call })
	return log, nil
}

func (l *Log) readFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open history %s: %w", path, err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 64<<10)
	lineNo := 0
	for {
		lineNo++
		line, readErr := readLine(reader)
		if len(line) > 0 {
			rec, parseErr := parseRecord(line)
			if parseErr != nil {
				// Only the final line of a file may be partial. bufio's
				// ReadLine hands back an unterminated final line with a nil
				// error and reports io.EOF on the following call, so "was
				// that the last line" has to be asked separately rather
				// than read off readErr.
				if errors.Is(readErr, io.EOF) || atEOF(reader) {
					l.Truncated++
					break
				}
				return fmt.Errorf("%s:%d: %w", path, lineNo, parseErr)
			}
			l.add(rec)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("read history %s: %w", path, readErr)
		}
	}
	return nil
}

// readLine returns one line without its terminator. A final line with no
// newline is returned with io.EOF, which is how a truncated write is
// detected.
func readLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, isPrefix, err := reader.ReadLine()
		line = append(line, chunk...)
		if len(line) > maxLineBytes {
			return nil, fmt.Errorf("history line exceeds %d bytes", maxLineBytes)
		}
		if err != nil {
			return line, err
		}
		if !isPrefix {
			return line, nil
		}
	}
}

// atEOF reports whether the reader has nothing left, without consuming
// anything.
func atEOF(reader *bufio.Reader) bool {
	_, err := reader.Peek(1)
	return errors.Is(err, io.EOF)
}

func parseRecord(line []byte) (Record, error) {
	var rec Record
	if err := json.Unmarshal(line, &rec); err != nil {
		return rec, fmt.Errorf("decode record: %w", err)
	}
	if rec.Op == "" {
		return rec, errors.New("record has no op")
	}
	return rec, nil
}

func (l *Log) add(rec Record) {
	if rec.Ret < rec.Call {
		// Porcupine requires a closed, non-inverted interval. Clamping
		// keeps the run checkable; the count is reported so nobody reads
		// the verdict as if the clock had behaved.
		rec.Ret = rec.Call
		l.Skewed++
	}
	switch rec.Op {
	case OpMeta:
		if !l.HasMeta {
			l.Meta = rec
			l.HasMeta = true
		}
	case OpFault:
		l.Faults = append(l.Faults, rec)
	case OpProduce, OpDeliver, OpAck:
		l.Ops = append(l.Ops, rec)
	default:
		// An unknown op is ignored rather than fatal, so an older checker
		// can still read a history written by a newer driver.
	}
}
