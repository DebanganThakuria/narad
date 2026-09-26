package ingress

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// produceRecordFormat versions the on-disk record layout. Bump only
// with a decoder that still accepts every prior format.
const produceRecordFormat byte = 1

// EncodeProduceRecord serializes a record into the ingress WAL payload
// format: a format byte followed by length-prefixed fields, all
// big-endian. The record's WAL field is not encoded — the WAL assigns
// it at append time.
func EncodeProduceRecord(record ProduceRecord) ([]byte, error) {
	if err := validateProduceRecord(record); err != nil {
		return nil, err
	}

	out := make([]byte, 0, produceRecordSize(record))
	return appendProduceRecord(out, record), nil
}

// validateProduceRecord is EncodeProduceRecord's validation half, for
// callers that encode in place (see Manager.AcceptProduce).
func validateProduceRecord(record ProduceRecord) error {
	if record.Topic == "" {
		return errors.New("ingress: topic required")
	}
	if record.TargetPartition < 0 {
		return errors.New("ingress: target partition must be >= 0")
	}
	if record.TargetPartition > math.MaxInt32 {
		return errors.New("ingress: target partition exceeds int32 range")
	}
	if len(record.Payload) == 0 {
		return errors.New("ingress: payload required")
	}
	return nil
}

// produceRecordSize is the exact encoded size of a valid record.
func produceRecordSize(record ProduceRecord) int {
	return 1 + stringSize(record.Topic) + stringSize(record.Key) + 4 + 8 + bytesSize(record.Payload)
}

// appendProduceRecord appends the encoding of a validated record to dst.
func appendProduceRecord(dst []byte, record ProduceRecord) []byte {
	// validateProduceRecord already bounds the partition to [0, MaxInt32];
	// the check is restated at the conversion site so it is provable
	// locally. It cannot fire on a validated record.
	if record.TargetPartition < 0 || record.TargetPartition > math.MaxInt32 {
		panic("ingress: appendProduceRecord: unvalidated target partition")
	}
	partition := uint32(record.TargetPartition)
	dst = append(dst, produceRecordFormat)
	dst = appendString(dst, record.Topic)
	dst = appendString(dst, record.Key)
	dst = binary.BigEndian.AppendUint32(dst, partition)
	dst = binary.BigEndian.AppendUint64(dst, uint64(record.CreatedAtUnixMs))
	return appendBytes(dst, record.Payload)
}

// DecodeProduceRecord parses a payload written by EncodeProduceRecord.
// It rejects unknown formats, truncated fields, and trailing bytes, so
// a corrupt WAL frame can never decode into a plausible-looking record.
// The returned record never aliases data.
func DecodeProduceRecord(data []byte) (ProduceRecord, error) {
	return decodeProduceRecord(data, false, nil)
}

// decodeProduceRecord is DecodeProduceRecord with the replay path's two
// allocation savers. With alias set, Payload is a subslice of data
// rather than a copy: only for data handed over for good, such as a WAL
// frame payload, which replay allocates fresh per record and never
// reuses (the payload is the last field, so the subslice is also capped
// at its own length and an append cannot run into anything). topics,
// when non-nil, interns Topic so a pass over thousands of records of a
// few topics allocates each name once. Key is always copied: keys are
// high-cardinality, and a string aliasing the frame would pin the whole
// frame for as long as anything kept the key.
func decodeProduceRecord(data []byte, alias bool, topics *topicInterner) (ProduceRecord, error) {
	r := byteReader{data: data}
	format, err := r.u8()
	if err != nil {
		return ProduceRecord{}, err
	}
	if format != produceRecordFormat {
		return ProduceRecord{}, fmt.Errorf("ingress: unsupported produce record format %d", format)
	}
	topicBytes, err := r.view()
	if err != nil {
		return ProduceRecord{}, err
	}
	keyBytes, err := r.view()
	if err != nil {
		return ProduceRecord{}, err
	}
	partition, err := r.u32()
	if err != nil {
		return ProduceRecord{}, err
	}
	createdAt, err := r.u64()
	if err != nil {
		return ProduceRecord{}, err
	}
	payload, err := r.view()
	if err != nil {
		return ProduceRecord{}, err
	}
	if err := r.done(); err != nil {
		return ProduceRecord{}, err
	}
	if !alias {
		payload = append([]byte(nil), payload...)
	}
	return ProduceRecord{
		Topic:           topics.intern(topicBytes),
		Key:             string(keyBytes),
		TargetPartition: int(partition),
		Payload:         payload,
		CreatedAtUnixMs: int64(createdAt),
	}, nil
}

// maxInternedTopics bounds a topicInterner: a pass over a WAL holding
// more distinct topics than this copies the rest per record, as it did
// before interning, instead of growing without limit.
const maxInternedTopics = 1024

// topicInterner hands out one string per distinct topic name seen in a
// replay pass. The zero value is ready to use; a nil interner copies
// every name. Not safe for concurrent use.
type topicInterner struct {
	names map[string]string
}

// intern returns b as a string, reusing an earlier copy of the same
// name. The lookup converts b without allocating.
func (t *topicInterner) intern(b []byte) string {
	if t == nil {
		return string(b)
	}
	if name, ok := t.names[string(b)]; ok {
		return name
	}
	name := string(b)
	if t.names == nil {
		t.names = make(map[string]string)
	}
	if len(t.names) < maxInternedTopics {
		t.names[name] = name
	}
	return name
}

func stringSize(s string) int {
	return 4 + len(s)
}

func bytesSize(b []byte) int {
	return 4 + len(b)
}

func appendString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(s)))
	return append(dst, s...)
}

func appendBytes(dst []byte, b []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b)))
	return append(dst, b...)
}

// byteReader is a bounds-checked cursor over an encoded record. view
// returns length-prefixed fields as subslices of the backing slice; the
// decoder decides per field whether to copy.
type byteReader struct {
	data []byte
	off  int
}

func (r *byteReader) u8() (byte, error) {
	if len(r.data)-r.off < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.data[r.off]
	r.off++
	return v, nil
}

func (r *byteReader) u32() (uint32, error) {
	if len(r.data)-r.off < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(r.data[r.off : r.off+4])
	r.off += 4
	return v, nil
}

func (r *byteReader) u64() (uint64, error) {
	if len(r.data)-r.off < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(r.data[r.off : r.off+8])
	r.off += 8
	return v, nil
}

// view reads a length-prefixed field and returns it as a subslice of
// the backing data, capped at its own length.
func (r *byteReader) view() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if uint64(len(r.data)-r.off) < uint64(n) {
		return nil, io.ErrUnexpectedEOF
	}
	end := r.off + int(n)
	out := r.data[r.off:end:end]
	r.off = end
	return out, nil
}

func (r *byteReader) done() error {
	if r.off != len(r.data) {
		return fmt.Errorf("ingress: trailing bytes: %d", len(r.data)-r.off)
	}
	return nil
}
