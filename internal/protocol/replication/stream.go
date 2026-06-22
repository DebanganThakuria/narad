package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	StreamPath            = "/internal/v1/replicate/stream"
	StreamUpgradeProtocol = "narad-replication-v1"

	MaxStreamFramePayloadBytes = 16 << 20

	streamFrameHeaderBytes = 20
	streamMagic            = uint32(0x4e525331) // NRS1
	streamVersion          = byte(1)
)

type StreamFrameType uint8

const (
	StreamFrameAppendBatch StreamFrameType = 1
	StreamFrameAck         StreamFrameType = 2
	StreamFrameError       StreamFrameType = 3
	StreamFramePing        StreamFrameType = 4
	StreamFramePong        StreamFrameType = 5
	StreamFrameAppendMulti StreamFrameType = 6
	StreamFrameMultiAck    StreamFrameType = 7
	StreamFrameNodeRequest StreamFrameType = 8
	StreamFrameNodeReply   StreamFrameType = 9
	StreamFrameReplicaRead StreamFrameType = 10
	StreamFrameReplicaData StreamFrameType = 11
)

type StreamFrame struct {
	Type      StreamFrameType
	RequestID uint64
	Payload   []byte
}

type StreamAppendBatch struct {
	Topic      string
	Partition  int
	BaseOffset int64
	Payloads   [][]byte
}

type StreamAppendGroup struct {
	Topic      string
	Partition  int
	BaseOffset int64
	Payloads   [][]byte
}

type StreamAppendMulti struct {
	Groups []StreamAppendGroup
}

type StreamAppendResult struct {
	NextOffset        int64
	ReplicaNextOffset int64
	Message           string
}

type StreamError struct {
	ReplicaNextOffset int64
	Message           string
}

type StreamReplicaRead struct {
	Topic         string
	Partition     int
	Offset        int64
	CommittedOnly bool
}

type StreamReplicaData struct {
	Found   bool
	Payload []byte
}

func WriteStreamFrame(w io.Writer, frame StreamFrame) error {
	if len(frame.Payload) > MaxStreamFramePayloadBytes {
		return fmt.Errorf("stream frame payload too large: %d bytes", len(frame.Payload))
	}

	var header [streamFrameHeaderBytes]byte
	binary.BigEndian.PutUint32(header[0:4], streamMagic)
	header[4] = streamVersion
	header[5] = byte(frame.Type)
	binary.BigEndian.PutUint64(header[8:16], frame.RequestID)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(frame.Payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(frame.Payload) == 0 {
		return nil
	}
	_, err := w.Write(frame.Payload)
	return err
}

func ReadStreamFrame(r io.Reader, maxPayloadBytes int) (StreamFrame, error) {
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = MaxStreamFramePayloadBytes
	}

	var header [streamFrameHeaderBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return StreamFrame{}, err
	}
	if got := binary.BigEndian.Uint32(header[0:4]); got != streamMagic {
		return StreamFrame{}, fmt.Errorf("invalid stream magic: 0x%x", got)
	}
	if got := header[4]; got != streamVersion {
		return StreamFrame{}, fmt.Errorf("unsupported stream version: %d", got)
	}

	payloadLen := int(binary.BigEndian.Uint32(header[16:20]))
	if payloadLen > maxPayloadBytes {
		return StreamFrame{}, fmt.Errorf("stream frame payload too large: %d bytes", payloadLen)
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return StreamFrame{}, err
		}
	}
	return StreamFrame{
		Type:      StreamFrameType(header[5]),
		RequestID: binary.BigEndian.Uint64(header[8:16]),
		Payload:   payload,
	}, nil
}

func EncodeStreamAppendBatch(topic string, partition int, baseOffset int64, payloads [][]byte) ([]byte, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic required")
	}
	if len(topic) > math.MaxUint16 {
		return nil, fmt.Errorf("topic too long: %d bytes", len(topic))
	}
	if partition < 0 || partition > math.MaxInt32 {
		return nil, fmt.Errorf("invalid partition: %d", partition)
	}
	if baseOffset < 0 {
		return nil, fmt.Errorf("invalid base offset: %d", baseOffset)
	}
	if len(payloads) == 0 {
		return nil, fmt.Errorf("empty batch")
	}
	if len(payloads) > math.MaxUint32 {
		return nil, fmt.Errorf("too many records: %d", len(payloads))
	}

	records, err := EncodeBatchPayload(payloads)
	if err != nil {
		return nil, err
	}

	total := 2 + len(topic) + 4 + 8 + 4 + len(records)
	out := make([]byte, total)
	binary.BigEndian.PutUint16(out[0:2], uint16(len(topic)))
	copy(out[2:], topic)
	pos := 2 + len(topic)
	binary.BigEndian.PutUint32(out[pos:pos+4], uint32(partition))
	pos += 4
	binary.BigEndian.PutUint64(out[pos:pos+8], uint64(baseOffset))
	pos += 8
	binary.BigEndian.PutUint32(out[pos:pos+4], uint32(len(payloads)))
	pos += 4
	copy(out[pos:], records)
	return out, nil
}

func DecodeStreamAppendBatch(payload []byte) (StreamAppendBatch, error) {
	if len(payload) < 2 {
		return StreamAppendBatch{}, io.ErrUnexpectedEOF
	}
	topicLen := int(binary.BigEndian.Uint16(payload[0:2]))
	pos := 2
	if len(payload) < pos+topicLen+4+8+4 {
		return StreamAppendBatch{}, io.ErrUnexpectedEOF
	}
	topic := string(payload[pos : pos+topicLen])
	pos += topicLen
	partition := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	baseOffset := int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
	pos += 8
	recordCount := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	records, err := DecodeBatchPayload(bytes.NewReader(payload[pos:]), recordCount)
	if err != nil {
		return StreamAppendBatch{}, err
	}
	if len(records) != recordCount {
		return StreamAppendBatch{}, fmt.Errorf("record count mismatch: got %d want %d", len(records), recordCount)
	}
	return StreamAppendBatch{
		Topic:      topic,
		Partition:  partition,
		BaseOffset: baseOffset,
		Payloads:   records,
	}, nil
}

func EncodeStreamAppendMulti(groups []StreamAppendGroup) ([]byte, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("empty multi append")
	}
	if len(groups) > math.MaxUint32 {
		return nil, fmt.Errorf("too many append groups: %d", len(groups))
	}

	encoded := make([][]byte, len(groups))
	total := 4
	for i, group := range groups {
		payload, err := EncodeStreamAppendBatch(group.Topic, group.Partition, group.BaseOffset, group.Payloads)
		if err != nil {
			return nil, err
		}
		if len(payload) > math.MaxUint32 {
			return nil, fmt.Errorf("append group too large: %d bytes", len(payload))
		}
		encoded[i] = payload
		total += 4 + len(payload)
	}

	out := make([]byte, total)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(encoded)))
	pos := 4
	for _, payload := range encoded {
		binary.BigEndian.PutUint32(out[pos:pos+4], uint32(len(payload)))
		pos += 4
		copy(out[pos:], payload)
		pos += len(payload)
	}
	return out, nil
}

func DecodeStreamAppendMulti(payload []byte) (StreamAppendMulti, error) {
	if len(payload) < 4 {
		return StreamAppendMulti{}, io.ErrUnexpectedEOF
	}
	groupCount := int(binary.BigEndian.Uint32(payload[0:4]))
	if groupCount == 0 {
		return StreamAppendMulti{}, fmt.Errorf("empty multi append")
	}

	pos := 4
	groups := make([]StreamAppendGroup, 0, groupCount)
	for range groupCount {
		if len(payload) < pos+4 {
			return StreamAppendMulti{}, io.ErrUnexpectedEOF
		}
		groupLen := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if groupLen == 0 {
			return StreamAppendMulti{}, fmt.Errorf("empty append group")
		}
		if len(payload) < pos+groupLen {
			return StreamAppendMulti{}, io.ErrUnexpectedEOF
		}
		batch, err := DecodeStreamAppendBatch(payload[pos : pos+groupLen])
		if err != nil {
			return StreamAppendMulti{}, err
		}
		groups = append(groups, StreamAppendGroup{
			Topic:      batch.Topic,
			Partition:  batch.Partition,
			BaseOffset: batch.BaseOffset,
			Payloads:   batch.Payloads,
		})
		pos += groupLen
	}
	if pos != len(payload) {
		return StreamAppendMulti{}, fmt.Errorf("trailing multi append data: %d bytes", len(payload)-pos)
	}
	return StreamAppendMulti{Groups: groups}, nil
}

func EncodeStreamAck(nextOffset int64) ([]byte, error) {
	if nextOffset < 0 {
		return nil, fmt.Errorf("invalid next offset: %d", nextOffset)
	}
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], uint64(nextOffset))
	return out[:], nil
}

func DecodeStreamAck(payload []byte) (int64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("invalid ack payload size: %d", len(payload))
	}
	return int64(binary.BigEndian.Uint64(payload)), nil
}

func EncodeStreamAppendResults(results []StreamAppendResult) ([]byte, error) {
	if len(results) == 0 {
		return nil, fmt.Errorf("empty append results")
	}
	if len(results) > math.MaxUint32 {
		return nil, fmt.Errorf("too many append results: %d", len(results))
	}

	total := 4
	for _, result := range results {
		if len(result.Message) > math.MaxUint16 {
			return nil, fmt.Errorf("append result message too long: %d bytes", len(result.Message))
		}
		total += 8 + 8 + 2 + len(result.Message)
	}

	out := make([]byte, total)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(results)))
	pos := 4
	for _, result := range results {
		binary.BigEndian.PutUint64(out[pos:pos+8], uint64(result.NextOffset))
		pos += 8
		binary.BigEndian.PutUint64(out[pos:pos+8], uint64(result.ReplicaNextOffset))
		pos += 8
		binary.BigEndian.PutUint16(out[pos:pos+2], uint16(len(result.Message)))
		pos += 2
		copy(out[pos:], result.Message)
		pos += len(result.Message)
	}
	return out, nil
}

func DecodeStreamAppendResults(payload []byte) ([]StreamAppendResult, error) {
	if len(payload) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	resultCount := int(binary.BigEndian.Uint32(payload[0:4]))
	if resultCount == 0 {
		return nil, fmt.Errorf("empty append results")
	}

	pos := 4
	results := make([]StreamAppendResult, 0, resultCount)
	for range resultCount {
		if len(payload) < pos+18 {
			return nil, io.ErrUnexpectedEOF
		}
		nextOffset := int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
		pos += 8
		replicaNextOffset := int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
		pos += 8
		messageLen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if len(payload) < pos+messageLen {
			return nil, io.ErrUnexpectedEOF
		}
		results = append(results, StreamAppendResult{
			NextOffset:        nextOffset,
			ReplicaNextOffset: replicaNextOffset,
			Message:           string(payload[pos : pos+messageLen]),
		})
		pos += messageLen
	}
	if pos != len(payload) {
		return nil, fmt.Errorf("trailing append result data: %d bytes", len(payload)-pos)
	}
	return results, nil
}

func EncodeStreamError(replicaNextOffset int64, message string) ([]byte, error) {
	if len(message) > math.MaxUint16 {
		message = message[:math.MaxUint16]
	}

	out := make([]byte, 1+8+2+len(message))
	if replicaNextOffset >= 0 {
		out[0] = 1
		binary.BigEndian.PutUint64(out[1:9], uint64(replicaNextOffset))
	}
	binary.BigEndian.PutUint16(out[9:11], uint16(len(message)))
	copy(out[11:], message)
	return out, nil
}

func DecodeStreamError(payload []byte) (StreamError, error) {
	if len(payload) < 11 {
		return StreamError{}, io.ErrUnexpectedEOF
	}
	next := int64(-1)
	if payload[0] == 1 {
		next = int64(binary.BigEndian.Uint64(payload[1:9]))
	}
	msgLen := int(binary.BigEndian.Uint16(payload[9:11]))
	if len(payload) < 11+msgLen {
		return StreamError{}, io.ErrUnexpectedEOF
	}
	return StreamError{
		ReplicaNextOffset: next,
		Message:           string(payload[11 : 11+msgLen]),
	}, nil
}

func EncodeStreamReplicaRead(topic string, partition int, offset int64, committedOnly bool) ([]byte, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic required")
	}
	if len(topic) > math.MaxUint16 {
		return nil, fmt.Errorf("topic too long: %d bytes", len(topic))
	}
	if partition < 0 || partition > math.MaxInt32 {
		return nil, fmt.Errorf("invalid partition: %d", partition)
	}
	if offset < 0 {
		return nil, fmt.Errorf("invalid offset: %d", offset)
	}

	out := make([]byte, 2+len(topic)+4+8+1)
	binary.BigEndian.PutUint16(out[0:2], uint16(len(topic)))
	copy(out[2:], topic)
	pos := 2 + len(topic)
	binary.BigEndian.PutUint32(out[pos:pos+4], uint32(partition))
	pos += 4
	binary.BigEndian.PutUint64(out[pos:pos+8], uint64(offset))
	pos += 8
	if committedOnly {
		out[pos] = 1
	}
	return out, nil
}

func DecodeStreamReplicaRead(payload []byte) (StreamReplicaRead, error) {
	if len(payload) < 2 {
		return StreamReplicaRead{}, io.ErrUnexpectedEOF
	}
	topicLen := int(binary.BigEndian.Uint16(payload[0:2]))
	pos := 2
	if len(payload) < pos+topicLen+4+8+1 {
		return StreamReplicaRead{}, io.ErrUnexpectedEOF
	}
	topic := string(payload[pos : pos+topicLen])
	pos += topicLen
	partition := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	offset := int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
	pos += 8
	committedOnly := payload[pos] == 1
	pos++
	if pos != len(payload) {
		return StreamReplicaRead{}, fmt.Errorf("trailing replica read data: %d bytes", len(payload)-pos)
	}
	return StreamReplicaRead{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		CommittedOnly: committedOnly,
	}, nil
}

func EncodeStreamReplicaData(found bool, payload []byte) ([]byte, error) {
	if len(payload) > math.MaxUint32 {
		return nil, fmt.Errorf("replica data too large: %d bytes", len(payload))
	}
	out := make([]byte, 1+4+len(payload))
	if found {
		out[0] = 1
	}
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	copy(out[5:], payload)
	return out, nil
}

func DecodeStreamReplicaData(payload []byte) (StreamReplicaData, error) {
	if len(payload) < 5 {
		return StreamReplicaData{}, io.ErrUnexpectedEOF
	}
	found := payload[0] == 1
	payloadLen := int(binary.BigEndian.Uint32(payload[1:5]))
	if len(payload) < 5+payloadLen {
		return StreamReplicaData{}, io.ErrUnexpectedEOF
	}
	if len(payload) != 5+payloadLen {
		return StreamReplicaData{}, fmt.Errorf("trailing replica data: %d bytes", len(payload)-5-payloadLen)
	}
	return StreamReplicaData{
		Found:   found,
		Payload: payload[5:],
	}, nil
}
