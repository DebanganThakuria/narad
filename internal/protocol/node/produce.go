package node

import (
	"fmt"
	"math"
)

// EncodeProduceRequest encodes an OpProduce payload.
func EncodeProduceRequest(req ProduceRequest) ([]byte, error) {
	w := opWriter(OpProduce, fieldLen(req.Topic)+fieldLen(req.Key)+4+fieldLenBytes(req.Payload))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	if err := w.string(req.Key); err != nil {
		return nil, err
	}
	partition, err := partitionField(req.Partition)
	if err != nil {
		return nil, err
	}
	w.i32(partition)
	if err := w.bytes(req.Payload); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeProduceRequest decodes an OpProduce payload.
func DecodeProduceRequest(payload []byte) (ProduceRequest, error) {
	r, err := opReader(payload, OpProduce)
	if err != nil {
		return ProduceRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return ProduceRequest{}, err
	}
	key, err := r.string()
	if err != nil {
		return ProduceRequest{}, err
	}
	partition, err := r.i32()
	if err != nil {
		return ProduceRequest{}, err
	}
	body, err := r.bytes()
	if err != nil {
		return ProduceRequest{}, err
	}
	if err := r.done(); err != nil {
		return ProduceRequest{}, err
	}
	return ProduceRequest{Topic: topic, Key: key, Partition: int(partition), Payload: body}, nil
}

// EncodeCommitProduceRequest encodes an OpCommitProduce payload.
func EncodeCommitProduceRequest(req CommitProduceRequest) ([]byte, error) {
	w := opWriter(OpCommitProduce, commitProduceLen(req))
	if err := writeCommitProduce(w, req); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeCommitProduceRequest decodes an OpCommitProduce payload.
func DecodeCommitProduceRequest(payload []byte) (CommitProduceRequest, error) {
	r, err := opReader(payload, OpCommitProduce)
	if err != nil {
		return CommitProduceRequest{}, err
	}
	record, err := readCommitProduce(r)
	if err != nil {
		return CommitProduceRequest{}, err
	}
	if err := r.done(); err != nil {
		return CommitProduceRequest{}, err
	}
	return record, nil
}

// EncodeCommitProduceBatchRequest encodes an OpCommitProduceBatch
// payload: a record count followed by that many commit-produce records.
func EncodeCommitProduceBatchRequest(req CommitProduceBatchRequest) ([]byte, error) {
	capacity := 4
	for _, record := range req.Records {
		capacity += commitProduceLen(record)
	}
	if len(req.Records) > math.MaxInt32 {
		return nil, fmt.Errorf("commit produce batch too large: %d records", len(req.Records))
	}
	w := opWriter(OpCommitProduceBatch, capacity)
	w.i32(int32(len(req.Records)))
	for _, record := range req.Records {
		if err := writeCommitProduce(w, record); err != nil {
			return nil, err
		}
	}
	return w.finish(), nil
}

// DecodeCommitProduceBatchRequest decodes an OpCommitProduceBatch
// payload.
func DecodeCommitProduceBatchRequest(payload []byte) (CommitProduceBatchRequest, error) {
	r, err := opReader(payload, OpCommitProduceBatch)
	if err != nil {
		return CommitProduceBatchRequest{}, err
	}
	count, err := r.i32()
	if err != nil {
		return CommitProduceBatchRequest{}, err
	}
	if count < 0 {
		return CommitProduceBatchRequest{}, fmt.Errorf("negative commit produce batch size %d", count)
	}
	// Each record occupies at least minCommitProduceBytes, so count can
	// never exceed remaining/minCommitProduceBytes. Cap the preallocation
	// by that bound so a claimed count cannot make the decoder allocate
	// more than a small multiple of the bytes actually received before it
	// fails: the old bound of one record per remaining byte let a 16 MiB
	// frame reserve over 1 GiB of record headers.
	records := make([]CommitProduceRequest, 0, min(int(count), r.remaining()/minCommitProduceBytes))
	for range int(count) {
		record, err := readCommitProduce(r)
		if err != nil {
			return CommitProduceBatchRequest{}, err
		}
		records = append(records, record)
	}
	if err := r.done(); err != nil {
		return CommitProduceBatchRequest{}, err
	}
	return CommitProduceBatchRequest{Records: records}, nil
}

func writeCommitProduce(w *writer, req CommitProduceRequest) error {
	if err := w.string(req.Topic); err != nil {
		return err
	}
	if err := w.string(req.Key); err != nil {
		return err
	}
	partition, err := partitionField(req.TargetPartition)
	if err != nil {
		return err
	}
	w.i32(partition)
	if err := w.bytes(req.Payload); err != nil {
		return err
	}
	w.i64(req.CreatedAtUnixMs)
	return nil
}

func readCommitProduce(r *reader) (CommitProduceRequest, error) {
	topic, err := r.string()
	if err != nil {
		return CommitProduceRequest{}, err
	}
	key, err := r.string()
	if err != nil {
		return CommitProduceRequest{}, err
	}
	partition, err := r.i32()
	if err != nil {
		return CommitProduceRequest{}, err
	}
	body, err := r.bytes()
	if err != nil {
		return CommitProduceRequest{}, err
	}
	createdAt, err := r.i64()
	if err != nil {
		return CommitProduceRequest{}, err
	}
	return CommitProduceRequest{
		Topic:           topic,
		Key:             key,
		TargetPartition: int(partition),
		Payload:         body,
		CreatedAtUnixMs: createdAt,
	}, nil
}

// minCommitProduceBytes is the encoded size of an empty commit-produce
// record: two empty strings, a partition, empty bytes, and a timestamp.
const minCommitProduceBytes = 4 + 4 + 4 + 4 + 8

// commitProduceLen is the encoded size of one commit-produce record.
func commitProduceLen(req CommitProduceRequest) int {
	return fieldLen(req.Topic) + fieldLen(req.Key) + 4 + fieldLenBytes(req.Payload) + 8
}
