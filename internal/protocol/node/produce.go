package node

import "fmt"

// EncodeProduceRequest encodes an OpProduce payload.
func EncodeProduceRequest(req ProduceRequest) ([]byte, error) {
	w := opWriter(OpProduce, fieldLen(req.Topic)+fieldLen(req.Key)+4+fieldLenBytes(req.Payload))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	if err := w.string(req.Key); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
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
	// Each record consumes several bytes, so count can never exceed the
	// remaining payload. Cap the preallocation so an attacker-controlled
	// count can't trigger a multi-gigabyte allocation before decode fails.
	records := make([]CommitProduceRequest, 0, min(int(count), r.remaining()))
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
	w.i32(int32(req.TargetPartition))
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

// commitProduceLen is the encoded size of one commit-produce record.
func commitProduceLen(req CommitProduceRequest) int {
	return fieldLen(req.Topic) + fieldLen(req.Key) + 4 + fieldLenBytes(req.Payload) + 8
}
