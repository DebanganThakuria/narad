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

// EncodeCommitProduceRequest encodes an OpCommitProduce payload. A
// non-empty TopicID follows the record as an optional trailing field.
func EncodeCommitProduceRequest(req CommitProduceRequest) ([]byte, error) {
	capacity := commitProduceLen(req)
	if req.TopicID != "" {
		capacity += fieldLen(req.TopicID)
	}
	w := opWriter(OpCommitProduce, capacity)
	if err := writeCommitProduce(w, req); err != nil {
		return nil, err
	}
	if req.TopicID != "" {
		if err := w.string(req.TopicID); err != nil {
			return nil, err
		}
	}
	return w.finish(), nil
}

// DecodeCommitProduceRequest decodes an OpCommitProduce payload.
func DecodeCommitProduceRequest(payload []byte) (CommitProduceRequest, error) {
	r, err := opReader(payload, OpCommitProduce)
	if err != nil {
		return CommitProduceRequest{}, err
	}
	record, err := readCommitProduce(&r)
	if err != nil {
		return CommitProduceRequest{}, err
	}
	if r.remaining() > 0 {
		// Optional trailing field; absent from older peers' requests.
		if record.TopicID, err = r.string(); err != nil {
			return CommitProduceRequest{}, err
		}
	}
	if err := r.done(); err != nil {
		return CommitProduceRequest{}, err
	}
	return record, nil
}

// EncodeCommitProduceBatchRequest encodes an OpCommitProduceBatch
// payload: a record count followed by that many commit-produce records,
// then, only when some record carries a TopicID, the records' topic IDs
// as an optional trailing section of runs (a run count, then per run a
// record count and the ID those consecutive records share). A batch is
// one partition's records, so it is almost always one run.
//
// The section is written only when set, so a batch without topic IDs is
// byte for byte what older releases send and accept. An owner on an
// older release refuses a batch that carries the section as trailing
// data (400); the dispatcher then resends it without topic IDs.
func EncodeCommitProduceBatchRequest(req CommitProduceBatchRequest) ([]byte, error) {
	if len(req.Records) > math.MaxInt32 {
		return nil, fmt.Errorf("commit produce batch too large: %d records", len(req.Records))
	}
	capacity := 4
	withIDs := false
	for _, record := range req.Records {
		capacity += commitProduceLen(record)
		withIDs = withIDs || record.TopicID != ""
	}
	runs := 0
	if withIDs {
		capacity += 4
		for i, record := range req.Records {
			if i == 0 || record.TopicID != req.Records[i-1].TopicID {
				runs++
				capacity += 4 + fieldLen(record.TopicID)
			}
		}
	}
	w := opWriter(OpCommitProduceBatch, capacity)
	w.i32(int32(len(req.Records)))
	for _, record := range req.Records {
		if err := writeCommitProduce(w, record); err != nil {
			return nil, err
		}
	}
	if withIDs {
		w.i32(int32(runs))
		for start := 0; start < len(req.Records); {
			end := start + 1
			for end < len(req.Records) && req.Records[end].TopicID == req.Records[start].TopicID {
				end++
			}
			w.i32(int32(end - start))
			if err := w.string(req.Records[start].TopicID); err != nil {
				return nil, err
			}
			start = end
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
		record, err := readCommitProduce(&r)
		if err != nil {
			return CommitProduceBatchRequest{}, err
		}
		records = append(records, record)
	}
	if r.remaining() > 0 {
		// Optional trailing section; absent from older peers' batches
		// and from batches without topic IDs.
		if err := readCommitProduceTopicIDs(&r, records); err != nil {
			return CommitProduceBatchRequest{}, err
		}
	}
	if err := r.done(); err != nil {
		return CommitProduceBatchRequest{}, err
	}
	return CommitProduceBatchRequest{Records: records}, nil
}

// readCommitProduceTopicIDs reads a batch's topic-ID runs (see
// EncodeCommitProduceBatchRequest) into records. The runs must cover
// every record exactly.
func readCommitProduceTopicIDs(r *reader, records []CommitProduceRequest) error {
	runs, err := r.i32()
	if err != nil {
		return err
	}
	if runs < 0 || int(runs) > len(records) {
		return fmt.Errorf("commit produce batch topic ids: %d runs for %d records", runs, len(records))
	}
	next := 0
	for range int(runs) {
		n, err := r.i32()
		if err != nil {
			return err
		}
		if n <= 0 || int(n) > len(records)-next {
			return fmt.Errorf("commit produce batch topic ids: run of %d records at record %d of %d", n, next, len(records))
		}
		id, err := r.string()
		if err != nil {
			return err
		}
		for i := next; i < next+int(n); i++ {
			records[i].TopicID = id
		}
		next += int(n)
	}
	if next != len(records) {
		return fmt.Errorf("commit produce batch topic ids: runs cover %d of %d records", next, len(records))
	}
	return nil
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
