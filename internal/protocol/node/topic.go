package node

// EncodeTopicBodyRequest encodes a topic+body payload under the given
// operation (create or alter topic).
func EncodeTopicBodyRequest(op Operation, req TopicBodyRequest) ([]byte, error) {
	w := opWriter(op, fieldLen(req.Topic)+fieldLenBytes(req.Body))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	if err := w.bytes(req.Body); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeTopicBodyRequest decodes a topic+body payload, verifying it
// carries the given operation.
func DecodeTopicBodyRequest(payload []byte, op Operation) (TopicBodyRequest, error) {
	r, err := opReader(payload, op)
	if err != nil {
		return TopicBodyRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return TopicBodyRequest{}, err
	}
	body, err := r.bytes()
	if err != nil {
		return TopicBodyRequest{}, err
	}
	if err := r.done(); err != nil {
		return TopicBodyRequest{}, err
	}
	return TopicBodyRequest{Topic: topic, Body: body}, nil
}

// EncodeTopicNameRequest encodes a topic-name-only payload under the
// given operation (delete or purge topic).
func EncodeTopicNameRequest(op Operation, req TopicNameRequest) ([]byte, error) {
	w := opWriter(op, fieldLen(req.Topic))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeTopicNameRequest decodes a topic-name-only payload, verifying
// it carries the given operation.
func DecodeTopicNameRequest(payload []byte, op Operation) (TopicNameRequest, error) {
	r, err := opReader(payload, op)
	if err != nil {
		return TopicNameRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return TopicNameRequest{}, err
	}
	if err := r.done(); err != nil {
		return TopicNameRequest{}, err
	}
	return TopicNameRequest{Topic: topic}, nil
}

// EncodeTopicPartitionStatsRequest encodes an OpTopicPartitionStats
// payload.
func EncodeTopicPartitionStatsRequest(req TopicPartitionStatsRequest) ([]byte, error) {
	w := opWriter(OpTopicPartitionStats, fieldLen(req.Topic)+4)
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	return w.finish(), nil
}

// DecodeTopicPartitionStatsRequest decodes an OpTopicPartitionStats
// payload.
func DecodeTopicPartitionStatsRequest(payload []byte) (TopicPartitionStatsRequest, error) {
	r, err := opReader(payload, OpTopicPartitionStats)
	if err != nil {
		return TopicPartitionStatsRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return TopicPartitionStatsRequest{}, err
	}
	partition, err := r.i32()
	if err != nil {
		return TopicPartitionStatsRequest{}, err
	}
	if err := r.done(); err != nil {
		return TopicPartitionStatsRequest{}, err
	}
	return TopicPartitionStatsRequest{Topic: topic, Partition: int(partition)}, nil
}
