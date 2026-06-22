package node

func EncodeTopicBodyRequest(op Operation, req TopicBodyRequest) ([]byte, error) {
	w := opWriter(op, fieldLen(req.Topic)+fieldLenBytes(req.Body))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	if err := w.bytes(req.Body); err != nil {
		return nil, err
	}
	return w.bytesOut(), nil
}

func DecodeTopicBodyRequest(payload []byte, op Operation) (TopicBodyRequest, error) {
	r, err := opReader(payload, op)
	if err != nil {
		return TopicBodyRequest{}, err
	}
	topicName, err := r.string()
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
	return TopicBodyRequest{Topic: topicName, Body: body}, nil
}

func EncodeTopicNameRequest(op Operation, req TopicNameRequest) ([]byte, error) {
	w := opWriter(op, fieldLen(req.Topic))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	return w.bytesOut(), nil
}

func DecodeTopicNameRequest(payload []byte, op Operation) (TopicNameRequest, error) {
	r, err := opReader(payload, op)
	if err != nil {
		return TopicNameRequest{}, err
	}
	topicName, err := r.string()
	if err != nil {
		return TopicNameRequest{}, err
	}
	if err := r.done(); err != nil {
		return TopicNameRequest{}, err
	}
	return TopicNameRequest{Topic: topicName}, nil
}

func EncodeTopicPartitionStatsRequest(req TopicPartitionStatsRequest) ([]byte, error) {
	w := opWriter(OpTopicPartitionStats, fieldLen(req.Topic)+4)
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	return w.bytesOut(), nil
}

func DecodeTopicPartitionStatsRequest(payload []byte) (TopicPartitionStatsRequest, error) {
	r, err := opReader(payload, OpTopicPartitionStats)
	if err != nil {
		return TopicPartitionStatsRequest{}, err
	}
	topicName, err := r.string()
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
	return TopicPartitionStatsRequest{Topic: topicName, Partition: int(partition)}, nil
}
