package replication

import (
	"errors"

	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

func streamAppendGroupForRecords(topic string, partition int, records []Record) replicationwire.StreamAppendGroup {
	payloads := make([][]byte, len(records))
	for i, record := range records {
		payloads[i] = record.Payload
	}
	return replicationwire.StreamAppendGroup{
		Topic:      topic,
		Partition:  partition,
		BaseOffset: records[0].Offset,
		Payloads:   payloads,
	}
}

func streamResultForRecords(records []Record, result replicationwire.StreamAppendResult) (int64, error) {
	if result.Message != "" {
		if result.ReplicaNextOffset >= 0 {
			return 0, &OffsetMismatchError{
				RequestedOffset:   records[0].Offset,
				ReplicaNextOffset: result.ReplicaNextOffset,
			}
		}
		return 0, errors.New(result.Message)
	}
	expectedNext := records[0].Offset + int64(len(records))
	if result.NextOffset != expectedNext {
		return 0, &OffsetMismatchError{
			RequestedOffset:   records[0].Offset,
			ReplicaNextOffset: result.NextOffset,
		}
	}
	return result.NextOffset, nil
}
