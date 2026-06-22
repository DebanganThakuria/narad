package replication

import "strings"

const (
	RawContentType   = "application/vnd.narad.replication.record.v1+json"
	BatchContentType = "application/vnd.narad.replication.batch.v1"

	HeaderTopic       = "X-Narad-Topic"
	HeaderPartition   = "X-Narad-Partition"
	HeaderOffset      = "X-Narad-Offset"
	HeaderLeaderID    = "X-Narad-Leader-ID"
	HeaderRecordCount = "X-Narad-Record-Count"

	HeaderReplicaNextOffset = "X-Narad-Replica-Next-Offset"
)

func IsRawContentType(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), RawContentType)
}

func IsBatchContentType(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), BatchContentType)
}
