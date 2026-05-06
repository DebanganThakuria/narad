// Package topic defines the value types used across the broker,
// metastore, and partition layers. There is no behavior here — these
// are wire- and storage-stable structs.
package topic

import "time"

// Topic is the user-facing logical stream. Partitions and
// ReplicationFactor are fixed at create time per the PRD.
type Topic struct {
	Name              string    `json:"name"`
	Partitions        int       `json:"partitions"`
	ReplicationFactor int       `json:"replication_factor"`
	CreatedAt         time.Time `json:"created_at"`
}
