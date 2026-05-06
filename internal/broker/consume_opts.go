package broker

import "time"

// ConsumeOpts is the input for Broker.Consume.
//
//   - If Partition is nil, the broker scans partitions in order looking
//     for the first one with an undelivered message (queue-style pull).
//   - If Offset is set, replay mode is engaged and Partition is required.
//   - Wait controls long-polling: if no message is available now, the
//     broker waits up to Wait for one (or for ctx to expire), whichever
//     is sooner.
type ConsumeOpts struct {
	Partition *int
	Offset    *int64
	Wait      time.Duration
}
