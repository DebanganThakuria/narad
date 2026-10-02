package main

import "context"

// metastoreHalter is the part of *metastore.Store the halt watch reads;
// tests fake it.
type metastoreHalter interface {
	Halted() <-chan struct{}
	HaltErr() error
}

// watchMetastoreHalt fails serve when the metastore stops applying Raft
// entries (an entry type this build does not know, or a write its disk
// refused for good). Raft has shut down by then, so the node can no
// longer follow the cluster's metadata: serve shuts down in order and
// exits non-zero with the reason, and a restart replays the entry the
// node stopped on. It returns when serve ends.
func watchMetastoreHalt(ctx context.Context, ms metastoreHalter, failServe func(error)) {
	select {
	case <-ctx.Done():
	case <-ms.Halted():
		failServe(ms.HaltErr())
	}
}
