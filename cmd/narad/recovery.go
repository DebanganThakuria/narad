package main

import (
	"context"
	"log/slog"
	"time"
)

const storeRecoveryRetryInterval = 5 * time.Second

type storeRepairer interface {
	RepairOwnedPartitions(context.Context) error
}

func runStoreRecoveryRepairer(ctx context.Context, repairer storeRepairer, interval time.Duration, log *slog.Logger) {
	if interval <= 0 {
		interval = storeRecoveryRetryInterval
	}

	for {
		if err := repairer.RepairOwnedPartitions(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("repair owned partitions failed; retrying", "err", err, "retry_after", interval)
		} else {
			log.Info("repair owned partitions complete")
			return
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
