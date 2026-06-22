package metastore

import (
	"time"

	bolt "go.etcd.io/bbolt"
)

type MetricsRecorder interface {
	ObserveMetastoreTx(operation, mode, status string, duration time.Duration)
	ObserveMetastoreBboltStats(stats BboltStats)
}

type BboltStats struct {
	OpenReadTransactions int
	ReadTransactions     int
	FreePages            int
	PendingPages         int
	FreeAllocBytes       int
	FreelistInuseBytes   int
	Writes               int64
	WriteSeconds         float64
	SpillSeconds         float64
	RebalanceSeconds     float64
}

func bboltStatsFrom(stats bolt.Stats) BboltStats {
	return BboltStats{
		OpenReadTransactions: stats.OpenTxN,
		ReadTransactions:     stats.TxN,
		FreePages:            stats.FreePageN,
		PendingPages:         stats.PendingPageN,
		FreeAllocBytes:       stats.FreeAlloc,
		FreelistInuseBytes:   stats.FreelistInuse,
		Writes:               stats.TxStats.GetWrite(),
		WriteSeconds:         stats.TxStats.GetWriteTime().Seconds(),
		SpillSeconds:         stats.TxStats.GetSpillTime().Seconds(),
		RebalanceSeconds:     stats.TxStats.GetRebalanceTime().Seconds(),
	}
}

func statusForErr(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}
