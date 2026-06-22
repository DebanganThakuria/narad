package metastore

import (
	"time"
)

type MetricsRecorder interface {
	ObserveMetastoreTx(operation, mode, status string, duration time.Duration)
}

func statusForErr(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}
