package metrics

import (
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func (m *Metrics) ObserveMetastoreBboltStats(stats metastore.BboltStats) {
	if m == nil {
		return
	}
	m.MetastoreBboltOpenReadTx.Set(float64(stats.OpenReadTransactions))
	m.MetastoreBboltReadTx.Set(float64(stats.ReadTransactions))
	m.MetastoreBboltFreePages.Set(float64(stats.FreePages))
	m.MetastoreBboltPendingPages.Set(float64(stats.PendingPages))
	m.MetastoreBboltFreeAllocBytes.Set(float64(stats.FreeAllocBytes))
	m.MetastoreBboltFreelistInuse.Set(float64(stats.FreelistInuseBytes))
	m.MetastoreBboltWrites.Set(float64(stats.Writes))
	m.MetastoreBboltWriteSeconds.Set(stats.WriteSeconds)
	m.MetastoreBboltSpillSeconds.Set(stats.SpillSeconds)
	m.MetastoreBboltRebalanceSeconds.Set(stats.RebalanceSeconds)
}
