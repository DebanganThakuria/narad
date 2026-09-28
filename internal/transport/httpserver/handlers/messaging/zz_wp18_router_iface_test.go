package messaging

import "github.com/debanganthakuria/narad/internal/cluster"

// The cluster router implements the optional batch surfaces the handlers
// assert for; a signature drift would otherwise silently drop them back
// to one record per forward.
var (
	_ batchConsumeRouter = (*cluster.Router)(nil)
	_ batchAckRouter     = (*cluster.Router)(nil)
)
