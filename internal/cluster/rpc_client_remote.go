package cluster

import (
	"context"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// RemoteWrite forwards a remote write to the leader at addr.
func (c *PeerClient) RemoteWrite(ctx context.Context, addr string, req nodewire.RemoteWriteRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeRemoteWriteRequest(req)
	return c.send(ctx, addr, "remote_write", laneControl, payload, err)
}

// RemoteCheck asks the member at addr to run a check, report its
// status or count its unshipped records.
func (c *PeerClient) RemoteCheck(ctx context.Context, addr string, req nodewire.RemoteCheckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeRemoteCheckRequest(req)
	return c.send(ctx, addr, "remote_check", laneControl, payload, err)
}
