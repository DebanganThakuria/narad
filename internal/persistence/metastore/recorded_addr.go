package metastore

import (
	"log/slog"

	"github.com/debanganthakuria/narad/internal/platform/netaddr"
	"github.com/hashicorp/raft"
)

// reportRecordedAddress says at startup when the Raft configuration
// records this node at an address other than the one it now advertises.
//
// A node's address enters the Raft configuration once: at bootstrap for
// a node that seeds a cluster, through the leader's AddVoter for one
// that joins. The other voters dial that recorded address, and a later
// cluster.addr or cluster.advertise_addr changes only this node's
// transport, not the configuration. So a node whose Raft first started
// on a loopback address and was rebound to grow it is recorded at the
// loopback address in the configuration the joiners replicate: once it
// is not the leader they cannot reach its Raft, and a joiner bound to
// every interface that dials the loopback address reaches its own Raft
// and steps down. Nothing else names that cause, so this does, at error
// level, with both addresses. It changes nothing.
//
// A node alone in the configuration that advertises a loopback address
// takes no peers, so no other node dials either address: that is the
// upgraded single node rebound to loopback, which needs no action and
// gets an info line.
func reportRecordedAddress(log *slog.Logger, conf raft.Configuration, nodeID, advertise string) {
	var recorded string
	found, others := false, 0
	for _, srv := range conf.Servers {
		if string(srv.ID) != nodeID {
			others++
			continue
		}
		recorded, found = string(srv.Address), true
	}
	if !found || recorded == advertise {
		return
	}
	if others == 0 && netaddr.IsLoopbackHostPort(advertise) {
		log.Info("raft configuration records this node at an address other than the one it advertises; harmless while no other node is in the configuration",
			"node", nodeID, "recorded_addr", recorded, "advertise_addr", advertise)
		return
	}
	log.Error("raft configuration records this node at an address other than the one it advertises: other nodes dial the recorded address, and a later cluster.addr does not change it, so if the recorded address does not reach this node they cannot reach its raft once it is not the leader; operator action required",
		"node", nodeID, "recorded_addr", recorded, "advertise_addr", advertise, "other_servers", others)
}
