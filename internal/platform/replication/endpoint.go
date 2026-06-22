package replication

import (
	"strings"

	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

const replicatePath = "/internal/v1/replicate"

func replicateEndpoint(addr string) string {
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr + replicatePath
	}
	return "http://" + addr + replicatePath
}

func streamEndpoint(addr string) string {
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr + replicationwire.StreamPath
	}
	return "http://" + addr + replicationwire.StreamPath
}
