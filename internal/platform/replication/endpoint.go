package replication

import "strings"

const replicatePath = "/internal/v1/replicate"

func replicateEndpoint(addr string) string {
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr + replicatePath
	}
	return "http://" + addr + replicatePath
}
