package cluster

import (
	"sort"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type cachedRouteTable struct {
	expires       time.Time
	entries       []routeEntry
	localEntries  []routeEntry
	remoteEntries []routeEntry
	byPartition   map[int]routeEntry
	partitions    int
}

type routeEntry struct {
	partition int

	ownerID    string
	ownerAddr  string
	ownerAlive bool

	followerID    string
	followerAddr  string
	followerAlive bool
}

func (rt *Router) routesForTopic(topicName string) (cachedRouteTable, bool) {
	start := time.Now()
	now := time.Now()
	rt.syncRouteCacheVersion()
	rt.routeMu.RLock()
	cached, ok := rt.routes[topicName]
	rt.routeMu.RUnlock()
	if ok && now.Before(cached.expires) {
		rt.observe("route_cache", "lookup", "hit", time.Since(start))
		return cached, true
	}

	assignments, err := rt.store.ListAssignments(topicName)
	if err != nil || len(assignments) == 0 {
		rt.observe("route_cache", "lookup", "miss", time.Since(start))
		return cachedRouteTable{}, false
	}
	members, err := rt.store.ListMembers()
	if err != nil {
		rt.observe("route_cache", "lookup", "error", time.Since(start))
		return cachedRouteTable{}, false
	}

	memberByID := make(map[string]metastore.Member, len(members))
	for _, member := range members {
		memberByID[member.ID] = member
	}

	table := cachedRouteTable{
		expires:       now.Add(rt.routeTTL),
		entries:       make([]routeEntry, 0, len(assignments)),
		localEntries:  make([]routeEntry, 0, len(assignments)),
		remoteEntries: make([]routeEntry, 0, len(assignments)),
		byPartition:   make(map[int]routeEntry, len(assignments)),
	}
	for _, assignment := range assignments {
		entry := routeEntry{
			partition:  assignment.Partition,
			ownerID:    assignment.OwnerID,
			followerID: assignment.FollowerID,
		}
		if owner, ok := memberByID[assignment.OwnerID]; ok {
			entry.ownerAddr = owner.Addr
			entry.ownerAlive = owner.Status != metastore.MemberDead
		}
		if follower, ok := memberByID[assignment.FollowerID]; ok {
			entry.followerAddr = follower.Addr
			entry.followerAlive = follower.Status != metastore.MemberDead
		}
		table.entries = append(table.entries, entry)
		table.byPartition[entry.partition] = entry
		if entry.ownerID == rt.selfID {
			table.localEntries = append(table.localEntries, entry)
		} else {
			table.remoteEntries = append(table.remoteEntries, entry)
		}
		if entry.partition+1 > table.partitions {
			table.partitions = entry.partition + 1
		}
	}
	sortRoutes(table.entries)
	sortRoutes(table.localEntries)
	sortRoutes(table.remoteEntries)

	rt.routeMu.Lock()
	rt.routes[topicName] = table
	rt.routeMu.Unlock()
	rt.observe("route_cache", "lookup", "fill", time.Since(start))
	return table, true
}

func (rt *Router) syncRouteCacheVersion() {
	version := rt.store.MetadataVersion()
	rt.routeMu.RLock()
	current := rt.routeVersion
	rt.routeMu.RUnlock()
	if current == version {
		return
	}

	rt.routeMu.Lock()
	changed := rt.routeVersion != version
	if changed {
		clear(rt.routes)
		rt.routeVersion = version
	}
	rt.routeMu.Unlock()
	if !changed {
		return
	}

	rt.consumeMu.Lock()
	clear(rt.consumeCursor)
	rt.consumeMu.Unlock()
}

func sortRoutes(entries []routeEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].partition < entries[j].partition
	})
}

func (rt *Router) nextConsumeCursor(topicName string, n int) int {
	if n <= 0 {
		return 0
	}
	rt.consumeMu.Lock()
	cursor := rt.consumeCursor[topicName]
	rt.consumeCursor[topicName] = cursor + 1
	rt.consumeMu.Unlock()
	return int(cursor % uint64(n))
}
