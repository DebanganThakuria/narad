package cluster

import (
	"sort"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// cachedRouteTable is a routing snapshot for one topic, valid for a specific
// (assignmentVersion, routingMembersVersion) pair.
type cachedRouteTable struct {
	assignmentVersion     uint64
	routingMembersVersion uint64
	entries               []routeEntry
	localEntries          []routeEntry
	remoteEntries         []routeEntry
	byPartition           map[int]routeEntry
	partitions            int
}

type routeEntry struct {
	partition int

	ownerID    string
	ownerAddr  string
	ownerAlive bool
}

// routesForTopic returns the topic's route table, cached and keyed by the
// store's assignment and routing-member versions. Versions are re-read after
// every store call: the reads are not atomic with the version counters, so a
// table is only cached (or a cache hit trusted) once a full pass observed
// stable versions — otherwise a concurrent reassignment or member change
// could freeze a stale table.
func (rt *Router) routesForTopic(topicName string) (cachedRouteTable, bool) {
	assignmentVersion := rt.store.AssignmentVersion(topicName)
	routingMembersVersion := rt.store.RoutingMembersVersion()
	rt.routeMu.RLock()
	cached, ok := rt.routes[topicName]
	rt.routeMu.RUnlock()
	if ok && cached.assignmentVersion == assignmentVersion && cached.routingMembersVersion == routingMembersVersion {
		if rt.store.AssignmentVersion(topicName) == assignmentVersion && rt.store.RoutingMembersVersion() == routingMembersVersion {
			return cached, true
		}
		assignmentVersion = rt.store.AssignmentVersion(topicName)
		routingMembersVersion = rt.store.RoutingMembersVersion()
	}

	for {
		assignments, err := rt.store.ListAssignments(topicName)
		currentAssignmentVersion := rt.store.AssignmentVersion(topicName)
		currentRoutingMembersVersion := rt.store.RoutingMembersVersion()
		if currentAssignmentVersion != assignmentVersion || currentRoutingMembersVersion != routingMembersVersion {
			assignmentVersion = currentAssignmentVersion
			routingMembersVersion = currentRoutingMembersVersion
			continue
		}
		if err != nil || len(assignments) == 0 {
			rt.routeMu.Lock()
			delete(rt.routes, topicName)
			rt.routeMu.Unlock()
			rt.clearConsumeCursor(topicName)
			return cachedRouteTable{}, false
		}

		members, err := rt.store.ListMembers()
		currentAssignmentVersion = rt.store.AssignmentVersion(topicName)
		currentRoutingMembersVersion = rt.store.RoutingMembersVersion()
		if currentAssignmentVersion != assignmentVersion || currentRoutingMembersVersion != routingMembersVersion {
			assignmentVersion = currentAssignmentVersion
			routingMembersVersion = currentRoutingMembersVersion
			continue
		}
		if err != nil {
			return cachedRouteTable{}, false
		}

		table := rt.buildRouteTable(assignments, members, assignmentVersion, routingMembersVersion)

		rt.routeMu.Lock()
		if previous, hadPrevious := rt.routes[topicName]; !hadPrevious || previous.assignmentVersion != table.assignmentVersion {
			rt.clearConsumeCursor(topicName)
		}
		rt.routes[topicName] = table
		rt.routeMu.Unlock()
		return table, true
	}
}

func (rt *Router) buildRouteTable(assignments []metastore.Assignment, members []metastore.Member, assignmentVersion, routingMembersVersion uint64) cachedRouteTable {
	memberByID := make(map[string]metastore.Member, len(members))
	for _, member := range members {
		memberByID[member.ID] = member
	}

	table := cachedRouteTable{
		assignmentVersion:     assignmentVersion,
		routingMembersVersion: routingMembersVersion,
		entries:               make([]routeEntry, 0, len(assignments)),
		localEntries:          make([]routeEntry, 0, len(assignments)),
		remoteEntries:         make([]routeEntry, 0, len(assignments)),
		byPartition:           make(map[int]routeEntry, len(assignments)),
	}
	for _, assignment := range assignments {
		entry := routeEntry{
			partition: assignment.Partition,
			ownerID:   assignment.OwnerID,
		}
		if owner, ok := memberByID[assignment.OwnerID]; ok {
			entry.ownerAddr = owner.Addr
			entry.ownerAlive = owner.Status != metastore.MemberDead
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
	return table
}

func (rt *Router) clearConsumeCursor(topicName string) {
	rt.consumeMu.Lock()
	delete(rt.consumeCursor, topicName)
	delete(rt.consumeCursor, topicName+":local")
	delete(rt.consumeCursor, topicName+":remote")
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
