package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// produceDispatchTarget identifies where a produce record commits: the local
// broker (local=true) or a remote owner at addr.
type produceDispatchTarget struct {
	local     bool
	addr      string
	topic     string
	partition int
}

type cachedProduceDispatchTarget struct {
	target produceDispatchTarget
	err    error
}

type cachedProduceDispatchTargets struct {
	assignmentVersion     uint64
	routingMembersVersion uint64
	byPartition           map[int]cachedProduceDispatchTarget
}

func (d *ProduceDispatcher) dispatchTarget(record ingress.ProduceRecord) (produceDispatchTarget, error) {
	if d.store == nil {
		return produceDispatchTarget{}, errors.New("produce dispatcher metastore is nil")
	}
	targets, err := d.dispatchTargetsForTopic(record.Topic)
	if err != nil {
		return produceDispatchTarget{}, err
	}

	cached, ok := targets.byPartition[record.TargetPartition]
	if !ok {
		return produceDispatchTarget{}, fmt.Errorf("lookup assignment: %w", errs.ErrNotFound)
	}
	if cached.err != nil {
		return produceDispatchTarget{}, cached.err
	}
	return cached.target, nil
}

// dispatchTargetsForTopic returns the per-partition commit targets for a
// topic, cached and keyed by the store's assignment and routing-member
// versions. Versions are re-read after every store call: the reads are not
// atomic with the version counters, so a table is only cached (or a cache hit
// trusted) once a full pass observed stable versions — otherwise a concurrent
// reassignment or member change could freeze a stale table.
func (d *ProduceDispatcher) dispatchTargetsForTopic(topicName string) (cachedProduceDispatchTargets, error) {
	assignmentVersion := d.store.AssignmentVersion(topicName)
	routingMembersVersion := d.store.RoutingMembersVersion()

	d.targetMu.RLock()
	cached, ok := d.targetCache[topicName]
	d.targetMu.RUnlock()
	if ok && cached.assignmentVersion == assignmentVersion && cached.routingMembersVersion == routingMembersVersion {
		if d.store.AssignmentVersion(topicName) == assignmentVersion && d.store.RoutingMembersVersion() == routingMembersVersion {
			return cached, nil
		}
		assignmentVersion = d.store.AssignmentVersion(topicName)
		routingMembersVersion = d.store.RoutingMembersVersion()
	}

	for {
		assignments, err := d.store.ListAssignments(topicName)
		currentAssignmentVersion := d.store.AssignmentVersion(topicName)
		currentRoutingMembersVersion := d.store.RoutingMembersVersion()
		if currentAssignmentVersion != assignmentVersion || currentRoutingMembersVersion != routingMembersVersion {
			assignmentVersion = currentAssignmentVersion
			routingMembersVersion = currentRoutingMembersVersion
			continue
		}
		if err != nil {
			return cachedProduceDispatchTargets{}, fmt.Errorf("lookup assignment: %w", err)
		}
		if len(assignments) == 0 {
			// The topic is deleted, or new and not assigned yet. Drop
			// the entry rather than cache an empty table (as
			// route_cache.go does): the cache would otherwise keep one
			// entry for every topic name ever dispatched.
			d.targetMu.Lock()
			delete(d.targetCache, topicName)
			d.targetMu.Unlock()
			return cachedProduceDispatchTargets{assignmentVersion: assignmentVersion, routingMembersVersion: routingMembersVersion}, nil
		}

		targets := cachedProduceDispatchTargets{
			assignmentVersion:     assignmentVersion,
			routingMembersVersion: routingMembersVersion,
			byPartition:           make(map[int]cachedProduceDispatchTarget, len(assignments)),
		}
		needsMembers := false
		for _, assignment := range assignments {
			if d.selfID != "" && assignment.OwnerID != d.selfID {
				needsMembers = true
				break
			}
		}
		memberByID := map[string]metastore.Member{}
		if needsMembers {
			members, err := d.store.ListMembers()
			currentAssignmentVersion = d.store.AssignmentVersion(topicName)
			currentRoutingMembersVersion = d.store.RoutingMembersVersion()
			if currentAssignmentVersion != assignmentVersion || currentRoutingMembersVersion != routingMembersVersion {
				assignmentVersion = currentAssignmentVersion
				routingMembersVersion = currentRoutingMembersVersion
				continue
			}
			if err != nil {
				return cachedProduceDispatchTargets{}, fmt.Errorf("lookup owner member: %w", err)
			}
			memberByID = make(map[string]metastore.Member, len(members))
			for _, member := range members {
				memberByID[member.ID] = member
			}
		}

		for _, assignment := range assignments {
			target := produceDispatchTarget{
				local:     d.selfID == "" || assignment.OwnerID == d.selfID,
				topic:     topicName,
				partition: assignment.Partition,
			}
			if target.local {
				targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{target: target}
				continue
			}
			member, ok := memberByID[assignment.OwnerID]
			if !ok {
				targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{
					err: fmt.Errorf("lookup owner member: %w", errs.ErrNotFound),
				}
				continue
			}
			if member.Status == metastore.MemberDead || member.Addr == "" {
				targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{
					err: fmt.Errorf("owner %q is unavailable", assignment.OwnerID),
				}
				continue
			}
			target.addr = member.Addr
			targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{target: target}
		}

		d.targetMu.Lock()
		d.targetCache[topicName] = targets
		d.targetMu.Unlock()
		return targets, nil
	}
}

// cachedDispatchTopic is the part of a topic's record the dispatcher
// reads per record: whether it exists, its incarnation and its
// partition count, cached by the store's topic version.
type cachedDispatchTopic struct {
	version    uint64
	exists     bool
	id         string
	partitions int
}

// topicInfo returns topicName's cached record from the local replica.
// The version is re-read after the lookup, so an entry is only cached
// once it is known to match a version. An absent topic is not cached at
// all, so a churn of deleted names cannot grow the cache, and neither is
// a failed read, so the next call tries again.
func (d *ProduceDispatcher) topicInfo(st *produceDispatchState, topicName string) cachedDispatchTopic {
	version := d.store.TopicVersion(topicName)
	if cached, ok := st.topics[topicName]; ok && cached.version == version {
		return cached
	}
	t, err := d.store.GetTopic(context.Background(), topicName)
	info := cachedDispatchTopic{version: version, exists: err == nil, id: t.ID, partitions: t.Partitions}
	if err != nil && !errors.Is(err, errs.ErrNotFound) {
		// Unreadable: treat it as present without an incarnation, so
		// nothing is discarded on its account.
		info.exists = true
	}
	if err != nil || d.store.TopicVersion(topicName) != version {
		delete(st.topics, topicName)
		return info
	}
	if len(st.topics) >= produceDispatchMemoLimit {
		clear(st.topics)
	}
	st.topics[topicName] = info
	return info
}
