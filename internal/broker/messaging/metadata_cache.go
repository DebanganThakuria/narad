package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type cachedAssignments struct {
	values  []metastore.Assignment
	byPart  map[int]metastore.Assignment
	version uint64
}

type routingMember struct {
	Status metastore.MemberStatus
	Addr   string
}

type cachedRoutingMember struct {
	value   routingMember
	version uint64
}

type cachedTopic struct {
	value   topic.Topic
	version uint64
}

type cachedSchemaLoad struct {
	loaded  bool
	version uint64
}

type metadataVersioner interface {
	MetadataVersion() uint64
}

type topicVersioner interface {
	TopicVersion(name string) uint64
}

type assignmentVersioner interface {
	AssignmentVersion(topicName string) uint64
}

type schemaVersioner interface {
	SchemaVersion(topicName string) uint64
}

type routingMembersVersioner interface {
	RoutingMembersVersion() uint64
}

func (e *Engine) getTopic(ctx context.Context, name string) (topic.Topic, error) {
	version, ok := e.topicVersion(name)
	if !ok {
		return e.loadTopic(ctx, name)
	}

	for {
		e.cacheMu.RLock()
		cached, hit := e.topicCache[name]
		e.cacheMu.RUnlock()
		if hit && cached.version == version {
			if current, _ := e.topicVersion(name); current == version {
				return cached.value, nil
			}
			version, _ = e.topicVersion(name)
			continue
		}

		t, err := e.loadTopic(ctx, name)
		current, _ := e.topicVersion(name)
		if current != version {
			version = current
			continue
		}
		if err != nil {
			if errors.Is(err, ErrTopicNotFound) {
				e.cacheMu.Lock()
				delete(e.topicCache, name)
				e.cacheMu.Unlock()
			}
			return topic.Topic{}, err
		}
		e.cacheMu.Lock()
		e.topicCache[name] = cachedTopic{value: t, version: version}
		e.cacheMu.Unlock()
		return t, nil
	}
}

func (e *Engine) loadTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := e.metastore.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrTopicNotFound
		}
		return topic.Topic{}, fmt.Errorf("messaging: get topic: %w", err)
	}
	return t, nil
}

func (e *Engine) listAssignments(topicName string) ([]metastore.Assignment, error) {
	cached, ok, err := e.assignmentsForTopic(topicName)
	if err != nil || !ok {
		return nil, err
	}
	return cached.values, nil
}

func (e *Engine) getAssignment(topicName string, partition int) (metastore.Assignment, error) {
	cached, ok, err := e.assignmentsForTopic(topicName)
	if err != nil {
		return metastore.Assignment{}, err
	}
	if !ok {
		return metastore.Assignment{}, errs.ErrNotFound
	}
	assignment, ok := cached.byPart[partition]
	if !ok {
		return metastore.Assignment{}, errs.ErrNotFound
	}
	return assignment, nil
}

func (e *Engine) assignmentsForTopic(topicName string) (cachedAssignments, bool, error) {
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return cachedAssignments{}, false, nil
	}

	version, versioned := e.assignmentVersion(topicName)
	if !versioned {
		rows, err := assignments.ListAssignments(topicName)
		if err != nil {
			return cachedAssignments{}, true, err
		}
		return buildCachedAssignments(rows, 0), true, nil
	}

	for {
		e.cacheMu.RLock()
		cached, hit := e.assignmentCache[topicName]
		e.cacheMu.RUnlock()
		if hit && cached.version == version {
			if current, _ := e.assignmentVersion(topicName); current == version {
				return cached, true, nil
			}
			version, _ = e.assignmentVersion(topicName)
			continue
		}

		rows, err := assignments.ListAssignments(topicName)
		current, _ := e.assignmentVersion(topicName)
		if current != version {
			version = current
			continue
		}
		if err != nil {
			return cachedAssignments{}, true, err
		}
		cached = buildCachedAssignments(rows, version)
		e.cacheMu.Lock()
		e.assignmentCache[topicName] = cached
		e.cacheMu.Unlock()
		return cached, true, nil
	}
}

func buildCachedAssignments(rows []metastore.Assignment, version uint64) cachedAssignments {
	cached := cachedAssignments{
		values:  rows,
		byPart:  make(map[int]metastore.Assignment, len(rows)),
		version: version,
	}
	for _, row := range rows {
		cached.byPart[row.Partition] = row
	}
	return cached
}

func (e *Engine) getRoutingMember(id string) (routingMember, error) {
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return routingMember{}, errs.ErrNotFound
	}

	version, versioned := e.routingMembersVersion()
	if !versioned {
		return loadRoutingMember(assignments, id)
	}

	for {
		e.cacheMu.RLock()
		cached, hit := e.memberCache[id]
		e.cacheMu.RUnlock()
		if hit && cached.version == version {
			if current, _ := e.routingMembersVersion(); current == version {
				return cached.value, nil
			}
			version, _ = e.routingMembersVersion()
			continue
		}

		member, err := loadRoutingMember(assignments, id)
		current, _ := e.routingMembersVersion()
		if current != version {
			version = current
			continue
		}
		if err != nil {
			e.cacheMu.Lock()
			delete(e.memberCache, id)
			e.cacheMu.Unlock()
			return routingMember{}, err
		}
		e.cacheMu.Lock()
		e.memberCache[id] = cachedRoutingMember{value: member, version: version}
		e.cacheMu.Unlock()
		return member, nil
	}
}

func loadRoutingMember(assignments assignmentReader, id string) (routingMember, error) {
	member, err := assignments.GetMember(id)
	if err != nil {
		return routingMember{}, err
	}
	return routingMember{Status: member.Status, Addr: member.Addr}, nil
}

func (e *Engine) topicVersion(name string) (uint64, bool) {
	if versioner, ok := e.metastore.(topicVersioner); ok {
		return versioner.TopicVersion(name), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) assignmentVersion(topicName string) (uint64, bool) {
	if versioner, ok := e.metastore.(assignmentVersioner); ok {
		return versioner.AssignmentVersion(topicName), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) schemaVersion(topicName string) (uint64, bool) {
	if versioner, ok := e.metastore.(schemaVersioner); ok {
		return versioner.SchemaVersion(topicName), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) routingMembersVersion() (uint64, bool) {
	if versioner, ok := e.metastore.(routingMembersVersioner); ok {
		return versioner.RoutingMembersVersion(), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) globalMetadataVersion() (uint64, bool) {
	versioner, ok := e.metastore.(metadataVersioner)
	if !ok {
		return 0, false
	}
	return versioner.MetadataVersion(), true
}
