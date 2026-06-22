package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type cachedAssignments struct {
	values  []metastore.Assignment
	byPart  map[int]metastore.Assignment
	expires time.Time
}

type cachedMember struct {
	value   metastore.Member
	expires time.Time
}

type cachedTopic struct {
	value   topic.Topic
	expires time.Time
}

type cachedSchemaLoad struct {
	loaded  bool
	expires time.Time
}

type metadataVersioner interface {
	MetadataVersion() uint64
}

func (e *Engine) syncMetadataCacheVersion() {
	versioner, ok := e.metastore.(metadataVersioner)
	if !ok {
		return
	}
	version := versioner.MetadataVersion()

	e.cacheMu.RLock()
	current := e.cacheVersion
	e.cacheMu.RUnlock()
	if current == version {
		return
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	if e.cacheVersion == version {
		return
	}
	clear(e.topicCache)
	clear(e.assignmentCache)
	clear(e.memberCache)
	clear(e.schemaLoadCache)
	e.cacheVersion = version
}

func (e *Engine) getTopic(ctx context.Context, name string) (topic.Topic, error) {
	e.syncMetadataCacheVersion()
	now := time.Now()
	e.cacheMu.RLock()
	cached, hit := e.topicCache[name]
	e.cacheMu.RUnlock()
	if hit && now.Before(cached.expires) {
		return cached.value, nil
	}

	t, err := e.metastore.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrTopicNotFound
		}
		return topic.Topic{}, fmt.Errorf("messaging: get topic: %w", err)
	}
	e.cacheMu.Lock()
	e.topicCache[name] = cachedTopic{value: t, expires: now.Add(e.cacheTTL)}
	e.cacheMu.Unlock()
	return t, nil
}

func (e *Engine) getTopicFresh(ctx context.Context, name string) (topic.Topic, error) {
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

	e.syncMetadataCacheVersion()
	now := time.Now()
	e.cacheMu.RLock()
	cached, hit := e.assignmentCache[topicName]
	e.cacheMu.RUnlock()
	if hit && now.Before(cached.expires) {
		return cached, true, nil
	}

	rows, err := assignments.ListAssignments(topicName)
	if err != nil {
		return cachedAssignments{}, true, err
	}
	cached = cachedAssignments{
		values:  rows,
		byPart:  make(map[int]metastore.Assignment, len(rows)),
		expires: now.Add(e.cacheTTL),
	}
	for _, row := range rows {
		cached.byPart[row.Partition] = row
	}

	e.cacheMu.Lock()
	e.assignmentCache[topicName] = cached
	e.cacheMu.Unlock()
	return cached, true, nil
}

func (e *Engine) getMember(id string) (metastore.Member, error) {
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return metastore.Member{}, errs.ErrNotFound
	}

	e.syncMetadataCacheVersion()
	now := time.Now()
	e.cacheMu.RLock()
	cached, hit := e.memberCache[id]
	e.cacheMu.RUnlock()
	if hit && now.Before(cached.expires) {
		return cached.value, nil
	}

	member, err := assignments.GetMember(id)
	if err != nil {
		return metastore.Member{}, err
	}
	e.cacheMu.Lock()
	e.memberCache[id] = cachedMember{value: member, expires: now.Add(e.cacheTTL)}
	e.cacheMu.Unlock()
	return member, nil
}
