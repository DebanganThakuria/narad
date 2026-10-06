package ingress

import "errors"

// errPendingLimit stops a PendingForTopic scan at its limit.
var errPendingLimit = errors.New("ingress: pending scan limit reached")

// PendingForTopic counts one topic's records in this node's dispatch
// backlog: the range DispatchBacklog measures, from the stored dispatch
// checkpoint to the durable end, replayed and matched record by record.
// That range holds every record this node accepted (answered 202) but
// has not yet committed to its partition, which nothing downstream of
// the partition log can see yet. It can also hold records already
// committed whose checkpoint was not stored yet, so the count is an
// upper bound: the conservative side for refusing a delete.
//
// A record matches on its TopicID when both it and topicID are set, and
// on the topic name otherwise (a record accepted before incarnations
// were stamped, or a topic created before IDs existed). A name match can
// count a record of an earlier topic of the same name: an over-count.
//
// The scan stops at limit records and reports complete=false: a backlog
// that large counts as unshipped. It runs only on a delete, never on the
// produce path.
func (m *Manager) PendingForTopic(topicID, name string, limit int) (count uint64, complete bool, err error) {
	if m == nil || m.log == nil {
		return 0, true, nil
	}
	from := m.storedCheckpoint.Load()
	if from >= m.durableNext.Load() {
		return 0, true, nil
	}
	err = m.ReplayProduce(from, func(rec ProduceRecord) error {
		if !pendingMatch(rec, topicID, name) {
			return nil
		}
		count++
		if limit > 0 && count >= uint64(limit) {
			return errPendingLimit
		}
		return nil
	})
	if errors.Is(err, errPendingLimit) {
		return count, false, nil
	}
	if err != nil {
		return count, false, err
	}
	return count, true, nil
}

func pendingMatch(rec ProduceRecord, topicID, name string) bool {
	if rec.TopicID != "" && topicID != "" {
		return rec.TopicID == topicID
	}
	return rec.Topic == name
}
