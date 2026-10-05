package metastore

// Metastore and Raft metrics. Every value is read when Prometheus
// scrapes: the FSM's counters are atomics its apply, snapshot and
// restore paths bump, and Raft's are read through Stats, State,
// LastContact, LeaderWithID and GetConfiguration, none of which waits
// on Raft's main loop. So a scrape still answers while the node is
// wedged, which is when these series matter most.

import (
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	descFSMBytes = prometheus.NewDesc("narad_metastore_fsm_bytes",
		"Size of the metadata database file (fsm.db) on disk. bbolt never shrinks the file, and each snapshot needs as much free disk for its copy.", nil, nil)
	descAppliedIndex = prometheus.NewDesc("narad_metastore_applied_index",
		"Highest Raft index whose effects are in this node's metadata database.", nil, nil)
	descApplyErrors = prometheus.NewDesc("narad_metastore_apply_errors_total",
		"Raft entries the metadata FSM could not apply as proposed, by kind: storage (a write the local disk refused; retried, then the node stops), unknown_entry_type (proposed by a newer release; the node stops), undecodable (consumed, identical on every node), newer_database (a snapshot from a newer release; the node stops).", []string{"kind"}, nil)
	descApplyStalled = prometheus.NewDesc("narad_metastore_apply_stalled",
		"1 while the metadata FSM retries a Raft entry its local disk refused, else 0.", nil, nil)
	descApplyStopped = prometheus.NewDesc("narad_metastore_apply_stopped",
		"1 once the metadata FSM has stopped applying Raft entries (the node leaves Raft and exits), else 0.", nil, nil)
	descSnapshotBytes = prometheus.NewDesc("narad_metastore_snapshot_bytes",
		"Size of the last Raft snapshot of the metadata database this node persisted (0 before the first).", nil, nil)
	descSnapshotDuration = prometheus.NewDesc("narad_metastore_snapshot_duration_seconds",
		"How long the last Raft snapshot this node persisted took, from the copy of fsm.db to the snapshot file's close (0 before the first).", nil, nil)
	descSnapshotFailures = prometheus.NewDesc("narad_metastore_snapshot_failures_total",
		"Raft snapshots of the metadata database that failed on this node (no room for the copy, a failed write); Raft tries again at its next interval.", nil, nil)
)

// applyErrorKinds are the label values of narad_metastore_apply_errors_total,
// indexed like fsmState.applyErrors.
var applyErrorKinds = [applyErrKinds]string{
	applyErrStorage:          "storage",
	applyErrUnknownEntryType: "unknown_entry_type",
	applyErrUndecodable:      "undecodable",
	applyErrNewerDatabase:    "newer_database",
}

// metastoreCollector exports the FSM's series.
type metastoreCollector struct{ s *Store }

func (c *metastoreCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descFSMBytes, descAppliedIndex, descApplyErrors, descApplyStalled, descApplyStopped,
		descSnapshotBytes, descSnapshotDuration, descSnapshotFailures,
	} {
		ch <- d
	}
}

func (c *metastoreCollector) Collect(ch chan<- prometheus.Metric) {
	f := c.s.fsm
	if st, err := os.Stat(f.dbPath); err == nil {
		ch <- prometheus.MustNewConstMetric(descFSMBytes, prometheus.GaugeValue, float64(st.Size()))
	}
	ch <- prometheus.MustNewConstMetric(descAppliedIndex, prometheus.GaugeValue, float64(c.s.AppliedIndex()))
	for kind, name := range applyErrorKinds {
		ch <- prometheus.MustNewConstMetric(descApplyErrors, prometheus.CounterValue, float64(f.applyErrors[kind].Load()), name)
	}
	ch <- prometheus.MustNewConstMetric(descApplyStalled, prometheus.GaugeValue, boolValue(f.stalled.Load()))
	ch <- prometheus.MustNewConstMetric(descApplyStopped, prometheus.GaugeValue, boolValue(f.stopErr() != nil))
	ch <- prometheus.MustNewConstMetric(descSnapshotBytes, prometheus.GaugeValue, float64(f.snapshotBytes.Load()))
	ch <- prometheus.MustNewConstMetric(descSnapshotDuration, prometheus.GaugeValue, time.Duration(f.snapshotNanos.Load()).Seconds())
	ch <- prometheus.MustNewConstMetric(descSnapshotFailures, prometheus.CounterValue, float64(f.snapshotFailures.Load()))
}

var (
	descRaftState = prometheus.NewDesc("narad_raft_state",
		"Raft state of this node: 1 for the current state, 0 for the other three.", []string{"state"}, nil)
	descRaftTerm = prometheus.NewDesc("narad_raft_term",
		"Current Raft term.", nil, nil)
	descRaftLastLogIndex = prometheus.NewDesc("narad_raft_last_log_index",
		"Index of the last entry in this node's Raft log.", nil, nil)
	descRaftCommitIndex = prometheus.NewDesc("narad_raft_commit_index",
		"Raft commit index as known to this node.", nil, nil)
	descRaftAppliedIndex = prometheus.NewDesc("narad_raft_applied_index",
		"Last Raft index handed to this node's FSM (narad_metastore_applied_index says when its effects are in fsm.db).", nil, nil)
	descRaftFSMPending = prometheus.NewDesc("narad_raft_fsm_pending",
		"Batches of committed entries queued for this node's FSM and not yet applied.", nil, nil)
	descRaftHasLeader = prometheus.NewDesc("narad_raft_has_leader",
		"1 while this node knows a Raft leader (itself included), else 0.", nil, nil)
	descRaftLastContact = prometheus.NewDesc("narad_raft_last_contact_seconds",
		"Seconds since this node last heard from the Raft leader: 0 on the leader, and the time since the store opened on a node that has never heard from one.", nil, nil)
	descRaftVoters = prometheus.NewDesc("narad_raft_voters",
		"Voters in the latest Raft configuration this node knows.", nil, nil)
	descRaftNonvoters = prometheus.NewDesc("narad_raft_nonvoters",
		"Non-voting servers in the latest Raft configuration this node knows.", nil, nil)
)

// raftStates are the label values of narad_raft_state.
var raftStates = [...]string{"follower", "candidate", "leader", "shutdown"}

// raftCollector exports Raft's own state.
type raftCollector struct{ s *Store }

func (c *raftCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descRaftState, descRaftTerm, descRaftLastLogIndex, descRaftCommitIndex, descRaftAppliedIndex,
		descRaftFSMPending, descRaftHasLeader, descRaftLastContact, descRaftVoters, descRaftNonvoters,
	} {
		ch <- d
	}
}

func (c *raftCollector) Collect(ch chan<- prometheus.Metric) {
	r := c.s.r
	state := raftStateLabel(r.State())
	for _, st := range raftStates {
		ch <- prometheus.MustNewConstMetric(descRaftState, prometheus.GaugeValue, boolValue(st == state), st)
	}
	stats := r.Stats()
	for _, g := range []struct {
		desc *prometheus.Desc
		key  string
	}{
		{descRaftTerm, "term"},
		{descRaftLastLogIndex, "last_log_index"},
		{descRaftCommitIndex, "commit_index"},
		{descRaftAppliedIndex, "applied_index"},
		{descRaftFSMPending, "fsm_pending"},
	} {
		if v, err := strconv.ParseUint(stats[g.key], 10, 64); err == nil {
			ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, float64(v))
		}
	}
	addr, _ := r.LeaderWithID()
	ch <- prometheus.MustNewConstMetric(descRaftHasLeader, prometheus.GaugeValue, boolValue(addr != ""))
	ch <- prometheus.MustNewConstMetric(descRaftLastContact, prometheus.GaugeValue, c.s.lastContactSeconds())
	voters, nonvoters := 0, 0
	for _, srv := range r.GetConfiguration().Configuration().Servers {
		if srv.Suffrage == raft.Voter {
			voters++
		} else {
			nonvoters++
		}
	}
	ch <- prometheus.MustNewConstMetric(descRaftVoters, prometheus.GaugeValue, float64(voters))
	ch <- prometheus.MustNewConstMetric(descRaftNonvoters, prometheus.GaugeValue, float64(nonvoters))
}

// lastContactSeconds is 0 on the leader, the age of the last contact
// with the leader on any other node, and the time since the store
// opened on a node that has never heard from one: "no contact for at
// least this long", which an alert threshold reads correctly.
func (s *Store) lastContactSeconds() float64 {
	if s.r.State() == raft.Leader {
		return 0
	}
	if lc := s.r.LastContact(); !lc.IsZero() {
		return time.Since(lc).Seconds()
	}
	return time.Since(s.opened).Seconds()
}

func raftStateLabel(st raft.RaftState) string {
	switch st {
	case raft.Follower:
		return "follower"
	case raft.Candidate:
		return "candidate"
	case raft.Leader:
		return "leader"
	default:
		return "shutdown"
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// registerMetrics adds the store's collectors to reg (nil: no metrics).
// A collector that cannot be registered (another store in this process
// holds the names) is logged at error and skipped: metrics never stop
// the store from opening.
func (s *Store) registerMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	for _, c := range []prometheus.Collector{&metastoreCollector{s: s}, &raftCollector{s: s}} {
		if err := reg.Register(c); err != nil {
			s.log.Error("metastore: could not register the metastore and raft metrics; they are missing from /metrics, and alerts on them cannot fire", "error", err)
			continue
		}
		s.collectors = append(s.collectors, c)
	}
	s.registerer = reg
}

// unregisterMetrics takes the store's collectors off the registry, so a
// store opened after this one in the same process exports its own.
func (s *Store) unregisterMetrics() {
	for _, c := range s.collectors {
		s.registerer.Unregister(c)
	}
	s.collectors = nil
}
