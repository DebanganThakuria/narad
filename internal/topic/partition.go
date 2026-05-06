package topic

// Partition identifies a single partition belonging to a topic, along
// with the (logical) replica assignment. Leader and Replicas are
// populated by the partition manager once node membership is wired up;
// they remain zero in the single-node wiring pass.
type Partition struct {
	Topic    string   `json:"topic"`
	Index    int      `json:"index"`
	Leader   string   `json:"leader,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
}
