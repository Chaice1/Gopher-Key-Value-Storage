package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	termVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_term",
		Help: "Current Raft term",
	}, []string{"node_id"})

	stateVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_state",
		Help: "Node state: 0=follower, 1=candidate, 2=leader",
	}, []string{"node_id"})

	electionsVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "raft_total_elections",
		Help: "Elections started",
	}, []string{"node_id"})

	commitsVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "raft_commited_commands",
		Help: "Commands commited",
	}, []string{"node_id"})
)

type Metrics struct {
	Term             prometheus.Gauge
	State            prometheus.Gauge
	TotalElections   prometheus.Counter
	CommitedCommands prometheus.Counter
}

func NewMetrics(nodeID string) *Metrics {

	return &Metrics{
		Term:             termVec.WithLabelValues(nodeID),
		State:            stateVec.WithLabelValues(nodeID),
		TotalElections:   electionsVec.WithLabelValues(nodeID),
		CommitedCommands: commitsVec.WithLabelValues(nodeID),
	}
}
