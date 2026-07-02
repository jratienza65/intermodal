// Package metrics polls Railway's metrics API in the background and exposes the
// latest snapshot to metric exporters (Prometheus pull collector, OTLP push).
package metrics

import (
	"sync"
	"time"

	"github.com/jratienza65/intermodal/internal/railway"
)

// Labels is the identity of a single metric series. IDs come from Railway's
// metric tags; names are enriched from discovery.
type Labels struct {
	ProjectID            string
	ProjectName          string
	EnvironmentID        string
	EnvironmentName      string
	ServiceID            string
	ServiceName          string
	Region               string
	DeploymentID         string
	DeploymentInstanceID string
}

// LabelNames is the fixed, ordered set of Prometheus label names. Values()
// returns values in this exact order.
var LabelNames = []string{
	"project_id", "project_name",
	"environment_id", "environment_name",
	"service_id", "service_name",
	"region", "deployment_id", "deployment_instance_id",
}

// Values returns label values in the same order as LabelNames.
func (l Labels) Values() []string {
	return []string{
		l.ProjectID, l.ProjectName,
		l.EnvironmentID, l.EnvironmentName,
		l.ServiceID, l.ServiceName,
		l.Region, l.DeploymentID, l.DeploymentInstanceID,
	}
}

// Sample is one measured value for one series at a point in time. Value is the
// raw Railway value (e.g. GB, cores); unit conversion is applied by exporters
// via the shared measurement mapping so it happens in exactly one place.
type Sample struct {
	Measurement railway.MetricMeasurement
	Labels      Labels
	Value       float64
	Timestamp   time.Time
}

// Snapshot is the full set of samples from one poll cycle.
type Snapshot struct {
	Samples []Sample
	Time    time.Time
}

// Store holds the most recent snapshot and is safe for concurrent access: the
// poller writes, exporters read.
type Store struct {
	mu   sync.RWMutex
	snap *Snapshot
}

// NewStore returns an empty store.
func NewStore() *Store { return &Store{} }

// Set replaces the current snapshot.
func (s *Store) Set(snap *Snapshot) {
	s.mu.Lock()
	s.snap = snap
	s.mu.Unlock()
}

// Snapshot returns the current snapshot, or nil if none has been stored yet.
func (s *Store) Snapshot() *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}
