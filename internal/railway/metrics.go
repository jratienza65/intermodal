package railway

import (
	"context"
	"time"
)

// MetricsQuery describes a single call to Railway's `metrics` field. Scalar
// scope fields (ProjectID/EnvironmentID/ServiceID) are optional and omitted
// when empty; use GroupBy to break a broad scope down into per-service or
// per-replica series.
type MetricsQuery struct {
	Measurements      []MetricMeasurement
	ProjectID         string
	EnvironmentID     string
	ServiceID         string
	StartDate         time.Time
	EndDate           time.Time // zero => omitted (Railway defaults to now)
	GroupBy           []MetricTag
	SampleRateSeconds int
}

const metricsQuery = `query intermodalMetrics($measurements: [MetricMeasurement!]!, $startDate: DateTime!, $endDate: DateTime, $projectId: String, $environmentId: String, $serviceId: String, $groupBy: [MetricTag!], $sampleRateSeconds: Int) {
  metrics(measurements: $measurements, startDate: $startDate, endDate: $endDate, projectId: $projectId, environmentId: $environmentId, serviceId: $serviceId, groupBy: $groupBy, sampleRateSeconds: $sampleRateSeconds) {
    measurement
    tags { deploymentId deploymentInstanceId environmentId projectId region serviceId volumeId volumeInstanceId }
    values { ts value }
  }
}`

// Metrics runs the metrics query and returns the raw result series.
func (c *Client) Metrics(ctx context.Context, q MetricsQuery) ([]MetricsResult, error) {
	if q.StartDate.IsZero() {
		q.StartDate = time.Now().Add(-10 * time.Minute)
	}
	measurements := q.Measurements
	if len(measurements) == 0 {
		measurements = DefaultMeasurements
	}

	vars := map[string]any{
		"measurements": measurements,
		"startDate":    q.StartDate.UTC().Format(time.RFC3339),
	}
	if !q.EndDate.IsZero() {
		vars["endDate"] = q.EndDate.UTC().Format(time.RFC3339)
	}
	if q.ProjectID != "" {
		vars["projectId"] = q.ProjectID
	}
	if q.EnvironmentID != "" {
		vars["environmentId"] = q.EnvironmentID
	}
	if q.ServiceID != "" {
		vars["serviceId"] = q.ServiceID
	}
	if len(q.GroupBy) > 0 {
		vars["groupBy"] = q.GroupBy
	}
	if q.SampleRateSeconds > 0 {
		vars["sampleRateSeconds"] = q.SampleRateSeconds
	}

	var data struct {
		Metrics []MetricsResult `json:"metrics"`
	}
	if err := c.Execute(ctx, Request{OpName: "intermodalMetrics", Query: metricsQuery, Variables: vars}, &data); err != nil {
		return nil, err
	}
	return data.Metrics, nil
}
