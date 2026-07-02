package railway

import (
	"context"
	"encoding/json"
	"time"
)

// LogStreamParams configures an environmentLogs subscription.
type LogStreamParams struct {
	EnvironmentID string
	// Filter is Railway's log filter DSL, e.g. "@service:<id>" or "@level:error".
	Filter string
	// AfterDate resumes streaming after this instant (used on reconnect to
	// minimize gaps). Zero means "only new logs from now".
	AfterDate time.Time
	// BeforeLimit is how many historical lines to backfill on connect. Keep at
	// 0 for a drain to avoid re-delivering old logs on every reconnect.
	BeforeLimit int
}

const environmentLogsSubscription = `subscription intermodalEnvironmentLogs($environmentId: String!, $filter: String, $beforeLimit: Int!, $beforeDate: String, $anchorDate: String, $afterDate: String, $afterLimit: Int) {
  environmentLogs(environmentId: $environmentId, filter: $filter, beforeDate: $beforeDate, anchorDate: $anchorDate, afterDate: $afterDate, beforeLimit: $beforeLimit, afterLimit: $afterLimit) {
    timestamp
    message
    severity
    tags { projectId environmentId pluginId serviceId deploymentId deploymentInstanceId snapshotId }
    attributes { key value }
  }
}`

// StreamEnvironmentLogs opens a real-time subscription for an environment's
// logs and calls fn for each entry. It blocks until ctx is cancelled (returns
// nil) or the connection fails (returns the error, so the caller can reconnect).
func (c *Client) StreamEnvironmentLogs(ctx context.Context, p LogStreamParams, fn func(LogEntry) error) error {
	vars := map[string]any{
		"environmentId": p.EnvironmentID,
		"beforeLimit":   p.BeforeLimit,
	}
	if p.Filter != "" {
		vars["filter"] = p.Filter
	}
	if !p.AfterDate.IsZero() {
		vars["afterDate"] = p.AfterDate.UTC().Format(time.RFC3339Nano)
	}

	return c.subscribe(ctx, environmentLogsSubscription, vars, func(data json.RawMessage) error {
		var res struct {
			EnvironmentLogs []LogEntry `json:"environmentLogs"`
		}
		if err := json.Unmarshal(data, &res); err != nil {
			return err
		}
		for _, e := range res.EnvironmentLogs {
			if err := fn(e); err != nil {
				return err
			}
		}
		return nil
	})
}

// HTTPLogStreamParams configures an httpLogs (edge/router) subscription. HTTP
// logs are keyed by a single deployment and only exist for services with a
// public domain.
type HTTPLogStreamParams struct {
	DeploymentID string
	// Filter is Railway's HTTP-log filter DSL, e.g. "@httpStatus:>=500" or
	// "@path:/api".
	Filter      string
	AfterDate   time.Time
	BeforeLimit int
}

const httpLogsSubscription = `subscription intermodalHttpLogs($deploymentId: String!, $filter: String, $beforeLimit: Int!, $beforeDate: String, $anchorDate: String, $afterDate: String, $afterLimit: Int) {
  httpLogs(deploymentId: $deploymentId, filter: $filter, beforeDate: $beforeDate, anchorDate: $anchorDate, afterDate: $afterDate, beforeLimit: $beforeLimit, afterLimit: $afterLimit) {
    timestamp
    requestId
    method
    path
    host
    httpStatus
    upstreamProto
    downstreamProto
    responseDetails
    totalDuration
    upstreamAddress
    upstreamErrors
    clientUa
    upstreamRqDuration
    txBytes
    rxBytes
    srcIp
    edgeRegion
    deploymentId
    deploymentInstanceId
  }
}`

// StreamHTTPLogs opens a real-time subscription for a deployment's HTTP/edge
// logs and calls fn for each entry. It blocks until ctx is cancelled (returns
// nil) or the connection fails (returns the error, so the caller can reconnect).
func (c *Client) StreamHTTPLogs(ctx context.Context, p HTTPLogStreamParams, fn func(HTTPLogEntry) error) error {
	vars := map[string]any{
		"deploymentId": p.DeploymentID,
		"beforeLimit":  p.BeforeLimit,
	}
	if p.Filter != "" {
		vars["filter"] = p.Filter
	}
	if !p.AfterDate.IsZero() {
		vars["afterDate"] = p.AfterDate.UTC().Format(time.RFC3339Nano)
	}

	return c.subscribe(ctx, httpLogsSubscription, vars, func(data json.RawMessage) error {
		var res struct {
			HTTPLogs []HTTPLogEntry `json:"httpLogs"`
		}
		if err := json.Unmarshal(data, &res); err != nil {
			return err
		}
		for _, e := range res.HTTPLogs {
			if err := fn(e); err != nil {
				return err
			}
		}
		return nil
	})
}

// BuildLogStreamParams configures a buildLogs subscription. Build logs are a
// single deployment's build output — a finite stream that completes when the
// build's logs have been delivered (so this is a one-shot fetch, not a
// long-lived subscription).
type BuildLogStreamParams struct {
	DeploymentID string
	Filter       string
	Limit        int
}

const buildLogsSubscription = `subscription intermodalBuildLogs($deploymentId: String!, $filter: String, $limit: Int) {
  buildLogs(deploymentId: $deploymentId, filter: $filter, limit: $limit) {
    timestamp
    message
    severity
    tags { projectId environmentId pluginId serviceId deploymentId deploymentInstanceId snapshotId }
    attributes { key value }
  }
}`

// StreamBuildLogs streams a deployment's build logs and calls fn for each line.
// It returns when the build stream completes (nil), ctx is cancelled (nil), or
// the connection fails (error). Build logs reuse the LogEntry shape.
func (c *Client) StreamBuildLogs(ctx context.Context, p BuildLogStreamParams, fn func(LogEntry) error) error {
	vars := map[string]any{"deploymentId": p.DeploymentID}
	if p.Filter != "" {
		vars["filter"] = p.Filter
	}
	if p.Limit > 0 {
		vars["limit"] = p.Limit
	}

	return c.subscribe(ctx, buildLogsSubscription, vars, func(data json.RawMessage) error {
		var res struct {
			BuildLogs []LogEntry `json:"buildLogs"`
		}
		if err := json.Unmarshal(data, &res); err != nil {
			return err
		}
		for _, e := range res.BuildLogs {
			if err := fn(e); err != nil {
				return err
			}
		}
		return nil
	})
}
