package railway

// This file mirrors the parts of Railway's GraphQL schema that intermodal
// depends on. Enums and object shapes were confirmed against the live schema
// via introspection (see the design brief) — do not add fields that aren't in
// the real schema.

// MetricMeasurement is Railway's `MetricMeasurement` enum. Only the members we
// actually export are listed; MEASUREMENT_UNSPECIFIED / UNRECOGNIZED are
// protobuf sentinels and intentionally omitted.
type MetricMeasurement string

const (
	MeasurementCPUUsage           MetricMeasurement = "CPU_USAGE"
	MeasurementCPULimit           MetricMeasurement = "CPU_LIMIT"
	MeasurementMemoryUsageGB      MetricMeasurement = "MEMORY_USAGE_GB"
	MeasurementMemoryLimitGB      MetricMeasurement = "MEMORY_LIMIT_GB"
	MeasurementDiskUsageGB        MetricMeasurement = "DISK_USAGE_GB"
	MeasurementEphemeralDiskUseGB MetricMeasurement = "EPHEMERAL_DISK_USAGE_GB"
	MeasurementBackupUsageGB      MetricMeasurement = "BACKUP_USAGE_GB"
	MeasurementNetworkRxGB        MetricMeasurement = "NETWORK_RX_GB"
	MeasurementNetworkTxGB        MetricMeasurement = "NETWORK_TX_GB"
)

// DefaultMeasurements is the set intermodal polls unless overridden.
var DefaultMeasurements = []MetricMeasurement{
	MeasurementCPUUsage,
	MeasurementCPULimit,
	MeasurementMemoryUsageGB,
	MeasurementMemoryLimitGB,
	MeasurementDiskUsageGB,
	MeasurementNetworkRxGB,
	MeasurementNetworkTxGB,
}

// MetricTag is Railway's `MetricTag` enum, used for `groupBy`.
type MetricTag string

const (
	TagDeploymentID         MetricTag = "DEPLOYMENT_ID"
	TagDeploymentInstanceID MetricTag = "DEPLOYMENT_INSTANCE_ID"
	TagEnvironmentID        MetricTag = "ENVIRONMENT_ID"
	TagServiceID            MetricTag = "SERVICE_ID"
	TagProjectID            MetricTag = "PROJECT_ID"
	TagRegion               MetricTag = "REGION"
	TagVolumeID             MetricTag = "VOLUME_ID"
	TagVolumeInstanceID     MetricTag = "VOLUME_INSTANCE_ID"
)

// MetricsResult mirrors GraphQL `MetricsResult`: one time series for one
// measurement, labeled by a set of tags.
type MetricsResult struct {
	Measurement MetricMeasurement `json:"measurement"`
	Tags        MetricTags        `json:"tags"`
	Values      []MetricValue     `json:"values"`
}

// MetricValue is a single sample. Ts is unix seconds.
type MetricValue struct {
	Ts    int64   `json:"ts"`
	Value float64 `json:"value"`
}

// Latest returns the most recent sample by timestamp and whether one exists.
func (r MetricsResult) Latest() (MetricValue, bool) {
	if len(r.Values) == 0 {
		return MetricValue{}, false
	}
	latest := r.Values[0]
	for _, v := range r.Values[1:] {
		if v.Ts >= latest.Ts {
			latest = v
		}
	}
	return latest, true
}

// MetricTags mirrors GraphQL `MetricTags` — all fields are nullable strings.
type MetricTags struct {
	DeploymentID         string `json:"deploymentId"`
	DeploymentInstanceID string `json:"deploymentInstanceId"`
	EnvironmentID        string `json:"environmentId"`
	ProjectID            string `json:"projectId"`
	Region               string `json:"region"`
	ServiceID            string `json:"serviceId"`
	VolumeID             string `json:"volumeId"`
	VolumeInstanceID     string `json:"volumeInstanceId"`
}

// LogEntry mirrors a single element from the environmentLogs subscription.
type LogEntry struct {
	Timestamp  string         `json:"timestamp"`
	Message    string         `json:"message"`
	Severity   string         `json:"severity"`
	Tags       LogTags        `json:"tags"`
	Attributes []LogAttribute `json:"attributes"`
}

// LogTags mirrors the `tags` object on a log entry.
type LogTags struct {
	ProjectID            string `json:"projectId"`
	EnvironmentID        string `json:"environmentId"`
	PluginID             string `json:"pluginId"`
	ServiceID            string `json:"serviceId"`
	DeploymentID         string `json:"deploymentId"`
	DeploymentInstanceID string `json:"deploymentInstanceId"`
	SnapshotID           string `json:"snapshotId"`
}

// LogAttribute is a structured-log key/value pair.
type LogAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// --- Discovery types ---

// Me is the authenticated user (account tokens only).
type Me struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Project with its environments and services (as returned by project(id)).
type Project struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Environments []Environment     `json:"-"`
	Services     []Service         `json:"-"`
	Instances    []ServiceInstance `json:"-"`
}

// ServiceInstance is one service in one environment: its active deployment and
// whether it has a public domain (both needed to stream HTTP/edge logs).
type ServiceInstance struct {
	ServiceID        string
	EnvironmentID    string
	DeploymentID     string
	DeploymentStatus string
	HasDomain        bool
}

// Environment is a project environment.
type Environment struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Service is a project service.
type Service struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ProjectTokenInfo is the scope implied by a project token.
type ProjectTokenInfo struct {
	ProjectID     string `json:"projectId"`
	EnvironmentID string `json:"environmentId"`
}

// HTTPLogEntry mirrors a single element from the httpLogs (edge/router)
// subscription. It carries deploymentId/deploymentInstanceId but not
// service/project/environment — those are enriched from the subscription's
// target context.
type HTTPLogEntry struct {
	Timestamp            string `json:"timestamp"`
	RequestID            string `json:"requestId"`
	Method               string `json:"method"`
	Path                 string `json:"path"`
	Host                 string `json:"host"`
	HTTPStatus           int    `json:"httpStatus"`
	UpstreamProto        string `json:"upstreamProto"`
	DownstreamProto      string `json:"downstreamProto"`
	ResponseDetails      string `json:"responseDetails"`
	TotalDuration        int    `json:"totalDuration"`
	UpstreamAddress      string `json:"upstreamAddress"`
	UpstreamErrors       string `json:"upstreamErrors"`
	ClientUA             string `json:"clientUa"`
	UpstreamRqDuration   int    `json:"upstreamRqDuration"`
	TxBytes              int    `json:"txBytes"`
	RxBytes              int    `json:"rxBytes"`
	SrcIP                string `json:"srcIp"`
	EdgeRegion           string `json:"edgeRegion"`
	DeploymentID         string `json:"deploymentId"`
	DeploymentInstanceID string `json:"deploymentInstanceId"`
}
