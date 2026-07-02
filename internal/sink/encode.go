package sink

import (
	"time"

	"github.com/jratienza65/intermodal/internal/model"
)

// EncodeRecord flattens a LogRecord into a JSON-friendly map. Sinks that emit
// JSON (stdout, generic webhooks) share this so field naming is consistent.
// Custom structured-log attributes are nested under "attributes" to avoid
// colliding with intermodal's own fields.
func EncodeRecord(r model.LogRecord) map[string]any {
	m := map[string]any{
		"timestamp": r.Timestamp.UTC().Format(time.RFC3339Nano),
		"message":   r.Message,
	}
	if r.Level != "" {
		m["level"] = string(r.Level)
	}
	putNonEmpty(m, "severity", r.Severity)
	putNonEmpty(m, "kind", string(r.Kind))
	putNonEmpty(m, "project_id", r.ProjectID)
	putNonEmpty(m, "project_name", r.ProjectName)
	putNonEmpty(m, "environment_id", r.EnvironmentID)
	putNonEmpty(m, "environment_name", r.EnvironmentName)
	putNonEmpty(m, "service_id", r.ServiceID)
	putNonEmpty(m, "service_name", r.ServiceName)
	putNonEmpty(m, "deployment_id", r.DeploymentID)
	putNonEmpty(m, "deployment_instance_id", r.DeploymentInstanceID)
	putNonEmpty(m, "region", r.Region)
	putNonEmpty(m, "plugin_id", r.PluginID)
	putNonEmpty(m, "snapshot_id", r.SnapshotID)
	if len(r.Attributes) > 0 {
		m["attributes"] = r.Attributes
	}
	return m
}

func putNonEmpty(m map[string]any, key, val string) {
	if val != "" {
		m[key] = val
	}
}
