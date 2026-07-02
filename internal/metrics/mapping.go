package metrics

import "github.com/jratienza65/intermodal/internal/railway"

// MetricType distinguishes gauges from monotonic counters.
type MetricType int

const (
	Gauge MetricType = iota
	Counter
)

// Info describes how a Railway measurement maps to an exported metric: its
// name, semantics, and the factor to convert Railway's raw value into the
// exported unit. Both the Prometheus collector and the OTLP exporter consume
// this table so naming and unit conversion live in exactly one place.
type Info struct {
	Name   string
	Help   string
	Type   MetricType
	Unit   string  // OTLP unit (UCUM-ish)
	Factor float64 // multiply the raw Railway value by this
}

// gbToBytes converts Railway's decimal "GB" values to bytes (1 GB = 1e9 bytes).
const gbToBytes = 1e9

// Measurements maps each polled Railway measurement to its exported metric.
// Network counters are cumulative-looking and exported as `_total` counters.
var Measurements = map[railway.MetricMeasurement]Info{
	railway.MeasurementCPUUsage: {
		Name: "railway_service_cpu_usage_cores", Help: "Current CPU usage in vCPU cores.",
		Type: Gauge, Unit: "{cpu}", Factor: 1,
	},
	railway.MeasurementCPULimit: {
		Name: "railway_service_cpu_limit_cores", Help: "Allocated CPU limit in vCPU cores.",
		Type: Gauge, Unit: "{cpu}", Factor: 1,
	},
	railway.MeasurementMemoryUsageGB: {
		Name: "railway_service_memory_usage_bytes", Help: "Current memory usage in bytes.",
		Type: Gauge, Unit: "By", Factor: gbToBytes,
	},
	railway.MeasurementMemoryLimitGB: {
		Name: "railway_service_memory_limit_bytes", Help: "Allocated memory limit in bytes.",
		Type: Gauge, Unit: "By", Factor: gbToBytes,
	},
	railway.MeasurementDiskUsageGB: {
		Name: "railway_service_disk_usage_bytes", Help: "Current disk usage in bytes.",
		Type: Gauge, Unit: "By", Factor: gbToBytes,
	},
	railway.MeasurementEphemeralDiskUseGB: {
		Name: "railway_service_ephemeral_disk_usage_bytes", Help: "Current ephemeral disk usage in bytes.",
		Type: Gauge, Unit: "By", Factor: gbToBytes,
	},
	railway.MeasurementBackupUsageGB: {
		Name: "railway_service_backup_usage_bytes", Help: "Current backup storage usage in bytes.",
		Type: Gauge, Unit: "By", Factor: gbToBytes,
	},
	railway.MeasurementNetworkRxGB: {
		Name: "railway_service_network_receive_bytes_total", Help: "Cumulative network bytes received (ingress).",
		Type: Counter, Unit: "By", Factor: gbToBytes,
	},
	railway.MeasurementNetworkTxGB: {
		Name: "railway_service_network_transmit_bytes_total", Help: "Cumulative network bytes transmitted (egress).",
		Type: Counter, Unit: "By", Factor: gbToBytes,
	},
}
