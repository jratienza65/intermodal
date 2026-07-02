// Package model holds intermodal's sink-agnostic domain types.
//
// It has no dependencies beyond the standard library so that every other
// package (railway client, log pipeline, sinks, exporters) can share these
// types without creating import cycles.
package model

import (
	"maps"
	"strings"
	"time"
)

// Level is a normalized log severity. Railway emits a free-form `severity`
// string (e.g. "info", "err", "warn") which we fold into these canonical
// values so sinks don't each have to re-implement the mapping.
type Level string

const (
	LevelDebug   Level = "debug"
	LevelInfo    Level = "info"
	LevelWarn    Level = "warn"
	LevelError   Level = "error"
	LevelUnknown Level = ""
)

// NormalizeLevel folds Railway's severity/level strings into a canonical Level.
func NormalizeLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug", "trace", "verbose":
		return LevelDebug
	case "info", "information", "notice", "log":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "err", "error", "fatal", "critical", "crit", "panic", "emerg", "alert":
		return LevelError
	default:
		return LevelUnknown
	}
}

// LogRecord is intermodal's normalized log line. Every log source produces
// these and every sink consumes them, so the fan-out pipeline never has to
// know where a log came from or where it's going.
type LogRecord struct {
	Timestamp time.Time
	Message   string

	// Level is the normalized severity; Severity is the raw Railway value,
	// preserved so sinks that want fidelity (e.g. OTLP severity text) can use it.
	Level    Level
	Severity string

	// Source identity. IDs come from the log's Railway tags; the human-readable
	// names are enriched from discovery (see internal/target) when available.
	ProjectID            string
	ProjectName          string
	EnvironmentID        string
	EnvironmentName      string
	ServiceID            string
	ServiceName          string
	DeploymentID         string
	DeploymentInstanceID string
	Region               string
	PluginID             string
	SnapshotID           string

	// Kind distinguishes deploy/application logs from HTTP (edge) logs.
	Kind LogKind

	// Attributes carries structured-log key/values (Railway `attributes`, plus
	// any HTTP request fields). Values are kept as strings as Railway returns them.
	Attributes map[string]string
}

// LogKind identifies which Railway log stream a record came from.
type LogKind string

const (
	KindDeploy LogKind = "deploy"
	KindHTTP   LogKind = "http"
	KindBuild  LogKind = "build"
)

// Clone returns a deep copy safe to hand to a sink that may retain or mutate it.
func (r LogRecord) Clone() LogRecord {
	if r.Attributes != nil {
		attrs := make(map[string]string, len(r.Attributes))
		maps.Copy(attrs, r.Attributes)
		r.Attributes = attrs
	}
	return r
}
