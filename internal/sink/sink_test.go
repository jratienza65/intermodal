package sink

import (
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/model"
)

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestRegisteredIncludesBuiltins(t *testing.T) {
	reg := Registered()
	for _, want := range []string{"stdout", "loki", "otlp"} {
		if !contains(reg, want) {
			t.Errorf("Registered() = %v, missing %q", reg, want)
		}
	}
}

func TestBuildUnknownErrors(t *testing.T) {
	if _, err := Build("does-not-exist", Options{}); err == nil {
		t.Fatal("Build(unknown) returned nil error, want error")
	}
}

func TestBuildStdoutSucceeds(t *testing.T) {
	s, err := Build("stdout", Options{})
	if err != nil {
		t.Fatalf("Build(stdout) error = %v", err)
	}
	if s == nil {
		t.Fatal("Build(stdout) returned nil sink")
	}
	if s.Name() != "stdout" {
		t.Errorf("Name() = %q, want stdout", s.Name())
	}
}

func TestEncodeRecordMapsFields(t *testing.T) {
	ts := time.Date(2026, 7, 2, 12, 0, 0, 123, time.UTC)
	r := model.LogRecord{
		Timestamp:       ts,
		Message:         "hello",
		Level:           model.LevelInfo,
		Severity:        "info",
		Kind:            model.KindDeploy,
		ProjectID:       "p1",
		ProjectName:     "proj",
		EnvironmentID:   "e1",
		EnvironmentName: "prod",
		ServiceID:       "s1",
		ServiceName:     "api",
		DeploymentID:    "d1",
		Region:          "us-west",
		Attributes: map[string]string{
			"request_id": "abc",
			"path":       "/x",
		},
	}

	m := EncodeRecord(r)

	if m["message"] != "hello" {
		t.Errorf("message = %v, want hello", m["message"])
	}
	if m["level"] != "info" {
		t.Errorf("level = %v, want info", m["level"])
	}
	if m["timestamp"] != ts.Format(time.RFC3339Nano) {
		t.Errorf("timestamp = %v", m["timestamp"])
	}
	if m["kind"] != "deploy" {
		t.Errorf("kind = %v, want deploy", m["kind"])
	}
	if m["project_name"] != "proj" {
		t.Errorf("project_name = %v", m["project_name"])
	}
	if m["service_name"] != "api" {
		t.Errorf("service_name = %v", m["service_name"])
	}
	if m["region"] != "us-west" {
		t.Errorf("region = %v", m["region"])
	}

	attrs, ok := m["attributes"].(map[string]string)
	if !ok {
		t.Fatalf("attributes not nested as map[string]string: %T", m["attributes"])
	}
	if attrs["request_id"] != "abc" || attrs["path"] != "/x" {
		t.Errorf("attributes = %v", attrs)
	}
}

func TestEncodeRecordOmitsEmpty(t *testing.T) {
	r := model.LogRecord{
		Timestamp: time.Now(),
		Message:   "m",
	}
	m := EncodeRecord(r)
	for _, key := range []string{"level", "severity", "kind", "project_id", "attributes"} {
		if _, ok := m[key]; ok {
			t.Errorf("expected %q to be omitted, got %v", key, m[key])
		}
	}
}
