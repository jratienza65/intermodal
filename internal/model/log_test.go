package model

import "testing"

func TestNormalizeLevel(t *testing.T) {
	cases := map[string]Level{
		"debug":       LevelDebug,
		"TRACE":       LevelDebug,
		"info":        LevelInfo,
		"Information": LevelInfo,
		"log":         LevelInfo,
		"warn":        LevelWarn,
		"warning":     LevelWarn,
		"err":         LevelError,
		"ERROR":       LevelError,
		"fatal":       LevelError,
		"critical":    LevelError,
		"":            LevelUnknown,
		"nonsense":    LevelUnknown,
		"  info  ":    LevelInfo,
	}
	for in, want := range cases {
		if got := NormalizeLevel(in); got != want {
			t.Errorf("NormalizeLevel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLogRecordCloneIsolatesAttributes(t *testing.T) {
	orig := LogRecord{
		Message:    "hi",
		Attributes: map[string]string{"k": "v"},
	}
	clone := orig.Clone()
	clone.Attributes["k"] = "changed"
	clone.Attributes["new"] = "x"

	if orig.Attributes["k"] != "v" {
		t.Errorf("clone mutation leaked into original: %q", orig.Attributes["k"])
	}
	if _, ok := orig.Attributes["new"]; ok {
		t.Error("clone added key leaked into original")
	}
}

func TestLogRecordCloneNilAttributes(t *testing.T) {
	orig := LogRecord{Message: "hi"}
	clone := orig.Clone()
	if clone.Attributes != nil {
		t.Error("expected nil attributes to stay nil after Clone")
	}
}
