package sink

import (
	"testing"

	"github.com/jratienza65/intermodal/internal/model"
)

func TestUniqueKey(t *testing.T) {
	m := map[string]string{"region": "us", "region_2": "x"}
	if got := uniqueKey(m, "service"); got != "service" {
		t.Errorf("free key = %q, want service", got)
	}
	if got := uniqueKey(m, "region"); got != "region_3" {
		t.Errorf("collision key = %q, want region_3", got)
	}
}

// A user attribute named like a reserved field must not overwrite the
// authoritative Railway-derived value in structured metadata.
func TestStructuredMetadataNoReservedOverwrite(t *testing.T) {
	r := model.LogRecord{
		Region:   "us-west1",
		Severity: "info",
		Attributes: map[string]string{
			"region":  "app-supplied-region",
			"user.id": "42",
			"user-id": "43", // sanitizes to the same key as user.id
		},
	}
	meta := structuredMetadata(r)
	if meta["region"] != "us-west1" {
		t.Errorf("reserved region overwritten: %q", meta["region"])
	}
	if meta["region_2"] != "app-supplied-region" {
		t.Errorf("colliding attribute lost: %q", meta["region_2"])
	}
	// The two punctuation-distinct attributes must both survive.
	if meta["user_id"] == "" || meta["user_id_2"] == "" {
		t.Errorf("punctuation-collapsed attributes lost: %#v", meta)
	}
}
