package queue

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestParseTaskTokenSupportsPreviousStructuredVersion(t *testing.T) {
	payload, err := json.Marshal(struct {
		QueueName      string `json:"q"`
		PartitionID    int    `json:"p"`
		TaskID         int64  `json:"t"`
		OwnershipEpoch int64  `json:"e"`
	}{
		QueueName:      "rolling-upgrade-q",
		PartitionID:    3,
		TaskID:         17,
		OwnershipEpoch: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	token := legacyTokenVersionPrefix + base64.RawURLEncoding.EncodeToString(payload)

	parsed, err := ParseTaskToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.QueueName != "rolling-upgrade-q" || parsed.PartitionID != 3 || parsed.TaskID != 17 || parsed.OwnershipEpoch != 9 {
		t.Fatalf("parsed token = %+v", parsed)
	}
	if parsed.DeliveryID != 0 {
		t.Fatalf("legacy delivery ID = %d, want 0", parsed.DeliveryID)
	}
}
