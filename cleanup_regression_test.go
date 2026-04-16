package taskq_test

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestLegacyPartitionedQueueArtifactsAreRemoved(t *testing.T) {
	t.Helper()

	for _, path := range []string{
		"queue/queue.go",
		"queue/queue_test.go",
	} {
		_, err := os.Stat(path)
		if err == nil {
			t.Fatalf("legacy artifact still present: %s", path)
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %s: %v", path, err)
		}
	}

	content, err := os.ReadFile("queue/physical_queue.go")
	if err != nil {
		t.Fatalf("read physical_queue.go: %v", err)
	}

	for _, token := range []string{
		"forwardFn",
		"needForward",
		"shouldForward",
	} {
		if strings.Contains(string(content), token) {
			t.Fatalf("legacy forwarding token still present in physical_queue.go: %s", token)
		}
	}
}

func TestReadmeDoesNotMentionLegacyPartitionedQueueModel(t *testing.T) {
	t.Helper()

	content, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}

	for _, token := range []string{
		"PartitionedQueue",
		"forwardToRoot",
		"root 转发模型",
		"queue.Options{",
	} {
		if strings.Contains(string(content), token) {
			t.Fatalf("README still mentions removed legacy model: %s", token)
		}
	}
}
