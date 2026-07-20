package queue

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	tokenVersionPrefix       = "v2."
	legacyTokenVersionPrefix = "v1."
)

type TaskToken struct {
	QueueName      string `json:"q"`
	PartitionID    int    `json:"p"`
	TaskID         int64  `json:"t"`
	OwnershipEpoch int64  `json:"e"`
	DeliveryID     int64  `json:"d"`
}

func formatToken(queueName string, partitionID int, taskID int64, ownershipEpoch, deliveryID int64) string {
	payload, err := json.Marshal(TaskToken{
		QueueName:      queueName,
		PartitionID:    partitionID,
		TaskID:         taskID,
		OwnershipEpoch: ownershipEpoch,
		DeliveryID:     deliveryID,
	})
	if err != nil {
		panic(err)
	}
	return tokenVersionPrefix + base64.RawURLEncoding.EncodeToString(payload)
}

func ParseTaskToken(token string) (TaskToken, error) {
	if strings.HasPrefix(token, tokenVersionPrefix) {
		parsed, err := parseStructuredToken(token, tokenVersionPrefix)
		if err != nil {
			return TaskToken{}, err
		}
		if parsed.QueueName == "" || parsed.PartitionID < 0 || parsed.TaskID <= 0 || parsed.OwnershipEpoch <= 0 || parsed.DeliveryID <= 0 {
			return TaskToken{}, fmt.Errorf("%w: %q", ErrInvalidToken, token)
		}
		return parsed, nil
	}
	if strings.HasPrefix(token, legacyTokenVersionPrefix) {
		parsed, err := parseStructuredToken(token, legacyTokenVersionPrefix)
		if err != nil {
			return TaskToken{}, err
		}
		if parsed.QueueName == "" || parsed.PartitionID < 0 || parsed.TaskID <= 0 || parsed.OwnershipEpoch <= 0 {
			return TaskToken{}, fmt.Errorf("%w: %q", ErrInvalidToken, token)
		}
		return parsed, nil
	}

	var partitionID int
	var taskID int64
	n, err := fmt.Sscanf(token, "%d:%d", &partitionID, &taskID)
	if err != nil || n != 2 {
		return TaskToken{}, fmt.Errorf("%w: %q", ErrInvalidToken, token)
	}
	return TaskToken{PartitionID: partitionID, TaskID: taskID}, nil
}

func parseStructuredToken(token, prefix string) (TaskToken, error) {
	raw := strings.TrimPrefix(token, prefix)
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return TaskToken{}, fmt.Errorf("%w: %q", ErrInvalidToken, token)
	}
	var parsed TaskToken
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return TaskToken{}, fmt.Errorf("%w: %q", ErrInvalidToken, token)
	}
	return parsed, nil
}

func parseToken(token string) (int, int64, error) {
	parsed, err := ParseTaskToken(token)
	if err != nil {
		return 0, 0, err
	}
	return parsed.PartitionID, parsed.TaskID, nil
}

func validateTaskToken(queueName string, partitionID int, ownershipEpoch int64, token TaskToken) error {
	if token.QueueName != "" && token.QueueName != queueName {
		return fmt.Errorf("%w: token queue %q does not match queue %q", ErrInvalidToken, token.QueueName, queueName)
	}
	if err := validateTokenPartition(partitionID, token.PartitionID); err != nil {
		return err
	}
	if token.OwnershipEpoch != ownershipEpoch {
		return fmt.Errorf("%w: token epoch %d does not match queue epoch %d", ErrStaleToken, token.OwnershipEpoch, ownershipEpoch)
	}
	return nil
}

func validateTokenPartition(queuePartitionID, tokenPartitionID int) error {
	if queuePartitionID != tokenPartitionID {
		return fmt.Errorf(
			"%w: token partition %d does not match queue partition %d",
			ErrInvalidToken,
			tokenPartitionID,
			queuePartitionID,
		)
	}
	return nil
}
