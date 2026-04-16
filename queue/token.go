package queue

import "fmt"

func formatToken(partitionID int, taskID int64) string {
	return fmt.Sprintf("%d:%d", partitionID, taskID)
}

func parseToken(token string) (int, int64, error) {
	var partitionID int
	var taskID int64
	n, err := fmt.Sscanf(token, "%d:%d", &partitionID, &taskID)
	if err != nil || n != 2 {
		return 0, 0, fmt.Errorf("%w: %q", ErrInvalidToken, token)
	}
	return partitionID, taskID, nil
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
