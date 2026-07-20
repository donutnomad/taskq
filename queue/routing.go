package queue

import (
	"fmt"
	"time"
)

// MaxRoutingKeyBytes is the maximum UTF-8 byte length stored consistently by
// the supported SQLite and MySQL schemas.
const MaxRoutingKeyBytes = 191

type routingKeySet map[string]struct{}

func validateRoutingKey(key string) error {
	if len(key) > MaxRoutingKeyBytes {
		return fmt.Errorf("%w: length %d exceeds %d bytes", ErrInvalidRoutingKey, len(key), MaxRoutingKeyBytes)
	}
	return nil
}

func normalizePollOptions(opts PollOptions) (PollOptions, error) {
	seen := make(routingKeySet, len(opts.AcceptedRoutingKeys))
	keys := make([]string, 0, len(opts.AcceptedRoutingKeys))
	for _, key := range opts.AcceptedRoutingKeys {
		if err := validateRoutingKey(key); err != nil {
			return PollOptions{}, err
		}
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return PollOptions{AcceptedRoutingKeys: keys}, nil
}

func newRoutingKeySet(keys []string) routingKeySet {
	set := make(routingKeySet, len(keys))
	for _, key := range keys {
		if key != "" {
			set[key] = struct{}{}
		}
	}
	return set
}

func (s routingKeySet) accepts(row *TaskRow, at time.Time) bool {
	if row.RoutingKey == "" || routingFallback(row.RoutingDeadline, at) {
		return true
	}
	_, ok := s[row.RoutingKey]
	return ok
}

func routingFallback(deadline, at time.Time) bool {
	return !deadline.IsZero() && !deadline.After(at)
}

func newLease(row *TaskRow, token string, at time.Time) Lease {
	return Lease{
		Data:            row.Data,
		Token:           token,
		RoutingKey:      row.RoutingKey,
		RoutingDeadline: row.RoutingDeadline,
		RoutingFallback: routingFallback(row.RoutingDeadline, at),
	}
}

func taskDispatchTime(row *TaskRow) time.Time {
	if row.DispatchedAt == nil {
		return time.Now()
	}
	return *row.DispatchedAt
}
