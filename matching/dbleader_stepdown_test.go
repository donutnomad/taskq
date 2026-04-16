package matching

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
)

type stepDownLockStore struct {
	mu      sync.Mutex
	leader  string
	expire  time.Time
	version uint64
}

func (s *stepDownLockStore) TryAcquire(_ context.Context, _ string, nodeIP string, ttlSeconds int) (dbleader.AcquireResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if s.leader == "" || s.expire.Before(now) {
		s.version++
		s.leader = nodeIP
		s.expire = now.Add(time.Duration(ttlSeconds) * time.Second)
		return dbleader.AcquireResult{Acquired: true, LeaderIP: nodeIP, Version: s.version}, nil
	}
	if s.leader == nodeIP {
		s.expire = now.Add(time.Duration(ttlSeconds) * time.Second)
		return dbleader.AcquireResult{Acquired: true, LeaderIP: nodeIP, Version: s.version}, nil
	}
	return dbleader.AcquireResult{Acquired: false, LeaderIP: s.leader, Version: s.version}, nil
}

func (s *stepDownLockStore) Renew(_ context.Context, _ string, nodeIP string, ttlSeconds int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leader != nodeIP || s.expire.Before(time.Now()) {
		return false, nil
	}
	s.expire = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	return true, nil
}

func (s *stepDownLockStore) Release(_ context.Context, _ string, nodeIP string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leader != nodeIP || s.expire.Before(time.Now()) {
		return false, nil
	}
	s.expire = time.Now().Add(-time.Second)
	return true, nil
}

func (s *stepDownLockStore) GetLeaderAddr(_ context.Context, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expire.Before(time.Now()) {
		return "", nil
	}
	return s.leader, nil
}

type stepDownTask struct {
	once sync.Once
}

func (t *stepDownTask) Name() string { return "step-down" }

func (t *stepDownTask) Start(ctx context.Context) error {
	var err error
	t.once.Do(func() {
		err = dbleader.StepDown(errors.New("ownership lost"))
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

type blockingLeaderTask struct{}

func (t *blockingLeaderTask) Name() string { return "blocking" }

func (t *blockingLeaderTask) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func TestDBLeaderStepDownErrorTransfersLeadership(t *testing.T) {
	store := &stepDownLockStore{}
	opts := []dbleader.Option{
		dbleader.WithLeaseTTL(3 * time.Second),
		dbleader.WithHeartbeat(200 * time.Millisecond),
		dbleader.WithRetryInterval(50 * time.Millisecond),
	}

	m1 := dbleader.NewManager(store, "lock", "node-1", []dbleader.LeaderTask{&stepDownTask{}}, opts...)
	m2 := dbleader.NewManager(store, "lock", "node-2", []dbleader.LeaderTask{&blockingLeaderTask{}}, opts...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m1.Run(ctx)
	go m2.Run(ctx)

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if m2.IsLeader() {
			if m1.IsLeader() {
				t.Fatal("expected node-1 to step down after fatal task error")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("expected leadership to transfer to node-2 after fatal task error")
}
