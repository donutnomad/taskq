package frontend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/store/gormstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// jitterStore 在投递与归还的 UPDATE 前随机停顿，模拟负载下变慢的数据库。两者都在分区的状态转换锁内执行，停顿让
// 同一分区的投递与归还排队；停顿随调用方的 ctx 结束，与等待连接池的驱动行为一致。
type jitterStore struct {
	matching.Store
	maxDelay time.Duration
}

func (s *jitterStore) pause(ctx context.Context) error {
	if s.maxDelay <= 0 {
		return nil
	}
	timer := time.NewTimer(rand.N(s.maxDelay))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *jitterStore) DispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64, dispatchedAt time.Time) error {
	if err := s.pause(ctx); err != nil {
		return err
	}
	return s.Store.DispatchTask(ctx, queueName, partitionID, taskID, dispatchedAt)
}

func (s *jitterStore) UnDispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64) error {
	if err := s.pause(ctx); err != nil {
		return err
	}
	return s.Store.UnDispatchTask(ctx, queueName, partitionID, taskID)
}

// TestFrontend_StressShortDeadlinePollsLoseNoDelivery 是 Tenuto TestTaskQShortDeadlinePollsLoseNoDelivery 的加压版：
// 真实 matching + frontend + SQLite（单连接，数据库调用本来就排队），8 个消费者以 5–26ms 的期限反复 poll，
// 投递与归还的 UPDATE 前随机停顿，模拟负载下变慢的数据库。每轮 300 个任务都必须在 15s 内领到；失败时列出没领到
// 的任务在库里的状态：已投递（租约丢失，要等可见性超时）或可见（一直没被领到）。
//
// 默认跳过；运行：
//
//	TASKQ_STRESS=1 go test ./frontend -run TestFrontend_StressShortDeadlinePollsLoseNoDelivery -count=1 -v
//
// 强度：TASKQ_STRESS_ROUNDS（轮数，默认 20）、TASKQ_STRESS_TASKS（每轮任务数，默认 300）、TASKQ_STRESS_DELAY_MS
// （每次 UPDATE 前的最大随机停顿，默认 10）、TASKQ_STRESS_BURNERS（空转抢占调度的 goroutine 数，默认 0）；可配合
// GOMAXPROCS=1/2 与 -race。空转 goroutine 让网络事件只靠 sysmon 每 10ms 轮询一次，单次 RPC 往返就超过多数消费者的
// 期限，这时领不完是期限本身做不到，不代表丢失；判断丢失看失败信息里的 dispatched 行。
func TestFrontend_StressShortDeadlinePollsLoseNoDelivery(t *testing.T) {
	if os.Getenv("TASKQ_STRESS") == "" {
		t.Skip("stress test; set TASKQ_STRESS=1 to run")
	}
	rounds := stressEnvInt(t, "TASKQ_STRESS_ROUNDS", 20)
	tasks := stressEnvInt(t, "TASKQ_STRESS_TASKS", 300)
	maxDelay := time.Duration(stressEnvInt(t, "TASKQ_STRESS_DELAY_MS", 10)) * time.Millisecond

	stopBurn := make(chan struct{})
	var burners sync.WaitGroup
	for range stressEnvInt(t, "TASKQ_STRESS_BURNERS", 0) {
		burners.Go(func() {
			for {
				select {
				case <-stopBurn:
					return
				default:
					runtime.Gosched()
				}
			}
		})
	}
	defer func() {
		close(stopBurn)
		burners.Wait()
	}()

	for round := range rounds {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			runShortDeadlineStressRound(t, tasks, maxDelay)
		})
		if t.Failed() {
			return
		}
	}
}

func stressEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		t.Fatalf("%s=%q: want a non-negative integer", name, raw)
	}
	return value
}

func runShortDeadlineStressRound(t *testing.T, tasks int, maxDelay time.Duration) {
	const queueName = "stress-q"
	sqlite := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, &jitterStore{Store: sqlite, maxDelay: maxDelay}, resolver, map[string]int{queueName: 2})

	newFrontend := func() *Frontend {
		fe, err := New(Config{
			ResolverProvider: &stubResolverProvider{resolver: resolver},
			QueueName:        queueName,
			DialOpts:         []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
			PollTimeout:      250 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(fe.Close)
		return fe
	}
	producer := newFrontend()

	received := make(chan string, tasks*2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var consumers sync.WaitGroup
	for consumer := range 8 {
		fe := newFrontend()
		consumers.Go(func() {
			for ctx.Err() == nil {
				pollCtx, pollCancel := context.WithTimeout(ctx, time.Duration(5+consumer*3)*time.Millisecond)
				lease, err := fe.DequeueWithOptions(pollCtx, PollOptions{})
				pollCancel()
				if err != nil {
					continue
				}
				if err := fe.Ack(context.Background(), lease.Token); err != nil {
					t.Errorf("ack %s: %v", lease.Data, err)
				}
				received <- string(lease.Data)
			}
		})
	}

	for i := range tasks {
		if err := producer.Enqueue(context.Background(), fmt.Appendf(nil, "task-%d", i)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(i%3) * time.Millisecond)
	}

	seen := make(map[string]bool, tasks)
	timeout := time.After(15 * time.Second)
	for len(seen) < tasks {
		select {
		case data := <-received:
			seen[data] = true
		case <-timeout:
			cancel()
			consumers.Wait()
			// 在途的租约此时已被归还；之后仍是已投递的行没有任何一方持有。
			time.Sleep(2 * time.Second)
			var rows []struct {
				TaskID       int64
				Data         []byte
				DispatchedAt *time.Time
			}
			if err := sqlite.(*gormstore.Store).DB().Raw("SELECT task_id, data, dispatched_at FROM taskq_tasks").Scan(&rows).Error; err != nil {
				t.Fatal(err)
			}
			var lost, visible []string
			for _, row := range rows {
				switch {
				case seen[string(row.Data)]:
				case row.DispatchedAt != nil:
					lost = append(lost, string(row.Data))
				default:
					visible = append(visible, string(row.Data))
				}
			}
			t.Fatalf("received %d of %d tasks within 15s; dispatched but held by no one: %v; visible but never claimed: %v",
				len(seen), tasks, lost, visible)
		}
	}
	cancel()
	consumers.Wait()
}
