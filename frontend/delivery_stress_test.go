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
	"gorm.io/gorm"
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
// 投递与归还的 UPDATE 前随机停顿，模拟负载下变慢的数据库。15s 内没领完时记下没领到的任务在库里的状态（已投递：
// 租约丢失，要等可见性超时；可见：一直没被领到），再换 2 个 1s 期限的消费者排空 5s：可见的任务必须全部领到，
// 已投递的行不会在可见性超时之前回来，排空后仍缺任务即失败。
//
// 默认跳过；运行：
//
//	TASKQ_STRESS=1 go test ./frontend -run TestFrontend_StressShortDeadlinePollsLoseNoDelivery -count=1 -v
//
// 强度：TASKQ_STRESS_ROUNDS（轮数，默认 20）、TASKQ_STRESS_TASKS（每轮任务数，默认 300）、TASKQ_STRESS_DELAY_MS
// （每次 UPDATE 前的最大随机停顿，默认 10）、TASKQ_STRESS_BURNERS（空转抢占调度的 goroutine 数，默认 0）；可配合
// GOMAXPROCS=1/2 与 -race。空转 goroutine 让网络事件只靠 sysmon 每 10ms 轮询一次，单次 RPC 往返就超过多数消费者的
// 期限，这时领不完是期限本身做不到，由排空阶段收尾；丢失的租约在排空之后仍是 dispatched，测试失败。
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
	startConsumers := func(count int, deadline func(consumer int) time.Duration) (stop func()) {
		ctx, cancel := context.WithCancel(context.Background())
		var consumers sync.WaitGroup
		for consumer := range count {
			fe := newFrontend()
			consumers.Go(func() {
				for ctx.Err() == nil {
					pollCtx, pollCancel := context.WithTimeout(ctx, deadline(consumer))
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
		return func() {
			cancel()
			consumers.Wait()
		}
	}

	stopTiny := startConsumers(8, func(consumer int) time.Duration { return time.Duration(5+consumer*3) * time.Millisecond })
	for i := range tasks {
		if err := producer.Enqueue(context.Background(), fmt.Appendf(nil, "task-%d", i)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(i%3) * time.Millisecond)
	}

	seen := make(map[string]bool, tasks)
	collectDeliveries(received, seen, tasks, 15*time.Second)
	stopTiny()
	collectDeliveries(received, seen, tasks, 0)
	if len(seen) == tasks {
		return
	}
	db := sqlite.(*gormstore.Store).DB()
	t.Logf("short-deadline consumers received %d of %d tasks within 15s; %s", len(seen), tasks, describeUndelivered(t, db, queueName, seen))

	// 期限短于一次往返时领不完是期限本身做不到，不是正确性问题；换成 1s 期限的消费者排空。可见的任务都必须领到；
	// 丢失的租约（已投递、无人持有）在可见性超时之前一直隐藏，排空之后仍缺的就是它。
	stopDrain := startConsumers(2, func(int) time.Duration { return time.Second })
	collectDeliveries(received, seen, tasks, 5*time.Second)
	stopDrain()
	collectDeliveries(received, seen, tasks, 0)
	if len(seen) < tasks {
		t.Fatalf("received %d of %d tasks after draining with 1s-deadline consumers; %s", len(seen), tasks, describeUndelivered(t, db, queueName, seen))
	}
}

// collectDeliveries 把 received 里的任务记入 seen，直到收齐 tasks 个或等满 wait；wait 为 0 时只取走已到达的。
func collectDeliveries(received <-chan string, seen map[string]bool, tasks int, wait time.Duration) {
	timeout := time.After(wait)
	for len(seen) < tasks {
		select {
		case data := <-received:
			seen[data] = true
			continue
		default:
		}
		if wait == 0 {
			return
		}
		select {
		case data := <-received:
			seen[data] = true
		case <-timeout:
			return
		}
	}
}

// describeUndelivered 列出没收到的任务在库里的状态：dispatched 是已投递但无人持有（租约丢失，可见性超时之前一直隐藏），
// visible 是可见但没被领到。调用方已停下所有消费者；被放弃的租约在后台归还，最多等 2s 让已投递的行回到可见。
func describeUndelivered(t *testing.T, db *gorm.DB, queueName string, seen map[string]bool) string {
	t.Helper()
	type taskRow struct {
		TaskID       int64
		Data         []byte
		Attempt      int64
		DispatchedAt *time.Time
	}
	var dispatched, visible []string
	for settle := time.Now().Add(2 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		var rows []taskRow
		if err := db.Raw("SELECT task_id, data, attempt, dispatched_at FROM taskq_tasks WHERE queue_name = ? ORDER BY task_id", queueName).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		dispatched, visible = nil, nil
		for _, row := range rows {
			if seen[string(row.Data)] {
				continue
			}
			entry := fmt.Sprintf("%s(task_id=%d attempt=%d)", row.Data, row.TaskID, row.Attempt)
			if row.DispatchedAt != nil {
				dispatched = append(dispatched, entry)
			} else {
				visible = append(visible, entry)
			}
		}
		if len(dispatched) == 0 || time.Now().After(settle) {
			break
		}
	}
	return fmt.Sprintf("dispatched but held by no one (lost lease): %v; visible but never claimed (starvation): %v", dispatched, visible)
}
