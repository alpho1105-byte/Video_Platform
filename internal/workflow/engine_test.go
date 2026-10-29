package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"video-platform/internal/domain"
	"video-platform/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "w.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func startVideo(t *testing.T, st *store.Store, id string, root []store.TaskSpec) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreateVideo(ctx, domain.Video{ID: id, Uploader: "u", Title: id, Size: 1, PartSize: 1, Status: domain.StatusUploading, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.StartProcessing(ctx, id, "etag", root); err != nil {
		t.Fatal(err)
	}
}

func fastConfig() Config {
	return Config{LeaseTTL: 150 * time.Millisecond, Poll: 5 * time.Millisecond, BaseBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func allDone(st *store.Store) func() bool {
	return func() bool {
		counts, _ := st.TaskCounts(context.Background())
		return counts["ready"] == 0 && counts["running"] == 0 && counts["waiting"] == 0
	}
}

// root 執行時產生 n 個平行的 leaf，join 等所有 leaf 完成後把它們的輸出加總。
func fanOutHandlers(n int, leafCalls *atomic.Int64, joined chan<- int) map[string]Handler {
	return map[string]Handler{
		"root": func(ctx context.Context, task store.Task) (Result, error) {
			var spawn []store.TaskSpec
			var leaves []string
			for i := range n {
				id := fmt.Sprintf("%s:leaf:%d", task.WorkflowID, i)
				leaves = append(leaves, id)
				spawn = append(spawn, store.TaskSpec{ID: id, Kind: "leaf", Args: i, MaxAttempts: 5})
			}
			spawn = append(spawn, store.TaskSpec{ID: task.WorkflowID + ":join", Kind: "join", After: leaves, MaxAttempts: 5})
			return Result{Spawn: spawn}, nil
		},
		"leaf": func(ctx context.Context, task store.Task) (Result, error) {
			leafCalls.Add(1)
			var i int
			json.Unmarshal(task.Args, &i)
			return Result{Output: i * i}, nil
		},
		"join": func(ctx context.Context, task store.Task) (Result, error) {
			sum := 0
			for _, raw := range task.Inputs {
				var v int
				json.Unmarshal(raw, &v)
				sum += v
			}
			joined <- sum
			return Result{}, nil
		},
	}
}

func TestDynamicFanOutJoinsOnceAllLeavesAreDone(t *testing.T) {
	st := openStore(t)
	var calls atomic.Int64
	joined := make(chan int, 1)
	e := New(st, fastConfig(), fanOutHandlers(20, &calls, joined), slog.New(slog.DiscardHandler))
	startVideo(t, st, "v1", []store.TaskSpec{{ID: "v1:root", Kind: "root", MaxAttempts: 3}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := range 4 {
		go e.NewWorker(fmt.Sprint(i)).Run(ctx, func() bool { return false })
	}
	select {
	case sum := <-joined:
		if sum != 2470 { // 0² + 1² + … + 19²
			t.Fatalf("join saw sum %d", sum)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("join never ran")
	}
	waitFor(t, "all tasks done", allDone(st))
	if calls.Load() != 20 {
		t.Fatalf("leaf ran %d times", calls.Load())
	}
}

// worker 停住超過租約：任務被別人重做並完成，停住的 worker 回來時結果被擋下，join 的計數只扣一次。
func TestStalledWorkerCannotCompleteATaskSomeoneElseFinished(t *testing.T) {
	st := openStore(t)
	var mu sync.Mutex
	runs := map[string]int{}
	stalled := make(chan struct{})
	var once sync.Once
	var e *Engine
	var slow *Worker
	handlers := map[string]Handler{
		"step": func(ctx context.Context, task store.Task) (Result, error) {
			mu.Lock()
			runs[task.ID]++
			mu.Unlock()
			once.Do(func() {
				slow.Stall(400 * time.Millisecond)
				close(stalled)
			})
			return Result{}, nil
		},
		"join": func(ctx context.Context, task store.Task) (Result, error) { return Result{}, nil },
	}
	e = New(st, fastConfig(), handlers, slog.New(slog.DiscardHandler))
	slow = e.NewWorker("slow")
	startVideo(t, st, "v1", []store.TaskSpec{
		{ID: "v1:a", Kind: "step", MaxAttempts: 3},
		{ID: "v1:join", Kind: "join", After: []string{"v1:a"}, MaxAttempts: 3},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go slow.Run(ctx, func() bool { return false })
	<-stalled
	go e.NewWorker("fast").Run(ctx, func() bool { return false })
	waitFor(t, "workflow done", allDone(st))
	waitFor(t, "the stalled worker to report", func() bool { return e.Stats().Rejected.Load() == 1 })
	tasks, _ := st.Tasks(context.Background())
	for _, task := range tasks {
		if task.Status != "done" || task.DepsLeft != 0 {
			t.Fatalf("task %+v", task)
		}
	}
	if runs["v1:a"] != 2 {
		t.Fatalf("step ran %d times, want 2 (once by each worker)", runs["v1:a"])
	}
}

func TestTransientErrorsAreRetriedAndPermanentOnesFailTheVideo(t *testing.T) {
	st := openStore(t)
	var attempts atomic.Int64
	handlers := map[string]Handler{
		"flaky": func(ctx context.Context, task store.Task) (Result, error) {
			if attempts.Add(1) < 3 {
				return Result{}, errors.New("temporary")
			}
			return Result{}, nil
		},
		"broken": func(ctx context.Context, task store.Task) (Result, error) {
			return Result{}, Permanent(errors.New("corrupt upload"))
		},
		"never": func(ctx context.Context, task store.Task) (Result, error) {
			t.Error("a task after a failed step ran")
			return Result{}, nil
		},
	}
	e := New(st, fastConfig(), handlers, slog.New(slog.DiscardHandler))
	startVideo(t, st, "ok", []store.TaskSpec{{ID: "ok:flaky", Kind: "flaky", MaxAttempts: 5}})
	startVideo(t, st, "bad", []store.TaskSpec{{ID: "bad:broken", Kind: "broken", MaxAttempts: 5},
		{ID: "bad:next", Kind: "never", After: []string{"bad:broken"}, MaxAttempts: 5}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.NewWorker("w").Run(ctx, func() bool { return false })
	waitFor(t, "both workflows to settle", allDone(st))
	if attempts.Load() != 3 || e.Stats().Retried.Load() != 2 || e.Stats().Failed.Load() != 1 {
		t.Fatalf("attempts %d, retried %d, failed %d", attempts.Load(), e.Stats().Retried.Load(), e.Stats().Failed.Load())
	}
	v, _ := st.GetVideo(context.Background(), "bad")
	if v.Status != domain.StatusFailed || v.Error != "corrupt upload" {
		t.Fatalf("video = %s %q", v.Status, v.Error)
	}
}

func TestCrashedWorkersTaskIsPickedUpAfterTheLeaseExpires(t *testing.T) {
	st := openStore(t)
	started := make(chan struct{}, 1)
	var finished atomic.Int64
	handlers := map[string]Handler{
		"work": func(ctx context.Context, task store.Task) (Result, error) {
			if task.Attempt == 1 {
				started <- struct{}{}
				<-ctx.Done()
				return Result{}, ctx.Err()
			}
			finished.Add(1)
			return Result{}, nil
		},
	}
	e := New(st, fastConfig(), handlers, slog.New(slog.DiscardHandler))
	startVideo(t, st, "v1", []store.TaskSpec{{ID: "v1:work", Kind: "work", MaxAttempts: 3}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	crashCtx, crash := context.WithCancel(ctx)
	go e.NewWorker("doomed").Run(crashCtx, func() bool { return false })
	<-started
	crash()
	go e.NewWorker("survivor").Run(ctx, func() bool { return false })
	waitFor(t, "the task to finish elsewhere", allDone(st))
	if finished.Load() != 1 || e.Stats().Abandoned.Load() != 1 {
		t.Fatalf("finished %d, abandoned %d", finished.Load(), e.Stats().Abandoned.Load())
	}
}
