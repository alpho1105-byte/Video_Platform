// Package workflow 是一個小型的 DAG 編排引擎（概念上類似 Temporal）：任務與相依關係存在資料庫，
// worker 以租約領取任務、定期續約、完成時可以產生新的節點。worker 當機時租約過期，任務由別人接手；
// 租約過期的 worker 晚一步回來，結果會被 lease token 擋下。
package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"video-platform/internal/store"
)

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent 標記不該重試的錯誤，例如上傳的檔案本身就是壞的。
func Permanent(err error) error { return permanentError{err: err} }

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Result 是任務的輸出；Spawn 是執行後才知道要建立的節點（例如切段後才知道有幾段要轉碼）。
type Result struct {
	Output any
	Spawn  []store.TaskSpec
}

type Handler func(ctx context.Context, t store.Task) (Result, error)

type Config struct {
	LeaseTTL    time.Duration
	Poll        time.Duration
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func DefaultConfig() Config {
	return Config{LeaseTTL: 30 * time.Second, Poll: 100 * time.Millisecond, BaseBackoff: time.Second, MaxBackoff: time.Minute}
}

type Stats struct {
	Completed atomic.Int64
	Retried   atomic.Int64
	Failed    atomic.Int64
	// Rejected 是租約已經過期的 worker 想回報結果、被擋下的次數。
	Rejected atomic.Int64
	// Abandoned 是 worker 在任務進行中當機、沒有回報的次數。
	Abandoned atomic.Int64
}

type Engine struct {
	store    *store.Store
	cfg      Config
	handlers map[string]Handler
	log      *slog.Logger
	stats    Stats
}

func New(st *store.Store, cfg Config, handlers map[string]Handler, log *slog.Logger) *Engine {
	return &Engine{store: st, cfg: cfg, handlers: handlers, log: log}
}

func (e *Engine) Stats() *Stats { return &e.stats }

// backoff 是指數退避加 full jitter：在 [0, min(max, base·2^(attempt-1))] 之間隨機。
func (e *Engine) backoff(attempt int) time.Duration {
	d := e.cfg.MaxBackoff
	if attempt < 30 {
		d = min(e.cfg.MaxBackoff, e.cfg.BaseBackoff<<(attempt-1))
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// Worker 一次執行一個任務。
type Worker struct {
	id     string
	e      *Engine
	busy   atomic.Bool
	stall  atomic.Int64
	frozen atomic.Bool
}

func (e *Engine) NewWorker(id string) *Worker {
	return &Worker{id: id, e: e}
}

func (w *Worker) ID() string { return w.id }
func (w *Worker) Busy() bool { return w.busy.Load() }

// Stall 讓 worker 在下一個任務做完、回報之前整個停住 d（包括續約），模擬長時間的 GC pause 或網路分區。
func (w *Worker) Stall(d time.Duration) { w.stall.Store(int64(d)) }

// Run 持續領取並執行任務，直到 ctx 結束或 drain 回傳 true（在兩個任務之間檢查）。
func (w *Worker) Run(ctx context.Context, drain func() bool) {
	for ctx.Err() == nil && !drain() {
		t, ok, err := w.e.store.Lease(ctx, w.id, w.e.cfg.LeaseTTL)
		if err != nil || !ok {
			if err != nil && ctx.Err() == nil {
				w.e.log.Error("lease", "worker", w.id, "err", err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(w.e.cfg.Poll/2 + rand.N(w.e.cfg.Poll)):
			}
			continue
		}
		w.busy.Store(true)
		w.execute(ctx, t)
		w.busy.Store(false)
	}
}

func (w *Worker) execute(ctx context.Context, t store.Task) {
	report := context.WithoutCancel(ctx)
	if t.Attempt > t.MaxAttempt {
		// 租約一再過期代表執行這個任務的 worker 一直死掉，不能無限重來。
		w.fail(report, t, fmt.Errorf("gave up after %d attempts", t.Attempt-1), true)
		return
	}
	h, ok := w.e.handlers[t.Kind]
	if !ok {
		w.fail(report, t, fmt.Errorf("no handler for task kind %q", t.Kind), true)
		return
	}
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	hbDone := make(chan struct{})
	stopHB := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(taskCtx, t, stopHB, cancel)
	}()
	res, err := h(taskCtx, t)
	close(stopHB)
	<-hbDone
	if d := time.Duration(w.stall.Swap(0)); d > 0 {
		w.frozen.Store(true)
		time.Sleep(d)
		w.frozen.Store(false)
	}
	if ctx.Err() != nil {
		// worker 被終止（當機）：什麼都不回報，租約到期後由別人重做。
		w.e.stats.Abandoned.Add(1)
		return
	}
	if err != nil {
		w.fail(report, t, err, IsPermanent(err))
		return
	}
	switch err := w.e.store.Complete(report, t.ID, t.Token, res.Output, res.Spawn); {
	case errors.Is(err, store.ErrLeaseLost):
		w.e.stats.Rejected.Add(1)
		w.e.log.Warn("lease lost before completing", "task", t.ID, "worker", w.id)
	case err != nil:
		w.e.log.Error("complete", "task", t.ID, "err", err)
	default:
		w.e.stats.Completed.Add(1)
	}
}

func (w *Worker) heartbeat(ctx context.Context, t store.Task, stop <-chan struct{}, abort context.CancelFunc) {
	tick := time.NewTicker(w.e.cfg.LeaseTTL / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-tick.C:
			if err := w.e.store.Renew(ctx, t.ID, t.Token, w.e.cfg.LeaseTTL); errors.Is(err, store.ErrLeaseLost) {
				abort()
				return
			}
		}
	}
}

func (w *Worker) fail(ctx context.Context, t store.Task, cause error, permanent bool) {
	failed, err := w.e.store.Fail(ctx, t.ID, t.Token, cause.Error(), time.Now().Add(w.e.backoff(t.Attempt)), permanent)
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		w.e.stats.Rejected.Add(1)
	case err != nil:
		w.e.log.Error("fail", "task", t.ID, "err", err)
	case failed:
		w.e.stats.Failed.Add(1)
		w.e.log.Info("workflow failed", "workflow", t.WorkflowID, "task", t.ID, "err", cause)
	default:
		w.e.stats.Retried.Add(1)
	}
}
