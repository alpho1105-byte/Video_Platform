package workflow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"video-platform/internal/store"
)

type PoolConfig struct {
	Min, Max int
	// PerWorker：每個 worker 分攤多少個待處理任務。worker 數 = ceil(backlog / PerWorker)。
	PerWorker int
	Interval  time.Duration
	// ScaleDownAfter：backlog 持續低於目前容量這麼久才縮減，避免來回震盪。
	ScaleDownAfter time.Duration
}

type member struct {
	w      *Worker
	cancel context.CancelFunc
	drain  atomic.Bool
	done   chan struct{}
}

// Pool 依 backlog（ready + running 的任務數）自動調整 worker 數，吸收上傳尖峰。
type Pool struct {
	e     *Engine
	st    *store.Store
	cfg   PoolConfig
	ctx   context.Context
	mu    sync.Mutex
	live  []*member
	next  int
	peak  int
	below time.Time
}

func NewPool(e *Engine, st *store.Store, cfg PoolConfig) *Pool {
	return &Pool{e: e, st: st, cfg: cfg}
}

func (p *Pool) Run(ctx context.Context) {
	p.mu.Lock()
	p.ctx = ctx
	for range p.cfg.Min {
		p.add()
	}
	p.mu.Unlock()
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.scale(ctx)
		}
	}
}

func (p *Pool) scale(ctx context.Context) {
	backlog, err := p.st.Backlog(ctx)
	if err != nil {
		return
	}
	want := max(p.cfg.Min, min(p.cfg.Max, (backlog+p.cfg.PerWorker-1)/p.cfg.PerWorker))
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case want > len(p.live):
		for len(p.live) < want {
			p.add()
		}
		p.below = time.Time{}
	case want < len(p.live):
		if p.below.IsZero() {
			p.below = time.Now()
		}
		if time.Since(p.below) >= p.cfg.ScaleDownAfter {
			// 一次縮一台，讓它做完手上的任務再離開。
			m := p.live[len(p.live)-1]
			p.live = p.live[:len(p.live)-1]
			m.drain.Store(true)
			p.below = time.Now()
		}
	default:
		p.below = time.Time{}
	}
}

func (p *Pool) add() {
	p.next++
	ctx, cancel := context.WithCancel(p.ctx)
	m := &member{w: p.e.NewWorker(fmt.Sprintf("worker-%d", p.next)), cancel: cancel, done: make(chan struct{})}
	p.live = append(p.live, m)
	p.peak = max(p.peak, len(p.live))
	go func() {
		defer close(m.done)
		m.w.Run(ctx, m.drain.Load)
	}()
}

// Size 回傳目前的 worker 數與尖峰時的 worker 數。
func (p *Pool) Size() (current, peak int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.live), p.peak
}

// busyMember 找一個正在執行任務、而且沒有被凍結（或即將被凍結）的 worker。
func (p *Pool) busyMember() *member {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.live {
		if m.w.Busy() && !m.w.frozen.Load() && m.w.stall.Load() == 0 {
			return m
		}
	}
	return nil
}

// Crash 讓一個正在執行任務的 worker 直接消失（不回報），並立刻補上一台新的。回傳是否找到這樣的 worker。
func (p *Pool) Crash() bool {
	m := p.busyMember()
	if m == nil {
		return false
	}
	m.cancel()
	<-m.done
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, x := range p.live {
		if x == m {
			p.live = append(p.live[:i], p.live[i+1:]...)
			p.add()
			break
		}
	}
	return true
}

// Stall 讓一個正在執行任務的 worker 在回報前停住 d。
func (p *Pool) Stall(d time.Duration) bool {
	m := p.busyMember()
	if m == nil {
		return false
	}
	m.w.Stall(d)
	return true
}
