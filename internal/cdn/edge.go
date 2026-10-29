// Package cdn 是 CDN 的 edge 節點：把 origin（blob store 的公開前綴）的內容快取在離使用者近的地方。
// 命中就直接回傳；沒命中就向 origin 拉取、放進快取。同一個物件同時有很多人沒命中時，只向 origin 拉一次。
package cdn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Region string
	// Capacity 是快取的總位元組數，滿了就淘汰最久沒被讀取的物件。
	Capacity int64
	// OriginLatency 模擬 edge 到 origin 所在資料中心的往返時間。
	OriginLatency time.Duration
}

type Stats struct {
	Hits          atomic.Int64
	Misses        atomic.Int64
	Coalesced     atomic.Int64
	OriginFetches atomic.Int64
	Evictions     atomic.Int64
	BytesServed   atomic.Int64
}

type Edge struct {
	cfg    Config
	origin string
	http   *http.Client
	cache  *lru
	stats  Stats

	mu       sync.Mutex
	inflight map[string]*fetch
	fetched  map[string]int // 每個物件向 origin 拉過幾次，用來驗證合併請求
}

type fetch struct {
	done chan struct{}
	body []byte
	ct   string
	code int
}

func New(cfg Config, origin string) *Edge {
	e := &Edge{cfg: cfg, origin: origin, http: &http.Client{Timeout: 30 * time.Second},
		inflight: make(map[string]*fetch), fetched: make(map[string]int)}
	e.cache = newLRU(cfg.Capacity, func() { e.stats.Evictions.Add(1) })
	return e
}

func (e *Edge) Region() string { return e.cfg.Region }
func (e *Edge) Stats() *Stats  { return &e.stats }

// OriginFetches 回傳每個物件向 origin 拉取的次數。
func (e *Edge) OriginFetches() map[string]int {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]int, len(e.fetched))
	for k, v := range e.fetched {
		out[k] = v
	}
	return out
}

func (e *Edge) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /vod/{path...}", e.serve)
	return mux
}

func (e *Edge) serve(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	if it, ok := e.cache.get(key); ok {
		e.stats.Hits.Add(1)
		e.write(w, it.body, it.contentType, "HIT")
		return
	}
	e.stats.Misses.Add(1)
	f, leader := e.join(key)
	if leader {
		e.fill(r.Context(), key, f)
	} else {
		e.stats.Coalesced.Add(1)
		<-f.done
	}
	if f.code != http.StatusOK {
		// 404 之類的錯誤不快取：影片處理完成後同一個網址就會存在。
		http.Error(w, http.StatusText(f.code), f.code)
		return
	}
	e.write(w, f.body, f.ct, "MISS")
}

func (e *Edge) write(w http.ResponseWriter, body []byte, contentType, cacheStatus string) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("X-Cache", cacheStatus+" from "+e.cfg.Region)
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.Write(body)
	e.stats.BytesServed.Add(int64(len(body)))
}

// join 回傳這個 key 正在進行的拉取；沒有的話建立一個，呼叫者負責執行。
func (e *Edge) join(key string) (*fetch, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if f, ok := e.inflight[key]; ok {
		return f, false
	}
	f := &fetch{done: make(chan struct{})}
	e.inflight[key] = f
	return f, true
}

func (e *Edge) fill(ctx context.Context, key string, f *fetch) {
	defer func() {
		e.mu.Lock()
		delete(e.inflight, key)
		e.mu.Unlock()
		close(f.done)
	}()
	// 等待中的其他請求不應該因為發起者斷線而一起失敗。
	ctx = context.WithoutCancel(ctx)
	time.Sleep(e.cfg.OriginLatency)
	e.stats.OriginFetches.Add(1)
	e.mu.Lock()
	e.fetched[key]++
	e.mu.Unlock()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.origin+"/o/"+key, nil)
	if err != nil {
		f.code = http.StatusBadGateway
		return
	}
	resp, err := e.http.Do(req)
	if err != nil {
		f.code = http.StatusBadGateway
		return
	}
	defer resp.Body.Close()
	f.code = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		return
	}
	if f.body, err = io.ReadAll(resp.Body); err != nil {
		f.code = http.StatusBadGateway
		return
	}
	f.ct = resp.Header.Get("Content-Type")
	e.cache.put(key, item{body: f.body, contentType: f.ct})
}
