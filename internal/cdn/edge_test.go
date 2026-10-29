package cdn

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func origin(t *testing.T, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/o/vod/v1/seg.seg":
			w.Write(make([]byte, 100))
		case "/o/vod/v1/other.seg", "/o/vod/v1/third.seg":
			w.Write(make([]byte, 60))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (int, string, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("X-Cache"), len(body)
}

// 很多人同時要同一個還沒快取的段落：只向 origin 拉一次，其他人等它。
func TestConcurrentMissesAreCoalescedIntoOneOriginFetch(t *testing.T) {
	var hits atomic.Int64
	e := New(Config{Region: "tw", Capacity: 1 << 20, OriginLatency: 50 * time.Millisecond}, origin(t, &hits).URL)
	edge := httptest.NewServer(e.Handler())
	defer edge.Close()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if code, _, n := get(t, edge.URL+"/vod/v1/seg.seg"); code != 200 || n != 100 {
				t.Errorf("got %d with %d bytes", code, n)
			}
		}()
	}
	wg.Wait()
	if hits.Load() != 1 || e.Stats().Coalesced.Load() != 19 {
		t.Fatalf("origin hits %d, coalesced %d", hits.Load(), e.Stats().Coalesced.Load())
	}
	if _, cache, _ := get(t, edge.URL+"/vod/v1/seg.seg"); cache != "HIT from tw" {
		t.Fatalf("X-Cache = %q", cache)
	}
}

func TestNotFoundIsNotCached(t *testing.T) {
	var hits atomic.Int64
	e := New(Config{Region: "eu", Capacity: 1000}, origin(t, &hits).URL)
	edge := httptest.NewServer(e.Handler())
	defer edge.Close()
	for range 2 {
		if code, _, _ := get(t, edge.URL+"/vod/v1/missing.seg"); code != 404 {
			t.Fatalf("missing object: %d", code)
		}
	}
	if hits.Load() != 2 {
		t.Fatalf("404 was cached: origin hits %d", hits.Load())
	}
}

func TestLRUEvictsTheLeastRecentlyUsedAndSkipsHugeObjects(t *testing.T) {
	evicted := 0
	c := newLRU(800, func() { evicted++ })
	for _, k := range []string{"a", "b", "c", "d"} {
		c.put(k, item{body: make([]byte, 100)})
	}
	c.get("a") // a 變成最近使用
	c.put("huge", item{body: make([]byte, 101)})
	if _, ok := c.get("huge"); ok || evicted != 0 {
		t.Fatal("an object above 1/8 of the capacity was cached")
	}
	for _, k := range []string{"e", "f", "g", "h", "i"} {
		c.put(k, item{body: make([]byte, 100)}) // 900 > 800：淘汰最久沒用的 b
	}
	if _, ok := c.get("b"); ok || evicted != 1 {
		t.Fatalf("b survived, evictions %d", evicted)
	}
	if _, ok := c.get("a"); !ok {
		t.Fatal("the recently used object was evicted")
	}
}
