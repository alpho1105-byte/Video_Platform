package videos

import (
	"container/list"
	"sync"
	"sync/atomic"

	"video-platform/internal/domain"
)

// Cache 是 metadata 前面的 LRU 快取，只放已經不會再改變的影片，所以不需要失效機制。
type Cache struct {
	mu     sync.Mutex
	cap    int
	order  *list.List
	items  map[string]*list.Element
	hits   atomic.Int64
	misses atomic.Int64
}

func NewCache(capacity int) *Cache {
	return &Cache{cap: capacity, order: list.New(), items: make(map[string]*list.Element)}
}

func (c *Cache) Get(id string) (domain.Video, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[id]
	if !ok {
		c.misses.Add(1)
		return domain.Video{}, false
	}
	c.hits.Add(1)
	c.order.MoveToFront(e)
	return e.Value.(domain.Video), true
}

func (c *Cache) Put(v domain.Video) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[v.ID]; ok {
		e.Value = v
		c.order.MoveToFront(e)
		return
	}
	c.items[v.ID] = c.order.PushFront(v)
	if c.order.Len() > c.cap {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(domain.Video).ID)
	}
}

func (c *Cache) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}
