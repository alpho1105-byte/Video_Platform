package cdn

import (
	"container/list"
	"sync"
)

type item struct {
	body        []byte
	contentType string
}

type entry struct {
	key string
	item
}

// lru 以位元組數為容量的 LRU 快取。單一物件超過容量的 1/8 就不放，避免一個大物件擠掉一大批熱門段落。
type lru struct {
	mu      sync.Mutex
	cap     int64
	size    int64
	order   *list.List
	items   map[string]*list.Element
	onEvict func()
}

func newLRU(capacity int64, onEvict func()) *lru {
	return &lru{cap: capacity, order: list.New(), items: make(map[string]*list.Element), onEvict: onEvict}
}

func (c *lru) get(key string) (item, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return item{}, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*entry).item, true
}

func (c *lru) put(key string, it item) {
	n := int64(len(it.body))
	if n > c.cap/8 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.size += n - int64(len(e.Value.(*entry).body))
		e.Value.(*entry).item = it
		c.order.MoveToFront(e)
	} else {
		c.items[key] = c.order.PushFront(&entry{key: key, item: it})
		c.size += n
	}
	for c.size > c.cap {
		oldest := c.order.Back()
		ent := oldest.Value.(*entry)
		c.order.Remove(oldest)
		delete(c.items, ent.key)
		c.size -= int64(len(ent.body))
		c.onEvict()
	}
}
