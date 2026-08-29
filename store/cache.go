package store

import (
	"container/list"
	"sync"
	"time"
)

type Entry struct {
	Value     []byte
	VersionID string
	ETag      string
	UpdatedAt time.Time

	// CachedAt is when this instance last confirmed the entry fresh
	// (via a write, a peer push, or an S3 Head match) — distinct from
	// UpdatedAt, which is S3's own object modification time. Only
	// kv.Service reads or writes this field; S3Store implementations
	// leave it zero.
	CachedAt time.Time
}

type cacheItem struct {
	key   string
	entry Entry
}

// Cache is an in-memory, LRU-evicting store for Entry values. When
// maxEntries is 0 (or negative), it never evicts and grows unbounded —
// matching the behavior before eviction existed. Get counts as a "use"
// and refreshes an entry's recency, same as Set.
type Cache struct {
	mu         sync.Mutex
	maxEntries int
	order      *list.List
	items      map[string]*list.Element
}

func NewCache(maxEntries int) *Cache {
	return &Cache{
		maxEntries: maxEntries,
		order:      list.New(),
		items:      make(map[string]*list.Element),
	}
}

func (c *Cache) Get(key string) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return Entry{}, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*cacheItem).entry, true
}

func (c *Cache) Set(key string, entry Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		el.Value.(*cacheItem).entry = entry
		c.order.MoveToFront(el)
		return
	}

	el := c.order.PushFront(&cacheItem{key: key, entry: entry})
	c.items[key] = el

	if c.maxEntries > 0 && c.order.Len() > c.maxEntries {
		c.evictOldest()
	}
}

// SetIfNewer atomically decides whether entry should replace whatever is
// currently cached for key, in one lock acquisition. newer is called
// with the currently cached entry (zero value if none) and whether one
// exists; it returns true if entry should be stored. Use this instead of
// a separate Get-then-Set when the decision to overwrite depends on the
// current value — a check-then-act pair of calls has a race window
// between them where a concurrent caller can invalidate the check.
func (c *Cache) SetIfNewer(key string, entry Entry, newer func(existing Entry, exists bool) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		if !newer(el.Value.(*cacheItem).entry, true) {
			return
		}
		el.Value.(*cacheItem).entry = entry
		c.order.MoveToFront(el)
		return
	}

	if !newer(Entry{}, false) {
		return
	}

	el := c.order.PushFront(&cacheItem{key: key, entry: entry})
	c.items[key] = el

	if c.maxEntries > 0 && c.order.Len() > c.maxEntries {
		c.evictOldest()
	}
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.order.Remove(el)
		delete(c.items, key)
	}
}

func (c *Cache) evictOldest() {
	el := c.order.Back()
	if el == nil {
		return
	}
	c.order.Remove(el)
	delete(c.items, el.Value.(*cacheItem).key)
}
