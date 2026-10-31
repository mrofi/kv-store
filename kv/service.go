package kv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mrofi/kv-store/store"
)

var (
	ErrNotFound    = errors.New("kv: not found")
	ErrUnavailable = errors.New("kv: unavailable")
)

// pruneTimeout bounds the detached prune goroutine spawned by Put. It
// runs independently of the request that triggered it, so without a
// deadline a hung or slow S3 endpoint would leak that goroutine (and any
// resources it holds) for the lifetime of the process.
const pruneTimeout = 30 * time.Second

// Pusher broadcasts writes and deletes to peer instances so their local
// caches warm up (or evict) without waiting for their own S3 round trip.
// It is best-effort: implementations must not surface a per-peer failure
// to the caller, only log it and move on.
type Pusher interface {
	Push(ctx context.Context, key string, entry store.Entry)
	PushDelete(ctx context.Context, key string)
}

type noopPusher struct{}

func (noopPusher) Push(ctx context.Context, key string, entry store.Entry) {}
func (noopPusher) PushDelete(ctx context.Context, key string)              {}

type Service struct {
	cache       *store.Cache
	s3          store.S3Store
	pusher      Pusher
	maxVersions int
	cacheTTL    time.Duration
	locks       keyedMutex
	wg          sync.WaitGroup
}

// cacheTTL, when greater than zero, lets a cache hit skip the S3 Head
// check entirely as long as it was cached within the TTL window —
// trading a bounded amount of staleness for avoiding a network round
// trip on every single read. Zero disables this and always confirms
// freshness via Head, as before.
func NewService(cache *store.Cache, s3 store.S3Store, pusher Pusher, maxVersions int, cacheTTL time.Duration) *Service {
	if pusher == nil {
		pusher = noopPusher{}
	}
	return &Service{cache: cache, s3: s3, pusher: pusher, maxVersions: maxVersions, cacheTTL: cacheTTL}
}

// cacheSet stamps entry with the current time as CachedAt before storing
// it, so the TTL fast path in Get can measure how long ago this instance
// last confirmed the entry fresh.
func (s *Service) cacheSet(key string, entry store.Entry) store.Entry {
	entry.CachedAt = time.Now()
	s.cache.Set(key, entry)
	return entry
}

// Get returns the latest value for key. stale is true only when S3 was
// unreachable and a cached value was served instead of the confirmed
// latest value.
func (s *Service) Get(ctx context.Context, key string) (entry store.Entry, stale bool, err error) {
	cached, hit := s.cache.Get(key)

	if hit && s.cacheTTL > 0 && time.Since(cached.CachedAt) < s.cacheTTL {
		return cached, false, nil
	}

	if !hit {
		fresh, err := s.s3.Get(ctx, key)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return store.Entry{}, false, ErrNotFound
			}
			return store.Entry{}, false, ErrUnavailable
		}
		fresh = s.cacheSet(key, fresh)
		return fresh, false, nil
	}

	head, err := s.s3.Head(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.cache.Delete(key)
			return store.Entry{}, false, ErrNotFound
		}
		// S3 unreachable/erroring: fall back to the cached value.
		return cached, true, nil
	}

	if cached.VersionID == head.VersionID {
		cached = s.cacheSet(key, cached)
		return cached, false, nil
	}

	fresh, err := s.s3.Get(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.cache.Delete(key)
			return store.Entry{}, false, ErrNotFound
		}
		return cached, true, nil
	}

	fresh = s.cacheSet(key, fresh)
	return fresh, false, nil
}

func (s *Service) GetVersion(ctx context.Context, key, versionID string) (store.Entry, error) {
	entry, err := s.s3.GetVersion(ctx, key, versionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Entry{}, ErrNotFound
		}
		return store.Entry{}, err
	}
	return entry, nil
}

func (s *Service) ListVersions(ctx context.Context, key string) ([]store.ObjectVersion, error) {
	return s.s3.ListVersions(ctx, key)
}

// List returns up to limit existing keys starting with prefix, in
// lexicographic order (limit <= 0 means store.MaxListLimit, the most one
// call returns). It always asks S3 directly: the in-memory cache only
// holds keys this instance happens to have seen, so it can't answer it.
func (s *Service) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	return s.s3.List(ctx, prefix, limit)
}

// Ping reports whether S3 is reachable, for readiness checks. It's
// deliberately not the same thing as liveness: S3 being briefly
// unreachable doesn't mean this process is broken and needs restarting,
// only that it can't currently serve most requests and traffic should be
// routed elsewhere until Ping succeeds again.
//
// A "not found" response for the probe key still means S3 itself
// answered the request — only some other error indicates S3 is actually
// unreachable.
func (s *Service) Ping(ctx context.Context) error {
	_, err := s.s3.Head(ctx, "__readyz_probe__")
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

// Put writes value to S3 and updates the local cache. It returns once S3
// has durably stored the value; peer cache-warming and version pruning
// continue asynchronously afterward and never affect the returned error.
//
// The per-key lock held for the duration of the S3 write and cache
// update prevents a concurrent Delete on the same key from interleaving
// between them — without it, Delete's S3 delete and cache eviction could
// both complete after Put's S3 write but before Put's cache write,
// leaving the cache holding a value S3 no longer has.
func (s *Service) Put(ctx context.Context, key string, value []byte) (store.Entry, error) {
	unlock := s.locks.Lock(key)
	defer unlock()

	entry, err := s.s3.Put(ctx, key, value)
	if err != nil {
		return store.Entry{}, err
	}
	entry = s.cacheSet(key, entry)

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.pusher.Push(context.Background(), key, entry)
	}()
	go func() {
		defer s.wg.Done()
		pruneCtx, cancel := context.WithTimeout(context.Background(), pruneTimeout)
		defer cancel()
		if err := s.s3.PruneToMax(pruneCtx, key, s.maxVersions); err != nil {
			log.Printf("kv: prune failed for key %q: %v", key, err)
		}
	}()

	return entry, nil
}

func (s *Service) Delete(ctx context.Context, key string) error {
	unlock := s.locks.Lock(key)
	defer unlock()

	if err := s.s3.Delete(ctx, key); err != nil {
		return err
	}
	s.cache.Delete(key)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.pusher.PushDelete(context.Background(), key)
	}()
	return nil
}

// Wait blocks until every in-flight async push/prune goroutine spawned by
// Put or Delete has finished, or ctx is done first. Call it during
// shutdown, after the HTTP server has stopped accepting new requests
// (e.g. after http.Server.Shutdown returns) — that only drains in-flight
// requests, which Put and Delete have already returned from by the time
// their background work is still running.
func (s *Service) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kv: async work still in flight: %w", ctx.Err())
	}
}

// ApplyPush updates the local cache from a peer's cache-warm push. It
// never touches S3 — the next Get for this key still confirms freshness
// against S3 via Head, so an incorrect or stale push can never serve wrong
// data for long, only cost a slower cache hit (or, if CACHE_TTL is set,
// serve stale data for up to the TTL window — which is why an
// out-of-order push is rejected below rather than trusted blindly).
//
// A push whose entry is not newer than what's already cached is ignored:
// peer pushes travel over the network and can arrive out of order, and
// applying a stale one would regress the cache to older data. UpdatedAt
// (the writer's local clock at write time) is the ordering signal here,
// since S3 version IDs are opaque and not comparable for recency.
// The comparison and the write happen atomically via Cache.SetIfNewer —
// a separate Get-then-Set would leave a race window where two concurrent
// pushes could both read the same "current" value and let the older one
// win depending on scheduling, undoing the ordering guarantee entirely.
func (s *Service) ApplyPush(key string, entry store.Entry) {
	entry.CachedAt = time.Now()
	s.cache.SetIfNewer(key, entry, func(existing store.Entry, exists bool) bool {
		return !exists || entry.UpdatedAt.After(existing.UpdatedAt)
	})
}

// ApplyDelete evicts key from the local cache in response to a peer's
// delete notification. Like ApplyPush, this is a latency optimization,
// not a correctness guarantee: a sufficiently stale, out-of-order push
// arriving after this could still repopulate the cache with the
// pre-delete value until the next S3 Head confirms it's gone (immediately
// if CACHE_TTL is disabled, otherwise within the TTL window).
func (s *Service) ApplyDelete(key string) {
	s.cache.Delete(key)
}
