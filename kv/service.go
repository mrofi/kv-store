package kv

import (
	"context"
	"errors"
	"log"
	"time"

	"kvstore/store"
)

var (
	ErrNotFound    = errors.New("kv: not found")
	ErrUnavailable = errors.New("kv: unavailable")
)

// Pusher broadcasts a freshly written entry to peer instances so their
// local caches warm up without waiting for their own S3 round trip. It is
// best-effort: implementations must not surface a per-peer failure to the
// caller, only log it and move on.
type Pusher interface {
	Push(ctx context.Context, key string, entry store.Entry)
}

type noopPusher struct{}

func (noopPusher) Push(ctx context.Context, key string, entry store.Entry) {}

type Service struct {
	cache       *store.Cache
	s3          store.S3Store
	pusher      Pusher
	maxVersions int
	cacheTTL    time.Duration
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

// Put writes value to S3 and updates the local cache. It returns once S3
// has durably stored the value; peer cache-warming and version pruning
// continue asynchronously afterward and never affect the returned error.
func (s *Service) Put(ctx context.Context, key string, value []byte) (store.Entry, error) {
	entry, err := s.s3.Put(ctx, key, value)
	if err != nil {
		return store.Entry{}, err
	}
	entry = s.cacheSet(key, entry)

	go s.pusher.Push(context.Background(), key, entry)
	go func() {
		if err := s.s3.PruneToMax(context.Background(), key, s.maxVersions); err != nil {
			log.Printf("kv: prune failed for key %q: %v", key, err)
		}
	}()

	return entry, nil
}

func (s *Service) Delete(ctx context.Context, key string) error {
	if err := s.s3.Delete(ctx, key); err != nil {
		return err
	}
	s.cache.Delete(key)
	return nil
}

// ApplyPush updates the local cache from a peer's cache-warm push. It
// never touches S3 — the next Get for this key still confirms freshness
// against S3 via Head, so an incorrect or stale push can never serve wrong
// data, only cost a slower cache hit.
func (s *Service) ApplyPush(key string, entry store.Entry) {
	s.cacheSet(key, entry)
}
