package kv

import (
	"context"
	"errors"

	"kvstore/internal/store"
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
}

func NewService(cache *store.Cache, s3 store.S3Store, pusher Pusher, maxVersions int) *Service {
	if pusher == nil {
		pusher = noopPusher{}
	}
	return &Service{cache: cache, s3: s3, pusher: pusher, maxVersions: maxVersions}
}

// Get returns the latest value for key. stale is true only when S3 was
// unreachable and a cached value was served instead of the confirmed
// latest value.
func (s *Service) Get(ctx context.Context, key string) (entry store.Entry, stale bool, err error) {
	cached, hit := s.cache.Get(key)

	if !hit {
		fresh, err := s.s3.Get(ctx, key)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return store.Entry{}, false, ErrNotFound
			}
			return store.Entry{}, false, ErrUnavailable
		}
		s.cache.Set(key, fresh)
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

	s.cache.Set(key, fresh)
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
