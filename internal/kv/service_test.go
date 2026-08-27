package kv

import (
	"context"
	"errors"
	"testing"

	"kvstore/internal/store"
)

type recordedPush struct {
	key   string
	entry store.Entry
}

type recordingPusher struct {
	calls chan recordedPush
}

func newRecordingPusher() *recordingPusher {
	return &recordingPusher{calls: make(chan recordedPush, 10)}
}

func (p *recordingPusher) Push(ctx context.Context, key string, entry store.Entry) {
	p.calls <- recordedPush{key: key, entry: entry}
}

func TestGetCacheHitAvoidsBodyFetch(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	s3.HeadCalls = 0
	s3.GetCalls = 0

	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if stale {
		t.Fatal("expected fresh result")
	}
	if string(got.Value) != "bar" {
		t.Fatalf("got value %q, want %q", got.Value, "bar")
	}
	if s3.GetCalls != 0 {
		t.Fatalf("expected no GetObject calls on cache hit, got %d", s3.GetCalls)
	}
	if s3.HeadCalls != 1 {
		t.Fatalf("expected exactly 1 HeadObject call, got %d", s3.HeadCalls)
	}
}

func TestGetCacheMissFetchesDirectlyWithoutHead(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	if _, err := s3.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if stale {
		t.Fatal("expected fresh result")
	}
	if string(got.Value) != "bar" {
		t.Fatalf("got value %q, want %q", got.Value, "bar")
	}
	if s3.HeadCalls != 0 {
		t.Fatalf("expected no HeadObject call on cache miss, got %d", s3.HeadCalls)
	}
	if s3.GetCalls != 1 {
		t.Fatalf("expected exactly 1 GetObject call, got %d", s3.GetCalls)
	}
}

func TestGetCacheStaleRefetches(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	v1, err := s3.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", v1)
	// Simulate another instance writing a newer version directly to S3
	// without pushing to this instance's cache.
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if stale {
		t.Fatal("expected non-stale result since S3 is reachable")
	}
	if string(got.Value) != "v2" {
		t.Fatalf("got value %q, want %q", got.Value, "v2")
	}
}

func TestGetNotFound(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, _, err := svc.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetServesStaleCacheWhenS3Unreachable(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	s3.HeadErr = errors.New("simulated network error")
	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !stale {
		t.Fatal("expected stale=true when S3 is unreachable but cache has a value")
	}
	if string(got.Value) != "bar" {
		t.Fatalf("got value %q, want %q", got.Value, "bar")
	}
}

func TestGetUnavailableWhenS3DownAndNoCache(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	s3.GetErr = errors.New("simulated network error")
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, _, err := svc.Get(context.Background(), "foo")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestGetCacheHitButKeyNowDeletedInS3(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	if err := s3.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, _, err = svc.Get(context.Background(), "foo")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, ok := cache.Get("foo"); ok {
		t.Fatal("expected cache entry to be evicted")
	}
}

func TestGetVersionReturnsSpecificVersion(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	v1, err := s3.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	got, err := svc.GetVersion(context.Background(), "foo", v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersion returned error: %v", err)
	}
	if string(got.Value) != "v1" {
		t.Fatalf("got value %q, want v1", got.Value)
	}
}

func TestGetVersionNotFound(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, err := svc.GetVersion(context.Background(), "foo", "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListVersionsReturnsAll(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	if _, err := s3.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	versions, err := svc.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}
