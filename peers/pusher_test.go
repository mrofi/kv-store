package peers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kvstore/store"
)

func TestHTTPPusherDeliversToReachablePeer(t *testing.T) {
	received := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	registry := NewRegistry("", []string{srv.URL})
	pusher := NewHTTPPusher(registry, "secret", time.Second)

	pusher.Push(context.Background(), "foo", store.Entry{
		Value:     []byte("bar"),
		VersionID: "v1",
		ETag:      "etag1",
		UpdatedAt: time.Now(),
	})

	select {
	case h := <-received:
		if h.Get("X-Key") != "foo" || h.Get("X-Version-Id") != "v1" || h.Get("X-Internal-Auth") != "secret" {
			t.Fatalf("unexpected headers: %+v", h)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for push request")
	}
}

func TestHTTPPusherSkipsUnreachablePeerWithinTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	registry := NewRegistry("", []string{slow.URL})
	pusher := NewHTTPPusher(registry, "", 50*time.Millisecond)

	start := time.Now()
	pusher.Push(context.Background(), "foo", store.Entry{Value: []byte("bar")})
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("Push took %v, expected it to give up around the 50ms per-peer timeout", elapsed)
	}
}
