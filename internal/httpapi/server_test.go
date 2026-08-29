package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kvstore/kv"
	"kvstore/peers"
	"kvstore/store"
)

func TestHandlePutReturnsVersionHeaders(t *testing.T) {
	svc := &fakeKVService{putEntry: store.Entry{VersionID: "v1", ETag: "etag1"}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader("bar"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Header().Get("X-Version-Id") != "v1" {
		t.Fatalf("X-Version-Id = %q, want v1", rec.Header().Get("X-Version-Id"))
	}
}

func TestHandleGetReturnsValueAndHeaders(t *testing.T) {
	svc := &fakeKVService{getEntry: store.Entry{Value: []byte("bar"), VersionID: "v1", UpdatedAt: time.Now()}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "bar" {
		t.Fatalf("body = %q, want bar", rec.Body.String())
	}
	if rec.Header().Get("X-Stale") != "" {
		t.Fatal("expected no X-Stale header for fresh result")
	}
}

func TestHandleGetSetsStaleHeader(t *testing.T) {
	svc := &fakeKVService{getEntry: store.Entry{Value: []byte("bar")}, getStale: true}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Header().Get("X-Stale") != "true" {
		t.Fatalf("X-Stale = %q, want true", rec.Header().Get("X-Stale"))
	}
}

func TestHandleGetNotFound(t *testing.T) {
	svc := &fakeKVService{getErr: kv.ErrNotFound}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestHandleGetUnavailable(t *testing.T) {
	svc := &fakeKVService{getErr: kv.ErrUnavailable}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestHandleGetSpecificVersion(t *testing.T) {
	svc := &fakeKVService{versionEntry: store.Entry{Value: []byte("old"), VersionID: "v0"}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo?version=v0", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Body.String() != "old" {
		t.Fatalf("body = %q, want old", rec.Body.String())
	}
}

func TestHandleListVersions(t *testing.T) {
	svc := &fakeKVService{versions: []store.ObjectVersion{{VersionID: "v1"}, {VersionID: "v0"}}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo/versions", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "v1") || !strings.Contains(rec.Body.String(), "v0") {
		t.Fatalf("body = %q, expected both versions listed", rec.Body.String())
	}
}

func TestHandleDelete(t *testing.T) {
	srv := NewServer(&fakeKVService{}, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodDelete, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
}

func TestHandlePutRejectsKeyContainingSlash(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodPut, "/kv/foo%2Fbar", strings.NewReader("bar"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandleGetRejectsOversizedKey(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/"+strings.Repeat("a", 513), nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandleDeleteRejectsKeyContainingSlash(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodDelete, "/kv/foo%2Fbar", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandleListVersionsRejectsKeyContainingSlash(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo%2Fbar/versions", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandleHealthz(t *testing.T) {
	srv := NewServer(&fakeKVService{}, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
