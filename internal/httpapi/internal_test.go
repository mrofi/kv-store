package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kvstore/internal/peers"
)

func TestInternalAuthRejectsMissingToken(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestInternalAuthAcceptsCorrectToken(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	req.Header.Set("X-Internal-Auth", "secret")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if svc.appliedPushKey != "foo" || string(svc.appliedPushEntry.Value) != "body" {
		t.Fatalf("ApplyPush not called correctly: key=%q value=%q", svc.appliedPushKey, svc.appliedPushEntry.Value)
	}
}

func TestInternalAuthSkippedWhenTokenUnset(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestHandleSetPeersReplacesRegistry(t *testing.T) {
	registry := peers.NewRegistry("", []string{"http://old"})
	srv := NewServer(&fakeKVService{}, registry, "")

	req := httptest.NewRequest(http.MethodPut, "/internal/peers", bytes.NewReader([]byte(`{"peers":["http://a","http://b"]}`)))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	got := registry.List()
	if len(got) != 2 || got[0] != "http://a" || got[1] != "http://b" {
		t.Fatalf("registry.List() = %v, want [http://a http://b]", got)
	}
}
