package peers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kvstore/store"
)

type recordedApplyPush struct {
	key   string
	entry store.Entry
}

type fakePushReceiver struct {
	calls []recordedApplyPush
}

func (f *fakePushReceiver) ApplyPush(key string, entry store.Entry) {
	f.calls = append(f.calls, recordedApplyPush{key: key, entry: entry})
}

func TestNewPushHandlerAppliesPush(t *testing.T) {
	receiver := &fakePushReceiver{}
	handler := NewPushHandler(receiver, "")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	req.Header.Set("X-Version-Id", "v1")
	req.Header.Set("X-ETag", "etag1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(receiver.calls) != 1 {
		t.Fatalf("expected 1 ApplyPush call, got %d", len(receiver.calls))
	}
	call := receiver.calls[0]
	if call.key != "foo" || string(call.entry.Value) != "body" || call.entry.VersionID != "v1" || call.entry.ETag != "etag1" {
		t.Fatalf("unexpected ApplyPush call: %+v", call)
	}
}

func TestNewPushHandlerRejectsMissingXKey(t *testing.T) {
	receiver := &fakePushReceiver{}
	handler := NewPushHandler(receiver, "")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestNewPushHandlerRejectsMissingAuthWhenTokenSet(t *testing.T) {
	receiver := &fakePushReceiver{}
	handler := NewPushHandler(receiver, "secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestNewPushHandlerAcceptsCorrectAuth(t *testing.T) {
	receiver := &fakePushReceiver{}
	handler := NewPushHandler(receiver, "secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	req.Header.Set("X-Internal-Auth", "secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(receiver.calls) != 1 {
		t.Fatalf("expected 1 ApplyPush call, got %d", len(receiver.calls))
	}
}

func TestNewSetPeersHandlerReplacesRegistry(t *testing.T) {
	registry := NewRegistry("", []string{"http://old"})
	handler := NewSetPeersHandler(registry, "")

	req := httptest.NewRequest(http.MethodPut, "/internal/peers", bytes.NewReader([]byte(`{"peers":["http://a","http://b"]}`)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	got := registry.List()
	if len(got) != 2 || got[0] != "http://a" || got[1] != "http://b" {
		t.Fatalf("registry.List() = %v, want [http://a http://b]", got)
	}
}

func TestNewSetPeersHandlerRejectsInvalidBody(t *testing.T) {
	registry := NewRegistry("", nil)
	handler := NewSetPeersHandler(registry, "")

	req := httptest.NewRequest(http.MethodPut, "/internal/peers", strings.NewReader("not json"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestNewSetPeersHandlerRejectsMissingAuthWhenTokenSet(t *testing.T) {
	registry := NewRegistry("", nil)
	handler := NewSetPeersHandler(registry, "secret")

	req := httptest.NewRequest(http.MethodPut, "/internal/peers", bytes.NewReader([]byte(`{"peers":["http://a"]}`)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}
