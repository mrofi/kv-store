package peers

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"kvstore/store"
)

// PushReceiver is the subset of kv.Service a push handler needs. Declared
// here (rather than importing kv directly) so this package never depends
// on kv — kv.Service already satisfies this interface as-is.
type PushReceiver interface {
	ApplyPush(key string, entry store.Entry)
	ApplyDelete(key string)
}

func withInternalAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.Header.Get("X-Internal-Auth") != token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// NewPushHandler returns an http.Handler for receiving a peer's cache-warm
// push and applying it to receiver. Mount it at whatever path you like —
// the standalone kv-store binary uses POST /internal/push. If authToken is
// non-empty, requests must carry a matching X-Internal-Auth header.
func NewPushHandler(receiver PushReceiver, authToken string) http.Handler {
	return withInternalAuth(authToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		key := r.Header.Get("X-Key")
		if key == "" {
			http.Error(w, "missing X-Key header", http.StatusBadRequest)
			return
		}
		updatedAt, _ := time.Parse(time.RFC3339Nano, r.Header.Get("X-Updated-At"))
		receiver.ApplyPush(key, store.Entry{
			Value:     body,
			VersionID: r.Header.Get("X-Version-Id"),
			ETag:      r.Header.Get("X-ETag"),
			UpdatedAt: updatedAt,
		})
		w.WriteHeader(http.StatusOK)
	}))
}

// NewDeleteHandler returns an http.Handler for receiving a peer's delete
// notification and applying it via receiver.ApplyDelete. Mount it at
// whatever path you like — the standalone kv-store binary uses
// POST /internal/delete. If authToken is non-empty, requests must carry a
// matching X-Internal-Auth header.
func NewDeleteHandler(receiver PushReceiver, authToken string) http.Handler {
	return withInternalAuth(authToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Key")
		if key == "" {
			http.Error(w, "missing X-Key header", http.StatusBadRequest)
			return
		}
		receiver.ApplyDelete(key)
		w.WriteHeader(http.StatusOK)
	}))
}

type setPeersRequest struct {
	Peers []string `json:"peers"`
}

// NewSetPeersHandler returns an http.Handler that replaces registry's peer
// list from a JSON body {"peers": ["http://...", ...]}. Mount it at
// whatever path you like — the standalone kv-store binary uses
// PUT /internal/peers. If authToken is non-empty, requests must carry a
// matching X-Internal-Auth header.
func NewSetPeersHandler(registry *Registry, authToken string) http.Handler {
	return withInternalAuth(authToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req setPeersRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		registry.Set(req.Peers)
		w.WriteHeader(http.StatusOK)
	}))
}
