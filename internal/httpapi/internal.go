package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"kvstore/peers"
	"kvstore/store"
)

func withInternalAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.Header.Get("X-Internal-Auth") != token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func handlePush(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		svc.ApplyPush(key, store.Entry{
			Value:     body,
			VersionID: r.Header.Get("X-Version-Id"),
			ETag:      r.Header.Get("X-ETag"),
			UpdatedAt: updatedAt,
		})
		w.WriteHeader(http.StatusOK)
	}
}

type setPeersRequest struct {
	Peers []string `json:"peers"`
}

func handleSetPeers(registry *peers.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req setPeersRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		registry.Set(req.Peers)
		w.WriteHeader(http.StatusOK)
	}
}
