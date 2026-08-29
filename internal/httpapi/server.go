package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"kvstore/kv"
	"kvstore/peers"
	"kvstore/store"
)

// KVService is the subset of *kv.Service the HTTP layer depends on,
// declared here (rather than importing *kv.Service directly) so this
// package can be tested with a fake.
type KVService interface {
	Put(ctx context.Context, key string, value []byte) (store.Entry, error)
	Get(ctx context.Context, key string) (entry store.Entry, stale bool, err error)
	GetVersion(ctx context.Context, key, versionID string) (store.Entry, error)
	ListVersions(ctx context.Context, key string) ([]store.ObjectVersion, error)
	Delete(ctx context.Context, key string) error
	ApplyPush(key string, entry store.Entry)
}

type Server struct {
	mux *http.ServeMux
}

// registry and internalAuthToken are threaded through now, even though
// this task doesn't use them yet, so the signature doesn't change again
// when Task 11 wires up the internal routes that need them.
func NewServer(svc KVService, registry *peers.Registry, internalAuthToken string) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /kv/{key}", handlePut(svc))
	mux.HandleFunc("GET /kv/{key}/versions", handleListVersions(svc))
	mux.HandleFunc("GET /kv/{key}", handleGet(svc))
	mux.HandleFunc("DELETE /kv/{key}", handleDelete(svc))
	mux.HandleFunc("GET /healthz", handleHealthz)

	mux.Handle("POST /internal/push", peers.NewPushHandler(svc, internalAuthToken))
	mux.Handle("PUT /internal/peers", peers.NewSetPeersHandler(registry, internalAuthToken))

	return &Server{mux: mux}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func handlePut(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := validateKey(key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		entry, err := svc.Put(r.Context(), key, body)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Version-Id", entry.VersionID)
		w.Header().Set("X-ETag", entry.ETag)
		w.WriteHeader(http.StatusOK)
	}
}

func handleGet(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := validateKey(key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if versionID := r.URL.Query().Get("version"); versionID != "" {
			entry, err := svc.GetVersion(r.Context(), key, versionID)
			if err != nil {
				writeGetError(w, err)
				return
			}
			writeEntry(w, entry, false)
			return
		}

		entry, stale, err := svc.Get(r.Context(), key)
		if err != nil {
			writeGetError(w, err)
			return
		}
		writeEntry(w, entry, stale)
	}
}

func writeEntry(w http.ResponseWriter, entry store.Entry, stale bool) {
	w.Header().Set("X-Version-Id", entry.VersionID)
	w.Header().Set("X-ETag", entry.ETag)
	w.Header().Set("X-Updated-At", entry.UpdatedAt.Format(time.RFC3339Nano))
	if stale {
		w.Header().Set("X-Stale", "true")
	}
	w.WriteHeader(http.StatusOK)
	w.Write(entry.Value)
}

func writeGetError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, kv.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, kv.ErrUnavailable):
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func handleListVersions(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := validateKey(key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		versions, err := svc.ListVersions(r.Context(), key)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(versions)
	}
}

func handleDelete(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := validateKey(key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := svc.Delete(r.Context(), key); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
