package peers

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"kvstore/store"
)

// HTTPPusher broadcasts a written entry to every peer's /internal/push
// endpoint. It is best-effort: a slow or unreachable peer is skipped after
// a short timeout and never causes Push to return an error, since a lost
// push only costs a peer one extra S3 round trip on its next read, never
// correctness.
type HTTPPusher struct {
	registry  *Registry
	client    *http.Client
	authToken string
}

func NewHTTPPusher(registry *Registry, authToken string, perPeerTimeout time.Duration) *HTTPPusher {
	return &HTTPPusher{
		registry:  registry,
		client:    &http.Client{Timeout: perPeerTimeout},
		authToken: authToken,
	}
}

func (p *HTTPPusher) Push(ctx context.Context, key string, entry store.Entry) {
	for _, peer := range p.registry.List() {
		p.pushOne(ctx, peer, key, entry)
	}
}

func (p *HTTPPusher) pushOne(ctx context.Context, peer, key string, entry store.Entry) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer+"/internal/push", bytes.NewReader(entry.Value))
	if err != nil {
		log.Printf("peers: building push request to %s failed: %v", peer, err)
		return
	}
	req.Header.Set("X-Key", key)
	req.Header.Set("X-Version-Id", entry.VersionID)
	req.Header.Set("X-ETag", entry.ETag)
	req.Header.Set("X-Updated-At", entry.UpdatedAt.Format(time.RFC3339Nano))
	req.Header.Set("Content-Length", strconv.Itoa(len(entry.Value)))
	if p.authToken != "" {
		req.Header.Set("X-Internal-Auth", p.authToken)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("peers: push to %s failed: %v", peer, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("peers: push to %s returned status %d", peer, resp.StatusCode)
	}
}
