package peers

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/mrofi/kv-store/store"
)

// HTTPPusher broadcasts writes and deletes to every peer's /internal/push
// and /internal/delete endpoints. It is best-effort: a slow or
// unreachable peer is skipped after a short timeout and never causes a
// call to return an error, since a lost push only costs a peer one extra
// S3 round trip on its next read, never correctness.
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
	p.do(req, peer, "push")
}

func (p *HTTPPusher) PushDelete(ctx context.Context, key string) {
	for _, peer := range p.registry.List() {
		p.pushDeleteOne(ctx, peer, key)
	}
}

func (p *HTTPPusher) pushDeleteOne(ctx context.Context, peer, key string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer+"/internal/delete", nil)
	if err != nil {
		log.Printf("peers: building delete-push request to %s failed: %v", peer, err)
		return
	}
	req.Header.Set("X-Key", key)
	p.do(req, peer, "delete-push")
}

func (p *HTTPPusher) do(req *http.Request, peer, action string) {
	if p.authToken != "" {
		req.Header.Set("X-Internal-Auth", p.authToken)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("peers: %s to %s failed: %v", action, peer, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("peers: %s to %s returned status %d", action, peer, resp.StatusCode)
	}
}
