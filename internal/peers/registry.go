package peers

import "sync"

// Registry holds the set of peer base URLs used for cache-warm pushes. It
// is safe for concurrent use and can be updated at runtime without a
// process restart.
type Registry struct {
	mu    sync.RWMutex
	self  string
	peers []string
}

func NewRegistry(self string, initial []string) *Registry {
	r := &Registry{self: self}
	r.Set(initial)
	return r
}

// List returns the current peer URLs, excluding self.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.peers))
	copy(out, r.peers)
	return out
}

// Set replaces the peer list, excluding self if present.
func (r *Registry) Set(newPeers []string) {
	filtered := make([]string, 0, len(newPeers))
	for _, p := range newPeers {
		if p != "" && p != r.self {
			filtered = append(filtered, p)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = filtered
}
