package tcmux

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Server bundles the merge-on-read endpoints and the cumulative
// collision counters they report.
type Server struct {
	cache *Cache
	log   *slog.Logger

	mu         sync.Mutex
	collisions map[collisionKey]uint64
}

// collisionKey identifies one (path, losing-namespace) collision so the
// counter aggregates by both — operators want to see "namespace X kept
// losing on path Y" trends, not just per-path totals.
type collisionKey struct {
	Path      string
	Namespace string
}

// NewServer returns an http.Handler exposing /config, /healthz and
// /debug. The handler holds a reference to cache; it does not own the
// cache's lifecycle.
func NewServer(cache *Cache, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		cache:      cache,
		log:        log,
		collisions: make(map[collisionKey]uint64),
	}
}

// ServeHTTP routes the three endpoints. Anything else is 404.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/config":
		s.serveConfig(w, r)
	case "/healthz":
		s.serveHealthz(w, r)
	case "/debug":
		s.serveDebug(w, r)
	default:
		http.NotFound(w, r)
	}
}

// serveConfig builds a fresh merged document from the current cache.
// Entries that have never succeeded (LastGood zero) are skipped — they
// have no Doc to contribute. Entries are merged in ascending namespace
// order so Merge's "existing value wins" behaviour realises the
// design's "smaller namespace wins" rule.
func (s *Server) serveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snap := s.cache.Snapshot()

	type contrib struct {
		namespace string
		id        string
		doc       map[string]any
	}
	contribs := make([]contrib, 0, len(snap))
	for id, e := range snap {
		if e.LastGood.IsZero() || e.Doc == nil {
			continue
		}
		ns := e.Namespace
		if ns == "" {
			ns = id
		}
		contribs = append(contribs, contrib{namespace: ns, id: id, doc: e.Doc})
	}
	sort.Slice(contribs, func(i, j int) bool {
		if contribs[i].namespace != contribs[j].namespace {
			return contribs[i].namespace < contribs[j].namespace
		}
		return contribs[i].id < contribs[j].id
	})

	merged := map[string]any{}
	for _, c := range contribs {
		Merge(merged, c.doc, c.namespace, s.recordCollision)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(merged); err != nil {
		s.log.Warn("encode /config response", "err", err)
	}
}

func (s *Server) serveHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

// debugUpstream is the per-upstream shape exposed on /debug. Times are
// RFC3339 (or empty when never-set) so the output is grep-friendly.
type debugUpstream struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace,omitempty"`
	LastGood  string `json:"lastGood,omitempty"`
	Staleness string `json:"staleness"`
	LastErr   string `json:"lastErr,omitempty"`
}

// debugCollision is one row of the cumulative collision counter table.
type debugCollision struct {
	Path      string `json:"path"`
	Namespace string `json:"losingNamespace"`
	Count     uint64 `json:"count"`
}

type debugPayload struct {
	Upstreams  []debugUpstream  `json:"upstreams"`
	Collisions []debugCollision `json:"collisions"`
}

func (s *Server) serveDebug(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snap := s.cache.Snapshot()
	ups := make([]debugUpstream, 0, len(snap))
	for id, e := range snap {
		var lastGood string
		if !e.LastGood.IsZero() {
			lastGood = e.LastGood.UTC().Format(time.RFC3339Nano)
		}
		ups = append(ups, debugUpstream{
			ID:        id,
			Namespace: e.Namespace,
			LastGood:  lastGood,
			Staleness: e.Staleness.String(),
			LastErr:   e.LastErr,
		})
	}
	sort.Slice(ups, func(i, j int) bool { return ups[i].ID < ups[j].ID })

	cols := s.snapshotCollisions()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(debugPayload{Upstreams: ups, Collisions: cols}); err != nil {
		s.log.Warn("encode /debug response", "err", err)
	}
}

func (s *Server) recordCollision(path, namespace string) {
	s.mu.Lock()
	s.collisions[collisionKey{Path: path, Namespace: namespace}]++
	s.mu.Unlock()
	s.log.Warn("merge collision", "path", path, "losingNamespace", namespace)
}

func (s *Server) snapshotCollisions() []debugCollision {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]debugCollision, 0, len(s.collisions))
	for k, n := range s.collisions {
		out = append(out, debugCollision{Path: k.Path, Namespace: k.Namespace, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out
}
