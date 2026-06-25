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

	// Optional issuance annotation. A router that declares its own `tls`
	// block opts out of Traefik's entrypoint-level TLS defaults entirely
	// (including any default `certResolver`), so in a split-issuer
	// topology the ACME-issuing Traefik never sees a resolver on such a
	// router and never issues its cert. `?certresolver=<name>` re-supplies
	// the resolver to exactly those routers, for the poller that issues.
	// See README "Per-consumer certResolver injection".
	//
	// `?stripcertresolvers` is the mirror image, for the TERMINATING
	// poller. When an upstream app sets a per-router resolver itself (e.g.
	// `certResolver: http` to pin a non-owned domain to HTTP-01 issuance),
	// that name reaches the terminating Traefik verbatim — but it has no
	// resolver registered and would disable the router with "nonexistent
	// certificate resolver". This flag drops every per-router resolver so
	// the terminating poll is always resolver-free, regardless of what the
	// app declared. The issuer poll (`?certresolver=<name>`) instead keeps
	// app-set resolvers and only fills in the missing ones. The two flags
	// are mutually exclusive; strip wins if both are somehow present.
	switch {
	case r.URL.Query().Has("stripcertresolvers"):
		merged = stripCertResolvers(merged)
	default:
		if name := r.URL.Query().Get("certresolver"); name != "" {
			merged = injectCertResolver(merged, name)
		}
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

// injectCertResolver returns doc with certResolver=name stamped onto
// every http.router that already carries a `tls` block but hasn't set its
// own resolver. Routers without a `tls` block are left alone: they still
// inherit the issuer's entrypoint-level TLS defaults (resolver included),
// so they need no help. An existing per-router certResolver is never
// overwritten — an app that names its own resolver knows what it wants.
//
// It is copy-on-write: every map it changes is rebuilt fresh and untouched
// subtrees are shared by reference, so it never mutates the input. That
// matters because Merge assigns absent keys by reference (dst[k] = sv), so
// the merged document handed in here aliases the cache's stored Doc —
// mutating in place would persist the resolver into every later request,
// including the resolver-less poll the terminating Traefik makes (which
// would then disable the router with "nonexistent certificate resolver").
//
// A router whose `tls` value isn't an object (e.g. the bare `tls: true`
// form some configs use) is left as-is: there's no map to set a key on,
// and that form already inherits the entrypoint default anyway. Scope is
// http.routers only; TCP/UDP ACME isn't muxed here.
func injectCertResolver(doc map[string]any, name string) map[string]any {
	httpv, ok := doc["http"].(map[string]any)
	if !ok {
		return doc
	}
	routers, ok := httpv["routers"].(map[string]any)
	if !ok {
		return doc
	}

	var newRouters map[string]any // built lazily on the first change
	for rn, rv := range routers {
		router, ok := rv.(map[string]any)
		if !ok {
			continue
		}
		tls, ok := router["tls"].(map[string]any)
		if !ok {
			continue
		}
		if _, set := tls["certResolver"]; set {
			continue
		}
		// This router needs stamping. Rebuild it and its tls map rather
		// than mutating the shared originals.
		newTLS := make(map[string]any, len(tls)+1)
		for k, v := range tls {
			newTLS[k] = v
		}
		newTLS["certResolver"] = name
		newRouter := make(map[string]any, len(router))
		for k, v := range router {
			newRouter[k] = v
		}
		newRouter["tls"] = newTLS

		if newRouters == nil {
			newRouters = make(map[string]any, len(routers))
			for k, v := range routers {
				newRouters[k] = v
			}
		}
		newRouters[rn] = newRouter
	}
	if newRouters == nil {
		return doc // nothing stamped; original is already correct
	}

	newHTTP := make(map[string]any, len(httpv))
	for k, v := range httpv {
		newHTTP[k] = v
	}
	newHTTP["routers"] = newRouters
	newDoc := make(map[string]any, len(doc))
	for k, v := range doc {
		newDoc[k] = v
	}
	newDoc["http"] = newHTTP
	return newDoc
}

// stripCertResolvers returns doc with `certResolver` removed from every
// http.router's `tls` block. It is the terminating-poll counterpart to
// injectCertResolver: the issuing Traefik wants resolvers present, the
// terminating Traefik wants them all gone (it has none registered, so any
// name disables the router). A router with no `tls` block, or a `tls` block
// with no resolver, is left untouched.
//
// Copy-on-write, for the same reason injectCertResolver is: the merged
// document aliases the cache's stored Doc (Merge assigns absent keys by
// reference), so mutating in place would corrupt later polls — including the
// issuer's, which must still see app-declared resolvers. Every map this
// rebuilds is fresh; untouched subtrees are shared by reference.
func stripCertResolvers(doc map[string]any) map[string]any {
	httpv, ok := doc["http"].(map[string]any)
	if !ok {
		return doc
	}
	routers, ok := httpv["routers"].(map[string]any)
	if !ok {
		return doc
	}

	var newRouters map[string]any // built lazily on the first change
	for rn, rv := range routers {
		router, ok := rv.(map[string]any)
		if !ok {
			continue
		}
		tls, ok := router["tls"].(map[string]any)
		if !ok {
			continue
		}
		if _, set := tls["certResolver"]; !set {
			continue
		}
		// Rebuild this router and its tls map without the resolver rather
		// than mutating the shared originals.
		newTLS := make(map[string]any, len(tls))
		for k, v := range tls {
			if k == "certResolver" {
				continue
			}
			newTLS[k] = v
		}
		newRouter := make(map[string]any, len(router))
		for k, v := range router {
			newRouter[k] = v
		}
		newRouter["tls"] = newTLS

		if newRouters == nil {
			newRouters = make(map[string]any, len(routers))
			for k, v := range routers {
				newRouters[k] = v
			}
		}
		newRouters[rn] = newRouter
	}
	if newRouters == nil {
		return doc // nothing stripped; original is already correct
	}

	newHTTP := make(map[string]any, len(httpv))
	for k, v := range httpv {
		newHTTP[k] = v
	}
	newHTTP["routers"] = newRouters
	newDoc := make(map[string]any, len(doc))
	for k, v := range doc {
		newDoc[k] = v
	}
	newDoc["http"] = newHTTP
	return newDoc
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
