package tcmux

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T, cache *Cache) *Server {
	t.Helper()
	return NewServer(cache, discardLog())
}

// seedEntry stuffs a fully-populated Entry into the cache, bypassing
// Set/Fail. /config and /debug operate on the snapshot, so a hand-crafted
// fixture is the cleanest way to drive corner cases (zero LastGood,
// errored entries, etc.) without orchestrating a fake HTTP server.
func seedEntry(c *Cache, id string, e Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := e
	c.m[id] = &cp
}

func TestServer_Healthz(t *testing.T) {
	s := newTestServer(t, NewCache(nil))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ok") {
		t.Fatalf("body = %q, want to contain ok", w.Body.String())
	}
}

func TestServer_NotFound(t *testing.T) {
	s := newTestServer(t, NewCache(nil))
	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestServer_Config_MergesEntries(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })

	seedEntry(c, "alpha", Entry{
		Namespace: "alpha",
		LastGood:  now,
		Doc: map[string]any{
			"http": map[string]any{
				"routers": map[string]any{
					"alpha-web": map[string]any{"rule": "Host(`alpha`)"},
				},
			},
			"tls": map[string]any{
				"certificates": []any{map[string]any{"certFile": "alpha.crt"}},
			},
		},
	})
	seedEntry(c, "beta", Entry{
		Namespace: "beta",
		LastGood:  now,
		Doc: map[string]any{
			"http": map[string]any{
				"routers": map[string]any{
					"beta-web": map[string]any{"rule": "Host(`beta`)"},
				},
			},
			"tls": map[string]any{
				"certificates": []any{map[string]any{"certFile": "beta.crt"}},
			},
		},
	})

	s := newTestServer(t, c)
	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v\nbody=%s", err, w.Body.String())
	}

	routers := got["http"].(map[string]any)["routers"].(map[string]any)
	if _, ok := routers["alpha-web"]; !ok {
		t.Errorf("missing alpha-web in merged routers: %#v", routers)
	}
	if _, ok := routers["beta-web"]; !ok {
		t.Errorf("missing beta-web in merged routers: %#v", routers)
	}

	certs := got["tls"].(map[string]any)["certificates"].([]any)
	if len(certs) != 2 {
		t.Fatalf("certs len = %d, want 2 (concatenation): %#v", len(certs), certs)
	}
}

func TestServer_Config_SkipsNeverSucceeded(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })

	// One healthy entry, one that has only ever failed.
	seedEntry(c, "good", Entry{
		Namespace: "good",
		LastGood:  now,
		Doc: map[string]any{
			"http": map[string]any{"routers": map[string]any{"g": map[string]any{}}},
		},
	})
	seedEntry(c, "broken", Entry{
		Namespace: "broken",
		LastErr:   "dial tcp: connection refused",
	})

	s := newTestServer(t, c)
	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	routers := got["http"].(map[string]any)["routers"].(map[string]any)
	if _, ok := routers["g"]; !ok {
		t.Fatalf("merged config missing healthy router: %#v", got)
	}
	if len(routers) != 1 {
		t.Fatalf("merged config has unexpected routers: %#v", routers)
	}
}

func TestServer_Config_CollisionSmallerNamespaceWins(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })

	// Two entries declare the same router. "alpha" is lexicographically
	// smaller than "beta", so its rule must win.
	seedEntry(c, "alpha", Entry{
		Namespace: "alpha", LastGood: now,
		Doc: map[string]any{
			"http": map[string]any{
				"routers": map[string]any{
					"shared": map[string]any{"rule": "Host(`from-alpha`)"},
				},
			},
		},
	})
	seedEntry(c, "beta", Entry{
		Namespace: "beta", LastGood: now,
		Doc: map[string]any{
			"http": map[string]any{
				"routers": map[string]any{
					"shared": map[string]any{"rule": "Host(`from-beta`)"},
				},
			},
		},
	})

	s := newTestServer(t, c)
	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	rule := got["http"].(map[string]any)["routers"].(map[string]any)["shared"].(map[string]any)["rule"]
	if rule != "Host(`from-alpha`)" {
		t.Fatalf("rule = %q, want alpha to win", rule)
	}

	// And the collision should be recorded for /debug.
	cols := s.snapshotCollisions()
	if len(cols) != 1 {
		t.Fatalf("collisions = %#v, want exactly one", cols)
	}
	if cols[0].Path != "http.routers.shared.rule" {
		t.Errorf("collision path = %q, want http.routers.shared.rule", cols[0].Path)
	}
	if cols[0].Namespace != "beta" {
		t.Errorf("losing namespace = %q, want beta", cols[0].Namespace)
	}
	if cols[0].Count != 1 {
		t.Errorf("count = %d, want 1", cols[0].Count)
	}

	// Hitting /config again should accumulate (cumulative counters).
	w2 := httptest.NewRecorder()
	s.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/config", nil))
	cols = s.snapshotCollisions()
	if cols[0].Count != 2 {
		t.Fatalf("count after second request = %d, want 2 (cumulative)", cols[0].Count)
	}
}

func TestServer_Debug_Shape(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })

	seedEntry(c, "alpha", Entry{
		Namespace: "alpha",
		LastGood:  now,
		Doc:       map[string]any{"http": map[string]any{}},
	})
	seedEntry(c, "broken", Entry{
		Namespace: "broken",
		Staleness: 90 * time.Second,
		LastErr:   "boom",
		LastGood:  now.Add(-90 * time.Second),
		Doc:       map[string]any{},
	})

	s := newTestServer(t, c)
	req := httptest.NewRequest(http.MethodGet, "/debug", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var got debugPayload
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode debug payload: %v\nbody=%s", err, w.Body.String())
	}

	wantIDs := []string{"alpha", "broken"}
	gotIDs := make([]string, len(got.Upstreams))
	for i, u := range got.Upstreams {
		gotIDs[i] = u.ID
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("upstream IDs = %v, want %v (sorted)", gotIDs, wantIDs)
	}

	for _, u := range got.Upstreams {
		switch u.ID {
		case "alpha":
			if u.Namespace != "alpha" {
				t.Errorf("alpha namespace = %q", u.Namespace)
			}
			if u.LastErr != "" {
				t.Errorf("alpha LastErr = %q, want empty", u.LastErr)
			}
			if u.LastGood == "" {
				t.Errorf("alpha LastGood empty; want RFC3339 stamp")
			}
		case "broken":
			if u.LastErr != "boom" {
				t.Errorf("broken LastErr = %q, want boom", u.LastErr)
			}
			if u.Staleness != (90 * time.Second).String() {
				t.Errorf("broken Staleness = %q, want %v", u.Staleness, 90*time.Second)
			}
		}
	}
}

func TestServer_Debug_ExposesCollisions(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })
	seedEntry(c, "alpha", Entry{Namespace: "alpha", LastGood: now,
		Doc: map[string]any{"k": "from-alpha"},
	})
	seedEntry(c, "beta", Entry{Namespace: "beta", LastGood: now,
		Doc: map[string]any{"k": "from-beta"},
	})

	s := newTestServer(t, c)
	// Trigger a collision by serving /config.
	s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/config", nil))

	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug", nil))

	var got debugPayload
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Collisions) != 1 {
		t.Fatalf("collisions = %#v, want one", got.Collisions)
	}
	if got.Collisions[0].Path != "k" || got.Collisions[0].Namespace != "beta" || got.Collisions[0].Count != 1 {
		t.Fatalf("collision row = %#v, want path=k namespace=beta count=1", got.Collisions[0])
	}
}

func TestServer_Config_CertResolverInjection(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })

	seedEntry(c, "app", Entry{
		Namespace: "app", LastGood: now,
		Doc: map[string]any{
			"http": map[string]any{
				"routers": map[string]any{
					// Wildcard router: declares its own tls block (with
					// domains) and so loses the entrypoint default resolver.
					"catchall": map[string]any{
						"rule": "HostRegexp(`[^.]+\\.example\\.com`)",
						"tls": map[string]any{
							"domains": []any{map[string]any{
								"main": "example.com",
								"sans": []any{"*.example.com"},
							}},
						},
					},
					// Plain router: no tls block, inherits the entrypoint
					// default on the issuer; must be left untouched.
					"plain": map[string]any{"rule": "Host(`plain.example.com`)"},
					// Router that named its own resolver; never overwrite it.
					"opinionated": map[string]any{
						"rule": "Host(`opin.example.com`)",
						"tls":  map[string]any{"certResolver": "other"},
					},
				},
			},
		},
	})
	s := newTestServer(t, c)

	routersFor := func(query string) map[string]any {
		t.Helper()
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/config"+query, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		var got map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v\nbody=%s", err, w.Body.String())
		}
		return got["http"].(map[string]any)["routers"].(map[string]any)
	}

	// Edge view (no param): document untouched. The catchall keeps its
	// tls.domains but gains no resolver — exactly what the resolver-less
	// gateway needs to keep the router enabled.
	edge := routersFor("")
	if tls, ok := edge["catchall"].(map[string]any)["tls"].(map[string]any); !ok {
		t.Fatal("edge: catchall lost its tls block")
	} else if _, set := tls["certResolver"]; set {
		t.Errorf("edge: catchall.tls.certResolver = %v, want absent", tls["certResolver"])
	}

	// Issuer view (?certresolver=dns): the tls-bearing router gains the
	// resolver; the plain router stays bare; the opinionated router keeps
	// its own.
	iss := routersFor("?certresolver=dns")
	if cr := iss["catchall"].(map[string]any)["tls"].(map[string]any)["certResolver"]; cr != "dns" {
		t.Errorf("issuer: catchall.tls.certResolver = %v, want dns", cr)
	}
	if _, hasTLS := iss["plain"].(map[string]any)["tls"]; hasTLS {
		t.Errorf("issuer: plain router gained a tls block, want none: %#v", iss["plain"])
	}
	if cr := iss["opinionated"].(map[string]any)["tls"].(map[string]any)["certResolver"]; cr != "other" {
		t.Errorf("issuer: opinionated.tls.certResolver = %v, want preserved 'other'", cr)
	}
}

func TestInjectCertResolver_Tolerates(t *testing.T) {
	// Must not panic or mangle on documents that don't match the
	// expected http.routers shape, or on the bare `tls: true` form.
	cases := []map[string]any{
		{},
		{"http": "not-a-map"},
		{"http": map[string]any{"routers": "not-a-map"}},
		{"http": map[string]any{"routers": map[string]any{"r": "not-a-map"}}},
		{"http": map[string]any{"routers": map[string]any{
			"r": map[string]any{"tls": true}, // bare tls:true, not an object
		}}},
	}
	for _, doc := range cases {
		injectCertResolver(doc, "dns") // just assert no panic
	}

	// The bare `tls: true` form is left as-is (can't set a key on a bool).
	doc := map[string]any{"http": map[string]any{"routers": map[string]any{
		"r": map[string]any{"tls": true},
	}}}
	injectCertResolver(doc, "dns")
	if tls := doc["http"].(map[string]any)["routers"].(map[string]any)["r"].(map[string]any)["tls"]; tls != true {
		t.Errorf("tls:true was modified to %#v, want left as true", tls)
	}
}

func TestServer_MethodNotAllowed(t *testing.T) {
	s := newTestServer(t, NewCache(nil))
	for _, path := range []string{"/config", "/healthz", "/debug"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(""))
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s: status = %d, want 405", path, w.Code)
		}
		if allow := w.Header().Get("Allow"); allow == "" {
			t.Errorf("POST %s: missing Allow header", path)
		}
	}
}
