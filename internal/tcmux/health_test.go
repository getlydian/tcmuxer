package tcmux

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestHealthy_NoUpstreamsIsReady pins the idle case: an environment
// where nothing carries a tcmuxer.url label has nothing to mux and is
// legitimately idle. If this returned not-ready, a correctly-configured
// tcmuxer would restart forever.
func TestHealthy_NoUpstreamsIsReady(t *testing.T) {
	h := Healthy(map[string]Entry{})
	if !h.Ready {
		t.Fatalf("Ready = false, want true for an empty cache (reason %q)", h.Reason)
	}
	if h.Upstreams != 0 || h.Good != 0 {
		t.Fatalf("counts = %d/%d, want 0/0", h.Good, h.Upstreams)
	}
}

// TestHealthy_ColdStartIsNotReady covers the state this check exists to
// catch: upstreams were discovered, but the task's DNS view is broken so
// not one poll has ever succeeded. /config serves {}, Traefik installs
// no routers, and the orchestrator still sees a healthy process.
func TestHealthy_ColdStartIsNotReady(t *testing.T) {
	snap := map[string]Entry{
		"a": {Namespace: "app-a", LastErr: `lookup app-a on 127.0.0.11:53: no such host`},
		"b": {Namespace: "app-b", LastErr: `lookup app-a on 127.0.0.11:53: no such host`},
	}
	h := Healthy(snap)
	if h.Ready {
		t.Fatal("Ready = true, want false when no upstream has ever succeeded")
	}
	if h.Upstreams != 2 || h.Good != 0 {
		t.Fatalf("counts = good %d, upstreams %d; want 0 and 2", h.Good, h.Upstreams)
	}
	// The two upstreams share an error string; it should be deduplicated
	// so the healthcheck output stays readable with many upstreams.
	want := []string{`lookup app-a on 127.0.0.11:53: no such host`}
	if !reflect.DeepEqual(h.Errors, want) {
		t.Fatalf("Errors = %q, want %q", h.Errors, want)
	}
}

// TestHealthy_OneGoodUpstreamIsReady is the anti-restart-loop guarantee:
// a single sick app must never take tcmuxer down, because tcmuxer is
// still serving real routes for everyone else.
func TestHealthy_OneGoodUpstreamIsReady(t *testing.T) {
	now := time.Date(2026, 8, 6, 18, 54, 0, 0, time.UTC)
	snap := map[string]Entry{
		"good": {Namespace: "alpha", LastGood: now, Doc: map[string]any{}},
		"bad":  {Namespace: "beta", LastErr: "connection refused"},
	}
	h := Healthy(snap)
	if !h.Ready {
		t.Fatalf("Ready = false, want true when one upstream is good (reason %q)", h.Reason)
	}
	if h.Good != 1 || h.Upstreams != 2 {
		t.Fatalf("counts = good %d, upstreams %d; want 1 and 2", h.Good, h.Upstreams)
	}
}

// TestHealthy_StaleButServingIsReady covers "succeeded once, failing
// now". tcmuxer keeps serving last-known-good here by design, so it is
// degraded rather than unhealthy — restarting would discard the very
// config that is keeping sites up.
func TestHealthy_StaleButServingIsReady(t *testing.T) {
	now := time.Date(2026, 8, 6, 18, 54, 0, 0, time.UTC)
	snap := map[string]Entry{
		"a": {
			Namespace: "alpha",
			LastGood:  now,
			Doc:       map[string]any{},
			Staleness: 20 * time.Minute,
			LastErr:   "context deadline exceeded",
		},
	}
	h := Healthy(snap)
	if !h.Ready {
		t.Fatal("Ready = false, want true for a stale-but-serving upstream")
	}
	if h.Stale != 1 {
		t.Fatalf("Stale = %d, want 1", h.Stale)
	}
	// A stale entry still has last-known-good, so it is not a cold-start
	// error and must not appear in Errors.
	if len(h.Errors) != 0 {
		t.Fatalf("Errors = %q, want none for a stale-but-serving upstream", h.Errors)
	}
}

func TestServer_Healthz_ColdStartReturns503(t *testing.T) {
	c := NewCache(nil)
	c.Fail("a", errors.New("no such host"))

	s := newTestServer(t, c)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), "not ready") {
		t.Fatalf("body = %q, want to mention not ready", w.Body.String())
	}
}

func TestServer_Healthz_VerboseJSON(t *testing.T) {
	now := time.Date(2026, 8, 6, 18, 54, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })
	seedEntry(c, "a", Entry{Namespace: "alpha", LastGood: now, Doc: map[string]any{}})
	seedEntry(c, "b", Entry{Namespace: "beta", LastErr: "no such host"})

	s := newTestServer(t, c)
	req := httptest.NewRequest(http.MethodGet, "/healthz?verbose", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var got healthPayload
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body %q: %v", w.Body.String(), err)
	}
	if got.Status != "ok" {
		t.Fatalf("status field = %q, want ok", got.Status)
	}
	if got.Upstreams != 2 || got.Good != 1 {
		t.Fatalf("payload counts = good %d, upstreams %d; want 1 and 2", got.Good, got.Upstreams)
	}
	if got.OldestLastGood == "" {
		t.Fatal("oldestLastGood is empty, want the good entry's timestamp")
	}
}

// TestServer_Healthz_PlainTextStaysCompatible guards the existing probe
// contract: a healthy tcmuxer still answers a bare GET with "ok".
func TestServer_Healthz_PlainTextStaysCompatible(t *testing.T) {
	s := newTestServer(t, NewCache(nil))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if strings.TrimSpace(w.Body.String()) != "ok" {
		t.Fatalf("body = %q, want ok", w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
}

func TestServer_Healthz_AcceptJSONNegotiates(t *testing.T) {
	s := newTestServer(t, NewCache(nil))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}
