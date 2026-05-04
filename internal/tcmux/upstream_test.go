package tcmux

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCache_SetSnapshot(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })

	doc := map[string]any{"http": map[string]any{}}
	c.Set("a", "a", doc)

	snap := c.Snapshot()
	got, ok := snap["a"]
	if !ok {
		t.Fatal("entry a missing from snapshot")
	}
	if !reflect.DeepEqual(got.Doc, doc) {
		t.Fatalf("doc = %#v, want %#v", got.Doc, doc)
	}
	if !got.LastGood.Equal(now) {
		t.Fatalf("LastGood = %v, want %v", got.LastGood, now)
	}
	if got.Staleness != 0 {
		t.Fatalf("Staleness = %v, want 0", got.Staleness)
	}
	if got.LastErr != "" {
		t.Fatalf("LastErr = %q, want empty", got.LastErr)
	}
}

func TestCache_FailThenSet_StalenessReset(t *testing.T) {
	var now time.Time
	c := NewCache(func() time.Time { return now })

	now = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c.Set("a", "a", map[string]any{"v": 1})

	now = now.Add(2 * time.Minute)
	c.Fail("a", errors.New("boom"))

	snap := c.Snapshot()["a"]
	if snap.Staleness != 2*time.Minute {
		t.Fatalf("Staleness after fail = %v, want 2m", snap.Staleness)
	}
	if snap.LastErr != "boom" {
		t.Fatalf("LastErr = %q, want boom", snap.LastErr)
	}
	// Doc and LastGood preserved across a failure.
	if v := snap.Doc["v"]; v != 1 {
		t.Fatalf("Doc not preserved on Fail: %#v", snap.Doc)
	}

	now = now.Add(time.Minute)
	c.Set("a", "a", map[string]any{"v": 2})

	snap = c.Snapshot()["a"]
	if snap.Staleness != 0 {
		t.Fatalf("Staleness after recovery = %v, want 0", snap.Staleness)
	}
	if snap.LastErr != "" {
		t.Fatalf("LastErr after recovery = %q, want empty", snap.LastErr)
	}
}

func TestCache_FailWithoutPriorSuccess(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	c := NewCache(func() time.Time { return now })

	c.Fail("a", errors.New("first"))
	snap := c.Snapshot()["a"]
	// No LastGood yet, so Staleness stays zero (the cache reports
	// elapsed-since-LastGood, not elapsed-since-creation).
	if snap.Staleness != 0 {
		t.Fatalf("Staleness with no prior success = %v, want 0", snap.Staleness)
	}
	if !snap.LastGood.IsZero() {
		t.Fatalf("LastGood = %v, want zero", snap.LastGood)
	}
	if snap.LastErr != "first" {
		t.Fatalf("LastErr = %q, want first", snap.LastErr)
	}
	if snap.Doc != nil {
		t.Fatalf("Doc = %#v, want nil", snap.Doc)
	}
}

func TestCache_Drop(t *testing.T) {
	c := NewCache(nil)
	c.Set("a", "a", map[string]any{})
	c.Set("b", "b", map[string]any{})
	c.Drop("a")

	snap := c.Snapshot()
	if _, ok := snap["a"]; ok {
		t.Fatal("expected a to be dropped")
	}
	if _, ok := snap["b"]; !ok {
		t.Fatal("expected b to remain")
	}
}

func TestCache_Concurrent(t *testing.T) {
	c := NewCache(nil)
	var wg sync.WaitGroup
	const N = 50
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			c.Set("k", "k", map[string]any{"i": i})
		}(i)
		go func() {
			defer wg.Done()
			_ = c.Snapshot()
		}()
	}
	wg.Wait()

	if _, ok := c.Snapshot()["k"]; !ok {
		t.Fatal("expected k to be present after concurrent writes")
	}
}

func TestCache_SnapshotIsCopy(t *testing.T) {
	c := NewCache(nil)
	c.Set("a", "a", map[string]any{"v": 1})
	snap := c.Snapshot()
	// Mutating the snapshot's outer map must not affect the cache.
	delete(snap, "a")
	if _, ok := c.Snapshot()["a"]; !ok {
		t.Fatal("mutating snapshot affected cache")
	}
}

// scriptedHandler returns a sequence of canned responses, one per
// request, so a single Poller can step through success → failure →
// garbage transitions deterministically.
type scriptedResp struct {
	status int
	body   string
}

type scriptedHandler struct {
	mu    sync.Mutex
	steps []scriptedResp
	hits  int32
	done  chan struct{}
	want  int
}

func (h *scriptedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	i := int(atomic.AddInt32(&h.hits, 1)) - 1
	if i >= len(h.steps) {
		i = len(h.steps) - 1
	}
	step := h.steps[i]
	hits := int(atomic.LoadInt32(&h.hits))
	h.mu.Unlock()

	w.WriteHeader(step.status)
	_, _ = io.WriteString(w, step.body)

	if h.done != nil && hits == h.want {
		select {
		case <-h.done:
		default:
			close(h.done)
		}
	}
}

func TestPoller_SuccessFailureGarbage(t *testing.T) {
	h := &scriptedHandler{
		steps: []scriptedResp{
			{200, `{"http":{"routers":{"a":{"rule":"Host(` + "`a`" + `)"}}}}`},
			{500, `oops`},
			{200, `not-json`},
		},
		done: make(chan struct{}),
		want: 3,
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	clk := &fakeClock{t: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)}
	cache := NewCache(clk.NowAdvancing(time.Second))

	p := &Poller{Up: Upstream{
		ID:        "u1",
		Namespace: "ns",
		URL:       srv.URL,
		Interval:  5 * time.Millisecond,
		Timeout:   time.Second,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pollerDone := make(chan struct{})
	go func() {
		p.Run(ctx, srv.Client(), cache, slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(pollerDone)
	}()

	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never received expected number of requests")
	}
	cancel()
	<-pollerDone

	snap := cache.Snapshot()["u1"]
	if snap.Doc == nil {
		t.Fatal("Doc dropped after subsequent failures; expected last-known-good preserved")
	}
	routers, _ := snap.Doc["http"].(map[string]any)["routers"].(map[string]any)
	if _, ok := routers["a"]; !ok {
		t.Fatalf("expected last-good doc with router a, got %#v", snap.Doc)
	}
	if snap.LastErr == "" {
		t.Fatal("LastErr empty after a 500 + garbage; expected an error string")
	}
	if snap.Staleness <= 0 {
		t.Fatalf("Staleness = %v, want > 0 after failures following success", snap.Staleness)
	}
}

func TestPoller_StalenessGrowsAcrossFailures(t *testing.T) {
	// First request succeeds, every subsequent one fails. Staleness on
	// the cache entry should reflect time elapsed since the success.
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			w.WriteHeader(200)
			_, _ = io.WriteString(w, `{"ok":true}`)
			return
		}
		w.WriteHeader(503)
	}))
	defer srv.Close()

	clk := &fakeClock{t: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)}
	// Each call to Now advances the clock by one minute. Set always
	// reads Now once; Fail reads it once when LastGood is non-zero.
	cache := NewCache(clk.NowAdvancing(time.Minute))

	p := &Poller{Up: Upstream{
		ID:       "u1",
		URL:      srv.URL,
		Interval: 2 * time.Millisecond,
		Timeout:  time.Second,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		p.Run(ctx, srv.Client(), cache, slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&hits) >= 4 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	snap := cache.Snapshot()["u1"]
	if snap.Staleness < time.Minute {
		t.Fatalf("Staleness = %v, want at least 1m after several failed polls", snap.Staleness)
	}
	if snap.LastErr == "" {
		t.Fatal("expected LastErr after failures")
	}
}

func TestPoller_StopsOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	cache := NewCache(nil)
	p := &Poller{Up: Upstream{
		ID:       "u1",
		URL:      srv.URL,
		Interval: 5 * time.Millisecond,
		Timeout:  time.Second,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx, srv.Client(), cache, slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Poller.Run did not exit after context cancel")
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// NowAdvancing returns a Now func that advances the clock by d on every
// call. Useful for asserting that staleness grows across many polls
// without relying on real time.
func (c *fakeClock) NowAdvancing(d time.Duration) func() time.Time {
	return func() time.Time {
		c.mu.Lock()
		defer c.mu.Unlock()
		t := c.t
		c.t = c.t.Add(d)
		return t
	}
}
