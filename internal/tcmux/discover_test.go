package tcmux

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeSpawner records spawn/cancel calls so tests can assert on the
// sequence the reconciler produced. It returns a real cancel func and
// done channel; cancelling the context closes done, so Apply's
// "wait for goroutine to exit" path runs the same way it does in prod.
type fakeSpawner struct {
	mu      sync.Mutex
	spawned []Upstream
	stopped []string
	live    map[string]chan struct{}
}

func newFakeSpawner() *fakeSpawner {
	return &fakeSpawner{live: make(map[string]chan struct{})}
}

func (f *fakeSpawner) Spawn(ctx context.Context, up Upstream) (context.CancelFunc, <-chan struct{}) {
	f.mu.Lock()
	f.spawned = append(f.spawned, up)
	done := make(chan struct{})
	f.live[up.ID] = done
	f.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-ctx.Done()
		close(done)
	}()
	wrapped := func() {
		f.mu.Lock()
		f.stopped = append(f.stopped, up.ID)
		delete(f.live, up.ID)
		f.mu.Unlock()
		cancel()
	}
	return wrapped, done
}

func (f *fakeSpawner) snapshot() ([]Upstream, []string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	spawned := append([]Upstream(nil), f.spawned...)
	stopped := append([]string(nil), f.stopped...)
	live := make([]string, 0, len(f.live))
	for id := range f.live {
		live = append(live, id)
	}
	sort.Strings(live)
	return spawned, stopped, live
}

func mkUp(id, url string) Upstream {
	return Upstream{
		ID:        id,
		Namespace: id,
		URL:       url,
		Interval:  time.Second,
		Timeout:   time.Second,
	}
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestReconciler_AddRemove(t *testing.T) {
	fs := newFakeSpawner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := NewReconciler(ctx, fs.Spawn, nil, discardLog())

	r.Apply([]Upstream{mkUp("a", "http://a"), mkUp("b", "http://b")})
	_, _, live := fs.snapshot()
	if !reflect.DeepEqual(live, []string{"a", "b"}) {
		t.Fatalf("after first apply, live = %v, want [a b]", live)
	}

	// Drop b, keep a.
	r.Apply([]Upstream{mkUp("a", "http://a")})
	_, stopped, live := fs.snapshot()
	if !reflect.DeepEqual(live, []string{"a"}) {
		t.Fatalf("after drop b, live = %v, want [a]", live)
	}
	if !reflect.DeepEqual(stopped, []string{"b"}) {
		t.Fatalf("stopped = %v, want [b]", stopped)
	}

	// Empty list = stop everything.
	r.Apply(nil)
	_, _, live = fs.snapshot()
	if len(live) != 0 {
		t.Fatalf("after empty apply, live = %v, want []", live)
	}
}

func TestReconciler_NoOpOnUnchanged(t *testing.T) {
	fs := newFakeSpawner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewReconciler(ctx, fs.Spawn, nil, discardLog())

	list := []Upstream{mkUp("a", "http://a"), mkUp("b", "http://b")}
	r.Apply(list)
	r.Apply(list)
	r.Apply(list)

	spawned, stopped, _ := fs.snapshot()
	if len(spawned) != 2 {
		t.Fatalf("spawned = %d, want 2 (no respawns on identical applies)", len(spawned))
	}
	if len(stopped) != 0 {
		t.Fatalf("stopped = %v, want none", stopped)
	}
}

func TestReconciler_UpdateRespawnsWhenUpstreamChanges(t *testing.T) {
	fs := newFakeSpawner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewReconciler(ctx, fs.Spawn, nil, discardLog())

	r.Apply([]Upstream{mkUp("a", "http://a")})

	// Same ID, different URL. Reconciler should cancel + respawn so
	// the new URL takes effect.
	updated := mkUp("a", "http://a-new")
	r.Apply([]Upstream{updated})

	spawned, stopped, live := fs.snapshot()
	if len(spawned) != 2 {
		t.Fatalf("spawned = %d, want 2 (initial + respawn)", len(spawned))
	}
	if spawned[1].URL != "http://a-new" {
		t.Fatalf("respawn URL = %q, want http://a-new", spawned[1].URL)
	}
	if !reflect.DeepEqual(stopped, []string{"a"}) {
		t.Fatalf("stopped = %v, want [a]", stopped)
	}
	if !reflect.DeepEqual(live, []string{"a"}) {
		t.Fatalf("live = %v, want [a]", live)
	}
}

func TestReconciler_DropsCacheEntryOnRemove(t *testing.T) {
	fs := newFakeSpawner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cache := NewCache(nil)
	cache.Set("a", "a", map[string]any{"v": 1})
	cache.Set("b", "b", map[string]any{"v": 2})

	r := NewReconciler(ctx, fs.Spawn, cache, discardLog())
	r.Apply([]Upstream{mkUp("a", "http://a"), mkUp("b", "http://b")})

	r.Apply([]Upstream{mkUp("a", "http://a")})

	snap := cache.Snapshot()
	if _, ok := snap["b"]; ok {
		t.Fatal("expected cache entry b to be dropped after upstream removed")
	}
	if _, ok := snap["a"]; !ok {
		t.Fatal("expected cache entry a to remain")
	}
}

func TestReconciler_StopAll(t *testing.T) {
	fs := newFakeSpawner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewReconciler(ctx, fs.Spawn, nil, discardLog())

	r.Apply([]Upstream{mkUp("a", "http://a"), mkUp("b", "http://b"), mkUp("c", "http://c")})
	r.StopAll()

	_, stopped, live := fs.snapshot()
	if len(live) != 0 {
		t.Fatalf("live after StopAll = %v, want empty", live)
	}
	sort.Strings(stopped)
	if !reflect.DeepEqual(stopped, []string{"a", "b", "c"}) {
		t.Fatalf("stopped = %v, want [a b c]", stopped)
	}
}

func TestDefaultSpawn_CancelStopsPoller(t *testing.T) {
	// Smoke-test the production SpawnFunc: a cancelled context should
	// close the done channel. We point the poller at a URL that 404s
	// fast so it doesn't hang on connection.
	cache := NewCache(nil)
	spawn := DefaultSpawn(nil, cache, discardLog())

	ctx, cancel := context.WithCancel(context.Background())
	stop, done := spawn(ctx, Upstream{
		ID:       "x",
		URL:      "http://127.0.0.1:1/",
		Interval: 10 * time.Millisecond,
		Timeout:  10 * time.Millisecond,
	})
	stop()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DefaultSpawn poller did not exit after cancel")
	}
}
