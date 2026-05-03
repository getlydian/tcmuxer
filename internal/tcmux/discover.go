package tcmux

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
)

// Backend is a discovery source. It runs until ctx is cancelled and pushes
// the *current full set* of upstreams onto out whenever the set changes
// (or whenever it re-reads its source — emitting unchanged sets is fine,
// the reconciler is idempotent).
//
// Backends own the channel-send side; the reconciler owns the receive
// side. A backend that exits early (config error, etc.) returns the
// underlying error; a clean shutdown via ctx cancellation returns nil.
type Backend interface {
	Run(ctx context.Context, out chan<- []Upstream) error
}

// runningPoller is one entry in the reconciler's registry: the upstream
// it was started with, plus the cancel func that stops its goroutine.
type runningPoller struct {
	up     Upstream
	cancel context.CancelFunc
	done   <-chan struct{}
}

// SpawnFunc starts a poller goroutine for up and returns a cancel func
// and a channel that closes when the goroutine exits. The reconciler
// uses this seam so tests can record spawn/cancel calls without
// standing up real HTTP traffic.
type SpawnFunc func(ctx context.Context, up Upstream) (cancel context.CancelFunc, done <-chan struct{})

// DefaultSpawn returns a SpawnFunc that runs a real Poller against cache
// using client and log. main wires this up; tests substitute a fake.
func DefaultSpawn(client *http.Client, cache *Cache, log *slog.Logger) SpawnFunc {
	return func(parent context.Context, up Upstream) (context.CancelFunc, <-chan struct{}) {
		ctx, cancel := context.WithCancel(parent)
		done := make(chan struct{})
		p := &Poller{Up: up}
		go func() {
			defer close(done)
			p.Run(ctx, client, cache, log)
		}()
		return cancel, done
	}
}

// Reconciler keeps a registry of running pollers in sync with the latest
// upstream list a backend has reported. It is not safe for concurrent
// Apply calls; the discovery loop calls Apply serially.
type Reconciler struct {
	parent context.Context
	spawn  SpawnFunc
	cache  *Cache
	log    *slog.Logger

	mu      sync.Mutex
	current map[string]runningPoller
}

// NewReconciler builds a reconciler that spawns pollers under parent.
// cache may be nil if spawn does not need it (tests); log defaults to
// slog.Default. parent is the lifetime of the whole discovery loop —
// when it cancels, every running poller is torn down via StopAll.
func NewReconciler(parent context.Context, spawn SpawnFunc, cache *Cache, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{
		parent:  parent,
		spawn:   spawn,
		cache:   cache,
		log:     log,
		current: make(map[string]runningPoller),
	}
}

// Apply diffs the next list against the running set: spawns pollers for
// new IDs, cancels pollers for removed IDs (and drops their cache
// entry), and for IDs whose Upstream changed, cancels-then-respawns so
// the new interval/URL/etc. take effect. Apply blocks until cancelled
// pollers have exited so the registry never lies about what's running.
func (r *Reconciler) Apply(next []Upstream) {
	r.mu.Lock()
	defer r.mu.Unlock()

	wanted := make(map[string]Upstream, len(next))
	for _, up := range next {
		wanted[up.ID] = up
	}

	// Remove or restart entries that disappeared or changed.
	for id, running := range r.current {
		w, ok := wanted[id]
		if !ok {
			r.log.Info("upstream removed", "id", id, "namespace", running.up.Namespace)
			running.cancel()
			<-running.done
			if r.cache != nil {
				r.cache.Drop(id)
			}
			delete(r.current, id)
			continue
		}
		if w != running.up {
			r.log.Info("upstream changed", "id", id, "namespace", w.Namespace)
			running.cancel()
			<-running.done
			cancel, done := r.spawn(r.parent, w)
			r.current[id] = runningPoller{up: w, cancel: cancel, done: done}
		}
	}

	// Add brand-new entries.
	for id, up := range wanted {
		if _, ok := r.current[id]; ok {
			continue
		}
		r.log.Info("upstream added", "id", id, "namespace", up.Namespace)
		cancel, done := r.spawn(r.parent, up)
		r.current[id] = runningPoller{up: up, cancel: cancel, done: done}
	}
}

// StopAll cancels every running poller and waits for them to exit.
// Used during shutdown after the discovery loop returns.
func (r *Reconciler) StopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, running := range r.current {
		running.cancel()
		<-running.done
		delete(r.current, id)
	}
}

// Running returns the IDs currently registered. For tests and /debug.
func (r *Reconciler) Running() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.current))
	for id := range r.current {
		ids = append(ids, id)
	}
	return ids
}
