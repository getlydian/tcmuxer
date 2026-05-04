package tcmux

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Upstream describes one config endpoint tcmuxer polls.
type Upstream struct {
	ID        string
	Namespace string
	URL       string
	Interval  time.Duration
	Timeout   time.Duration
}

// Entry is one upstream's last-known-good cache slot. A zero LastGood
// time means "never succeeded"; in that case Doc is nil and the
// upstream contributes nothing to the merged output.
//
// Namespace is the merge namespace the upstream was registered under —
// stored on the Entry so /config can iterate the cache snapshot in
// namespace order without a second registry lookup.
type Entry struct {
	Namespace string
	Doc       map[string]any
	LastGood  time.Time
	Staleness time.Duration
	LastErr   string
}

// Cache holds one Entry per upstream ID. Safe for concurrent use.
type Cache struct {
	mu  sync.RWMutex
	now func() time.Time
	m   map[string]*Entry
}

// NewCache returns an empty cache. now defaults to time.Now if nil; tests
// inject a fake clock to make staleness assertions deterministic.
func NewCache(now func() time.Time) *Cache {
	if now == nil {
		now = time.Now
	}
	return &Cache{now: now, m: make(map[string]*Entry)}
}

// Set records a successful poll: stores the parsed document, stamps
// LastGood, clears Staleness/LastErr, and remembers the namespace this
// upstream merges under. Pass an empty namespace to leave the existing
// value unchanged (older tests that don't care about namespace use this).
func (c *Cache) Set(id, namespace string, doc map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok {
		e = &Entry{}
		c.m[id] = e
	}
	if namespace != "" {
		e.Namespace = namespace
	}
	e.Doc = doc
	e.LastGood = c.now()
	e.Staleness = 0
	e.LastErr = ""
}

// Fail records a failed poll. The previous Doc and LastGood are kept;
// Staleness becomes the elapsed time since LastGood (zero if never
// succeeded), and LastErr captures the error string.
func (c *Cache) Fail(id string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok {
		e = &Entry{}
		c.m[id] = e
	}
	if !e.LastGood.IsZero() {
		e.Staleness = c.now().Sub(e.LastGood)
	}
	if err != nil {
		e.LastErr = err.Error()
	} else {
		e.LastErr = ""
	}
}

// Snapshot returns a deep-enough copy of the cache for read-only use:
// the outer map and Entry values are fresh, but Doc is shared by
// reference (callers must treat it as read-only, which the merger does).
func (c *Cache) Snapshot() map[string]Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]Entry, len(c.m))
	for id, e := range c.m {
		out[id] = *e
	}
	return out
}

// Drop removes an upstream from the cache. Used when discovery reports
// the upstream is gone, or when the max-staleness window is exceeded.
func (c *Cache) Drop(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, id)
}

// Poller owns one upstream and pushes its responses into a Cache on a
// fixed interval. One Poller per upstream, one goroutine per Poller.
type Poller struct {
	Up Upstream
}

// Run blocks until ctx is cancelled. It performs an immediate first
// poll, then ticks at Up.Interval. Each poll uses a per-poll context
// bounded by Up.Timeout so a slow upstream cannot wedge the loop.
func (p *Poller) Run(ctx context.Context, client *http.Client, cache *Cache, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if client == nil {
		client = http.DefaultClient
	}
	p.pollOnce(ctx, client, cache, log)

	t := time.NewTicker(p.Up.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.pollOnce(ctx, client, cache, log)
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context, client *http.Client, cache *Cache, log *slog.Logger) {
	pctx, cancel := context.WithTimeout(ctx, p.Up.Timeout)
	defer cancel()

	doc, err := fetchJSON(pctx, client, p.Up.URL)
	if err != nil {
		// ctx-cancel during shutdown is not an upstream failure.
		if ctx.Err() != nil {
			return
		}
		cache.Fail(p.Up.ID, err)
		log.Warn("upstream poll failed",
			"id", p.Up.ID, "namespace", p.Up.Namespace, "url", p.Up.URL, "err", err)
		return
	}
	cache.Set(p.Up.ID, p.Up.Namespace, doc)
	log.Debug("upstream poll ok",
		"id", p.Up.ID, "namespace", p.Up.Namespace, "url", p.Up.URL)
}

func fetchJSON(ctx context.Context, client *http.Client, url string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		// Drain a bit so the connection can be reused.
		_, _ = io.CopyN(io.Discard, resp.Body, 1<<14)
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}
	if doc == nil {
		// Top-level `null` decodes without error but is not useful.
		return nil, fmt.Errorf("decode json: top-level null")
	}
	return doc, nil
}
