package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRunStaticEndToEnd spins up a fake upstream, points a static config
// at it, runs the binary's wiring, and asserts /config returns the
// upstream's document merged under its namespace.
func TestRunStaticEndToEnd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"http":{"routers":{"r1":{"rule":"Host(`+"`a`"+`)"}}}}`)
	}))
	defer upstream.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "static.yaml")
	yamlBody := fmt.Sprintf("upstreams:\n  - name: alpha\n    url: %s\n    interval: 50ms\n    timeout: 1s\n", upstream.URL)
	if err := os.WriteFile(configPath, []byte(yamlBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	listenAddr := freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := []string{
		"TCMUXER_LISTEN=" + listenAddr,
		"TCMUXER_BACKEND=static",
		"TCMUXER_STATIC_FILE=" + configPath,
		"TCMUXER_INTERVAL=50ms",
		"TCMUXER_TIMEOUT=1s",
		"TCMUXER_MAX_STALENESS=10m",
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- run(ctx, nil, env, io.Discard, io.Discard)
	}()

	// Wait for /config to report the merged document.
	deadline := time.Now().Add(3 * time.Second)
	var got map[string]any
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + listenAddr + "/config")
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var doc map[string]any
		if err := json.Unmarshal(body, &doc); err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if hasRouter(doc, "r1") {
			got = doc
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got == nil {
		t.Fatalf("did not see merged router from upstream within deadline")
	}

	// /healthz sanity check.
	resp, err := http.Get("http://" + listenAddr + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status=%d, want 200", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return within 5s of cancel")
	}
}

// TestRunRejectsUnknownBackend confirms early validation surfaces bad
// configuration instead of silently doing nothing.
func TestRunRejectsUnknownBackend(t *testing.T) {
	err := run(context.Background(), nil, []string{"TCMUXER_BACKEND=etcd"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected error for unknown backend, got nil")
	}
}

// TestRunRejectsStaticWithoutFile confirms backend=static demands a path.
func TestRunRejectsStaticWithoutFile(t *testing.T) {
	err := run(context.Background(), nil, []string{"TCMUXER_BACKEND=static"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected error when TCMUXER_STATIC_FILE missing, got nil")
	}
}

// hasRouter walks the merged doc looking for a router by key.
func hasRouter(doc map[string]any, key string) bool {
	httpv, ok := doc["http"].(map[string]any)
	if !ok {
		return false
	}
	routers, ok := httpv["routers"].(map[string]any)
	if !ok {
		return false
	}
	_, ok = routers[key]
	return ok
}

// freeAddr asks the kernel for an unused TCP port and returns the
// "127.0.0.1:N" string. The listener is closed immediately so the
// server under test can bind to it.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}
