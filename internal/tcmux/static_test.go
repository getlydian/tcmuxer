package tcmux

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func writeTempYAML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "upstreams.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return p
}

func TestLoadStatic_RoundTrip(t *testing.T) {
	path := writeTempYAML(t, `
upstreams:
  - name: theater
    url: http://nginx/_internal/traefik-config/v3
    interval: 30s
    timeout: 5s
  - name: another-app
    url: http://other/config
  - name: explicit-ns
    namespace: shared
    url: http://shared/config
    interval: 1m
`)

	got, err := loadStatic(path)
	if err != nil {
		t.Fatalf("loadStatic: %v", err)
	}
	want := []Upstream{
		{ID: "theater", Namespace: "theater", URL: "http://nginx/_internal/traefik-config/v3", Interval: 30 * time.Second, Timeout: 5 * time.Second},
		{ID: "another-app", Namespace: "another-app", URL: "http://other/config", Interval: DefaultInterval, Timeout: DefaultTimeout},
		{ID: "explicit-ns", Namespace: "shared", URL: "http://shared/config", Interval: time.Minute, Timeout: DefaultTimeout},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loadStatic mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestLoadStatic_Errors(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing name", "upstreams:\n  - url: http://x\n"},
		{"missing url", "upstreams:\n  - name: x\n"},
		{"duplicate name", "upstreams:\n  - name: x\n    url: http://a\n  - name: x\n    url: http://b\n"},
		{"bad yaml", "upstreams: [this is: not valid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempYAML(t, tc.body)
			if _, err := loadStatic(path); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestLoadStatic_FileMissing(t *testing.T) {
	if _, err := loadStatic(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestStaticBackend_EmitsOnceAndReloads(t *testing.T) {
	path := writeTempYAML(t, `
upstreams:
  - name: a
    url: http://a/config
`)

	reload := make(chan struct{}, 1)
	b := &StaticBackend{Path: path, Reload: reload, Log: discardLog()}

	out := make(chan []Upstream, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- b.Run(ctx, out) }()

	// First emission: initial load.
	select {
	case got := <-out:
		if len(got) != 1 || got[0].ID != "a" {
			t.Fatalf("initial emission = %#v, want one upstream named a", got)
		}
	case <-time.After(time.Second):
		t.Fatal("backend never emitted initial list")
	}

	// Rewrite the file, then signal reload.
	if err := os.WriteFile(path, []byte("upstreams:\n  - name: a\n    url: http://a/config\n  - name: b\n    url: http://b/config\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reload <- struct{}{}

	select {
	case got := <-out:
		if len(got) != 2 {
			t.Fatalf("post-reload emission = %#v, want two upstreams", got)
		}
	case <-time.After(time.Second):
		t.Fatal("backend never emitted reloaded list")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error after clean cancel: %v", err)
	}
}

func TestStaticBackend_ReloadFailureKeepsServing(t *testing.T) {
	path := writeTempYAML(t, `
upstreams:
  - name: a
    url: http://a/config
`)
	reload := make(chan struct{}, 1)
	b := &StaticBackend{Path: path, Reload: reload, Log: discardLog()}

	out := make(chan []Upstream, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- b.Run(ctx, out) }()

	<-out // initial

	// Corrupt the file and signal reload. Backend should NOT crash and
	// should NOT emit a new list.
	if err := os.WriteFile(path, []byte("not: [valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	reload <- struct{}{}

	select {
	case got := <-out:
		t.Fatalf("backend emitted on reload failure: %#v", got)
	case <-time.After(50 * time.Millisecond):
		// Good — no emission.
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestStaticBackend_InitialLoadErrorReturned(t *testing.T) {
	b := &StaticBackend{Path: filepath.Join(t.TempDir(), "missing.yaml"), Log: discardLog()}
	out := make(chan []Upstream, 1)
	err := b.Run(context.Background(), out)
	if err == nil {
		t.Fatal("expected initial load error to be returned")
	}
}
