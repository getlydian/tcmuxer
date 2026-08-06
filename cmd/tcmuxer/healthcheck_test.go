package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsHealthcheck(t *testing.T) {
	cases := []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"-healthcheck"}, true},
		{[]string{"--healthcheck"}, true},
		{[]string{"-healthcheck=true"}, true},
		{[]string{"-healthcheck=false"}, false},
		{[]string{"-backend", "swarm"}, false},
		{[]string{"-backend", "swarm", "-healthcheck"}, true},
		// After `--`, everything is a positional operand and must not be
		// read as the flag.
		{[]string{"--", "-healthcheck"}, false},
	}
	for _, tc := range cases {
		if got := isHealthcheck(tc.args); got != tc.want {
			t.Errorf("isHealthcheck(%q) = %v, want %v", tc.args, got, tc.want)
		}
	}
}

// TestProbeHost checks that wildcard listen addresses become dialable
// loopback destinations. ":80" is the default listen value, so getting
// this wrong would break the healthcheck in the default deployment.
func TestProbeHost(t *testing.T) {
	cases := map[string]string{
		":80":            "127.0.0.1:80",
		"0.0.0.0:80":     "127.0.0.1:80",
		"[::]:80":        "127.0.0.1:80",
		"80":             "127.0.0.1:80",
		"127.0.0.1:8080": "127.0.0.1:8080",
		"tcmuxer:80":     "tcmuxer:80",
	}
	for in, want := range cases {
		if got := probeHost(in); got != want {
			t.Errorf("probeHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRunHealthcheck_ReadyExitsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("path = %q, want /healthz", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok","reason":"serving config from 2/2 upstreams"}`)
	}))
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	err := runHealthcheck(context.Background(),
		[]string{"-healthcheck", "-healthcheck-addr", strings.TrimPrefix(srv.URL, "http://")},
		map[string]string{}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runHealthcheck returned %v, want nil", err)
	}
	if !strings.Contains(stdout.String(), "serving config") {
		t.Fatalf("stdout = %q, want the server's reason echoed", stdout.String())
	}
}

// TestRunHealthcheck_NotReadyExitsNonZero is the behaviour Swarm acts
// on: a non-nil error becomes exit 1, which marks the task unhealthy and
// lets the orchestrator reschedule it automatically.
func TestRunHealthcheck_NotReadyExitsNonZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"status":"not ready","errors":["no such host"]}`)
	}))
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	err := runHealthcheck(context.Background(),
		[]string{"-healthcheck", "-healthcheck-addr", strings.TrimPrefix(srv.URL, "http://")},
		map[string]string{}, &stdout, &stderr)
	if err == nil {
		t.Fatal("runHealthcheck returned nil, want an error for a 503")
	}
	// The server's diagnosis must reach stderr so `docker inspect` shows
	// why the container is unhealthy.
	if !strings.Contains(stderr.String(), "no such host") {
		t.Fatalf("stderr = %q, want the server's error echoed", stderr.String())
	}
}

// TestRunHealthcheck_UnreachableExitsNonZero covers a wedged or
// not-yet-listening HTTP server.
func TestRunHealthcheck_UnreachableExitsNonZero(t *testing.T) {
	addr := freeAddr(t) // nothing is listening here

	var stdout, stderr bytes.Buffer
	err := runHealthcheck(context.Background(),
		[]string{"-healthcheck", "-healthcheck-addr", addr},
		map[string]string{"TCMUXER_HEALTHCHECK_TIMEOUT": "500ms"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("runHealthcheck returned nil, want an error when nothing is listening")
	}
}

// TestRunHealthcheck_DefaultsToListenAddr confirms the probe finds the
// server without extra configuration — the container sets TCMUXER_LISTEN
// and the healthcheck should just work.
func TestRunHealthcheck_DefaultsToListenAddr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	}))
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	err := runHealthcheck(context.Background(),
		[]string{"-healthcheck"},
		map[string]string{"TCMUXER_LISTEN": strings.TrimPrefix(srv.URL, "http://")},
		&stdout, &stderr)
	if err != nil {
		t.Fatalf("runHealthcheck returned %v, want nil", err)
	}
}

// TestRunHealthcheck_IgnoresServerFlags lets the healthcheck be written
// as the full server command plus -healthcheck, which is how compose
// healthchecks are usually copy-pasted.
func TestRunHealthcheck_IgnoresServerFlags(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	}))
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	err := runHealthcheck(context.Background(),
		[]string{"-backend", "swarm", "-healthcheck", "-healthcheck-addr", strings.TrimPrefix(srv.URL, "http://")},
		map[string]string{}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runHealthcheck returned %v, want nil", err)
	}
}

// TestRunDispatchesHealthcheck confirms run() routes to the probe before
// validating backend config — `tcmuxer -healthcheck` must not require
// TCMUXER_STATIC_FILE just to ask whether the server is up.
func TestRunDispatchesHealthcheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	}))
	defer srv.Close()

	err := run(context.Background(),
		[]string{"-healthcheck", "-healthcheck-addr", strings.TrimPrefix(srv.URL, "http://")},
		[]string{"TCMUXER_BACKEND=static"}, // would otherwise fail: no static file
		io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("run returned %v, want nil", err)
	}
}
