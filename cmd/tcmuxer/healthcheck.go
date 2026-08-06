package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// isHealthcheck reports whether args ask for the probe mode. It is a
// pre-scan rather than a flag on the main FlagSet because the probe must
// not inherit the server's config validation — `tcmuxer -healthcheck`
// has to work in a container whose TCMUXER_BACKEND/TCMUXER_STATIC_FILE
// would otherwise be required. Scanning stops at `--` so a literal
// argument after the terminator is never mistaken for the flag.
func isHealthcheck(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "-healthcheck" || a == "--healthcheck" ||
			strings.HasPrefix(a, "-healthcheck=") || strings.HasPrefix(a, "--healthcheck=") {
			// `-healthcheck=false` explicitly opts out.
			if _, v, ok := strings.Cut(a, "="); ok && (v == "false" || v == "0") {
				return false
			}
			return true
		}
	}
	return false
}

// runHealthcheck probes a running tcmuxer's /healthz and maps the result
// to a process exit status: nil (exit 0) when ready, an error (exit 1)
// otherwise. This is what a distroless `healthcheck:` invokes.
//
// The probe deliberately talks to the HTTP endpoint rather than
// inspecting shared state, because it runs as a *separate process* from
// the server — Docker execs it into the running container. Reaching the
// listener is itself part of what we are asserting: a tcmuxer whose HTTP
// server is wedged is just as broken as one with no config, and this
// catches both.
func runHealthcheck(ctx context.Context, args []string, env map[string]string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("tcmuxer -healthcheck", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		healthcheck bool
		addr        = fs.String("healthcheck-addr", envOr(env, "TCMUXER_HEALTHCHECK_ADDR", ""),
			"address to probe; defaults to -listen (TCMUXER_HEALTHCHECK_ADDR)")
		timeout = fs.Duration("healthcheck-timeout", 3*time.Second,
			"probe timeout (TCMUXER_HEALTHCHECK_TIMEOUT)")
		listen = fs.String("listen", envOr(env, "TCMUXER_LISTEN", ":80"),
			"address the server listens on (TCMUXER_LISTEN)")
	)
	fs.BoolVar(&healthcheck, "healthcheck", false, "probe a running tcmuxer and exit 0/1")

	// The server's flags are also accepted (and ignored) so that a
	// healthcheck command can be written as the full CMD plus
	// -healthcheck without the flag package rejecting the extras.
	var ignored config
	fs.StringVar(&ignored.backend, "backend", "", "ignored in healthcheck mode")
	fs.StringVar(&ignored.staticFile, "static-file", "", "ignored in healthcheck mode")
	fs.DurationVar(&ignored.interval, "interval", 0, "ignored in healthcheck mode")
	fs.DurationVar(&ignored.timeout, "timeout", 0, "ignored in healthcheck mode")
	fs.DurationVar(&ignored.maxStaleness, "max-staleness", 0, "ignored in healthcheck mode")
	fs.DurationVar(&ignored.reconcile, "reconcile", 0, "ignored in healthcheck mode")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if d, err := envDuration(env, "TCMUXER_HEALTHCHECK_TIMEOUT", *timeout); err != nil {
		return err
	} else if !isFlagSet(fs, "healthcheck-timeout") {
		*timeout = d
	}

	target := *addr
	if target == "" {
		target = *listen
	}

	url := "http://" + probeHost(target) + "/healthz?verbose"

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "tcmuxer-healthcheck/"+version)

	resp, err := (&http.Client{Timeout: *timeout}).Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	trimmed := strings.TrimSpace(string(body))

	if resp.StatusCode != http.StatusOK {
		// Print the server's own diagnosis; `docker inspect` surfaces the
		// last healthcheck output, which is where an operator will look
		// first. That turns a bare "unhealthy" into "unhealthy because
		// <upstream host>: no such host".
		if trimmed != "" {
			_, _ = fmt.Fprintln(stderr, trimmed)
		}
		return fmt.Errorf("healthcheck: http %d", resp.StatusCode)
	}

	if trimmed != "" {
		_, _ = fmt.Fprintln(stdout, trimmed)
	}
	return nil
}

// probeHost turns a listen address into something dialable. A server
// bound to ":80" or "0.0.0.0:80" listens on every interface, but those
// are not valid *destination* addresses, so the probe targets loopback.
// A bare port ("80") is accepted for convenience.
func probeHost(addr string) string {
	if !strings.Contains(addr, ":") {
		return net.JoinHostPort("127.0.0.1", addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// isFlagSet reports whether name was explicitly provided on the command
// line, so an env default does not clobber an operator's explicit flag.
func isFlagSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
