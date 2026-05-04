// Command tcmuxer multiplexes Traefik HTTP-provider configs from many
// upstreams into one merged document.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/getlydian/tcmuxer/internal/tcmux"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Environ(), os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// config is the resolved set of options after merging defaults, env, and flags.
type config struct {
	listen        string
	backend       string
	staticFile    string
	interval      time.Duration
	timeout       time.Duration
	maxStaleness  time.Duration
	reconcile     time.Duration
	staleSweep    time.Duration
	shutdownGrace time.Duration
}

// run is the testable entry point. It blocks until ctx cancels or a
// fatal error occurs; on clean shutdown it returns nil.
func run(ctx context.Context, args []string, environ []string, stdout, stderr io.Writer) error {
	env := envMap(environ)

	cfg := config{
		listen:        envOr(env, "TCMUXER_LISTEN", ":80"),
		backend:       envOr(env, "TCMUXER_BACKEND", "static"),
		staticFile:    envOr(env, "TCMUXER_STATIC_FILE", ""),
		shutdownGrace: 5 * time.Second,
		staleSweep:    30 * time.Second,
	}

	var err error
	if cfg.interval, err = envDuration(env, "TCMUXER_INTERVAL", tcmux.DefaultInterval); err != nil {
		return err
	}
	if cfg.timeout, err = envDuration(env, "TCMUXER_TIMEOUT", tcmux.DefaultTimeout); err != nil {
		return err
	}
	if cfg.maxStaleness, err = envDuration(env, "TCMUXER_MAX_STALENESS", 10*time.Minute); err != nil {
		return err
	}
	if cfg.reconcile, err = envDuration(env, "TCMUXER_RECONCILE", tcmux.DefaultReconcile); err != nil {
		return err
	}

	fs := flag.NewFlagSet("tcmuxer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.listen, "listen", cfg.listen, "address to listen on (TCMUXER_LISTEN)")
	fs.StringVar(&cfg.backend, "backend", cfg.backend, "discovery backend: static|swarm (TCMUXER_BACKEND)")
	fs.StringVar(&cfg.staticFile, "static-file", cfg.staticFile, "path to static upstream YAML (TCMUXER_STATIC_FILE)")
	fs.DurationVar(&cfg.interval, "interval", cfg.interval, "default poll interval (TCMUXER_INTERVAL)")
	fs.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "default poll timeout (TCMUXER_TIMEOUT)")
	fs.DurationVar(&cfg.maxStaleness, "max-staleness", cfg.maxStaleness, "drop cache entries older than this (TCMUXER_MAX_STALENESS)")
	fs.DurationVar(&cfg.reconcile, "reconcile", cfg.reconcile, "swarm: how often to re-list services (TCMUXER_RECONCILE)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(stderr, nil))

	backend, err := buildBackend(cfg, log)
	if err != nil {
		return err
	}

	// runCtx mirrors ctx but we can also cancel it ourselves on fatal
	// errors so pollers and the discovery loop tear down even when the
	// shutdown trigger isn't a signal.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	cache := tcmux.NewCache(nil)
	httpClient := &http.Client{Timeout: cfg.timeout}
	spawn := tcmux.DefaultSpawn(httpClient, cache, log)
	reconciler := tcmux.NewReconciler(runCtx, spawn, cache, log)
	server := tcmux.NewServer(cache, log)

	httpSrv := &http.Server{
		Addr:              cfg.listen,
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Start HTTP server.
	httpErr := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", cfg.listen)
		err := httpSrv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
			return
		}
		httpErr <- nil
	}()

	// Start discovery loop.
	updates := make(chan []tcmux.Upstream, 1)
	discErr := make(chan error, 1)
	go func() {
		discErr <- backend.Run(runCtx, updates)
	}()

	// Stale-sweeper drops cache entries past the max-staleness window.
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		runStaleSweeper(runCtx, cache, cfg.maxStaleness, cfg.staleSweep, log)
	}()

	// Apply discovery updates until shutdown.
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		for {
			select {
			case <-runCtx.Done():
				return
			case ups, ok := <-updates:
				if !ok {
					return
				}
				reconciler.Apply(ups)
			}
		}
	}()

	// Wait for shutdown signal or fatal error.
	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-httpErr:
		if err != nil {
			log.Error("http server failed", "err", err)
			runErr = err
		}
	case err := <-discErr:
		if err != nil {
			log.Error("discovery backend failed", "err", err)
			runErr = err
		}
	}

	// Graceful shutdown: cancel runCtx so background goroutines unblock,
	// stop the HTTP server with a bounded grace window, then wait for
	// pollers and discovery to drain.
	cancelRun()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	<-loopDone
	<-sweepDone
	reconciler.StopAll()

	return runErr
}

func buildBackend(cfg config, log *slog.Logger) (tcmux.Backend, error) {
	switch cfg.backend {
	case "static":
		if cfg.staticFile == "" {
			return nil, fmt.Errorf("backend=static requires TCMUXER_STATIC_FILE / -static-file")
		}
		return &tcmux.StaticBackend{Path: cfg.staticFile, Log: log}, nil
	case "swarm":
		return &tcmux.SwarmBackend{
			Reconcile:       cfg.reconcile,
			DefaultInterval: cfg.interval,
			DefaultTimeout:  cfg.timeout,
			Log:             log,
		}, nil
	default:
		return nil, fmt.Errorf("unknown backend %q (want static|swarm)", cfg.backend)
	}
}

// runStaleSweeper periodically drops cache entries whose last successful
// poll is older than max. A non-positive max disables the sweeper.
func runStaleSweeper(ctx context.Context, cache *tcmux.Cache, max, period time.Duration, log *slog.Logger) {
	if max <= 0 || period <= 0 {
		return
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			for id, e := range cache.Snapshot() {
				if e.LastGood.IsZero() {
					continue
				}
				if now.Sub(e.LastGood) > max {
					log.Warn("dropping stale upstream", "id", id, "namespace", e.Namespace, "age", now.Sub(e.LastGood))
					cache.Drop(id)
				}
			}
		}
	}
}

func envMap(environ []string) map[string]string {
	out := make(map[string]string, len(environ))
	for _, kv := range environ {
		i := strings.IndexByte(kv, '=')
		if i < 0 {
			continue
		}
		out[kv[:i]] = kv[i+1:]
	}
	return out
}

func envOr(env map[string]string, key, def string) string {
	if v, ok := env[key]; ok && v != "" {
		return v
	}
	return def
}

func envDuration(env map[string]string, key string, def time.Duration) (time.Duration, error) {
	v, ok := env[key]
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

