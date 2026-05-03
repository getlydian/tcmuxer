package tcmux

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// Defaults applied to upstreams that omit a value. Exported so main and
// the swarm backend can reuse the same numbers.
const (
	DefaultInterval = 30 * time.Second
	DefaultTimeout  = 5 * time.Second
)

// staticFile is the on-disk YAML shape. Mirrors DESIGN.md §"Static
// config (secondary backend)".
type staticFile struct {
	Upstreams []staticEntry `yaml:"upstreams"`
}

type staticEntry struct {
	Name      string        `yaml:"name"`
	Namespace string        `yaml:"namespace"`
	URL       string        `yaml:"url"`
	Interval  time.Duration `yaml:"interval"`
	Timeout   time.Duration `yaml:"timeout"`
}

// StaticBackend implements Backend by reading a YAML file on start and
// re-reading it whenever a signal arrives. Tests inject Reload directly
// to drive re-reads without OS signals.
type StaticBackend struct {
	Path string
	Log  *slog.Logger

	// Reload, if non-nil, replaces the default SIGHUP source. A receive
	// on this channel triggers a re-read. Tests close or send on this
	// channel directly; production leaves it nil and gets SIGHUP.
	Reload <-chan struct{}
}

// Run reads the file once, emits the parsed upstream list, then waits
// for reload signals. A failed re-read logs and keeps the last good
// list in service — operators fix the file and signal again.
func (b *StaticBackend) Run(ctx context.Context, out chan<- []Upstream) error {
	log := b.Log
	if log == nil {
		log = slog.Default()
	}

	ups, err := loadStatic(b.Path)
	if err != nil {
		return fmt.Errorf("static: initial load: %w", err)
	}
	if err := send(ctx, out, ups); err != nil {
		return err
	}

	reload := b.Reload
	var sigCh chan os.Signal
	if reload == nil {
		sigCh = make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGHUP)
		defer signal.Stop(sigCh)
		reload = sighupBridge(sigCh)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-reload:
			if !ok {
				// Closed reload channel = stop watching for reloads but
				// keep serving the current list until ctx cancels.
				reload = nil
				continue
			}
			ups, err := loadStatic(b.Path)
			if err != nil {
				log.Warn("static: reload failed, keeping previous list",
					"path", b.Path, "err", err)
				continue
			}
			log.Info("static: reloaded", "path", b.Path, "upstreams", len(ups))
			if err := send(ctx, out, ups); err != nil {
				return nil
			}
		}
	}
}

// sighupBridge converts an os.Signal channel into a struct{} channel so
// the main select doesn't care which source it came from.
func sighupBridge(sig <-chan os.Signal) <-chan struct{} {
	out := make(chan struct{}, 1)
	go func() {
		for range sig {
			select {
			case out <- struct{}{}:
			default:
			}
		}
	}()
	return out
}

func send(ctx context.Context, out chan<- []Upstream, ups []Upstream) error {
	select {
	case out <- ups:
		return nil
	case <-ctx.Done():
		return nil
	}
}

func loadStatic(path string) ([]Upstream, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f staticFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	out := make([]Upstream, 0, len(f.Upstreams))
	seen := make(map[string]struct{}, len(f.Upstreams))
	for i, e := range f.Upstreams {
		if e.Name == "" {
			return nil, fmt.Errorf("upstream %d: name is required", i)
		}
		if e.URL == "" {
			return nil, fmt.Errorf("upstream %q: url is required", e.Name)
		}
		if _, dup := seen[e.Name]; dup {
			return nil, fmt.Errorf("upstream %q: duplicate name", e.Name)
		}
		seen[e.Name] = struct{}{}

		ns := e.Namespace
		if ns == "" {
			ns = e.Name
		}
		interval := e.Interval
		if interval <= 0 {
			interval = DefaultInterval
		}
		timeout := e.Timeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		out = append(out, Upstream{
			ID:        e.Name,
			Namespace: ns,
			URL:       e.URL,
			Interval:  interval,
			Timeout:   timeout,
		})
	}
	return out, nil
}
