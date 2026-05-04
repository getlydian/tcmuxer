package tcmux

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

// Swarm label keys. Apps opt in by setting tcmuxer.url; the others are
// optional companions documented in DESIGN.md §"Docker Swarm".
const (
	LabelURL       = "tcmuxer.url"
	LabelInterval  = "tcmuxer.interval"
	LabelTimeout   = "tcmuxer.timeout"
	LabelNamespace = "tcmuxer.namespace"
)

// DefaultReconcile is how often the swarm backend polls the Docker API
// for a fresh service list when no explicit interval is configured.
const DefaultReconcile = 30 * time.Second

// swarmAPI is the small subset of the Docker client surface we need.
// Defined as an interface so tests can substitute a fake without
// standing up a real Docker daemon.
type swarmAPI interface {
	ServiceList(ctx context.Context, options swarm.ServiceListOptions) ([]swarm.Service, error)
}

// SwarmBackend implements Backend by listing Docker Swarm services
// labelled with `tcmuxer.url` on a fixed interval and translating each
// match into an Upstream.
type SwarmBackend struct {
	// API is the Docker client. If nil, Run constructs one from the
	// environment via client.NewClientWithOpts(client.FromEnv).
	API swarmAPI

	// Reconcile is the interval between ServiceList calls. Zero means
	// DefaultReconcile.
	Reconcile time.Duration

	// DefaultInterval and DefaultTimeout are applied to upstreams whose
	// service does not set the corresponding label. Zero values fall
	// back to the package defaults.
	DefaultInterval time.Duration
	DefaultTimeout  time.Duration

	Log *slog.Logger
}

// Run lists services once on entry, then re-lists every Reconcile until
// ctx cancels. List errors are logged and the previous upstream set is
// kept in service — a transient Docker API blip should not nuke the
// merged config.
func (b *SwarmBackend) Run(ctx context.Context, out chan<- []Upstream) error {
	log := b.Log
	if log == nil {
		log = slog.Default()
	}

	api := b.API
	if api == nil {
		c, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			return fmt.Errorf("swarm: docker client: %w", err)
		}
		api = c
	}

	period := b.Reconcile
	if period <= 0 {
		period = DefaultReconcile
	}

	if err := b.tick(ctx, api, out, log); err != nil {
		return err
	}

	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := b.tick(ctx, api, out, log); err != nil {
				return err
			}
		}
	}
}

// tick performs one ServiceList + emit cycle. List errors are logged
// and swallowed so the loop keeps trying; only ctx-cancellation while
// sending stops the loop (and that's reported as nil by send).
func (b *SwarmBackend) tick(ctx context.Context, api swarmAPI, out chan<- []Upstream, log *slog.Logger) error {
	services, err := api.ServiceList(ctx, swarm.ServiceListOptions{
		Filters: serviceFilter(),
	})
	if err != nil {
		// ctx cancellation during shutdown is not an error.
		if ctx.Err() != nil {
			return nil
		}
		log.Warn("swarm: ServiceList failed, keeping previous list", "err", err)
		return nil
	}
	ups := b.servicesToUpstreams(services, log)
	return send(ctx, out, ups)
}

// serviceFilter restricts ServiceList to services that carry the
// tcmuxer.url label, so we don't drag the entire stack across the wire.
func serviceFilter() filters.Args {
	f := filters.NewArgs()
	f.Add("label", LabelURL)
	return f
}

// servicesToUpstreams maps the raw Docker response into our Upstream
// shape, applying label defaults. Services missing tcmuxer.url (which
// shouldn't happen given the filter, but defence in depth) or with
// malformed durations are skipped with a warning. Output is sorted by
// ID so reconciler diffs and tests are deterministic.
func (b *SwarmBackend) servicesToUpstreams(services []swarm.Service, log *slog.Logger) []Upstream {
	defInterval := b.DefaultInterval
	if defInterval <= 0 {
		defInterval = DefaultInterval
	}
	defTimeout := b.DefaultTimeout
	if defTimeout <= 0 {
		defTimeout = DefaultTimeout
	}

	out := make([]Upstream, 0, len(services))
	for _, s := range services {
		labels := s.Spec.Labels
		url := labels[LabelURL]
		if url == "" {
			continue
		}

		interval := defInterval
		if v := labels[LabelInterval]; v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				log.Warn("swarm: bad tcmuxer.interval, using default",
					"service", s.Spec.Name, "value", v, "err", err)
			} else {
				interval = d
			}
		}

		timeout := defTimeout
		if v := labels[LabelTimeout]; v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				log.Warn("swarm: bad tcmuxer.timeout, using default",
					"service", s.Spec.Name, "value", v, "err", err)
			} else {
				timeout = d
			}
		}

		ns := labels[LabelNamespace]
		if ns == "" {
			ns = s.Spec.Name
		}

		out = append(out, Upstream{
			ID:        s.ID,
			Namespace: ns,
			URL:       url,
			Interval:  interval,
			Timeout:   timeout,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
