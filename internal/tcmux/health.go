package tcmux

import (
	"fmt"
	"sort"
	"time"
)

// Health is the readiness verdict for the process, derived from the
// cache. It is deliberately a *cold-start* check: it answers "has this
// process ever managed to build a routing table?", not "is every
// upstream healthy right now".
//
// That distinction is the whole point. tcmuxer holds last-known-good
// config when an upstream blips, which is correct for a running process
// — a single sick app must never fail tcmuxer's healthcheck and trigger
// a restart that drops routing for everyone else. But a *fresh* task
// that has never succeeded once has no last-known-good to hold: it
// serves `{}`, Traefik installs no routers, and every site 404s.
//
// That cold-start state can be both silent and permanent. If a task
// starts with a broken view of its network — a stale embedded-DNS
// record, say, so upstream names never resolve even though the datapath
// is fine — nothing in the system retries it. The orchestrator sees a
// process that started and stayed up, so it never reschedules the task,
// and the empty config persists until a human intervenes. Detecting it
// requires asking whether the process ever did its job, which is what
// this check does.
//
// So: never-succeeded-and-upstreams-are-expected is unhealthy;
// succeeded-once-and-now-stale is healthy but reported as degraded.
type Health struct {
	// Ready is the verdict the healthcheck acts on. False means "this
	// process has never produced a usable config and should be replaced".
	Ready bool

	// Reason is a short human-readable explanation, logged by the
	// healthcheck subcommand and returned in the /healthz body.
	Reason string

	// Upstreams is how many upstreams discovery currently knows about.
	Upstreams int

	// Good is how many of those have a last-known-good document, i.e.
	// are contributing to /config right now.
	Good int

	// Stale is how many succeeded at some point but whose most recent
	// poll failed. These do not affect Ready — their last-known-good is
	// still being served, which is the intended behaviour.
	Stale int

	// Errors lists the distinct last-error strings across upstreams that
	// have never succeeded, sorted and deduplicated. This is what makes
	// the "no such host" case self-diagnosing in the healthcheck output.
	Errors []string
}

// Healthy reports readiness for the given cache snapshot.
//
// The three cases:
//
//   - No upstreams discovered at all → ready. This is a legitimate
//     steady state, not a fault: an environment where no service carries
//     a `tcmuxer.url` label has nothing to mux, and `/config` returning
//     `{}` is the correct answer. Failing here would restart-loop a
//     correctly-idle tcmuxer forever.
//   - Upstreams discovered, none has ever succeeded → NOT ready. We were
//     told there is work to do and have never once managed to do it.
//     Replacing the task is the right move, and is exactly what an
//     operator would otherwise have to do by hand.
//   - At least one upstream has a last-known-good document → ready, even
//     if others are failing and even if this one's latest poll failed.
//     We are serving real routes; a restart would only make things worse.
func Healthy(snap map[string]Entry) Health {
	h := Health{Upstreams: len(snap)}

	var errs map[string]struct{}
	for _, e := range snap {
		switch {
		case !e.LastGood.IsZero():
			h.Good++
			if e.LastErr != "" {
				h.Stale++
			}
		case e.LastErr != "":
			if errs == nil {
				errs = make(map[string]struct{})
			}
			errs[e.LastErr] = struct{}{}
		}
	}

	if len(errs) > 0 {
		h.Errors = make([]string, 0, len(errs))
		for e := range errs {
			h.Errors = append(h.Errors, e)
		}
		sort.Strings(h.Errors)
	}

	switch {
	case h.Upstreams == 0:
		h.Ready = true
		h.Reason = "no upstreams discovered"
	case h.Good > 0:
		h.Ready = true
		h.Reason = fmt.Sprintf("serving config from %d/%d upstreams", h.Good, h.Upstreams)
	default:
		h.Ready = false
		h.Reason = fmt.Sprintf("no upstream has ever returned a usable config (%d discovered)", h.Upstreams)
	}
	return h
}

// staleness is unused by the readiness verdict but kept alongside it so
// the /healthz payload can report the oldest last-good time, which is
// what an operator wants to see when diagnosing a degraded-but-ready
// process.
func oldestLastGood(snap map[string]Entry) time.Time {
	var oldest time.Time
	for _, e := range snap {
		if e.LastGood.IsZero() {
			continue
		}
		if oldest.IsZero() || e.LastGood.Before(oldest) {
			oldest = e.LastGood
		}
	}
	return oldest
}
