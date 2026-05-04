package tcmux

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/swarm"
)

// fakeSwarm is a swarmAPI stub. It returns a scripted sequence of
// ServiceList responses so a single test can drive the backend through
// add/remove/update transitions without a live Docker daemon.
type fakeSwarm struct {
	mu     sync.Mutex
	calls  int
	script []fakeReply
}

type fakeReply struct {
	services []swarm.Service
	err      error
}

func (f *fakeSwarm) ServiceList(_ context.Context, _ swarm.ServiceListOptions) ([]swarm.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.calls
	if idx >= len(f.script) {
		idx = len(f.script) - 1 // hold on the last reply forever
	}
	f.calls++
	r := f.script[idx]
	return r.services, r.err
}

func mkService(id, name string, labels map[string]string) swarm.Service {
	return swarm.Service{
		ID: id,
		Spec: swarm.ServiceSpec{
			Annotations: swarm.Annotations{Name: name, Labels: labels},
		},
	}
}

func TestSwarm_ServicesToUpstreams_Defaults(t *testing.T) {
	b := &SwarmBackend{}
	got := b.servicesToUpstreams([]swarm.Service{
		mkService("svc-1", "alpha", map[string]string{
			LabelURL: "http://alpha/cfg",
		}),
	}, discardLog())

	want := []Upstream{{
		ID:        "svc-1",
		Namespace: "alpha", // defaults to service name
		URL:       "http://alpha/cfg",
		Interval:  DefaultInterval,
		Timeout:   DefaultTimeout,
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSwarm_ServicesToUpstreams_LabelOverrides(t *testing.T) {
	b := &SwarmBackend{}
	got := b.servicesToUpstreams([]swarm.Service{
		mkService("svc-1", "alpha", map[string]string{
			LabelURL:       "http://alpha/cfg",
			LabelInterval:  "15s",
			LabelTimeout:   "2s",
			LabelNamespace: "alpha-ns",
		}),
	}, discardLog())

	want := []Upstream{{
		ID:        "svc-1",
		Namespace: "alpha-ns",
		URL:       "http://alpha/cfg",
		Interval:  15 * time.Second,
		Timeout:   2 * time.Second,
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSwarm_ServicesToUpstreams_BackendDefaults(t *testing.T) {
	// Unlabelled durations fall back to the backend's defaults, not the
	// package constants, when the operator has set TCMUXER_INTERVAL etc.
	b := &SwarmBackend{
		DefaultInterval: 7 * time.Second,
		DefaultTimeout:  3 * time.Second,
	}
	got := b.servicesToUpstreams([]swarm.Service{
		mkService("svc-1", "alpha", map[string]string{LabelURL: "http://a"}),
	}, discardLog())

	if got[0].Interval != 7*time.Second {
		t.Errorf("Interval = %v, want 7s", got[0].Interval)
	}
	if got[0].Timeout != 3*time.Second {
		t.Errorf("Timeout = %v, want 3s", got[0].Timeout)
	}
}

func TestSwarm_ServicesToUpstreams_BadDurationFallsBack(t *testing.T) {
	// A malformed duration label should not drop the upstream — it
	// should warn and use the default. Operators fix the label without
	// the merged config losing the service in the meantime.
	b := &SwarmBackend{}
	got := b.servicesToUpstreams([]swarm.Service{
		mkService("svc-1", "alpha", map[string]string{
			LabelURL:      "http://a",
			LabelInterval: "not-a-duration",
		}),
	}, discardLog())

	if len(got) != 1 {
		t.Fatalf("expected 1 upstream, got %d", len(got))
	}
	if got[0].Interval != DefaultInterval {
		t.Errorf("Interval = %v, want %v", got[0].Interval, DefaultInterval)
	}
}

func TestSwarm_ServicesToUpstreams_SortedByID(t *testing.T) {
	b := &SwarmBackend{}
	got := b.servicesToUpstreams([]swarm.Service{
		mkService("svc-c", "c", map[string]string{LabelURL: "http://c"}),
		mkService("svc-a", "a", map[string]string{LabelURL: "http://a"}),
		mkService("svc-b", "b", map[string]string{LabelURL: "http://b"}),
	}, discardLog())

	ids := []string{got[0].ID, got[1].ID, got[2].ID}
	want := []string{"svc-a", "svc-b", "svc-c"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
}

func TestSwarm_ServicesToUpstreams_SkipsMissingURL(t *testing.T) {
	// Filtered queries shouldn't return label-less services, but if
	// they ever do we drop them rather than register a useless poller.
	b := &SwarmBackend{}
	got := b.servicesToUpstreams([]swarm.Service{
		mkService("svc-1", "alpha", map[string]string{LabelURL: ""}),
		mkService("svc-2", "beta", map[string]string{LabelURL: "http://beta"}),
	}, discardLog())

	if len(got) != 1 || got[0].ID != "svc-2" {
		t.Fatalf("got %+v, want only svc-2", got)
	}
}

func TestSwarm_Run_EmitsAndReconciles(t *testing.T) {
	api := &fakeSwarm{script: []fakeReply{
		{services: []swarm.Service{
			mkService("svc-a", "a", map[string]string{LabelURL: "http://a"}),
		}},
		{services: []swarm.Service{
			mkService("svc-a", "a", map[string]string{LabelURL: "http://a"}),
			mkService("svc-b", "b", map[string]string{LabelURL: "http://b"}),
		}},
	}}

	b := &SwarmBackend{
		API:       api,
		Reconcile: 10 * time.Millisecond,
		Log:       discardLog(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan []Upstream, 4)
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, out) }()

	first := mustRecv(t, out, time.Second)
	if len(first) != 1 || first[0].ID != "svc-a" {
		t.Fatalf("first emit = %+v, want only svc-a", first)
	}

	second := mustRecv(t, out, time.Second)
	if len(second) != 2 {
		t.Fatalf("second emit = %+v, want both services", second)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestSwarm_Run_ListErrorIsTransient(t *testing.T) {
	// A failed ServiceList should be logged and tolerated; the next
	// tick recovers and the backend keeps running.
	api := &fakeSwarm{script: []fakeReply{
		{err: errors.New("boom")},
		{services: []swarm.Service{
			mkService("svc-a", "a", map[string]string{LabelURL: "http://a"}),
		}},
	}}

	b := &SwarmBackend{
		API:       api,
		Reconcile: 10 * time.Millisecond,
		Log:       discardLog(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan []Upstream, 4)
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, out) }()

	got := mustRecv(t, out, time.Second)
	if len(got) != 1 || got[0].ID != "svc-a" {
		t.Fatalf("got %+v, want svc-a after error recovery", got)
	}

	cancel()
	<-done
}

func mustRecv(t *testing.T, ch <-chan []Upstream, d time.Duration) []Upstream {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatal("timed out waiting for upstream list")
		return nil
	}
}
