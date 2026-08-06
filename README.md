# tcmuxer

A small daemon that aggregates Traefik dynamic-config documents from many
HTTP endpoints into one, so Traefik's HTTP provider (which only accepts
a single URL) can be backed by N independent producers.

## Problem

Traefik's HTTP provider polls one URL and expects a full dynamic config
document in response. That works fine when a single component owns
routing, but breaks down when multiple apps in the same cluster each
want to contribute **dynamic** routes — routes whose shape depends on
data the app owns (tenants, customer custom domains, alias redirects,
per-account vhosts, etc.).

The usual workarounds:

- Run a Traefik per app. Wastes resources, fragments TLS state, and
  every app reinvents cert-resolver config.
- Have one app generate config for everyone. Couples unrelated apps
  and makes the "config owner" a deployment chokepoint.
- Use the file provider and have apps drop YAML on a shared volume.
  Trades HTTP for filesystem coordination; not better.

What's actually wanted: keep one shared Traefik, let each app expose
its own HTTP config endpoint, and have *something* in front of Traefik
that fans those endpoints into one document.

## Approach

`tcmuxer` is that something. It:

1. Discovers upstream config endpoints (Docker Swarm labels, or a
   static file).
2. Polls each upstream's HTTP endpoint for a Traefik dynamic config
   document.
3. Deep-merges the responses into one combined document.
4. Serves the merged document on its own HTTP endpoint.

Traefik points at tcmuxer with a single
`--providers.http.endpoint=http://tcmuxer/config` flag. tcmuxer is
generic; the only app-specific knowledge lives in the labels (or
static entries) operators choose to set. Apps keep producing config
exactly the way Traefik's HTTP provider expects
(`{"http": {"routers": {...}, ...}}`), so existing endpoints need no
behavioural change to be muxed.

## Quickstart

Point Traefik at tcmuxer's `/config` endpoint:

```yaml
# traefik.yml
providers:
  http:
    endpoint: http://tcmuxer/config
    pollInterval: 30s
```

Run tcmuxer with a static upstream list:

```yaml
# upstreams.yml
upstreams:
  - name: app-a
    url: http://app-a/traefik-config
    interval: 30s
    timeout: 5s
  - name: app-b
    url: http://app-b/traefik-config
```

```sh
docker run --rm -p 80:80 \
  -v "$PWD/upstreams.yml:/etc/tcmuxer/upstreams.yml:ro" \
  -e TCMUXER_BACKEND=static \
  -e TCMUXER_STATIC_FILE=/etc/tcmuxer/upstreams.yml \
  ghcr.io/getlydian/tcmuxer:edge
```

`curl http://localhost/config` returns the merged document. SIGHUP
re-reads the file.

## Configuration

All options have an env var and a matching `-flag`. Flags win over env;
env wins over defaults.

| Env var                 | Flag              | Default | Purpose                                                |
|-------------------------|-------------------|---------|--------------------------------------------------------|
| `TCMUXER_LISTEN`        | `-listen`         | `:80`   | HTTP listen address.                                   |
| `TCMUXER_BACKEND`       | `-backend`        | `static`| Discovery backend: `static` or `swarm`.                |
| `TCMUXER_STATIC_FILE`   | `-static-file`    | —       | Path to upstream YAML (required when backend=static).  |
| `TCMUXER_INTERVAL`      | `-interval`       | `30s`   | Default per-upstream poll interval.                    |
| `TCMUXER_TIMEOUT`       | `-timeout`        | `5s`    | Default per-poll HTTP timeout.                         |
| `TCMUXER_MAX_STALENESS` | `-max-staleness`  | `10m`   | Drop an upstream from output once its cache is older.  |
| `TCMUXER_RECONCILE`     | `-reconcile`      | `30s`   | Swarm: how often to re-list services.                  |

Logs are slog JSON on stderr.

## Discovery backends

### Static (`TCMUXER_BACKEND=static`)

For non-Swarm deployments and for testing, tcmuxer reads a static list
of upstreams from a YAML file at `TCMUXER_STATIC_FILE`:

```yaml
upstreams:
  - name: app-a                             # required, also the upstream ID
    namespace: app-a                        # optional, defaults to name
    url: http://app-a/traefik-config        # required
    interval: 30s                           # optional, default 30s
    timeout: 5s                             # optional, default 5s
```

Send SIGHUP to re-read. A failed reload logs a warning and keeps the
previous list in service — operators fix the file and signal again.

### Docker Swarm (`TCMUXER_BACKEND=swarm`)

In a Swarm cluster, tcmuxer discovers upstreams by querying the Swarm
API for services that carry a `tcmuxer.url` deploy-label. There are
three pieces to wire up: the socket source, the tcmuxer service, and
each app that wants to contribute config.

**1. Expose the Docker socket read-only.** tcmuxer needs `SERVICES`,
`TASKS`, and `NETWORKS` access to enumerate services and resolve their
overlay addresses. Don't bind the raw socket; run a proxy:

```yaml
# compose-stack.yml
services:
  docker-socket-proxy:
    image: tecnativa/docker-socket-proxy
    networks: [socket]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    environment:
      SERVICES: 1
      TASKS: 1
      NETWORKS: 1
    deploy:
      placement:
        constraints: [node.role == manager]
```

**2. Run tcmuxer.** Attach it to the same network as your apps so it
can resolve their service names, and to the socket network so it can
reach the proxy. Point Traefik at it.

```yaml
  tcmuxer:
    image: ghcr.io/getlydian/tcmuxer:edge
    networks: [traefik_public, socket]
    environment:
      TCMUXER_BACKEND: swarm
      DOCKER_HOST: tcp://docker-socket-proxy:2375
    deploy:
      replicas: 1
      placement:
        constraints: [node.role == manager]

  traefik:
    image: traefik:v3
    networks: [traefik_public]
    command:
      - --providers.http.endpoint=http://tcmuxer/config
      - --providers.http.pollInterval=30s
      # ...your entrypoints, cert resolvers, etc.
```

**3. Opt apps in.** Each app that wants to contribute config sets
`tcmuxer.url` on the service that hosts its config endpoint. The URL
must be reachable from tcmuxer over a shared overlay network.

```yaml
  app-a:
    image: example/app-a
    networks: [traefik_public]   # same network as tcmuxer
    deploy:
      labels:
        - "tcmuxer.url=http://app-a/traefik-config"
        - "tcmuxer.namespace=app-a"     # optional
        - "tcmuxer.interval=30s"        # optional
        - "tcmuxer.timeout=5s"          # optional
```

| Label               | Default      | Purpose                                            |
|---------------------|--------------|----------------------------------------------------|
| `tcmuxer.url`       | —            | Required. Full URL tcmuxer GETs.                   |
| `tcmuxer.interval`  | `30s`        | Per-upstream poll interval.                        |
| `tcmuxer.timeout`   | `5s`         | Per-poll HTTP timeout.                             |
| `tcmuxer.namespace` | service name | Logical name shown in logs and collision warnings. |

tcmuxer reconciles the upstream list every `TCMUXER_RECONCILE` (default
30s). Adding a new app = deploy it with the label; removing one =
redeploy without it. No tcmuxer restart needed.

## Endpoints

- `GET /config` — current merged Traefik config (JSON). What Traefik polls.
- `GET /healthz` — readiness. 200 when tcmuxer has a usable config to
  serve, 503 only on cold start (see below).
- `GET /debug` — JSON dump of discovered upstreams: last-good timestamp,
  staleness, last error per upstream, and cumulative merge collision
  counters.

## Health checking

`/healthz` answers one question: **has this process ever managed to
build a routing table?** It is a cold-start check, not a rollup of
upstream health.

| State                                          | Status | Rationale                                                    |
|------------------------------------------------|--------|--------------------------------------------------------------|
| No upstreams discovered                        | `200`  | Nothing to mux is a valid steady state, not a fault.         |
| At least one upstream has a last-known-good doc | `200`  | Real routes are being served, even if other upstreams fail.  |
| Upstreams discovered, none ever succeeded       | `503`  | `/config` is `{}` — Traefik would install no routers at all. |

The asymmetry is deliberate. tcmuxer keeps serving last-known-good config
when an upstream blips, so one sick app must never fail tcmuxer's
healthcheck and trigger a restart that drops routing for everyone else.
But a task that has *never* succeeded has no last-known-good to fall back
on: it serves an empty document, and every site behind Traefik 404s.
That state can be silent and permanent — a task that comes up with a
broken view of its network never re-resolves it, and an orchestrator that
only checks "is the process running?" will happily leave it in place
forever. Returning 503 is what lets the orchestrator replace it.

Add `?verbose` (or `Accept: application/json`) for a JSON breakdown:

```json
{
  "status": "not ready",
  "reason": "no upstream has ever returned a usable config (2 discovered)",
  "upstreams": 2,
  "good": 0,
  "stale": 0,
  "errors": ["lookup app-a on 127.0.0.11:53: no such host"]
}
```

### `tcmuxer -healthcheck`

The runtime image is distroless — no shell, no `wget`, no `curl` — so a
shell-form `test:` healthcheck cannot work. The binary therefore ships
its own probe: `tcmuxer -healthcheck` GETs `/healthz` on the local
listener and exits `0` (ready) or `1` (not ready), printing the server's
reason so `docker inspect` shows *why* a container went unhealthy.

The published image already declares a `HEALTHCHECK`, so a Swarm service
picks it up with no extra configuration. To tune it, override in compose:

```yaml
  tcmuxer:
    image: ghcr.io/getlydian/tcmuxer:edge
    healthcheck:
      test: ["CMD", "/tcmuxer", "-healthcheck"]
      interval: 30s
      timeout: 5s
      start_period: 60s
      retries: 3
```

Pair it with a `restart_policy` / `update_config` so an unhealthy task is
actually replaced rather than merely flagged.

| Flag / env                        | Default        | Purpose                                     |
|-----------------------------------|----------------|---------------------------------------------|
| `-healthcheck`                    | off            | Run the probe and exit instead of serving.  |
| `-healthcheck-addr` / `TCMUXER_HEALTHCHECK_ADDR` | `-listen` | Address to probe.               |
| `-healthcheck-timeout` / `TCMUXER_HEALTHCHECK_TIMEOUT` | `3s` | Probe timeout.             |

`start_period` matters: give tcmuxer long enough to discover upstreams
and complete a first poll, or a slow-starting app will fail the probe
before it has had a fair chance to answer.

### `?certresolver=<name>` / `?stripcertresolvers` on `/config`

These two mutually-exclusive params shape the per-router `tls.certResolver`
for the two halves of a **split-issuer topology**: one Traefik issues certs
via ACME, a separate Traefik (or several, for HA) only terminates TLS from a
shared cert store and has **no** `certResolver` registered.

- `GET /config?certresolver=<name>` (the **issuer** poll) stamps
  `tls.certResolver=<name>` onto every `http.router` that **already declares
  its own `tls` block** but hasn't named a resolver. Routers without a `tls`
  block, and routers that already set `certResolver`, are left untouched.
- `GET /config?stripcertresolvers` (the **terminating** poll) removes
  `tls.certResolver` from every router, regardless of who set it. Other `tls`
  fields (`domains`, `options`, …) are preserved; routers without a `tls`
  block are untouched.

The catch this works around is Traefik's TLS-inheritance rule — *a router
that declares any `tls` field opts out of the entrypoint-level TLS defaults
entirely, resolver included.* So a router carrying `tls.domains` (e.g. a
wildcard) never inherits the issuer's default resolver and its cert is never
issued; the issuer poll re-supplies it. Conversely, a router an app pinned to
a specific resolver itself (e.g. `tls.certResolver=http` to force HTTP-01 for
a non-owned domain) carries that name to the terminating Traefik verbatim,
which would disable the router (`nonexistent certificate resolver`) since it
has no resolver registered; the terminating poll strips it.

Point each Traefik at the variant it needs:

```yaml
# issuing Traefik — gets the resolver stamped on resolver-less tls routers,
# and keeps any resolver an app pinned itself
--providers.http.endpoint=http://tcmuxer/config?certresolver=dns

# terminating Traefik(s) — every per-router resolver stripped; naming any
# resolver it doesn't have would disable the router
--providers.http.endpoint=http://tcmuxer/config?stripcertresolvers
```

> Note: the terminating poll was previously the bare `/config` (verbatim).
> That only stayed correct while tcmuxer was the *only* thing adding
> resolvers (issuer-poll-only). Once an upstream app sets a resolver itself,
> the terminating poll must use `?stripcertresolvers` to stay resolver-free.

If both params are present, strip wins. tcmuxer stays generic: it knows
nothing about "issuer" vs "edge" roles — only "stamp this resolver name,
strip all of them, or neither." Scope is `http.routers`; TCP/UDP ACME is
not handled.

## Merge semantics

Traefik dynamic config is a tree under
`http.{routers,services,middlewares,serversTransports}`, `tcp.{...}`,
`udp.{...}`, and `tls.{certificates,options,stores}`. Merge rules:

- Maps are merged key-by-key, recursively.
- Lists (e.g. `tls.certificates`) are concatenated with no dedup.
- On key collision (two upstreams declare the same router name),
  tcmuxer **logs a loud warning** and the lexicographically-smaller
  upstream namespace wins (deterministic, not "last write"). Collisions
  are a configuration bug — they should be visible, not hidden.
- Top-level keys absent from an upstream are simply skipped.

Apps **should** prefix router/service/middleware names with their stack
or app name (e.g. `myapp-static-redirect-foo-com`) to avoid collisions
in the first place. tcmuxer warns on collisions but does not refuse to
serve — keeping the cluster routable beats strict enforcement during a
regression.

## Operations

Per upstream:

- **First poll fails** → upstream contributes nothing, warning logged.
  tcmuxer continues serving the rest.
- **Subsequent poll fails** → last-known-good is kept; the staleness
  counter on `/debug` grows. Once age exceeds `TCMUXER_MAX_STALENESS`
  (default `10m`), the upstream is dropped from the merged output.
- **Malformed JSON** → treated as a failed poll; tcmuxer never poisons
  the merged output with partial/garbage config.

Aggregate:

- The merged document is built fresh on each `GET /config` from each
  upstream's last-good cache. No partial reloads, no in-flight reads
  observing half-merged state.
- Process death → orchestrator restarts tcmuxer; until then, Traefik
  serves whatever it last polled. Acceptable: cluster routes rarely
  change minute-to-minute.

## Building & testing

```sh
go test ./...
go build ./cmd/tcmuxer
docker build -t tcmuxer:dev .
```

## License

MIT — see [LICENSE](LICENSE).
