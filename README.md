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
- `GET /healthz` — 200 while the process is up. Does **not** depend on
  upstream health (one bad app shouldn't fail tcmuxer's healthcheck and
  trigger a restart loop).
- `GET /debug` — JSON dump of discovered upstreams: last-good timestamp,
  staleness, last error per upstream, and cumulative merge collision
  counters.

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
