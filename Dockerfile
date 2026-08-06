FROM golang:1.26.2-alpine AS build
WORKDIR /src

ARG VERSION=dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/tcmuxer \
        ./cmd/tcmuxer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tcmuxer /tcmuxer
EXPOSE 80
USER nonroot:nonroot

# The image has no shell, wget or curl, so the exec-form healthcheck
# invokes the binary's own probe. It GETs /healthz on the local listener
# and exits non-zero when tcmuxer has never built a routing table, a
# state the orchestrator cannot otherwise distinguish from healthy.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["/tcmuxer", "-healthcheck"]

ENTRYPOINT ["/tcmuxer"]
