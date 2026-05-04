FROM golang:1.26-alpine AS build
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
ENTRYPOINT ["/tcmuxer"]
