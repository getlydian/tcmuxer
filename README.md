# tcmuxer

A small daemon that aggregates Traefik dynamic-config documents from many
HTTP endpoints into one, so Traefik's HTTP provider (which only accepts a
single URL) can be backed by N independent producers.

A complete README will land once the implementation does.
