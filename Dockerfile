# syntax=docker/dockerfile:1

# --- Build stage -----------------------------------------------------------
FROM golang:1.25 AS builder

# Version stamped into the binary via -ldflags; override at build time:
#   docker build --build-arg VERSION=$(git describe --tags --always) .
ARG VERSION=dev

WORKDIR /src

# Download modules first so they cache in their own layer.
COPY go.mod go.sum ./
RUN go mod download

# Build the static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /intermodal ./cmd/intermodal

# --- Final stage -----------------------------------------------------------
# distroless static ships CA certificates and runs as a non-root user, which
# is what we need for outbound TLS to the Railway GraphQL API and remote sinks.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /intermodal /intermodal

# Railway injects PORT and the container binds 0.0.0.0:$PORT. EXPOSE is not
# required on Railway but documents the default listen port for other runners.
EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/intermodal"]
CMD ["serve"]
