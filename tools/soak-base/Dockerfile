# Generic soak-runner base: Go toolchain + redis + pre-warmed module
# cache. Contains no harness code — the harness source is mounted at
# runtime (ConfigMap in-cluster) and executed with `go run`.
FROM golang:1.26-alpine
RUN apk add --no-cache redis ca-certificates
WORKDIR /warm
COPY go.mod go.sum ./
RUN go mod download
WORKDIR /work
