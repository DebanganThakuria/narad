ARG BUILDPLATFORM=linux/amd64
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# TARGETOS/TARGETARCH are populated per target platform by buildx.
# Hardcoded defaults would mask those values and bake one architecture's
# binary into every image variant; left empty, plain `docker build`
# falls back to the host's Go defaults.
ARG TARGETOS
ARG TARGETARCH
ARG GIT_REV=dev

ENV CGO_ENABLED=0

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
	go build -trimpath -ldflags="-s -w -X 'main.version=${GIT_REV}'" \
	-o /out/narad ./cmd/narad

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata && \
	addgroup -S narad && \
	adduser -S -D -H -u 10001 -G narad narad && \
	mkdir -p /var/lib/narad && \
	chown -R narad:narad /var/lib/narad

COPY --from=builder /out/narad /usr/local/bin/narad

USER narad
WORKDIR /var/lib/narad

EXPOSE 7942 7943 6060
VOLUME ["/var/lib/narad"]

ENTRYPOINT ["/usr/local/bin/narad"]
CMD ["serve", "--addr=0.0.0.0:7942", "--data-dir=/var/lib/narad"]
