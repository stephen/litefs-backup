FROM golang:1.23.0 as builder

WORKDIR /src/lfsb
COPY . .

ARG LFSB_VERSION

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-X main.Version=$LFSB_VERSION" -o /usr/local/bin/lfsb ./cmd/litefs-backup

FROM alpine

COPY --from=builder /usr/local/bin/lfsc /usr/local/bin/lfsc
COPY --from=flyio/ltx:0.3 /usr/local/bin/ltx /usr/local/bin/ltx

RUN apk add bash sqlite fuse3 curl ca-certificates

ENTRYPOINT lfsb

LABEL version=${LFSB_VERSION}
