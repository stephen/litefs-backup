FROM golang:1.23.1-alpine AS builder

WORKDIR /src/lfsb
COPY . .

ARG LFSB_VERSION

RUN apk add alpine-sdk

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go mod download

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-X 'main.Version=$LFSB_VERSION'" -o /usr/local/bin/lfsb-server ./cmd/lfsb-server

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -o /usr/local/bin/lfsb ./cmd/lfsb

FROM alpine

RUN apk add libgcc

COPY --from=builder /usr/local/bin/lfsb-server /usr/local/bin/lfsb-server
COPY --from=builder /usr/local/bin/lfsb /usr/local/bin/lfsb
COPY --from=flyio/ltx:0.3 /usr/local/bin/ltx /usr/local/bin/ltx

ENTRYPOINT ["/usr/local/bin/lfsb-server"]

ARG LFSB_VERSION
LABEL version="$LFSB_VERSION"
