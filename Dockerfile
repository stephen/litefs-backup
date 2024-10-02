FROM golang:1.23.1-alpine as builder

WORKDIR /src/lfsb
COPY . .

ARG LFSB_VERSION

RUN apk add --no-cache --update alpine-sdk

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go mod download

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-X 'main.Version=$LFSB_VERSION'" -o /usr/local/bin/lfsb-server ./cmd/litefs-backup

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -o /usr/local/bin/lfsb ./cmd/cli

FROM alpine

RUN apk add --no-cache --update libgcc

COPY --from=builder /usr/local/bin/lfsb-server /usr/local/bin/lfsb-server
COPY --from=builder /usr/local/bin/lfsb /usr/local/bin/lfsb
COPY --from=flyio/ltx:0.3 /usr/local/bin/ltx /usr/local/bin/ltx

ENTRYPOINT /usr/local/bin/lfsb-server

LABEL version=$LFSB_VERSION
