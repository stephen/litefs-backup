# litefs-backup (lfsb)

A drop-in replacement for the deprecated [LiteFS Cloud](https://fly.io/blog/litefs-cloud/) backup service.

Lfsb will back your LiteFS cluster up to any s3-compatible storage provider.

## Limitations
- Lfsb does not do any authentication and assumes access across a private network.

## Development

### Setup
```bash
go install github.com/amonks/run/cmd/run@latest
run install
```

### Test
```bash
run test
run test-integration
```

### Migrations
```bash
dbmate [up, down, new]
```

### Configuration

Configuration is done through environment variables

#### `LFSB_DATA_PATH` (required)
The directory where lfsb will keep its local data store. This should
be a persistent, durable location.

#### `LFSB_BIND` (optional)
The address to bind to. The default is `:2200`.

Note that the .envrc sets
this to `127.0.0.1:2200` to help with [macOS security prompting](https://apple.stackexchange.com/questions/393715/do-you-want-the-application-main-to-accept-incoming-network-connections-pop).

#### `BUCKET_NAME` (required)
The AWS S3 bucket name to use.

#### `AWS_ENDPOINT_URL_S3` (optional)
The AWS S3 endpoint to use. The default is AWS S3.

This option is useful for using S3-compatible storage providers.

### `AWS_REGION` (required)
The AWS S3 region to use.

### `AWS_ACCESS_KEY` (required)
The AWS S3 access key to use.

### `AWS_SECRET_KEY` (required)
The AWS S3 secret key to use.

#### `SENTRY_DSN` (optional)
The [Sentry DSN](https://docs.sentry.io/concepts/key-terms/dsn-explainer/) to use. Sentry reporting will be
disabled if unset.
