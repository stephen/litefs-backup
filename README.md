# 📦 litefs-backup (lfsb)

A drop-in replacement for LiteFS Cloud. Lfsb will backup your LiteFS cluster to any s3-compatible storage provider.

This project is sponsored by [Tender](https://tender.run) - a private, productive personal finance tool.

This codebase is a simplified fork of [LiteFS Cloud](https://fly.io/blog/litefs-cloud/), originally
authored and graciously donated by [@benbjohnson](http://github.com/benbjohnson) and co at [fly.io](https://fly.io).


* [Overview](#overview)
   * [Control plane](#control-plane)
   * [Authentication](#authentication)
   * [Storage subsystem](#storage-subsystem)
   * [Limitations &amp; differences with LiteFS Cloud](#limitations--differences-with-litefs-cloud)
* [Quickstart: switching from LiteFS Cloud](#quickstart-switching-from-litefs-cloud)
   * [Deployment on fly.io](#deployment-on-flyio)
   * [Configure LiteFS to use lfsb](#configure-litefs-to-use-lfsb)
   * [Using the control plane](#using-the-control-plane)
      * [Install and configure the cli](#install-and-configure-the-cli)
      * [Basic operation](#basic-operation)
* [Development](#development)
   * [Setup](#setup)
   * [Test](#test)
   * [Migrations](#migrations)
   * [Configuration](#configuration)

# Overview
Lfsb is organized into clusters. A cluster can contain multiple sqlite databases. For instance, you might have a `prod` cluster with `users.db` and `datapoints.db` and another cluster `beta` with separate `users.db` and `jobs.db`.

## Control plane
Lfsb comes with a cli (`lfsb`) for importing, exporting, and restoring a database.

```
$ lfsb help
lfsb is a cli for administrating a lfsb-server instance.

Usage:
  lfsb [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  delete      Permanently delete a db and its backups from the cluster
  export      Export and download the database at its current position
  help        Help about any command
  import      Import a database into the litefs cluster
  info        Fetch min and max restorable timestamps for given db
  list        Get databases and positions for this cluster
  restore     Restore a database to a timestamp or txid

Flags:
  -c, --cluster string    lfsb cluster name (default "canary")
  -e, --endpoint string   lfsb endpoint (default "http://tender-litefs-backup.flycast:2200")
  -h, --help              help for lfsb
```

See section on [using the control plane](#using-the-control-plane).

## Authentication
Lfsb does not support any authentication scheme. To keep API-compatibility with LiteFS,
the `Authorization` header is still used to identify which cluster LiteFS is connecting to.

The authorization header is in the format `cluster [cluster name]`,
instead of the LiteFS Cloud format `FlyV1 [token]`.

## Storage subsystem
Lfsb stores data in two places:
- a sqlite metadata db on disk
- s3-compatible remote storage

Both are expected to be available and durable for the system to function.

## Limitations & differences with LiteFS Cloud
- Lfsb does not do any authentication and assumes access across a private network.
- Lfsb is API-compatible with [LiteFS](http://github.com/superfly/litefs), but its control plane API slightly differs from LiteFS Cloud. Calls from packages like [lfsc-go](https://github.com/superfly/lfsc-go) may not work or have expected results.
- Lfsb expects to be run as a singleton service. Running multiple instances could cause corruption or inconsistencies.

# Quickstart: switching from LiteFS Cloud

Lfsb is intended to be a mostly drop-in replacement for LiteFS Cloud.

## Deployment on fly.io
Prerequisite: set up a [fly.io](https://fly.io) account and install [flyctl](https://fly.io/docs/flyctl/install/).

1. Launch the app from this repository.
```sh
mkdir litefs-backup
cd litefs-backup
fly launch --from git@github.com:stephen/litefs-backup.git --build-only --generate-name --copy-config
```

When prompted to tweak settings, choose no.

2. Add tigris object storage and sentry (optional)

If you want to use S3 or another S3-compatible provider, skip this step and use `fly secrets set` to set
the necessary [environment variables](#configuration).

```sh
fly ext storage create
fly ext sentry create # optional
```

These commands should automatically configure the necessary [environment variables](#configuration).

3. Deploy privately with a single instance
```sh
fly deploy --ha=false --no-public-ips --flycast
```

4. Finish
```sh
fly logs

# Expected output:
# INFO running litefs-backup
# INFO server listening addr=:2200
# INFO waiting for signal or subprocess to exit
# INFO monitoring compaction level=1 interval=10s retention=1h0m0s
# INFO monitoring compaction level=2 interval=5m0s retention=72h0m0s
# INFO monitoring compaction level=3 interval=1h0m0s retention=720h0m0s
```

## Configure LiteFS to use lfsb

Use litefs from commit [2e78724973211ba426c224e89d99c256fd6722be](https://github.com/superfly/litefs/commit/2e78724973211ba426c224e89d99c256fd6722be) or later. If you pull litefs from docker, this looks like:
```
COPY --from=flyio/litefs:sha-2e78724 /usr/local/bin/litefs /usr/local/bin/litefs
```

Configure your service running litefs with two environment variables:
- Set `LITEFS_CLOUD_ENDPOINT` to the location of your newly deployed lfsb. On fly, this might look like `http://someones-litefs-backup.internal:2200`.
- Set `LITEFS_CLOUD_TOKEN` to `cluster [cluster name]`, e.g. `cluster canary`. Clusters do not need to be pre-registered.

## Using the control plane

### Install and configure the cli
Install the proper [release](https://github.com/stephen/litefs-backup/releases) for your system and place it in your $PATH.

```sh
curl "https://github.com/stephen/litefs-backup/releases/latest/download/litefs-backup_$(uname -s)_$(uname -m).tar.gz" 2>/dev/null | tar -xz run 2>/dev/null
```

You can optionally set `LFSB_CLUSTER` and `LFSB_ENDPOINT` to your expected cluster and endpoint url, e.g.

```sh
export LFSB_CLUSTER="prod"
export LFSB_ENDPOINT="http://someones-litefs-backup.internal:2200"
```

If you are using fly.io, [setup a wireguard vpn](https://fly.io/docs/networking/private-networking/#private-network-vpn) to connect into your private network:
```sh
fly wireguard create
```

### Basic operation
List known databases:
```sh
lfsb list
```

List restore points for a database:
```sh
lfsb info data.db
```

Download the current snapshot of a database:
```sh
lfsb export data.db
```

Restore a database to a timestamp or txid:
```sh
# first, do a dry run to check that the restore point is available:
lfsb restore data.db --check --txid=[txid] # or --timestamp=[timestamp, e.g. 2024-06-08T12:22:39]

# now, actually run the restore
lfsb restore data.db --txid=[txid]
```

# Development

## Setup
```sh
go install github.com/amonks/run/cmd/run@latest
run install
```

## Test
```bash
run test
run test-integration
```

## Migrations
```bash
dbmate [up, down, new]
```

## Configuration

Configuration is done through environment variables

### `LFSB_DATA_PATH` (required)
The directory where lfsb will keep its local data store. This should
be a persistent, durable location.

### `LFSB_BIND` (optional)
The address to bind to. The default is `:2200`.

Note that the .envrc sets
this to `127.0.0.1:2200` to help with [macOS security prompting](https://apple.stackexchange.com/questions/393715/do-you-want-the-application-main-to-accept-incoming-network-connections-pop).

### `BUCKET_NAME` (required)
The AWS S3 bucket name to use.

### `AWS_ENDPOINT_URL_S3` (optional)
The AWS S3 endpoint to use. The default is AWS S3.

This option is useful for using S3-compatible storage providers.

### `AWS_REGION` (required)
The AWS S3 region to use.

### `AWS_ACCESS_KEY` (required)
The AWS S3 access key to use.

### `AWS_SECRET_KEY` (required)
The AWS S3 secret key to use.

### `SENTRY_DSN` (optional)
The [Sentry DSN](https://docs.sentry.io/concepts/key-terms/dsn-explainer/) to use. Sentry reporting will be
disabled if unset.
