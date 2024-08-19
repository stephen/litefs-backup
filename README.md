# litefs-backup

A drop-in replacement backup service for the deprecated [LiteFS Cloud](https://fly.io/blog/litefs-cloud/).

## Development

### Setup
```bash
make setup
```

### Test
```bash
go test ./...
```

### Migrations
```bash
dbmate [up, down, new]
```
