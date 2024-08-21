# litefs-backup

A drop-in replacement backup service for the deprecated [LiteFS Cloud](https://fly.io/blog/litefs-cloud/).

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
