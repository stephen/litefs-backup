//go:build tools
// +build tools

package main

import (
	_ "github.com/amacneil/dbmate"
	_ "github.com/amonks/run/cmd/run"
	_ "github.com/minio/minio"
)
