package store_test

import (
	"bytes"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
)

// newShard returns an instance of Shard with standard compaction levels.
func newStore(tb testing.TB, path string) *store.Store {
	tb.Helper()
	s := store.NewStore(tb.TempDir())
	s.RemoteClient = NewFileStorageClient(filepath.Dir(path))

	s.Levels = []*store.CompactionLevel{
		{Level: 0},
		{Level: 1, Interval: 1 * time.Minute},
		{Level: 2, Interval: 10 * time.Minute},
		{Level: 3, Interval: 20 * time.Minute},
	}
	return s
}

// newOpenStore returns a new instance of a test store.
func newOpenStore(tb testing.TB, path string, opts ...func(*store.Store)) *store.Store {
	tb.Helper()
	s := newStore(tb, path)
	for _, opt := range opts {
		opt(s)
	}
	if err := s.Open(); err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		tb.Helper()
	})
	return s
}

// ltxSpecBytes returns the serialized bytes of spec.
func ltxSpecBytes(tb testing.TB, spec *ltx.FileSpec) []byte {
	tb.Helper()
	var buf bytes.Buffer
	if _, err := spec.WriteTo(&buf); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

// ltxSpecReader returns a spec as an io.Reader of its serialized bytes.
func ltxSpecReader(tb testing.TB, spec *ltx.FileSpec) io.Reader {
	tb.Helper()
	return bytes.NewReader(ltxSpecBytes(tb, spec))
}

func compareFileSpec(tb testing.TB, got, want *ltx.FileSpec) {
	tb.Helper()
	if !reflect.DeepEqual(got, want) {
		tb.Fatalf("spec mismatch:\ngot:  %#v\nwant: %#v", got, want)
	}
}
