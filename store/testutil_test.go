package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
)

// newShard returns an instance of Shard with standard compaction levels.
func newStore(tb testing.TB, path string) *store.Store {
	tb.Helper()
	s := store.NewStore(&lfsb.Config{
		Path: tb.TempDir(),
	})
	s.CompactionEnabled = false
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
	if err := s.Open(context.TODO()); err != nil {
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

func makeSQLiteDB(tb testing.TB, pageSize, rows int) []byte {
	tb.Helper()

	f, err := os.CreateTemp(tb.TempDir(), "db")
	if err != nil {
		tb.Fatal(err)
	}

	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		tb.Fatal(err)
	}

	if _, err := db.Exec(fmt.Sprintf(`PRAGMA page_size = %d`, pageSize)); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec(`VACUUM`); err != nil {
		tb.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)`); err != nil {
		tb.Fatal(err)
	}

	for i := 0; i < rows; i++ {
		if _, err := db.Exec(`INSERT INTO test (data) VALUES (?)`, strings.Repeat(strconv.Itoa(i), 512)); err != nil {
			tb.Fatal(err)
		}
	}

	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		tb.Fatal(err)
	}

	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}

	return data
}

func compactUpToLevel(tb testing.TB, s *store.Store, cluster, database string, level int) {
	tb.Helper()
	for lvl := 1; lvl <= level; lvl += 1 {
		if _, err := s.CompactDBToLevel(context.Background(), nil, cluster, database, lvl); err != nil && lfsb.ErrorCode(err) != lfsb.ENOCOMPACTION {
			tb.Fatal(err)
		}
	}
}
