package testingutil

import (
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/dustin/go-humanize"
)

func GenerateDBIfNotExists(tb testing.TB, path string, size int64) {
	tb.Helper()
	rnd := rand.New(rand.NewSource(0))

	const rowsPerTx = 10000
	const rowSize = 256

	if _, err := os.Stat(path); err == nil {
		tb.Logf("database already exists, skipping generation: %s", path)
		return
	} else if err != nil && !os.IsNotExist(err) {
		tb.Fatalf("stat database: %s", err)
	}

	tb.Logf("generating database %q of at least %s", path, humanize.Bytes(uint64(size)))

	if err := os.MkdirAll(filepath.Dir(path), 0o777); err != nil {
		tb.Fatal(err)
	}

	tempPath := path + ".tmp"
	defer func() { _ = os.Remove(tempPath) }()

	if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
		tb.Fatalf("cannot remove: %s", err)
	}

	db, err := sql.Open("sqlite3", tempPath)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		tb.Fatalf("create table: %s", err)
	}

	for {
		func() {
			tx, err := db.Begin()
			if err != nil {
				tb.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()

			buf := make([]byte, rowSize/2)
			for i := 0; i < rowsPerTx; i++ {
				_, _ = rnd.Read(buf)
				if _, err := tx.Exec(`INSERT INTO t VALUES (?)`, fmt.Sprintf("%x", buf)); err != nil {
					tb.Fatalf("insert row: %s", err)
				}
			}

			if err := tx.Commit(); err != nil {
				tb.Fatal(err)
			}
		}()

		// Exit once we reach the requested size.
		fi, err := os.Stat(tempPath)
		if err != nil {
			tb.Fatal(err)
		} else if fi.Size() > size {
			break
		}

		tb.Logf("generation progress: %s", humanize.Bytes(uint64(fi.Size())))
	}

	if err := os.Rename(tempPath, path); err != nil {
		tb.Fatal(err)
	}
}

func GenerateDBIfNotExistsBytes(tb testing.TB, path string, size int64) []byte {
	tb.Helper()
	GenerateDBIfNotExists(tb, path, size)

	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func GenerateDBIfNotExistsReader(tb testing.TB, path string, size int64) io.Reader {
	tb.Helper()
	GenerateDBIfNotExists(tb, path, size)

	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := f.Close(); err != nil {
			tb.Fatalf("cannot close generated db file: %s", err)
		}
	})

	return f
}

type ResumableReader struct {
	reading chan struct{}
	resume  chan error
}

func NewResumableReader() *ResumableReader {
	return &ResumableReader{
		reading: make(chan struct{}),
		resume:  make(chan error),
	}
}

// Reading returns a channel that sends a value when the Read() function is called.
// Resume must be called after this to resume the Read().
func (r *ResumableReader) Reading() <-chan struct{} { return r.reading }

// Resume should be called after Reading() is signaled.
// The err value will be returned by the paused Read() call.
func (r *ResumableReader) Resume(err error) {
	r.resume <- err
}

func (r *ResumableReader) Read(p []byte) (int, error) {
	r.reading <- struct{}{}
	if err := <-r.resume; err != nil {
		return 0, err
	}
	return 0, io.EOF
}
