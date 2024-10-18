package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/internal/testingutil"
	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

func TestStore_Open(t *testing.T) {
	t.Run("ErrRemoteStorageClientRequired", func(t *testing.T) {
		s := newStore(t, t.TempDir())
		// t.Cleanup(func() { _ = s.Close() })
		s.RemoteClient = nil
		if err := s.Open(context.TODO()); err == nil || err.Error() != `remote storage client required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrCompactionLevelsRequired", func(t *testing.T) {
		s := newStore(t, t.TempDir())
		// t.Cleanup(func() { _ = s.Close() })
		s.Levels = nil
		if err := s.Open(context.TODO()); err == nil || err.Error() != `at least one compaction level is required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrCompactionLevelsOutOfOrder", func(t *testing.T) {
		s := newStore(t, t.TempDir())
		// t.Cleanup(func() { _ = s.Close() })
		s.Levels = []*store.CompactionLevel{{Level: 5}}
		if err := s.Open(context.TODO()); err == nil || err.Error() != `compaction level number out of order: 5, expected 0` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrCompactionLevelTooHigh", func(t *testing.T) {
		s := newStore(t, t.TempDir())
		// t.Cleanup(func() { _ = s.Close() })
		s.Levels = []*store.CompactionLevel{
			{Level: 0},
			{Level: 1, Interval: 1 * time.Second},
			{Level: 2, Interval: 2 * time.Second},
			{Level: 3, Interval: 3 * time.Second},
			{Level: 4, Interval: 4 * time.Second},
			{Level: 5, Interval: 5 * time.Second},
			{Level: 6, Interval: 6 * time.Second},
			{Level: 7, Interval: 7 * time.Second},
			{Level: 8, Interval: 8 * time.Second},
			{Level: 9, Interval: 9 * time.Second},
		}
		if err := s.Open(context.TODO()); err == nil || err.Error() != `compaction level cannot exceed 8` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrCompactionIntervalLevelZero", func(t *testing.T) {
		s := newStore(t, t.TempDir())
		// t.Cleanup(func() { _ = s.Close() })
		s.Levels = []*store.CompactionLevel{{Level: 0, Interval: 1 * time.Second}}
		if err := s.Open(context.TODO()); err == nil || err.Error() != `cannot set interval on compaction level zero` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestStore_WriteTx(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatalf("error: %#v", err)
		}

		db, err := s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 1, PostApplyChecksum: 0xeb1a999231044ddd}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		}

		// Write a second transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xc6e57aa102377eee},
		}), nil); err != nil {
			t.Fatal(err)
		}

		db, err = s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 2, PostApplyChecksum: 0xc6e57aa102377eee}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Write a transaction that resizes the database to zero pages.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 0, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag},
		}), nil); err != nil {
			t.Fatal(err)
		}

		db, err := s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 2, PostApplyChecksum: ltx.ChecksumFlag}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		} else if got, want := db.Commit, uint32(0); got != want {
			t.Fatalf("Commit=%v, want %v", got, want)
		}

		// Start a new database afterward.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 3, MaxTXID: 3, PreApplyChecksum: ltx.ChecksumFlag},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("3"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("3"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x80d21cd000000000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if db, err := s.FindDBByName(context.Background(), "bkt", "db"); err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 3, PostApplyChecksum: 0x80d21cd000000000}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		} else if got, want := db.Commit, uint32(2); got != want {
			t.Fatalf("Commit=%v, want %v", got, want)
		}
	})

	t.Run("Multibatch", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		b := testingutil.NewStatefulBlob(0, 4096)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 10<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, ltx.NewPos(1, 0xe05f7d3b740a45a3); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}
	})

	t.Run("Multiwrite", func(t *testing.T) {
		// This test used to check that two concurrent writes would block on each other,
		// after a timeout, but now we check that one fails.
		s := newOpenStore(t, t.TempDir())

		b := testingutil.NewStatefulBlob(0, 4096)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 1<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}

		// First transaction starts but will take too long to execute.
		b0 := b.Clone()
		rr := testingutil.NewResumableReader()
		var g errgroup.Group
		g.Go(func() error {
			data, _ := io.ReadAll(b0.Extend(t, 10<<20))
			r := io.MultiReader(bytes.NewReader(data[:len(data)/2]), rr, bytes.NewReader(data[len(data)/2:]))

			_, err := s.WriteTx(context.Background(), "bkt", "db", r, nil)
			posErr := &ltx.PosMismatchError{}
			if err == nil || !errors.As(err, &posErr) {
				return fmt.Errorf("unexpected error on first txn: %w", err)
			}
			return nil
		})

		// Second transaction should wait until the timeout is exceeded before writing.
		b1 := b.Clone()
		g.Go(func() error {
			// Wait for the timeout to be exceeded
			<-rr.Reading()
			time.Sleep(time.Second)
			defer rr.Resume(nil)

			pos, err := s.WriteTx(context.Background(), "bkt", "db", b1.Extend(t, 11<<20), nil)
			if err != nil {
				return err
			} else if got, want := pos, b1.Pos(); got != want {
				return fmt.Errorf("second txn pos=%s, want %v", got, want)
			}
			return nil
		})

		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}

		// A third transaction should be able to run after the successful second one.
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b1.Extend(t, 20<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b1.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}
	})

	t.Run("Multibatch/ContinueAfterTimeout", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		b := testingutil.NewStatefulBlob(0, 4096)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 1<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}

		// Execute a transaction that exceeds the write timeout between batches.
		// Since there are no competing transactions, this should still succeed.
		data, _ := io.ReadAll(b.Extend(t, 10<<20))
		rr := testingutil.NewResumableReader()
		r := io.MultiReader(bytes.NewReader(data[:len(data)/2]), rr, bytes.NewReader(data[len(data)/2:]))

		go func() { <-rr.Reading(); time.Sleep(time.Second); rr.Resume(nil) }()

		if pos, err := s.WriteTx(context.Background(), "bkt", "db", r, nil); err != nil {
			t.Fatalf("unexpected error on first txn: %s", err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %s", got, want)
		}

		// Ensure next transaction works fine.
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 11<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}
	})

	t.Run("Multibatch/PartialLTX", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		b := testingutil.NewStatefulBlob(0, 4096)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 1<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}

		// First write should be unsuccessful.
		b1 := b.Clone()
		data, _ := io.ReadAll(b1.Extend(t, 10<<20))
		r := bytes.NewReader(data[:len(data)-1]) // shave off last byte

		if _, err := s.WriteTx(context.Background(), "bkt", "db", r, nil); err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("unexpected error: %s", err)
		}

		// Ensure next transaction works fine after previous transaction expires.
		time.Sleep(time.Second)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 11<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}
	})

	t.Run("Multibatch/Compaction", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		s.Levels[0].Retention = 1 * time.Millisecond

		b := testingutil.NewStatefulBlob(0, 4096)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 1<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}

		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 2<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}
		lastCommittedPos := b.Pos()

		// Execute a multibatch transaction but perform a compaction in the middle.
		rr := testingutil.NewResumableReader()
		var g errgroup.Group
		g.Go(func() error {
			data, _ := io.ReadAll(b.Extend(t, 10<<20))
			r := io.MultiReader(bytes.NewReader(data[:len(data)/2]), rr, bytes.NewReader(data[len(data)/2:]))
			if _, err := s.WriteTx(context.Background(), "bkt", "db", r, nil); err != nil {
				return fmt.Errorf("unexpected error: %s", err)
			}
			return nil
		})

		// Wait until first batch has been inserted.
		<-rr.Reading()
		time.Sleep(1 * time.Second)

		var once sync.Once
		defer once.Do(func() { rr.Resume(nil) })

		// Run compaction and ensure that we are not using the partial transaction.
		s.WithTestTx(t, func(tx *sql.Tx) {
			path, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", 1)
			if err != nil {
				t.Fatal(err)
			} else if got, want := path.MaxTXID, lastCommittedPos.TXID; got != want {
				t.Fatalf("TXID=%s, want %s", got, want)
			} else if got, want := path.Metadata.PostApplyChecksum, lastCommittedPos.PostApplyChecksum; got != want {
				t.Fatalf("PostApplyChecksum=%016x, want %016x", got, want)
			}
		})

		// Resume write
		once.Do(func() { rr.Resume(nil) })
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)

		// Compact again and ensure last txn is included.
		s.WithTestTx(t, func(tx *sql.Tx) {
			path, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", 1)
			if err != nil {
				t.Fatal(err)
			} else if got, want := path.MaxTXID, b.Pos().TXID; got != want {
				t.Fatalf("TXID=%s, want %s", got, want)
			} else if got, want := path.Metadata.PostApplyChecksum, b.Pos().PostApplyChecksum; got != want {
				t.Fatalf("PostApplyChecksum=%016x, want %016x", got, want)
			}
		})
	})

	t.Run("ErrPosMismatch", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Next transaction should fail if its state doesn't line up with the previous LTX file.
		var pmErr *ltx.PosMismatchError
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 1001},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2000},
		}), nil); !errors.As(err, &pmErr) {
			t.Fatalf("expected PosMismatchError, got %#v", err)
		} else if got, want := *pmErr, (ltx.PosMismatchError{
			Pos: ltx.Pos{TXID: 1, PostApplyChecksum: 0xeb1a999231044ddd},
		}); got != want {
			t.Fatalf("err=%#v, want %#v", got, want)
		}

		// Position should still be at the original position.
		db, err := s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 1, PostApplyChecksum: 0xeb1a999231044ddd}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		}
	})

	t.Run("ErrEmptyReader", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		if _, err := s.WriteTx(context.Background(), "bkt", "db", strings.NewReader(""), nil); err == nil || err.Error() != `decode header: EOF` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrBadHeader", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		if _, err := s.WriteTx(context.Background(), "bkt", "db", strings.NewReader(strings.Repeat("X", 100)), nil); err == nil || err.Error() != `decode header: unmarshal header: invalid LTX file` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestStore_FindDBsByCluster(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write to first database.
		if _, err := s.WriteTx(context.Background(), "bkt", "db0", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := s.WriteTx(context.Background(), "bkt", "db0", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xc6e57aa102377eee},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Write to second database.
		if _, err := s.WriteTx(context.Background(), "bkt", "db1", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 5},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Write to a different cluster.
		if _, err := s.WriteTx(context.Background(), "other", "db3", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 5},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		dbs, err := s.FindDBsByCluster(context.Background(), "bkt")
		if err != nil {
			t.Fatal(err)
		} else if got, want := len(dbs), 2; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}

		if got, want := dbs[0], (&store.DB{
			ID:                1,
			Cluster:           "bkt",
			Name:              "db0",
			HWM:               0,
			TXID:              2,
			PostApplyChecksum: 0xc6e57aa102377eee,
			PageSize:          512,
			Commit:            1,
			Timestamp:         time.Unix(0, 0).UTC(),
		}); !reflect.DeepEqual(got, want) {
			t.Fatalf("db[0]=%#v, want %#v", got, want)
		}

		if got, want := dbs[1], (&store.DB{
			ID:                2,
			Cluster:           "bkt",
			Name:              "db1",
			HWM:               0,
			TXID:              5,
			PostApplyChecksum: 0xeb1a999231044ddd,
			PageSize:          0x200,
			Commit:            0x1,
			Timestamp:         time.Unix(0, 0).UTC(),
		}); !reflect.DeepEqual(got, want) {
			t.Fatalf("db[1]=%#v, want %#v", got, want)
		}
	})

	t.Run("Empty", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		if dbs, err := s.FindDBsByCluster(context.Background(), "bkt"); err != nil {
			t.Fatal(err)
		} else if got, want := len(dbs), 0; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}
	})
}

func TestStore_FindClusters(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		if _, err := s.WriteTx(context.Background(), "bkt", "db1", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 5},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "bkt", "db2", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 10},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "bkt2", "db3", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 10},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		clusters, err := s.FindClusters(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := len(clusters), 2; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		} else if got, want := clusters[0], ("bkt"); got != want {
			t.Fatalf("[0]=%#v, want %#v", got, want)
		} else if got, want := clusters[1], ("bkt2"); got != want {
			t.Fatalf("[1]=%#v, want %#v", got, want)
		}
	})
}

func TestStore_FindDBByName(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		if _, err := s.WriteTx(context.Background(), "bkt", "db1", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 5},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		db, err := s.FindDBByName(context.Background(), "bkt", "db1")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db, (&store.DB{
			ID:                1,
			Cluster:           "bkt",
			Name:              "db1",
			HWM:               0x0,
			TXID:              0x5,
			PostApplyChecksum: 0xeb1a999231044ddd,
			PageSize:          0x200,
			Commit:            0x1,
			Timestamp:         time.Unix(0, 0).UTC(),
		}); !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%#v, want %#v", got, want)
		}
	})

	t.Run("ErrDatabaseNotFound", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		if _, err := s.FindDBByName(context.Background(), "bkt", "db"); err != lfsb.ErrDatabaseNotFound {
			t.Fatal(err)
		}
	})
}

func TestStore_WriteSnapshotTo(t *testing.T) {
	t.Run("Local", func(t *testing.T) {
		s := newOpenStore(t, filepath.Join(t.TempDir(), "00"))

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 5},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("5"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x80d21cd000000000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if db, err := s.FindDBByName(context.Background(), "cl", "db"); err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 5, PostApplyChecksum: 0x80d21cd000000000}); got != want {
			t.Fatalf("pos=%s, want %s", got, want)
		}

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 6, MaxTXID: 6, PreApplyChecksum: 0x80d21cd000000000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("6"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if db, err := s.FindDBByName(context.Background(), "cl", "db"); err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 6, PostApplyChecksum: 0xad2dffe333333333}); got != want {
			t.Fatalf("pos=%s, want %s", got, want)
		}

		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "cl", "db", 6, &buf); err != nil {
			t.Fatal(err)
		}

		var spec ltx.FileSpec
		if _, err := spec.ReadFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		compareFileSpec(t, &spec, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 6},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("6"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333, FileChecksum: 0xbb0e2065ff682755},
		})

		if err := ltx.NewDecoder(&buf).Verify(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Local/ZeroCommit", func(t *testing.T) {
		s := newOpenStore(t, filepath.Join(t.TempDir(), "00"))

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 5},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("5"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x80d21cd000000000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 0, MinTXID: 6, MaxTXID: 6, PreApplyChecksum: 0x80d21cd000000000},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag},
		}), nil); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "cl", "db", 6, &buf); err != nil {
			t.Fatal(err)
		}

		var spec ltx.FileSpec
		if _, err := spec.ReadFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		compareFileSpec(t, &spec, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 0, MinTXID: 1, MaxTXID: 6},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag, FileChecksum: 0x8553be86dc216640},
		})

		if err := ltx.NewDecoder(&buf).Verify(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Remote", func(t *testing.T) {
		s := newOpenStore(t, filepath.Join(t.TempDir(), "00"))
		s.Levels[0].Retention = 1 * time.Millisecond

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 5},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("5"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x80d21cd000000000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 6, MaxTXID: 6, PreApplyChecksum: 0x80d21cd000000000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("6"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Wait for retention & compact.
		time.Sleep(s.Levels[0].Retention)
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "cl", "db", 1); err != nil {
				t.Fatal(err)
			}
		})

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 7, MaxTXID: 7, PreApplyChecksum: 0xad2dffe333333333},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("7"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xb0e55ff457622bbb},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Wait for retention & compact.
		time.Sleep(s.Levels[0].Retention)
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "cl", "db", 1); err != nil {
				t.Fatal(err)
			}
		})

		// The snapshot should be pulled from remote storage since TXID 6 was compacted away to L1.
		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "cl", "db", 6, &buf); err != nil {
			t.Fatal(err)
		}

		var spec ltx.FileSpec
		if _, err := spec.ReadFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		compareFileSpec(t, &spec, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 6},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("6"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333, FileChecksum: 0xbb0e2065ff682755},
		})

		if err := ltx.NewDecoder(&buf).Verify(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Remote/ZeroCommit", func(t *testing.T) {
		s := newOpenStore(t, filepath.Join(t.TempDir(), "00"))
		s.Levels[0].Retention = 1 * time.Millisecond

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 5},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("5"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x80d21cd000000000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 0, MinTXID: 6, MaxTXID: 6, PreApplyChecksum: 0x80d21cd000000000},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Wait for retention & compact.
		time.Sleep(s.Levels[0].Retention)
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "cl", "db", 1); err != nil {
				t.Fatal(err)
			}
		})

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 7, MaxTXID: 7, PreApplyChecksum: ltx.ChecksumFlag},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("7"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xb0e55ff457622bbb},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Wait for retention & compact.
		time.Sleep(s.Levels[0].Retention)
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "cl", "db", 1); err != nil {
				t.Fatal(err)
			}
		})

		// The snapshot should be pulled from remote storage since TXID 6 was compacted away to L1.
		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "cl", "db", 6, &buf); err != nil {
			t.Fatal(err)
		}

		var spec ltx.FileSpec
		if _, err := spec.ReadFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		compareFileSpec(t, &spec, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 0, MinTXID: 1, MaxTXID: 6},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag, FileChecksum: 0x8553be86dc216640},
		})

		if err := ltx.NewDecoder(&buf).Verify(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure that requesting a snapshot ahead of the TXID returns an error.
	t.Run("ErrTXIDAhead", func(t *testing.T) {
		s := newOpenStore(t, filepath.Join(t.TempDir(), "00"))

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 5},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("5"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x80d21cd000000000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "cl", "db", 6, &buf); err != lfsb.ErrTxNotAvailable {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	// Ensure that requesting a snapshot within a TXID range returns an error.
	t.Run("ErrTxNotAvailable", func(t *testing.T) {
		s := newOpenStore(t, filepath.Join(t.TempDir(), "00"))
		s.Levels[0].Retention = 1 * time.Millisecond

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 5},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("5"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("5"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x80d21cd000000000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "cl", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 6, MaxTXID: 6, PreApplyChecksum: 0x80d21cd000000000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("6"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Wait for retention & compact.
		time.Sleep(s.Levels[0].Retention)
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "cl", "db", 1); err != nil {
				t.Fatal(err)
			}
		})

		// The compacted file in remote storage has a range of [1-6] so 5 is not found.
		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "cl", "db", 5, &buf); err != lfsb.ErrTxNotAvailable {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestStore_WriteDatabaseTo(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write a transaction.
		spec1 := &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, spec1), nil); err != nil {
			t.Fatal(err)
		}

		// Write another transaction.
		spec2 := &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, spec2), nil); err != nil {
			t.Fatal(err)
		}

		// Read it back as a DB dump.
		var buf bytes.Buffer
		if err := s.WriteDatabaseTo(context.Background(), "bkt", "db", 2, &buf); err != nil {
			t.Fatal(err)
		}

		if got, want := buf.Bytes(), append(bytes.Repeat([]byte("1"), 512), bytes.Repeat([]byte("2"), 512)...); !bytes.Equal(got, want) {
			t.Fatalf("data mismatch:\ngot:  %#v\nwant: %#v", got, want)
		}
	})

	t.Run("ZeroCommit", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 0, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Read it back as a DB dump.
		var buf bytes.Buffer
		if err := s.WriteDatabaseTo(context.Background(), "bkt", "db", 2, &buf); err != nil {
			t.Fatal(err)
		}
		if got, want := buf.Len(), 0; got != want {
			t.Fatalf("len=%d, want %d", got, want)
		}
	})
}

func TestStore_RestoreToTx(t *testing.T) {
	t.Run("SameSize", func(t *testing.T) {
		ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
		s := newOpenStore(t, t.TempDir(),
			func(s *store.Store) {
				s.Now = func() time.Time {
					return ts
				}
			},
		)

		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Write a second transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xc6e57aa102377eee},
		}), nil); err != nil {
			t.Fatal(err)
		}

		txid, err := s.RestoreToTx(context.Background(), "bkt", "db", 1)
		if err != nil {
			t.Fatal(err)
		}
		if want, got := ltx.TXID(3), txid; want != got {
			t.Fatalf("unexpected txid, want = %v, got = %v", want, got)
		}

		db, err := s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 3, PostApplyChecksum: 0xeb1a999231044ddd}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		}

		// Read it back as a snapshot.
		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "bkt", "db", 3, &buf); err != nil {
			t.Fatal(err)
		}

		// Unmarshal to a spec and compare.
		var other ltx.FileSpec
		if _, err := other.ReadFrom(&buf); err != nil {
			t.Fatal(err)
		}

		compareFileSpec(t, &other, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 3, Timestamp: ts.UnixMilli()},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd, FileChecksum: 0xd3745d48bef1677c},
		})
	})

	t.Run("SourceShorter", func(t *testing.T) {
		ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
		s := newOpenStore(t, t.TempDir(),
			func(s *store.Store) {
				s.Now = func() time.Time {
					return ts
				}
			},
		)

		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Write a second transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}), nil); err != nil {
			t.Fatal(err)
		}

		txid, err := s.RestoreToTx(context.Background(), "bkt", "db", 1)
		if err != nil {
			t.Fatal(err)
		}
		if want, got := ltx.TXID(3), txid; want != got {
			t.Fatalf("unexpected txid, want = %v, got = %v", want, got)
		}

		db, err := s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 3, PostApplyChecksum: 0xeb1a999231044ddd}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		}

		// Read it back as a snapshot.
		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "bkt", "db", 3, &buf); err != nil {
			t.Fatal(err)
		}

		// Unmarshal to a spec and compare.
		var other ltx.FileSpec
		if _, err := other.ReadFrom(&buf); err != nil {
			t.Fatal(err)
		}

		compareFileSpec(t, &other, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 3, Timestamp: ts.UnixMilli()},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd, FileChecksum: 0xd3745d48bef1677c},
		})
	})

	t.Run("CurrentShorter", func(t *testing.T) {
		ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
		s := newOpenStore(t, t.TempDir(),
			func(s *store.Store) {
				s.Now = func() time.Time {
					return ts
				}
			},
		)

		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 1},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Write a second transaction that shrinks the DB.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xad2dffe333333333},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("3"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xcdb0244fecd99000},
		}), nil); err != nil {
			t.Fatal(err)
		}

		txid, err := s.RestoreToTx(context.Background(), "bkt", "db", 1)
		if err != nil {
			t.Fatal(err)
		}
		if want, got := ltx.TXID(3), txid; want != got {
			t.Fatalf("unexpected txid, want = %v, got = %v", want, got)
		}

		db, err := s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 3, PostApplyChecksum: 0xad2dffe333333333}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		}

		// Read it back as a snapshot.
		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "bkt", "db", 3, &buf); err != nil {
			t.Fatal(err)
		}

		// Unmarshal to a spec and compare.
		var other ltx.FileSpec
		if _, err := other.ReadFrom(&buf); err != nil {
			t.Fatal(err)
		}

		compareFileSpec(t, &other, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 3, Timestamp: ts.UnixMilli()},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333, FileChecksum: 0xbda6246263241293},
		})
	})

	t.Run("LastTXID", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Write a second transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}), nil); err != nil {
			t.Fatal(err)
		}

		txid, err := s.RestoreToTx(context.Background(), "bkt", "db", 2)
		if err != nil {
			t.Fatal(err)
		}
		if want, got := ltx.TXID(2), txid; want != got {
			t.Fatalf("unexpected txid, want = %v, got = %v", want, got)
		}

		db, err := s.FindDBByName(context.Background(), "bkt", "db")
		if err != nil {
			t.Fatal(err)
		} else if got, want := db.Pos(), (ltx.Pos{TXID: 2, PostApplyChecksum: 0xad2dffe333333333}); got != want {
			t.Fatalf("Pos=%s, want %s", got, want)
		}

		// Read it back as a snapshot.
		var buf bytes.Buffer
		if err := s.WriteSnapshotTo(context.Background(), "bkt", "db", 2, &buf); err != nil {
			t.Fatal(err)
		}

		// Unmarshal to a spec and compare.
		var other ltx.FileSpec
		if _, err := other.ReadFrom(&buf); err != nil {
			t.Fatal(err)
		}

		compareFileSpec(t, &other, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 2},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333, FileChecksum: 0xb140bf35427d1cff},
		})
	})

	t.Run("Large", func(t *testing.T) {
		t.Run("Shrink", func(t *testing.T) {
			if testing.Short() {
				t.Skip("long running test, skipping in short mode")
			}

			tx1Path := filepath.Join("testdata", "1500mb.sqlite3")
			testingutil.GenerateDBIfNotExists(t, tx1Path, 1500<<20)
			tx2Path := filepath.Join("testdata", "500mb.sqlite3")
			testingutil.GenerateDBIfNotExists(t, tx2Path, 500<<20)

			ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
			s := newOpenStore(t, t.TempDir(),
				func(s *store.Store) {
					s.Now = func() time.Time {
						return ts
					}
				},
			)

			tx1Rdr, err := os.Open(tx1Path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx1Rdr.Close() }()

			t.Log("storing first tx")
			if _, err := s.StoreDatabase(context.Background(), "bkt", "db", tx1Rdr); err != nil {
				t.Fatal(err)
			}

			tx2Rdr, err := os.Open(tx2Path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx2Rdr.Close() }()

			t.Log("storing second tx")
			if _, err := s.StoreDatabase(context.Background(), "bkt", "db", tx2Rdr); err != nil {
				t.Fatal(err)
			}

			t.Log("restoring to first tx")
			txid, err := s.RestoreToTx(context.Background(), "bkt", "db", 1)
			if err != nil {
				t.Fatal(err)
			}
			if want, got := ltx.TXID(3), txid; want != got {
				t.Fatalf("unexpected txid, want = %v, got = %v", want, got)
			}
		})
	})

	t.Run("ErrWrongTXID", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		_, err := s.RestoreToTx(context.Background(), "bkt", "db", 2)
		if want, got := lfsb.ErrTxNotAvailable, err; want != got {
			t.Fatalf("unexpected err, want = %v, got = %v", want, got)
		}
	})
}

func TestStore_StoreDatabase(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		data := makeSQLiteDB(t, 4096, 10)

		pos, err := s.StoreDatabase(context.Background(), "bkt", "db", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		} else if want, got := ltx.TXID(1), pos.TXID; want != got {
			t.Fatalf("unexpected TX ID, want = %v, got = %v", want, got)
		}

		// Read it back as a DB dump.
		var buf bytes.Buffer
		if err := s.WriteDatabaseTo(context.Background(), "bkt", "db", pos.TXID, &buf); err != nil {
			t.Fatal(err)
		}

		if want, got := data, buf.Bytes(); !bytes.Equal(want, got) {
			t.Fatalf("data mismatch:\ngot:  %#v\nwant: %#v", got, want)
		}
	})

	t.Run("Existing", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 4096)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xba568a48153a6705},
		}), nil); err != nil {
			t.Fatal(err)
		}

		data := makeSQLiteDB(t, 4096, 10)
		pos, err := s.StoreDatabase(context.Background(), "bkt", "db", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		} else if want, got := ltx.TXID(2), pos.TXID; want != got {
			t.Fatalf("unexpected TX ID, want = %v, got = %v", want, got)
		}

		// Read it back as a DB dump.
		var buf bytes.Buffer
		if err := s.WriteDatabaseTo(context.Background(), "bkt", "db", pos.TXID, &buf); err != nil {
			t.Fatal(err)
		}

		if want, got := data, buf.Bytes(); !bytes.Equal(want, got) {
			t.Fatalf("data mismatch:\ngot:  %#v\nwant: %#v", got, want)
		}
	})

	t.Run("Multibatch", func(t *testing.T) {
		path := filepath.Join("testdata", "10mb.sqlite3")
		testingutil.GenerateDBIfNotExists(t, path, 10<<20)

		ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
		s := newOpenStore(t, t.TempDir(),
			func(s *store.Store) {
				s.Now = func() time.Time {
					return ts
				}
			},
		)

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}

		pos, err := s.StoreDatabase(context.Background(), "bkt", "db", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		// Note that the original lfsc test thought this checksum should be 0xdf469dbe97710e58.
		// I tested this behavior on production litefs.fly.io and got the current response as well,
		// 0xccab895e14ae1f66.
		if got, want := pos, ltx.NewPos(1, 0xccab895e14ae1f66); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}
	})
	t.Run("ErrPageSizeMismatch", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		data := makeSQLiteDB(t, 4096, 10)

		pos, err := s.StoreDatabase(context.Background(), "bkt", "db", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		} else if want, got := ltx.TXID(1), pos.TXID; want != got {
			t.Fatalf("unexpected TX ID, want = %v, got = %v", want, got)
		}

		data = makeSQLiteDB(t, 512, 10)

		_, err = s.StoreDatabase(context.Background(), "bkt", "db", bytes.NewReader(data))
		if want, got := lfsb.ErrPageSizeMismatch, err; !errors.Is(got, want) {
			t.Fatalf("unexpected error, want = %v, got = %v", want, got)
		}
	})
}

func TestStore_FindTXIDByTimestamp(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: ts.UnixMilli()},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}
		compactUpToLevel(t, s, "bkt", "db", store.CompactionLevelRestoreTarget)

		// Write a second transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, Timestamp: ts.Add(5 * time.Minute).UnixMilli(), PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xc6e57aa102377eee},
		}), nil); err != nil {
			t.Fatal(err)
		}
		compactUpToLevel(t, s, "bkt", "db", store.CompactionLevelRestoreTarget)

		txid, err := s.FindTXIDByTimestamp(context.Background(), "bkt", "db", ts.Add(1*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if want, got := ltx.TXID(1), txid; want != got {
			t.Fatalf("unexpected txid, want = %v, got = %v", want, got)
		}
	})

	t.Run("Level3", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: ts.UnixMilli()},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}
		compactUpToLevel(t, s, "bkt", "db", store.CompactionLevelRestoreTarget+1)

		// Write a second transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, Timestamp: ts.Add(5 * time.Minute).UnixMilli(), PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xc6e57aa102377eee},
		}), nil); err != nil {
			t.Fatal(err)
		}
		compactUpToLevel(t, s, "bkt", "db", store.CompactionLevelRestoreTarget)

		txid, err := s.FindTXIDByTimestamp(context.Background(), "bkt", "db", ts.Add(1*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if want, got := ltx.TXID(1), txid; want != got {
			t.Fatalf("unexpected txid, want = %v, got = %v", want, got)
		}
	})

	t.Run("ErrNoTimestamp", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		ts := time.Date(2023, time.June, 15, 14, 0, 0, 0, time.UTC)
		// Write initial transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: ts.UnixMilli()},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}
		compactUpToLevel(t, s, "bkt", "db", store.CompactionLevelRestoreTarget)

		// Write a second transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, Timestamp: ts.Add(5 * time.Minute).UnixMilli(), PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xc6e57aa102377eee},
		}), nil); err != nil {
			t.Fatal(err)
		}
		compactUpToLevel(t, s, "bkt", "db", store.CompactionLevelRestoreTarget-1)

		_, err := s.FindTXIDByTimestamp(context.Background(), "bkt", "db", ts.Add(-1*time.Minute))
		if want, got := lfsb.ErrTimestampNotAvailable, err; want != got {
			t.Fatalf("unexpected err, want = %v, got = %v", want, got)
		}
	})
}

func TestStore_CompactToSnapshot(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write a transaction.
		spec1 := &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, spec1), nil); err != nil {
			t.Fatal(err)
		}

		// Compact up each of the levels
		for _, lvl := range s.Levels[1:] {
			s.WithTestTx(t, func(tx *sql.Tx) {
				path, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lvl.Level)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("compacted to: %s", path)
			})
		}

		// Compact to snapshot.

		s.WithTestTx(t, func(tx *sql.Tx) {
			path, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lfsb.CompactionLevelSnapshot)
			if err != nil {
				t.Fatal(err)
			} else if got, want := path, (store.StoragePath{
				Cluster:  "bkt",
				Database: "db",
				Level:    9,
				MinTXID:  1,
				MaxTXID:  1,
				Metadata: store.StorageMetadata{
					PageSize:          512,
					Commit:            1,
					Timestamp:         time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
					PreApplyChecksum:  0,
					PostApplyChecksum: 0xeb1a999231044ddd,
				},
			}); got != want {
				t.Fatalf("path=%#v, want %#v", got, want)
			}
		})

		// Verify that HWM is updated.
		if db, err := s.FindDBByName(context.Background(), "bkt", "db"); err != nil {
			t.Fatal(err)
		} else if got, want := db.HWM, ltx.TXID(1); got != want {
			t.Fatalf("HWM=%s, want %s", got, want)
		}

		// Write another transaction.
		spec2 := &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, spec2), nil); err != nil {
			t.Fatal(err)
		}

		// Compact up each of the levels
		for _, lvl := range s.Levels[1:] {
			s.WithTestTx(t, func(tx *sql.Tx) {
				path, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lvl.Level)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("compacted to: %s", path)
			})
		}

		// Compact to snapshot again.
		s.WithTestTx(t, func(tx *sql.Tx) {
			path, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lfsb.CompactionLevelSnapshot)
			if err != nil {
				t.Fatal(err)
			} else if got, want := path, (store.StoragePath{
				Cluster:  "bkt",
				Database: "db",
				Level:    9,
				MinTXID:  1,
				MaxTXID:  2,
				Metadata: store.StorageMetadata{
					PageSize:          512,
					Commit:            2,
					Timestamp:         time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
					PreApplyChecksum:  0,
					PostApplyChecksum: 0xad2dffe333333333,
				},
			}); got != want {
				t.Fatalf("path=%#v, want %#v", got, want)
			}

			// Ensure compacted file exists and is correct.
			var other ltx.FileSpec
			if f, err := s.RemoteClient.OpenFile(context.Background(), path); err != nil {
				t.Fatal(err)
			} else if _, err := other.ReadFrom(f); err != nil {
				t.Fatal(err)
			} else if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			compareFileSpec(t, &other, &ltx.FileSpec{
				Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 2},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
					{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)},
				},
				Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333, FileChecksum: 0xb140bf35427d1cff},
			})
		})
	})

	t.Run("ErrNoCompaction/NoData", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		s.WithTestTx(t, func(tx *sql.Tx) {
			_, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lfsb.CompactionLevelSnapshot)
			if lfsb.ErrorCode(err) != lfsb.ENOCOMPACTION {
				t.Fatal(err)
			}
		})
	})

	t.Run("ErrNoCompaction/NoNewData", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		// Write a transaction.
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t,
			&ltx.FileSpec{
				Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
				Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
			},
		), nil); err != nil {
			t.Fatal(err)
		}

		// Compact up each of the levels
		for _, lvl := range s.Levels[1:] {
			s.WithTestTx(t, func(tx *sql.Tx) {
				if _, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lvl.Level); err != nil {
					t.Fatal(err)
				}
			})
		}

		// Compact to snapshot.
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lfsb.CompactionLevelSnapshot); err != nil {
				t.Fatal(err)
			}
		})

		// Compact up each of the levels
		for _, lvl := range s.Levels[1:] {
			s.WithTestTx(t, func(tx *sql.Tx) {
				if _, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lvl.Level); lfsb.ErrorCode(err) != lfsb.ENOCOMPACTION {
					t.Fatal(err)
				}
			})
		}

		// Compacting again without new data should return a marker error.
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lfsb.CompactionLevelSnapshot); lfsb.ErrorCode(err) != lfsb.ENOCOMPACTION {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	})
}

func TestStore_ProcessCompactions(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		spec1 := &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, NodeID: 123},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}
		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, spec1), nil); err != nil {
			t.Fatal(err)
		}

		if err := s.ProcessCompactions(context.TODO(), 1); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStore_DeleteCluster(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		remoteClient := s.RemoteClient

		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, NodeID: 123},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "bkt", "db", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xeb1a999231044ddd, NodeID: 456},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xad2dffe333333333},
		}), nil); err != nil {
			t.Fatal(err)
		}

		if _, err := s.WriteTx(context.Background(), "bkt", "db2", ltxSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, NodeID: 123},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb1a999231044ddd},
		}), nil); err != nil {
			t.Fatal(err)
		}

		// Compact to all levels & snapshot.
		for _, lvl := range s.Levels[1:] {
			s.WithTestTx(t, func(tx *sql.Tx) {
				if _, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lvl.Level); err != nil {
					t.Fatal(err)
				}
			})
		}
		s.WithTestTx(t, func(tx *sql.Tx) {
			if _, err := s.CompactDBToLevel(context.Background(), tx, "bkt", "db", lfsb.CompactionLevelSnapshot); err != nil {
				t.Fatal(err)
			}
		})

		// Delete cluster.
		if err := s.DeleteCluster(context.Background(), "bkt"); err != nil {
			t.Fatal(err)
		}

		// Ensure the cluster cannot be found locally.
		if a, err := s.FindClusters(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := len(a), 0; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}

		// Ensure no files exist on remote storage.
		for _, lvl := range s.Levels {
			if paths, err := store.FindStoragePaths(context.Background(), s.RemoteClient, "bkt", "db", lvl.Level, nil); err != nil {
				t.Fatal(err)
			} else if len(paths) != 0 {
				t.Fatalf("unexpected paths found on level %d", lvl.Level)
			}
		}

		if paths, err := store.FindStoragePaths(context.Background(), s.RemoteClient, "bkt", "db", lfsb.CompactionLevelSnapshot, nil); err != nil {
			t.Fatal(err)
		} else if len(paths) != 0 {
			t.Fatal("unexpected snapshots found")
		}

		s = newStore(t, s.Path())
		s.RemoteClient = remoteClient
		if err := s.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		// t.Cleanup(func() { _ = s.Close() })

		if a, err := s.FindClusters(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := len(a), 0; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}
	})
}
