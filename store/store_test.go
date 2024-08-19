package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stephen/litefs-backup/internal/testingutil"
	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

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

	t.Run("Multibatch/ErrTxPending", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())

		b := testingutil.NewStatefulBlob(0, 4096)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 10<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, ltx.NewPos(1, 0xe05f7d3b740a45a3); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}

		rr := testingutil.NewResumableReader()
		var g errgroup.Group

		// One multibatch transaction is written but we pause it in the middle.
		b0 := b.Clone()
		g.Go(func() error {
			data, _ := io.ReadAll(b0.Extend(t, 20<<20))
			r := io.MultiReader(
				bytes.NewReader(data[:len(data)/2]),
				rr,
				bytes.NewReader(data[len(data)/2:]),
			)

			pos, err := s.WriteTx(context.Background(), "bkt", "db", r, nil)
			if err != nil {
				return err
			} else if got, want := pos, ltx.NewPos(2, 0xec8959317e958c80); got != want {
				return fmt.Errorf("first txn pos=%s, want %v", got, want)
			}

			return nil
		})

		// Another transaction starts while we're in the middle of the previous tx.
		// This one should error out as a conflict.
		b1 := b.Clone()
		g.Go(func() error {
			<-rr.Reading()

			_, err := s.WriteTx(context.Background(), "bkt", "db", b1.Extend(t, 21<<20), nil)
			rr.Resume(nil)
			if err == nil || err.Error() != `write batch 0: conflict (ETXPENDING): cannot write while transaction in progress` {
				return fmt.Errorf("unexpected error on second tx: %w", err)
			}

			return nil
		})

		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multibatch/Timeout", func(t *testing.T) {
		s := newOpenStore(t, t.TempDir())
		s.WriteTxBatchTimeout = 1 * time.Second

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
			if err == nil || err.Error() != `write batch 1: conflict (ETXTIMEOUT): write transaction timed out` {
				return fmt.Errorf("unexpected error on first txn: %w", err)
			}
			return nil
		})

		// Second transaction should wait until the timeout is exceeded before writing.
		b1 := b.Clone()
		g.Go(func() error {
			// Wait for the timeout to be exceeded
			<-rr.Reading()
			time.Sleep(s.WriteTxBatchTimeout)
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
		s.WriteTxBatchTimeout = 1 * time.Second

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

		go func() { <-rr.Reading(); time.Sleep(s.WriteTxBatchTimeout); rr.Resume(nil) }()

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
		s.WriteTxBatchTimeout = 1 * time.Second

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

		if _, err := s.WriteTx(context.Background(), "bkt", "db", r, nil); err == nil || err.Error() != `write batch 2: close ltx decoder: unexpected EOF` {
			t.Fatalf("unexpected error: %s", err)
		}

		// Ensure next transaction works fine after previous transaction expires.
		time.Sleep(s.WriteTxBatchTimeout)
		if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 11<<20), nil); err != nil {
			t.Fatal(err)
		} else if got, want := pos, b.Pos(); got != want {
			t.Fatalf("pos=%s, want %v", got, want)
		}
	})

	// t.Run("Multibatch/Compaction", func(t *testing.T) {
	// 	s := newOpenStore(t, t.TempDir())
	// 	s.WriteTxBatchTimeout = 1 * time.Second
	// 	// s.Levels[0].Retention = 1 * time.Millisecond
	// 	// XXX

	// 	b := testingutil.NewStatefulBlob(0, 4096)
	// 	if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 1<<20), nil); err != nil {
	// 		t.Fatal(err)
	// 	} else if got, want := pos, b.Pos(); got != want {
	// 		t.Fatalf("pos=%s, want %v", got, want)
	// 	}

	// 	if pos, err := s.WriteTx(context.Background(), "bkt", "db", b.Extend(t, 2<<20), nil); err != nil {
	// 		t.Fatal(err)
	// 	} else if got, want := pos, b.Pos(); got != want {
	// 		t.Fatalf("pos=%s, want %v", got, want)
	// 	}
	// 	lastCommittedPos := b.Pos()

	// 	// Execute a multibatch transaction but perform a compaction in the middle.
	// 	rr := testingutil.NewResumableReader()
	// 	var g errgroup.Group
	// 	g.Go(func() error {
	// 		data, _ := io.ReadAll(b.Extend(t, 10<<20))
	// 		r := io.MultiReader(bytes.NewReader(data[:len(data)/2]), rr, bytes.NewReader(data[len(data)/2:]))
	// 		if _, err := s.WriteTx(context.Background(), "bkt", "db", r, nil); err != nil {
	// 			return fmt.Errorf("unexpected error: %s", err)
	// 		}
	// 		return nil
	// 	})

	// 	// Wait until first batch has been inserted.
	// 	<-rr.Reading()
	// 	time.Sleep(1 * time.Second)

	// 	var once sync.Once
	// 	defer once.Do(func() { rr.Resume(nil) })

	// 	// Run compaction and ensure that we are not using the partial transaction.
	// 	path, err := s.CompactDBToLevel(context.Background(), "bkt", "db", 1)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	} else if got, want := path.MaxTXID, lastCommittedPos.TXID; got != want {
	// 		t.Fatalf("TXID=%s, want %s", got, want)
	// 	} else if got, want := path.Metadata.PostApplyChecksum, lastCommittedPos.PostApplyChecksum; got != want {
	// 		t.Fatalf("PostApplyChecksum=%016x, want %016x", got, want)
	// 	}

	// 	// Resume write
	// 	once.Do(func() { rr.Resume(nil) })
	// 	if err := g.Wait(); err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	time.Sleep(time.Second)

	// 	// Compact again and ensure last txn is included.
	// 	path, err = s.CompactDBToLevel(context.Background(), "bkt", "db", 1)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	} else if got, want := path.MaxTXID, b.Pos().TXID; got != want {
	// 		t.Fatalf("TXID=%s, want %s", got, want)
	// 	} else if got, want := path.Metadata.PostApplyChecksum, b.Pos().PostApplyChecksum; got != want {
	// 		t.Fatalf("PostApplyChecksum=%016x, want %016x", got, want)
	// 	}
	// })

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
