package store_test

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/mock"
	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
)

func TestCalcSnapshotPlan(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var client mock.StorageClient
		client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
			switch level {
			case lfsb.CompactionLevelSnapshot:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: lfsb.CompactionLevelSnapshot, MinTXID: 1, MaxTXID: 10},
					{Level: lfsb.CompactionLevelSnapshot, MinTXID: 1, MaxTXID: 50},
					{Level: lfsb.CompactionLevelSnapshot, MinTXID: 1, MaxTXID: 80},
					{Level: lfsb.CompactionLevelSnapshot, MinTXID: 1, MaxTXID: 110},
				}), nil
			case 3:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 3, MinTXID: 70, MaxTXID: 78},
					{Level: 3, MinTXID: 79, MaxTXID: 80},
					{Level: 3, MinTXID: 81, MaxTXID: 87},
					{Level: 3, MinTXID: 88, MaxTXID: 89},
					{Level: 3, MinTXID: 90, MaxTXID: 101},
				}), nil
			case 2:
				return store.NewStoragePathSliceIterator([]store.StoragePath{}), nil
			case 1:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 1, MinTXID: 88, MaxTXID: 89},
					{Level: 1, MinTXID: 90, MaxTXID: 94},
					{Level: 1, MinTXID: 95, MaxTXID: 97},
					{Level: 1, MinTXID: 98, MaxTXID: 101},
				}), nil
			case 0:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 0, MinTXID: 98, MaxTXID: 99},
					{Level: 0, MinTXID: 100, MaxTXID: 100},
					{Level: 0, MinTXID: 101, MaxTXID: 101},
				}), nil
			default:
				return store.NewStoragePathSliceIterator(nil), nil
			}
		}

		if a, err := store.CalcSnapshotPlan(context.Background(), &client, "cluster", "db", 100); err != nil {
			t.Fatal(err)
		} else if got, want := a, []store.StoragePath{
			{Level: lfsb.CompactionLevelSnapshot, MinTXID: 1, MaxTXID: 80},
			{Level: 3, MinTXID: 81, MaxTXID: 87},
			{Level: 3, MinTXID: 88, MaxTXID: 89},
			{Level: 1, MinTXID: 90, MaxTXID: 94},
			{Level: 1, MinTXID: 95, MaxTXID: 97},
			{Level: 0, MinTXID: 98, MaxTXID: 99},
			{Level: 0, MinTXID: 100, MaxTXID: 100},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("paths=%#v\nexpected=%#v", got, want)
		}
	})

	t.Run("NoSnapshot", func(t *testing.T) {
		var client mock.StorageClient
		client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
			switch level {
			case lfsb.CompactionLevelSnapshot, 2, 1:
				return store.NewStoragePathSliceIterator([]store.StoragePath{}), nil
			case 0:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 0, MinTXID: 1, MaxTXID: 1},
					{Level: 0, MinTXID: 2, MaxTXID: 2},
					{Level: 0, MinTXID: 3, MaxTXID: 3},
				}), nil
			default:
				return store.NewStoragePathSliceIterator(nil), nil
			}
		}

		if a, err := store.CalcSnapshotPlan(context.Background(), &client, "cluster", "db", 100); err != nil {
			t.Fatal(err)
		} else if got, want := a, []store.StoragePath{
			{Level: 0, MinTXID: 1, MaxTXID: 1},
			{Level: 0, MinTXID: 2, MaxTXID: 2},
			{Level: 0, MinTXID: 3, MaxTXID: 3},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("paths=%#v\nexpected=%#v", got, want)
		}
	})

	t.Run("ErrNonContiguousTXID", func(t *testing.T) {
		var client mock.StorageClient
		client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
			switch level {
			case lfsb.CompactionLevelSnapshot:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: lfsb.CompactionLevelSnapshot, MinTXID: 1, MaxTXID: 10},
				}), nil
			case 0:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 0, MinTXID: 12, MaxTXID: 13},
				}), nil
			default:
				return store.NewStoragePathSliceIterator(nil), nil
			}
		}

		_, err := store.CalcSnapshotPlan(context.Background(), &client, "cluster", "db", 100)
		if err == nil || err.Error() != `non-contiguous transaction files: 000000000000000a -> 000000000000000c-000000000000000d` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrTxNotAvailable", func(t *testing.T) {
		var client mock.StorageClient
		client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
			switch level {
			case lfsb.CompactionLevelSnapshot:
				return store.NewStoragePathSliceIterator([]store.StoragePath{}), nil
			case 0:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 0, MinTXID: 12, MaxTXID: 12},
				}), nil
			default:
				return store.NewStoragePathSliceIterator(nil), nil
			}
		}

		_, err := store.CalcSnapshotPlan(context.Background(), &client, "cluster", "db", 10)
		if err != lfsb.ErrTxNotAvailable {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestCalcChangesPlan(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var client mock.StorageClient
		client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
			switch level {
			case 2:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 2, MinTXID: 70, MaxTXID: 78},
					{Level: 2, MinTXID: 79, MaxTXID: 80},
					{Level: 2, MinTXID: 81, MaxTXID: 87},
					{Level: 2, MinTXID: 88, MaxTXID: 89},
					{Level: 2, MinTXID: 90, MaxTXID: 101},
				}), nil
			case 1:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 1, MinTXID: 88, MaxTXID: 89},
					{Level: 1, MinTXID: 90, MaxTXID: 94},
					{Level: 1, MinTXID: 95, MaxTXID: 97},
					{Level: 1, MinTXID: 98, MaxTXID: 101},
					{Level: 1, MinTXID: 102, MaxTXID: 103},
				}), nil
			default:
				return store.NewStoragePathSliceIterator(nil), nil
			}
		}

		if a, err := store.CalcChangesPlan(context.Background(), &client, "cluster", "db", 2, 80); err != nil {
			t.Fatal(err)
		} else if got, want := a, []store.StoragePath{
			{Level: 2, MinTXID: 81, MaxTXID: 87},
			{Level: 2, MinTXID: 88, MaxTXID: 89},
			{Level: 2, MinTXID: 90, MaxTXID: 101},
			{Level: 1, MinTXID: 102, MaxTXID: 103},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("paths=%#v\nexpected=%#v", got, want)
		}

		if a, err := store.CalcChangesPlan(context.Background(), &client, "cluster", "db", 2, 86); err != nil {
			t.Fatal(err)
		} else if got, want := a, []store.StoragePath{
			{Level: 2, MinTXID: 81, MaxTXID: 87},
			{Level: 2, MinTXID: 88, MaxTXID: 89},
			{Level: 2, MinTXID: 90, MaxTXID: 101},
			{Level: 1, MinTXID: 102, MaxTXID: 103},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("paths=%#v\nexpected=%#v", got, want)
		}
	})

	t.Run("ErrNonContiguousTXID", func(t *testing.T) {
		var client mock.StorageClient
		client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
			switch level {
			case 2:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 2, MinTXID: 1, MaxTXID: 10},
				}), nil
			case 1:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 1, MinTXID: 12, MaxTXID: 13},
				}), nil
			default:
				return store.NewStoragePathSliceIterator(nil), nil
			}
		}

		_, err := store.CalcChangesPlan(context.Background(), &client, "cluster", "db", 2, 5)
		if err == nil || err.Error() != `non-contiguous transaction files: 000000000000000a -> 000000000000000c-000000000000000d` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrTxNotAvailable", func(t *testing.T) {
		var client mock.StorageClient
		client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
			switch level {
			case 2:
				return store.NewStoragePathSliceIterator([]store.StoragePath{
					{Level: 2, MinTXID: 12, MaxTXID: 12},
				}), nil
			default:
				return store.NewStoragePathSliceIterator(nil), nil
			}
		}

		_, err := store.CalcChangesPlan(context.Background(), &client, "cluster", "db", 2, 10)
		if err != lfsb.ErrTxNotAvailable {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestAttachMetadata(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var client mock.StorageClient
		client.MetadataFunc = func(ctx context.Context, path store.StoragePath) (store.StorageMetadata, error) {
			return store.StorageMetadata{
				PreApplyChecksum: ltx.Checksum(path.MinTXID),
			}, nil
		}

		// Create paths with minimal data.
		paths := make([]store.StoragePath, 1000)
		for i := range paths {
			paths[i].MinTXID = ltx.TXID(i)
		}

		// Attach metadata in parallel.
		if err := store.AttachMetadata(context.Background(), &client, paths, 32); err != nil {
			t.Fatal(err)
		}

		// Verify metadata is attached.
		for i, path := range paths {
			if got, want := path.Metadata.PreApplyChecksum, ltx.Checksum(i); got != want {
				t.Fatalf("PreApplyChecksum=%v, want %v", got, want)
			}
		}
	})
}

func TestFileStorageClient_WriteFile(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := NewFileStorageClient(t.TempDir())

		// Write data to a given storage path.
		if err := c.WriteFile(context.Background(), store.NewStoragePath("b", "c", 1, 1, 1), strings.NewReader("foo")); err != nil {
			t.Fatal(err)
		}

		// Read the file back out of the client.
		f, err := c.OpenFile(context.Background(), store.NewStoragePath("b", "c", 1, 1, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()

		if buf, err := io.ReadAll(f); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), "foo"; err != nil {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})
}

func TestFileStorageClient_OpenFile(t *testing.T) {
	t.Run("ErrNotExist", func(t *testing.T) {
		_, err := NewFileStorageClient(t.TempDir()).OpenFile(context.Background(), store.NewStoragePath("b", "c", 1, 1, 1))
		if err != os.ErrNotExist {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestFileStorageClient_Iterator(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		ctx := context.Background()
		c := NewFileStorageClient(t.TempDir())

		// Write several files to the same level.
		if err := c.WriteFile(ctx, store.NewStoragePath("b", "c", 1, 2, 2), strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if err := c.WriteFile(ctx, store.NewStoragePath("b", "c", 1, 1, 1), strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if err := c.WriteFile(ctx, store.NewStoragePath("b", "c", 1, 3, 3), strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}

		// Write other files to different orgs, clusters, databases, & levels.
		if err := c.WriteFile(ctx, store.NewStoragePath("X", "c", 1, 1, 1), strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if err := c.WriteFile(ctx, store.NewStoragePath("b", "X", 1, 1, 1), strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if err := c.WriteFile(ctx, store.NewStoragePath("b", "c", 2, 1, 1), strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}

		// Iterate over a single level and ensure results match.
		itr, err := c.Iterator(ctx, "b", "c", 1)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = itr.Close() }()

		if p, err := itr.NextStoragePath(ctx); err != nil {
			t.Fatal(err)
		} else if got, want := p, store.NewStoragePath("b", "c", 1, 1, 1); !got.Equal(want) {
			t.Fatalf("Next[0]: %#v, want %#v", got, want)
		}

		if p, err := itr.NextStoragePath(ctx); err != nil {
			t.Fatal(err)
		} else if got, want := p, store.NewStoragePath("b", "c", 1, 2, 2); !got.Equal(want) {
			t.Fatalf("Next[0]: %#v, want %#v", got, want)
		}

		if p, err := itr.NextStoragePath(ctx); err != nil {
			t.Fatal(err)
		} else if got, want := p, store.NewStoragePath("b", "c", 1, 3, 3); !got.Equal(want) {
			t.Fatalf("Next[0]: %#v, want %#v", got, want)
		}

		// Ensure iterator returns EOF after last path.
		if _, err := itr.NextStoragePath(ctx); err != io.EOF {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Empty", func(t *testing.T) {
		c := NewFileStorageClient(t.TempDir())
		itr, err := c.Iterator(context.Background(), "b", "c", 1)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = itr.Close() }()

		if _, err := itr.NextStoragePath(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestFindStoragePaths(t *testing.T) {
	var client mock.StorageClient
	client.IteratorFunc = func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
		return store.NewStoragePathSliceIterator([]store.StoragePath{
			{Level: 3, MinTXID: 70, MaxTXID: 78},
			{Level: 3, MinTXID: 79, MaxTXID: 80},
			{Level: 3, MinTXID: 81, MaxTXID: 87},
			{Level: 3, MinTXID: 88, MaxTXID: 89},
			{Level: 3, MinTXID: 90, MaxTXID: 101},
		}), nil
	}

	t.Run("NoFilter", func(t *testing.T) {
		paths, err := store.FindStoragePaths(context.Background(), &client, "cluster", "db", 3, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := paths, []store.StoragePath{
			{Level: 3, MinTXID: 70, MaxTXID: 78},
			{Level: 3, MinTXID: 79, MaxTXID: 80},
			{Level: 3, MinTXID: 81, MaxTXID: 87},
			{Level: 3, MinTXID: 88, MaxTXID: 89},
			{Level: 3, MinTXID: 90, MaxTXID: 101},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("paths=%#v\nexpected=%#v", got, want)
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		paths, err := store.FindStoragePaths(context.Background(), &client, "cluster", "db", 3, func(p store.StoragePath) (bool, error) {
			return p.MaxTXID <= 87, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := paths, []store.StoragePath{
			{Level: 3, MinTXID: 70, MaxTXID: 78},
			{Level: 3, MinTXID: 79, MaxTXID: 80},
			{Level: 3, MinTXID: 81, MaxTXID: 87},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("paths=%#v\nexpected=%#v", got, want)
		}
	})

	t.Run("StopIter", func(t *testing.T) {
		paths, err := store.FindStoragePaths(context.Background(), &client, "cluster", "db", 3, func(p store.StoragePath) (bool, error) {
			if p.MaxTXID > 87 {
				return false, store.ErrStopIter
			}

			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := paths, []store.StoragePath{
			{Level: 3, MinTXID: 70, MaxTXID: 78},
			{Level: 3, MinTXID: 79, MaxTXID: 80},
			{Level: 3, MinTXID: 81, MaxTXID: 87},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("paths=%#v\nexpected=%#v", got, want)
		}
	})

	t.Run("Error", func(t *testing.T) {
		retErr := errors.New("test error")
		_, err := store.FindStoragePaths(context.Background(), &client, "cluster", "db", 3, func(p store.StoragePath) (bool, error) {
			return false, retErr
		})
		if got, want := err, retErr; got != want {
			t.Fatalf("unexpected error, got = %v, want = %v", got, want)
		}
	})
}
