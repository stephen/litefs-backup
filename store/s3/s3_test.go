package s3_test

import (
	"context"
	"flag"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/store"
	"github.com/stephen/litefs-backup/store/s3"
)

var integration = flag.Bool("integration", false, "run integration tests")

const (
	Bucket = "lfsb-integration-test"
)

func TestStorageClient_OpenFile(t *testing.T) {
	t.Run("ErrNotExists", func(t *testing.T) {
		client := newOpenStorageClient(t)
		_, err := client.OpenFile(context.Background(), store.NewStoragePath("cluster", "database", 1, 1000, 1000))
		if err != os.ErrNotExist {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestStorageClient_WriteFile(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := newOpenStorageClient(t)

		// Write file to storage.
		path := store.NewStoragePath("cluster", "database", 1, 1000, 2000)
		path.Metadata = store.StorageMetadata{PageSize: 512}
		if err := client.WriteFile(context.Background(), path, strings.NewReader("foo")); err != nil {
			t.Fatal(err)
		}

		// Read the same path back out of storage.
		rc, err := client.OpenFile(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rc.Close() }()

		if buf, err := io.ReadAll(rc); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), "foo"; got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStorageClient_Metadata(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := newOpenStorageClient(t)

		// Write file to storage.
		path := store.NewStoragePath("cluster", "database", 1, 1000, 2000)
		path.Metadata = store.StorageMetadata{
			PageSize:          512,
			Commit:            1000,
			Timestamp:         time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
			PreApplyChecksum:  2000,
			PostApplyChecksum: 3000,
		}
		if err := client.WriteFile(context.Background(), path, strings.NewReader("foo")); err != nil {
			t.Fatal(err)
		}

		// Read metadata back out of S3.
		md, err := client.Metadata(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		} else if got, want := md, path.Metadata; !reflect.DeepEqual(got, want) {
			t.Fatalf("metadata=%#v, want %#v", got, want)
		}
	})
}

func TestStorageClient_Clusters(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := newOpenStorageClient(t)
		for _, path := range []store.StoragePath{
			store.NewStoragePath("cluster0", "database0", 1, 1, 1),
			store.NewStoragePath("cluster0", "database0", 1, 2, 2),
			store.NewStoragePath("cluster1", "database0", 1, 1, 1),
		} {
			path.Metadata = store.StorageMetadata{PageSize: 512}
			if err := client.WriteFile(context.Background(), path, strings.NewReader("foo")); err != nil {
				t.Fatal(err)
			}
		}

		a, err := client.Clusters(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := a, []string{"cluster0", "cluster1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%v, want %#v", got, want)
		}
	})
}

func TestStorageClient_Databases(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := newOpenStorageClient(t)
		for _, path := range []store.StoragePath{
			store.NewStoragePath("cluster3", "database0", 1, 1, 1),
			store.NewStoragePath("cluster0", "database1", 1, 1, 1),
			store.NewStoragePath("cluster0", "database0", 1, 2, 2),
			store.NewStoragePath("cluster1", "database3", 1, 1, 1),
		} {
			path.Metadata = store.StorageMetadata{PageSize: 512}
			if err := client.WriteFile(context.Background(), path, strings.NewReader("foo")); err != nil {
				t.Fatal(err)
			}
		}

		a, err := client.Databases(context.Background(), "cluster0")
		if err != nil {
			t.Fatal(err)
		} else if got, want := a, []string{"database0", "database1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%v, want %#v", got, want)
		}
	})

	t.Run("ErrClusterRequired", func(t *testing.T) {
		client := newOpenStorageClient(t)
		if _, err := client.Databases(context.Background(), ""); err != lfsb.ErrClusterRequired {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestStorageClient_Levels(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := newOpenStorageClient(t)
		for _, path := range []store.StoragePath{
			store.NewStoragePath("cluster3", "database0", 0, 1, 1),
			store.NewStoragePath("cluster3", "database0", 2, 1, 1),
			store.NewStoragePath("cluster3", "database0", 9, 1, 1),
			store.NewStoragePath("cluster0", "database1", 1, 1, 1),
		} {
			path.Metadata = store.StorageMetadata{PageSize: 512}
			if err := client.WriteFile(context.Background(), path, strings.NewReader("foo")); err != nil {
				t.Fatal(err)
			}
		}

		a, err := client.Levels(context.Background(), "cluster3", "database0")
		if err != nil {
			t.Fatal(err)
		} else if got, want := a, []int{0, 2, 9}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%v, want %#v", got, want)
		}
	})

	t.Run("ErrClusterRequired", func(t *testing.T) {
		client := newOpenStorageClient(t)
		if _, err := client.Levels(context.Background(), "", "database0"); err != lfsb.ErrClusterRequired {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrClusterRequired", func(t *testing.T) {
		client := newOpenStorageClient(t)
		if _, err := client.Levels(context.Background(), "cluster0", ""); err != lfsb.ErrDatabaseRequired {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestStorageClient_Iterator(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := newOpenStorageClient(t)
		for _, path := range []store.StoragePath{
			store.NewStoragePath("cluster3", "database0", 1, 1, 1), // wrong org/cluster
			store.NewStoragePath("cluster0", "database1", 1, 2, 2), // wrong database
			store.NewStoragePath("cluster0", "database0", 1, 1, 1),
			store.NewStoragePath("cluster0", "database0", 1, 2, 2),
			store.NewStoragePath("cluster0", "database0", 1, 3, 4),
			store.NewStoragePath("cluster0", "database0", 2, 1, 1), // wrong level
		} {
			path.Metadata = store.StorageMetadata{PageSize: 5012}
			if err := client.WriteFile(context.Background(), path, strings.NewReader("foo")); err != nil {
				t.Fatal(err)
			}
		}

		itr, err := client.Iterator(context.Background(), "cluster0", "database0", 1)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = itr.Close() }()

		if path, err := itr.NextStoragePath(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := path, store.NewStoragePath("cluster0", "database0", 1, 1, 1); !got.Equal(want) {
			t.Fatalf("got=%#v, want %#v", got, want)
		}

		if path, err := itr.NextStoragePath(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := path, store.NewStoragePath("cluster0", "database0", 1, 2, 2); !got.Equal(want) {
			t.Fatalf("got=%#v, want %#v", got, want)
		}

		if path, err := itr.NextStoragePath(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := path, store.NewStoragePath("cluster0", "database0", 1, 3, 4); !got.Equal(want) {
			t.Fatalf("got=%#v, want %#v", got, want)
		}

		if _, err := itr.NextStoragePath(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %s", err)
		}

		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEncodeStorageMetadata(t *testing.T) {
	md := store.StorageMetadata{
		PageSize:          4096,
		Commit:            1000,
		Timestamp:         time.Date(2000, time.January, 1, 0, 0, 0, 123000000, time.UTC),
		PreApplyChecksum:  3000,
		PostApplyChecksum: 4000,
	}

	// Verify all fields encode correctly.
	m := s3.EncodeStorageMetadata(md)
	if got, want := *m["Page-Size"], "4096"; got != want {
		t.Fatalf("Page-Size=%q, want %q", got, want)
	}
	if got, want := *m["Commit"], "1000"; got != want {
		t.Fatalf("Commit=%q, want %q", got, want)
	}
	if got, want := *m["Timestamp"], "2000-01-01T00:00:00.123Z"; got != want {
		t.Fatalf("Timestamp=%q, want %q", got, want)
	}
	if got, want := *m["Pre-Apply-Checksum"], "0000000000000bb8"; got != want {
		t.Fatalf("Pre-Apply-Checksum=%q, want %q", got, want)
	}
	if got, want := *m["Post-Apply-Checksum"], "0000000000000fa0"; got != want {
		t.Fatalf("Post-Apply-Checksum=%q, want %q", got, want)
	}

	// Verify that we can decode back to the original metadata.
	if other, err := s3.DecodeStorageMetadata(m); err != nil {
		t.Fatal(err)
	} else if got, want := md, other; !reflect.DeepEqual(got, want) {
		t.Fatalf("Decode=%#v, want %#v", got, want)
	}
}

func newStorageClient(tb testing.TB) *s3.StorageClient {
	tb.Helper()
	if !*integration {
		tb.Skip("integration tests not enabled, skipping")
	}
	return s3.NewStorageClient(&lfsb.Config{S3Bucket: Bucket, S3Endpoint: os.Getenv("AWS_ENDPOINT_URL_S3")})
}

func newOpenStorageClient(tb testing.TB) *s3.StorageClient {
	tb.Helper()
	client := newStorageClient(tb)
	if err := client.Open(); err != nil {
		tb.Fatal(err)
	}

	// Clear all files from cluster before test.
	if n, err := client.DeleteAll(context.Background()); err != nil {
		tb.Fatalf("cannot delete all objects from cluster before test: %s", err)
	} else if n > 0 {
		tb.Logf("deleted %d files from cluster before tests", n)
	}

	tb.Cleanup(func() {
		// Clear all files from cluster after test.
		if n, err := client.DeleteAll(context.Background()); err != nil {
			tb.Fatalf("cannot delete all items from cluster: %s", err)
		} else if n > 0 {
			tb.Logf("deleted %d files from cluster after tests", n)
		}
		if err := client.Close(); err != nil {
			tb.Fatalf("cannot close s3 client: %s", err)
		}
	})

	return client
}
