package store_test

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/internal"
	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
)

var _ store.StorageClient = (*FileStorageClient)(nil)

// FileStorageClient implements StorageClient for testing.
type FileStorageClient struct {
	dir string
}

func NewFileStorageClient(dir string) *FileStorageClient {
	return &FileStorageClient{dir: dir}
}

func (c *FileStorageClient) Close() error { return nil }

func (c *FileStorageClient) Type() string { return "file" }

func (c *FileStorageClient) Clusters(ctx context.Context) ([]string, error) {
	ents, err := os.ReadDir(filepath.Join(c.dir))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var a []string
	for _, ent := range ents {
		if ent.IsDir() && lfsb.ValidateClusterName(ent.Name()) == nil {
			a = append(a, ent.Name())
		}
	}
	sort.Strings(a)

	return a, nil
}

func (c *FileStorageClient) Databases(ctx context.Context, cluster string) ([]string, error) {
	root := filepath.Join(c.dir, cluster)
	ents, err := os.ReadDir(root)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var a []string
	for _, ent := range ents {
		if !ent.IsDir() || lfsb.ValidateDatabase(ent.Name()) != nil {
			continue
		}

		// Ensure directory contains at least one file.
		if hasFiles, err := dirHasFiles(filepath.Join(root, ent.Name())); err != nil {
			return nil, err
		} else if !hasFiles {
			continue
		}

		a = append(a, ent.Name())
	}
	sort.Strings(a)

	return a, nil
}

func (c *FileStorageClient) Levels(ctx context.Context, cluster, database string) ([]int, error) {
	ents, err := os.ReadDir(filepath.Join(c.dir, cluster, database))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var a []int
	for _, ent := range ents {
		if !ent.IsDir() { // must be a dir & a single character
			continue
		}
		level, err := strconv.Atoi(ent.Name())
		if err != nil || level < 0 || level > lfsb.CompactionLevelSnapshot {
			continue
		}
		a = append(a, level)
	}
	sort.Ints(a)

	return a, nil
}

// Metadata returns storage metadata for the given path. The file-based client
// does not store the metadata separately so it has to decode the file to derive it.
func (c *FileStorageClient) Metadata(ctx context.Context, path store.StoragePath) (store.StorageMetadata, error) {
	f, err := c.OpenFile(ctx, path)
	if err != nil {
		return store.StorageMetadata{}, err
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return store.StorageMetadata{}, err
	}
	return store.NewStorageMetadataFromLTX(dec.Header(), dec.Trailer()), nil
}

func (c *FileStorageClient) OpenFile(ctx context.Context, path store.StoragePath) (io.ReadCloser, error) {
	return c.OpenOSFile(ctx, path)
}

func (c *FileStorageClient) OpenOSFile(ctx context.Context, path store.StoragePath) (*os.File, error) {
	filename, err := store.FormatStoragePath(path, os.PathSeparator)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(filepath.Join(c.dir, filename))
	if os.IsNotExist(err) {
		err = os.ErrNotExist // convert to a generic error
	}
	return f, err
}

// WriteFile writes the contents of r to a path in long-term storage.
func (c *FileStorageClient) WriteFile(ctx context.Context, path store.StoragePath, r io.Reader) (retErr error) {
	filename, err := store.FormatStoragePath(path, os.PathSeparator)
	if err != nil {
		return err
	}
	filename = filepath.Join(c.dir, filename)

	// If this is the first TXID then ensure that the folder is cleaned up on
	// error. Otherwise, it will appear that an empty database has been created.
	//
	// NOTE: This defer must be before the temp file clean up below because it
	// relies on the directories being empty.
	if path.Level == 0 && path.MinTXID == 1 && path.MaxTXID == 1 {
		defer func() {
			if retErr != nil {
				_ = os.Remove(filepath.Dir(filename))               // attempt empty L0 dir deletion
				_ = os.Remove(filepath.Dir(filepath.Dir(filename))) // attempt empty database dir deletion
			}
		}()
	}

	tempFilename := filename + ".tmp"
	defer func() { _ = os.Remove(tempFilename) }()

	// Ensure all parent directories are created first.
	if err := os.MkdirAll(filepath.Dir(tempFilename), 0o777); err != nil {
		return err
	}

	// Create, copy, & sync temporary file first.
	f, err := os.Create(tempFilename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := io.Copy(f, r); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	}

	// For L0, ensure that file is valid before committing.
	if path.Level == 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		dec := ltx.NewDecoder(f)
		if err := dec.Verify(); err != nil {
			return err
		}
	}

	// Close file before we rename it. It's already sync'd so it shouldn't error.
	if err := f.Close(); err != nil {
		return err
	}

	// Atomically rename & sync parent directory.
	if err := os.Rename(tempFilename, filename); err != nil {
		return err
	} else if err := internal.Sync(filepath.Dir(filename)); err != nil {
		return err
	}

	return nil
}

// DeleteFile removes a single file from storage.
func (c *FileStorageClient) DeleteFile(ctx context.Context, path store.StoragePath) error {
	filename, err := store.FormatStoragePath(path, os.PathSeparator)
	if err != nil {
		return err
	}

	if err := os.Remove(filepath.Join(c.dir, filename)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// DeleteFiles removes one or more files from storage.
func (c *FileStorageClient) DeleteFiles(ctx context.Context, paths []store.StoragePath) error {
	for _, path := range paths {
		if err := c.DeleteFile(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

// MkdirLevel ensures a directory exists for a given level.
func (c *FileStorageClient) MkdirLevel(ctx context.Context, cluster, database string, level int) error {
	dir, err := store.FormatStorageLevelDir(cluster, database, level, os.PathSeparator)
	if err != nil {
		return err
	}
	return os.MkdirAll(filepath.Join(c.dir, dir), 0o777)
}

// DeleteAllLevel deletes all DB files under the specified level.
func (c *FileStorageClient) DeleteAllLevel(ctx context.Context, cluster, database string, level int) error {
	dir, err := store.FormatStorageLevelDir(cluster, database, level, os.PathSeparator)
	if err != nil {
		return err
	}

	return os.RemoveAll(filepath.Join(c.dir, dir))
}

// DeleteCluster deletes cluster's data from the local FS.
func (c *FileStorageClient) DeleteCluster(ctx context.Context, cluster string) error {
	return os.RemoveAll(filepath.Join(c.dir, cluster))
}

// Returns an iterator over a given database's compaction level.
func (c *FileStorageClient) Iterator(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
	subDir, err := store.FormatStorageLevelDir(cluster, database, level, os.PathSeparator)
	if err != nil {
		return nil, err
	}

	ents, err := os.ReadDir(filepath.Join(c.dir, subDir))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	a := make([]store.StoragePath, 0, len(ents))
	for _, ent := range ents {
		minTXID, maxTXID, err := lfsb.ParseLTXFilename(ent.Name())
		if err != nil {
			continue
		}

		fi, err := ent.Info()
		if err != nil {
			return nil, err
		}

		a = append(a, store.StoragePath{
			Cluster:   cluster,
			Database:  database,
			Level:     level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			CreatedAt: fi.ModTime().UTC(),
			Size:      fi.Size(),
		})
	}

	// Ensure the results are sorted.
	sort.Slice(a, func(i, j int) bool {
		return store.CompareStoragePath(&a[i], &a[j]) == -1
	})

	// Wrap with an iterator over a slice.
	return store.NewStoragePathSliceIterator(a), nil
}

// dirHasFiles returns true if the directory contains any regular files.
func dirHasFiles(root string) (hasFiles bool, _ error) {
	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		} else if strings.HasPrefix(filepath.Base(path), ".") {
			return nil // skip hidden
		}

		if !info.IsDir() {
			hasFiles = true
		}
		return nil
	})
	return hasFiles, err
}
