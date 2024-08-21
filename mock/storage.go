package mock

import (
	"context"
	"io"

	"github.com/stephen/litefs-backup/store"
)

var _ store.StorageClient = (*StorageClient)(nil)

type StorageClient struct {
	CloseFunc       func() error
	OrgIDsFunc      func(ctx context.Context) ([]int, error)
	ClustersFunc    func(ctx context.Context) ([]string, error)
	DatabasesFunc   func(ctx context.Context, cluster string) ([]string, error)
	LevelsFunc      func(ctx context.Context, cluster, database string) ([]int, error)
	MetadataFunc    func(ctx context.Context, path store.StoragePath) (store.StorageMetadata, error)
	OpenFileFunc    func(ctx context.Context, path store.StoragePath) (io.ReadCloser, error)
	WriteFileFunc   func(ctx context.Context, path store.StoragePath, r io.Reader) error
	DeleteFilesFunc func(ctx context.Context, paths []store.StoragePath) error
	IteratorFunc    func(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error)
}

func (c *StorageClient) Close() error {
	return c.CloseFunc()
}

func (c *StorageClient) OrgIDs(ctx context.Context) ([]int, error) {
	return c.OrgIDsFunc(ctx)
}

func (c *StorageClient) Clusters(ctx context.Context) ([]string, error) {
	return c.ClustersFunc(ctx)
}

func (c *StorageClient) Databases(ctx context.Context, cluster string) ([]string, error) {
	return c.DatabasesFunc(ctx, cluster)
}

func (c *StorageClient) Levels(ctx context.Context, cluster, database string) ([]int, error) {
	return c.LevelsFunc(ctx, cluster, database)
}

func (c *StorageClient) Metadata(ctx context.Context, path store.StoragePath) (store.StorageMetadata, error) {
	return c.MetadataFunc(ctx, path)
}

func (c *StorageClient) OpenFile(ctx context.Context, path store.StoragePath) (io.ReadCloser, error) {
	return c.OpenFileFunc(ctx, path)
}

func (c *StorageClient) WriteFile(ctx context.Context, path store.StoragePath, r io.Reader) error {
	return c.WriteFileFunc(ctx, path, r)
}

func (c *StorageClient) DeleteFiles(ctx context.Context, paths []store.StoragePath) error {
	return c.DeleteFilesFunc(ctx, paths)
}

func (c *StorageClient) Iterator(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
	return c.IteratorFunc(ctx, cluster, database, level)
}
