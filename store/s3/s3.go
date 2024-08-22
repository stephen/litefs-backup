package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

var _ store.StorageClient = (*StorageClient)(nil)

// StorageClient represents a client for long-term storage on the file system.
type StorageClient struct {
	bucket   string // s3 bucket name
	svc      *s3.S3 // s3 service
	uploader *s3manager.Uploader
}

// NewStorageClient returns a new instance of StorageClient.
func NewStorageClient(bucket string) *StorageClient {
	return &StorageClient{
		bucket: bucket,
	}
}

// Open initializes the s3 client.
//
// See AWS session docs for details: https://docs.aws.amazon.com/sdk-for-go/api/aws/session/
func (c *StorageClient) Open() (err error) {
	var opts []*aws.Config
	if endpoint := os.Getenv("LFSB_S3_ENDPOINT"); endpoint != "" {
		opts = append(opts, &aws.Config{
			Endpoint:         &endpoint,
			S3ForcePathStyle: aws.Bool(true),
		})
	}

	sess, err := session.NewSession(opts...)
	if err != nil {
		return err
	}
	c.svc = s3.New(sess)
	c.uploader = s3manager.NewUploaderWithClient(c.svc)

	return nil
}

// Close is a no-op.
func (c *StorageClient) Close() error { return nil }

func (c *StorageClient) Type() string { return "s3" }

// Clusters returns a list of clusters within an org.
func (c *StorageClient) Clusters(ctx context.Context) ([]string, error) {
	var a []string
	if err := c.svc.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.bucket),
		Delimiter: aws.String("/"),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, prefix := range page.CommonPrefixes {
			key := path.Base(*prefix.Prefix)
			if err := lfsb.ValidateClusterName(key); err != nil {
				continue // skip invalid names
			}
			a = append(a, key)
		}
		return true
	}); err != nil {
		return nil, err
	}
	return a, nil
}

// Databases returns a list of databases within a cluster.
func (c *StorageClient) Databases(ctx context.Context, cluster string) ([]string, error) {
	if err := lfsb.ValidateClusterName(cluster); err != nil {
		return nil, err
	}

	var a []string
	if err := c.svc.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.bucket),
		Prefix:    aws.String(cluster + "/"),
		Delimiter: aws.String("/"),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, prefix := range page.CommonPrefixes {
			key := path.Base(*prefix.Prefix)
			if err := lfsb.ValidateDatabase(key); err != nil {
				continue // skip invalid names
			}
			a = append(a, key)
		}
		return true
	}); err != nil {
		return nil, err
	}
	return a, nil
}

// Levels returns a list of levels within a cluster.
func (c *StorageClient) Levels(ctx context.Context, cluster, database string) ([]int, error) {
	if err := lfsb.ValidateClusterName(cluster); err != nil {
		return nil, err
	} else if err := lfsb.ValidateDatabase(database); err != nil {
		return nil, err
	}

	var a []int
	if err := c.svc.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.bucket),
		Prefix:    aws.String(path.Join(cluster, database) + "/"),
		Delimiter: aws.String("/"),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, prefix := range page.CommonPrefixes {
			level, err := strconv.Atoi(path.Base(*prefix.Prefix))
			if err != nil || level < 0 || level > lfsb.CompactionLevelSnapshot {
				continue // skip invalid levels
			}
			a = append(a, level)
		}
		return true
	}); err != nil {
		return nil, err
	}
	return a, nil
}

// Metadata returns metadata for a specific file. Returns os.NotExist if not found.
func (c *StorageClient) Metadata(ctx context.Context, path store.StoragePath) (md store.StorageMetadata, err error) {
	key, err := store.FormatStoragePath(path, PathSeparator)
	if err != nil {
		return md, err
	}

	out, err := c.svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})

	var awsErr awserr.Error
	if errors.As(err, &awsErr) && awsErr.Code() == s3.ErrCodeNoSuchKey {
		return md, os.ErrNotExist
	} else if err != nil {
		return md, err
	}
	return DecodeStorageMetadata(out.Metadata)
}

// OpenFile returns a reader for a specific file. Returns os.NotExist if not found.
func (c *StorageClient) OpenFile(ctx context.Context, path store.StoragePath) (io.ReadCloser, error) {
	key, err := store.FormatStoragePath(path, PathSeparator)
	if err != nil {
		return nil, err
	}

	out, err := c.svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})

	var awsErr awserr.Error
	if errors.As(err, &awsErr) && awsErr.Code() == s3.ErrCodeNoSuchKey {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// WriteFile writes the contents of r to a path in long-term storage.
func (c *StorageClient) WriteFile(ctx context.Context, path store.StoragePath, r io.Reader) error {
	if path.Metadata.IsZero() {
		return fmt.Errorf("storage path metadata required")
	}

	key, err := store.FormatStoragePath(path, PathSeparator)
	if err != nil {
		return err
	}

	if _, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:   aws.String(c.bucket),
		Key:      aws.String(key),
		Metadata: EncodeStorageMetadata(path.Metadata),
		Body:     r,
	}); err != nil {
		return err
	}
	return nil
}

// Returns an iterator over a given database's compaction level.
func (c *StorageClient) Iterator(ctx context.Context, cluster, database string, level int) (store.StoragePathIterator, error) {
	return newStoragePathIterator(ctx, c, cluster, database, level)
}

// DeleteFiles removes a list of paths from the cluster.
func (c *StorageClient) DeleteFiles(ctx context.Context, paths []store.StoragePath) error {
	for len(paths) > 0 {
		// Build chunks of object IDs to send.
		var objIDs []*s3.ObjectIdentifier
		for i := 0; len(paths) != 0 && i < maxKeys; i++ {
			key, err := store.FormatStoragePath(paths[0], PathSeparator)
			if err != nil {
				return err
			}
			objIDs, paths = append(objIDs, &s3.ObjectIdentifier{Key: &key}), paths[1:]
		}

		// Send the chunk for deletion.
		if _, err := c.svc.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &s3.Delete{Objects: objIDs, Quiet: aws.Bool(true)},
		}); err != nil {
			return err
		}
	}

	return nil
}

// DeleteAll removes all objects from the cluster. Returns number deleted. Used for testing.
func (c *StorageClient) DeleteAll(ctx context.Context) (int, error) {
	var objIDs []*s3.ObjectIdentifier
	if err := c.svc.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			objIDs = append(objIDs, &s3.ObjectIdentifier{Key: obj.Key})
		}
		return true
	}); err != nil {
		return 0, err
	}
	cnt := len(objIDs)

	// Delete all files in batches.
	for len(objIDs) > 0 {
		n := maxKeys
		if len(objIDs) < n {
			n = len(objIDs)
		}

		if _, err := c.svc.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &s3.Delete{Objects: objIDs[:n], Quiet: aws.Bool(true)},
		}); err != nil {
			return 0, err
		}

		objIDs = objIDs[n:]
	}

	return cnt, nil
}

// StoragePathIterator returns an iterator over a given level.
type StoragePathIterator struct {
	client *StorageClient
	prefix string

	cluster  string
	database string
	level    int
	ch       chan storagePathResponse

	g      errgroup.Group
	ctx    context.Context
	cancel context.CancelCauseFunc
}

// newStoragePathIterator returns a new instance of StoragePathIterator and starts a background fetch goroutine.
func newStoragePathIterator(ctx context.Context, client *StorageClient, cluster, database string, level int) (*StoragePathIterator, error) {
	prefix, err := store.FormatStorageLevelDir(cluster, database, level, PathSeparator)
	if err != nil {
		return nil, err
	}

	itr := &StoragePathIterator{
		client: client,
		prefix: prefix + "/",
		ch:     make(chan storagePathResponse),

		cluster:  cluster,
		database: database,
		level:    level,
	}

	itr.ctx, itr.cancel = context.WithCancelCause(ctx)

	itr.g.Go(func() error {
		// defer sentry.Recover() XXX
		defer close(itr.ch)
		if err := itr.fetch(itr.ctx); err != nil {
			itr.ch <- storagePathResponse{err: err}
		}
		return nil
	})

	return itr, nil
}

// Close cancels the iterator and waits for the background goroutine to finish.
func (itr *StoragePathIterator) Close() error {
	itr.cancel(errors.New("iterator closed"))
	return itr.g.Wait()
}

// NextStoragePath returns the next available storage path.
// Returns io.EOF when no more paths are available.
func (itr *StoragePathIterator) NextStoragePath(ctx context.Context) (store.StoragePath, error) {
	select {
	case <-ctx.Done():
		return store.StoragePath{}, ctx.Err()
	case <-itr.ctx.Done():
		return store.StoragePath{}, itr.ctx.Err()
	case resp, ok := <-itr.ch:
		if !ok {
			return store.StoragePath{}, io.EOF
		}
		return resp.path, resp.err
	}
}

// fetch runs in the background and sends storage paths to ch.
func (itr *StoragePathIterator) fetch(ctx context.Context) error {
	return itr.client.svc.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(itr.client.bucket),
		Prefix:    aws.String(itr.prefix),
		Delimiter: aws.String("/"),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			minTXID, maxTXID, err := lfsb.ParseLTXFilename(path.Base(*obj.Key))
			if err != nil {
				continue
			}

			p := store.StoragePath{
				Cluster:   itr.cluster,
				Database:  itr.database,
				Level:     itr.level,
				MinTXID:   minTXID,
				MaxTXID:   maxTXID,
				CreatedAt: obj.LastModified.UTC(),
				Size:      *obj.Size,
			}

			select {
			case <-ctx.Done():
				return false
			case itr.ch <- storagePathResponse{path: p}:
			}
		}
		return true
	})
}

type storagePathResponse struct {
	path store.StoragePath
	err  error
}

// EncodeStorageMetadata encodes m into S3 object metadata.
func EncodeStorageMetadata(m store.StorageMetadata) map[string]*string {
	return map[string]*string{
		"Page-Size":           aws.String(strconv.FormatUint(uint64(m.PageSize), 10)),
		"Commit":              aws.String(strconv.FormatUint(uint64(m.Commit), 10)),
		"Timestamp":           aws.String(ltx.FormatTimestamp(m.Timestamp)),
		"Pre-Apply-Checksum":  aws.String(fmt.Sprint(m.PreApplyChecksum)),
		"Post-Apply-Checksum": aws.String(fmt.Sprint(m.PostApplyChecksum)),
	}
}

// DecodeStorageMetadata decodes m into a metadata structure from S3 object metadata.
func DecodeStorageMetadata(m map[string]*string) (md store.StorageMetadata, retErr error) {
	var err error

	if v := m["Page-Size"]; v != nil {
		out, err := strconv.ParseUint(*v, 10, 32)
		if retErr == nil {
			retErr = err
		}
		md.PageSize = uint32(out)
	}

	if v := m["Commit"]; v != nil {
		out, err := strconv.ParseUint(*v, 10, 32)
		if retErr == nil {
			retErr = err
		}
		md.Commit = uint32(out)
	}

	if v := m["Timestamp"]; v != nil {
		out, err := time.Parse(time.RFC3339, *v)
		if retErr == nil {
			retErr = err
		}
		md.Timestamp = out
	}

	if v := m["Pre-Apply-Checksum"]; v != nil {
		preApplyChecksum, err := strconv.ParseUint(*v, 16, 64)
		if retErr == nil {
			retErr = err
		}
		md.PreApplyChecksum = ltx.Checksum(preApplyChecksum)
	}

	if v := m["Post-Apply-Checksum"]; v != nil {
		postApplyChecksum, err := strconv.ParseUint(*v, 16, 64)
		if retErr == nil {
			retErr = err
		}
		md.PostApplyChecksum = ltx.Checksum(postApplyChecksum)
	}

	return md, retErr
}

// PathSeparator is the character used to separate parts of an S3 path.
const PathSeparator = '/'

// maxKeys is the maximum number of keys that can be sent in a DELETE operation.
const maxKeys = 1000
