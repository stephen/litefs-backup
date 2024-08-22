package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/superfly/ltx"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

// ErrStopIter indicates that the ongoing iteration should be aborted
var ErrStopIter = errors.New("stop iter")

// StorageClient represents a client for long-term storage.
type StorageClient interface {
	io.Closer

	// Clusters returns a list of clusters within an org.
	Clusters(ctx context.Context) ([]string, error)

	// Databases returns a list of databases within a cluster.
	Databases(ctx context.Context, cluster string) ([]string, error)

	// Levels returns a list of levels for a database.
	Levels(ctx context.Context, cluster, database string) ([]int, error)

	// Metadata returns the metadata for a given path.
	Metadata(ctx context.Context, path StoragePath) (StorageMetadata, error)

	// OpenFile returns a reader for a specific file. Returns os.NotExist if not found.
	OpenFile(ctx context.Context, path StoragePath) (io.ReadCloser, error)

	// WriteFile writes the contents of r to a path in long-term storage.
	WriteFile(ctx context.Context, path StoragePath, r io.Reader) error

	// DeleteFiles removes one or more files from storage.
	DeleteFiles(ctx context.Context, paths []StoragePath) error

	// Iterator returns an iterator over a given database's compaction level.
	Iterator(ctx context.Context, cluster, db string, level int) (StoragePathIterator, error)
}

// CalcSnapshotPlan returns a list of storage paths to create a snapshot at the given TXID.
func CalcSnapshotPlan(ctx context.Context, client StorageClient, cluster, database string, txID ltx.TXID) ([]StoragePath, error) {
	var paths StoragePathSlice

	// Start with latest snapshot before target TXID.
	if a, err := FindStoragePaths(ctx, client, cluster, database, lfsb.CompactionLevelSnapshot, func(p StoragePath) (bool, error) {
		return p.MaxTXID <= txID, nil
	}); err != nil {
		return nil, err
	} else if len(a) > 0 {
		paths = append(paths, a[len(a)-1])
	}

	// Starting from the highest compaction level, collect all paths after the
	// latest TXID for each level. Compactions are based on the previous level's
	// TXID granularity so the TXIDs should align between compaction levels.
	const maxLevel = lfsb.CompactionLevelSnapshot - 1
	for level := maxLevel; level >= 0; level-- {
		a, err := FindStoragePaths(ctx, client, cluster, database, level, func(p StoragePath) (bool, error) {
			return p.MinTXID > paths.MaxTXID() && p.MaxTXID <= txID, nil
		})
		if err != nil {
			return nil, err
		}

		// Append each storage path to the list
		for i := range a {
			// Ensure TXIDs are contiguous between each paths.
			if paths.MaxTXID()+1 != a[i].MinTXID {
				return nil, fmt.Errorf("non-contiguous transaction files: %s -> %s-%s",
					paths.MaxTXID().String(), a[i].MinTXID.String(), a[i].MaxTXID.String())
			}
			paths = append(paths, a[i])
		}
	}

	// Return an error if we are unable to find any set of LTX files before
	// target TXID. This shouldn't happen under normal circumstances. Only if
	// lower level LTX files are removed before a snapshot has occurred.
	if len(paths) == 0 {
		return nil, lfsb.ErrTxNotAvailable
	}

	return paths, nil
}

// CalcChangesPlan returns a list of storage paths needed to query a list of changed DB pages from the given txID up
// until the latest available DB TX.
func CalcChangesPlan(ctx context.Context, client StorageClient, cluster, database string, maxLevel int, txID ltx.TXID) ([]StoragePath, error) {
	var paths StoragePathSlice

	for level := maxLevel; level >= 0; level-- {
		a, err := FindStoragePaths(ctx, client, cluster, database, level, func(p StoragePath) (bool, error) {
			// We are iterating over the first oldest level. Collect everything starting from the first LTX file
			// that has the required txID.
			if len(paths) == 0 {
				return p.MaxTXID > txID, nil
			}

			// Otherwise, just collect continious LTX files up until the last one.
			return p.MinTXID > paths.MaxTXID(), nil
		})
		if err != nil {
			return nil, err
		}

		// Append each storage path to the list
		for i := range a {
			// Ensure TXIDs are contiguous between each paths.
			if len(paths) > 0 && paths.MaxTXID()+1 != a[i].MinTXID {
				return nil, fmt.Errorf("non-contiguous transaction files: %s -> %s-%s",
					paths.MaxTXID().String(), a[i].MinTXID.String(), a[i].MaxTXID.String())
			}
			paths = append(paths, a[i])
		}
	}

	// Return an error if we are unable to find any set of LTX files that includes
	// or goes right after the given TXID.
	if len(paths) == 0 || paths[0].MinTXID-1 > txID {
		return nil, lfsb.ErrTxNotAvailable
	}

	return paths, nil
}

// NewCompactorFromPaths returns a compactor from a set of storage paths.
func NewCompactorFromPaths(ctx context.Context, client StorageClient, paths []StoragePath, w io.Writer) (c *ltx.Compactor, close func() error, retErr error) {
	// Build a list of file handles for the compactor but ensure they are closed later.
	rdrs := make([]io.Reader, 0, len(paths))
	close = func() (fnErr error) {
		for _, r := range rdrs {
			if err := r.(io.Closer).Close(); fnErr == nil {
				fnErr = err
			}
		}
		return fnErr
	}

	// Close all readers if this function exits with an error.
	defer func() {
		if retErr != nil {
			_ = close()
		}
	}()

	for _, path := range paths {
		f, err := client.OpenFile(ctx, path)
		if err != nil {
			return nil, close, fmt.Errorf("open storage file (%s): %w", path, err)
		}
		rdrs = append(rdrs, f)
	}

	compactor := ltx.NewCompactor(w, rdrs)
	compactor.HeaderFlags = ltx.HeaderFlagCompressLZ4
	return compactor, close, nil
}

// ForEachStorageCluster executes fn once for each cluster in storage.
func ForEachStorageCluster(ctx context.Context, client StorageClient, fn func(cluster string) error) error {
	clusters, err := client.Clusters(ctx)
	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		if err := fn(cluster); err != nil {
			return err
		}
	}

	return nil
}

// ForEachStorageDatabase executes fn once for each database in storage.
func ForEachStorageDatabase(ctx context.Context, client StorageClient, fn func(cluster, database string) error) error {
	clusters, err := client.Clusters(ctx)
	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		databases, err := client.Databases(ctx, cluster)
		if err != nil {
			return err
		}

		for _, database := range databases {
			if err := fn(cluster, database); err != nil {
				return err
			}
		}
	}

	return nil
}

// FindStoragePaths returns a list of storage paths that match filter.
func FindStoragePaths(ctx context.Context, client StorageClient, cluster, database string, level int, filter func(StoragePath) (bool, error)) ([]StoragePath, error) {
	itr, err := client.Iterator(ctx, cluster, database, level)
	if err != nil {
		return nil, err
	}
	defer func() { _ = itr.Close() }()

	var a []StoragePath
	for i := 0; ; i++ {
		path, err := itr.NextStoragePath(ctx)
		if err == io.EOF {
			return a, itr.Close()
		} else if err != nil {
			return nil, err
		}

		if filter != nil {
			if match, err := filter(path); errors.Is(err, ErrStopIter) {
				return a, itr.Close()
			} else if err != nil {
				return a, err
			} else if !match {
				continue
			}
		}

		a = append(a, path)
	}
}

// AttachMetadata fetches metadata for every path in parallel.
func AttachMetadata(ctx context.Context, client StorageClient, paths []StoragePath, parallelism int) error {
	if parallelism < 1 {
		parallelism = 1
	}

	g, ctx := errgroup.WithContext(ctx)

	// Send path indexes to a channel.
	ch := make(chan int)
	g.Go(func() error {
		// defer sentry.Recover()
		defer close(ch)

		for i := range paths {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- i:
			}
		}
		return nil
	})

	// Start worker goroutines to process each path.
	for i := 0; i < parallelism; i++ {
		g.Go(func() error {
			// defer sentry.Recover()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case index, ok := <-ch:
					if !ok {
						return nil
					}

					md, err := client.Metadata(ctx, paths[index])
					if err != nil {
						return err
					}
					paths[index].Metadata = md
				}
			}
		})
	}

	return g.Wait()
}

// MaxStoragePath returns the last storage path for a level.
// Returns a zero value if no storage files exists in the level.
func MaxStoragePath(ctx context.Context, client StorageClient, cluster, database string, level int) (StoragePath, error) {
	itr, err := client.Iterator(ctx, cluster, database, level)
	if err != nil {
		return StoragePath{}, err
	}
	defer func() { _ = itr.Close() }()

	var ret StoragePath
	for i := 0; ; i++ {
		path, err := itr.NextStoragePath(ctx)
		if err == io.EOF {
			return ret, itr.Close()
		} else if err != nil {
			return StoragePath{}, err
		}
		ret = path
	}
}

// StorageTXIDRange returns the minimum & maximum TXID for a compaction level.
func StorageTXIDRange(ctx context.Context, client StorageClient, cluster, database string, level int) (min, max ltx.TXID, err error) {
	itr, err := client.Iterator(ctx, cluster, database, level)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = itr.Close() }()

	for i := 0; ; i++ {
		path, err := itr.NextStoragePath(ctx)
		if err == io.EOF {
			return min, max, itr.Close()
		} else if err != nil {
			return min, max, err
		}

		if i == 0 || path.MinTXID < min {
			min = path.MinTXID
		}
		if path.MaxTXID > max {
			max = path.MaxTXID
		}
	}
}

// StoragePathIterator iterates over a sorted list of storage paths.
type StoragePathIterator interface {
	io.Closer

	// NextStoragePath returns the next available storage path.
	// Returns io.EOF when no more paths are available.
	NextStoragePath(ctx context.Context) (StoragePath, error)
}

var _ StoragePathIterator = (*StoragePathSliceIterator)(nil)

// StoragePathSliceIterator iterates over a slice of storage files.
type StoragePathSliceIterator struct {
	a []StoragePath
}

// NewStoragePathSliceIterator returns a new instance of StoragePathSliceIterator.
func NewStoragePathSliceIterator(a []StoragePath) *StoragePathSliceIterator {
	return &StoragePathSliceIterator{a: a}
}

// Close is a no-op.
func (itr *StoragePathSliceIterator) Close() error { return nil }

// NextStoragePath returns the next available storage path.
// Returns io.EOF when no more paths are available.
func (itr *StoragePathSliceIterator) NextStoragePath(ctx context.Context) (StoragePath, error) {
	if len(itr.a) == 0 {
		return StoragePath{}, io.EOF
	}

	p := itr.a[0]
	itr.a = itr.a[1:]
	return p, nil
}

// StoragePath represents an path identifier for a file in long-term storage.
type StoragePath struct {
	Cluster  string
	Database string
	Level    int
	MinTXID  ltx.TXID
	MaxTXID  ltx.TXID

	Metadata  StorageMetadata // LTX-based metadata
	Size      int64           // file size
	CreatedAt time.Time       // timestamp of when the file was created (determined by storage system)
}

// NewStoragePath returns a new instance of StoragePath.
func NewStoragePath(cluster, database string, level int, minTXID, maxTXID ltx.TXID) StoragePath {
	return StoragePath{
		Cluster:  cluster,
		Database: database,
		Level:    level,
		MinTXID:  minTXID,
		MaxTXID:  maxTXID,
	}
}

// Equal returns true if the path portion of p equals other.
func (p StoragePath) Equal(other StoragePath) bool {
	return p.Cluster == other.Cluster &&
		p.Database == other.Database &&
		p.Level == other.Level &&
		p.MinTXID == other.MinTXID &&
		p.MaxTXID == other.MaxTXID
}

// IsZero returns true if p is the zero value.
func (p StoragePath) IsZero() bool {
	return p == (StoragePath{})
}

// String returns the string representation of p.
func (p StoragePath) String() string {
	s, err := FormatStoragePath(p, '/')
	if err != nil {
		return fmt.Sprintf("INVALIDPATH(%s)", s)
	}
	return s
}

// Validate returns nil if the path is valid.
func (p *StoragePath) Validate() error {
	if err := lfsb.ValidateClusterName(p.Cluster); err != nil {
		return err
	}
	if err := lfsb.ValidateDatabase(p.Database); err != nil {
		return err
	}
	if p.MinTXID == 0 {
		return lfsb.ErrMinTXIDRequired
	}
	if p.MaxTXID == 0 {
		return lfsb.ErrMaxTXIDRequired
	}
	return nil
}

// CompareStoragePath returns an integer comparing two paths.
// The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
func CompareStoragePath(a, b *StoragePath) int {
	if cmp := strings.Compare(a.Cluster, b.Cluster); cmp != 0 {
		return cmp
	}
	if cmp := strings.Compare(a.Database, b.Database); cmp != 0 {
		return cmp
	}

	if a.Level < b.Level {
		return -1
	} else if a.Level > b.Level {
		return 1
	}

	if a.MinTXID < b.MinTXID {
		return -1
	} else if a.MinTXID > b.MinTXID {
		return 1
	}

	if a.MaxTXID < b.MaxTXID {
		return -1
	} else if a.MaxTXID > b.MaxTXID {
		return 1
	}
	return 0
}

// StoragePathSlice represents a slice of storage paths.
type StoragePathSlice []StoragePath

// MaxTXID returns the MaxTXID of the last element in the slice.
// Returns zero if the slice is empty.
func (a StoragePathSlice) MaxTXID() ltx.TXID {
	if len(a) == 0 {
		return 0
	}
	return a[len(a)-1].MaxTXID
}

// FormatStorageLevelDir formats path to the storage level.
func FormatStorageLevelDir(cluster, database string, level int, sep rune) (_ string, retErr error) {
	if err := lfsb.ValidateClusterName(cluster); retErr == nil {
		retErr = err
	}
	if err := lfsb.ValidateDatabase(database); retErr == nil {
		retErr = err
	}
	return strings.Join([]string{cluster, database, strconv.Itoa(level)}, string(sep)), retErr
}

// FormatStoragePath formats path into a string path.
func FormatStoragePath(p StoragePath, sep rune) (_ string, retErr error) {
	if err := p.Validate(); retErr == nil {
		retErr = err
	}

	dir, err := FormatStorageLevelDir(p.Cluster, p.Database, p.Level, sep)
	if retErr == nil {
		retErr = err
	}

	return strings.Join([]string{dir, lfsb.FormatLTXFilename(p.MinTXID, p.MaxTXID)}, string(sep)), retErr
}

// StorageMetadata represents metadata associated with a StoragePath.
// This is data that is optionally stored with the path but is not a part of the formatted pathname.
type StorageMetadata struct {
	PageSize          uint32       // size of the pages in the file
	Commit            uint32       // size of the database after applying, in pages
	Timestamp         time.Time    // timestamp of when the transaction was committed
	PreApplyChecksum  ltx.Checksum // checksum of the database before applying LTX file
	PostApplyChecksum ltx.Checksum // checksum of the database after applying LTX file
}

// NewStorageMetadataFromLTX returns storage metadata from an LTX file's header & trailer.
func NewStorageMetadataFromLTX(header ltx.Header, trailer ltx.Trailer) StorageMetadata {
	return StorageMetadata{
		PageSize:          header.PageSize,
		Commit:            header.Commit,
		Timestamp:         time.UnixMilli(header.Timestamp).UTC(),
		PreApplyChecksum:  header.PreApplyChecksum,
		PostApplyChecksum: trailer.PostApplyChecksum,
	}
}

// IsZero returns true if all the fields are zero values.
func (m StorageMetadata) IsZero() bool {
	return m == (StorageMetadata{})
}

// ChangeSet represents a set of changed database pages.
type ChangeSet map[uint32]struct{}

// Add adds the given page number to the change set
func (c ChangeSet) Add(pgno uint32) {
	c[pgno] = struct{}{}
}

// AsSlice returns the contents of the change set as sorted slice
func (c ChangeSet) AsSlice() []uint32 {
	pgnos := make([]uint32, 0, len(c))

	for pgno := range c {
		pgnos = append(pgnos, pgno)
	}
	slices.Sort(pgnos)

	return pgnos
}

// NewChangeSetFromPaths computes a set of changed pages using the provided LTX files.
func NewChangeSetFromPaths(ctx context.Context, client StorageClient, paths []StoragePath) (ChangeSet, error) {
	changeSet := make(ChangeSet)

	for _, p := range paths {
		if err := addChangesFromPath(ctx, client, p, changeSet); err != nil {
			return nil, err
		}
	}

	return changeSet, nil
}

func addChangesFromPath(ctx context.Context, client StorageClient, path StoragePath, changeSet ChangeSet) error {
	f, err := client.OpenFile(ctx, path)
	if err != nil {
		return fmt.Errorf("open storage file (%s): %w", path, err)
	}
	defer func() {
		_ = f.Close()
	}()

	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode LTX header (%s): %w", path, err)
	}

	var hdr ltx.PageHeader
	page := make([]byte, dec.Header().PageSize)
	for {
		if err := dec.DecodePage(&hdr, page); err != nil && err != io.EOF {
			return fmt.Errorf("decode page (%s): %w", path, err)
		} else if err == io.EOF {
			break
		}
		changeSet.Add(hdr.Pgno)
	}

	return dec.Close()
}
