package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	_ "github.com/amacneil/dbmate/v2/pkg/driver/sqlite"
	"github.com/getsentry/sentry-go"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/db/migrations"
	"github.com/stephen/litefs-backup/db/sqliteutil"
	"github.com/superfly/ltx"
)

const (
	// CompactionLevelRestoreTarget is the default compaction level used to find an LTX file
	// suitable for restore to specific timestamp.
	//
	// This value corresponds with the granularity that restore requests can see.
	// Levels below CompactionLevelRestoreTarget effectively
	// become used for better write durability (writes after the L1
	// 10s window will be in durable storage) instead of for restorability.
	//
	// We choose a restore target higher than L1 to improve restore speed and usability,
	// since pulling many small L1 (default 10s granularity) files may take a while.
	CompactionLevelRestoreTarget = 2
)

type WriteTxOptions struct {
	AppendSnapshot bool
	Timestamp      time.Time
}

type WriteTxPage struct {
	Pgno uint32
	Data []byte
}

func NewStore(config *lfsb.Config) *Store {
	return &Store{
		config:            config,
		path:              config.Path,
		CompactionEnabled: true,
		SnapshotInterval:  24 * time.Hour,
		Levels: []*CompactionLevel{
			{Level: 0, Retention: 1 * time.Hour},
			{Level: 1, Interval: 10 * time.Second, Retention: 1 * time.Hour},
			{Level: 2, Interval: 5 * time.Minute, Retention: 30 * 24 * time.Hour},
			{Level: 3, Interval: 1 * time.Hour, Retention: 30 * 24 * time.Hour},
		},
		Now: func() time.Time {
			return time.Now()
		},
	}
}

type Store struct {
	config        *lfsb.Config
	db            *sql.DB
	compactionMus [lfsb.CompactionLevelSnapshot + 1]sync.Mutex

	// path is the directory where we store our data.
	path string

	RemoteClient StorageClient

	Levels CompactionLevels

	Now func() time.Time

	// If true, compactions are run in background goroutines.
	// This is typically disabled for testing purposes.
	CompactionEnabled bool

	// Time between snapshots
	SnapshotInterval time.Duration
}

// Path returns the root data directory.
func (s *Store) Path() string { return s.path }

func (s *Store) dbLogger(cluster, database string) *slog.Logger {
	return slog.With(
		slog.String("cluster", cluster),
		slog.String("database", database),
	)
}

func (s *Store) Open(ctx context.Context) error {
	if s.RemoteClient == nil {
		return fmt.Errorf("remote storage client required")
	}
	if s.SnapshotInterval <= 0 {
		return fmt.Errorf("snapshot interval required")
	}
	if err := s.Levels.Validate(); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}

	// Drop & recreate our "tmp" directory on the mount.
	// We use space on the mount so we don't get our IO throttled.
	if err := os.RemoveAll(s.TempDir()); err != nil {
		return err
	} else if err := os.MkdirAll(s.TempDir(), 0o777); err != nil {
		return err
	}

	path := filepath.Join(filepath.Dir(s.path), "server.db")

	db, err := sql.Open("lfsb-sqlite", path)
	if err != nil {
		return err
	}
	s.db = db
	u, _ := url.Parse(fmt.Sprintf("sqlite:%s", path))
	dbm := dbmate.New(u)
	dbm.FS = migrations.FS
	dbm.AutoDumpSchema = false
	dbm.MigrationsDir = []string{"."}
	dbm.Log = io.Discard

	if err := dbm.CreateAndMigrate(); err != nil {
		return err
	}

	if s.CompactionEnabled {
		// Begin monitoring at each compaction level.
		for _, lvl := range s.Levels {
			lvl := lvl
			if lvl.Interval == 0 {
				continue
			}

			// XXX: wait groups.
			go (func() error {
				defer sentry.Recover()
				slog.InfoContext(ctx, "monitoring compaction", slog.Int("level", lvl.Level), slog.Duration("interval", lvl.Interval), slog.Duration("retention", lvl.Retention))
				s.monitorCompactionLevel(ctx, lvl.Level)
				return nil
			})()
		}

		// Monitor snapshot compactions.
		go (func() error {
			defer sentry.Recover()
			s.monitorCompactionLevel(ctx, lfsb.CompactionLevelSnapshot)
			return nil
		})()
	}

	return nil
}

// TempDir returns a "tmp" directory under the data directory for temporary files.
func (s *Store) TempDir() string { return filepath.Join(s.path, "tmp") }

func (s *Store) createTemp(pattern string) (*os.File, error) {
	return os.CreateTemp(s.TempDir(), pattern)
}

// WriteTx appends an LTX file to a database.
func (s *Store) WriteTx(ctx context.Context, cluster, database string, r io.Reader, opt *WriteTxOptions) (ltx.Pos, error) {
	logger := s.dbLogger(cluster, database)
	if opt == nil {
		opt = &WriteTxOptions{}
	}

	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return ltx.Pos{}, fmt.Errorf("decode header: %w", err)
	}
	hdr := dec.Header()

	if opt.AppendSnapshot && !hdr.IsSnapshot() {
		return ltx.Pos{}, fmt.Errorf("ltx file must be a snapshot to append")
	}

	// Override the header timestamp, if requested.
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()
	if !opt.Timestamp.IsZero() {
		timestamp = opt.Timestamp.UTC()
	}

	var pages []*WriteTxPage
	for {
		var h ltx.PageHeader
		data := make([]byte, hdr.PageSize)
		if err := dec.DecodePage(&h, data); err == io.EOF {
			break
		} else if err != nil {
			return ltx.Pos{}, fmt.Errorf("decode page: %w", err)
		}

		pages = append(pages, &WriteTxPage{
			Pgno: h.Pgno,
			Data: data,
		})
	}

	// Read trailer once we've finished with pages. We'll attach the post-apply
	// checksum to indicate that this is the final write of the batch.
	if err := dec.Close(); err != nil {
		return ltx.Pos{}, fmt.Errorf("close ltx decoder: %w", err)
	}
	trailerPostApplyChecksum := dec.Trailer().PostApplyChecksum

	tx, err := sqliteutil.BeginImmediate(s.db)
	if err != nil {
		return ltx.Pos{}, err
	}
	defer tx.Rollback()

	// Lookup the database entry so we have a database ID to work with.
	db := &DB{
		Cluster: cluster,
		Name:    database,
	}
	if err := createDBIfNotExist(ctx, tx, db); err != nil {
		return ltx.Pos{}, fmt.Errorf("create database: %w", err)
	}

	if db.PageSize == 0 {
		db.PageSize = hdr.PageSize
	} else if db.PageSize != hdr.PageSize {
		return ltx.Pos{}, lfsb.ErrPageSizeMismatch
	}

	minTXID := hdr.MinTXID
	maxTXID := hdr.MaxTXID
	preApplyChecksum := hdr.PreApplyChecksum

	// If this is a restore or a replacement, update the header to be contiguous
	// with the previous transaction.
	if opt.AppendSnapshot {
		minTXID = db.TXID + 1
		maxTXID = db.TXID + 1
		preApplyChecksum = db.PostApplyChecksum
	}

	// Verify that LTX file is contiguous.
	if (db.Pos() != ltx.Pos{
		TXID:              ltx.TXID(minTXID - 1),
		PostApplyChecksum: preApplyChecksum,
	}) {
		logger.Debug("tx position mismatch",
			slog.String("pos", db.Pos().String()),
			slog.Group("ltx",
				slog.String("min_txid", ltx.TXID(minTXID).String()),
				slog.String("pre_apply_checksum", fmt.Sprintf("%x016x", preApplyChecksum)),
			),
		)
		return ltx.Pos{}, ltx.NewPosMismatchError(db.Pos())
	}

	txn := &Txn{
		DBID:              db.ID,
		MinTXID:           minTXID,
		MaxTXID:           maxTXID,
		PageSize:          hdr.PageSize,
		Commit:            hdr.Commit,
		Timestamp:         timestamp,
		PreApplyChecksum:  preApplyChecksum,
		PostApplyChecksum: ltx.ChecksumFlag, // temporary, updated before end of tx
	}

	if err := createTxn(ctx, tx, txn); err != nil {
		return ltx.Pos{}, fmt.Errorf("create txn: %w", err)
	}

	// Inserting pages will return a rolling post apply checksum.
	postApplyChecksum, err := insertPages(ctx, tx, db, txn, pages)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("insert pages: %w", err)
	}

	// Verify resulting checksum matches the incoming trailer's post-apply checksum.
	if trailerPostApplyChecksum != postApplyChecksum {
		return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADCHECKSUM", "incorrect post-apply checksum of %016x, expected %016x",
			trailerPostApplyChecksum, postApplyChecksum)
	}

	// Update write fields and the rolling checksum.
	txn.PostApplyChecksum = postApplyChecksum
	if _, err := tx.Exec(`
		UPDATE txns
		SET post_apply_checksum = ?
		WHERE db_id = ? AND min_txid = ? AND max_txid = ?
	`,
		sqliteutil.Checksum(postApplyChecksum),
		db.ID,
		minTXID,
		maxTXID,
	); err != nil {
		return ltx.Pos{}, fmt.Errorf("finalize txn state: %w", err)
	}

	// Request L1 compaction.
	if err := requestCompaction(ctx, tx, db.ID, 1, int(rand.Int63())); err != nil {
		return ltx.Pos{}, fmt.Errorf("request compaction: %w", err)
	}

	if minTXID == 1 {
		logger.Info("database created", slog.Int64("size", int64(hdr.Commit)*int64(hdr.PageSize)))
	}

	if err := tx.Commit(); err != nil {
		return ltx.Pos{}, err
	}

	logger.Debug("tx written",
		slog.String("pos", txn.PostApplyPos().String()))

	return ltx.Pos{
		TXID:              txn.MaxTXID,
		PostApplyChecksum: txn.PostApplyChecksum,
	}, nil
}

// FindDBByName returns a database entry by cluster/name.
func (s *Store) FindDBByName(ctx context.Context, cluster, name string) (*DB, error) {
	return findDBByName(ctx, s.db, cluster, name)
}

func (s *Store) FindDBsByCluster(ctx context.Context, cluster string) ([]*DB, error) {
	return findDBsByCluster(ctx, s.db, cluster)
}

// FindClusters returns a list of all clusters in the store.
func (s *Store) FindClusters(ctx context.Context) ([]string, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	return findClusters(ctx, tx)
}

// WriteLTXPageRangeFrom writes a compacted LTX file for all updated pages since minTXID inclusive.
// Returns the maximum transaction ID read up to. Returns ENOCOMPACTION if no transactions found.
func (s *Store) WriteLTXPageRangeFrom(ctx context.Context, cluster, database string, minTXID ltx.TXID, w io.Writer) (ltx.Header, ltx.Trailer, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return ltx.Header{}, ltx.Trailer{}, err
	}
	defer func() { _ = tx.Rollback() }()

	db, err := findDBByName(ctx, tx, cluster, database)
	if err != nil {
		return ltx.Header{}, ltx.Trailer{}, err
	}

	var txnN int
	var maxTXID ltx.TXID
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), IFNULL(MAX(max_txid),0)
		FROM txns
		WHERE db_id = ? AND min_txid >= ?
	`,
		db.ID, minTXID,
	).Scan(
		&txnN, &maxTXID,
	); err != nil {
		return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("count pending txns: %w", err)
	} else if txnN == 0 {
		return ltx.Header{}, ltx.Trailer{}, lfsb.Errorf(lfsb.ErrorTypeNotFound, lfsb.ENOCOMPACTION, "no compaction") // no new data, skip compaction
	}

	header, trailer, err := writeLTXPageRangeTo(ctx, tx, db, minTXID, maxTXID, w)
	if err != nil {
		return ltx.Header{}, ltx.Trailer{}, err
	}
	return header, trailer, nil
}

// FindTXIDByTimestamp returns the TX closest to the given timestamp.
func (s *Store) FindTXIDByTimestamp(ctx context.Context, cluster, database string, timestamp time.Time) (ltx.TXID, error) {
	path, err := s.FindStoragePathByTimestamp(ctx, cluster, database, timestamp)
	if err != nil {
		return 0, err
	}
	return path.MaxTXID, nil
}

func (s *Store) FindStoragePathByTimestamp(ctx context.Context, cluster, database string, timestamp time.Time) (StoragePath, error) {
	// Start at level 2, which is relatively low granularity and long retention and go
	// upper looking for older data.
	for level := CompactionLevelRestoreTarget; s.Levels.IsValidLevel(level); level = s.Levels.NextLevel(level) {
		paths, err := FindStoragePaths(ctx, s.RemoteClient, cluster, database, level, func(p StoragePath) (bool, error) {
			md, err := s.RemoteClient.Metadata(ctx, p)
			if err != nil {
				return false, err
			}
			if !md.Timestamp.After(timestamp) {
				return true, nil
			}

			return false, ErrStopIter
		})
		if err != nil {
			return StoragePath{}, err
		}

		if len(paths) > 0 {
			return paths[len(paths)-1], nil
		}
	}

	return StoragePath{}, lfsb.ErrTimestampNotAvailable

}

// RestoreToTx restores the database to the give TX ID.
func (s *Store) RestoreToTx(ctx context.Context, cluster, database string, txID ltx.TXID) (ltx.TXID, error) {
	logger := s.dbLogger(cluster, database)

	// Read TXID + post-apply checksum from last LTX file.
	db, err := s.FindDBByName(ctx, cluster, database)
	if err != nil {
		return 0, err
	}
	pos := db.Pos()

	if txID > pos.TXID {
		logger.Debug("restore to non-existing pos requested",
			slog.String("pos", pos.String()),
			slog.String("tx", txID.String()),
		)
		return 0, lfsb.ErrTxNotAvailable
	} else if txID == pos.TXID {
		logger.Debug("restore to last pos requested",
			slog.String("pos", pos.String()),
			slog.String("tx", txID.String()),
		)
		return pos.TXID, nil
	}

	logger.Info("fetching database for restore",
		slog.String("pos", pos.String()),
		slog.String("tx", txID.String()))

	// Write a full snapshot at the given TXID to a temporary file so we don't
	// block the database when we apply the diff under a write transaction.
	f, err := s.createTemp("restore-*.ltx")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()

	if err := s.WriteSnapshotTo(ctx, cluster, database, txID, f); err != nil {
		return 0, fmt.Errorf("write snapshot to temp file: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	newPos, err := s.WriteTx(ctx, cluster, database, f, &WriteTxOptions{
		AppendSnapshot: true,
		Timestamp:      s.Now(),
	})
	if err != nil {
		return 0, err
	}

	logger.Info("database restored", slog.Any("to", txID), slog.Any("newPos", newPos))

	return newPos.TXID, nil
}

// StoreDatabase stores a SQLite database read from rd as a single LTX file on
// top of any existing LTX files.
func (s *Store) StoreDatabase(ctx context.Context, cluster, database string, source io.Reader) (ltx.Pos, error) {
	source, hdr, err := readSQLiteDatabaseHeader(source)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("read sqlite header: %w", err)
	}

	// Rewrite database to LTX temp file.
	tempFile, err := s.createTemp("store-*.ltx")
	if err != nil {
		return ltx.Pos{}, err
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()
	defer func() { _ = tempFile.Close() }()

	enc := ltx.NewEncoder(tempFile)
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		PageSize:  hdr.pageSize,
		Commit:    hdr.pageN,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: s.Now().UnixMilli(),
	}); err != nil {
		return ltx.Pos{}, fmt.Errorf("encode ltx header: %w", err)
	}

	data := make([]byte, hdr.pageSize)
	var postApplyChecksum ltx.Checksum
	for pgno := uint32(1); pgno <= hdr.pageN; pgno++ {
		if _, err := io.ReadFull(source, data); err != nil {
			return ltx.Pos{}, fmt.Errorf("read page %d: %w", pgno, err)
		}

		if pgno == ltx.LockPgno(hdr.pageSize) {
			continue
		}

		postApplyChecksum = ltx.ChecksumFlag | (postApplyChecksum ^ ltx.ChecksumPage(pgno, data))

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			return ltx.Pos{}, fmt.Errorf("encode page %d: %w", pgno, err)
		}
	}

	enc.SetPostApplyChecksum(postApplyChecksum)
	if err := enc.Close(); err != nil {
		return ltx.Pos{}, fmt.Errorf("close ltx encoder: %w", err)
	} else if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return ltx.Pos{}, err
	}

	pos, err := s.WriteTx(ctx, cluster, database, tempFile, &WriteTxOptions{AppendSnapshot: true})
	if err != nil {
		return ltx.Pos{}, err
	}

	logger := s.dbLogger(cluster, database)
	logger.InfoContext(ctx, "database stored",
		slog.Any("tx", pos.TXID))

	return pos, nil
}

const (
	SQLITE_DATABASE_HEADER_STRING = "SQLite format 3\x00"

	SQLITE_DATABASE_HEADER_SIZE = 100
)

type sqliteDatabaseHeader struct {
	pageSize uint32
	pageN    uint32
}

func readSQLiteDatabaseHeader(rd io.Reader) (ord io.Reader, hdr sqliteDatabaseHeader, err error) {
	b := make([]byte, SQLITE_DATABASE_HEADER_SIZE)
	if _, err := io.ReadFull(rd, b); err == io.ErrUnexpectedEOF {
		return ord, hdr, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, "EBADHEADER", "invalid database header")
	} else if err != nil {
		return ord, hdr, err
	} else if !bytes.Equal(b[:len(SQLITE_DATABASE_HEADER_STRING)], []byte(SQLITE_DATABASE_HEADER_STRING)) {
		return ord, hdr, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, "EBADHEADER", "invalid database header")
	}

	hdr.pageSize = uint32(binary.BigEndian.Uint16(b[16:]))
	hdr.pageN = binary.BigEndian.Uint32(b[28:])
	if hdr.pageSize == 1 {
		hdr.pageSize = 65536
	}

	ord = io.MultiReader(bytes.NewReader(b), rd)

	return ord, hdr, nil
}

// DBInfo holds basic info about a single database.
type DBInfo struct {
	Name            string
	RestorablePaths []StoragePath
}

func (s *Store) Info(ctx context.Context, cluster, database string, all bool) (*DBInfo, error) {
	info := &DBInfo{Name: database}

	// We are restoring from level 2, so for now just list min/max L2 timestamps.
	paths, err := FindStoragePaths(ctx, s.RemoteClient, cluster, database, CompactionLevelRestoreTarget, nil)
	if err != nil {
		return nil, err
	} else if len(paths) == 0 {
		return info, nil // no files at level, not available for restore
	}

	// Fetch upper timestamp bound from last available path.
	last, err := s.RemoteClient.Metadata(ctx, paths[len(paths)-1])
	if err != nil {
		return nil, err
	}
	paths[len(paths)-1].Metadata = last

	info.RestorablePaths = paths
	if !all {
		info.RestorablePaths = []StoragePath{paths[0], paths[len(paths)-1]}
	}

	if err := AttachMetadata(ctx, s.RemoteClient, info.RestorablePaths, 32); err != nil {
		return nil, err
	}

	return info, nil
}
